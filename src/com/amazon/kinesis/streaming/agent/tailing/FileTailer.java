/*
 * Copyright 2014-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License. 
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/asl/
 *  
 * or in the "license" file accompanying this file. 
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;

import org.joda.time.Duration;
import org.slf4j.Logger;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.IHeartbeatProvider;
import com.amazon.kinesis.streaming.agent.Logging;
import com.amazon.kinesis.streaming.agent.metrics.Metrics;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpoint;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpointStore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AbstractScheduledService;

/**
 * Component responsible for tailing a single file, parsing into records and
 * passing buffers of records downstream to the destination.
 * The tailer is configured using a {@link FileFlow} and supports
 * tailing logs within a single directory.
 */
// TODO: Refactor into two classes: FileTailer and FileTailerService, similar to AsyncPublisher and AsyncPublisherService
public class FileTailer<R extends IRecord> extends AbstractExecutionThreadService implements IHeartbeatProvider {
    private static final int NO_TIMEOUT = -1;
    private static final int MAX_SPIN_WAIT_TIME_MILLIS = 1000;
    private static final Logger LOGGER = Logging.getLogger(FileTailer.class);

    @Getter private final AgentContext agentContext;
    @Getter private final FileFlow<R> flow;
    private final String serviceName;
    protected final SourceFileTracker fileTracker;
    protected final IParser<R> parser;
    @VisibleForTesting
    @Getter private final FileCheckpointStore checkpoints;
    protected final AsyncPublisherService<R> publisher;

    protected long lastStatusReportTime = 0;
    private R pendingRecord = null;
    private boolean isNewFile = false;
    protected final long minTimeBetweenFilePollsMillis;
    protected final long maxTimeBetweenFileTrackerRefreshMillis;
    private AbstractScheduledService metricsEmitter;

    private boolean isInitialized = false;
    private final AtomicLong recordsTruncated = new AtomicLong();

    public FileTailer(AgentContext agentContext,
            FileFlow<R> flow,
            SourceFileTracker fileTracker,
            AsyncPublisherService<R> publisher,
            IParser<R> parser,
            FileCheckpointStore checkpoints) throws IOException {
        super();
        this.agentContext = agentContext;
        this.flow = flow;
        this.serviceName = super.serviceName() + "[" + this.flow.getId() + "]";
        this.checkpoints = checkpoints;
        this.publisher = publisher;
        this.parser = parser;
        this.fileTracker = new SourceFileTracker(this.agentContext, this.flow);
        this.minTimeBetweenFilePollsMillis = flow.minTimeBetweenFilePollsMillis();
        this.maxTimeBetweenFileTrackerRefreshMillis = flow.maxTimeBetweenFileTrackerRefreshMillis();
        this.metricsEmitter = new AbstractScheduledService() {
            @Override
            protected void runOneIteration() throws Exception {
                FileTailer.this.emitStatus();
            }

            @Override
            protected Scheduler scheduler() {
                return Scheduler.newFixedRateSchedule(FileTailer.this.agentContext.logStatusReportingPeriodSeconds(),
                        FileTailer.this.agentContext.logStatusReportingPeriodSeconds(), TimeUnit.SECONDS);
            }

            @Override
            protected String serviceName() {
                return FileTailer.this.serviceName() + ".MetricsEmitter";
            }

            @Override
            protected void shutDown() throws Exception {
                LOGGER.debug("{}: shutting down...", serviceName());
                // Emit status one last time before shutdown
                FileTailer.this.emitStatus();
                super.shutDown();
            }
        };
    }

    @Override
    public void run() {
        do {
            if (0 == runOnce() && !isNewFile) {
                // Sleep only if the previous run did not process any records
                if(isRunning() && minTimeBetweenFilePollsMillis > 0) {
                    try {
                        Thread.sleep(minTimeBetweenFilePollsMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOGGER.trace("{}: Thread interrupted", e);
                    }
                }
            }
        } while (isRunning());
    }

    protected int runOnce() {
        return processRecords();
    }

    @VisibleForTesting
    public synchronized void initialize() throws IOException {
        if(isInitialized)
            return;
        FileCheckpoint cp = checkpoints.getCheckpointForFlow(flow);
        if (cp != null) {
            LOGGER.debug("{}: Found checkpoint {}.", serviceName(), cp);
            if (fileTracker.initialize(cp) && parser.continueParsingWithFile(fileTracker.getCurrentOpenFile())) {
                Preconditions.checkNotNull(fileTracker.getCurrentOpenFile());
                LOGGER.info("{}: Successfully started tailing from previous checkpoint {}@{}", serviceName(), fileTracker.getCurrentOpenFile().getPath(), cp.getOffset());
            } else {
                LOGGER.warn("{}: Failed to start tailing from previous checkpoint {}@{}", serviceName(), cp.getFile(), cp.getOffset());
                parser.startParsingFile(fileTracker.getCurrentOpenFile());
            }
        } else {
            LOGGER.debug("{}: No checkpoints were found. Looking for new files to tail...", serviceName());
            fileTracker.initialize();
            parser.startParsingFile(fileTracker.getCurrentOpenFile());
        }
        // SANITYCHECK: can remove when done debugging
        Preconditions.checkState(fileTracker.getCurrentOpenFile() == parser.getCurrentFile());
        if(fileTracker.getCurrentOpenFile() != null) {
            Preconditions.checkState(fileTracker.getCurrentOpenFile().getCurrentOffset() == parser.getCurrentFile().getCurrentOffset());
        }
        isNewFile = parser.isParsing();
        isInitialized = true;
    }

    /**
     * @return {@code false} if there are any records queued for sending or
     *         currently being sent (e.g. asynchronously), or if there are more
     *         data pending in files, else {@code true}.
     */
    public synchronized boolean isIdle() {
        try {
            return publisher.isIdle() && !moreInputPending();
        } catch (IOException e) {
            LOGGER.error("{}: Error when checking Idle status", serviceName(), e);
            return false;
        }
    }

    @VisibleForTesting
    public void waitForIdle() {
        waitForIdle(NO_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /**
     * @param timeout Use a value {@code <= 0} to wait indefinitely.
     * @param unit
     * @return {@code true} if idle state was reached before the timeout
     *         expired, or {@code false} if idle state was not successfully
     *         reached.
     */
    public boolean waitForIdle(long timeout, TimeUnit unit) {
        Stopwatch timer = Stopwatch.createStarted();
        while(!isIdle()) {
            long remaining = timeout > 0 ?
                    (unit.toMillis(timeout) - timer.elapsed(TimeUnit.MILLISECONDS))
                    : Long.MAX_VALUE;
            if(remaining <= 0)
                return false;
            long sleepTime = Math.min(MAX_SPIN_WAIT_TIME_MILLIS, remaining);
            LOGGER.trace("{}: Waiting IDLE. Sleeping {}ms. {}", serviceName(), sleepTime, toString());
            publisher.flush();
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                // No need to make this method interruptible. Just return false signifying timeout.
                Thread.currentThread().interrupt();
                LOGGER.trace("{}: Thread interrupted while waiting for tailer to go idle.", serviceName(), e);
                return false;
            }
        }
        return true;
    }

    @Override
    protected synchronized void startUp() throws Exception {
        LOGGER.debug("{}: Starting up...", serviceName());
        super.startUp();
        initialize();
        metricsEmitter.startAsync();
        publisher.startPublisher();
    }

    @Override
    protected synchronized void triggerShutdown() {
        LOGGER.debug("{}: Shutdown triggered...", serviceName());
        super.triggerShutdown();
        publisher.stopPublisher();
        metricsEmitter.stopAsync();
    }

    @Override
    protected String serviceName() {
        return serviceName;
    }

    protected synchronized int processRecords() {
        int processed = 0;
        try {
            if(!updateRecordParser(false)) {
                LOGGER.trace("{}: There's no file being tailed.", serviceName());
                return 0;
            }
            processed = processRecordsInCurrentFile();
            if (parser.isAtEndOfCurrentFile()) {
                // We think we reached the end of the current file...
                LOGGER.debug("{}: We reached end of file {}.", serviceName(), parser.getCurrentFile());
                if(isRunning() && fileTracker.onEndOfCurrentFile()) {
                    LOGGER.debug("{}: Switching to next file {}.", serviceName(), fileTracker.getCurrentOpenFile());
                    // fileTracker.onEndOfCurrentFile() returns true if the current
                    // file has changed. In that case, point to the new file without
                    // resetting parser. If it returns false, it means the current file
                    // is the only available file and we only need to wait for more
                    // data to be written to it.
                    isNewFile = parser.continueParsingWithFile(fileTracker.getCurrentOpenFile());
                }
            }
        } catch (Exception e) {
            LOGGER.error("{}: Error when processing current input file or when tracking its status.", serviceName(), e);
        }
        return processed;
    }

    protected synchronized int processRecordsInCurrentFile() throws IOException {
        isNewFile = false;
        // See if there's a pending record from the previous run, and start
        // with it, otherwise, read a new record from the parser.
        R record = pendingRecord == null ? parser.readRecord() : pendingRecord;
        pendingRecord = null;
        int processed = 0;
        while(record != null) {
        	if (record.length() > flow.getMaxRecordSizeBytes()) {
                record.truncate();
                recordsTruncated.incrementAndGet();
                LOGGER.warn("{}: Truncated a record in {}, because it exceeded the the configured max record size: {}", serviceName(), parser.getCurrentFile(), flow.getMaxRecordSizeBytes());
            }
            // Process a slice of records, and then check if we've been asked to stop
            if(isRunning() && publisher.publishRecord(record)) {
                ++processed;
                // Read the next record
                record = parser.readRecord();
            } else {
                // Publisher is exerting back-pressure. Return immediately
                // to stop further processing and give time for publisher to
                // catch up.
                LOGGER.debug("{}: record publisher exerting backpressure. Backing off a bit.", serviceName());
                pendingRecord = record;
                return processed;
            }
        }
        return processed;
    }

    /**
     * @return {@code true} if we suspect there's more data in current file, or
     *         if there are newer files waiting to be tailed, {@code false}
     *         otherwise.
     * @throws IOException
     */
    public synchronized boolean moreInputPending() throws IOException {
        try {
            updateRecordParser(false);
        } catch (IOException e) {
            // Ignore
        }
        TrackedFile currentFile = parser.getCurrentFile();
        return parser.bufferedBytesRemaining() > 0
                || (currentFile != null && currentFile.getCurrentOffset() < currentFile.getSize())
                || fileTracker.newerFilesPending();
    }

    protected synchronized long bytesBehind() {
        try {
            long result = parser.bufferedBytesRemaining();
            TrackedFile currentFile = parser.getCurrentFile();
            if (currentFile != null) {
                result += currentFile.getChannel().size() - currentFile.getChannel().position();
            }
            for(TrackedFile f : fileTracker.getPendingFiles()) {
                result += f.getSize();
            }
            return result;
        } catch (IOException e) {
            LOGGER.error("{}: Failed when calculating bytes behind.", serviceName(), e);
            return 0;
        }
    }

    protected synchronized int filesBehind() {
        return fileTracker.getPendingFiles().size();
    }

    /**
     *
     * @param forceRefresh If {@code true}, the file tracker will force a
     *        refresh of the current snapshot.
     * @return {@code true} if there's a file currently being parser, and
     *         {@code false} otherwise.
     * @throws IOException
     */
    protected synchronized boolean updateRecordParser(boolean forceRefresh) throws IOException {
        if(isRunning()) {
            boolean resetParsing = false;
            boolean refreshed = false;
            long elapsedSinceLastRefresh = System.currentTimeMillis() - fileTracker.getLastRefreshTimestamp();
            // Refresh sparingly to save CPU cycles and unecessary IO...
            if(forceRefresh
                    || elapsedSinceLastRefresh >= maxTimeBetweenFileTrackerRefreshMillis
                    || fileTracker.mustRefresh()) {
                LOGGER.trace("{} is refreshing current tailed file.", serviceName());
                if(fileTracker.getLastRefreshTimestamp() > 0)
                    LOGGER.trace("{}: Time since last refresh: {}", serviceName(), Duration.millis(elapsedSinceLastRefresh));
                resetParsing = !fileTracker.refresh();
                refreshed = true;
            }
            // Only update the parser if something changed
            if(refreshed || (!parser.isParsing() && fileTracker.getCurrentOpenFile() != null)) {
                TrackedFile currentFile = parser.getCurrentFile();
                TrackedFile newFile = fileTracker.getCurrentOpenFile();
                if (LOGGER.isDebugEnabled()) {
                    if (currentFile != null) {
                        if (newFile.getId().equals(currentFile.getId()) && newFile.getPath().equals(currentFile.getPath())) {
                            LOGGER.trace("{}: Continuing to tail current file {}.", serviceName(), currentFile.getPath());
                        } else {
                            LOGGER.debug("{}: Switching files. Rotation might have happened.\nOld file: {}\nNew file: {}",
                                    serviceName(), currentFile, newFile);
                        }
                    } else if (newFile != null)
                        LOGGER.debug("{}: Found file to tail: {}", serviceName(), newFile);
                }
                if (resetParsing) {
                    if(currentFile != null) {
                        LOGGER.warn("{}: Parsing was reset when switching to new file {} (from {})!", serviceName(), newFile, currentFile);
                    }
                    parser.switchParsingToFile(newFile);
                } else {
                    parser.continueParsingWithFile(newFile);
                }
            }
        }
        return parser.isParsing();
    }

    public String getId() {
        return flow.getId();
    }

    @Override
    public String toString() {
        return serviceName();
    }

    @Override
    public Object heartbeat(AgentContext agent) {
        return publisher.heartbeat(agent);
    }

    private void emitStatus() {
        try {
            Map<String, Object> metrics = getMetrics();
            if (flow.logEmitInternalMetrics()) {
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    LOGGER.info("{}: File Tailer Status: {}", serviceName(), mapper.writeValueAsString(metrics));
                } catch (JsonProcessingException e) {
                    LOGGER.error("{}: Failed when emitting file tailer status metrics.", serviceName(), e);
                }
            }
            AtomicLong zero = new AtomicLong(0);
            long bytesBehind = Metrics.getMetric(metrics, Metrics.FILE_TAILER_BYTES_BEHIND_METRIC, 0L);
            int filesBehind = Metrics.getMetric(metrics, Metrics.FILE_TAILER_FILES_BEHIND_METRIC, 0);
            long bytesConsumed = Metrics.getMetric(metrics, Metrics.PARSER_TOTAL_BYTES_CONSUMED_METRIC, zero).get();
            long recordsParsed = Metrics.getMetric(metrics, Metrics.PARSER_TOTAL_RECORDS_PARSED_METRIC, zero).get();
            long recordsProcessed = Metrics.getMetric(metrics, Metrics.PARSER_TOTAL_RECORDS_PROCESSED_METRIC, zero).get();
            long recordsSkipped = Metrics.getMetric(metrics, Metrics.PARSER_TOTAL_RECORDS_SKIPPED_METRIC, zero).get();
            long recordsSent = Metrics.getMetric(metrics, Metrics.SENDER_TOTAL_RECORDS_SENT_METRIC, zero).get();
            LOGGER.info("{}: Tailer Progress: Tailer has parsed {} records ({} bytes), transformed {} records, skipped {} records, and has successfully sent {} records to destination.",
                    serviceName(), recordsParsed, bytesConsumed, recordsProcessed, recordsSkipped, recordsSent);
            String msg = String.format("%s: Tailer is %02f MB (%d bytes) behind.", serviceName(),
                    bytesBehind / 1024 / 1024.0, bytesBehind);
            if (filesBehind > 0) {
                msg += String.format(" There are %d file(s) newer than current file(s) being tailed.", filesBehind);
            }
            if (bytesBehind >= Metrics.BYTES_BEHIND_WARN_LEVEL) {
                LOGGER.warn(msg);
            } else if (bytesBehind >= Metrics.BYTES_BEHIND_INFO_LEVEL || agentContext.logEmitInternalMetrics()) {
                LOGGER.info(msg);
            } else if (bytesBehind > 0) {
                LOGGER.debug(msg);
            }
        } catch (Exception e) {
            LOGGER.error("{}: Failed while emitting tailer status.", serviceName(), e);
        }
    }

    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = publisher.getMetrics();
        metrics.putAll(parser.getMetrics());
        metrics.put("FileTailer.FilesBehind", filesBehind());
        metrics.put("FileTailer.BytesBehind", bytesBehind());
        metrics.put("FileTailer.RecordsTruncated", recordsTruncated);
        return metrics;
    }
}
