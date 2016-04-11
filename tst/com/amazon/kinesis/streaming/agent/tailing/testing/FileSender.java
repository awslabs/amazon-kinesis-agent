/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.testing;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;

import org.slf4j.Logger;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.tailing.AbstractSender;
import com.amazon.kinesis.streaming.agent.tailing.BufferSendResult;
import com.amazon.kinesis.streaming.agent.tailing.IRecord;
import com.amazon.kinesis.streaming.agent.tailing.RecordBuffer;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;

/**
 * A sender that writes data to a file.
 * Provides many knobs to control latency, partial and complete failures at
 * different stages.
 *
 * @param <R> The type of the record.
 */
public class FileSender<R extends IRecord> extends AbstractSender<R> {
    private static final Logger LOGGER = TestUtils.getLogger(FileSender.class);

    public static final double DEFAULT_ERROR_AFTER_PARTIAL_COMMIT_RATE = 0.0;
    public static final int DEFAULT_AVERAGE_LATENCY_MILLIS = 100;
    public static final double DEFAULT_LATENCY_JITTER = 0.50;
    public static final double DEFAULT_PARTIAL_FAILURE_RATE = 0.0;
    public static final double DEFAULT_ERROR_BEFORE_COMMIT_RATE = 0.0;

    public static final int BAD_LATENCY = 50;
    public static final double BAD_JITTER = 0.75;
    public static final double BAD_PARTIAL_FAILURE_RATE = 0.05;
    public static final double BAD_ERROR_BEFORE_COMMIT_RATE = 0.05;
    public static final double BAD_ERROR_AFTER_PARTIAL_COMMIT_RATE = 0.03;


    @Getter private final Path outputFile;
    @Getter private final AgentContext agentContext;
    @Getter private final long averageLatencyMillis;
    @Getter private final double latencyJitter;
    @Getter private final double partialFailureRate;
    @Getter private final double errorBeforeCommitRate;
    @Getter private final double errorAfterPartialCommitRate;
    @Getter private final int maxSendBatchSizeRecords = -1;
    @Getter private final long maxSendBatchSizeBytes = -1;

    private final AtomicInteger expectedDuplicateRecords = new AtomicInteger();
    private final AtomicLong totalRecordsAttempted = new AtomicLong();
    private final AtomicLong totalRecordsSent = new AtomicLong();
    private final AtomicLong totalRecordsFailed = new AtomicLong();
    private final AtomicLong totalErrors = new AtomicLong();

    public FileSender(AgentContext agentContext, Path output) {
        this(agentContext, output, DEFAULT_AVERAGE_LATENCY_MILLIS, DEFAULT_LATENCY_JITTER, DEFAULT_PARTIAL_FAILURE_RATE, DEFAULT_ERROR_BEFORE_COMMIT_RATE, DEFAULT_ERROR_AFTER_PARTIAL_COMMIT_RATE);
    }

    /**
     *
     * @param agentContext
     * @param outputFile
     * @param averageLatencyMillis Average latency in millis to add to each
     *        request.
     * @param latencyJitter
     * @param partialFailureRate Percent of records in buffer that that will
     *        report failure to write to file.
     * @param errorBeforeCommitRate Percent of time to raise an error before
     *        any records were written to file. NOTE: this will not produce
     *        data duplication if buffer was retried.
     * @param errorAfterPartialCommitRate Percent of time to raise an error
     *        after a few records have been written to the file. NOTE: this,
     *        combined with the retry policies of the publisher, could result
     *        in data duplication.
     */
    public FileSender(AgentContext agentContext, Path outputFile, long averageLatencyMillis, double latencyJitter, double partialFailureRate, double errorBeforeCommitRate, double errorAfterPartialCommitRate) {
        Preconditions.checkNotNull(outputFile);
        this.agentContext = agentContext;
        this.outputFile = outputFile;
        this.averageLatencyMillis = averageLatencyMillis;
        this.latencyJitter = latencyJitter;
        this.partialFailureRate = partialFailureRate;
        this.errorBeforeCommitRate = errorBeforeCommitRate;
        this.errorAfterPartialCommitRate = errorAfterPartialCommitRate;
    }

    protected String name() {
        return getClass().getSimpleName() + "[" + outputFile + "]";
    }

    @Override
    protected synchronized BufferSendResult<R> attemptSend(RecordBuffer<R> buffer) {
        Stopwatch timer = Stopwatch.createStarted();
        try {
            long sleepMillis = TestUtils.sleep(averageLatencyMillis, latencyJitter);
            LOGGER.trace("{}:{} Slept for {}ms", name(), buffer, sleepMillis);
        } catch (InterruptedException e) {
            // When interrupted, proceed with the send
            Thread.currentThread().interrupt();
        }
        try {
            TestUtils.throwOccasionalError(errorBeforeCommitRate, new RuntimeException("Simulated service error in FileSender#attemptSend"));
            List<Integer> successfulRecords = new ArrayList<>();

            // Pick an index where we check if it's time to raise an error after a partial commit
            int indexForAfterCommitError = ThreadLocalRandom.current().nextInt(buffer.sizeRecords());

            // Decide ahead of time if we're going to have a partial failure
            int maxPartialFailures = TestUtils.decide(partialFailureRate) ?
                    (1 + ThreadLocalRandom.current().nextInt(Math.max(1, buffer.sizeRecords() / 4))) : 0;
            // If we're going to have a partial failure, we should ensure that at least one record fails
            int minPartialFailures = maxPartialFailures > 0 ? 1 : 0;
            try (FileChannel channel = FileChannel.open(outputFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                int index = 0;
                for (R record : buffer) {
                    if(minPartialFailures == 0 && (maxPartialFailures == 0 || TestUtils.decide(0.5))) {
                        successfulRecords.add(index);
                        channel.write(record.data().duplicate());
                    } else {
                        --maxPartialFailures;
                        minPartialFailures = Math.max(minPartialFailures - 1, 0);
                    }
                    if(index == indexForAfterCommitError) {
                        // Keep track of possible duplicates, in case we raise an exception
                        expectedDuplicateRecords.addAndGet(index + 1);
                        TestUtils.throwOccasionalError(errorAfterPartialCommitRate, new RuntimeException("Simulated failure after partial commit in FileSender#attemptSend"));
                        // No exeception raise == no duplicates for this buffer
                        expectedDuplicateRecords.addAndGet(-index - 1);
                    }
                    ++index;
                }
                int totalRecords = buffer.sizeRecords();
                totalRecordsAttempted.addAndGet(totalRecords);
                totalRecordsSent.addAndGet(successfulRecords.size());
                totalRecordsFailed.addAndGet(totalRecords - successfulRecords.size());
                if(successfulRecords.size() == totalRecords) {
                    LOGGER.trace("{}:{} Buffer written to file successfully.",
                            name(), buffer);
                    return BufferSendResult.succeeded(buffer);
                } else {
                    LOGGER.trace("{}:{} Buffer written to file with partial failure: {} written, {} failed.",
                            name(), buffer, successfulRecords.size(),
                            totalRecords - successfulRecords.size());
                    buffer = buffer.remove(successfulRecords);
                    return BufferSendResult.succeeded_partially(buffer, totalRecords);
                }
            } catch (IOException e) {
                LOGGER.trace("{}:{} error writing buffer data.", name(), buffer, e);
                throw Throwables.propagate(e);
            } finally {
                LOGGER.trace("{}:{} Total buffer send time: {}ms", name(), buffer, timer.elapsed(TimeUnit.MILLISECONDS));
            }
        } catch (Exception e) {
            totalErrors.incrementAndGet();
            throw Throwables.propagate(e);
        }
    }

    @Override
    public String getDestination() {
        return outputFile.toString();
    }

    @SuppressWarnings("serial")
    @Override
    public Map<String, Object> getMetrics() {
        return new HashMap<String, Object>() {{
            put("FileSender.ExpectedDuplicateRecords", expectedDuplicateRecords);
            put("FileSender.TotalRecordsAttempted", totalRecordsAttempted);
            put("FileSender.TotalRecordsSent", totalRecordsSent);
            put("FileSender.TotalRecordsFailed", totalRecordsFailed);
            put("FileSender.TotalErrors", totalErrors);
        }};
    }


    public static abstract class FileSenderFactory<R extends IRecord> {
        public abstract FileSender<R> create(AgentContext context, Path outputFile);
        public boolean producesDuplicates() { return false; }
    }

    public static class PerfectFileSenderFactory<R extends IRecord> extends FileSenderFactory<R> {
        @Override
        public FileSender<R> create(AgentContext context, Path outputFile) {
            return new FileSender<R>(context, outputFile);
        }
    }

    public static class FileSenderWithHighLatencyFactory<R extends IRecord> extends FileSenderFactory<R> {

        @Override
        public FileSender<R> create(AgentContext context, Path outputFile) {
            return new FileSender<R>(context, outputFile,
                    BAD_LATENCY, BAD_JITTER, 0.0, 0.0, 0.0);
        }
    }

    public static class FileSenderWithPartialFailuresFactory<R extends IRecord> extends FileSenderFactory<R> {
        @Override
        public FileSender<R> create(AgentContext context, Path outputFile) {
            return new FileSender<R>(context, outputFile,
                    FileSender.DEFAULT_AVERAGE_LATENCY_MILLIS,
                    FileSender.DEFAULT_LATENCY_JITTER,
                    BAD_PARTIAL_FAILURE_RATE, 0.0, 0.0);
        }
    }

    public static class FileSenderWithErrorsBeforeCommitFactory<R extends IRecord> extends FileSenderFactory<R> {

        @Override
        public FileSender<R> create(AgentContext context, Path outputFile) {
            return new FileSender<R>(context, outputFile,
                    FileSender.DEFAULT_AVERAGE_LATENCY_MILLIS,
                    FileSender.DEFAULT_LATENCY_JITTER,
                    0.0, BAD_ERROR_BEFORE_COMMIT_RATE, 0.0);
        }
    }

    /**
     * This sender can cause data duplication.
     * @param <R>
     */
    public static class FileSenderWithErrorsAfterPartialCommitFactory<R extends IRecord> extends FileSenderFactory<R> {
        @Override
        public FileSender<R> create(AgentContext context, Path outputFile) {
            return new FileSender<R>(context, outputFile,
                    FileSender.DEFAULT_AVERAGE_LATENCY_MILLIS,
                    FileSender.DEFAULT_LATENCY_JITTER,
                    0.0, 0.0, BAD_ERROR_AFTER_PARTIAL_COMMIT_RATE);
        }

        @Override
        public boolean producesDuplicates() { return true; }
    }

    public static class FileSenderWithPartialFailuresAndErrorsBeforeCommitFactory<R extends IRecord> extends FileSenderFactory<R> {
        @Override
        public FileSender<R> create(AgentContext context, Path outputFile) {
            return new FileSender<R>(context, outputFile,
                    FileSender.DEFAULT_AVERAGE_LATENCY_MILLIS,
                    FileSender.DEFAULT_LATENCY_JITTER,
                    BAD_PARTIAL_FAILURE_RATE, BAD_ERROR_BEFORE_COMMIT_RATE, 0.0);
        }
    }

    /**
     * This sender can cause data duplication.
     * @param <R>
     */
    public static class MisbehavingFileSenderFactory<R extends IRecord> extends FileSenderFactory<R> {

        @Override
        public FileSender<R> create(AgentContext context, Path outputFile) {
            return new FileSender<R>(context, outputFile,
                    BAD_LATENCY, BAD_JITTER, BAD_PARTIAL_FAILURE_RATE,
                    BAD_ERROR_BEFORE_COMMIT_RATE,
                    BAD_ERROR_AFTER_PARTIAL_COMMIT_RATE);
        }

        @Override
        public boolean producesDuplicates() { return true; }
    }
}
