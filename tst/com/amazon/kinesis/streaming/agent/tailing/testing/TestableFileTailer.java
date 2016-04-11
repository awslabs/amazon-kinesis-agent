/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.testing;

import java.io.IOException;

import org.slf4j.Logger;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.tailing.AsyncPublisherService;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FileTailer;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseConstants;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseParser;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;
import com.amazon.kinesis.streaming.agent.tailing.IParser;
import com.amazon.kinesis.streaming.agent.tailing.IRecord;
import com.amazon.kinesis.streaming.agent.tailing.SourceFileTracker;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpointStore;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.SQLiteFileCheckpointStore;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Service;

/**
 * A file tailer with additional functionality to support unit tests.
 *
 * @param <R>
 */
public class TestableFileTailer<R extends IRecord> extends FileTailer<R> {
    private static final Logger LOGGER = TestUtils.getLogger(TestableFileTailer.class);

    public static TestableFileTailer<FirehoseRecord> createFirehoseTailer(AgentContext context, FileFlow<FirehoseRecord> flow, FileSender<FirehoseRecord> sender, FileCheckpointStore checkpoints) throws IOException {
        IParser<FirehoseRecord> parser = new FirehoseParser(flow, FirehoseConstants.DEFAULT_PARSER_BUFFER_SIZE_BYTES);
        SourceFileTracker fileTracker = new SourceFileTracker(context, flow);
        checkpoints = checkpoints == null ? new SQLiteFileCheckpointStore(context) : checkpoints;
        AsyncPublisherService<FirehoseRecord> publisher = new AsyncPublisherService<>(context, flow, checkpoints, sender, context.createSendingExecutor());
        return new TestableFileTailer<FirehoseRecord>(context, flow, fileTracker, publisher, parser, checkpoints);
    }

    private volatile boolean paused = false;

    public TestableFileTailer(AgentContext agentContext, FileFlow<R> flow, SourceFileTracker fileTracker,
            AsyncPublisherService<R> publisher, IParser<R> parser, FileCheckpointStore checkpoints) throws IOException {
        super(agentContext, flow, fileTracker, publisher, parser, checkpoints);
    }

    public synchronized Service pause() {
        paused = true;
        return this;
    }

    public synchronized Service resume() {
        if(paused) {
            paused = false;
        }
        return this;
    }

    public synchronized Service startAsync(boolean paused) {
        this.paused = paused;
        return startAsync();
    }

    @Override
    protected int runOnce() {
        if (!paused)
            return super.runOnce();
        else
            return 0;
    }

    @Override
    protected int processRecords() {
        Preconditions.checkArgument(isRunning());
        int processed = super.processRecords();
        LOGGER.debug("Processed {} records.", processed);
        return processed;
    }

    @Override
    public void waitForIdle() {
        Preconditions.checkState(isRunning());
        super.waitForIdle();
    }

    public void processAllRecords() throws Exception {
        Preconditions.checkState(isRunning() && paused);
        final long sleepTime = 1000;
        // Even if processRecords returned without anything done, there
        //   could still be files pending. Waiting and refreshing file system
        //   view will process them.
        updateRecordParser(true);
        while(moreInputPending()) {
            LOGGER.debug("More input is pending in tailer: " +
            		"Parser buffer rmaining: {}, " +
            		"Parser current open file: {}, " +
                    "Parser open file offset: {}, " +
                    "Parser open file size: {}, " +
            		"Tracker newer files pending: {}",
                    parser.bufferedBytesRemaining(),
                    parser.getCurrentFile(),
                    parser.getCurrentFile().getCurrentOffset(),
                    parser.getCurrentFile().getSize(),
                    fileTracker.newerFilesPending());
            updateRecordParser(true);
            processRecords();
            Thread.sleep(sleepTime);
        }
        publisher.flush();
        LOGGER.debug("No more input is pending in tailer.");
    }
}