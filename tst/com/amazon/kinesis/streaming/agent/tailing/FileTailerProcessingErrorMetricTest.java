/*
 * Copyright (c) 2014-2017 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpointStore;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.SQLiteFileCheckpointStore;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileSender;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileRotator;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;

/**
 * Verifies that {@link FileTailer} tracks the consecutive-processing-error gauge
 * that backs the agent's stuck-tailer failure metric: it climbs while every
 * iteration fails (the error-loop signal alarms watch for) and resets to 0 as soon
 * as the tailer makes progress again or reaches a healthy idle state.
 */
public class FileTailerProcessingErrorMetricTest extends TailingTestBase {

    /**
     * A tailer whose record-processing step can be forced to throw, so we can drive
     * the error-accounting paths in {@link FileTailer#processRecords()} directly
     * without standing up the full async publisher/sender machinery.
     */
    private static class ControllableFileTailer extends FileTailer<FirehoseRecord> {
        volatile boolean failNextProcess = false;

        ControllableFileTailer(AgentContext context, FileFlow<FirehoseRecord> flow,
                AsyncPublisherService<FirehoseRecord> publisher, IParser<FirehoseRecord> parser,
                FileCheckpointStore checkpoints) throws IOException {
            super(context, flow, new SourceFileTracker(context, flow), publisher, parser, checkpoints);
        }

        @Override
        protected synchronized boolean updateRecordParser(boolean forceRefresh) throws IOException {
            if (failNextProcess) {
                throw new IllegalArgumentException("simulated unrecoverable tracker error");
            }
            // Pretend there is no file to tail: a clean, no-op iteration.
            return false;
        }
    }

    private ControllableFileTailer createTailer() throws IOException {
        FileRotator rotator = new CreateFileRotatorFactory().create();
        rotator.rotate();
        AgentContext context = getTestAgentContext(rotator.getInputFileGlob());
        @SuppressWarnings("unchecked")
        FileFlow<FirehoseRecord> flow = (FileFlow<FirehoseRecord>) context.flows().get(0);
        FileCheckpointStore checkpoints = new SQLiteFileCheckpointStore(context);
        IParser<FirehoseRecord> parser = new FirehoseParser(flow, FirehoseConstants.DEFAULT_PARSER_BUFFER_SIZE_BYTES);
        FileSender<FirehoseRecord> sender = new FileSender.PerfectFileSenderFactory<FirehoseRecord>()
                .create(context, testFiles.createTempFile());
        AsyncPublisherService<FirehoseRecord> publisher =
                new AsyncPublisherService<>(context, flow, checkpoints, sender, context.createSendingExecutor());
        return new ControllableFileTailer(context, flow, publisher, parser, checkpoints);
    }

    @Test
    public void testConsecutiveErrorsClimbWhileStuckAndResetOnRecovery() throws IOException {
        ControllableFileTailer tailer = createTailer();
        Assert.assertEquals(tailer.getConsecutiveProcessingErrors(), 0);

        // Every iteration fails: the gauge must climb monotonically (the signal an
        // alarm fires on when the tailer is wedged in an error loop).
        tailer.failNextProcess = true;
        tailer.processRecords();
        Assert.assertEquals(tailer.getConsecutiveProcessingErrors(), 1);
        tailer.processRecords();
        tailer.processRecords();
        Assert.assertEquals(tailer.getConsecutiveProcessingErrors(), 3);

        // The tailer recovers: a clean iteration resets the gauge to 0 so the alarm
        // clears and a single transient error never lingers.
        tailer.failNextProcess = false;
        tailer.processRecords();
        Assert.assertEquals(tailer.getConsecutiveProcessingErrors(), 0);
    }

    @Test
    public void testTransientErrorDoesNotLatchGauge() throws IOException {
        ControllableFileTailer tailer = createTailer();

        // A single failure followed by a healthy (idle) iteration must leave the
        // gauge at 0, so transient blips don't trip the alarm.
        tailer.failNextProcess = true;
        tailer.processRecords();
        Assert.assertEquals(tailer.getConsecutiveProcessingErrors(), 1);

        tailer.failNextProcess = false;
        tailer.processRecords();
        Assert.assertEquals(tailer.getConsecutiveProcessingErrors(), 0);
    }
}
