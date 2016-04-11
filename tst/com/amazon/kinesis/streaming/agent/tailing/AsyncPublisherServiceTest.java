/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.tailing.AsyncPublisherService;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;
import com.amazon.kinesis.streaming.agent.tailing.ISender;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpoint;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpointStore;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.SQLiteFileCheckpointStore;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileSender;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileSender.FileSenderFactory;
import com.google.common.base.Joiner;


public class AsyncPublisherServiceTest extends TailingTestBase {
    private static final int TEST_TIMEOUT = 60_000;
    private static final int TEST_REPS = 2;
    private AgentContext context;
    private FileFlow<FirehoseRecord> flow;

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void setup() throws IOException {
        context = getTestAgentContext();
        flow = (FileFlow<FirehoseRecord>) context.flows().get(0);
    }

    @DataProvider(name="senders")
    private Object[][] getSendersData() {
        return new Object[][] {
                {new FileSender.PerfectFileSenderFactory<FirehoseRecord>()},
                {new FileSender.FileSenderWithHighLatencyFactory<FirehoseRecord>()},
                {new FileSender.FileSenderWithPartialFailuresFactory<FirehoseRecord>()},
                {new FileSender.FileSenderWithErrorsBeforeCommitFactory<FirehoseRecord>()},
                {new FileSender.FileSenderWithPartialFailuresAndErrorsBeforeCommitFactory<FirehoseRecord>()},

                // TODO: The following two senders will produce duplicate records and need the tests need to be modified to handle those
                //{new FileSender.FileSenderWithErrorsAfterPartialCommitFactory<FirehoseRecord>()},
                //{new FileSender.MisbehavingFileSenderFactory<FirehoseRecord>()},
        };
    }

    private AsyncPublisherService<FirehoseRecord> getAsyncPublisher(ISender<FirehoseRecord> sender) {
        FileCheckpointStore checkpoints = new SQLiteFileCheckpointStore(context);
        AsyncPublisherService<FirehoseRecord> publisher = new AsyncPublisherService<>(context, flow, checkpoints, sender, context.createSendingExecutor());
        return publisher;
    }

    @Test(dataProvider="senders", invocationCount=TEST_REPS, skipFailedInvocations=true, timeOut=TEST_TIMEOUT)
    public void testPublishRecordsAndStop(FileSenderFactory<FirehoseRecord> senderFactory) throws Exception {
        final int recordCount = 3 * flow.getMaxBufferSizeRecords();
        Path outputFile = testFiles.createTempFile();
        AsyncPublisherService<FirehoseRecord> publisher = getAsyncPublisher(senderFactory.create(context, outputFile));
        List<FirehoseRecord> records = new ArrayList<>();
        for(int i = 0; i < recordCount; ++i)
            records.add(getTestRecord(flow));
        publisher.startPublisher();
        for(FirehoseRecord record : records) {
            publisher.publishRecord(record);
        }
        Assert.assertTrue(publisher.isRunning());
        publisher.waitForIdle();
        Assert.assertTrue(publisher.isRunning());
        Assert.assertTrue(publisher.isIdle());
        Assert.assertEquals(publisher.queue().pendingRecords(), 0);
        Assert.assertEquals(publisher.queue().totalRecords(), 0);
        Assert.assertEquals(publisher.queue().totalBytes(), 0);
        Assert.assertEquals(publisher.queue().size(), 0);
        assertOutputFileRecordsMatchInputRecords(outputFile, records);

        publisher.stopPublisher();
        Assert.assertFalse(publisher.isRunning());
    }

    @Test(invocationCount=TEST_REPS, skipFailedInvocations=true, timeOut=TEST_TIMEOUT)
    public void testCheckpointUpdateOnSuccess() throws Exception {
        final int intermediateRecordCount = (int) (1.5 * flow.getMaxBufferSizeRecords());
        Path outputFile = testFiles.createTempFile();
        ISender<FirehoseRecord> sender = new FileSender<>(context, outputFile);
        AsyncPublisherService<FirehoseRecord> publisher = getAsyncPublisher(sender);
        publisher.startPublisher();
        initTestRecord(flow);
        TrackedFile inputFile = sourceFileForTestRecords;

        // Check that there are no checkpoints for input file
        Assert.assertNull(publisher.checkpointer().getStore().getCheckpointForPath(flow, inputFile.getPath()));

        // Publish few records and flush, then see the new checkpoint
        for(int i = 0; i < intermediateRecordCount; ++i)
            publisher.publishRecord(getTestRecord(flow));
        FirehoseRecord lastRecord = getTestRecord(flow);
        publisher.publishRecord(lastRecord);
        publisher.flush();
        publisher.waitForIdle();
        Thread.sleep(500);
        FileCheckpoint cp = publisher.checkpointer().getStore().getCheckpointForPath(flow, inputFile.getPath());
        Assert.assertNotNull(cp);
        Assert.assertEquals(cp.getOffset(), lastRecord.endOffset());

        // Now publish another record, and see the checkpoint update
        FirehoseRecord lastRecord2 = getTestRecord(flow);
        publisher.publishRecord(lastRecord2);
        publisher.flush();
        publisher.waitForIdle();
        logger.info("Last record: {}", lastRecord2);
        cp = publisher.checkpointer().getStore().getCheckpointForPath(flow, inputFile.getPath());
        Assert.assertEquals(cp.getOffset(), lastRecord2.endOffset());

        // Cleanup
        publisher.stopPublisher();
    }

    @Test(invocationCount=TEST_REPS, skipFailedInvocations=true, timeOut=TEST_TIMEOUT)
    public void testCheckpointUpdateOnSuccessMultipleFiles() throws Exception {
        final int intermediateRecordCount = (int) (1.5 * flow.getMaxBufferSizeRecords());
        Path outputFile = testFiles.createTempFile();
        ISender<FirehoseRecord> sender = new FileSender<>(context, outputFile);
        AsyncPublisherService<FirehoseRecord> publisher = getAsyncPublisher(sender);
        publisher.startPublisher();
        initTestRecord(flow);
        TrackedFile inputFile1 = sourceFileForTestRecords;
        // Check that there are no checkpoints for input file
        Assert.assertNull(publisher.checkpointer().getStore().getCheckpointForPath(flow, inputFile1.getPath()));

        // Publish few records from first file, then simulate rotation
        for(int i = 0; i < intermediateRecordCount; ++i)
            publisher.publishRecord(getTestRecord(flow));
        publisher.flush();
        publisher.waitForIdle();

        // Now, simulate rotation (open new file) and write more records
        initTestRecord(flow);
        TrackedFile inputFile2 = sourceFileForTestRecords;
        Assert.assertNull(publisher.checkpointer().getStore().getCheckpointForPath(flow, inputFile2.getPath()));
        for(int i = 0; i < intermediateRecordCount; ++i)
            publisher.publishRecord(getTestRecord(flow));

        // Now see the checkpoint update for the second file
        FirehoseRecord lastRecord = getTestRecord(flow);
        publisher.publishRecord(lastRecord);
        publisher.flush();
        publisher.waitForIdle();
        Thread.sleep(500);
        logger.info("Last record: {}", lastRecord);
        FileCheckpoint cp = publisher.checkpointer().getStore().getCheckpointForPath(flow, inputFile2.getPath());
        logCheckpoints(publisher.checkpointer().getStore());
        Assert.assertEquals(cp.getOffset(), lastRecord.endOffset());

        // Cleanup
        publisher.stopPublisher();
    }

    private void logCheckpoints(FileCheckpointStore checkpoints) {
        logger.debug(">> Checkpoints in {}:", checkpoints);
        for(Map<String, Object> cpdata : checkpoints.dumpCheckpoints())
            logger.debug(">>>> " + Joiner.on(",").withKeyValueSeparator("=").join(cpdata));
    }

    @DataProvider(name="noCheckpointUpdateOnFailuresData")
    public Object[][] testNoCheckpointUpdateOnFailuresData() {
        return new Object[][] { { 1.0, 0.0 }, { 0.0, 1.0 } };
    }

    @Test(dataProvider="noCheckpointUpdateOnFailuresData", invocationCount=TEST_REPS, skipFailedInvocations=true, timeOut=TEST_TIMEOUT)
    public void testNoCheckpointUpdateOnFailures(double partialFailureRate, double errorBeforeCommitRate) throws Exception {
        final int intermediateRecordCount = (int) (1.5 * flow.getMaxBufferSizeRecords());
        Path outputFile = testFiles.createTempFile();
        ISender<FirehoseRecord> sender = new FileSender<>(context, outputFile, FileSender.DEFAULT_AVERAGE_LATENCY_MILLIS, FileSender.DEFAULT_LATENCY_JITTER, partialFailureRate, errorBeforeCommitRate, 0.0);
        AsyncPublisherService<FirehoseRecord> publisher = getAsyncPublisher(sender);
        publisher.startPublisher();
        initTestRecord(flow);
        TrackedFile inputFile = sourceFileForTestRecords;

        // Check that there are no checkpoints for input file
        Assert.assertNull(publisher.checkpointer().getStore().getCheckpointForPath(flow, inputFile.getPath()));

        // Publish few records and flush
        for(int i = 0; i < intermediateRecordCount; ++i)
            publisher.publishRecord(getTestRecord(flow));

        publisher.flush();

        // Make sure it doesn't get idle since it will keep retrying
        Assert.assertFalse(publisher.waitForIdle(1000, TimeUnit.MILLISECONDS));

        // Stop the publisher, to kill the retries, and then check that no checkpoints were created
        publisher.stopPublisher();
        Assert.assertNull(publisher.checkpointer().getStore().getCheckpointForPath(flow, inputFile.getPath()));
    }
}
