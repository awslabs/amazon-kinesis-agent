/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;
import com.amazon.kinesis.streaming.agent.tailing.ISender;
import com.amazon.kinesis.streaming.agent.tailing.SimplePublisher;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpointStore;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.SQLiteFileCheckpointStore;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileSender;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileSender.FileSenderFactory;


public class SimplePublisherTest extends TailingTestBase {
    private AgentContext context;
    private FileFlow<FirehoseRecord> flow;

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void setup() throws IOException {
        context = getTestAgentContext();
        flow = (FileFlow<FirehoseRecord>) context.flows().get(0);
    }

    private SimplePublisher<FirehoseRecord> getSimplePublisher(ISender<FirehoseRecord> sender) {
        FileCheckpointStore checkpoints = new SQLiteFileCheckpointStore(context);
        SimplePublisher<FirehoseRecord> publisher = new SimplePublisher<>(context, flow, checkpoints, sender);
        return publisher;
    }

    @Test
    public void testPublishRecordFailsAfterClose() {
        FileSenderFactory<FirehoseRecord> fsFactory = new FileSender.FileSenderWithPartialFailuresAndErrorsBeforeCommitFactory<FirehoseRecord>();
        SimplePublisher<FirehoseRecord> publisher = getSimplePublisher(fsFactory.create(context, testFiles.createTempFile()));
        assertTrue(publisher.publishRecord(getTestRecord(flow)));
        publisher.close();
        assertFalse(publisher.publishRecord(getTestRecord(flow)));
    }

    @Test(enabled=false)
    public void testPollNextRecordReturnsImmediatelyAfterClose() {
        // TODO
    }
}
