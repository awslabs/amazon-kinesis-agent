/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.checkpoints;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;
import com.amazon.kinesis.streaming.agent.tailing.RecordBuffer;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.Checkpointer;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpointStore;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.SQLiteFileCheckpointStore;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;

public class CheckpointerTest extends TailingTestBase {
    private AgentContext context;
    private FileFlow<FirehoseRecord> flow;

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void setup() throws IOException {
        context = getTestAgentContext();
        flow = (FileFlow<FirehoseRecord>) context.flows().get(0);
    }

    @Test
    public void testAlwaysCheckpointFirstBuffer() {
        FileCheckpointStore store = new SQLiteFileCheckpointStore(context);
        Checkpointer<FirehoseRecord> checkpointer = new Checkpointer<FirehoseRecord>(flow, store);
        RecordBuffer<FirehoseRecord> buffer = new RecordBuffer<FirehoseRecord>(flow);

        buffer.add(getTestRecord(flow));
        buffer.add(getTestRecord(flow));
        buffer.add(getTestRecord(flow));
        assertNotNull(checkpointer.saveCheckpoint(buffer));
    }

    @Test
    public void testCheckpointBufferWithIncreasingSequenceNumber() {
        FileCheckpointStore store = new SQLiteFileCheckpointStore(context);
        Checkpointer<FirehoseRecord> checkpointer = new Checkpointer<FirehoseRecord>(flow, store);
        RecordBuffer<FirehoseRecord> buffer1 = new RecordBuffer<FirehoseRecord>(flow);

        // First buffer is always saved
        buffer1.add(getTestRecord(flow));
        buffer1.add(getTestRecord(flow));
        buffer1.add(getTestRecord(flow));
        assertNotNull(checkpointer.saveCheckpoint(buffer1));

        // Create second buffer, and make sure it'll be saved because it has higher sequence number
        RecordBuffer<FirehoseRecord> buffer2 = new RecordBuffer<FirehoseRecord>(flow);
        buffer2.add(getTestRecord(flow));
        buffer2.add(getTestRecord(flow));
        assertTrue(buffer2.id() > buffer1.id());
        assertNotNull(checkpointer.saveCheckpoint(buffer2));
    }

    @Test
    public void testCheckpointBufferWithDecreasingSequenceNumber() {
        FileCheckpointStore store = new SQLiteFileCheckpointStore(context);
        Checkpointer<FirehoseRecord> checkpointer = new Checkpointer<FirehoseRecord>(flow, store);
        RecordBuffer<FirehoseRecord> buffer1 = new RecordBuffer<FirehoseRecord>(flow);

        // Create first buffer but don't commit it
        buffer1.add(getTestRecord(flow));
        buffer1.add(getTestRecord(flow));
        buffer1.add(getTestRecord(flow));

        // Create second buffer and commit it
        RecordBuffer<FirehoseRecord> buffer2 = new RecordBuffer<FirehoseRecord>(flow);
        buffer2.add(getTestRecord(flow));
        buffer2.add(getTestRecord(flow));
        assertNotNull(checkpointer.saveCheckpoint(buffer2));

        // Now try to commit first buffer, which has lower sequence number
        assertTrue(buffer2.id() > buffer1.id());
        assertNull(checkpointer.saveCheckpoint(buffer1));
    }
}
