/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;
import com.amazon.kinesis.streaming.agent.tailing.RecordBuffer;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;

public class RecordBufferTest extends TailingTestBase {
    private AgentContext context;
    private FileFlow<FirehoseRecord> flow;

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void setup() throws IOException {
        context = getTestAgentContext();
        flow = (FileFlow<FirehoseRecord>) context.flows().get(0);
    }

    private RecordBuffer<FirehoseRecord> getTestBuffer() {
        return new RecordBuffer<FirehoseRecord>(flow);
    }

    @Test
    public void testAgeStartsWithFirstRecord() throws Exception {
        RecordBuffer<FirehoseRecord> buffer = getTestBuffer();
        assertEquals(buffer.age(), 0);
        final long sleepTime = 500;
        Thread.sleep(sleepTime);
        assertEquals(buffer.age(), 0);
        // Now add one record
        long beforeAddingFirstRecord = System.currentTimeMillis();
        FirehoseRecord record1 = getTestRecord(flow);
        buffer.add(record1);
        long afterAddingFirstRecord = System.currentTimeMillis();
        Thread.sleep(sleepTime);
        // Verify age starts counting when the first record was added
        long now = System.currentTimeMillis();
        long age = buffer.age(now);
        assertTrue(age <= now - beforeAddingFirstRecord);
        assertTrue(age >= now - afterAddingFirstRecord);

        // Now add another record
        FirehoseRecord record2 = getTestRecord(flow);
        buffer.add(record2);
        Thread.sleep(sleepTime);

        // Verify that age accounting is not affected by second record
        now = System.currentTimeMillis();
        age = buffer.age();
        assertTrue(age <= now - beforeAddingFirstRecord);
        assertTrue(age >= now - afterAddingFirstRecord);
    }

    @Test
    public void testAddRecordUpdates() throws Exception {
        RecordBuffer<FirehoseRecord> buffer = getTestBuffer();
        // Initial conditions
        assertEquals(buffer.age(), 0);
        assertEquals(buffer.sizeBytes(), 0);
        assertEquals(buffer.sizeRecords(), 0);
        assertEquals(buffer.isEmpty(), true);

        // Now add one record
        FirehoseRecord record1 = getTestRecord(flow);
        buffer.add(record1);
        assertEquals(buffer.sizeBytes(), record1.lengthWithOverhead());
        assertEquals(buffer.sizeRecords(), 1);
        assertEquals(buffer.isEmpty(), false);

        // Now add another record
        FirehoseRecord record2 = getTestRecord(flow);
        buffer.add(record2);
        assertEquals(buffer.sizeBytes(),
                record1.lengthWithOverhead() +
                record2.lengthWithOverhead());
        assertEquals(buffer.sizeRecords(), 2);
        assertEquals(buffer.isEmpty(), false);
    }

    @Test
    public void testRemove() throws Exception {
        RecordBuffer<FirehoseRecord> buffer = getTestBuffer();
        // Now add few records
        FirehoseRecord record0 = getTestRecord(flow);
        buffer.add(record0);
        FirehoseRecord record1 = getTestRecord(flow);
        buffer.add(record1);
        FirehoseRecord record2 = getTestRecord(flow);
        buffer.add(record2);
        FirehoseRecord record3 = getTestRecord(flow);
        buffer.add(record3);

        Assert.assertEquals(buffer.sizeRecords(), 4);
        TrackedFile originalCheckpointFile = buffer.checkpointFile();
        long originalCheckpointOffset = buffer.checkpointOffset();
        long originalId = buffer.id();

        // Now remove the second and fourth records
        buffer = buffer.remove(1, 3);
        Assert.assertEquals(buffer.sizeRecords(), 2);
        Assert.assertEquals(buffer.sizeBytes(), record0.lengthWithOverhead() + record2.lengthWithOverhead());
        Assert.assertEquals(buffer.id(), originalId);
        Assert.assertSame(buffer.checkpointFile(), originalCheckpointFile);
        Assert.assertEquals(buffer.checkpointOffset(), originalCheckpointOffset);

        // Check that iterator retrieves first and third records only
        Iterator<FirehoseRecord> it = buffer.iterator();
        Assert.assertSame(it.next(), record0);
        Assert.assertSame(it.next(), record2);
        Assert.assertFalse(it.hasNext());
    }
}
