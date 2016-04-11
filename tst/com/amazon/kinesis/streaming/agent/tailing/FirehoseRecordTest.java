/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.lang3.RandomUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseConstants;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseFileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;

public class FirehoseRecordTest extends TestBase {

    @Test
    public void testStartEndOffset() {
        FirehoseRecord record = new FirehoseRecord(null, 1023, new byte[100]);
        Assert.assertEquals(record.startOffset(), 1023);
        Assert.assertEquals(record.endOffset(), 1123);
    }

    @Test
    public void testRecordLength() {
        FirehoseRecord record = new FirehoseRecord(null, 1023, new byte[200]);
        Assert.assertEquals(record.lengthWithOverhead(), 200 + FirehoseConstants.PER_RECORD_OVERHEAD_BYTES);
        Assert.assertEquals(record.dataLength(), 200);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testTruncate() throws IOException {
        byte[] data = RandomUtils.nextBytes((FirehoseConstants.MAX_RECORD_SIZE_BYTES) + RandomUtils.nextInt(1, 100));
        FileFlow flow = Mockito.mock(FirehoseFileFlow.class);
        when(flow.getRecordTerminatorBytes()).thenReturn(FirehoseFileFlow.DEFAULT_TRUNCATED_RECORD_TERMINATOR.getBytes(StandardCharsets.UTF_8));
        TrackedFile file = Mockito.mock(TrackedFile.class);
        when(file.getFlow()).thenReturn(flow);
        FirehoseRecord record = new FirehoseRecord(file, 1023, data);
        record.truncate();
        Assert.assertEquals(record.lengthWithOverhead(), FirehoseConstants.MAX_RECORD_SIZE_BYTES + FirehoseConstants.PER_RECORD_OVERHEAD_BYTES);
        Assert.assertEquals(record.length(), FirehoseConstants.MAX_RECORD_SIZE_BYTES);
        Assert.assertTrue(ByteBuffers.toString(record.data, StandardCharsets.UTF_8).endsWith(FirehoseFileFlow.DEFAULT_TRUNCATED_RECORD_TERMINATOR));
    }
}
