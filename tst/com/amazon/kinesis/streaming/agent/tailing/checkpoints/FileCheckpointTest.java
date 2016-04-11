/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.checkpoints;

import java.io.IOException;
import java.nio.file.Path;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpoint;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;


public class FileCheckpointTest extends TailingTestBase {
    private AgentContext context;
    private FileFlow<FirehoseRecord> flow;

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void setup() throws IOException {
        context = getTestAgentContext();
        flow = (FileFlow<FirehoseRecord>) context.flows().get(0);
    }

    @Test
    public void testHappyConstructor() throws IOException {
        // all the following should work just fine
        Path tmp1 = testFiles.createTempFile();
        TrackedFile file1 = new TrackedFile(flow, tmp1);
        FileCheckpoint cp1 = new FileCheckpoint(file1, 0);
        Assert.assertEquals(cp1.getOffset(), 0L);
        Assert.assertEquals(cp1.getFile(), file1);
        Path tmp2 = testFiles.createTempFile();
        TrackedFile file2 = new TrackedFile(flow, tmp2);
        FileCheckpoint cp2 = new FileCheckpoint(file2, 1000000000000L);
        Assert.assertEquals(cp2.getOffset(), 1000000000000L);
    }

    @Test(expectedExceptions=IllegalArgumentException.class, expectedExceptionsMessageRegExp="The offset .* must be a non-negative integer")
    public void testNegativePosition() throws IOException {
        Path tmp1 = testFiles.createTempFile();
        TrackedFile file1 = new TrackedFile(flow, tmp1);
        new FileCheckpoint(file1, -1);
    }

    @Test(expectedExceptions=NullPointerException.class)
    public void testNullFileId() throws IOException {
        new FileCheckpoint(null, 0);
    }
}
