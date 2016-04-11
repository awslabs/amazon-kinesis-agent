/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;

public class TrackedFileTest extends TestBase {
    @Test
    public void testOffsetAndRemainingChangeAfterReading() throws IOException {
        Path tmp = testFiles.createTempFile();
        final String testLine1 = "test1\n";
        final String testLine2 = "test22\n";
        final long expectedSize = testLine1.length() + testLine2.length();
        testFiles.appendToFile(testLine1, tmp);
        testFiles.appendToFile(testLine2, tmp);
        Assert.assertEquals(Files.size(tmp), expectedSize);    // sanity check
        TrackedFile uf = new TrackedFile(null, tmp);
        uf.open(0);
        Assert.assertNotNull(uf.getChannel());
        Assert.assertEquals(uf.getCurrentOffset(), 0);
        Assert.assertEquals(uf.getSize(), expectedSize);
        Assert.assertEquals(uf.getRemainingBytes(), expectedSize);
        ByteBuffer buff = ByteBuffer.allocate(testLine1.length());
        uf.getChannel().read(buff);
        buff.flip();
        Assert.assertEquals(ByteBuffers.toString(buff, StandardCharsets.UTF_8), testLine1);
        Assert.assertEquals(uf.getCurrentOffset(), testLine1.length());
        Assert.assertEquals(uf.getRemainingBytes(), testLine2.length());
    }

    @Test
    public void testOpenWithNonZeroOffset() throws IOException {
        Path tmp = testFiles.createTempFile();
        final String testLine1 = "test1\n";
        final String testLine2 = "test22\n";
        final long expectedSize = testLine1.length() + testLine2.length();
        testFiles.appendToFile(testLine1, tmp);
        testFiles.appendToFile(testLine2, tmp);
        Assert.assertEquals(Files.size(tmp), expectedSize);    // sanity check
        TrackedFile uf = new TrackedFile(null, tmp);
        uf.open(testLine1.length());
        Assert.assertNotNull(uf.getChannel());
        Assert.assertEquals(uf.getCurrentOffset(), testLine1.length());
        Assert.assertEquals(uf.getSize(), expectedSize);
        Assert.assertEquals(uf.getRemainingBytes(), testLine2.length());
        ByteBuffer buff = ByteBuffer.allocate(testLine2.length());
        uf.getChannel().read(buff);
        buff.flip();
        Assert.assertEquals(ByteBuffers.toString(buff, StandardCharsets.UTF_8), testLine2);
        Assert.assertEquals(uf.getCurrentOffset(), expectedSize);
        Assert.assertEquals(uf.getRemainingBytes(), 0);
    }

    @Test
    public void testFileAppendAfterOpenDoesntChangeSize() throws IOException {
        Path tmp = testFiles.createTempFile();
        final String testLine1 = "test1\n";
        final String testLine2 = "test22\n";
        testFiles.appendToFile(testLine1, tmp);
        Assert.assertEquals(Files.size(tmp), testLine1.length());    // sanity check
        TrackedFile uf = new TrackedFile(null, tmp);
        uf.open(0);
        Assert.assertNotNull(uf.getChannel());
        Assert.assertEquals(uf.getCurrentOffset(), 0);
        Assert.assertEquals(uf.getSize(), testLine1.length());
        Assert.assertEquals(uf.getRemainingBytes(), testLine1.length());
        // now read the first line, assert new values
        ByteBuffer buff = ByteBuffer.allocate(testLine1.length());
        uf.getChannel().read(buff);
        buff.flip();
        Assert.assertEquals(ByteBuffers.toString(buff, StandardCharsets.UTF_8), testLine1);
        Assert.assertEquals(uf.getCurrentOffset(), testLine1.length());
        Assert.assertEquals(uf.getRemainingBytes(), 0);
        // now append one more line, and assert values remain the same
        testFiles.appendToFile(testLine2, tmp);
        Assert.assertEquals(uf.getCurrentOffset(), testLine1.length());
        Assert.assertEquals(uf.getSize(), testLine1.length());
        Assert.assertEquals(uf.getRemainingBytes(), 0);
    }

    @Test(expectedExceptions=IllegalStateException.class, expectedExceptionsMessageRegExp="File ID differ.*")
    public void testInheritChannelFailsIfFileIdAreDifferent() throws IOException {
        Path tmp1 = testFiles.createTempFile();
        TrackedFile uf1 = new TrackedFile(null, tmp1);
        uf1.open(0);
        Path tmp2 = testFiles.createTempFile();
        TrackedFile uf2 = new TrackedFile(null, tmp2);
        uf2.inheritChannel(uf1);
    }

    @Test(expectedExceptions=IllegalStateException.class, expectedExceptionsMessageRegExp=".*has an open channel already.*")
    public void testInheritChannelFailsIfFileHasAnOpenChannel() throws IOException {
        Path tmp1 = testFiles.createTempFile();
        TrackedFile uf1 = new TrackedFile(null, tmp1);
        uf1.open(0);
        Path tmp2 = testFiles.createTempFile();
        TrackedFile uf2 = new TrackedFile(null, tmp2);
        uf2.open(0);
        uf2.inheritChannel(uf1);
    }
}
