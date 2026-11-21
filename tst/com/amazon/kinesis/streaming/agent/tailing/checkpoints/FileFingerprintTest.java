/*
 * Copyright (c) 2014-2017 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.checkpoints;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.tailing.FileId;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;
import com.google.common.base.Strings;

public class FileFingerprintTest extends TailingTestBase {
    private static final int SIZE = FileFingerprint.DEFAULT_SIZE_BYTES;

    private Path writeLines(String prefix, int count) throws IOException {
        Path file = testFiles.createTempFile();
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= count; i++) {
            sb.append("{\"file\":\"").append(prefix).append("\",\"n\":").append(i).append("}\n");
        }
        Files.write(file, sb.toString().getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private Path writeContent(String content) throws IOException {
        Path file = testFiles.createTempFile();
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private static FileFingerprint compute(Path file, long offset) throws IOException {
        return FileFingerprint.compute(file, FileId.get(file), offset, SIZE);
    }

    @Test
    public void testSameFileMatches() throws IOException {
        Path file = writeLines("A", 200);
        long offset = Files.size(file) - 100;
        FileFingerprint fp = compute(file, offset);
        Assert.assertNotNull(fp);
        Assert.assertEquals(fp.getOffset(), offset);
        Assert.assertEquals(fp.getHeadLength(), SIZE);
        Assert.assertEquals(fp.getTailLength(), SIZE);
        Assert.assertTrue(fp.matches(file));
        // Same fingerprint every time
        Assert.assertEquals(compute(file, offset), fp);
    }

    @Test
    public void testFileThatKeepsGrowingStillMatches() throws IOException {
        Path file = writeLines("A", 200);
        FileFingerprint fp = compute(file, Files.size(file));
        Files.write(file, "{\"more\":1}\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        Assert.assertTrue(fp.matches(file));
    }

    @Test
    public void testDifferentFileDoesNotMatch() throws IOException {
        Path a = writeLines("A", 200);
        Path b = writeLines("B", 300);
        FileFingerprint fp = compute(a, Files.size(a));
        Assert.assertTrue(Files.size(b) >= fp.getOffset());
        Assert.assertFalse(fp.matches(b));
    }

    @Test
    public void testSameHeadDifferentTailDoesNotMatch() throws IOException {
        // Both files start with the same 2 KB header, longer than the head hash covers
        String header = Strings.repeat("h", 2 * SIZE) + "\n";
        Path a = writeContent(header + Strings.repeat("{\"file\":\"A\"}\n", 200));
        Path b = writeContent(header + Strings.repeat("{\"file\":\"B\"}\n", 300));
        FileFingerprint fpA = compute(a, Files.size(a));
        FileFingerprint fpB = compute(b, Files.size(a));
        Assert.assertEquals(fpB.getHeadHash(), fpA.getHeadHash(), "heads should be the same");
        Assert.assertNotEquals(fpB.getTailHash(), fpA.getTailHash(), "tails should differ");
        Assert.assertFalse(fpA.matches(b));
    }

    @Test
    public void testFileShorterThanOffset() throws IOException {
        Path a = writeLines("A", 200);
        Path b = writeLines("B", 10);
        FileFingerprint fp = compute(a, Files.size(a));
        Assert.assertTrue(Files.size(b) < fp.getOffset());
        Assert.assertFalse(fp.matches(b));
        // Nothing to fingerprint past the end of the file
        Assert.assertNull(compute(b, Files.size(b) + 1));
    }

    @Test
    public void testOffsetSmallerThanSizeHashesOnlySentBytes() throws IOException {
        Path file = writeLines("A", 200);
        FileFingerprint fp = compute(file, 100);
        Assert.assertEquals(fp.getHeadLength(), 100);
        Assert.assertEquals(fp.getTailLength(), 100);
        Assert.assertEquals(fp.getHeadHash(), fp.getTailHash(), "both cover bytes [0, 100)");
        Assert.assertTrue(fp.matches(file));
    }

    @Test
    public void testEmptyFile() throws IOException {
        Path empty = writeContent("");
        Path a = writeLines("A", 200);
        Assert.assertNull(compute(empty, 1));
        Assert.assertFalse(compute(a, Files.size(a)).matches(empty));
    }

    @Test
    public void testZeroOffsetHasNoFingerprint() throws IOException {
        Path file = writeLines("A", 10);
        Assert.assertNull(compute(file, 0));
    }

    @Test
    public void testFileIdMismatchHasNoFingerprint() throws IOException {
        Path a = writeLines("A", 200);
        Path b = writeLines("B", 200);
        Assert.assertNull(FileFingerprint.compute(a, FileId.get(b), 100, SIZE));
    }

    @Test
    public void testMissingFileComputeReturnsNull() throws IOException {
        Path a = writeLines("A", 200);
        FileId id = FileId.get(a);
        Files.delete(a);
        // compute() stays non-fatal on the write side: no fingerprint is saved
        Assert.assertNull(FileFingerprint.compute(a, id, 100, SIZE));
    }

    @Test(expectedExceptions = IOException.class)
    public void testMatchesThrowsWhenFileUnreadable() throws IOException {
        Path a = writeLines("A", 200);
        FileFingerprint fp = compute(a, Files.size(a));
        Files.delete(a);
        // An unreadable file must NOT look like a content mismatch; it propagates so the caller can retry.
        fp.matches(a);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidLengthsRejected() {
        new FileFingerprint(10, 11, "abc", 10, "def");
    }
}
