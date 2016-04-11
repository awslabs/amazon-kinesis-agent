/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.testing;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;

/**
 * Unit tests for {@link TestUtils}.
 */
public class TestUtilsTest extends TestBase {
    @DataProvider(name = "randomCase")
    public Object[][] testRandomCaseData() {
        return new Object[][] { { "abc defg12345", "abc defg12345" },
                { "12345", "12345" }, { "+12345.67", "+12345.67" },
                { "!AnyText!+123.,~", "!AnyText!+123.,~" }, { "", "" },
                { "  ", "  " } };
    }

    @Test(dataProvider = "randomCase")
    public void testRandomCase(String input, String expected) {
        Assert.assertNull(TestUtils.randomCase(null));
        Assert.assertTrue(TestUtils.randomCase(input)
                .equalsIgnoreCase(expected));
        // System.out.println(TestUtils.randomCase(input));
    }

    @Test
    public void testRandomCaseWithNull() {
        Assert.assertNull(TestUtils.randomCase(null));
    }

    @Test
    public void testEnsureIncreasingLastModifiedDate2Files() throws IOException {
        Path f1 = testFiles.createTempFile();
        Path f2 = testFiles.createTempFile();
        TestUtils.ensureIncreasingLastModifiedTime(f1, f2);
        Assert.assertTrue(Files.getLastModifiedTime(f1).toMillis() < Files.getLastModifiedTime(f2).toMillis());
        // now reverse the order
        TestUtils.ensureIncreasingLastModifiedTime(f2, f1);
        Assert.assertTrue(Files.getLastModifiedTime(f1).toMillis() > Files.getLastModifiedTime(f2).toMillis());
    }

    @Test
    public void testEnsureIncreasingLastModifiedDate() throws IOException {
        Path f1 = testFiles.createTempFile();
        Path f2 = testFiles.createTempFile();
        Path f3 = testFiles.createTempFile();
        Path f4 = testFiles.createTempFile();
        TestUtils.ensureIncreasingLastModifiedTime(f1, f2, f3, f4);
        Assert.assertTrue(Files.getLastModifiedTime(f1).toMillis() < Files.getLastModifiedTime(f2).toMillis());
        Assert.assertTrue(Files.getLastModifiedTime(f2).toMillis() < Files.getLastModifiedTime(f3).toMillis());
        Assert.assertTrue(Files.getLastModifiedTime(f3).toMillis() < Files.getLastModifiedTime(f4).toMillis());
    }

    @Test
    public void testCrossParamsEmptyInput() {
        // both empty/null
        Object[][] input1 = new Object[0][];
        Object[][] input2 = new Object[0][];
        Assert.assertEquals(TestUtils.crossTestParameters(input1, input2).length, 0);
        Assert.assertEquals(TestUtils.crossTestParameters(input2, input1).length, 0);
        Assert.assertEquals(TestUtils.crossTestParameters(null, input2).length, 0);
        Assert.assertEquals(TestUtils.crossTestParameters(input2, null).length, 0);
        Assert.assertEquals(TestUtils.crossTestParameters(null, null).length, 0);
        Assert.assertEquals(TestUtils.crossTestParameters(null, null).length, 0);
        // one of them is empty/null
        Object[][] input3 = new Object[][] {{1,2}, {3,4}};
        Assert.assertEquals(TestUtils.crossTestParameters(input1, input3).length, input3.length);
        Assert.assertEquals(TestUtils.crossTestParameters(input3, input1).length, input3.length);
        Assert.assertEquals(TestUtils.crossTestParameters(null, input3).length, input3.length);
        Assert.assertEquals(TestUtils.crossTestParameters(input3, null).length, input3.length);
        Assert.assertEquals(TestUtils.crossTestParameters(input3, input1)[0], new Object[] {1,2});
        Assert.assertEquals(TestUtils.crossTestParameters(input3, input1)[1], new Object[] {3,4});
        Assert.assertEquals(TestUtils.crossTestParameters(input3, null)[0], new Object[] {1,2});
        Assert.assertEquals(TestUtils.crossTestParameters(input3, null)[1], new Object[] {3,4});
    }

    @Test
    public void testCrossParams() {
        // singletons
        Object[][] input1 = new Object[][] {{1}, {2}};
        Object[][] input2 = new Object[][] {{3}, {4}, {5}};
        Assert.assertEquals(TestUtils.crossTestParameters(input1, input2).length, 6);
        Assert.assertEquals(TestUtils.crossTestParameters(input1, input2)[0], new Object[]{1,3});
        Assert.assertEquals(TestUtils.crossTestParameters(input1, input2)[1], new Object[]{1,4});
        Assert.assertEquals(TestUtils.crossTestParameters(input1, input2)[2], new Object[]{1,5});
        Assert.assertEquals(TestUtils.crossTestParameters(input1, input2)[3], new Object[]{2,3});
        Assert.assertEquals(TestUtils.crossTestParameters(input1, input2)[4], new Object[]{2,4});
        Assert.assertEquals(TestUtils.crossTestParameters(input1, input2)[5], new Object[]{2,5});

        // multiple sizes
        Object[][] input3 = new Object[][] {{1,2,3}, {4,5,6}};
        Object[][] input4 = new Object[][] {{7,8}};
        Assert.assertEquals(TestUtils.crossTestParameters(input3, input4).length, 2);
        Assert.assertEquals(TestUtils.crossTestParameters(input3, input4)[0], new Object[]{1,2,3,7,8});
        Assert.assertEquals(TestUtils.crossTestParameters(input3, input4)[1], new Object[]{4,5,6,7,8});
    }

    @Test
    public void testCrossParamsMultiple() {
        Object[][] input1 = TestUtils.toTestParameters(1);
        Object[][] input2 = TestUtils.toTestParameters(3,4);
        Object[][] input3 = TestUtils.toTestParameters(new Object[]{5,6}, new Object[]{7,8}, new Object[]{9,10});
        Object[][] cross = TestUtils.crossTestParameters(input1, input2, input3);
        Assert.assertEquals(cross.length, 6);
        Assert.assertEquals(cross[0], new Object[] {1,3,5,6});
        Assert.assertEquals(cross[1], new Object[] {1,3,7,8});
        Assert.assertEquals(cross[2], new Object[] {1,3,9,10});
        Assert.assertEquals(cross[3], new Object[] {1,4,5,6});
        Assert.assertEquals(cross[4], new Object[] {1,4,7,8});
        Assert.assertEquals(cross[5], new Object[] {1,4,9,10});
    }

    @Test
    public void testConcatParams() {
        Object[][] input1 = new Object[][] {{1, 11}, {2, 22}, {3, 33}};
        Object[][] input2 = new Object[][] {{4, 44}, {5, 55}, {6, 66}};
        Assert.assertEquals(TestUtils.concatTestParameters(input1, input2).length, 6);
        Assert.assertEquals(TestUtils.concatTestParameters(input1, input2)[0], new Object[]{1, 11});
        Assert.assertEquals(TestUtils.concatTestParameters(input1, input2)[1], new Object[]{2, 22});
        Assert.assertEquals(TestUtils.concatTestParameters(input1, input2)[2], new Object[]{3, 33});
        Assert.assertEquals(TestUtils.concatTestParameters(input1, input2)[3], new Object[]{4, 44});
        Assert.assertEquals(TestUtils.concatTestParameters(input1, input2)[4], new Object[]{5, 55});
        Assert.assertEquals(TestUtils.concatTestParameters(input1, input2)[5], new Object[]{6, 66});
    }

    @Test
    public void testGetMD5() {
        String input = "The quick brown fox jumps over the lazy dog";
        String expectedMD5 = "9e107d9d372bb6826bd81d3542a419d6";
        ByteBuffer inputBuffer = ByteBuffers.fromString(input, StandardCharsets.UTF_8);
        ReadableByteChannel inputChannel = Channels.newChannel(ByteBuffers.asInputStream(inputBuffer));
        Assert.assertEquals(TestUtils.getMD5(inputChannel, inputBuffer.limit()), expectedMD5);
    }

    @Test
    public void testGetMD5Partial() {
        String input = "The quick brown fox jumps over the lazy dog";
        String realInput = "The quick brown fox";
        String expectedMD5 = "a2004f37730b9445670a738fa0fc9ee5";
        ByteBuffer inputBuffer = ByteBuffers.fromString(input, StandardCharsets.UTF_8);
        ReadableByteChannel inputChannel = Channels.newChannel(ByteBuffers.asInputStream(inputBuffer));
        Assert.assertEquals(TestUtils.getMD5(inputChannel, realInput.length()), expectedMD5);
    }

    @Test
    public void testGetContentMD5LargeFile() throws NoSuchAlgorithmException, IOException {
        final int size = 10*1024*1024;
        String input = RandomStringUtils.randomAscii(size);
        MessageDigest digest = MessageDigest.getInstance("MD5");
        digest.update(ByteBuffers.fromString(input, StandardCharsets.UTF_8));
        String expectedMD5 = new BigInteger(1, digest.digest()).toString(16);
        Path tmp = testFiles.createTempFile();
        testFiles.appendToFile(input, tmp);
        Assert.assertEquals(TestUtils.getContentMD5(tmp, size), expectedMD5);
    }

    @Test
    public void testGetContentMD5LargeFilePartial() throws NoSuchAlgorithmException, IOException {
        final int size = 10*1024*1024;
        final int md5Size = size / 2;
        String input = RandomStringUtils.randomAscii(size);
        MessageDigest digest = MessageDigest.getInstance("MD5");
        digest.update(ByteBuffers.fromString(input.substring(0, md5Size), StandardCharsets.UTF_8));
        String expectedMD5 = new BigInteger(1, digest.digest()).toString(16);
        Path tmp = testFiles.createTempFile();
        testFiles.appendToFile(input, tmp);
        Assert.assertEquals(TestUtils.getContentMD5(tmp, md5Size), expectedMD5);
    }

    @Test
    public void testGetContentRetunsNullIfFileNotFound() throws NoSuchAlgorithmException, IOException {
        Assert.assertNull(TestUtils.getContentMD5(testFiles.getTempFilePath(), 1));
    }
}
