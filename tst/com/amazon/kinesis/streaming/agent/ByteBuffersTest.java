/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;

public class ByteBuffersTest {

    final String testString = "foo";
    final byte[] testBytes = testString.getBytes();
    ByteBuffer testBuffer;

    @BeforeMethod
    public void initBuffer() {
        // Create a buffer that has non-zero arrayOffset and position
        // as well as having non-maximal capacity and limit
        testBuffer = ByteBuffer.wrap("barfoobar".getBytes(), 2, 5).slice();
        testBuffer.position(1).limit(4);
    }

    @DataProvider
    public Object[][] hasArrayOrNot() {
        initBuffer();
        return new Object[][] {
                {testBuffer.duplicate()},
                {testBuffer.asReadOnlyBuffer()}, // hasArray will return false
        };
    }

    @Test
    public void testVisibleWhiteSpaces() {
        assertNull(ByteBuffers.visibleWhiteSpaces(null));
        assertEquals(ByteBuffers.visibleWhiteSpaces(""), "");
        assertEquals(ByteBuffers.visibleWhiteSpaces(" "), " ");
        assertEquals(ByteBuffers.visibleWhiteSpaces("\n"), "\\n\n");
        assertEquals(ByteBuffers.visibleWhiteSpaces(" \t\n\r"), " \\t\\n\n\\r");
        assertEquals(ByteBuffers.visibleWhiteSpaces(" \t\n\r"), " \\t\\n\n\\r");
        assertEquals(ByteBuffers.visibleWhiteSpaces("a b c d"), "a b c d");
        assertEquals(ByteBuffers.visibleWhiteSpaces("a\tb c d"), "a\\tb c d");
        assertEquals(ByteBuffers.visibleWhiteSpaces("a\tb\nc\rd"), "a\\tb\\n\nc\\rd");
    }

    @Test(dataProvider = "hasArrayOrNot")
    public void testToString(ByteBuffer buffer) {
        assertEquals(ByteBuffers.toString(buffer, StandardCharsets.UTF_8), testString);
        assertEquals(buffer.remaining(), 3);
    }

    @Test(dataProvider = "hasArrayOrNot")
    public void testToStringSizeGreaterThanRemaining(ByteBuffer buffer) {
        assertEquals(ByteBuffers.toString(buffer, 20, StandardCharsets.UTF_8), testString);
        assertEquals(buffer.remaining(), 3);
    }

    @Test(dataProvider = "hasArrayOrNot")
    public void testToStringSizeLessThanRemaining(ByteBuffer buffer) {
        assertEquals(ByteBuffers.toString(buffer, 2, StandardCharsets.UTF_8), "fo");
        assertEquals(buffer.remaining(), 3);
    }

    @Test(dataProvider = "hasArrayOrNot")
    public void testToArray(ByteBuffer buffer) {
        assertEquals(ByteBuffers.toArray(buffer), testBytes);
        assertEquals(buffer.remaining(), 3);
    }

    @Test
    public void testFromString() {
        assertEquals(ByteBuffers.fromString(testString, StandardCharsets.UTF_8), testBuffer);
    }

    @Test(dataProvider = "hasArrayOrNot")
    public void testCopy(ByteBuffer buffer) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffers.copy(buffer, out);
        assertEquals(out.toByteArray(), testBytes);
        assertEquals(buffer.remaining(), 3);
    }

    @Test(dataProvider = "hasArrayOrNot")
    public void testAsInputStream(ByteBuffer buffer) throws IOException {
        InputStream in = ByteBuffers.asInputStream(buffer);
        assertEquals(ByteStreams.toByteArray(in), testBytes);
        assertEquals(buffer.remaining(), 3);
    }

    @Test(expectedExceptions = EOFException.class)
    public void testReadFullyEOFException() throws IOException {
        FileChannel mockChannel = mock(FileChannel.class);
        when(mockChannel.read(any(ByteBuffer.class), anyInt())).thenReturn(-1);
        ByteBuffers.readFully(mockChannel, 6, 8);
    }

    @Test
    public void testReadFully() throws IOException {
        //                       6   8
        //                       |<---->|
        String contents = "stack overflow works best with javascript enabled";
        File to = File.createTempFile("temp", "test");
        Files.write(contents, to, StandardCharsets.UTF_8);

        ByteBuffer readBuffer;
        try (FileChannel fc = FileChannel.open(to.toPath(), StandardOpenOption.READ)) {
            readBuffer = ByteBuffers.readFully(fc, 6, 8);
        }
        to.delete();

        assertEquals(ByteBuffers.toString(readBuffer, StandardCharsets.UTF_8), "overflow");
    }

    @Test
    public void testGetPartialView() {
        String originalStr = RandomStringUtils.randomAscii(100);
        ByteBuffer original = ByteBuffers.fromString(originalStr, StandardCharsets.UTF_8);
        // Set a non-zero position
        final int originalPosition = 4;
        original.position(originalPosition);
        final int originalLimit = original.limit();
        final int originalCapacity = original.capacity();
        // Get a view and assert it has expected attributes
        ByteBuffer view = ByteBuffers.getPartialView(original, 10, 25);
        assertEquals(view.position(), 0);
        assertEquals(view.limit(), 25);
        assertEquals(ByteBuffers.toString(view, StandardCharsets.UTF_8), originalStr.substring(10, 35));

        // Assert the original is intact
        assertEquals(original.position(), originalPosition);
        assertEquals(original.limit(), originalLimit);
        assertEquals(original.capacity(), originalCapacity);
        original.position(0);
        assertEquals(ByteBuffers.toString(original, StandardCharsets.UTF_8), originalStr);
    }
}
