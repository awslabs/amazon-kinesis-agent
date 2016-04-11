/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.ByteBufferInputStream;
import com.amazon.kinesis.streaming.agent.ByteBuffers;

public class ByteBufferInputStreamTest {
    ByteBuffer buffer;
    InputStream unit;

    @BeforeMethod
    public void setup() {
        buffer = ByteBuffers.fromString("123456", StandardCharsets.UTF_8);
        buffer.position(1);
        unit = new ByteBufferInputStream(buffer);
    }

    /**
     * Make sure that sign conversion is done properly when converting from
     * bytes to ints.
     */
    @Test
    public void testAllBytes() throws IOException {
        byte[] bytes = new byte[256];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        assertEquals(bytes[0], 0);
        assertEquals(bytes[127], Byte.MAX_VALUE);
        assertEquals(bytes[128], Byte.MIN_VALUE);
        assertEquals(bytes[255], -1);
        buffer = ByteBuffer.wrap(bytes).asReadOnlyBuffer();

        unit = new ByteBufferInputStream(buffer);
        for (int i = 0; i < 256; i++) {
            assertEquals(unit.read(), i);
        }
        assertEquals(unit.read(), -1);
    }

    @Test
    public void testSkip() throws IOException {
        unit.skip(-1); // no effect
        assertEquals(unit.available(), 5);
        unit.skip(3);
        assertEquals(unit.available(), 2);
        unit.skip(3);
        assertEquals(unit.available(), 0);
        unit.skip(3);
        assertEquals(unit.available(), 0);
    }

    @Test
    public void testMarkAndReset() throws IOException {
        assertTrue(unit.markSupported());
        assertEquals(unit.read(), '2');
        unit.mark(10);
        assertEquals(unit.read(), '3');
        assertEquals(unit.read(), '4');
        unit.reset();
        assertEquals(unit.read(), '3');
    }

    @Test
    public void testMarkWithoutReset() throws IOException {
        try {
            unit.reset();
            fail("expected IOException");
        } catch (IOException e) {
            // expected
        }
    }

    @Test
    public void testCloseDoesNothing() throws IOException {
        unit.close();
        assertEquals(unit.read(), '2');
    }

    @Test
    public void testReadAdvancesBufferPosition() throws IOException {
        unit.read(new byte[3]);
        assertEquals(buffer.position(), 4);
    }
}
