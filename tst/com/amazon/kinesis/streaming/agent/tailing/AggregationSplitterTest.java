/*
 * Copyright (c) 2014-2017 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class AggregationSplitterTest extends TestBase {
    private ISplitter splitter;
    private ByteBuffer buffer;

    @BeforeMethod
    public void setup() {
        splitter = new AggregationSplitter(10);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConstructorGivenInvalidRecordSizeHint() {
        new AggregationSplitter(0);
    }

    @Test
    public void testConstructorGivenValidRecordSizeHint() {
        final int recordSizeHint = 5 * 1024;
        final AggregationSplitter splitter = new AggregationSplitter(recordSizeHint);
        Assert.assertEquals(splitter.getRecordSizeHint(), recordSizeHint);
    }

    @Test
    public void testLocateNextRecordGivenOneCompleteLineWithinRecordSizeHint() {
        final String[] lines = new String[] {
                "AA\n"
        };
        buffer = ByteBuffers.fromString(String.join("", lines), StandardCharsets.US_ASCII);

        Assert.assertEquals(splitter.locateNextRecord(buffer), lines[0].length());
        Assert.assertEquals(splitter.locateNextRecord(buffer), -1);
        Assert.assertEquals(buffer.position(), buffer.limit());
    }

    @Test
    public void testLocateNextRecordGivenMultipleCompleteLinesWithinRecordSizeHint() {
        final String[] lines = new String[] {
                "AA\n",
                "BB\n",
                "CC\n"
        };
        buffer = ByteBuffers.fromString(String.join("", lines), StandardCharsets.US_ASCII);

        Assert.assertEquals(splitter.locateNextRecord(buffer), lines[0].length() + lines[1].length() + lines[2].length());
        Assert.assertEquals(splitter.locateNextRecord(buffer), -1);
        Assert.assertEquals(buffer.position(), buffer.limit());
    }

    @Test
    public void testLocateNextRecordGivenOneIncompleteLineWithinRecordSizeHint() {
        final String[] lines = new String[] {
                "AA"
        };
        buffer = ByteBuffers.fromString(String.join("", lines), StandardCharsets.US_ASCII);

        Assert.assertEquals(splitter.locateNextRecord(buffer), -1);
        Assert.assertEquals(buffer.position(), buffer.limit());
    }

    @Test
    public void testLocateNextRecordGivenMultipleCompleteLinesAndOneIncompleteLineWithinRecordSizeHint() {
        final String[] lines = new String[] {
                "AA\n",
                "BB\n",
                "CC"
        };
        buffer = ByteBuffers.fromString(String.join("", lines), StandardCharsets.US_ASCII);

        Assert.assertEquals(splitter.locateNextRecord(buffer), lines[0].length() + lines[1].length());
        Assert.assertEquals(splitter.locateNextRecord(buffer), -1);
        Assert.assertEquals(buffer.position(), buffer.limit());
    }

    @Test
    public void testLocateNextRecordGivenOneCompleteLineBeyondRecordSizeHint() {
        final String[] lines = new String[] {
                "AAAAAAAAAAAA\n"
        };
        buffer = ByteBuffers.fromString(String.join("", lines), StandardCharsets.US_ASCII);

        Assert.assertEquals(splitter.locateNextRecord(buffer), lines[0].length());
        Assert.assertEquals(splitter.locateNextRecord(buffer), -1);
        Assert.assertEquals(buffer.position(), buffer.limit());
    }

    @Test
    public void testLocateNextRecordGivenMultipleCompleteLinesBeyondRecordSizeHintIndividually() {
        final String[] lines = new String[] {
                "AAAAAAAAAAAA\n",
                "BBBBBBBBBBBB\n",
                "CCCCCCCCCCCC\n"
        };
        buffer = ByteBuffers.fromString(String.join("", lines), StandardCharsets.US_ASCII);

        Assert.assertEquals(splitter.locateNextRecord(buffer), lines[0].length());
        Assert.assertEquals(splitter.locateNextRecord(buffer), lines[0].length() + lines[1].length());
        Assert.assertEquals(splitter.locateNextRecord(buffer), lines[0].length() + lines[1].length() + lines[2].length());
        Assert.assertEquals(splitter.locateNextRecord(buffer), -1);
        Assert.assertEquals(buffer.position(), buffer.limit());
    }

    @Test
    public void testLocateNextRecordGivenMultipleCompleteLinesBeyondRecordSizeHintCollectively() {
        final String[] lines = new String[] {
                "AAAA\n",
                "BBBB\n",
                "CCCC\n"
        };
        buffer = ByteBuffers.fromString(String.join("", lines), StandardCharsets.US_ASCII);

        Assert.assertEquals(splitter.locateNextRecord(buffer), lines[0].length() + lines[1].length());
        Assert.assertEquals(splitter.locateNextRecord(buffer), lines[0].length() + lines[1].length() + lines[2].length());
        Assert.assertEquals(splitter.locateNextRecord(buffer), -1);
        Assert.assertEquals(buffer.position(), buffer.limit());
    }

    @Test
    public void testLocateNextRecordGivenOneIncompleteLineBeyondRecordSizeHint() {
        final String[] lines = new String[] {
                "AAAAAAAAAAAA"
        };
        buffer = ByteBuffers.fromString(String.join("", lines), StandardCharsets.US_ASCII);

        Assert.assertEquals(splitter.locateNextRecord(buffer), -1);
        Assert.assertEquals(buffer.position(), buffer.limit());
    }

    @Test
    public void testLocateNextRecordGivenMultipleCompleteLinesAndOneIncompleteLineBeyondRecordSizeHintIndividually() {
        final String[] lines = new String[] {
                "AAAAAAAAAAAA\n",
                "BBBBBBBBBBBB\n",
                "CCCCCCCCCCCC"
        };
        buffer = ByteBuffers.fromString(String.join("", lines), StandardCharsets.US_ASCII);

        Assert.assertEquals(splitter.locateNextRecord(buffer), lines[0].length());
        Assert.assertEquals(splitter.locateNextRecord(buffer), lines[0].length() + lines[1].length());
        Assert.assertEquals(splitter.locateNextRecord(buffer), -1);
        Assert.assertEquals(buffer.position(), buffer.limit());
    }

    @Test
    public void testLocateNextRecordGivenMultipleCompleteLinesAndOneIncompleteLineBeyondRecordSizeHintCollectively() {
        final String[] lines = new String[] {
                "AAAA\n",
                "BBBB\n",
                "CCCC"
        };
        buffer = ByteBuffers.fromString(String.join("", lines), StandardCharsets.US_ASCII);

        Assert.assertEquals(splitter.locateNextRecord(buffer), lines[0].length() + lines[1].length());
        Assert.assertEquals(splitter.locateNextRecord(buffer), -1);
        Assert.assertEquals(buffer.position(), buffer.limit());
    }
}
