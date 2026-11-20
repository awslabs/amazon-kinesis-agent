/*
 * Copyright (c) 2014-2017 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.RandomUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.tailing.KinesisConstants.PartitionKeyOption;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;

public class KinesisRecordTest extends TestBase {
    @SuppressWarnings("rawtypes")
    private FileFlow flow;
    private TrackedFile file;
    
    @BeforeMethod
    public void setup() throws IOException {
        flow = Mockito.mock(KinesisFileFlow.class);
        when(((KinesisFileFlow)flow).getPartitionKeyOption()).thenReturn(KinesisConstants.PartitionKeyOption.RANDOM);
        when(flow.getRecordTerminatorBytes()).thenReturn(KinesisFileFlow.DEFAULT_TRUNCATED_RECORD_TERMINATOR.getBytes(StandardCharsets.UTF_8));
        file = Mockito.mock(TrackedFile.class);
        when(file.getFlow()).thenReturn(flow);
    }

    @Test
    public void testStartEndOffset() {
    	KinesisRecord record = new KinesisRecord(file, 1023, new byte[100], 100);
        Assert.assertEquals(record.startOffset(), 1023);
        Assert.assertEquals(record.endOffset(), 1123);
    }

    @Test
    public void testRecordLength() {
    	KinesisRecord record = new KinesisRecord(file, 1023, new byte[200], 200);
    	String partitionKey = record.partitionKey();
        Assert.assertEquals(record.lengthWithOverhead(), 200 + partitionKey.length() + KinesisConstants.PER_RECORD_OVERHEAD_BYTES);
        Assert.assertEquals(record.length(), 200 + partitionKey.length());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testTruncate() throws IOException {
        byte[] data = RandomUtils.nextBytes((KinesisConstants.MAX_RECORD_SIZE_BYTES) + RandomUtils.nextInt(1, 100));
        KinesisRecord record = new KinesisRecord(file, 1023, data, data.length);
        record.truncate();
        Assert.assertEquals(record.lengthWithOverhead(), KinesisConstants.MAX_RECORD_SIZE_BYTES + KinesisConstants.PER_RECORD_OVERHEAD_BYTES);
        Assert.assertEquals(record.length(), KinesisConstants.MAX_RECORD_SIZE_BYTES);
        Assert.assertTrue(ByteBuffers.toString(record.data, StandardCharsets.UTF_8).endsWith(KinesisFileFlow.DEFAULT_TRUNCATED_RECORD_TERMINATOR));
    }
    
    @SuppressWarnings("rawtypes")
    @Test
    public void testGeneratePartitionKey() {
        final PartitionKeyOption partitionKeyOption = KinesisConstants.PartitionKeyOption.DETERMINISTIC;
        when(((KinesisFileFlow)flow).getPartitionKeyOption()).thenReturn(partitionKeyOption);
        
        byte[] data = RandomUtils.nextBytes(200);
        KinesisRecord record = new KinesisRecord(file, 1023, data, data.length);
        Assert.assertNotNull(record.partitionKey());
        Assert.assertEquals(record.partitionKey(), record.generatePartitionKey(partitionKeyOption, null));
    }

    @Test
    public void testGeneratePartitionKeyWithCustomOption() {
        when(((KinesisFileFlow)flow).getPartitionKeyOption()).thenReturn(KinesisConstants.PartitionKeyOption.CUSTOM);
        when(((KinesisFileFlow)flow).getPartitionKeyPath()).thenReturn(Arrays.asList("userId"));

        String jsonData = "{\"userId\": \"12345\", \"name\": \"John\"},{\"userId\": \"34567\", \"name\": \"Doe\"}";
        byte[] data = jsonData.getBytes(StandardCharsets.UTF_8);
        ByteBuffer view = ByteBuffers.getPartialView(ByteBuffer.wrap(data), 36,34);

        KinesisRecord record = new KinesisRecord(file, 1023, view, view.remaining());

        Assert.assertEquals(record.partitionKey(), "34567");
    }

    @Test
    public void testGeneratePartitionKeyWithInvalidIdentifier() {
        when(((KinesisFileFlow)flow).getPartitionKeyOption()).thenReturn(KinesisConstants.PartitionKeyOption.CUSTOM);
        when(((KinesisFileFlow)flow).getPartitionKeyPath()).thenReturn(Arrays.asList("nonexistent"));

        String jsonData = "{\"userId\": \"12345\", \"name\": \"John\"},{\"userId\": \"34567\", \"name\": \"Doe\"}";
        byte[] data = jsonData.getBytes(StandardCharsets.UTF_8);
        ByteBuffer view = ByteBuffers.getPartialView(ByteBuffer.wrap(data), 36,34);

        try {
            KinesisRecord record = new KinesisRecord(file, 1023, view, view.remaining());
            record.partitionKey();
            Assert.fail("Expected NoSuchElementException to be thrown");
        } catch (NoSuchElementException e) {
            Assert.assertTrue(e.getMessage().contains("Partition key path not found"),
                    "Exception message should mention missing identifier");
        }
    }
    
    @Test
    public void testGeneratePartitionKeyWithInvalidJson() {
        when(((KinesisFileFlow)flow).getPartitionKeyOption()).thenReturn(KinesisConstants.PartitionKeyOption.CUSTOM);
        when(((KinesisFileFlow)flow).getPartitionKeyPath()).thenReturn(Arrays.asList("userId"));

        byte[] data = "invalid json".getBytes(StandardCharsets.UTF_8);
        ByteBuffer view = ByteBuffers.getPartialView(ByteBuffer.wrap(data), 0,12);

        try {
            KinesisRecord record = new KinesisRecord(file, 1023, view, view.remaining());
            record.partitionKey();
            Assert.fail("Expected UncheckedIOException to be thrown");
        } catch (UncheckedIOException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to generate partition key"),
                    "Exception message should mention JSON parsing failure");
        }
    }

    @Test
    public void testGeneratePartitionKeyWithNullValue() {
        when(((KinesisFileFlow)flow).getPartitionKeyOption()).thenReturn(KinesisConstants.PartitionKeyOption.CUSTOM);
        when(((KinesisFileFlow)flow).getPartitionKeyPath()).thenReturn(Arrays.asList("userId"));

        String jsonData = "{\"userId\": \"12345\", \"name\": \"John\"},{\"userId\": \"\", \"name\": \"Doe\"}";
        byte[] data = jsonData.getBytes(StandardCharsets.UTF_8);
        ByteBuffer view = ByteBuffers.getPartialView(ByteBuffer.wrap(data), 36,29);

        try {
            KinesisRecord record = new KinesisRecord(file, 1023, view, view.remaining());
            record.partitionKey();
            Assert.fail("Expected IllegalStateException to be thrown");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Partition key value cannot be null or empty"),
                    "Exception message should mention null Partition key");
        }
    }

    private KinesisRecord customKeyRecord(List<String> path, String json) {
        when(((KinesisFileFlow)flow).getPartitionKeyOption()).thenReturn(KinesisConstants.PartitionKeyOption.CUSTOM);
        when(((KinesisFileFlow)flow).getPartitionKeyPath()).thenReturn(path);
        byte[] data = json.getBytes(StandardCharsets.UTF_8);
        return new KinesisRecord(file, 0, ByteBuffer.wrap(data), data.length);
    }

    @DataProvider(name="customPartitionKeyFound")
    public Object[][] customPartitionKeyFoundData() {
        return new Object[][] {
                { Arrays.asList("foo"), "{\"baz\":\"e1\",\"foo\":\"v1\",\"qux\":\"x\"}", "v1" },
                { Arrays.asList("foo", "bar"),
                        "{\"baz\":\"e1\",\"qux\":{\"s\":1},\"foo\":{\"x\":{\"y\":[1,{\"z\":2}]},\"bar\":\"v2\"},\"quux\":\"ok\"}",
                        "v2" },
                { Arrays.asList("id"), "{\"meta\":{\"x\":1},\"id\":\"y\"}", "y" },              // key after a nested object
                { Arrays.asList("id"), "{\"items\":[{\"a\":1}],\"id\":\"y\"}", "y" },         // key after an array of objects
                { Arrays.asList("id"), "{\"meta\":{\"id\":\"x\"},\"id\":\"y\"}", "y" },    // top-level wins over nested same name
                { Arrays.asList("a/b", "c~d"), "{\"a/b\":{\"c~d\":\"v\"}}", "v" },             // segments with '/' and '~'
                { Arrays.asList("n"), "{\"n\":42}", "42" },
                { Arrays.asList("b"), "{\"b\":true}", "true" },
        };
    }

    @Test(dataProvider="customPartitionKeyFound")
    public void testCustomPartitionKeyFound(List<String> path, String json, String expected) {
        Assert.assertEquals(customKeyRecord(path, json).partitionKey(), expected);
    }

    @DataProvider(name="customPartitionKeyNotFound")
    public Object[][] customPartitionKeyNotFoundData() {
        return new Object[][] {
                { Arrays.asList("id"), "{\"meta\":{\"id\":\"x\"}}" },                          // nested only; path is top-level
                { Arrays.asList("foo", "bar"), "{\"foo\":\"abc\"}" },    // path goes through a scalar
                { Arrays.asList("foo", "bar"), "{\"foo\":{\"other\":1}}" },
                { Arrays.asList("id"), "{}" },
        };
    }

    @Test(dataProvider="customPartitionKeyNotFound", expectedExceptions=NoSuchElementException.class)
    public void testCustomPartitionKeyNotFound(List<String> path, String json) {
        customKeyRecord(path, json);
    }

    // Records that used to make the old token scan loop forever must now fail fast.
    @DataProvider(name="customPartitionKeyNotAnObject")
    public Object[][] customPartitionKeyNotAnObjectData() {
        return new Object[][] { { "123" }, { "\"abc\"" }, { "true" }, { "[1,2]" }, { "[{\"id\":\"a\"}]" }, { "   " } };
    }

    @Test(dataProvider="customPartitionKeyNotAnObject", expectedExceptions=IllegalArgumentException.class, timeOut=5000)
    public void testCustomPartitionKeyRecordNotAnObject(String json) {
        customKeyRecord(Arrays.asList("id"), json);
    }

    @DataProvider(name="customPartitionKeyBadValue")
    public Object[][] customPartitionKeyBadValueData() {
        return new Object[][] { { "{\"id\":null}" }, { "{\"id\":\"\"}" }, { "{\"id\":{\"k\":\"v\"}}" }, { "{\"id\":[1]}" } };
    }

    @Test(dataProvider="customPartitionKeyBadValue", expectedExceptions=IllegalStateException.class)
    public void testCustomPartitionKeyBadValue(String json) {
        customKeyRecord(Arrays.asList("id"), json);
    }

    @Test(expectedExceptions=UncheckedIOException.class, timeOut=5000)
    public void testCustomPartitionKeyTruncatedRecord() {
        customKeyRecord(Arrays.asList("foo", "bar"), "{\"foo\":{\"x\":1");
    }
}
