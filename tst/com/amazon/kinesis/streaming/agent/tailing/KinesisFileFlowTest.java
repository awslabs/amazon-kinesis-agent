/*
 * Copyright (c) 2014-2017 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.config.ConfigurationException;
import com.amazon.kinesis.streaming.agent.tailing.KinesisConstants;
import com.amazon.kinesis.streaming.agent.tailing.KinesisFileFlow;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;

public class KinesisFileFlowTest extends FileFlowTest<KinesisFileFlow> {

	@Test
    public void testKinesisConfigurationDefaults() {
        AgentContext context = TestUtils.getTestAgentContext();
        KinesisFileFlow ff = buildFileFlow(context, getConfiguration("/tmp/testfile.log.*", "testdes"));
        assertEquals(ff.getMaxBufferAgeMillis(), KinesisConstants.DEFAULT_MAX_BUFFER_AGE_MILLIS);
        assertEquals(ff.getMaxBufferSizeRecords(), KinesisConstants.MAX_BUFFER_SIZE_RECORDS);
        assertEquals(ff.getMaxBufferSizeBytes(), KinesisConstants.MAX_BUFFER_SIZE_BYTES);
        assertEquals(ff.getRetryInitialBackoffMillis(), KinesisConstants.DEFAULT_RETRY_INITIAL_BACKOFF_MILLIS);
        assertEquals(ff.getRetryMaxBackoffMillis(), KinesisConstants.DEFAULT_RETRY_MAX_BACKOFF_MILLIS);
        assertEquals(ff.getPartitionKeyOption(), KinesisConstants.PartitionKeyOption.RANDOM);
    }
	

    @SuppressWarnings("serial")
	@Test
	public void testPartitionKeyOption() {
	    AgentContext context = TestUtils.getTestAgentContext();
        final String file = "/var/log/message*";
        KinesisFileFlow ff1 = buildFileFlow(context, new Configuration(new HashMap<String, Object>() {{
            put("filePattern", file);
            put(getDestinationKey(), "des1");
            put(KinesisConstants.PARTITION_KEY, "DETERMINISTIC");
        }}));
        KinesisFileFlow ff2 = buildFileFlow(context, new Configuration(new HashMap<String, Object>() {{
            put("filePattern", file);
            put(getDestinationKey(), "des2");
            put(KinesisConstants.PARTITION_KEY, "RANDOM");
        }}));
        KinesisFileFlow ff3 = buildFileFlow(context, new Configuration(new HashMap<String, Object>() {{
            put("filePattern", file);
            put(getDestinationKey(), "des3");
            put(KinesisConstants.PARTITION_KEY, "CUSTOM");
            put(KinesisConstants.PARTITION_KEY_PATH, "userId");
        }}));
        assertEquals(ff1.getPartitionKeyOption(), KinesisConstants.PartitionKeyOption.DETERMINISTIC);
        assertEquals(ff2.getPartitionKeyOption(), KinesisConstants.PartitionKeyOption.RANDOM);
        assertEquals(ff3.getPartitionKeyOption(), KinesisConstants.PartitionKeyOption.CUSTOM);
        assertNull(ff1.getPartitionKeyPath());
        assertNull(ff2.getPartitionKeyPath());
        assertEquals(ff3.getPartitionKeyPath(), Arrays.asList("userId"));
	}

    @DataProvider(name="badPartitionKeyPathInConfig")
    public Object[][] testBadPartitionKeyPathInConfigData(){
        // { partitionKeyOption, partitionKeyPath } -- null means the key is not set
        return new Object[][] {
                { "CUSTOM", null },           // CUSTOM needs a path
                { "CUSTOM", "" },
                { "RANDOM", "a" },            // path without CUSTOM
                { "DETERMINISTIC", "a" },
                { null, "a" },                // option defaults to RANDOM
                { "CUSTOM", "a//b" },         // empty segments
                { "CUSTOM", "/a" },
                { "CUSTOM", "a/" },
                { "CUSTOM", "a~2b" },         // invalid escapes
                { "CUSTOM", "a~" },
                { "CUSTOM", "a~b" },
        };
    }

    @SuppressWarnings("serial")
    @Test(dataProvider="badPartitionKeyPathInConfig",
          expectedExceptions=ConfigurationException.class)
    public void testBadPartitionKeyPathConfig(final String partitionKeyOption,
            final String partitionKeyPath) {
        AgentContext context = TestUtils.getTestAgentContext();
        final String file = "/var/log/message*";
        buildFileFlow(context, new Configuration(new HashMap<String, Object>() {{
            put("filePattern", file);
            put(getDestinationKey(), "des");
            if (partitionKeyOption != null) put(KinesisConstants.PARTITION_KEY, partitionKeyOption);
            if (partitionKeyPath != null) put(KinesisConstants.PARTITION_KEY_PATH, partitionKeyPath);
        }}));
    }

    @DataProvider(name="partitionKeyPathParsing")
    public Object[][] testPartitionKeyPathParsingData(){
        return new Object[][] {
                { "foo", Arrays.asList("foo") },
                { "foo/bar", Arrays.asList("foo", "bar") },
                { "a~1b", Arrays.asList("a/b") },           // ~1 -> '/'
                { "a~0b/c", Arrays.asList("a~b", "c") },    // ~0 -> '~'
                { "a~01", Arrays.asList("a~1") },           // ~1 is decoded before ~0
                { "k8s.pod.name", Arrays.asList("k8s.pod.name") },
        };
    }

    @Test(dataProvider="partitionKeyPathParsing")
    public void testParsePartitionKeyPath(String value, List<String> expected) {
        assertEquals(KinesisFileFlow.parsePartitionKeyPath(value), expected);
    }
    
    @DataProvider(name="badPartitionKeyOptionInConfig")
    public Object[][] testPartitionKeyOptionInConfigData(){
        return new Object[][] { { "UNSUPPORTED" }, { "random" }, { "" } };
    }
    
    @SuppressWarnings("serial")
    @Test(dataProvider="badPartitionKeyOptionInConfig",
          expectedExceptions=ConfigurationException.class)
    public void testWrongPartitionKeyOption(final String partitionKeyOption) {
        AgentContext context = TestUtils.getTestAgentContext();
        final String file = "/var/log/message*";
        KinesisFileFlow ff = buildFileFlow(context, new Configuration(new HashMap<String, Object>() {{
            put("filePattern", file);
            put(getDestinationKey(), "des");
            put(KinesisConstants.PARTITION_KEY, partitionKeyOption);
        }}));
        ff.getPartitionKeyOption();
    }
    
    @DataProvider(name="badMaxBufferAgeMillisInConfig")
    @Override
    public Object[][] testMaxBufferAgeMillisInConfigData(){
        return new Object[][] { { 0 }, { -1 },
                { KinesisFileFlow.VALID_MAX_BUFFER_AGE_RANGE_MILLIS.lowerEndpoint() - 1 },
                { KinesisFileFlow.VALID_MAX_BUFFER_AGE_RANGE_MILLIS.upperEndpoint() + 1 },
        };
    }

    @DataProvider(name="badMaxBufferSizeRecordsInConfig")
    @Override
    public Object[][] testBadMaxBufferSizeRecordsInConfigData(){
        return new Object[][] { { 0 }, { -1 },
                { KinesisFileFlow.VALID_MAX_BUFFER_SIZE_RECORDS_RANGE.lowerEndpoint() - 1 },
                { KinesisFileFlow.VALID_MAX_BUFFER_SIZE_RECORDS_RANGE.upperEndpoint() + 1 },
        };
    }

    @DataProvider(name="badMaxBufferSizeBytesInConfig")
    @Override
    public Object[][] testMaxBufferSizeBytesInConfigData(){
        return new Object[][] { { 0 }, { -1 },
                { KinesisFileFlow.VALID_MAX_BUFFER_SIZE_BYTES_RANGE.lowerEndpoint() - 1 },
                { KinesisFileFlow.VALID_MAX_BUFFER_SIZE_BYTES_RANGE.upperEndpoint() + 1 },
        };
    }

	@Override
	protected String getDestinationKey() {
		return KinesisConstants.DESTINATION_KEY;
	}

	@Override
	protected KinesisFileFlow buildFileFlow(AgentContext context, Configuration config) {
		return new KinesisFileFlow(context, config);
	}

}
