/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import static org.testng.Assert.assertEquals;

import java.util.HashMap;

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
        assertEquals(ff1.getPartitionKeyOption(), KinesisConstants.PartitionKeyOption.DETERMINISTIC);
        assertEquals(ff2.getPartitionKeyOption(), KinesisConstants.PartitionKeyOption.RANDOM);
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
