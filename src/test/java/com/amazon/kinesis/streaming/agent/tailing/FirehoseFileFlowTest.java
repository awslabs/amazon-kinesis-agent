/*
 * Copyright (c) 2014-2017 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import static org.testng.Assert.assertEquals;

import java.util.HashMap;

import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;

public class FirehoseFileFlowTest extends FileFlowTest<FirehoseFileFlow> {

	@Test
    public void testFirehoseConfigurationDefaults() {
        AgentContext context = TestUtils.getTestAgentContext();
        FirehoseFileFlow ff = buildFileFlow(context, getConfiguration("/tmp/testfile.log.*", "testdes"));
        assertEquals(ff.getMaxBufferAgeMillis(), FirehoseConstants.DEFAULT_MAX_BUFFER_AGE_MILLIS);
        assertEquals(ff.getMaxBufferSizeRecords(), FirehoseConstants.MAX_BUFFER_SIZE_RECORDS);
        assertEquals(ff.getMaxBufferSizeBytes(), FirehoseConstants.MAX_BUFFER_SIZE_BYTES);
        assertEquals(ff.getRetryInitialBackoffMillis(), FirehoseConstants.DEFAULT_RETRY_INITIAL_BACKOFF_MILLIS);
        assertEquals(ff.getRetryMaxBackoffMillis(), FirehoseConstants.DEFAULT_RETRY_MAX_BACKOFF_MILLIS);
    }

    @Test
    public void testRecordAggregationSizeBytes() {
        AgentContext context = TestUtils.getTestAgentContext();
        final String file = "/var/log/message*";
        FirehoseFileFlow ff1 = buildFileFlow(context, new Configuration(new HashMap<String, Object>() {{
            put("filePattern", file);
            put(getDestinationKey(), "des1");
            put("aggregatedRecordSizeBytes", 5 * 1024);
        }}));
        FirehoseFileFlow ff2 = buildFileFlow(context, new Configuration(new HashMap<String, Object>() {{
            put("filePattern", file);
            put(getDestinationKey(), "des2");
            put("aggregatedRecordSizeBytes", 0);
        }}));
        assertEquals(ff1.getRecordSplitter().getClass(), AggregationSplitter.class);
        assertEquals(ff2.getRecordSplitter().getClass(), SingleLineSplitter.class);
    }

	@Override
	protected String getDestinationKey() {
		return FirehoseConstants.DESTINATION_KEY;
	}

	@Override
	protected FirehoseFileFlow buildFileFlow(AgentContext context, Configuration config) {
		return new FirehoseFileFlow(context, config);
	}
}
