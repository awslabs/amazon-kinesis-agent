/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import static org.testng.Assert.assertEquals;

import java.nio.charset.StandardCharsets;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseConstants;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseFileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow.InitialPosition;
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

	@Override
	protected String getDestinationKey() {
		return FirehoseConstants.DESTINATION_KEY;
	}

	@Override
	protected FirehoseFileFlow buildFileFlow(AgentContext context, Configuration config) {
		return new FirehoseFileFlow(context, config);
	}
}
