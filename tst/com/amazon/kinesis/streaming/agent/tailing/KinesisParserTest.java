/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.BeforeMethod;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.KinesisConstants;
import com.amazon.kinesis.streaming.agent.tailing.KinesisParser;
import com.amazon.kinesis.streaming.agent.tailing.KinesisRecord;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;

public class KinesisParserTest extends AbstractParserTest<KinesisParser, KinesisRecord> {
    private final static int BUNCH_OF_BYTES = 30 * 1024 * 1024;
    private final static int TEST_BUFFER_SIZE = BUNCH_OF_BYTES / 10;   // This guarantees that multiple buffers are needed to read the file
    
    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void setup() throws IOException {
        context = getTestKinesisAgentContext();
        flow = (FileFlow<KinesisRecord>) context.flows().get(0);
    }
    
	@Override
	protected KinesisParser buildParser(FileFlow<KinesisRecord> flow, int bufferSize) {
        return new KinesisParser(flow, bufferSize);
	}

	@Override
	protected int getTestBytes() {
		return BUNCH_OF_BYTES;
	}

	@Override
	protected int getTestBufferSize() {
		return TEST_BUFFER_SIZE;
	}
	
	private AgentContext getTestKinesisAgentContext() {
    	Map<String, Object> config = new HashMap<>();
    	List<Configuration> flows = new ArrayList<>();
    	Map<String, Object> flow = new HashMap<>();
        flow.put("filePattern", "/var/log/message.log*");
        flow.put(KinesisConstants.DESTINATION_KEY, RandomStringUtils.randomAlphabetic(5));
        flows.add(new Configuration(flow));
        config.put("flows", flows);
        config.put("checkpointFile", globalTestFiles.getTempFilePath());
        config.put("cloudwatch.emitMetrics", false);
        return TestUtils.getTestAgentContext(config);
    }
}
