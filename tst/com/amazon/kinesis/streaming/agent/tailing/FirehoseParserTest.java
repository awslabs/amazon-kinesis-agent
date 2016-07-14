/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseParser;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;

public class FirehoseParserTest extends AbstractParserTest<FirehoseParser, FirehoseRecord> {
    private final static int BUNCH_OF_BYTES = 30 * 1024 * 1024;
    private final static int TEST_BUFFER_SIZE = BUNCH_OF_BYTES / 10;   // This guarantees that multiple buffers are needed to read the file

	@Override
	protected FirehoseParser buildParser(FileFlow<FirehoseRecord> flow, int bufferSize) {
        return new FirehoseParser(flow, bufferSize);
	}

	@Override
	protected int getTestBytes() {
		return BUNCH_OF_BYTES;
	}

	@Override
	protected int getTestBufferSize() {
		return TEST_BUFFER_SIZE;
	}
}
