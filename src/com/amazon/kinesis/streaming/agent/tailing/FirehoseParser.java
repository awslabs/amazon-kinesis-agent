/*
 * Copyright 2014-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License. 
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/asl/
 *  
 * or in the "license" file accompanying this file. 
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.nio.ByteBuffer;

/**
 * Implementation of {@link AbstractParser} specific to Firehose.
 */
public class FirehoseParser extends AbstractParser<FirehoseRecord> {

    public FirehoseParser(FileFlow<FirehoseRecord> flow) {
        super(flow);
    }

    public FirehoseParser(FileFlow<FirehoseRecord> flow, int bufferSize) {
        super(flow, bufferSize);
    }

    @Override
    protected synchronized FirehoseRecord buildRecord(TrackedFile recordFile, ByteBuffer data, long offset) {
        return new FirehoseRecord(recordFile, offset, data);
    }

    @Override
    protected int getMaxRecordSize() {
        return FirehoseConstants.MAX_RECORD_SIZE_BYTES;
    }
}
