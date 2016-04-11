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

public class FirehoseRecord extends AbstractRecord {
    public FirehoseRecord(TrackedFile file, long offset, ByteBuffer data) {
        super(file, offset, data);
    }

    public FirehoseRecord(TrackedFile file, long offset, byte[] data) {
        super(file, offset, data);
    }

    @Override
    public long lengthWithOverhead() {
        return length() + FirehoseConstants.PER_RECORD_OVERHEAD_BYTES;
    }

    @Override
    protected int getMaxDataSize() {
        return FirehoseConstants.MAX_RECORD_SIZE_BYTES;
    }
}
