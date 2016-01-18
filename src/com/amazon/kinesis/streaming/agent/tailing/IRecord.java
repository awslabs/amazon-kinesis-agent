/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

public interface IRecord {
    public ByteBuffer data();
    public long dataLength();
    public long lengthWithOverhead();
    public long length();
    public TrackedFile file();

    /**
     * NOTE: Used by {@link RecordBuffer} for checkpointing only.
     * @return Offset of in data buffer pointing to the end of data
     */
    public long endOffset();

    /**
     * NOTE: Used by {@link RecordBuffer} for checkpointing only.
     * @return Offset of in data buffer pointing to the beginning of data
     */
    public long startOffset();

    /**
     * This method should make sure the truncated record has the appropriate
     * terminator (e.g. newline).
     */
    public void truncate();

    /**
     * @return A string representation of the data in this record, encoded
     *         with UTF-8; use for debugging only please.
     */
    @Override
    public String toString();
}
