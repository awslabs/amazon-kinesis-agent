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
 * Encapsulates the logic to split a buffer into records.
 * Implementation will define what a record is and how it's delimited.
 */
public interface ISplitter {
    /**
     * Advances the buffer to the beginning of the next record, or to
     * the end of the buffer if a new record was not found.
     *
     * @param buffer
     * @return The position of the next record in the buffer, or {@code -1}
     *         if the beginning of the record was not found before the end of
     *         the buffer.
     */
    public int locateNextRecord(ByteBuffer buffer);
}
