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

import com.amazon.kinesis.streaming.agent.ByteBuffers;

/**
 * Returns one record per line.
 * TODO: Limitation of this splitter is that it requires a newline at the end
 *       of the file, otherwise it will miss the last record. We should be able
 *       to handle this at the level of the {@link IParser} implementation.
 * TODO: Should we parametrize the line delimiter?
 */
public class SingleLineSplitter implements ISplitter {
    public static final char LINE_DELIMITER = '\n';

    @Override
    public int locateNextRecord(ByteBuffer buffer) {
        // TODO: Skip empty records, commented records, header lines, etc...
        //       (based on FileFlow configuration?).
        return ByteBuffers.advanceBufferToNextLine(buffer);
    }
}
