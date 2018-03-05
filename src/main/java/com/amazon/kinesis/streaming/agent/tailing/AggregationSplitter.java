/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.google.common.base.Preconditions;
import lombok.Getter;

import java.nio.ByteBuffer;

/**
 * An implementation of {@link ISplitter} that splits a buffer into aggregated records that contain at least 1 complete line and are
 * generally smaller than or equal in size to the {@code recordSizeHint} unless there is only 1 complete line
 */
public class AggregationSplitter implements ISplitter {
    /**
     * The hint for the aggregated record size in bytes.
     */
    @Getter
    private final int recordSizeHint;

    /**
     * Constructs an {@link AggregationSplitter} object given the hint for the aggregated record size in bytes.
     *
     * @param recordSizeHint the hint for the aggregated record size in bytes
     */
    public AggregationSplitter(final int recordSizeHint) {
        Preconditions.checkArgument(recordSizeHint > 0);
        this.recordSizeHint = recordSizeHint;
    }

    @Override
    public int locateNextRecord(final ByteBuffer buffer) {
        return advanceBufferToNextAggregatedRecord(buffer);
    }

    /**
     * Advances the buffer current position to be at the index following the end of the current aggregated record, that is composed of at
     * least 1 complete line, and whose size is generally less than or equal to the {@code recordSizeHint} unless there is only 1 complete
     * line, or else the end of the buffer if there is no complete line.
     *
     * @param buffer the buffer to read the next aggregated record from
     * @return {@code position} of the buffer at the index following the end of the current aggregated record; {@code -1} if the end of the
     *         buffer was reached
     */
    private int advanceBufferToNextAggregatedRecord(final ByteBuffer buffer) {
        final int initialPosition = buffer.position();
        int lastLinePosition = -1;
        while (ByteBuffers.advanceBufferToNextLine(buffer) != -1) {
            if (lastLinePosition != -1 && buffer.position() - initialPosition > recordSizeHint) {
                buffer.position(lastLinePosition);
                return lastLinePosition;
            }
            lastLinePosition = buffer.position();
        }
        if (lastLinePosition != -1) {
            buffer.position(lastLinePosition);
        }
        return lastLinePosition;
    }
}
