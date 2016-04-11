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

import org.slf4j.Logger;

import com.amazon.kinesis.streaming.agent.Logging;

/**
 * Base implementation for an {@link ISender}.
 *
 * @param <R> The record type.
 */
public abstract class AbstractSender<R extends IRecord> implements ISender<R> {
    protected final Logger logger;

    public AbstractSender() {
        this.logger = Logging.getLogger(getClass());
    }

    @Override
    public BufferSendResult<R> sendBuffer(RecordBuffer<R> buffer) {
        if (getMaxSendBatchSizeRecords() > 0 && buffer.sizeRecords() > getMaxSendBatchSizeRecords()) {
            throw new IllegalArgumentException("Buffer is too large for service call: " + buffer.sizeRecords() + " records vs. allowed maximum of " + getMaxSendBatchSizeRecords());
        }
        if (getMaxSendBatchSizeBytes() > 0 && buffer.sizeBytesWithOverhead() > getMaxSendBatchSizeBytes()) {
            throw new IllegalArgumentException("Buffer is too large for service call: " + buffer.sizeBytesWithOverhead() + " bytes vs. allowed maximum of " + getMaxSendBatchSizeBytes());
        }
        return attemptSend(buffer);
    }

    protected abstract long getMaxSendBatchSizeBytes();
    protected abstract int getMaxSendBatchSizeRecords();
    protected abstract BufferSendResult<R> attemptSend(RecordBuffer<R> buffer);
}
