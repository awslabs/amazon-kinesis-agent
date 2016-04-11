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

import lombok.Getter;
import lombok.ToString;

/**
 * The result of a {@link ISender#sendBuffer(RecordBuffer)} call.
 *
 * @param <R> The record type.
 */
@ToString
public class BufferSendResult<R extends IRecord> {
    public static <R extends IRecord> BufferSendResult<R> succeeded(RecordBuffer<R> buffer) {
        return new BufferSendResult<R>(Status.SUCCESS, buffer, buffer.sizeRecords());
    }

    public static <R extends IRecord> BufferSendResult<R> succeeded_partially(RecordBuffer<R> retryBuffer, int originalRecordCount) {
        return new BufferSendResult<R>(Status.PARTIAL_SUCCESS, retryBuffer, originalRecordCount);
    }

    @Getter private final RecordBuffer<R> buffer;
    @Getter private final int originalRecordCount;
    @Getter private final Status status;

    private BufferSendResult(Status status, RecordBuffer<R> buffer, int originalRecordCount) {
        this.buffer = buffer;
        this.originalRecordCount = originalRecordCount;
        this.status = status;
    }

    public int sentRecordCount() {
        return status == Status.SUCCESS ? originalRecordCount : (originalRecordCount - buffer.sizeRecords());
    }

    public int remainingRecordCount() {
        return status == Status.SUCCESS ? 0 : buffer.sizeRecords();
    }

    public static enum Status {
        SUCCESS,
        PARTIAL_SUCCESS,
    }
}
