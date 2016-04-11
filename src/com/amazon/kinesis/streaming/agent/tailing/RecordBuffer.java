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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

/**
 * An record buffer.
 *
 * @param <R> The record type.
 */
@NotThreadSafe
public class RecordBuffer<R extends IRecord> implements Iterable<R> {
    private static final int DEFAULT_INITIAL_CAPACITY = 500;
    private static final AtomicLong NEXT_BUFFER_ID = new AtomicLong(1);

    protected final FileFlow<R> flow;
    protected final List<R> records;

    protected long timestamp = -1;
    /** Cumulative size of records including any per-record overhead. */
    protected long currentSizeBytes = 0;
    protected IRecord lastRecord;
    protected final long id;

    public RecordBuffer(FileFlow<R> flow) {
        this.flow = flow;
        this.records = new ArrayList<R>(DEFAULT_INITIAL_CAPACITY);
        this.id = NEXT_BUFFER_ID.incrementAndGet();
    }

    /**
     * Adds a record to the buffer.
     * @param record
     */
    public void add(R record) {
        records.add(record);
        lastRecord = record;
        currentSizeBytes += record.lengthWithOverhead();
        if(timestamp < 0) {
            timestamp = System.currentTimeMillis();
        }
    }

    /**
     * @return A unique sequence number for this buffer.
     */
    public long id() {
        return id;
    }

    public FileFlow<R> flow() {
        return flow;
    }

    public long timestamp() {
        return timestamp;
    }

    public long age() {
        return age(System.currentTimeMillis());
    }

    @VisibleForTesting
    long age(long at) {
        return timestamp > 0 ? (at - timestamp) : 0;
    }

    public int sizeRecords() {
        return records.size();
    }

    public long sizeBytes() {
        return currentSizeBytes;
    }

    public long sizeBytesWithOverhead() {
        return currentSizeBytes + flow.getPerBufferOverheadBytes();
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public TrackedFile checkpointFile() {
        return lastRecord.file();
    }

    public long checkpointOffset() {
        return lastRecord.endOffset();
    }

    /**
     * Removes the records at the specified indices.
     * @param itemsToRemoveSorted Indices of the records to be removed.
     * @return A buffer identical to the current one with the specified records
     *         removed.
     */
    public RecordBuffer<R> remove(int... itemsToRemoveSorted) {
        return remove(Ints.asList(itemsToRemoveSorted));
    }

    /**
     * Removes the records at the specified indices.
     * @param itemsToRemoveSorted Indices of the records to be removed.
     * @return A buffer identical to the current one with the specified records
     *         removed.
     */
    public RecordBuffer<R> remove(List<Integer> itemsToRemoveSorted) {
        if(!itemsToRemoveSorted.isEmpty()) {
            Iterator<Integer> toRemoveIt = itemsToRemoveSorted.iterator();
            int toRemoveIndex = toRemoveIt.next();
            int newIndex = toRemoveIndex;
            for(int originalIndex = toRemoveIndex; originalIndex < records.size(); ++originalIndex) {
                if(originalIndex == toRemoveIndex) {
                    currentSizeBytes -= records.get(toRemoveIndex).length();
                    toRemoveIndex = toRemoveIt.hasNext() ? toRemoveIt.next() : -1;
                } else {
                    records.set(newIndex++, records.get(originalIndex));
                }
            }
            // Shrink list to include only non-null elements
            records.subList(newIndex, records.size()).clear();
        }
        return this;
    }

    @Override
    public Iterator<R> iterator() {
        return records.iterator();
    }

    /**
     * NOTE: Use for debugging only please.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName())
            .append("(id=").append(id)
            .append(",records=").append(sizeRecords())
            .append(",bytes=").append(sizeBytes())
            .append(")");
        return sb.toString();
    }
}
