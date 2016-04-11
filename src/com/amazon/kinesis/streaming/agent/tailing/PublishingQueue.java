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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.IHeartbeatProvider;
import com.amazon.kinesis.streaming.agent.Logging;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * A queue that keeps track of record buffers (aka batches) ready for
 * publishing. It tracks buffers that were never published (the
 * "never-published queue") as well as buffers that were previously published
 * unsuccessfully and are slated for a retry (the "retry queue").
 *
 * Buffers in the "retry queue" are always given priority over those in the
 * "never-published queue" when polling. Specifically, {@link #take()} returns
 * any buffers in the retry-queue immediately (or as they become available).
 *
 * New records (added via {@link #offerRecord(IRecord)}) are kept in a temporary
 * buffer until it "matures" (based on implementation-specific policy and/or
 * configuration, such as maximum time or maximum age), at which time it's
 * pushed into the bounded "never-published queue".
 *
 * A buffer "matures" (and is queued for processing) based on any one of the
 * following three criteria:
 * <ul>
 *   <li>Number of records in temporary buffer: cannot exceed value set by
 *       {@link FileFlow#getMaxBufferSizeRecords()}.</li>
 *   <li>Size of the temporary buffer (in bytes): cannot exceed value set by
 *       {@link FileFlow#getMaxBufferSizeBytes()}.</li>
 *   <li>Age of the buffer (time since first record was added): this class will
 *       try to maintain it below the value set by
 *       {@link FileFlow#getMaxBufferAgeMillis()}; however this is only checked
 *       before/after buffers are removed from the queue (in calls to
 *       {@link #take()} and {@link #tryTake()}) or with an explicit call to
 *       {@link #checkPendingRecords()} because for a consumer the behavior is
 *       the same. If {@link #offerRecord(IRecord) offerRecord} was called at least
 *       once and no calls to {@link #take()}/{@link #tryTake()}/
 *       {@link #checkPendingRecords()} followed, the buffer age can grow
 *       indefinitely.</li>
 * </ul>
 *
 * @param <R> The record type.
 */
public final class PublishingQueue<R extends IRecord> implements IHeartbeatProvider {
    private static final Logger LOGGER = Logging.getLogger(PublishingQueue.class);

    private final FileFlow<R> flow;
    private final String name;

    private final Queue<RecordBuffer<R>> neverPubQueue;
    private final int neverPubCapacity;
    private final Queue<RecordBuffer<R>> retryQueue;
    private boolean isOpen = true;

    private final ReentrantLock lock;
    private final Condition notEmpty;
    private final Condition notFull;

    private int queuedRecords = 0;
    private long queuedBytes = 0;

    private final AtomicLong totalQueuedRecords = new AtomicLong(0);
    private final AtomicLong totalQueuedBuffers = new AtomicLong(0);
    private final AtomicLong totalQueueWaitTimeMillis = new AtomicLong(0);
    private final AtomicLong totalTakenBuffers = new AtomicLong(0);
    private final AtomicLong totalTakeTimeouts = new AtomicLong(0);
    private final AtomicLong totalBuffersQueuedForRetry = new AtomicLong(0);
    private final AtomicLong totalQueueTimeouts = new AtomicLong(0);
    // TODO: time a buffer spends in the queue
    //private final AtomicLong totalTimeInQueue = new AtomicLong(0);

    /** Where records are held before being queued, a.k.a. temporary buffer. */
    private RecordBuffer<R> currentBuffer;

    public PublishingQueue(
            FileFlow<R> flow,
            int capacity) {
        Preconditions.checkNotNull(flow);
        this.flow = flow;
        this.name = getClass().getSimpleName() + "[" + flow.getId() + "]";
        this.neverPubQueue = new LinkedList<>();
        this.neverPubCapacity = capacity;
        this.retryQueue = new LinkedList<>();
        this.lock = new ReentrantLock();
        this.notEmpty = lock.newCondition();
        this.notFull =  lock.newCondition();
        this.currentBuffer = new RecordBuffer<>(flow);
    }

    public boolean offerRecord(R record) {
        return offerRecord(record, true);
    }

    public boolean waitNotEmpty() {
        lock.lock();
        try {
            // It's a good time to check if temp buffer needs to be queued, in case the queue is empty
            checkPendingRecords();
            long waitMillis = flow.getWaitOnEmptyPublishQueueMillis();
            if (isOpen && waitMillis != 0) {
                try {
                    if (waitMillis > 0) {
                        // Wait for a limited time
                        long nanos = TimeUnit.MILLISECONDS.toNanos(waitMillis);
                        while (isOpen && neverPubQueue.isEmpty() && retryQueue.isEmpty() && nanos > 0) {
                            nanos = notEmpty.awaitNanos(nanos);
                        }
                    } else {
                        // Wait indefinitely
                        if (neverPubQueue.isEmpty() && retryQueue.isEmpty())
                            notEmpty.await();
                    }
                    return size() > 0;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // No need to make method interruptable; just return null.
                    LOGGER.trace("{}: Thread interrupted.", name, e);
                    return size() > 0;
                }
            } else
                return size() > 0;
        } finally {
            lock.unlock();
        }
    }

    public boolean offerRecord(R record, boolean block) {
        lock.lock();
        try {
            if (!isOpen)
                return false;
            // Check if we need to publish before this record, and then proceed
            if (checkPendingRecordsBeforeNewRecord(record, block)) {
                if(LOGGER.isDebugEnabled()) {
                    // SANITYCHECK: TODO: Remove when done debugging.
                    Preconditions.checkState(currentBuffer.sizeRecords() < flow.getMaxBufferSizeRecords());
                    Preconditions.checkState(currentBuffer.sizeBytesWithOverhead() + record.lengthWithOverhead() <= flow.getMaxBufferSizeBytes());
                }
                // Add record
                if (!record.shouldSkip()) {
                    currentBuffer.add(record);
                }
                return true;
            } else
                return false;
        } finally {
            lock.unlock();
        }
    }

    public boolean queueBufferForRetry(RecordBuffer<R> buffer) {
        lock.lock();
        try {
            if (!isOpen)
                return false;
            //LOGGER.trace("{}:{} Buffer added to retry queue.", name, buffer);
            retryQueue.add(buffer);
            totalBuffersQueuedForRetry.incrementAndGet();
            onQueueBufferSuccess(buffer, 0);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public RecordBuffer<R> peek() {
        lock.lock();
        try {
            if(!retryQueue.isEmpty()) {
                return retryQueue.peek();
            } else {
                return neverPubQueue.peek();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Keep private. Call only when holding lock.
     * @param elapsedWaiting
     * @return
     */
    private boolean tryQueueCurrentBuffer(long elapsedWaiting) {
        if (currentBuffer.isEmpty())
            return true;    // no-op
        if (isOpen && neverPubQueue.size() < neverPubCapacity) {
            neverPubQueue.add(currentBuffer);
            //LOGGER.trace("{}:{} Buffer added to never-published queue.", name, currentBuffer);
            totalQueuedRecords.addAndGet(currentBuffer.sizeRecords());
            totalQueuedBuffers.incrementAndGet();
            onQueueBufferSuccess(currentBuffer, elapsedWaiting);
            currentBuffer = new RecordBuffer<>(flow);
            return true;
        } else if (elapsedWaiting > 0) {
            onQueueBufferTimeout(currentBuffer, elapsedWaiting);
            return false;
        } else
            return false;
    }

    private boolean queueCurrentBuffer(boolean block) {
        lock.lock();
        try {
            if (!isOpen)
                return false;
            else if (currentBuffer.isEmpty())
                return true;    // practically a no-op
            long waitMillis = flow.getWaitOnFullPublishQueueMillis();
            if (block && waitMillis != 0) {
                Stopwatch timer = Stopwatch.createStarted();
                try {
                    if (waitMillis > 0) {
                        // Wait for a limited time
                        long nanos = TimeUnit.MILLISECONDS.toNanos(waitMillis);
                        while (isOpen && neverPubQueue.size() == neverPubCapacity && nanos > 0) {
                            nanos = notFull.awaitNanos(nanos);
                        }
                    } else {
                        if (neverPubQueue.size() == neverPubCapacity) {
                            // Wait indefinitely
                            notFull.await();
                        }
                    }
                    return tryQueueCurrentBuffer(timer.elapsed(TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // Doesn't make sense to make this method interruptable; just return immediately
                    LOGGER.trace("{}: Thread interrupted.", name, e);
                    return tryQueueCurrentBuffer(timer.elapsed(TimeUnit.MILLISECONDS));
                }
            } else {
                return tryQueueCurrentBuffer(0);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Keep private. Call only when holding lock.
     * @return
     */
    private RecordBuffer<R> tryTake(long elapsedWaiting) {
        RecordBuffer<R> result = null;
        if(!retryQueue.isEmpty()) {
            result = retryQueue.poll();
            //LOGGER.trace("{}:{} Polled from retry queue.", name, result);
        } else {
            result = neverPubQueue.poll();
            //LOGGER.trace("{}:{} Polled from never-published queue.", name, result);
        }
        if(result != null) {
            return onTakeSuccess(result, elapsedWaiting);
        } else if (elapsedWaiting > 0) {
            return onTakeTimeout(elapsedWaiting);
        } else
            return null;
    }

    public RecordBuffer<R> take() {
        return take(true);
    }

    public RecordBuffer<R> take(boolean block) {
        lock.lock();
        try {
            if (block) {
                Stopwatch timer = Stopwatch.createStarted();
                waitNotEmpty();
                return tryTake(timer.elapsed(TimeUnit.MILLISECONDS));
            } else {
                return tryTake(0);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param buffer
     * @param elapsed
     * @return
     */
    private boolean onQueueBufferTimeout(RecordBuffer<R> buffer, long elapsed) {
        LOGGER.debug("{}:{} Timed-out while waiting to queue buffer (waited for {} milliseconds).",
                name, buffer, elapsed);
        totalQueueTimeouts.incrementAndGet();
        return false;
    }

    /**
     * Keep private. Call only when holding lock.
     * @param buffer
     * @param elapsed
     * @return
     */
    private boolean onQueueBufferSuccess(RecordBuffer<R> buffer, long elapsed) {
        queuedRecords += buffer.sizeRecords();
        queuedBytes += buffer.sizeBytesWithOverhead();
        totalQueueWaitTimeMillis.addAndGet(elapsed);
        notEmpty.signal();
        return true;
    }

    /**
     * Keep private. Call only when holding lock.
     * @param buffer
     * @param elapsed
     * @return
     */
    private RecordBuffer<R> onTakeSuccess(RecordBuffer<R> buffer, long elapsed) {
        queuedRecords -= buffer.sizeRecords();
        queuedBytes -= buffer.sizeBytesWithOverhead();
        // It's a good time to check if temp buffer needs to be queued, in case
        //  the queue was full
        checkPendingRecords();
        totalTakenBuffers.incrementAndGet();
        notFull.signal();
        return buffer;
    }

    /**
     * @param elapsed
     * @return
     */
    private RecordBuffer<R> onTakeTimeout(long elapsed) {
        totalTakeTimeouts.incrementAndGet();
        return null;
    }

    public int pendingRecords() {
        lock.lock();
        try {
            return currentBuffer.sizeRecords();
        } finally {
            lock.unlock();
        }
    }

    public long pendingBytes() {
        lock.lock();
        try {
            return currentBuffer.sizeBytesWithOverhead();
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return {@code true} either if the temp buffer does not need to be queued
     *         or if was queued successfully, and {@code false} if it needed to
     *         be queued, but could not for any reason.
     */
    public boolean checkPendingRecords() {
        lock.lock();
        try {
            if (!currentBuffer.isEmpty() &&
                    (currentBuffer.sizeBytesWithOverhead() >= flow.getMaxBufferSizeBytes()
                    || currentBuffer.sizeRecords() >= flow.getMaxBufferSizeRecords()
                    || currentBuffer.age() >= flow.getMaxBufferAgeMillis())) {
                return queueCurrentBuffer(false);
            } else
                return true;
        } finally {
            lock.unlock();
        }
    }

    public RecordBuffer<R> flushPendingRecords() {
        RecordBuffer<R> buffer = currentBuffer;
        if (queueCurrentBuffer(false))
            return buffer;
        else
            return null;
    }

    public int discardPendingRecords() {
        lock.lock();
        try {
            int discarded = currentBuffer.sizeRecords();
            if (!currentBuffer.isEmpty()) {
                LOGGER.trace("{}: Discarding {} records in the temporary buffer...",
                        name, currentBuffer.sizeRecords());
            }
            currentBuffer = new RecordBuffer<>(flow);
            return discarded;
        } finally {
            lock.unlock();
        }
    }

    public int discardAllRecords() {
        lock.lock();
        try {
            int discarded = discardPendingRecords();
            while (size() > 0) {
                RecordBuffer<R> buffer = tryTake(0);
                discarded += buffer.sizeRecords();
            }
            LOGGER.trace("{}: Discarded {} records.", name, discarded);
            return discarded;
        } finally {
            lock.unlock();
        }
    }

    public synchronized int size() {
        return neverPubQueue.size() + retryQueue.size();
    }

    public synchronized int retrySize() {
        return retryQueue.size();
    }

    public int capacity() {
        return neverPubCapacity;
    }

    public int totalRecords() {
        lock.lock();
        try {
            return queuedRecords + currentBuffer.sizeRecords();
        } finally {
            lock.unlock();
        }
    }

    public long totalBytes() {
        lock.lock();
        try {
            return queuedBytes + currentBuffer.sizeBytesWithOverhead();
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        lock.lock();
        try {
            isOpen = false;
            queueCurrentBuffer(false);

            // Any pending offer() calls should return false
            notFull.signalAll();

            // Any take() calls waiting on queue to fill should return null immediately
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param record
     * @param block
     * @return {@code true} either if the temp buffer does not need to be queued
     *         or if was queued successfully, and {@code false} if it needed to
     *         be queued, but could not for any reason.
     */
    private boolean checkPendingRecordsBeforeNewRecord(R record, boolean block) {
        lock.lock();
        try {
            if (!currentBuffer.isEmpty() && (
                    currentBuffer.sizeBytesWithOverhead() + flow.getPerBufferOverheadBytes() + record.lengthWithOverhead() > flow.getMaxBufferSizeBytes()
                    || currentBuffer.sizeRecords() >= flow.getMaxBufferSizeRecords())
            ) {
                return queueCurrentBuffer(block);
            } else
                return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Object heartbeat(AgentContext agent) {
        checkPendingRecords();
        return null;
    }

    // Use for debugging only please.
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName())
          .append("(neverPubQueueSize=").append(neverPubQueue.size())
          //.append(",neverPubQueueCapacity=").append(neverPubCapacity)
          .append(",retryQueueSize=").append(retryQueue.size())
          .append(",totalRecords=").append(totalRecords())
          //.append(",totalBytes=").append(totalBytes())
          .append(",pendingRecords=").append(pendingRecords())
          //.append(",pendingBytes=").append(pendingBytes())
          //.append(",flow=").append(flow.getId())
          .append(")");
        return sb.toString();
    }

    @SuppressWarnings("serial")
    public Map<String, Object> getMetrics() {
        return new HashMap<String, Object>() {{
            put("PublishingQueue.PendingRecords", currentBuffer.sizeRecords());
            put("PublishingQueue.TotalRecords", totalRecords());
            put("PublishingQueue.RetryQueueSize", retryQueue.size());
            put("PublishingQueue.NeverPublishedQueueSize", neverPubQueue.size());
            put("PublishingQueue.TotalQueuedRecords", totalQueuedRecords);
            put("PublishingQueue.TotalQueuedBuffers", totalQueuedBuffers);
            put("PublishingQueue.TotalTakenBuffers", totalTakenBuffers);
            put("PublishingQueue.TotalTakeTimeouts", totalTakeTimeouts);
            put("PublishingQueue.TotalBuffersQueuedForRetry", totalBuffersQueuedForRetry);
            put("PublishingQueue.TotalQueueTimeouts", totalQueueTimeouts);
            put("PublishingQueue.TotalQueuedWaitTimeMillis", totalQueueWaitTimeMillis);
            put("PublishingQueue.AverageQueueWaitTimeMillis", totalQueuedBuffers.get() == 0 ? 0.0 : (totalQueueWaitTimeMillis.doubleValue() / totalQueuedBuffers.doubleValue()));
            put("PublishingQueue.TotalTimeInQueueMillis", "NA");
            put("PublishingQueue.AverageTimeInQueueMillis", "NA");
        }};
    }
}
