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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpointStore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

/**
 * A publisher that buffers records into an {@link PublishingQueue}, and can
 * make send requests asynchronously.
 *
 * @param <R> The record type.
 */
class AsyncPublisher<R extends IRecord> extends SimplePublisher<R> {
    private static final int NO_TIMEOUT = -1;
    private static final int MAX_SPIN_WAIT_TIME_MILLIS = 1000;
    protected final ExecutorService sendingExecutor;
    protected final AsyncPublisherThrottler<R> throttler;
    protected final AtomicInteger activeSendTasks = new AtomicInteger();
    protected final AtomicInteger waitingSendTasks = new AtomicInteger();

    private final AtomicLong totalRejectedSendTasks = new AtomicLong();

    /**
     *
     * @param agentContext
     * @param flow
     * @param checkpoints
     * @param sender
     * @param sendingExecutor The executor that will run the async send
     *        requests.
     */
    public AsyncPublisher(
            AgentContext agentContext,
            FileFlow<R> flow,
            FileCheckpointStore checkpoints,
            ISender<R> sender,
            ExecutorService sendingExecutor) {
        super(agentContext, flow, checkpoints, sender);
        this.sendingExecutor = sendingExecutor;
        this.throttler = new AsyncPublisherThrottler<>(
                this, flow.getRetryInitialBackoffMillis(),
                flow.getRetryMaxBackoffMillis());
    }

    /**
     * @return {@code false} if there are any records queued for sending or
     *         currently being sent (e.g. asynchronously), else {@code true}.
     */
    public synchronized boolean isIdle() {
        return queue.totalRecords() == 0 &&
                activeSendTasks.get() == 0 &&
                waitingSendTasks.get() == 0;
    }

    @VisibleForTesting
    void waitForIdle() {
        waitForIdle(NO_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /**
     * @param timeout Use a value {@code <= 0} to wait indefinitely.
     * @param unit
     * @return {@code true} if idle state was reached before the timeout
     *         expired, or {@code false} if idle state was not successfully
     *         reached.
     */
    public boolean waitForIdle(long timeout, TimeUnit unit) {
        Stopwatch timer = Stopwatch.createStarted();
        while(!isIdle()) {
            long remaining = timeout > 0 ?
                    (unit.toMillis(timeout) - timer.elapsed(TimeUnit.MILLISECONDS))
                    : Long.MAX_VALUE;
            if(remaining <= 0)
                return false;
            long sleepTime = Math.min(MAX_SPIN_WAIT_TIME_MILLIS, remaining);
            logger.trace("{}: Waiting for idle state. Sleeping {}ms. {}", name(), sleepTime, toString());
            // Perform a check on any pending records in case it's time to publish them before sleeping
            queue.checkPendingRecords();
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                // No need to make this method interruptible. Just return false signifying timeout.
                Thread.currentThread().interrupt();
                logger.trace("{}: Thread interrupted.", name(), e);
                return false;
            }
        }
        return true;
    }

    public boolean sendNextBufferAsync(boolean block) {
        if (block)
            queue.waitNotEmpty();
        waitingSendTasks.incrementAndGet();
        try {
            final RecordBuffer<R> buffer = pollNextBuffer(false);
            if (buffer != null) {
                return sendBufferAsync(buffer);
            } else
                return false;
        } finally {
            waitingSendTasks.decrementAndGet();
        }
    }

    public synchronized boolean sendBufferAsync(final RecordBuffer<R> buffer) {
        Runnable task = new Runnable() {
            @Override
            public void run() {
                AsyncPublisher.super.sendBufferSync(buffer);
            }
        };
        try {
            sendingExecutor.execute(task);
            onSendAccepted(buffer);
            return true;
        } catch(RejectedExecutionException e) {
            onSendRejected(buffer);
            return false;
        }
    }

    public void backoff() {
        if (isOpen)
            throttler.backoff();
    }

    /**
     * This method should not raise any exceptions.
     * @param buffer
     */
    protected synchronized void onSendAccepted(RecordBuffer<R> buffer) {
        logger.trace("{}:{} Send Scheduled", name(), buffer);
        throttler.onSendAccepted();
        activeSendTasks.incrementAndGet();
    }

    /**
     * This method should not raise any exceptions.
     * @param buffer
     */
    protected synchronized void onSendRejected(RecordBuffer<R> buffer) {
        logger.trace("{}:{} Send Rejected", name(), buffer);
        totalRejectedSendTasks.incrementAndGet();
        throttler.onSendRejected();
        queueBufferForRetry(buffer);
    }

    /**
     * This method should not raise any exceptions.
     * @param buffer
     */
    protected synchronized void onSendTaskCompleted(RecordBuffer<R> buffer) {
        logger.trace("{}:{} Send Completed", name(), buffer);
        activeSendTasks.decrementAndGet();
    }

    @Override
    protected synchronized void onSendSuccess(RecordBuffer<R> buffer) {
        super.onSendSuccess(buffer);
        throttler.onSendSuccess();
        onSendTaskCompleted(buffer);
    }

    @Override
    protected synchronized boolean onSendPartialSuccess(RecordBuffer<R> buffer, BufferSendResult<R> result) {
        double failure = (double)buffer.sizeRecords() / result.getOriginalRecordCount();
        throttler.onSendPartialSuccess(failure);
        try {
            return super.onSendPartialSuccess(buffer, result);
        } finally {
            // Must be called last
            onSendTaskCompleted(buffer);
        }
    }

    @Override
    protected synchronized boolean onSendError(RecordBuffer<R> buffer, Throwable t) {
        throttler.onSendError();
        try {
            return super.onSendError(buffer, t);
        } finally {
            // Must be called last
            onSendTaskCompleted(buffer);
        }
    }

    // Use for debugging only please.
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName())
          .append("(")
          .append("queue=").append(queue)
          .append(",activeSendTasks=").append(activeSendTasks.get())
          .append(")");
        return sb.toString();
    }

    @Override
    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = super.getMetrics();
        metrics.putAll(throttler.getMetrics());
        metrics.put("AsyncPublisher.WaitingSendTasks", waitingSendTasks.get());
        metrics.put("AsyncPublisher.ActiveSendTasks", activeSendTasks.get());
        metrics.put("AsyncPublisher.TotalRejectedSendTasks", totalRejectedSendTasks);
        return metrics;
    }
}
