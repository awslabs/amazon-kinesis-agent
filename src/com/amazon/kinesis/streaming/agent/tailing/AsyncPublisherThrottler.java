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
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;

import org.slf4j.Logger;

import com.amazon.kinesis.streaming.agent.Logging;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * Provides backoffs that increase exponentially (up to a max) in response to
 * any retriable failures while sending data to a destination.
 * The backoff times include random jitter to ensure retries are spread and
 * dont' cluster periodically in time. Each backoff will be adjusted +/- some
 * random percentage &lt;= to the jitter percentage specified. Setting the
 * jitter percentage to 0.0 will disable jitter entirely (not recommended in
 * production).
 *
 * After a number {@code FAILURES} of successive failures, the backoff
 * induced will be within:
 * {@code min(MAX_BACKOFF, (FACTOR ^ FAILURES) x INITIAL_BACKOFF) +/- JITTER}.
 *
 * When a send operation fails (partially or completely), {@code FAILURES} is
 * incremented by 1. When a send operation succeeds, {@code FAILURES} is
 * reduced (divided) by another factor ({@code RECOVERY}) until it reaches 0.
 *
 * A publisher can also signal backpressure calling {@link #onSendRejected()}
 * which will be followed by <em>at least</em> a single backoff of duration
 * {@code INITIAL_BACKOFF +/- JITTER}.
 *
 */
public class AsyncPublisherThrottler<R extends IRecord> {
    static final double DEFAULT_BACKOFF_FACTOR = 2.0;
    static final double DEFAULT_JITTER_PERCENT = 0.30;
    static final double DEFAULT_RECOVERY_FACTOR = 10.0;
    static final int DEFAULT_ACCEPTED_SENDS_BEFORE_YIELDING = 5;
    static final int DEFAULT_PERIOD_BETWEEN_YIELDS_MILLIS = 1_000;
    static final long SPIN_TIME_MILLIS = 250;

    private static final Logger LOGGER = Logging.getLogger(AsyncPublisherThrottler.class);

    /** initial backoff in milliseconds */
    @Getter private final long initialBackoffMillis;
    /** maximum dedlay in milliseconds */
    @Getter private final long maxBackoffMillis;
    /** backoff increase factor */
    @Getter private final double backoffFactor;
    /** factor for  **/
    @Getter private final double recoveryFactor;
    /** jitter factor in percentage **/
    @Getter private final double jitter;
    /** number of consecutive failures */
    private double failures;
    /** whether or not backpressure has been signaled to this class */
    private double rejections;
    /** the publisher that's using this instance */
    private final AsyncPublisher<R> publisher;
    /** flag to abort a backoff if a success was signaled in parallel */
    private volatile boolean abortBackoff = false;

    private final AtomicLong totalBackoffTime = new AtomicLong();
    private final AtomicLong totalBackoffCount = new AtomicLong();

    /**
     * Applies the default backoff factor, recovery factor and jitter.
     *
     * @param publisher the publisher that's using this instance
     * @param initialBackoffMillis initial backoff in milliseconds
     * @param maxBackoffMillis maximum backoff in milliseconds
     */
    public AsyncPublisherThrottler(AsyncPublisher<R> publisher,
            long initialBackoffMillis, long maxBackoffMillis) {
        this(publisher, initialBackoffMillis, maxBackoffMillis, DEFAULT_BACKOFF_FACTOR,
                DEFAULT_JITTER_PERCENT, DEFAULT_RECOVERY_FACTOR);
    }

    /**
     * @param publisher the publisher that's using this instance
     * @param initialBackoffMillis initial backoff in milliseconds; must be
     *         &gt;= 0
     * @param maxBackoffMillis maximum backoff in milliseconds; must be &gt;= 0
     * @param backoffFactor backoff factor; must be &gt;= 1.0
     * @param jitter maximum percentage of random jitter; setting the jitter
     *         percentage to 0.0 will disable jitter entirely (not recommended
     *         in production); must be between 0.0 and 1.0
     * @param recoveryFactory factor by which to divide failure count in case of
     *         success, to yield measured, slow recovery; must be &gt;= 2.0
     */
    public AsyncPublisherThrottler(AsyncPublisher<R> publisher,
            long initialBackoffMillis, long maxBackoffMillis,
            double backoffFactor, double jitter, double recoveryFactory) {
        Preconditions.checkArgument(jitter <= 1.0);
        Preconditions.checkArgument(jitter >= 0.0);
        Preconditions.checkArgument(backoffFactor >= 1.0);
        Preconditions.checkArgument(initialBackoffMillis >= 0);
        Preconditions.checkArgument(maxBackoffMillis >= 0);
        Preconditions.checkArgument(recoveryFactory >= 2.0);
        this.publisher = publisher;
        this.initialBackoffMillis = initialBackoffMillis;
        this.maxBackoffMillis = maxBackoffMillis;
        this.jitter = jitter;
        this.backoffFactor = backoffFactor;
        this.recoveryFactor = recoveryFactory;
        this.failures = 0.0;
        this.rejections = 0.0;
    }

    public synchronized void onSendRejected() {
        rejections += 1;
    }

    public synchronized void onSendAccepted() {
        if (rejections > 0.0) {
            rejections /= recoveryFactor;
            if (rejections < 1) {
                rejections = 0;
            }
        }
    }

    public synchronized void onSendError() {
        failures += 1;
    }

    public synchronized void onSendPartialSuccess(double failure) {
        failures += failure;
    }

    public synchronized void onSendSuccess() {
        if (failures > 0.0) {
            failures = failures / recoveryFactor;
            if (failures < 1) {
                failures = 0;
            }
        }
        abortBackoff = true;
    }

    public int getFailures() {
        return (int) Math.round(failures);
    }

    public int getRejections() {
        return (int) Math.round(rejections);
    }

    /**
     * If no sleep is required (e.g. no failures and no rejections), this method
     * call returns immediately. Otherwise, sleeps up to the amount of time
     * returned by {@link #getNextBackoff()}. If a call to
     * {@link #onSendSuccess()} is received after sleep has started, sleep is
     * interrupted and this method returns immediately after.
     * @return
     *
     * @return the actual time spent sleeping in milliseconds.
     */
    public long backoff() {
        long delay = 0;
        synchronized(this) {
            delay = getNextBackoff();
            if (delay > 0) {
                LOGGER.debug("{}: Backing off for {} millis (failures: {}, rejections: {})...",
                        publisher.name(), delay, failures, rejections);
                rejections = 0;
            }
            abortBackoff = false;
        }
        if (delay > 0) {
            totalBackoffCount.incrementAndGet();
            return sleepUpTo(delay);
        } else
            return 0;
    }

    private long sleepUpTo(long delay) {
        Stopwatch timer = Stopwatch.createStarted();
        try {
            long remaining = delay;
            while (remaining > 0) {
                if (abortBackoff) {
                    LOGGER.trace("{}: Backoff was aborted.", publisher.name());
                    break;
                } else {
                    try {
                        Thread.sleep(SPIN_TIME_MILLIS);
                    } catch (InterruptedException e) {
                        // Preserve interruption
                        Thread.currentThread().interrupt();
                        LOGGER.trace("{}: Backoff was interrupted", publisher.name(), e);
                        break;
                    }
                    remaining = delay - timer.elapsed(TimeUnit.MILLISECONDS);
                }
            }
        } finally {
            totalBackoffTime.addAndGet(timer.elapsed(TimeUnit.MILLISECONDS));
        }
        return timer.elapsed(TimeUnit.MILLISECONDS);
    }

    /**
     * @return 0 if healthy ({@code failures == 0}), or the current
     *         backoff given current state accoding to the formula
     *         {@code BACKOFF = min(MAX_BACKOFF, F^MULTIPLIER x INITIAL_BACKOFF)},
     *         with additional randomized jitter within
     *         {@code +/- BACKOFF x JITTER}.
     */
    @VisibleForTesting
    synchronized long getNextBackoff() {
        if(getFailures() == 0 && getRejections() == 0) {
            return 0;
        } else {
            // When backpressure exists, or we're yielding, then power = 0 and backoff = initialBackoffMillis
            int power = Math.max(0, getFailures() - 1);
            long delay = (long) Math.min(maxBackoffMillis,
                    initialBackoffMillis * Math.pow(backoffFactor, power));
            if(jitter > 0.0) {
                double jitterFactor = 1 + ThreadLocalRandom.current().nextDouble(-1, 1) * jitter;
                delay *= jitterFactor;
            }
            return delay;
        }
    }

    @SuppressWarnings("serial")
    public Map<String, Object> getMetrics() {
        return new HashMap<String, Object>() {{
            put("AsyncPublisherSendBackoff.CurrentFailures", failures);
            put("AsyncPublisherSendBackoff.CurrentRejections", rejections);
            put("AsyncPublisherSendBackoff.TotalBackoffCount", totalBackoffCount);
            put("AsyncPublisherSendBackoff.TotalBackoffTimeMillis", totalBackoffTime);
            put("AsyncPublisherSendBackoff.AverageBackoffTimeMillis", totalBackoffCount.get() == 0 ? 0.0 : (totalBackoffTime.doubleValue() / totalBackoffCount.doubleValue()));
        }};
    }
}
