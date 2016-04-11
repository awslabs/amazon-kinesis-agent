/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.tailing.AsyncPublisher;
import com.amazon.kinesis.streaming.agent.tailing.AsyncPublisherThrottler;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;
import com.google.common.base.Stopwatch;

public class AsyncPublisherThrottlerTest extends TailingTestBase {

    private AsyncPublisher<FirehoseRecord> getTestPublisher() {
        @SuppressWarnings("unchecked")
        AsyncPublisher<FirehoseRecord> pub = mock(AsyncPublisher.class);
        when(pub.getAgentContext()).thenReturn(this.getTestAgentContext());
        when(pub.name()).thenReturn("testpublisher");
        return pub;
    }

    @Test
    public void testMaxBackoff() {
        final int maxBackoffMillis = 1000;
        AsyncPublisherThrottler<FirehoseRecord> bo = new AsyncPublisherThrottler<FirehoseRecord>(
                getTestPublisher(), 100, maxBackoffMillis,
                AsyncPublisherThrottler.DEFAULT_BACKOFF_FACTOR, 0.0,
                AsyncPublisherThrottler.DEFAULT_RECOVERY_FACTOR);
        while(bo.getNextBackoff() < maxBackoffMillis)
            bo.onSendError();
        // More failures... and make sure that backoff doesn't change
        bo.onSendError();
        assertEquals(bo.getNextBackoff(), maxBackoffMillis);
        bo.onSendError();
        assertEquals(bo.getNextBackoff(), maxBackoffMillis);
    }

    @Test
    public void testBackoffComputationWithRejections() {
        final int initialBackoffMillis = 100;
        final int maxBackoffMillis = 1000;
        final double jitter = 0.0;
        final double backoffFactor = 2.0;
        AsyncPublisherThrottler<FirehoseRecord> bo = new AsyncPublisherThrottler<FirehoseRecord>(
                getTestPublisher(), initialBackoffMillis, maxBackoffMillis,
                backoffFactor, jitter, AsyncPublisherThrottler.DEFAULT_RECOVERY_FACTOR);
        assertEquals(bo.getNextBackoff(), 0);

        bo.onSendRejected();
        assertEquals(bo.getNextBackoff(), initialBackoffMillis);

        bo.onSendRejected();
        bo.onSendRejected();
        assertEquals(bo.getNextBackoff(), initialBackoffMillis);

        // Once failures happen, rejections are overtaken
        bo.onSendError();
        assertEquals(bo.getNextBackoff(), initialBackoffMillis);
        bo.onSendError();
        long expected = (long) (backoffFactor * initialBackoffMillis);
        assertEquals(bo.getNextBackoff(), expected);
        bo.onSendError();
        expected = (long) (backoffFactor * backoffFactor * initialBackoffMillis);
        assertEquals(bo.getNextBackoff(), expected);
        // etc...
    }

    @Test
    public void testBackoffComputationWithoutJitter() {
        final int initialBackoffMillis = 100;
        final int maxBackoffMillis = 1000;
        final double jitter = 0.0;
        final double backoffFactor = 2.0;
        AsyncPublisherThrottler<FirehoseRecord> bo = new AsyncPublisherThrottler<FirehoseRecord>(
                getTestPublisher(), initialBackoffMillis, maxBackoffMillis,
                backoffFactor, jitter, AsyncPublisherThrottler.DEFAULT_RECOVERY_FACTOR);
        assertEquals(bo.getNextBackoff(), 0);

        bo.onSendError();
        assertEquals(bo.getNextBackoff(), initialBackoffMillis);

        bo.onSendError();
        long expected = (long) (backoffFactor * initialBackoffMillis);
        assertEquals(bo.getNextBackoff(), expected);

        bo.onSendError();
        expected = (long) (backoffFactor * backoffFactor * initialBackoffMillis);
        assertEquals(bo.getNextBackoff(), expected);

        bo.onSendError();
        expected = (long) (backoffFactor * backoffFactor * backoffFactor * initialBackoffMillis);
        assertEquals(bo.getNextBackoff(), expected);

        bo.onSendError();
        assertEquals(bo.getNextBackoff(), maxBackoffMillis);

        bo.onSendError();
        assertEquals(bo.getNextBackoff(), maxBackoffMillis);
    }

    @Test
    public void testBackoffComputationWithJitter() {
        final int initialBackoffMillis = 100;
        final int maxBackoffMillis = 1000;
        final double backoffFactor = 2.0;
        final double jitter = 0.3;
        AsyncPublisherThrottler<FirehoseRecord> bo = new AsyncPublisherThrottler<FirehoseRecord>(
                getTestPublisher(), initialBackoffMillis, maxBackoffMillis,
                backoffFactor, jitter, AsyncPublisherThrottler.DEFAULT_RECOVERY_FACTOR);
        assertEquals(bo.getNextBackoff(), 0);

        bo.onSendError();
        long min = (long) (initialBackoffMillis * (1 - jitter));
        long max = (long) (initialBackoffMillis * (1 + jitter));
        assertTrue(bo.getNextBackoff() >= min);
        assertTrue(bo.getNextBackoff() <= max);

        bo.onSendError();
        long expected = (long) (backoffFactor * initialBackoffMillis);
        min = (long) (expected * (1 - jitter));
        max = (long) (expected * (1 + jitter));
        assertTrue(bo.getNextBackoff() >= min);
        assertTrue(bo.getNextBackoff() <= max);

        bo.onSendError();
        expected = (long) (backoffFactor * backoffFactor * initialBackoffMillis);
        min = (long) (expected * (1 - jitter));
        max = (long) (expected * (1 + jitter));
        assertTrue(bo.getNextBackoff() >= min);
        assertTrue(bo.getNextBackoff() <= max);

        bo.onSendError();
        expected = (long) (backoffFactor * backoffFactor * backoffFactor * initialBackoffMillis);
        min = (long) (expected * (1 - jitter));
        max = (long) (expected * (1 + jitter));
        assertTrue(bo.getNextBackoff() >= min);
        assertTrue(bo.getNextBackoff() <= max);

        bo.onSendError();
        min = (long) (maxBackoffMillis * (1 - jitter));
        max = (long) (maxBackoffMillis * (1 + jitter));
        assertTrue(bo.getNextBackoff() >= min);
        assertTrue(bo.getNextBackoff() <= max);

        bo.onSendError();
        min = (long) (maxBackoffMillis * (1 - jitter));
        max = (long) (maxBackoffMillis * (1 + jitter));
        assertTrue(bo.getNextBackoff() >= min);
        assertTrue(bo.getNextBackoff() <= max);
    }

    @Test
    public void testRecoveryOnSuccess() {
        final int initialBackoffMillis = 100;
        final int maxBackoffMillis = 1000;
        final double backoffFactor = 2.0;
        final double jitter = 0.3;
        final double recoveryFactor = 2.0;
        AsyncPublisherThrottler<FirehoseRecord> bo = new AsyncPublisherThrottler<FirehoseRecord>(
                getTestPublisher(), initialBackoffMillis, maxBackoffMillis,
                backoffFactor, jitter, recoveryFactor);
        for(int i = 0; i < 10; ++i)
            bo.onSendError();
        int expected = 10;
        assertEquals(bo.getFailures(), expected);

        bo.onSendSuccess();
        expected = (int) Math.round(expected / recoveryFactor); // 5
        assertEquals(bo.getFailures(), expected);

        bo.onSendSuccess();
        expected = (int) Math.round(expected / recoveryFactor); // 2.5
        assertEquals(bo.getFailures(), expected);

        bo.onSendSuccess();
        expected = (int) Math.round(expected / recoveryFactor); // 1.25
        assertEquals(bo.getFailures(), 1);

        bo.onSendSuccess();
        assertEquals(bo.getFailures(), 0); // 0.625 -> 0
    }

    @Test
    public void testFastRecoveryOnSuccess() {
        final int initialBackoffMillis = 100;
        final int maxBackoffMillis = 1000;
        final double backoffFactor = 2.0;
        final double jitter = 0.3;
        final double recoveryFactor = Double.MAX_VALUE;
        AsyncPublisherThrottler<FirehoseRecord> bo = new AsyncPublisherThrottler<FirehoseRecord>(
                getTestPublisher(), initialBackoffMillis, maxBackoffMillis,
                backoffFactor, jitter, recoveryFactor);
        for(int i = 0; i < 10; ++i)
            bo.onSendError();
        int expected = 10;
        assertEquals(bo.getFailures(), expected);

        bo.onSendSuccess();
        assertEquals(bo.getFailures(), 0);
    }

    @Test
    public void testNoSleepWithNoFailures() {
        final int initialBackoffMillis = 100;
        AsyncPublisherThrottler<FirehoseRecord> bo = new AsyncPublisherThrottler<FirehoseRecord>(
                getTestPublisher(), initialBackoffMillis, 1000);
        assertEquals(bo.getNextBackoff(), 0);
        Stopwatch timer = Stopwatch.createStarted();
        bo.backoff();
        assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < initialBackoffMillis);
    }

    @Test
    public void testSleepAfterFailures() {
        final double jitter = 0.0;
        AsyncPublisherThrottler<FirehoseRecord> bo = new AsyncPublisherThrottler<FirehoseRecord>(
                getTestPublisher(), 100, 1000,
                AsyncPublisherThrottler.DEFAULT_BACKOFF_FACTOR, jitter,
                AsyncPublisherThrottler.DEFAULT_RECOVERY_FACTOR);
        bo.onSendError();
        long expectedBackoff = bo.getNextBackoff();
        assertTrue(expectedBackoff > 0);
        Stopwatch timer = Stopwatch.createStarted();
        bo.backoff();
        assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) >= expectedBackoff);
    }

    @Test
    public void testSleepAfterRejection() {
        final double jitter = 0.0;
        AsyncPublisherThrottler<FirehoseRecord> bo = new AsyncPublisherThrottler<FirehoseRecord>(
                getTestPublisher(), 100, 1000,
                AsyncPublisherThrottler.DEFAULT_BACKOFF_FACTOR, jitter,
                AsyncPublisherThrottler.DEFAULT_RECOVERY_FACTOR);
        bo.onSendRejected();
        long expectedBackoff = bo.getNextBackoff();
        assertTrue(expectedBackoff > 0);
        Stopwatch timer = Stopwatch.createStarted();
        bo.backoff();
        assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) >= expectedBackoff);
        // Backpressure is reset after call to #backoff
        assertEquals(bo.getNextBackoff(), 0);
    }

    @Test
    public void testRejectionsResetAfterBackoff() {
        final double jitter = 0.0;
        AsyncPublisherThrottler<FirehoseRecord> bo = new AsyncPublisherThrottler<FirehoseRecord>(
                getTestPublisher(), 100, 1000,
                AsyncPublisherThrottler.DEFAULT_BACKOFF_FACTOR, jitter,
                AsyncPublisherThrottler.DEFAULT_RECOVERY_FACTOR);
        bo.onSendError();
        long expectedBackoff = bo.getNextBackoff();
        assertTrue(expectedBackoff > 0);

        // Rejections do not affect backoff time
        bo.onSendRejected();
        bo.onSendRejected();
        long newBackoff = bo.getNextBackoff();
        assertEquals(newBackoff, expectedBackoff);

        // Backoff and assert rejections have been reset
        assertTrue(bo.getRejections() > 0);
        Stopwatch timer = Stopwatch.createStarted();
        bo.backoff();
        assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) >= expectedBackoff);
        assertEquals(bo.getRejections(), 0);
    }

    @Test
    public void testSleepAbortedOnSuccess() throws InterruptedException {
        final double jitter = 0.0;
        final long minBackoff = 10000;
        final AsyncPublisherThrottler<FirehoseRecord> bo = new AsyncPublisherThrottler<FirehoseRecord>(
                getTestPublisher(), minBackoff, 10*minBackoff,
                AsyncPublisherThrottler.DEFAULT_BACKOFF_FACTOR, jitter,
                AsyncPublisherThrottler.DEFAULT_RECOVERY_FACTOR);
        bo.onSendError();
        bo.onSendError();
        bo.onSendError();
        bo.onSendError();
        long expectedBackoff = bo.getNextBackoff();
        assertTrue(expectedBackoff >= minBackoff);
        final Stopwatch timer = Stopwatch.createStarted();
        new Thread() {
            @Override
            public void run() {
                bo.backoff();
                timer.stop();
            }
        }.start();
        bo.onSendSuccess();
        // Sleep long enough for the abortion to be detected
        Thread.sleep(2*AsyncPublisherThrottler.SPIN_TIME_MILLIS);
        assertFalse(timer.isRunning());

        // we slept less than the expected backoff... as good as it gets
        assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < expectedBackoff);
    }
}
