/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;
import com.amazon.kinesis.streaming.agent.tailing.PublishingQueue;
import com.amazon.kinesis.streaming.agent.tailing.RecordBuffer;
import com.amazon.kinesis.streaming.agent.tailing.testing.RecordGenerator;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;
import com.google.common.base.Stopwatch;

/**
 * TODO: Add tests for thread-safety of implementation.
 */
public class PublishingQueueTest extends TailingTestBase {
    private static final int TEST_TIMEOUT = 2000;

    private AgentContext context;
    private FileFlow<FirehoseRecord> flow;
    private int capacity = 10;
    private final long maxBufferAge = 1000;
    private final long maxWaitOnFullQueue = 500;
    private final long maxWaitOnEmptyQueue = 500;

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void setup() throws IOException {
        context = getTestAgentContext();
        flow = (FileFlow<FirehoseRecord>) context.flows().get(0);
        flow = Mockito.spy(flow);
        Mockito.when(flow.getWaitOnFullPublishQueueMillis()).thenReturn(maxWaitOnFullQueue);
        Mockito.when(flow.getWaitOnEmptyPublishQueueMillis()).thenReturn(maxWaitOnEmptyQueue);
        Mockito.when(flow.getMaxBufferAgeMillis()).thenReturn(maxBufferAge);
    }

    private PublishingQueue<FirehoseRecord> getTestQueue() {
        return new PublishingQueue<>(flow, capacity);
    }

    private List<FirehoseRecord> getTestRecords(int n) {
        List<FirehoseRecord> result = new ArrayList<>(n);
        for(int i = 0; i < n; ++i)
            result.add(getTestRecord(flow));
        return result;
    }

    private RecordBuffer<FirehoseRecord> queueAndFlushBuffer(PublishingQueue<FirehoseRecord> q) {
        int n = ThreadLocalRandom.current().nextInt(1, 100);
        List<FirehoseRecord> records = getTestRecords(n);
        // SANITYCHECK
        assertEquals(q.pendingRecords(), 0);
        for(FirehoseRecord record : records)
            assertTrue(q.offerRecord(record, false));
        // SANITYCHECK
        assertEquals(q.pendingRecords(), n);
        return q.flushPendingRecords();
    }

    @Test
    public void testDoesNotQueueEmptyBuffers() throws Exception {
        PublishingQueue<FirehoseRecord> q = getTestQueue();
        Assert.assertEquals(q.pendingRecords(), 0);
        // Even after waiting for buffer to expire
        Thread.sleep(maxBufferAge + 100);
        q.flushPendingRecords();
        Assert.assertEquals(q.pendingRecords(), 0);
        Assert.assertEquals(q.size(), 0);
    }

    @Test
    public void testQueuingOnCheckPendingRecordsCall() throws Exception {
        PublishingQueue<FirehoseRecord> q = getTestQueue();
        // Insert single record and wait
        FirehoseRecord record = getTestRecord(flow);
        Assert.assertTrue(q.offerRecord(record));

        // Assert call triggers queueing of pending records
        Thread.sleep(maxBufferAge + 100);
        assertTrue(q.checkPendingRecords());
        assertEquals(q.size(), 1);
        assertEquals(q.pendingRecords(), 0);
        assertEquals(q.totalRecords(), 1);
    }

    @Test
    public void testQueuingTriggeredByMaxAge() throws Exception {
        // Buffering triggered by age happens before/after a call to take/tryTake.
        // TODO: Missing tests for tryTake and the case where the queue was full before the take/tryTake call was made
        PublishingQueue<FirehoseRecord> q = getTestQueue();
        // Insert single record and wait
        FirehoseRecord record = getTestRecord(flow);
        Assert.assertTrue(q.offerRecord(record));
        Thread.sleep(maxBufferAge + 100);
        Assert.assertEquals(q.size(), 0);
        Assert.assertEquals(q.pendingRecords(), 1);
        Assert.assertEquals(q.totalRecords(), 1);

        // Now a call to take will cause the buffer to be queued and returned
        RecordBuffer<FirehoseRecord> buffer = q.take();
        Assert.assertNotNull(buffer);
        Assert.assertEquals(buffer.sizeRecords(), 1);
        Assert.assertEquals(q.size(), 0);
        Assert.assertEquals(q.pendingRecords(), 0);
        Assert.assertEquals(q.totalRecords(), 0);
    }

    @Test
    public void testQueuingTriggeredByMaxBytes() throws IOException {
        PublishingQueue<FirehoseRecord> q = getTestQueue();
        final int recordSize = flow.getMaxRecordSizeBytes();
        final double recordSizeJitter = 0.0;
        // Put records in queue
        Stopwatch timer = Stopwatch.createStarted();
        RecordGenerator generator = new RecordGenerator(recordSize, recordSizeJitter);
        long totalBytes = 0;
        int recordCount = 0;
        while(totalBytes + recordSize <= flow.getMaxBufferSizeBytes()) {
            FirehoseRecord record = getTestRecord(flow, generator);
            Assert.assertTrue(q.offerRecord(record));
            ++recordCount;
            totalBytes += record.lengthWithOverhead();
        }
        // SANITYCHECK: We shouldn't have any buffers yet, assuming this didn't
        //              take longer than max buffer age, and we didn't exceed
        //              the max buffer size in bytes
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < flow.getMaxBufferAgeMillis());
        Assert.assertTrue(totalBytes <= flow.getMaxBufferSizeBytes());
        Assert.assertTrue(recordCount <= flow.getMaxBufferSizeRecords());

        Assert.assertEquals(q.size(), 0);
        Assert.assertEquals(q.pendingRecords(), recordCount);
        Assert.assertEquals(q.totalRecords(), recordCount);

        // The next record should cause the buffer to spill and be added to the queue
        FirehoseRecord record = getTestRecord(flow, generator);
        Assert.assertTrue(q.offerRecord(record));

        // Assert it happened and all is as expected
        Assert.assertEquals(q.size(), 1);
        Assert.assertEquals(q.totalRecords(), recordCount + 1);

        // Make sure the max buffer size was not exceeded
        Assert.assertTrue(q.take().sizeBytesWithOverhead() <= flow.getMaxBufferSizeBytes());
    }

    @Test
    public void testQueuingTriggeredByMaxRecords() throws IOException {
        PublishingQueue<FirehoseRecord> q = getTestQueue();
        Stopwatch timer = Stopwatch.createStarted();

        // Make sure we don't exceed the max batch limits
        int recordCount = flow.getMaxBufferSizeRecords() - 1;
        final int recordSize = flow.getMaxRecordSizeBytes() / recordCount / 2;
        final double recordSizeJitter = 0.0;
        RecordGenerator generator = new RecordGenerator(recordSize, recordSizeJitter);
        long totalBytes = 0;
        for(int i = 0; i < recordCount; ++i) {
            FirehoseRecord record = getTestRecord(flow, generator);
            Assert.assertTrue(q.offerRecord(record));
            totalBytes += record.lengthWithOverhead();
        }
        // SANITYCHECK: We shouldn't have any buffers yet, assuming this didn't
        //              take longer than max buffer age, and we didn't exceed
        //              the max buffer size in bytes
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < flow.getMaxBufferAgeMillis());
        Assert.assertTrue(totalBytes < flow.getMaxBufferSizeBytes());

        Assert.assertEquals(q.size(), 0);
        Assert.assertEquals(q.pendingRecords(), recordCount);
        Assert.assertEquals(q.totalRecords(), recordCount);

        // The next record should cause the buffer to be full but will not be added to the queue yet
        FirehoseRecord record = getTestRecord(flow, generator);
        Assert.assertTrue(q.offerRecord(record));
        Assert.assertEquals(q.size(), 0);
        Assert.assertEquals(q.pendingRecords(), recordCount + 1);
        Assert.assertEquals(q.totalRecords(), recordCount + 1);

        // One more record will trigger the previous buffer being queued
        record = getTestRecord(flow, generator);
        Assert.assertTrue(q.offerRecord(record));
        Assert.assertEquals(q.size(), 1);
        Assert.assertEquals(q.pendingRecords(), 1);
        Assert.assertEquals(q.totalRecords(), recordCount + 2);
    }

    @Test
    public void testFlushSinglePendingRecord() throws IOException {
        PublishingQueue<FirehoseRecord> q = getTestQueue();

        // Make sure we don't exceed the max batch limits
        FirehoseRecord record = getTestRecord(flow);
        Assert.assertTrue(q.offerRecord(record));

        Assert.assertEquals(q.size(), 0);
        Assert.assertEquals(q.pendingRecords(), 1);
        Assert.assertEquals(q.totalRecords(), 1);

        // Now flush the temp buffer
        q.flushPendingRecords();
        Assert.assertEquals(q.size(), 1);
        Assert.assertEquals(q.pendingRecords(), 0);
        Assert.assertEquals(q.totalRecords(), 1);
    }

    @Test
    public void testFlushPendingRecords() throws IOException {
        PublishingQueue<FirehoseRecord> q = getTestQueue();
        Stopwatch timer = Stopwatch.createStarted();

        // Make sure we don't exceed the max batch limits
        final int recordCount = flow.getMaxBufferSizeRecords() - 1;
        final int recordSize = flow.getMaxRecordSizeBytes() / recordCount / 2;
        final double recordSizeJitter = 0.0;
        RecordGenerator generator = new RecordGenerator(recordSize, recordSizeJitter);
        long totalBytes = 0;
        for(int i = 0; i < recordCount; ++i) {
            FirehoseRecord record = getTestRecord(flow, generator);
            Assert.assertTrue(q.offerRecord(record));
            totalBytes += record.lengthWithOverhead();
        }
        // SANITYCHECK: We shouldn't have any buffers yet, assuming this didn't
        //              take longer than max buffer age, and we didn't exceed
        //              the max buffer size in bytes
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < flow.getMaxBufferAgeMillis());
        Assert.assertTrue(totalBytes < flow.getMaxBufferSizeBytes());

        Assert.assertEquals(q.size(), 0);
        Assert.assertEquals(q.pendingRecords(), recordCount);
        Assert.assertEquals(q.totalRecords(), recordCount);

        // Now flush the temp buffer
        q.flushPendingRecords();
        Assert.assertEquals(q.size(), 1);
        Assert.assertEquals(q.pendingRecords(), 0);
        Assert.assertEquals(q.totalRecords(), recordCount);
    }

    @Test
    public void testDiscardPendingRecords() throws IOException {
        PublishingQueue<FirehoseRecord> q = getTestQueue();

        // Put couple of buffers
        RecordBuffer<FirehoseRecord> buffer1 = queueAndFlushBuffer(q);
        RecordBuffer<FirehoseRecord> buffer2 = queueAndFlushBuffer(q);
        Assert.assertEquals(q.size(), 2);
        int queuedRecords = buffer1.sizeRecords() + buffer2.sizeRecords();
        long queuedBytes = buffer1.sizeBytesWithOverhead() + buffer2.sizeBytesWithOverhead();

        // Now add a few records
        Assert.assertTrue(q.offerRecord(getTestRecord(flow)));
        Assert.assertTrue(q.offerRecord(getTestRecord(flow)));
        Assert.assertTrue(q.offerRecord(getTestRecord(flow)));
        Assert.assertEquals(q.pendingRecords(), 3);
        Assert.assertEquals(q.totalRecords(), 3 + queuedRecords);

        // Discard and validate side effects
        q.discardPendingRecords();
        Assert.assertEquals(q.pendingRecords(), 0);
        Assert.assertEquals(q.size(), 2);
        Assert.assertEquals(q.totalRecords(), queuedRecords);
        Assert.assertEquals(q.totalBytes(), queuedBytes);
    }

    @Test
    public void testDiscardAllRecords() throws IOException {
        PublishingQueue<FirehoseRecord> q = getTestQueue();

        // Put couple of buffers
        RecordBuffer<FirehoseRecord> buffer1 = queueAndFlushBuffer(q);
        RecordBuffer<FirehoseRecord> buffer2 = queueAndFlushBuffer(q);
        Assert.assertEquals(q.size(), 2);
        int queuedRecords = buffer1.sizeRecords() + buffer2.sizeRecords();

        // Now add a few records
        Assert.assertTrue(q.offerRecord(getTestRecord(flow)));
        Assert.assertTrue(q.offerRecord(getTestRecord(flow)));
        Assert.assertTrue(q.offerRecord(getTestRecord(flow)));
        Assert.assertEquals(q.pendingRecords(), 3);
        Assert.assertEquals(q.totalRecords(), 3 + queuedRecords);

        // Discard all records and validate side effects
        q.discardAllRecords();
        Assert.assertEquals(q.pendingRecords(), 0);
        Assert.assertEquals(q.size(), 0);
        Assert.assertEquals(q.totalRecords(), 0);
        Assert.assertEquals(q.totalBytes(), 0);
    }

    @Test(timeOut=TEST_TIMEOUT)
    public void testBlockingOfferRecordWithFullQueue() throws Exception {
        PublishingQueue<FirehoseRecord> q = getTestQueue();

        // Fill the queue up
        Stopwatch timer = Stopwatch.createStarted();
        while(q.size() < q.capacity()) {
            queueAndFlushBuffer(q);
        }
        Assert.assertEquals(q.size(), q.capacity());
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue);

        // Make sure we don't exceed the max batch limits
        timer = Stopwatch.createStarted();
        final int recordCount = flow.getMaxBufferSizeRecords();
        final int recordSize = flow.getMaxRecordSizeBytes() / recordCount / 2;
        final double recordSizeJitter = 0.0;
        RecordGenerator generator = new RecordGenerator(recordSize, recordSizeJitter);
        long totalBytes = 0;
        for(int i = 0; i < recordCount; ++i) {
            FirehoseRecord record = getTestRecord(flow, generator);
            Assert.assertTrue(q.offerRecord(record, true));
            totalBytes += record.lengthWithOverhead();
        }
        // SANITYCHECK: We shouldn't have any buffers yet, assuming this didn't
        //              take longer than max buffer age, and we didn't exceed
        //              the max buffer size in bytes
        Assert.assertTrue(totalBytes < flow.getMaxBufferSizeBytes());
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < flow.getMaxBufferAgeMillis());
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue);

        // Assert record insertions succeeded but no new buffer was queued
        Assert.assertEquals(q.pendingRecords(), recordCount);
        Assert.assertEquals(q.size(), q.capacity());

        // The next record will not be inserted (and will block) since the temp buffer is already full
        FirehoseRecord record = getTestRecord(flow, generator);
        timer = Stopwatch.createStarted();
        Assert.assertFalse(q.offerRecord(record, true));
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) >= maxWaitOnFullQueue);
        // Assert no side-effects
        Assert.assertEquals(q.pendingRecords(), recordCount);
        Assert.assertEquals(q.size(), q.capacity());

        // Freeing-up the queue by a call to take will cause the yet un-queued mature temp buffer to be queued
        Assert.assertNotNull(q.take());
        timer = Stopwatch.createStarted();
        Assert.assertEquals(q.size(), q.capacity());
        Assert.assertEquals(q.pendingRecords(), 0);
        Assert.assertTrue(q.offerRecord(record, true));
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue);
        Assert.assertEquals(q.pendingRecords(), 1);
    }


    @Test(timeOut=TEST_TIMEOUT)
    public void testNonBlockingOfferRecordWithFullQueue() throws Exception {
        PublishingQueue<FirehoseRecord> q = getTestQueue();

        // Fill the queue up
        Stopwatch timer = Stopwatch.createStarted();
        while(q.size() < q.capacity()) {
            queueAndFlushBuffer(q);
        }
        Assert.assertEquals(q.size(), q.capacity());
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue);

        // Make sure we don't exceed the max batch limits
        timer = Stopwatch.createStarted();
        final int recordCount = flow.getMaxBufferSizeRecords();
        final int recordSize = flow.getMaxRecordSizeBytes() / recordCount / 2;
        final double recordSizeJitter = 0.0;
        RecordGenerator generator = new RecordGenerator(recordSize, recordSizeJitter);
        long totalBytes = 0;
        for(int i = 0; i < recordCount; ++i) {
            FirehoseRecord record = getTestRecord(flow, generator);
            Assert.assertTrue(q.offerRecord(record, false));
            totalBytes += record.lengthWithOverhead();
        }
        // SANITYCHECK: We shouldn't have any buffers yet, assuming this didn't
        //              take longer than max buffer age, and we didn't exceed
        //              the max buffer size in bytes
        Assert.assertTrue(totalBytes < flow.getMaxBufferSizeBytes());
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < flow.getMaxBufferAgeMillis());
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue);

        // Assert record insertions succeeded but no new buffer was queued
        Assert.assertEquals(q.pendingRecords(), recordCount);
        Assert.assertEquals(q.size(), q.capacity());

        // The next record will not be inserted since the temp buffer is already full
        FirehoseRecord record = getTestRecord(flow, generator);
        timer = Stopwatch.createStarted();
        Assert.assertFalse(q.offerRecord(record, false));
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue);
        // Assert no side-effects
        Assert.assertEquals(q.pendingRecords(), recordCount);
        Assert.assertEquals(q.size(), q.capacity());

        // Freeing-up the queue by a call to take will cause the yet un-queued mature temp buffer to be queued
        Assert.assertNotNull(q.take());
        timer = Stopwatch.createStarted();
        Assert.assertEquals(q.size(), q.capacity());
        Assert.assertEquals(q.pendingRecords(), 0);
        Assert.assertTrue(q.offerRecord(record, false));
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue);
        Assert.assertEquals(q.pendingRecords(), 1);
    }

//    @Test(timeOut=TEST_TIMEOUT)
//    public void testOffer() throws Exception {
//        PublishingQueue<FirehoseRecord> q = getTestQueue();
//        Stopwatch timer = Stopwatch.createStarted();
//        while(q.size() < q.capacity() - 1) {
//            queueAndFlushBuffer(q);
//        }
//
//        // This buffer will get us to capacity
//        Assert.assertEquals(q.size(), q.capacity() - 1);
//        Assert.assertTrue(q.offer(getTestBuffer(flow)));
//        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue );
//
//        // Now we're at capacity, the next call will block for at least maxWaitOnFullQueue and return false eventually
//        timer = Stopwatch.createStarted();
//        Assert.assertEquals(q.size(), q.capacity());
//        Assert.assertFalse(q.offer(getTestBuffer(flow)));
//        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) >= maxWaitOnFullQueue );
//
//        // Free up the queue and try again...
//        Assert.assertNotNull(q.take());
//        timer = Stopwatch.createStarted();
//        Assert.assertEquals(q.size(), q.capacity() - 1);
//        Assert.assertTrue(q.offer(getTestBuffer(flow)));
//        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue );
//    }
//
//    @Test
//    public void testTryOffer() throws Exception {
//        PublishingQueue<FirehoseRecord> q = getTestQueue();
//        Stopwatch timer = Stopwatch.createStarted();
//        while(q.size() < q.capacity() - 1) {
//            Assert.assertTrue(q.tryOffer(getTestBuffer(flow)));
//        }
//
//        // This buffer will get us to capacity
//        Assert.assertEquals(q.size(), q.capacity() - 1);
//        Assert.assertTrue(q.tryOffer(getTestBuffer(flow)));
//        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue );
//
//        // Now we're at capacity, the next call will return false immediately
//        timer = Stopwatch.createStarted();
//        Assert.assertEquals(q.size(), q.capacity());
//        Assert.assertFalse(q.tryOffer(getTestBuffer(flow)));
//        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue );
//
//        // Free up the queue and try again...
//        Assert.assertNotNull(q.take());
//        timer = Stopwatch.createStarted();
//        Assert.assertEquals(q.size(), q.capacity() - 1);
//        Assert.assertTrue(q.tryOffer(getTestBuffer(flow)));
//        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue );
//    }
//
    @Test(timeOut=TEST_TIMEOUT)
    public void testOfferForRetry() throws Exception {
        PublishingQueue<FirehoseRecord> q = getTestQueue();
        // Add few regular buffers
        queueAndFlushBuffer(q);
        queueAndFlushBuffer(q);
        int size = q.size();
        int totalRecords = q.totalRecords();
        long totalBytes = q.totalBytes();

        // Now offer for retry... should not block and should cause size() to go over capacity
        RecordBuffer<FirehoseRecord> retryBuffer1 = getTestBuffer(flow);
        q.queueBufferForRetry(retryBuffer1);
        Assert.assertEquals(q.size(), size + 1);
        Assert.assertEquals(q.totalRecords(), totalRecords + retryBuffer1.sizeRecords());
        Assert.assertEquals(q.totalBytes(), totalBytes + retryBuffer1.sizeBytesWithOverhead());

        // Do it again, same thing...
        queueAndFlushBuffer(q);
        queueAndFlushBuffer(q);
        size = q.size();
        totalRecords = q.totalRecords();
        totalBytes = q.totalBytes();
        RecordBuffer<FirehoseRecord> retryBuffer2 = getTestBuffer(flow);
        q.queueBufferForRetry(retryBuffer2);
        Assert.assertEquals(q.size(), size + 1);
        Assert.assertEquals(q.totalRecords(), totalRecords + retryBuffer2.sizeRecords());
        Assert.assertEquals(q.totalBytes(), totalBytes + retryBuffer2.sizeBytesWithOverhead());
    }

    @Test(timeOut=TEST_TIMEOUT)
    public void testOfferForRetryWithFullQueue() throws Exception {
        PublishingQueue<FirehoseRecord> q = getTestQueue();
        Stopwatch timer = Stopwatch.createStarted();
        // Fill up the queue
        while(q.size() < q.capacity()) {
            queueAndFlushBuffer(q);
        }

        // Now offer for retry... should not block and should cause size() to go over capacity
        q.queueBufferForRetry(getTestBuffer(flow));
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue );
        Assert.assertEquals(q.size(), q.capacity() + 1);

        // Next call, same thing...
        timer = Stopwatch.createStarted();
        q.queueBufferForRetry(getTestBuffer(flow));
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue );
        Assert.assertEquals(q.size(), q.capacity() + 2);
    }

    @Test(timeOut=TEST_TIMEOUT)
    public void testBlockingTake() throws Exception {
        PublishingQueue<FirehoseRecord> q = getTestQueue();
        // Put couple of buffers
        RecordBuffer<FirehoseRecord> buffer1 = queueAndFlushBuffer(q);
        RecordBuffer<FirehoseRecord> buffer2 = queueAndFlushBuffer(q);
        int totalRecords = q.totalRecords();
        long totalBytes = q.totalBytes();

        // Following two calls will return immediately... take() returns buffers in FIFO order.
        Assert.assertEquals(q.size(), 2);
        Stopwatch timer = Stopwatch.createStarted();
        Assert.assertSame(q.take(true), buffer1);
        Assert.assertEquals(q.size(), 1);
        Assert.assertEquals(q.totalRecords(), totalRecords - buffer1.sizeRecords());
        Assert.assertEquals(q.totalBytes(), totalBytes - buffer1.sizeBytesWithOverhead());
        Assert.assertSame(q.take(true), buffer2);
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnEmptyQueue);
        Assert.assertEquals(q.size(), 0);
        Assert.assertEquals(q.totalRecords(), 0);
        Assert.assertEquals(q.totalBytes(), 0);

        // The next call will block and eventually return null
        timer = Stopwatch.createStarted();
        Assert.assertNull(q.take(true));
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) >= maxWaitOnEmptyQueue);

        // Now add another buffer and try again
        RecordBuffer<FirehoseRecord> buffer3 = queueAndFlushBuffer(q);
        Assert.assertEquals(q.size(), 1);
        Assert.assertEquals(q.totalRecords(), buffer3.sizeRecords());
        Assert.assertEquals(q.totalBytes(), buffer3.sizeBytesWithOverhead());
        timer = Stopwatch.createStarted();
        Assert.assertSame(q.take(true), buffer3);
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnEmptyQueue);
        Assert.assertEquals(q.totalRecords(), 0);
        Assert.assertEquals(q.totalBytes(), 0);
    }

    @Test(timeOut=TEST_TIMEOUT)
    public void testNonBlockingTake() throws Exception {
        PublishingQueue<FirehoseRecord> q = getTestQueue();
        // Put couple of buffers
        RecordBuffer<FirehoseRecord> buffer1 = queueAndFlushBuffer(q);
        RecordBuffer<FirehoseRecord> buffer2 = queueAndFlushBuffer(q);
        int totalRecords = q.totalRecords();
        long totalBytes = q.totalBytes();

        // Following two calls will return immediately... take() returns buffers in FIFO order.
        Assert.assertEquals(q.size(), 2);
        Stopwatch timer = Stopwatch.createStarted();
        Assert.assertSame(q.take(false), buffer1);
        Assert.assertEquals(q.size(), 1);
        Assert.assertEquals(q.totalRecords(), totalRecords - buffer1.sizeRecords());
        Assert.assertEquals(q.totalBytes(), totalBytes - buffer1.sizeBytesWithOverhead());
        Assert.assertSame(q.take(false), buffer2);
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnEmptyQueue);
        Assert.assertEquals(q.size(), 0);
        Assert.assertEquals(q.totalRecords(), 0);
        Assert.assertEquals(q.totalBytes(), 0);

        // The next call will not block and will return null immediately
        timer = Stopwatch.createStarted();
        Assert.assertNull(q.take(false));
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnEmptyQueue);

        // Now add another buffer and try again
        RecordBuffer<FirehoseRecord> buffer3 = queueAndFlushBuffer(q);
        Assert.assertEquals(q.size(), 1);
        Assert.assertEquals(q.totalRecords(), buffer3.sizeRecords());
        Assert.assertEquals(q.totalBytes(), buffer3.sizeBytesWithOverhead());
        timer = Stopwatch.createStarted();
        Assert.assertSame(q.take(false), buffer3);
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnEmptyQueue);
        Assert.assertEquals(q.totalRecords(), 0);
        Assert.assertEquals(q.totalBytes(), 0);
    }

    @Test(timeOut=TEST_TIMEOUT)
    public void testTakeWithRetryBuffers() throws Exception {
        PublishingQueue<FirehoseRecord> q = getTestQueue();
        // Put couple of regular (never-published) buffers
        RecordBuffer<FirehoseRecord> buffer1 = queueAndFlushBuffer(q);
        RecordBuffer<FirehoseRecord> buffer2 = queueAndFlushBuffer(q);
        Assert.assertEquals(q.size(), 2);

        // Then put a couple of retry buffers
        RecordBuffer<FirehoseRecord> retryBuffer1 = getTestBuffer(flow);
        RecordBuffer<FirehoseRecord> retryBuffer2 = getTestBuffer(flow);
        q.queueBufferForRetry(retryBuffer1);
        q.queueBufferForRetry(retryBuffer2);
        Assert.assertEquals(q.size(), 4);

        // Put couple more regular (never-published) buffers
        RecordBuffer<FirehoseRecord> buffer3 = queueAndFlushBuffer(q);
        RecordBuffer<FirehoseRecord> buffer4 = queueAndFlushBuffer(q);
        Assert.assertEquals(q.size(), 6);

        // Following two calls will return immediately with retry buffers in FIFO order.
        Stopwatch timer = Stopwatch.createStarted();
        Assert.assertSame(q.take(), retryBuffer1);
        Assert.assertSame(q.take(), retryBuffer2);
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnEmptyQueue);
        Assert.assertEquals(q.size(), 4);

        // Following two calls will return immediately with regular buffers in FIFO order.
        timer = Stopwatch.createStarted();
        Assert.assertSame(q.take(), buffer1);
        Assert.assertSame(q.take(), buffer2);
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnEmptyQueue);
        Assert.assertEquals(q.size(), 2);

        // Then put a couple more retry buffers and see that they take precedence
        RecordBuffer<FirehoseRecord> retryBuffer3 = getTestBuffer(flow);
        RecordBuffer<FirehoseRecord> retryBuffer4 = getTestBuffer(flow);
        q.queueBufferForRetry(retryBuffer3);
        q.queueBufferForRetry(retryBuffer4);
        Assert.assertEquals(q.size(), 4);
        timer = Stopwatch.createStarted();
        Assert.assertSame(q.take(), retryBuffer3);
        Assert.assertSame(q.take(), retryBuffer4);
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnEmptyQueue);
        Assert.assertEquals(q.size(), 2);

        // Following two calls will return immediately with remaining regular buffers in FIFO order.
        timer = Stopwatch.createStarted();
        Assert.assertSame(q.take(), buffer3);
        Assert.assertSame(q.take(), buffer4);
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnEmptyQueue);
        Assert.assertEquals(q.size(), 0);
        Assert.assertEquals(q.totalRecords(), 0);
        Assert.assertEquals(q.totalBytes(), 0);

        // The next call will block and eventually return null
        timer = Stopwatch.createStarted();
        Assert.assertEquals(q.size(), 0);
        Assert.assertNull(q.take());
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) >= maxWaitOnEmptyQueue);

        // Now add another retry buffer and try again
        RecordBuffer<FirehoseRecord> retryBuffer5 = getTestBuffer(flow);
        q.queueBufferForRetry(retryBuffer5);
        timer = Stopwatch.createStarted();
        Assert.assertSame(q.take(), retryBuffer5);
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnEmptyQueue);
    }

    @Test
    public void testTakeAfterClose() throws Exception {
        PublishingQueue<FirehoseRecord> q = getTestQueue();
        // Put couple of buffers
        RecordBuffer<FirehoseRecord> buffer1 = queueAndFlushBuffer(q);
        RecordBuffer<FirehoseRecord> buffer2 = queueAndFlushBuffer(q);

        // Close the queue, and check that behavior is as expected
        q.close();

        // Following two calls will return immediately... take() returns buffers in FIFO order.
        Assert.assertEquals(q.size(), 2);
        Stopwatch timer = Stopwatch.createStarted();
        Assert.assertSame(q.take(), buffer1);
        Assert.assertSame(q.take(), buffer2);
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnEmptyQueue);

        // The next call will return immediatly with a null
        timer = Stopwatch.createStarted();
        Assert.assertEquals(q.size(), 0);
        Assert.assertNull(q.take());
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnEmptyQueue);
    }

    @Test
    public void testOfferRecordAfterClose() throws Exception {
        PublishingQueue<FirehoseRecord> q = getTestQueue();

        // Fill the queue up
        Stopwatch timer = Stopwatch.createStarted();
        while(q.size() < q.capacity()) {
            queueAndFlushBuffer(q);
        }
        Assert.assertEquals(q.size(), q.capacity());
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue);

        // Make sure we don't exceed the max batch limits
        timer = Stopwatch.createStarted();
        final int recordCount = flow.getMaxBufferSizeRecords();
        final int recordSize = flow.getMaxRecordSizeBytes() / recordCount / 2;
        final double recordSizeJitter = 0.0;
        RecordGenerator generator = new RecordGenerator(recordSize, recordSizeJitter);
        long totalBytes = 0;
        for(int i = 0; i < recordCount; ++i) {
            FirehoseRecord record = getTestRecord(flow, generator);
            Assert.assertTrue(q.offerRecord(record));
            totalBytes += record.lengthWithOverhead();
        }
        // SANITYCHECK: We shouldn't have any buffers yet, assuming this didn't
        //              take longer than max buffer age, and we didn't exceed
        //              the max buffer size in bytes
        Assert.assertTrue(totalBytes < flow.getMaxBufferSizeBytes());
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < flow.getMaxBufferAgeMillis());
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue);

        // Assert record insertions succeeded but no new buffer was queued
        Assert.assertEquals(q.pendingRecords(), recordCount);
        Assert.assertEquals(q.size(), q.capacity());

        // Close the queue and check that behavior is as expected
        q.close();

        // The next record will not be inserted (and will not block)
        FirehoseRecord record = getTestRecord(flow, generator);
        timer = Stopwatch.createStarted();
        Assert.assertFalse(q.offerRecord(record));
        Assert.assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) < maxWaitOnFullQueue);

        // Assert no side-effects
        Assert.assertEquals(q.pendingRecords(), recordCount);
        Assert.assertEquals(q.size(), q.capacity());
    }

    @Test(enabled=false)
    public void testTakeReturnsImmediatelyWhenNewBufferAdded() {
        // TODO: wait on #take, then call #offer and see that #take
        //       returns immediately afterwards
    }

    @Test(enabled=false)
    public void testTakeReturnsImmediatelyWhenRetryBufferAdded() {
        // TODO: wait on #take, then call #offerForRetry and see that
        //       #take returns immediately afterwards
    }

    @Test(enabled=false)
    public void testTakeReturnsImmediatelyWhenClosed() {
        // TODO: wait on #take, then call #close() and see that
        //       #take returns immediately afterwards
    }

    @Test(enabled=false)
    public void testTakeReturnsImmediatelyWhenEnoughRecordsAdded() {
        // TODO: wait on #take(), then call #offerRecord() multiple times and
        //       see that #take() returns when temp buffer is queued
    }

    @Test(enabled=false)
    public void testTotalRecordsAndBytes() {
        // TODO: call #offer, #offerRecord, #offerForRetry and see that record
        //       and byte counters are incremented. Then call #take and see that
        //       they are decremented accordingly.
    }
}
