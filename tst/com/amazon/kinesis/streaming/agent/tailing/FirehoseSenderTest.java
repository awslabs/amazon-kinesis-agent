/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.tailing.BufferSendResult;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseFileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseSender;
import com.amazon.kinesis.streaming.agent.tailing.RecordBuffer;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.google.common.collect.Lists;

public class FirehoseSenderTest extends TailingTestBase {
    protected AgentContext context;
    protected FirehoseFileFlow flow;

    @BeforeMethod
    public void setup() throws IOException {
        context = getTestAgentContext();
        flow = (FirehoseFileFlow) context.flows().get(0);
    }

    protected PutRecordBatchResult answerFirehosePutRecordBatch(InvocationOnMock invocation, final RecordBuffer<FirehoseRecord> testBuffer, final int[] failedRecords) {
        PutRecordBatchRequest request = (PutRecordBatchRequest) invocation.getArguments()[0];
        Assert.assertEquals(request.getRecords().size(), testBuffer.sizeRecords());
        PutRecordBatchResult result = new PutRecordBatchResult();
        result.setFailedPutCount(failedRecords.length);
        List<PutRecordBatchResponseEntry> responseEntries = new ArrayList<>(request.getRecords().size());
        int i = 0, j = 0;
        for(FirehoseRecord inputRecord : testBuffer) {
            PutRecordBatchResponseEntry responseEntry = new PutRecordBatchResponseEntry();
            Record entry = request.getRecords().get(i);
            byte[] requestData = entry.getData().array();
            byte[] inputData = inputRecord.data().array();
            Assert.assertEquals(requestData, inputData);
            if(j < failedRecords.length && i == failedRecords[j]) {
                responseEntry.setErrorCode("503");
                responseEntry.setErrorCode("An error occurred.");
                ++j;
            }
            responseEntry.setRecordId(ByteBuffers.toString(entry.getData(), StandardCharsets.UTF_8));
            responseEntries.add(responseEntry);
            ++i;
        }
        Assert.assertEquals(j, failedRecords.length);
        result.setRequestResponses(responseEntries);
        return result;
    }

    @Test
    public void testSendWithSuccess() {
        final RecordBuffer<FirehoseRecord> testBuffer = getTestBuffer(flow, 10);
        AmazonKinesisFirehose firehose = Mockito.mock(AmazonKinesisFirehose.class);
        Mockito.when(firehose.putRecordBatch(Mockito.any(PutRecordBatchRequest.class))).then(new Answer<PutRecordBatchResult>() {
            @Override
            public PutRecordBatchResult answer(InvocationOnMock invocation) throws Throwable {
                return answerFirehosePutRecordBatch(invocation, testBuffer, new int[]{});
            }
        });
        context = Mockito.spy(context);
        Mockito.when(context.getFirehoseClient()).thenReturn(firehose);
        FirehoseSender sender = new FirehoseSender(context, flow);
        BufferSendResult<FirehoseRecord> result = sender.sendBuffer(testBuffer);
        Mockito.verify(firehose).putRecordBatch(Mockito.any(PutRecordBatchRequest.class));
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getStatus(), BufferSendResult.Status.SUCCESS);
        Assert.assertEquals(result.getBuffer().id(), testBuffer.id());
    }

    @DataProvider(name="sendWithPartialFailure")
    public Object[][] sendWithPartialFailureData() {
        return new Object[][] {
                {10, new int[] {2,4,6,8,9}, BufferSendResult.Status.PARTIAL_SUCCESS},
                {5, new int[] {0,1,2,3,4}, BufferSendResult.Status.PARTIAL_SUCCESS}  // all records failed
        };
    }

    @Test(dataProvider="sendWithPartialFailure")
    public void testSendWithPartialFailure(final int recordCount, final int[] failedRecords, final BufferSendResult.Status expectedStatus) {
        Arrays.sort(failedRecords);
        Assert.assertTrue(failedRecords.length > 0, "Failed records array cannot be empty in this test!");
        final RecordBuffer<FirehoseRecord> testBuffer = getTestBuffer(flow, recordCount);
        AmazonKinesisFirehose firehose = Mockito.mock(AmazonKinesisFirehose.class);
        Mockito.when(firehose.putRecordBatch(Mockito.any(PutRecordBatchRequest.class))).then(new Answer<PutRecordBatchResult>() {
            @Override
            public PutRecordBatchResult answer(InvocationOnMock invocation) throws Throwable {
                return answerFirehosePutRecordBatch(invocation, testBuffer, failedRecords);
            }
        });
        context = Mockito.spy(context);
        Mockito.when(context.getFirehoseClient()).thenReturn(firehose);
        FirehoseSender sender = new FirehoseSender(context, flow);
        List<FirehoseRecord> recordsInBuffer = Lists.newArrayList(testBuffer);
        BufferSendResult<FirehoseRecord> result = sender.sendBuffer(testBuffer);
        Mockito.verify(firehose).putRecordBatch(Mockito.any(PutRecordBatchRequest.class));
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getBuffer().id(), testBuffer.id());
        Assert.assertEquals(result.getStatus(), expectedStatus);
        Assert.assertEquals(result.getBuffer().sizeRecords(), failedRecords.length);
        for(FirehoseRecord remainingRecord : result.getBuffer()) {
            int indexInOriginal = recordsInBuffer.indexOf(remainingRecord);
            boolean found = Arrays.binarySearch(failedRecords, indexInOriginal) >= 0;
            Assert.assertTrue(found);
        }
    }


    @DataProvider(name="retryingPartialFailures")
    public Object[][] retryingPartialFailuresData() {
        return new Object[][] {
                {10, new int[][] {{2,4,6,8,9}, {0,1}, {1}}},
                {5, new int[][] {{0,1,2,3,4}, {0,4}}}
        };
    }

    @Test(dataProvider="retryingPartialFailures")
    public void testRetryingPartialFailures(final int recordCount, final int[][] successiveFailedRecords) {
        final RecordBuffer<FirehoseRecord> testBuffer = getTestBuffer(flow, recordCount);
        final AtomicInteger attempt = new AtomicInteger(0);
        for(int[] failedRecords : successiveFailedRecords)
            Arrays.sort(failedRecords);
        AmazonKinesisFirehose firehose = Mockito.mock(AmazonKinesisFirehose.class);
        Mockito.when(firehose.putRecordBatch(Mockito.any(PutRecordBatchRequest.class))).then(new Answer<PutRecordBatchResult>() {
            @Override
            public PutRecordBatchResult answer(InvocationOnMock invocation) throws Throwable {
                try {
                    // When successiveFailedRecords return assume no more failed records
                    int[] failedRecords = attempt.get() < successiveFailedRecords.length ? successiveFailedRecords[attempt.get()] : new int[]{};
                    return answerFirehosePutRecordBatch(invocation, testBuffer, failedRecords);
                } finally {
                    attempt.incrementAndGet();
                }
            }
        });
        context = Mockito.spy(context);
        Mockito.when(context.getFirehoseClient()).thenReturn(firehose);
        FirehoseSender sender = new FirehoseSender(context, flow);
        for(int i = 0; i < successiveFailedRecords.length; ++i) {
            Assert.assertTrue(successiveFailedRecords[i].length > 0, "Failed records array cannot be empty in this test!");
            List<FirehoseRecord> recordsInBuffer = Lists.newArrayList(testBuffer);
            BufferSendResult<FirehoseRecord> result = sender.sendBuffer(testBuffer);
            Assert.assertNotNull(result);
            Assert.assertEquals(result.getBuffer().id(), testBuffer.id());
            Assert.assertEquals(result.getStatus(), BufferSendResult.Status.PARTIAL_SUCCESS);
            Assert.assertEquals(result.getBuffer().sizeRecords(), successiveFailedRecords[i].length);
            for(FirehoseRecord remainingRecord : result.getBuffer()) {
                int indexInOriginal = recordsInBuffer.indexOf(remainingRecord);
                boolean found = Arrays.binarySearch(successiveFailedRecords[i], indexInOriginal) >= 0;
                Assert.assertTrue(found);
            }
        }
        // Since we exhausted the successiveFailedRecords, following call will return full success
        BufferSendResult<FirehoseRecord> result = sender.sendBuffer(testBuffer);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getStatus(), BufferSendResult.Status.SUCCESS);
        Assert.assertEquals(result.getBuffer().id(), testBuffer.id());
        Mockito.verify(firehose, Mockito.times(successiveFailedRecords.length + 1)).putRecordBatch(Mockito.any(PutRecordBatchRequest.class));
    }

    @SuppressWarnings("unchecked")
    @Test(expectedExceptions=AmazonServiceException.class)
    public void testSendWithException() {
        final int recordCount = 10;
        final RecordBuffer<FirehoseRecord> testBuffer = getTestBuffer(flow, recordCount);
        AmazonKinesisFirehose firehose = Mockito.mock(AmazonKinesisFirehose.class);
        Mockito.when(firehose.putRecordBatch(Mockito.any(PutRecordBatchRequest.class))).
            thenThrow(AmazonServiceException.class);
        context = Mockito.spy(context);
        Mockito.when(context.getFirehoseClient()).thenReturn(firehose);
        FirehoseSender sender = new FirehoseSender(context, flow);
        sender.sendBuffer(testBuffer);
    }

    @Test(enabled=false)
    public void testBatchTooLarge() {
        // TODO
    }
}
