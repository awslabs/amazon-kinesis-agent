/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.RandomStringUtils;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.tailing.BufferSendResult;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.KinesisConstants;
import com.amazon.kinesis.streaming.agent.tailing.KinesisFileFlow;
import com.amazon.kinesis.streaming.agent.tailing.KinesisRecord;
import com.amazon.kinesis.streaming.agent.tailing.KinesisSender;
import com.amazon.kinesis.streaming.agent.tailing.RecordBuffer;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.tailing.KinesisConstants.PartitionKeyOption;
import com.amazon.kinesis.streaming.agent.tailing.testing.RecordGenerator;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class KinesisSenderTest extends TailingTestBase {
    public static final PartitionKeyOption partitionKeyOption = PartitionKeyOption.DETERMINISTIC;
    
    protected AgentContext context;
    protected KinesisFileFlow flow;

    @BeforeMethod
    public void setup() throws IOException {
        context = getTestKinesisAgentContext();
        flow = (KinesisFileFlow) context.flows().get(0);
    }
    
    protected PutRecordsResult answerKinesisPutRecords(InvocationOnMock invocation, final RecordBuffer<KinesisRecord> testBuffer, final int[] failedRecords) {
        PutRecordsRequest request = (PutRecordsRequest) invocation.getArguments()[0];
        Assert.assertEquals(request.getRecords().size(), testBuffer.sizeRecords());
        PutRecordsResult result = new PutRecordsResult();
        result.setFailedRecordCount(failedRecords.length);
        List<PutRecordsResultEntry> responseEntries = new ArrayList<>(request.getRecords().size());
        int i = 0, j = 0;
        for(KinesisRecord inputRecord : testBuffer) {
            PutRecordsResultEntry responseEntry = new PutRecordsResultEntry();
            PutRecordsRequestEntry entry = request.getRecords().get(i);
            Assert.assertNotNull(entry.getPartitionKey());
            byte[] requestData = entry.getData().array();
            byte[] inputData = inputRecord.data().array();
            Assert.assertEquals(requestData, inputData);
            if(j < failedRecords.length && i == failedRecords[j]) {
                responseEntry.setErrorCode("503");
                responseEntry.setErrorCode("An error occurred.");
                ++j;
            }
            responseEntry.setSequenceNumber(ByteBuffers.toString(entry.getData(), StandardCharsets.UTF_8));
            responseEntries.add(responseEntry);
            ++i;
        }
        Assert.assertEquals(j, failedRecords.length);
        result.setRecords(responseEntries);
        return result;
    }
    
    @DataProvider(name="sendData")
    public Object[][] sendData() {
        return new Object[][] {
        		{10, new int[]{}, BufferSendResult.Status.SUCCESS}, // no record failed
                {10, new int[] {2,4,6,8,9}, BufferSendResult.Status.PARTIAL_SUCCESS},
                {5, new int[] {0,1,2,3,4}, BufferSendResult.Status.PARTIAL_SUCCESS}  // all records failed
        };
    }

    @Test(dataProvider="sendData")
    public void testSend(final int recordCount, final int[] failedRecords, final BufferSendResult.Status expectedStatus) {
    	if (failedRecords.length > 0)
    	    Arrays.sort(failedRecords);
        final RecordBuffer<KinesisRecord> testBuffer = getTestKinesisBuffer(flow, recordCount);
        AmazonKinesisClient kinesisClient = Mockito.mock(AmazonKinesisClient.class);
        Mockito.when(kinesisClient.putRecords(Mockito.any(PutRecordsRequest.class))).then(new Answer<PutRecordsResult>() {
            @Override
            public PutRecordsResult answer(InvocationOnMock invocation) throws Throwable {
                return answerKinesisPutRecords(invocation, testBuffer, failedRecords);
            }
        });
        context = Mockito.spy(context);
        Mockito.when(context.getKinesisClient()).thenReturn(kinesisClient);
        KinesisSender sender = new KinesisSender(context, flow);
        List<KinesisRecord> recordsInBuffer = Lists.newArrayList(testBuffer);
        BufferSendResult<KinesisRecord> result = sender.sendBuffer(testBuffer);
        Mockito.verify(kinesisClient).putRecords(Mockito.any(PutRecordsRequest.class));
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getStatus(), expectedStatus);
        Assert.assertEquals(result.getBuffer().id(), testBuffer.id());
        Assert.assertEquals(result.remainingRecordCount(), failedRecords.length);
        if (failedRecords.length > 0) {
            for(KinesisRecord remainingRecord : result.getBuffer()) {
                int indexInOriginal = recordsInBuffer.indexOf(remainingRecord);
                boolean found = Arrays.binarySearch(failedRecords, indexInOriginal) >= 0;
                Assert.assertTrue(found);
            }
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
        final RecordBuffer<KinesisRecord> testBuffer = getTestKinesisBuffer(flow, recordCount);
        final AtomicInteger attempt = new AtomicInteger(0);
        for(int[] failedRecords : successiveFailedRecords)
            Arrays.sort(failedRecords);
        AmazonKinesisClient kinesisClient = Mockito.mock(AmazonKinesisClient.class);
        Mockito.when(kinesisClient.putRecords(Mockito.any(PutRecordsRequest.class))).then(new Answer<PutRecordsResult>() {
            @Override
            public PutRecordsResult answer(InvocationOnMock invocation) throws Throwable {
                try {
                    // When successiveFailedRecords return assume no more failed records
                    int[] failedRecords = attempt.get() < successiveFailedRecords.length ? successiveFailedRecords[attempt.get()] : new int[]{};
                    return answerKinesisPutRecords(invocation, testBuffer, failedRecords);
                } finally {
                    attempt.incrementAndGet();
                }
            }
        });
        context = Mockito.spy(context);
        Mockito.when(context.getKinesisClient()).thenReturn(kinesisClient);
        KinesisSender sender = new KinesisSender(context, flow);
        for(int i = 0; i < successiveFailedRecords.length; ++i) {
            Assert.assertTrue(successiveFailedRecords[i].length > 0, "Failed records array cannot be empty in this test!");
            List<KinesisRecord> recordsInBuffer = Lists.newArrayList(testBuffer);
            BufferSendResult<KinesisRecord> result = sender.sendBuffer(testBuffer);
            Assert.assertNotNull(result);
            Assert.assertEquals(result.getBuffer().id(), testBuffer.id());
            Assert.assertEquals(result.getStatus(), BufferSendResult.Status.PARTIAL_SUCCESS);
            Assert.assertEquals(result.getBuffer().sizeRecords(), successiveFailedRecords[i].length);
            for(KinesisRecord remainingRecord : result.getBuffer()) {
                int indexInOriginal = recordsInBuffer.indexOf(remainingRecord);
                boolean found = Arrays.binarySearch(successiveFailedRecords[i], indexInOriginal) >= 0;
                Assert.assertTrue(found);
            }
        }
        // Since we exhausted the successiveFailedRecords, following call will return full success
        BufferSendResult<KinesisRecord> result = sender.sendBuffer(testBuffer);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getStatus(), BufferSendResult.Status.SUCCESS);
        Assert.assertEquals(result.getBuffer().id(), testBuffer.id());
        Mockito.verify(kinesisClient, Mockito.times(successiveFailedRecords.length + 1)).putRecords(Mockito.any(PutRecordsRequest.class));
    }
    
    @SuppressWarnings("unchecked")
	@Test(expectedExceptions=AmazonServiceException.class)
    public void testSendWithException() {
        final RecordBuffer<KinesisRecord> testBuffer = getTestKinesisBuffer(flow, 10);
        AmazonKinesisClient kinesisClient = Mockito.mock(AmazonKinesisClient.class);
        Mockito.when(kinesisClient.putRecords(Mockito.any(PutRecordsRequest.class))).
            thenThrow(AmazonServiceException.class);
        context = Mockito.spy(context);
        Mockito.when(context.getKinesisClient()).thenReturn(kinesisClient);
        KinesisSender sender = new KinesisSender(context, flow);
        sender.sendBuffer(testBuffer);
    }

    private AgentContext getTestKinesisAgentContext() {
    	Map<String, Object> config = new HashMap<>();
    	List<Configuration> flows = new ArrayList<>();
    	Map<String, Object> flow = new HashMap<>();
        flow.put("filePattern", "/var/log/message.log*");
        flow.put(KinesisConstants.DESTINATION_KEY, RandomStringUtils.randomAlphabetic(5));
        flows.add(new Configuration(flow));
        config.put("flows", flows);
        config.put("checkpointFile", globalTestFiles.getTempFilePath());
        config.put("cloudwatch.emitMetrics", false);
        return TestUtils.getTestAgentContext(config);
    }
    
    private RecordBuffer<KinesisRecord> getTestKinesisBuffer(FileFlow<KinesisRecord> flow, int nrecords) {
    	RecordBuffer<KinesisRecord> buffer = new RecordBuffer<KinesisRecord>(flow);
    	RecordGenerator generator = new RecordGenerator();
        for (int i = 0; i < nrecords; i++) {
            byte[] data = generator.getNewRecord();
            if (sourceFileForTestRecords == null) {
            	try {
                    sourceFileForTestRecordsOffset = 0;
                    sourceFileForTestRecords = new TrackedFile(flow, testFiles.createTempFile());
                } catch (IOException e) {
                    Throwables.propagate(e);
                }
            }
            KinesisRecord record = new KinesisRecord(sourceFileForTestRecords, sourceFileForTestRecordsOffset, data);
            sourceFileForTestRecordsOffset += data.length;
        	buffer.add(record);
        }
        return buffer;
    }
}
