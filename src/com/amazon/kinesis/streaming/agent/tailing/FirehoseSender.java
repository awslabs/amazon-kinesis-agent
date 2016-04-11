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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.metrics.IMetricsScope;
import com.amazon.kinesis.streaming.agent.metrics.Metrics;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class FirehoseSender extends AbstractSender<FirehoseRecord> {
    private static final String SERVICE_ERRORS_METRIC = "ServiceErrors";
    private static final String RECORDS_ATTEMPTED_METRIC = "RecordSendAttempts";
    private static final String RECORD_ERRORS_METRIC = "RecordSendErrors";
    private static final String BYTES_SENT_METRIC = "BytesSent";

    @Getter private final AgentContext agentContext;
    private final FirehoseFileFlow flow;

    private final AtomicLong totalRecordsAttempted = new AtomicLong();
    private final AtomicLong totalRecordsSent = new AtomicLong();
    private final AtomicLong totalRecordsFailed = new AtomicLong();
    private final AtomicLong totalBatchPutCalls = new AtomicLong();
    private final AtomicLong totalBatchPutServiceErrors = new AtomicLong();
    private final AtomicLong totalBatchPutOtherErrors = new AtomicLong();
    private final AtomicLong totalBatchPutLatency = new AtomicLong();
    private final AtomicLong activeBatchPutCalls = new AtomicLong();
    private final Map<String, AtomicLong> totalErrors = new HashMap<>();

    public FirehoseSender(AgentContext agentContext, FirehoseFileFlow flow) {
        Preconditions.checkNotNull(flow);
        this.agentContext = agentContext;
        this.flow = flow;
    }

    @Override
    protected long getMaxSendBatchSizeBytes() {
        return FirehoseConstants.MAX_BATCH_PUT_SIZE_BYTES;
    }

    @Override
    protected int getMaxSendBatchSizeRecords() {
        return FirehoseConstants.MAX_BATCH_PUT_SIZE_RECORDS;
    }

    @Override
    protected BufferSendResult<FirehoseRecord> attemptSend(RecordBuffer<FirehoseRecord> buffer) {
        activeBatchPutCalls.incrementAndGet();
        IMetricsScope metrics = agentContext.beginScope();
        metrics.addDimension(Metrics.DESTINATION_DIMENSION, "DeliveryStream:" + getDestination());
        try {
            BufferSendResult<FirehoseRecord> sendResult = null;
            List<Record> requestRecords = new ArrayList<>();
            for(FirehoseRecord data : buffer) {
                Record record = new Record();
                record.setData(data.data());
                requestRecords.add(record);
            }
            PutRecordBatchRequest request = new PutRecordBatchRequest();
            request.setRecords(requestRecords);
            request.setDeliveryStreamName(getDestination());
            PutRecordBatchResult result = null;
            Stopwatch timer = Stopwatch.createStarted();
            totalBatchPutCalls.incrementAndGet();
            try {
                logger.trace("{}: Sending buffer {} to firehose {}...", flow.getId(), buffer, getDestination());
                metrics.addCount(RECORDS_ATTEMPTED_METRIC, requestRecords.size());
                result = agentContext.getFirehoseClient().putRecordBatch(request);
                metrics.addCount(SERVICE_ERRORS_METRIC, 0);
            } catch (AmazonServiceException e) {
                metrics.addCount(SERVICE_ERRORS_METRIC, 1);
                totalBatchPutServiceErrors.incrementAndGet();
                throw e;
            } catch (Exception e) {
                metrics.addCount(SERVICE_ERRORS_METRIC, 1);
                totalBatchPutOtherErrors.incrementAndGet();
                throw e;
            } finally {
                totalBatchPutLatency.addAndGet(timer.elapsed(TimeUnit.MILLISECONDS));
            }
            if(sendResult == null) {
                List<Integer> sentRecords = new ArrayList<>(requestRecords.size());
                Multiset<String> errors = HashMultiset.<String> create();
                int index = 0;
                long totalBytesSent = 0;
                for (PutRecordBatchResponseEntry responseEntry : result.getRequestResponses()) {
                    Record record = requestRecords.get(index);
                    if (responseEntry.getErrorCode() == null) {
                        sentRecords.add(index);
                        totalBytesSent += record.getData().limit();
                    } else {
                        logger.trace("{}:{} Record {} returned error code {}: {}", flow.getId(), buffer, index, responseEntry.getErrorCode(),
                                responseEntry.getErrorMessage());
                        errors.add(responseEntry.getErrorCode());
                    }
                    ++index;
                }
                if(sentRecords.size() == requestRecords.size()) {
                    sendResult = BufferSendResult.succeeded(buffer);
                } else {
                    buffer = buffer.remove(sentRecords);
                    sendResult = BufferSendResult.succeeded_partially(buffer, requestRecords.size());
                }
                metrics.addData(BYTES_SENT_METRIC, totalBytesSent, StandardUnit.Bytes);
                int failedRecordCount = requestRecords.size() - sentRecords.size();
                metrics.addCount(RECORD_ERRORS_METRIC, failedRecordCount);
                logger.debug("{}:{} Records sent firehose {}: {}. Failed records: {}",
                        flow.getId(),
                        buffer,
                        getDestination(),
                        sentRecords.size(),
                        failedRecordCount);
                totalRecordsAttempted.addAndGet(requestRecords.size());
                totalRecordsSent.addAndGet(sentRecords.size());
                totalRecordsFailed.addAndGet(failedRecordCount);

                if(logger.isDebugEnabled() && !errors.isEmpty()) {
                    synchronized(totalErrors) {
                        StringBuilder strErrors = new StringBuilder();
                        for(Multiset.Entry<String> err : errors.entrySet()) {
                            AtomicLong counter = totalErrors.get(err.getElement());
                            if (counter == null)
                                totalErrors.put(err.getElement(), counter = new AtomicLong());
                            counter.addAndGet(err.getCount());
                            if(strErrors.length() > 0)
                                strErrors.append(", ");
                            strErrors.append(err.getElement()).append(": ").append(err.getCount());
                        }
                        logger.debug("{}:{} Errors from firehose {}: {}", flow.getId(), buffer, flow.getDestination(), strErrors.toString());
                    }
                }
            }
            return sendResult;
        } finally {
            metrics.commit();
            activeBatchPutCalls.decrementAndGet();
        }
    }

    @Override
    public String getDestination() {
        return flow.getDestination();
    }

    @SuppressWarnings("serial")
    @Override
    public Map<String, Object> getMetrics() {
        return new HashMap<String, Object>() {{
            put("FirehoseSender.TotalRecordsAttempted", totalRecordsAttempted);
            put("FirehoseSender.TotalRecordsSent", totalRecordsSent);
            put("FirehoseSender.TotalRecordsFailed", totalRecordsFailed);
            put("FirehoseSender.TotalBatchPutCalls", totalBatchPutCalls);
            put("FirehoseSender.TotalBatchPutServiceErrors", totalBatchPutServiceErrors);
            put("FirehoseSender.TotalBatchPutOtherErrors", totalBatchPutOtherErrors);
            put("FirehoseSender.TotalBatchPutLatency", totalBatchPutLatency);
            put("FirehoseSender.ActiveBatchPutCalls", activeBatchPutCalls);
            for(Map.Entry<String, AtomicLong> err : totalErrors.entrySet()) {
                put("FirehoseSender.Error(" + err.getKey() +")", err.getValue());
            }
        }};
    }
}
