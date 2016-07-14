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
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class KinesisSender extends AbstractSender<KinesisRecord> {
    private static final String SERVICE_ERRORS_METRIC = "ServiceErrors";
    private static final String RECORDS_ATTEMPTED_METRIC = "RecordSendAttempts";
    private static final String RECORD_ERRORS_METRIC = "RecordSendErrors";
    private static final String BYTES_SENT_METRIC = "BytesSent";
    private static final String SENDER_NAME = "KinesisSender";

    @Getter private final AgentContext agentContext;
    private final KinesisFileFlow flow;

    private final AtomicLong totalRecordsAttempted = new AtomicLong();
    private final AtomicLong totalRecordsSent = new AtomicLong();
    private final AtomicLong totalRecordsFailed = new AtomicLong();
    private final AtomicLong totalPutRecordsCalls = new AtomicLong();
    private final AtomicLong totalPutRecordsServiceErrors = new AtomicLong();
    private final AtomicLong totalPutRecordsOtherErrors = new AtomicLong();
    private final AtomicLong totalPutRecordsLatency = new AtomicLong();
    private final AtomicLong activePutRecordsCalls = new AtomicLong();
    private final Map<String, AtomicLong> totalErrors = new HashMap<>();

    public KinesisSender(AgentContext agentContext, KinesisFileFlow flow) {
        Preconditions.checkNotNull(flow);
        this.agentContext = agentContext;
        this.flow = flow;
    }

    @Override
    protected long getMaxSendBatchSizeBytes() {
        return KinesisConstants.MAX_PUT_RECORDS_SIZE_BYTES;
    }

    @Override
    protected int getMaxSendBatchSizeRecords() {
        return KinesisConstants.MAX_PUT_RECORDS_SIZE_RECORDS;
    }

    @Override
    protected BufferSendResult<KinesisRecord> attemptSend(RecordBuffer<KinesisRecord> buffer) {
        activePutRecordsCalls.incrementAndGet();
        IMetricsScope metrics = agentContext.beginScope();
        metrics.addDimension(Metrics.DESTINATION_DIMENSION, "KinesisStream:" + getDestination());
        try {
            BufferSendResult<KinesisRecord> sendResult = null;
            List<PutRecordsRequestEntry> requestRecords = new ArrayList<>();
            for(KinesisRecord data : buffer) {
                PutRecordsRequestEntry record = new PutRecordsRequestEntry();
                record.setData(data.data());
                record.setPartitionKey(data.partitionKey());
                requestRecords.add(record);
            }
            PutRecordsRequest request = new PutRecordsRequest();
            request.setStreamName(getDestination());
            request.setRecords(requestRecords);
            PutRecordsResult result = null;
            Stopwatch timer = Stopwatch.createStarted();
            totalPutRecordsCalls.incrementAndGet();
            try {
                logger.trace("{}: Sending buffer {} to kinesis stream {}...", flow.getId(), buffer, getDestination());
                metrics.addCount(RECORDS_ATTEMPTED_METRIC, requestRecords.size());
                result = agentContext.getKinesisClient().putRecords(request);
                metrics.addCount(SERVICE_ERRORS_METRIC, 0);
            } catch (AmazonServiceException e) {
                metrics.addCount(SERVICE_ERRORS_METRIC, 1);
                totalPutRecordsServiceErrors.incrementAndGet();
                throw e;
            } catch (Exception e) {
                metrics.addCount(SERVICE_ERRORS_METRIC, 1);
                totalPutRecordsOtherErrors.incrementAndGet();
                throw e;
            } finally {
                totalPutRecordsLatency.addAndGet(timer.elapsed(TimeUnit.MILLISECONDS));
            }
            if(sendResult == null) {
                List<Integer> sentRecords = new ArrayList<>(requestRecords.size());
                Multiset<String> errors = HashMultiset.<String> create();
                int index = 0;
                long totalBytesSent = 0;
                for (final PutRecordsResultEntry responseEntry : result.getRecords()) {
                	final PutRecordsRequestEntry record = requestRecords.get(index);
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
                logger.debug("{}:{} Records sent to kinesis stream {}: {}. Failed records: {}",
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
                        logger.debug("{}:{} Errors from kinesis stream {}: {}", flow.getId(), buffer, flow.getDestination(), strErrors.toString());
                    }
                }
            }
            return sendResult;
        } finally {
            metrics.commit();
            activePutRecordsCalls.decrementAndGet();
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
            put(SENDER_NAME + ".TotalRecordsAttempted", totalRecordsAttempted);
            put(SENDER_NAME + ".TotalRecordsSent", totalRecordsSent);
            put(SENDER_NAME + ".TotalRecordsFailed", totalRecordsFailed);
            put(SENDER_NAME + ".TotalPutRecordsCalls", totalPutRecordsCalls);
            put(SENDER_NAME + ".TotalPutRecordsServiceErrors", totalPutRecordsServiceErrors);
            put(SENDER_NAME + ".TotalPutRecordsOtherErrors", totalPutRecordsOtherErrors);
            put(SENDER_NAME + ".TotalPutRecordsLatency", totalPutRecordsLatency);
            put(SENDER_NAME + ".ActivePutRecordsCalls", activePutRecordsCalls);
            for(Map.Entry<String, AtomicLong> err : totalErrors.entrySet()) {
                put(SENDER_NAME + ".Error(" + err.getKey() +")", err.getValue());
            }
        }};
    }
}
