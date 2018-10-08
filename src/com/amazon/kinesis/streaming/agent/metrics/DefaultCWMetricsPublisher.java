/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazon.kinesis.streaming.agent.metrics;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;

/**
 * Default implementation for publishing metrics to CloudWatch.
 */

public class DefaultCWMetricsPublisher implements ICWMetricsPublisher<CWMetricKey> {

    private static final Logger LOG = LoggerFactory.getLogger(CWPublisherRunnable.class);

    // CloudWatch API has a limit of 20 MetricDatums per request
    private static final int BATCH_SIZE = 20;

    private final String namespace;
    private final AmazonCloudWatch cloudWatchClient;

    public DefaultCWMetricsPublisher(AmazonCloudWatch cloudWatchClient, String namespace) {
        this.cloudWatchClient = cloudWatchClient;
        this.namespace = namespace;
    }

    @Override
    public void publishMetrics(List<MetricDatumWithKey<CWMetricKey>> dataToPublish) {
        for (int startIndex = 0; startIndex < dataToPublish.size(); startIndex += BATCH_SIZE) {
            int endIndex = Math.min(dataToPublish.size(), startIndex + BATCH_SIZE);

            PutMetricDataRequest request = new PutMetricDataRequest();
            request.setNamespace(namespace);

            List<MetricDatum> metricData = new ArrayList<MetricDatum>();
            for (int i = startIndex; i < endIndex; i++) {
                MetricDatum metric = dataToPublish.get(i).datum;
                if(!metric.getMetricName().startsWith("."))
                    metricData.add(dataToPublish.get(i).datum);
            }
            if(metricData.isEmpty())
                return;

            request.setMetricData(metricData);
            try {
                cloudWatchClient.putMetricData(request);

                LOG.info(String.format("Successfully published %d datums.", endIndex - startIndex));
            } catch (AmazonClientException e) {
                LOG.warn(String.format("Could not publish %d datums to CloudWatch", endIndex - startIndex), e);
            }
        }
    }
}
