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
package com.amazon.kinesis.streaming.agent.metrics;

import org.slf4j.Logger;

import com.amazon.kinesis.streaming.agent.Logging;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StatisticSet;

/**
 * An AccumulatingMetricsScope that outputs via log4j.
 */
public class LogMetricsScope extends AccumulatingMetricsScope {
    public static final Logger LOGGER = Logging.getLogger(LogMetricsScope.class);

    public LogMetricsScope() {
        super();
    }

    @Override
    protected void realCommit() {
        if(!data.values().isEmpty()) {
            StringBuilder output = new StringBuilder();
            output.append("Metrics:\n");

            output.append("Dimensions: ");
            boolean needsComma = false;
            for (Dimension dimension : getDimensions()) {
                output.append(String.format("%s[%s: %s]", needsComma ? ", " : "", dimension.getName(), dimension.getValue()));
                needsComma = true;
            }
            output.append("\n");

            for (MetricDatum datum : data.values()) {
                StatisticSet statistics = datum.getStatisticValues();
                output.append(String.format("Name=%50s\tMin=%.2f\tMax=%.2f\tCount=%.2f\tSum=%.2f\tAvg=%.2f\tUnit=%s\n",
                        datum.getMetricName(),
                        statistics.getMinimum(),
                        statistics.getMaximum(),
                        statistics.getSampleCount(),
                        statistics.getSum(),
                        statistics.getSum() / statistics.getSampleCount(),
                        datum.getUnit()));
            }
            LOGGER.debug(output.toString());
        }
    }
}
