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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.StatisticSet;

/**
 * An base implementation of an {@link IMetricsScope} that accumulates data
 * from multiple calls to addData with the same name parameter.
 * It tracks min, max, sample count, and sum for each named metric.
 */
@NotThreadSafe
public abstract class AccumulatingMetricsScope extends AbstractMetricsScope {

    protected final Set<Dimension> dimensions = new HashSet<Dimension>();
    protected final Map<String, MetricDatum> data = new HashMap<String, MetricDatum>();
    protected final long startTime = System.currentTimeMillis();

    @Override
    protected void realAddData(String name, double value, StandardUnit unit) {
        MetricDatum datum = data.get(name);
        if (datum == null) {
            data.put(name,
                    new MetricDatum().withMetricName(name)
                            .withUnit(unit)
                            .withStatisticValues(new StatisticSet().withMaximum(value)
                                    .withMinimum(value)
                                    .withSampleCount(1.0)
                                    .withSum(value)));
        } else {
            if (!datum.getUnit().equals(unit.name())) {
                throw new IllegalArgumentException("Cannot add to existing metric with different unit");
            }
            StatisticSet statistics = datum.getStatisticValues();
            statistics.setMaximum(Math.max(value, statistics.getMaximum()));
            statistics.setMinimum(Math.min(value, statistics.getMinimum()));
            statistics.setSampleCount(statistics.getSampleCount() + 1);
            statistics.setSum(statistics.getSum() + value);
        }
    }

    @Override
    protected void realAddDimension(String name, String value) {
        dimensions.add(new Dimension().withName(name).withValue(value));
    }

    @Override
    protected Set<Dimension> realGetDimensions() {
        return dimensions;
    }
}
