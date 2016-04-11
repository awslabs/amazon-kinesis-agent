/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.metrics;

import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.StatisticSet;

public class TestHelper {
    public static MetricDatum constructDatum(String name,
            StandardUnit unit,
            double maximum,
            double minimum,
            double sum,
            double count) {
        return new MetricDatum().withMetricName(name)
                .withUnit(unit)
                .withStatisticValues(new StatisticSet().withMaximum(maximum)
                        .withMinimum(minimum)
                        .withSum(sum)
                        .withSampleCount(count));
    }
}
