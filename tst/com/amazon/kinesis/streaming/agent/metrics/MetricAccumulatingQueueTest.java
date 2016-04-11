/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.metrics;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.metrics.CWMetricKey;
import com.amazon.kinesis.streaming.agent.metrics.MetricAccumulatingQueue;
import com.amazon.kinesis.streaming.agent.metrics.MetricDatumWithKey;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;

public class MetricAccumulatingQueueTest extends TestBase {

    private static final int MAX_QUEUE_SIZE = 5;
    private MetricAccumulatingQueue<CWMetricKey> queue;

    @BeforeMethod
    public void setup() {
        this.queue = new MetricAccumulatingQueue<CWMetricKey>(MAX_QUEUE_SIZE);
    }

    /*
     * Test whether the MetricDatums offered into the queue will accumulate data based on the same metricName and
     * output those datums with the correctly accumulated output.
     */
    @Test
    public void testAccumulation() {
        Collection<Dimension> dimensionsA = Collections.singleton(new Dimension().withName("name").withValue("a"));
        Collection<Dimension> dimensionsB = Collections.singleton(new Dimension().withName("name").withValue("b"));
        String keyA = "a";
        String keyB = "b";

        MetricDatum datum1 =
                TestHelper.constructDatum(keyA, StandardUnit.Count, 10, 5, 15, 2).withDimensions(dimensionsA);
        queue.offer(new CWMetricKey(datum1), datum1);
        MetricDatum datum2 =
                TestHelper.constructDatum(keyA, StandardUnit.Count, 1, 1, 2, 2).withDimensions(dimensionsA);
        queue.offer(new CWMetricKey(datum2), datum2);

        MetricDatum datum3 =
                TestHelper.constructDatum(keyA, StandardUnit.Count, 1, 1, 2, 2).withDimensions(dimensionsB);
        queue.offer(new CWMetricKey(datum3), datum3);

        MetricDatum datum4 = TestHelper.constructDatum(keyA, StandardUnit.Count, 1, 1, 2, 2);
        queue.offer(new CWMetricKey(datum4), datum4);
        queue.offer(new CWMetricKey(datum4), datum4);

        MetricDatum datum5 =
                TestHelper.constructDatum(keyB, StandardUnit.Count, 100, 10, 110, 2).withDimensions(dimensionsA);
        queue.offer(new CWMetricKey(datum5), datum5);

        Assert.assertEquals(4, queue.size());
        List<MetricDatumWithKey<CWMetricKey>> items = queue.drain(4);

        Assert.assertEquals(items.get(0).datum, TestHelper.constructDatum(keyA, StandardUnit.Count, 10, 1, 17, 4)
                .withDimensions(dimensionsA));
        Assert.assertEquals(items.get(1).datum, datum3);
        Assert.assertEquals(items.get(2).datum, TestHelper.constructDatum(keyA, StandardUnit.Count, 1, 1, 4, 4));
        Assert.assertEquals(items.get(3).datum, TestHelper.constructDatum(keyB, StandardUnit.Count, 100, 10, 110, 2)
                .withDimensions(dimensionsA));
    }

    /*
     * Test that the number of MetricDatum that can be added to our queue is capped at the MAX_QUEUE_SIZE.
     * Therefore, any datums added to the queue that is greater than the capacity of our queue will be dropped.
     */
    @Test
    public void testDrop() {
        for (int i = 0; i < MAX_QUEUE_SIZE; i++) {
            MetricDatum datum = TestHelper.constructDatum(Integer.toString(i), StandardUnit.Count, 1, 1, 2, 2);
            CWMetricKey key = new CWMetricKey(datum);
            Assert.assertTrue(queue.offer(key, datum));
        }

        MetricDatum datum = TestHelper.constructDatum("foo", StandardUnit.Count, 1, 1, 2, 2);
        Assert.assertFalse(queue.offer(new CWMetricKey(datum), datum));
        Assert.assertEquals(MAX_QUEUE_SIZE, queue.size());
    }
}
