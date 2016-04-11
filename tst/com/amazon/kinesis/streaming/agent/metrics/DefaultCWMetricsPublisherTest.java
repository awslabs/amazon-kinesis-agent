/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.metrics.CWMetricKey;
import com.amazon.kinesis.streaming.agent.metrics.DefaultCWMetricsPublisher;
import com.amazon.kinesis.streaming.agent.metrics.MetricDatumWithKey;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;

public class DefaultCWMetricsPublisherTest extends TestBase {

    private final String NAMESPACE = "fakeNamespace";
    private final AmazonCloudWatch cloudWatchClient = Mockito.mock(AmazonCloudWatch.class);
    private DefaultCWMetricsPublisher publisher = new DefaultCWMetricsPublisher(cloudWatchClient, NAMESPACE);

    /*
     * Test whether the data input into metrics publisher is the equal to the data which will be published to CW
     */

    @Test
    public void testMetricsPublisher() {
        List<MetricDatumWithKey<CWMetricKey>> dataToPublish = constructMetricDatumWithKeyList(25);
        List<Map<String, MetricDatum>> expectedData = constructMetricDatumListMap(dataToPublish);
        publisher.publishMetrics(dataToPublish);

        ArgumentCaptor<PutMetricDataRequest> argument = ArgumentCaptor.forClass(PutMetricDataRequest.class);
        Mockito.verify(cloudWatchClient, Mockito.atLeastOnce()).putMetricData(argument.capture());

        List<PutMetricDataRequest> requests = argument.getAllValues();
        Assert.assertEquals(expectedData.size(), requests.size());

        for (int i = 0; i < requests.size(); i++) {
            assertMetricData(expectedData.get(i), requests.get(i));
        }

    }

    public static List<MetricDatumWithKey<CWMetricKey>> constructMetricDatumWithKeyList(int value) {
        List<MetricDatumWithKey<CWMetricKey>> data = new ArrayList<MetricDatumWithKey<CWMetricKey>>();
        for (int i = 1; i <= value; i++) {
            MetricDatum datum =
                    TestHelper.constructDatum("datum" + Integer.toString(i), StandardUnit.Count, i, i, i, 1);
            data.add(new MetricDatumWithKey<CWMetricKey>(new CWMetricKey(datum), datum));
        }

        return data;
    }

    // batchSize is the number of metrics sent in a single request.
    // In DefaultCWMetricsPublisher this number is set to 20.
    public List<Map<String, MetricDatum>> constructMetricDatumListMap(List<MetricDatumWithKey<CWMetricKey>> data) {
        int batchSize = 20;
        List<Map<String, MetricDatum>> dataList = new ArrayList<Map<String, MetricDatum>>();

        int expectedRequestcount = (int) Math.ceil(data.size() / 20.0);

        for (int i = 0; i < expectedRequestcount; i++) {
            dataList.add(i, new HashMap<String, MetricDatum>());
        }

        int batchIndex = 1;
        int listIndex = 0;
        for (MetricDatumWithKey<CWMetricKey> metricDatumWithKey : data) {
            if (batchIndex > batchSize) {
                batchIndex = 1;
                listIndex++;
            }
            batchIndex++;
            dataList.get(listIndex).put(metricDatumWithKey.datum.getMetricName(), metricDatumWithKey.datum);
        }
        return dataList;
    }

    public static void assertMetricData(Map<String, MetricDatum> expected, PutMetricDataRequest actual) {
        List<MetricDatum> actualData = actual.getMetricData();
        for (MetricDatum actualDatum : actualData) {
            String metricName = actualDatum.getMetricName();
            Assert.assertNotNull(expected.get(metricName));
            Assert.assertTrue(expected.get(metricName).equals(actualDatum));
            expected.remove(metricName);
        }

        Assert.assertTrue(expected.isEmpty());
    }
}
