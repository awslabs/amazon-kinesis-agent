/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.metrics;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.metrics.AccumulatingMetricsScope;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;

public class BaseMetricsScopeTest extends TestBase {

    private static class TestScope extends AccumulatingMetricsScope {

        public TestScope() {
            super();
        }

        public void assertMetrics(MetricDatum... expectedData) {
            for (MetricDatum expected : expectedData) {
                MetricDatum actual = data.remove(expected.getMetricName());
                Assert.assertEquals(expected, actual);
            }

            Assert.assertEquals(data.size(), 0, "Data should be empty at the end of assertMetrics");
        }
    }

    @Test
    public void testSingleAdd() {
        TestScope scope = new TestScope();

        scope.addData("name", 2.0, StandardUnit.Count);
        scope.assertMetrics(TestHelper.constructDatum("name", StandardUnit.Count, 2.0, 2.0, 2.0, 1));
    }

    @Test
    public void testAccumulate() {
        TestScope scope = new TestScope();

        scope.addData("name", 2.0, StandardUnit.Count);
        scope.addData("name", 3.0, StandardUnit.Count);
        scope.assertMetrics(TestHelper.constructDatum("name", StandardUnit.Count, 3.0, 2.0, 5.0, 2));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testAccumulatWrongUnit() {
        TestScope scope = new TestScope();

        scope.addData("name", 2.0, StandardUnit.Count);
        scope.addData("name", 3.0, StandardUnit.Megabits);
    }

    @Test
    public void testAddDataNotClosed() {
        TestScope scope = new TestScope();
        scope.addData("foo", 1.0, StandardUnit.Count);
    }

    @Test
    public void testAddDimensionNotClosed() {
        TestScope scope = new TestScope();
        scope.addDimension("foo", "bar");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testAddDataClosed() {
        TestScope scope = new TestScope();
        scope.commit();
        scope.addData("foo", 1.0, StandardUnit.Count);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testAddDimensionClosed() {
        TestScope scope = new TestScope();
        scope.commit();
        scope.addDimension("foo", "bar");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testDoubleCommit() {
        TestScope scope = new TestScope();
        scope.commit();
        scope.commit();
    }
}
