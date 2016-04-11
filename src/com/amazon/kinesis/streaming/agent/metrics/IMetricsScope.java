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

import java.util.Set;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.StandardUnit;

/**
 * An {@link IMetricsScope} represents a set of metric data that share a set of
 * dimensions. {@link IMetricsScope}s know how to output themselves
 * (perhaps to disk, perhaps over service calls, etc).
 */
public interface IMetricsScope {

    /**
     * Adds a data point to this scope.
     *
     * @param name data point name
     * @param value data point value
     * @param unit unit of data point
     */
    public void addData(String name, double value, StandardUnit unit);

    /**
     * @param name @see {@link #addData(String, double, StandardUnit)}
     * @param amount the amount to increment this counter.
     */
    public void addCount(String name, long amount);

    /**
     * @param name
     * @param duration duration of the tiumer in milliseconds
     */
    public void addTimeMillis(String name, long duration);

    /**
     * Adds a dimension that applies to all metrics in this IMetricsScope.
     *
     * @param name dimension name
     * @param value dimension value
     */
    public void addDimension(String name, String value);

    /**
     * Flushes the data from this scope and makes it unusable.
     */
    public void commit();

    /**
     * Cancels this scope and discards any data.
     */
    public void cancel();

    /**
     * @return <code>true</code> if {@link #commit()} or {@link #cancel()} have
     *         been called on this instance, otherwise <code>false</code>.
     */
    public boolean closed();

    /**
     * @return a set of dimensions for an IMetricsScope
     */
    public Set<Dimension> getDimensions();
}
