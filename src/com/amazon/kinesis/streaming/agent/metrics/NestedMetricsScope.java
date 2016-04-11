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

public class NestedMetricsScope implements IMetricsScope {
    private final IMetricsScope delegate;

    public NestedMetricsScope(IMetricsScope delegate) {
        this.delegate = delegate;
    }

    @Override
    public void addData(String name, double value, StandardUnit unit) {
        delegate.addData(name, value, unit);
    }

    @Override
    public void addCount(String name, long amount) {
        delegate.addCount(name, amount);
    }

    @Override
    public void addTimeMillis(String name, long duration) {
        delegate.addTimeMillis(name, duration);
    }

    @Override
    public void addDimension(String name, String value) {
        throw new UnsupportedOperationException("Cannot add dimensions for nested metrics.");
    }

    @Override
    public void commit() {
        // TODO: Implement reference counting
    }

    @Override
    public void cancel() {
        throw new UnsupportedOperationException("Cannot cancel nested metrics.");
    }

    @Override
    public boolean closed() {
        return delegate.closed();
    }

    @Override
    public Set<Dimension> getDimensions() {
        return delegate.getDimensions();
    }
}
