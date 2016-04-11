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

import javax.annotation.concurrent.NotThreadSafe;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.google.common.base.Preconditions;
/**
 * An base implementation of an {@link IMetricsScope} that accumulates data
 * from multiple calls to addData with the same name parameter.
 * It tracks min, max, sample count, and sum for each named metric.
 */
@NotThreadSafe
public abstract class AbstractMetricsScope implements IMetricsScope {
    private boolean closed = false;

    /**
     * Adds data points to the scope {@link IMetricsScope}.
     * Multiple calls to this method with the same name will have their data
     * accumulated.
     * @see IMetricsScope#addData(String, double, StandardUnit)
     */
    @Override
    public final void addData(String name, double value, StandardUnit unit) {
        Preconditions.checkState(!closed, "Scope is already closed.");
        realAddData(name, value, unit);
    }
    protected void realAddData(String name, double value, StandardUnit unit) { }

    @Override
    public void addCount(String name, long amount) {
        addData(name, amount, StandardUnit.Count);
    }

    @Override
    public void addTimeMillis(String name, long duration) {
        addData(name, duration, StandardUnit.Milliseconds);
    }

    @Override
    public final void addDimension(String name, String value) {
        Preconditions.checkState(!closed, "Scope is already closed.");
        realAddDimension(name, value);
    }
    protected void realAddDimension(String name, String value) { }

    @Override
    public final void commit() {
        Preconditions.checkState(!closed, "Scope is already closed.");
        try {
            realCommit();
        } finally {
            closed = true;
        }
    }

    @Override
    public final void cancel() {
        Preconditions.checkState(!closed, "Scope is already closed.");
        try {
            realCancel();
        } finally {
            closed = true;
        }
    }

    protected void realCancel() { }
    protected void realCommit() { }

    @Override
    public boolean closed() {
        return closed;
    }

    /**
     * @return a set of dimensions for an IMetricsScope
     */
    @Override
    public Set<Dimension> getDimensions() {
        Preconditions.checkState(!closed, "Scope is already closed.");
        return realGetDimensions();
    }
    protected abstract Set<Dimension> realGetDimensions();
}
