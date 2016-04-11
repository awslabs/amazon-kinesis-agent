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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.StandardUnit;

/**
 * A composite {@link IMetricsScope} that delegates all method calls to a list
 * of underlying instances.
 */
public class CompositeMetricsScope extends AbstractMetricsScope {
    private final List<IMetricsScope> scopes;

    /**
     * @param scopes
     */
    public CompositeMetricsScope(IMetricsScope... scopes) {
        this(Arrays.asList(scopes));
    }

    /**
     * @param scopes
     */
    public CompositeMetricsScope(Collection<IMetricsScope> scopes) {
        this.scopes = new ArrayList<>(scopes);
    }

    @Override
    protected void realAddData(String name, double value, StandardUnit unit) {
        for(IMetricsScope scope : this.scopes)
            scope.addData(name, value, unit);
    }

    @Override
    protected void realAddDimension(String name, String value) {
        for(IMetricsScope scope : this.scopes)
            scope.addDimension(name, value);
    }

    @Override
    protected void realCommit() {
        for(IMetricsScope scope : this.scopes)
            scope.commit();
    }

    @Override
    protected void realCancel() {
        for(IMetricsScope scope : this.scopes)
            scope.cancel();
    }

    @Override
    protected Set<Dimension> realGetDimensions() {
        return scopes.isEmpty() ? Collections.<Dimension> emptySet() : scopes.get(0).getDimensions();
    }
}
