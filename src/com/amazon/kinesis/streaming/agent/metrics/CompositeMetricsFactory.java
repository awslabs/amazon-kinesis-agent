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

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * A metrics factory that wraps multiple metrics factories.
 */
public class CompositeMetricsFactory implements IMetricsFactory {

    private final Collection<IMetricsFactory> factories;

    /**
     * @param factories
     */
    public CompositeMetricsFactory(IMetricsFactory... factories) {
        this(Arrays.asList(factories));
    }

    /**
     * @param factories
     */
    public CompositeMetricsFactory(Collection<IMetricsFactory> factories) {
        this.factories = new ArrayList<>(factories);
    }

    /**
     * @return a {@link CompositeMetricsScope} containing a scope for each
     *         of the factories backing this composite.
     */
    @Override
    public IMetricsScope createScope() {
        Collection<IMetricsScope> scopes = Collections2.transform(
                this.factories, new Function<IMetricsFactory, IMetricsScope>() {
                    @Override
                    @Nullable
                    public IMetricsScope apply(IMetricsFactory input) {
                        return input.createScope();
                    }
                });
        return new CompositeMetricsScope(scopes);

    }

}
