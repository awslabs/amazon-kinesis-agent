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
package com.amazon.kinesis.streaming.agent.config;

import javax.annotation.Nullable;

import com.google.common.base.Function;

/**
 * Wraps a <code>String</code> -> <code>T</code> function into an
 * <code>Object</code> -> T</code> function by calling <code>toString</code>
 * on the value.
 *
 * @param <T> the destination type.
 */
class StringConverterWrapper<T> implements
        Function<Object, T> {
    private final Function<String, T> delegate;

    public StringConverterWrapper(Function<String, T> delegate) {
        this.delegate = delegate;
    }

    @Override
    @Nullable
    public T apply(@Nullable Object input) {
        return input != null ? this.delegate.apply(input.toString()) : null;
    }
}