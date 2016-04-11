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

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Function;

/**
 * Function that converts an object into a {@link Configuration}.
 */
class ConfigurationConverter implements Function<Object, Configuration> {

    @SuppressWarnings("unchecked")
    @Override
    @Nullable
    public Configuration apply(@Nullable Object input) {
        if (input != null && !(input instanceof Configuration)) {
            if(input instanceof Map)
                return new Configuration((Map<String, Object>) input);
            else
                throw new ConfigurationException("Value is not a valid map: " + input.toString());
        } else
            return (Configuration) input;
    }
}