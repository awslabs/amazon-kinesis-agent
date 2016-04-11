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

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.Nullable;

import com.google.common.base.Function;

/**
 * Function that converts an object into a {@link Path}.
 */
class PathConverter implements Function<Object, Path> {

    @Override
    @Nullable
    public Path apply(@Nullable Object input) {
        if (input != null && !(input instanceof Path)) {
            if(input instanceof URI)
                return Paths.get((URI)input);
            else
                return Paths.get(input.toString());
        } else
            return (Path) input;
    }
}