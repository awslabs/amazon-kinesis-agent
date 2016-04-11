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
 * Function that converts an object into a boolean <code>true</code> or <code>false</code>.
 * Following (case insensitive) strings map to <code>true</code>: <ul>
 * <li>True</li>
 * <li>T</li>
 * <li>Yes</li>
 * <li>Y</li>
 * <li>1</li>
 * </ul>
 * Following (case insensitive) strings map to <code>false</code>: <ul>
 * <li>False</li>
 * <li>F</li>
 * <li>No</li>
 * <li>N</li>
 * <li>0</li>
 * </ul>
 * Everything else raises a {@link ConfigurationException}.
 */
class BooleanConverter implements Function<Object, Boolean> {

    /**
     * @throws ConfigurationException if the value could not be converted
     *         into a <code>Boolean</code>.
     */
    @Override
    @Nullable
    public Boolean apply(@Nullable Object input) {
        if (input != null && !(input instanceof Boolean)) {
            switch (input.toString().toLowerCase()) {
            case "true":
            case "yes":
            case "t":
            case "y":
            case "1":
                return true;
            case "false":
            case "no":
            case "f":
            case "n":
            case "0":
                return false;
            default:
                throw new ConfigurationException(
                        "Cannot convert value to boolean: "
                                + input.toString());

            }
        } else
            return (Boolean) input;
    }

}