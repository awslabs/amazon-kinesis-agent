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
package com.amazon.kinesis.streaming.agent.processing.interfaces;

import java.util.Map;

import com.amazon.kinesis.streaming.agent.processing.exceptions.DataConversionException;

public interface IJSONPrinter {
    
    /**
     * Write a map into a readable string in JSON format
     * 
     * @param recordMap
     * @return
     * @throws DataConversionException
     */
    public String writeAsString(Map<String, Object> recordMap) throws DataConversionException;
}
