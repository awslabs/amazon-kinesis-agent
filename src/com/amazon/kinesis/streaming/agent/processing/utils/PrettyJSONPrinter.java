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
package com.amazon.kinesis.streaming.agent.processing.utils;

import java.util.Map;

import com.amazon.kinesis.streaming.agent.processing.exceptions.DataConversionException;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IJSONPrinter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Write the given map in pretty printed JSON string
 * 
 * @author chaocheq
 *
 */
public class PrettyJSONPrinter implements IJSONPrinter {

    @Override
    public String writeAsString(Map<String, Object> recordMap)
            throws DataConversionException {
        try {
            return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(recordMap);
        } catch (JsonProcessingException e) {
            throw new DataConversionException("Unable to convert record map to JSON string", e);
        }
    }

}
