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

import java.util.List;
import java.util.Map;

import com.amazon.kinesis.streaming.agent.processing.exceptions.LogParsingException;

public interface ILogParser {

    public void setPattern(String pattern);
    public void setFields(List<String> fields);
    
    /**
     * Parse the record by pairing the values in the record with the given fields
     * 
     * @param record
     * @param fields
     * @return
     * @throws LogParsingException
     */
    public Map<String, Object> parseLogRecord(String record, List<String> fields) throws LogParsingException;
}
