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
package com.amazon.kinesis.streaming.agent.processing.processors;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.commons.lang3.StringUtils;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.processing.exceptions.DataConversionException;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IDataConverter;

/**
 * Replace the newline with a whitespace
 * Remove leading and trailing spaces for each line
 * 
 * Configuration looks like:
 * 
 * { "optionName": "SINGLELINE" }
 * 
 * @author chaocheq
 *
 */
public class SingleLineDataConverter implements IDataConverter {
    
    @Override
    public ByteBuffer convert(ByteBuffer data) throws DataConversionException {
        String dataStr = ByteBuffers.toString(data, StandardCharsets.UTF_8);
        String[] lines = dataStr.split(NEW_LINE);
        for (int i = 0; i < lines.length; i++) {
            // FIXME: Shall we trim each line?
            lines[i] = lines[i].trim();
        }
        
        String dataRes = StringUtils.join(lines) + NEW_LINE;
        return ByteBuffer.wrap(dataRes.getBytes(StandardCharsets.UTF_8));
    }
    
    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
