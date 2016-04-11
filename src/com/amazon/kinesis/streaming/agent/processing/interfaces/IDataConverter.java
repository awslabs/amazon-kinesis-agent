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

import java.nio.ByteBuffer;

import com.amazon.kinesis.streaming.agent.processing.exceptions.DataConversionException;

/**
 * Created by myltik on 12/01/2016.
 */
public interface IDataConverter {
    
    public static final String NEW_LINE = "\n";
    
    /**
     * Convert data from source to any other format
     * @param data    Source data
     * @return byte array of processed data
     * @throws DataConversionException
     */
    public ByteBuffer convert(ByteBuffer data) throws DataConversionException;
}
