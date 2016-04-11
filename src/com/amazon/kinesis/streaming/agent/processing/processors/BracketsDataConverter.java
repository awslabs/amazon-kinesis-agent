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

import com.amazon.kinesis.streaming.agent.processing.exceptions.DataConversionException;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IDataConverter;

/**
 * This example converter just wraps resulting data into brackets.
 *
 * For example, input line "hello there" will after conversion will look like "{hello there}"
 *
 * Created by myltik on 12/01/2016.
 */
public class BracketsDataConverter implements IDataConverter {

    @Override
    public ByteBuffer convert(ByteBuffer data) throws DataConversionException {
        final byte[] dataBin = new byte[data.remaining()];
        data.get(dataBin);

        StringBuilder sb = new StringBuilder(2 + dataBin.length);
        sb.append('{')
          .append(new String(dataBin, StandardCharsets.UTF_8))
          .append('}');

        return ByteBuffer.wrap(sb.toString().getBytes());
    }
    
    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
