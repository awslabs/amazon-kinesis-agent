/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.kinesis.streaming.agent.processing.interfaces.IDataConverter;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPOutputStream;



/**
 * Compress the data using the gzip codec
 *
 * Configuration looks like:
 * 
 * {
 *     "optionName": "GZIPCOMPRESSION"
 * }
 * 
 * @author achaibi
 *
 */
public class GzipDataConverter implements IDataConverter {
    @Override
    public ByteBuffer convert(ByteBuffer data) {
        return toCompressedByteBuffer(data);
    }

    private ByteBuffer toCompressedByteBuffer(ByteBuffer data) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new GZIPOutputStream(out);
            gzip.write(data.array());
            gzip.close();
            return ByteBuffer.wrap(out.toByteArray());
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
