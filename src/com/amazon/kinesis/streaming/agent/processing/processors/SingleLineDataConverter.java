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

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.processing.exceptions.DataConversionException;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IDataConverter;
import com.google.common.base.Splitter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An implementation of {@link IDataConverter} that converts the data specified
 * into a single line from (possibly) a multi-line representation and replaces
 * the newline character with a whitespace. All leading and trailing spaces for
 * a given line are removed.
 * <p>
 * This converter can be configured by setting {@code optionName} to {@code SINGLELINE}
 * when configuring the agent. Example:
 * <pre> {@code
 *     ...
 *     "dataProcessingOptions": [
 *       {
 *         "optionName": "SINGLELINE"
 *       }
 *     ]
 *     ...
 * } </pre>
 *
 * @author chaocheq
 * @author Willy Lulciuc
 */
public class SingleLineDataConverter implements IDataConverter {

    /** Splits each line from the data specified by the newline character. */
    private static final Splitter NEW_LINE_SPLITTER = Splitter.on(NEW_LINE).omitEmptyStrings().trimResults();

    @Override
    public ByteBuffer convert(ByteBuffer data) throws DataConversionException {
        String dataAsString = ByteBuffers.toString(checkNotNull(data), StandardCharsets.UTF_8);
        StringBuilder builder = new StringBuilder();
        for (String line : NEW_LINE_SPLITTER.split(dataAsString)) {
            builder.append(line);
        }
        // We want to append the newline character at the end of the string, using the
        // character as a delimiter.
        builder.append(NEW_LINE);
        String dataAsSingleLine = builder.toString();
        return ByteBuffer.wrap(dataAsSingleLine.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
