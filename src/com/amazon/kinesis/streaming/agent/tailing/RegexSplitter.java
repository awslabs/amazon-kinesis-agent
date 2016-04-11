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
package com.amazon.kinesis.streaming.agent.tailing;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Returns one record that splits records based on a regex that matches the beginning of a record
 * 
 */
public class RegexSplitter implements ISplitter {
    public final Pattern startingPattern;

    public RegexSplitter(String startingPattern) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(startingPattern));
        this.startingPattern = Pattern.compile(startingPattern);
    }

    public String getPattern() {
        return startingPattern.pattern();
    }

    @Override
    public int locateNextRecord(ByteBuffer buffer) {
        return advanceBufferToNextPattern(buffer);
    }

    /**
     * Advances the buffer current position to be at the index at the beginning of the
     * next pattern, or else the end of the buffer if there is no final pattern.
     * 
     * @return {@code position} of the buffer at the starting index of the next new pattern;
     *         {@code -1} if the end of the buffer was reached.
     */
    private int advanceBufferToNextPattern(ByteBuffer buffer) {
        Matcher matcher;
        // this marks the position before which we already attempted to match the pattern
        int currentLookedPosition = buffer.position();
        boolean firstLine = true;
        while (buffer.hasRemaining()) {
            // start matching from the current position on a line by line basis
            if (buffer.get() == SingleLineSplitter.LINE_DELIMITER) {
                // Skip the first line as it must be part of the current record
                if (!firstLine) {
                    String line = new String(buffer.array(), currentLookedPosition, buffer.position() - currentLookedPosition, StandardCharsets.UTF_8);
                    matcher = startingPattern.matcher(line);
                    if(matcher.lookingAt()) {
                        buffer.position(currentLookedPosition);
                        return currentLookedPosition;
                    }
                }
                firstLine = false;
                // update the position that we already looked at
                currentLookedPosition = buffer.position();
            }
        }
        
        // We've scanned to the end and there is only one complete record in the buffer, set the position to the end
        if (!firstLine && buffer.limit() < buffer.capacity()) {
            return buffer.position();
        }
        
        return -1;
    }
}
