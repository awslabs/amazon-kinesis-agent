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
package com.amazon.kinesis.streaming.agent.tailing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

import com.amazon.kinesis.streaming.agent.tailing.KinesisConstants.PartitionKeyOption;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KinesisRecord extends AbstractRecord {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisRecord.class);
    private static final JsonFactory JSON_FACTORY = new ObjectMapper().getFactory();
    protected final String partitionKey;
    
    public KinesisRecord(TrackedFile file, long offset, ByteBuffer data, long originalLength) {
        super(file, offset, data, originalLength);
        Preconditions.checkNotNull(file);
        KinesisFileFlow flow = (KinesisFileFlow)file.getFlow();
        partitionKey = generatePartitionKey(flow.getPartitionKeyOption(), flow.getPartitionKeyPath());
    }

    public KinesisRecord(TrackedFile file, long offset, byte[] data, long originalLength) {
        super(file, offset, data, originalLength);
        Preconditions.checkNotNull(file);
        KinesisFileFlow flow = (KinesisFileFlow)file.getFlow();
        partitionKey = generatePartitionKey(flow.getPartitionKeyOption(), flow.getPartitionKeyPath());
    }
    
    public String partitionKey() {
        return partitionKey;
    }

    @Override
    public long lengthWithOverhead() {
        return length() + KinesisConstants.PER_RECORD_OVERHEAD_BYTES;
    }
    
    @Override
    public long length() {
        return dataLength() + partitionKeyLength();
    }
    
    @Override
    protected int getMaxDataSize() {
        return KinesisConstants.MAX_RECORD_SIZE_BYTES - partitionKeyLength();
    }

    public int partitionKeyLength() {
        return partitionKey == null ? 0 : partitionKey.length();
    }
    
    @VisibleForTesting
    String generatePartitionKey(PartitionKeyOption option, List<String> partitionKeyPath) {
        Preconditions.checkNotNull(option);
        
        if (option == PartitionKeyOption.DETERMINISTIC) {
            Hasher hasher = Hashing.md5().newHasher();
            hasher.putBytes(data.array());
            return hasher.hash().toString();
        }
        else if (option == PartitionKeyOption.RANDOM)
            return "" + ThreadLocalRandom.current().nextDouble(1000000);

        else if (option == PartitionKeyOption.CUSTOM) {
            Preconditions.checkArgument(partitionKeyPath != null && !partitionKeyPath.isEmpty(),
                    "CUSTOM partition key option requires a partition key path");
            try (JsonParser parser = JSON_FACTORY.createParser(data.array(), (data.arrayOffset() + data.position()), data.remaining())) {
                return readPartitionKey(parser, partitionKeyPath);
            } catch (IOException e) {
                LOGGER.error("Failed to parse JSON data for partition key generation", e);
                throw new UncheckedIOException("Failed to generate partition key", e);
            }
        }
        else {

            throw new IllegalArgumentException("Unsupported partition key option: " + option);
        }
    }

    /**
     * Streams the record and returns the scalar value at {@code path}. The record must be a JSON object; each
     * segment is looked up only in the object reached so far, and every field off the path is skipped whole.
     * Every loop consumes a token and end of input is checked, so a malformed record cannot loop forever.
     */
    private static String readPartitionKey(JsonParser parser, List<String> path) throws IOException {
        if (parser.nextToken() != JsonToken.START_OBJECT) {
            throw new IllegalArgumentException("Record is not a JSON object");
        }
        for (int i = 0; i < path.size(); i++) {
            String segment = path.get(i);
            JsonToken token;
            while ((token = parser.nextToken()) == JsonToken.FIELD_NAME && !segment.equals(parser.currentName())) {
                parser.nextToken();      // move to the field's value
                parser.skipChildren();   // skip it whole if it is an object or array
            }
            if (token == null) {
                throw new IllegalArgumentException("Unexpected end of JSON record");
            }
            if (token != JsonToken.FIELD_NAME) {
                // Reached the end of this object without finding the segment.
                throw partitionKeyNotFound(path);
            }
            JsonToken value = parser.nextToken();
            if (i == path.size() - 1) {
                return scalarText(parser, value);
            }
            if (value != JsonToken.START_OBJECT) {
                throw partitionKeyNotFound(path);
            }
        }
        throw new IllegalStateException("Unreachable: partition key path is empty");
    }

    private static String scalarText(JsonParser parser, JsonToken value) throws IOException {
        if (value == null || value == JsonToken.VALUE_NULL) {
            throw new IllegalStateException("Partition key value cannot be null or empty");
        }
        if (!value.isScalarValue()) {
            throw new IllegalStateException("Partition key value must be a string, number or boolean, found " + value);
        }
        String text = parser.getText();
        if (text == null || text.isEmpty()) {
            throw new IllegalStateException("Partition key value cannot be null or empty");
        }
        return text;
    }

    private static NoSuchElementException partitionKeyNotFound(List<String> path) {
        return new NoSuchElementException("Partition key path not found: " + path);
    }
}
