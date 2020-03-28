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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.tailing.KinesisConstants.PartitionKeyOption;
import com.amazonaws.partitions.model.Partition;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.sun.xml.internal.ws.wsdl.writer.document.Part;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KinesisRecord extends AbstractRecord {

    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisRecord.class);

    protected final String partitionKey;

    public KinesisRecord(TrackedFile file, long offset, ByteBuffer data, long originalLength) {
        super(file, offset, data, originalLength);
        Preconditions.checkNotNull(file);
        KinesisFileFlow flow = ((KinesisFileFlow)file.getFlow());
        partitionKey = generatePartitionKey(flow.getPartitionKeyOption(), flow.getPartitionKeyPattern());
    }

    public KinesisRecord(TrackedFile file, long offset, byte[] data, long originalLength) {
        super(file, offset, data, originalLength);
        Preconditions.checkNotNull(file);
        KinesisFileFlow flow = ((KinesisFileFlow)file.getFlow());
        partitionKey = generatePartitionKey(flow.getPartitionKeyOption(), flow.getPartitionKeyPattern());
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
        return dataLength() + partitionKey.length();
    }

    @Override
    protected int getMaxDataSize() {
        return KinesisConstants.MAX_RECORD_SIZE_BYTES - partitionKey.length();
    }

    @VisibleForTesting
    String generatePartitionKey(PartitionKeyOption option, Pattern pattern) {
        Preconditions.checkNotNull(option);

        if (option == PartitionKeyOption.DETERMINISTIC) {
            Hasher hasher = Hashing.md5().newHasher();
            hasher.putBytes(data.array());
            return hasher.hash().toString();
        }
        if (option == PartitionKeyOption.RANDOM)
            return "" + ThreadLocalRandom.current().nextDouble(1000000);
        if (option == PartitionKeyOption.PATTERN) {
            return generatePartitionKeyByPattern(pattern);
        }

        return null;
    }

    String generatePartitionKeyByPattern(Pattern pattern) {
        if (pattern == null) {
            LOGGER.error("PartitionKeyPattern is required when PATTERN is set as PartitionKeyOption.");
            return generatePartitionKey(KinesisConstants.DefaultPartitionKeyOption, null);
        }
        Matcher matcher = pattern.matcher(getRemainingString());
        if (matcher.matches() && matcher.groupCount() == 1) {
            return matcher.group(1);
        } else {
            LOGGER.error("Failed to generate partition key based on pattern (" + pattern + ") on line [" + getRemainingString() + "]. Fallback to default.");
            return generatePartitionKey(KinesisConstants.DefaultPartitionKeyOption, null);
        }
    }

    private String getRemainingString() {
        byte[] bytes = Arrays.copyOfRange(data.array(), data.arrayOffset(), data.arrayOffset() + data.remaining());
        return new String(bytes, 0, bytes.length).trim();
    }

}
