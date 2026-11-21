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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import lombok.Getter;
import lombok.ToString;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.config.ConfigurationException;
import com.amazon.kinesis.streaming.agent.tailing.KinesisConstants.PartitionKeyOption;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpointStore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
/**
 * An implementation of a {@link FileFlow} where the destination is a kinesis stream.
 */
@ToString(callSuper=true)
public class KinesisFileFlow extends FileFlow<KinesisRecord> {
    public static final String PARTITION_KEY_PATH_DELIMITER = "/";
    // '~' is only valid as the start of an escape: "~0" or "~1".
    private static final Pattern INVALID_PATH_ESCAPE = Pattern.compile("~(?![01])");
    public static final Range<Long> VALID_MAX_BUFFER_AGE_RANGE_MILLIS = Range.closed(
            TimeUnit.MILLISECONDS.toMillis(1), TimeUnit.MINUTES.toMillis(15));
    public static final Range<Integer> VALID_MAX_BUFFER_SIZE_RECORDS_RANGE = Range.closed(1, KinesisConstants.MAX_BUFFER_SIZE_RECORDS);
    public static final Range<Integer> VALID_MAX_BUFFER_SIZE_BYTES_RANGE = Range.closed(1, KinesisConstants.MAX_BUFFER_SIZE_BYTES);
    public static final Range<Long> VALID_WAIT_ON_FULL_PUBLISH_QUEUE_MILLIS_RANGE = Range.closed(
            TimeUnit.SECONDS.toMillis(1), TimeUnit.MINUTES.toMillis(15));
    public static final Range<Long> VALID_WAIT_ON_EMPTY_PUBLISH_QUEUE_MILLIS_RANGE = Range.closed(
            TimeUnit.SECONDS.toMillis(1), TimeUnit.MINUTES.toMillis(15));

    @Getter protected final String id;
    @Getter protected final String destination;
    @Getter protected final PartitionKeyOption partitionKeyOption;
    /** Unescaped path segments to the CUSTOM partition key field, or null when the option is not CUSTOM. */
    @Getter protected final List<String> partitionKeyPath;

    public KinesisFileFlow(AgentContext context, Configuration config) {
        super(context, config);
        destination = readString(KinesisConstants.DESTINATION_KEY);
        id = "kinesis:" + destination + ":" + sourceFile.toString();
        partitionKeyOption = readEnum(PartitionKeyOption.class, KinesisConstants.PARTITION_KEY, PartitionKeyOption.RANDOM);
        partitionKeyPath = buildPartitionKeyPath(partitionKeyOption,
                readString(KinesisConstants.PARTITION_KEY_PATH, null));
    }

    /**
     * {@code partitionKeyOption = CUSTOM} requires {@code partitionKeyPath}.
     *
     * @return the parsed path for CUSTOM, otherwise null
     */
    @VisibleForTesting
    static List<String> buildPartitionKeyPath(PartitionKeyOption option, String path) {
        if (option != PartitionKeyOption.CUSTOM) {
            if (path != null) {
                throw new ConfigurationException(String.format("%s can only be set when %s is %s (found %s).",
                        KinesisConstants.PARTITION_KEY_PATH, KinesisConstants.PARTITION_KEY,
                        PartitionKeyOption.CUSTOM, option));
            }
            return null;
        }
        if (path == null) {
            throw new ConfigurationException(String.format("%s is required when %s is %s.",
                    KinesisConstants.PARTITION_KEY_PATH, KinesisConstants.PARTITION_KEY, PartitionKeyOption.CUSTOM));
        }
        return parsePartitionKeyPath(path);
    }

    /**
     * Splits a '/'-separated path into segments. In a segment, "~1" decodes to '/' and "~0" to '~'
     * (decoded in that order, as in JSON Pointer). Empty segments and any other use of '~' are rejected.
     */
    @VisibleForTesting
    static List<String> parsePartitionKeyPath(String value) {
        final String key = KinesisConstants.PARTITION_KEY_PATH;
        if (value.isEmpty()) {
            throw new ConfigurationException(String.format("%s cannot be empty.", key));
        }
        List<String> segments = new ArrayList<>();
        for (String raw : value.split(PARTITION_KEY_PATH_DELIMITER, -1)) {
            if (raw.isEmpty()) {
                throw new ConfigurationException(String.format("%s has an empty segment: '%s'.", key, value));
            }
            if (INVALID_PATH_ESCAPE.matcher(raw).find()) {
                throw new ConfigurationException(String.format(
                        "%s has an invalid escape: '%s'. Use ~1 for '/' and ~0 for '~'.", key, value));
            }
            segments.add(raw.replace("~1", "/").replace("~0", "~"));
        }
        return Collections.unmodifiableList(segments);
    }

    @Override
    public int getPerRecordOverheadBytes() {
        return KinesisConstants.PER_RECORD_OVERHEAD_BYTES;
    }

    @Override
    public int getMaxRecordSizeBytes() {
        return KinesisConstants.MAX_RECORD_SIZE_BYTES;
    }

    @Override
    public int getPerBufferOverheadBytes() {
        return KinesisConstants.PER_BUFFER_OVERHEAD_BYTES;
    }

    @Override
    public FileTailer<KinesisRecord> createNewTailer(
            FileCheckpointStore checkpoints,
            ExecutorService sendingExecutor) throws IOException {
        SourceFileTracker fileTracker = buildSourceFileTracker();
        AsyncPublisherService<KinesisRecord> publisher = getPublisher(checkpoints, sendingExecutor);
        return new FileTailer<KinesisRecord>(
        		agentContext, this, fileTracker,
                publisher, buildParser(), checkpoints);
    }

    @Override
    protected SourceFileTracker buildSourceFileTracker() throws IOException {
        return new SourceFileTracker(agentContext, this);
    }

    @Override
    protected AsyncPublisherService<KinesisRecord> getPublisher(
            FileCheckpointStore checkpoints,
            ExecutorService sendingExecutor) {
        return new AsyncPublisherService<>(agentContext, this, checkpoints,
                        buildSender(), sendingExecutor);
    }

    @Override
    protected IParser<KinesisRecord> buildParser() {
        return new KinesisParser(this, getParserBufferSize());
    }

    @Override
    protected ISender<KinesisRecord> buildSender() {
        return new KinesisSender(agentContext, this);
    }

    @Override
    public int getParserBufferSize() {
        return KinesisConstants.DEFAULT_PARSER_BUFFER_SIZE_BYTES;
    }

    @Override
    protected Range<Long> getWaitOnEmptyPublishQueueMillisValidRange() {
        return VALID_WAIT_ON_EMPTY_PUBLISH_QUEUE_MILLIS_RANGE;
    }

    @Override
    protected long getDefaultWaitOnEmptyPublishQueueMillis() {
        return KinesisConstants.DEFAULT_WAIT_ON_EMPTY_PUBLISH_QUEUE_MILLIS;
    }

    @Override
    protected Range<Long> getWaitOnPublishQueueMillisValidRange() {
        return VALID_WAIT_ON_FULL_PUBLISH_QUEUE_MILLIS_RANGE;
    }

    @Override
    protected long getDefaultWaitOnPublishQueueMillis() {
        return KinesisConstants.DEFAULT_WAIT_ON_FULL_PUBLISH_QUEUE_MILLIS;
    }

    @Override
    protected Range<Integer> getMaxBufferSizeBytesValidRange() {
        return VALID_MAX_BUFFER_SIZE_BYTES_RANGE;
    }

    @Override
    protected int getDefaultMaxBufferSizeBytes() {
        return KinesisConstants.MAX_BUFFER_SIZE_BYTES;
    }

    @Override
    protected Range<Integer> getBufferSizeRecordsValidRange() {
        return VALID_MAX_BUFFER_SIZE_RECORDS_RANGE;
    }

    @Override
    protected int getDefaultBufferSizeRecords() {
        return KinesisConstants.MAX_BUFFER_SIZE_RECORDS;
    }

    @Override
    protected Range<Long> getMaxBufferAgeMillisValidRange() {
        return VALID_MAX_BUFFER_AGE_RANGE_MILLIS;
    }

    @Override
    protected long getDefaultMaxBufferAgeMillis() {
        return KinesisConstants.DEFAULT_MAX_BUFFER_AGE_MILLIS;
    }

    @Override
    public long getDefaultRetryInitialBackoffMillis() {
        return KinesisConstants.DEFAULT_RETRY_INITIAL_BACKOFF_MILLIS;
    }

    @Override
    public long getDefaultRetryMaxBackoffMillis() {
        return KinesisConstants.DEFAULT_RETRY_MAX_BACKOFF_MILLIS;
    }

    @Override
    public int getDefaultPublishQueueCapacity() {
        return KinesisConstants.DEFAULT_PUBLISH_QUEUE_CAPACITY;
    }
}
