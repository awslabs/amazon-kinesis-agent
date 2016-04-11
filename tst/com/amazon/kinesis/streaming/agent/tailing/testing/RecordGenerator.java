/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.testing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.concurrent.NotThreadSafe;

import lombok.Getter;
import lombok.Setter;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.google.common.base.Preconditions;

/**
 * Generates records consisting of a combination of deterministic and random
 * data.
 *
 * NOTE: The records are internally strings terminated by a newline, encoded
 * with UTF-8 and wrapped in a {@link ByteBuffer}, though different kind of
 * record formats/encodings/terminators can be implemented by overriding this
 * class.
 *
 * The generated records are guaranteed to also be lexicographically ordered
 * strings; i.e. if {@code r1} was generated before {@code r2} it will also
 * sort before {@code r2} lexicogaphically.
 */
@NotThreadSafe
public class RecordGenerator {
    // Default datetime example: Jun 1, 2015 3:14:11
    public static final String DEFAULT_TEST_RECORD_DATETIME_PATTERN = "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\s+(0?[1-9]|[1-2][0-9]|3[01]),\\s+(19[0-9]{2}|[2-9][0-9]{3}|[0-9]{2})\\s+(2[0-3]|0?[0-9]|1[0-9]):([0-5][0-9]):([0-5][0-9])";
    private static final double DEFAULT_RCORD_SIZE_JITER = 0.50;
    private static final int DEFAULT_AVERAGE_RECORD_SIZE = 1024;
    private static final Logger LOGGER = TestUtils.getLogger(RecordGenerator.class);

    @Getter @Setter private int averageRecordSize;
    @Getter @Setter private double recordSizeJitter;
    private boolean multiline;
    private ByteBuffer partialRecord = null;
    @Getter @Setter private String recordTag = null;

    public RecordGenerator(int averageRecordSize, double recordSizeJitter, boolean multiline) {
        this.averageRecordSize = averageRecordSize;
        this.recordSizeJitter = recordSizeJitter;
        this.multiline = multiline;
    }

    public RecordGenerator(int averageRecordSize, double recordSizeJitter) {
        this(averageRecordSize, recordSizeJitter, false);
    }

    public RecordGenerator(boolean multiline) {
        this(DEFAULT_AVERAGE_RECORD_SIZE, DEFAULT_RCORD_SIZE_JITER, multiline);
        }

    public RecordGenerator() {
        this(DEFAULT_AVERAGE_RECORD_SIZE, DEFAULT_RCORD_SIZE_JITER, false);
    }

    public byte[] getNewRecord() {
        double jitter = recordSizeJitter == 0.0 ? 1.0 : ThreadLocalRandom.current().nextDouble(1-recordSizeJitter/2, 1+recordSizeJitter/2);
        int recordSize = (int) Math.round(averageRecordSize * jitter);
        return getNewRecord(recordSize);
    }

    public byte[] getNewRecord(int recordSize) {
        String prefix = String.format("%s\t%010d",
                SimpleDateFormat.getDateTimeInstance().format(new Date()),
                TestUtils.uniqueCounter());
        if (recordTag != null) {
            prefix += "\t" + recordTag;
        }
        String data = RandomStringUtils.randomAlphanumeric(recordSize - prefix.length() - 2);
        if (multiline) {
            int numLines = ThreadLocalRandom.current().nextInt(1, 10);
            for (int i = 0; i < numLines; i++) {
                int insertIndex = ThreadLocalRandom.current().nextInt(0, data.length());
                data = data.substring(0, insertIndex) + getRecordTerminator() + data.substring(insertIndex + 1);
            }
        }
        String line = prefix + "\t" + data + getRecordTerminator();
        return line.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Discards any partial records.
     */
    public void discardPartialRecord() {
        partialRecord = null;
    }

    /**
     *
     * @param channel
     * @return Number of bytes written to channel.
     * @throws IOException
     */
    public long startAppendPartialRecordToChannel(FileChannel channel) throws IOException {
        Preconditions.checkState(partialRecord == null, "There's already an un-finished partial record.");
        ByteBuffer record = ByteBuffer.wrap(getNewRecord());
        int length = record.limit()/2;
        ByteBuffer firstHalf = ByteBuffers.getPartialView(record, 0, length);
        channel.write(firstHalf);
        channel.force(true);
        partialRecord = ByteBuffers.getPartialView(record, length, record.limit() - length);
        return length;
    }

    /**
     *
     * @param channel
     * @return Number of bytes written to channel.
     * @throws IOException
     */
    public long finishAppendPartialRecordToChannel(FileChannel channel) throws IOException {
        if(partialRecord == null)
            return 0;
        else {
            int length = partialRecord.limit();
            channel.write(partialRecord);
            channel.force(true);
            partialRecord = null;
            return length;
        }
    }

   /**
    * @param channel
    * @param nRecords
    * @return Number of bytes written to channel.
    * @throws IOException
    */
    public long appendRecordsToChannel(FileChannel channel, int nRecords) throws IOException {
        return appendRecordsToChannel(channel, nRecords, "channel");
    }

    /**
     *
     * @param channel
     * @param nRecords
     * @param name
     * @return Number of bytes written to channel.
     * @throws IOException
     */
    public long appendRecordsToChannel(FileChannel channel, int nRecords, String name) throws IOException {
        Preconditions.checkArgument(nRecords >= 1);
        long totalSize = finishAppendPartialRecordToChannel(channel);
        int recordCount = 0;
        if(totalSize > 0) {
            ++recordCount;
        }
        while (recordCount < nRecords) {
            ByteBuffer record = ByteBuffer.wrap(getNewRecord());
            totalSize += record.limit();
            channel.write(record);
            ++recordCount;
        }
        channel.force(true);
        LOGGER.trace("Generated {} records ({} bytes) into {}.", recordCount, totalSize, name);
        return totalSize;
    }

    /**
     *
     * @param channel
     * @param maxBytes
     * @return Number of records written to channel.
     * @throws IOException
     */
    public int appendDataToChannel(FileChannel channel, long maxBytes) throws IOException {
        return appendDataToChannel(channel, maxBytes, "channel");
    }

    /**
     *
     * @param channel
     * @param maxBytes
     * @param name
     * @return Number of records written to channel.
     * @throws IOException
     */
    public int appendDataToChannel(FileChannel channel, long maxBytes, String name) throws IOException {
        long totalSize = finishAppendPartialRecordToChannel(channel);
        int recordCount = totalSize > 0 ? 1 : 0;
        while (totalSize < maxBytes) {
            ByteBuffer record = ByteBuffer.wrap(getNewRecord());
            totalSize += record.limit();
            if(totalSize >= maxBytes)
                break;
            channel.write(record);
            ++recordCount;
        }
        channel.force(true);
        LOGGER.trace("Generated {} records ({} bytes) into {}.", recordCount, totalSize, name);
        return recordCount;
    }

    /**
     *
     * @param file
     * @return Number of bytes written to file.
     * @throws IOException
     */
    public long startAppendPartialRecordToFile(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            return startAppendPartialRecordToChannel(channel);
        }
    }

    /**
     *
     * @param file
     * @return Number of bytes written to file.
     * @throws IOException
     */
    public long finishAppendPartialRecordToFile(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            return finishAppendPartialRecordToChannel(channel);
        }
    }

    /**
     *
     * @param file
     * @param nRecords
     * @return Number of bytes written to file.
     * @throws IOException
     */
    public long appendRecordsToFile(Path file, int nRecords) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            return appendRecordsToChannel(channel, nRecords, file.toString());
        }
    }

    /**
     * )
     * @param file
     * @param maxBytes
     * @return Number of records written to file.
     * @throws IOException
     */
    public int appendDataToFile(Path file, long maxBytes) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            return appendDataToChannel(channel, maxBytes, file.toString());
        }
    }

    public char getRecordTerminator() {
        return '\n';
    }
}
