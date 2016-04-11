/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.testing;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import lombok.Cleanup;
import lombok.Getter;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.BeforeMethod;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseConstants;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;
import com.amazon.kinesis.streaming.agent.tailing.IRecord;
import com.amazon.kinesis.streaming.agent.tailing.RecordBuffer;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;

public abstract class TailingTestBase extends TestBase {
    public static final String DEFAULT_TEST_RECORD_REGEX_PATTERN = ".+\\t\\d+\\t";

    public static abstract class FileRotatorFactory {
        @Getter long targetFileSize;
        @Getter String prefix = "app.log";
        @Getter int maxFilesToKeepOnDisk = FileRotator.DEFAULT_MAX_FILES_TO_KEEP_ON_DISK;

        public FileRotatorFactory() {
            this(FileRotator.DEFAULT_MIN_ROTATED_FILE_SIZE_BYTES);
        }

        public FileRotatorFactory(long targetFileSize) {
            this.targetFileSize = targetFileSize;
        }

        public FileRotator create() {
            FileRotator rotator = createInstance();
            rotator.setMinNewFileSize(this.targetFileSize / 4);
            rotator.setMinRotatedFileSize(this.targetFileSize);
            rotator.setMaxFilesToKeepOnDisk(maxFilesToKeepOnDisk);
            return rotator;
        }

        public FileRotatorFactory withTargetFileSize(long targetFileSize) {
            this.targetFileSize = targetFileSize;
            return this;
        }

        public FileRotatorFactory withPrefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public FileRotatorFactory withMaxFilesToKeepOnDisk(int maxFilesToKeepOnDisk) {
            this.maxFilesToKeepOnDisk = maxFilesToKeepOnDisk;
            return this;
        }

        protected abstract FileRotator createInstance();
    }

    public class TruncateFileRotatorFactory extends FileRotatorFactory {
        public TruncateFileRotatorFactory() {
            super();
        }

        public TruncateFileRotatorFactory(long targetFileSize) {
            super(targetFileSize);
        }

        @Override
        protected FileRotator createInstance() {
            Path dir = globalTestFiles.getTmpDir().resolve("truncatelogs" + TestUtils.uniqueCounter());
            return new TruncateFileRotator(dir, prefix);
        }
    }

    public class RenameFileRotatorFactory extends FileRotatorFactory {
        public RenameFileRotatorFactory() {
            super();
        }

        public RenameFileRotatorFactory(long targetFileSize) {
            super(targetFileSize);
        }

        @Override
        protected FileRotator createInstance() {
            Path dir = globalTestFiles.getTmpDir().resolve("renamelogs" + TestUtils.uniqueCounter());
            return new RenameFileRotator(dir, prefix);
        }
    }

    public class CreateFileRotatorFactory extends FileRotatorFactory {
        public CreateFileRotatorFactory() {
            super();
        }

        public CreateFileRotatorFactory(long targetFileSize) {
            super(targetFileSize);
        }

        @Override
        protected FileRotator createInstance() {
            Path dir = globalTestFiles.getTmpDir().resolve("createlogs" + TestUtils.uniqueCounter());
            return new CreateFileRotator(dir, prefix);
        }
    }

    public class CopyFileRotatorFactory extends FileRotatorFactory {
        public CopyFileRotatorFactory() {
            super();
        }

        public CopyFileRotatorFactory(long targetFileSize) {
            super(targetFileSize);
        }

        @Override
        protected FileRotator createInstance() {
            Path dir = globalTestFiles.getTmpDir().resolve("copylogs" + TestUtils.uniqueCounter());
            return new CopyFileRotator(dir, prefix);
        }
    }

    protected TrackedFile sourceFileForTestRecords;
    protected long sourceFileForTestRecordsOffset;

    @BeforeMethod(alwaysRun=true)
    public void cleanupTestRecord() {
        sourceFileForTestRecords = null;
        sourceFileForTestRecordsOffset = 0;
    }

    protected void initTestRecord(FileFlow<FirehoseRecord> flow) {
        try {
            sourceFileForTestRecordsOffset = 0;
            sourceFileForTestRecords = new TrackedFile(flow, testFiles.createTempFile());
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        logger.debug("Initialized new test records file: {}", sourceFileForTestRecords);
    }

    protected FirehoseRecord getTestRecord(FileFlow<FirehoseRecord> flow) {
        return getTestRecord(flow, null);
    }

    protected FirehoseRecord getTestRecord(FileFlow<FirehoseRecord> flow, RecordGenerator generator) {
        generator = generator != null ? generator : new RecordGenerator();
        byte[] data = generator.getNewRecord();
        if (sourceFileForTestRecords == null)
            initTestRecord(flow);
        FirehoseRecord record = new FirehoseRecord(sourceFileForTestRecords, sourceFileForTestRecordsOffset, data);
        sourceFileForTestRecordsOffset += data.length;
        return record;
    }

    protected RecordBuffer<FirehoseRecord> getTestBuffer(FileFlow<FirehoseRecord> flow) {
        return getTestBuffer(flow, ThreadLocalRandom.current().nextInt(1, 50));
    }

    protected RecordBuffer<FirehoseRecord> getTestBuffer(FileFlow<FirehoseRecord> flow, int nrecords) {
        return getTestBuffer(flow, nrecords, null);
    }

    protected RecordBuffer<FirehoseRecord> getTestBuffer(FileFlow<FirehoseRecord> flow, int nrecords, RecordGenerator generator) {
        RecordBuffer<FirehoseRecord> buffer = new RecordBuffer<FirehoseRecord>(flow);
        while(nrecords > 0) {
            buffer.add(getTestRecord(flow, generator));
            --nrecords;
        }
        return buffer;
    }

    protected AgentContext getTestAgentContext() {
        return getTestAgentContext(null);
    }

    protected AgentContext getTestAgentContext(String inputGlob) {
        Map<String, Object> config = new HashMap<>();
        return getTestAgentContext(inputGlob, config);
    }

    public AgentContext getTestAgentContext(String inputGlob, Map<String, Object> config) {
        @SuppressWarnings("unchecked")
        List<Configuration> flows = (List<Configuration>) config.get("flows");
        if (flows == null)
            config.put("flows", flows = new ArrayList<>());

        if (flows.isEmpty()) {
            Map<String, Object> flow = getTestFlowConfig(inputGlob);
            flows.add(new Configuration(flow));
        }

        if (!config.containsKey("checkpointFile"))
            config.put("checkpointFile", globalTestFiles.getTempFilePath());

        if (!config.containsKey("cloudwatch.emitMetrics"))
            config.put("cloudwatch.emitMetrics", false);

        return TestUtils.getTestAgentContext(config);
    }

    protected Map<String, Object> getTestFlowConfig(String inputGlob) {
        inputGlob = inputGlob == null ? "/var/log/message.log*" : inputGlob;
        Map<String, Object> flow = new HashMap<>();
        flow.put("filePattern", inputGlob);
        flow.put(FirehoseConstants.DESTINATION_KEY, RandomStringUtils.randomAlphabetic(5));
        flow.put("retryInitialBackoffMillis", 5);
        flow.put("retryMaxBackoffMillis", 50);
        return flow;
    }

    protected void assertOutputFileRecordsMatchInputRecords(Path outputFile, int maxRecordSize,
            int expectedOversizedRecordcount, List<? extends IRecord> expected) throws IOException {
        assertRecordsMatch(getLines(null, outputFile), recordsToStrings(expected, maxRecordSize), 0,
                maxRecordSize, expectedOversizedRecordcount);
    }

    protected void assertOutputFileRecordsMatchInputRecords(Path outputFile, List<? extends IRecord> expected)
            throws IOException {
        assertOutputFileRecordsMatchInputRecords(outputFile, -1, -1, expected);
    }

    protected void assertOutputFileRecordsMatchInputFiles(Path outputFile, int skippedInputRecords, int maxRecordSize,
            int expectedOversizedRecordcount, Path... files) throws IOException {
        List<String> expected = getLines(null, files);
        assertRecordsMatch(getLines(null, outputFile), expected, skippedInputRecords, maxRecordSize, expectedOversizedRecordcount);
    }

    protected void assertOutputFileRecordsMatchInputFiles(Path outputFile, int maxRecordSize,
            int expectedOversizedRecordcount, Path... files) throws IOException {
        assertOutputFileRecordsMatchInputFiles(outputFile, 0, maxRecordSize, expectedOversizedRecordcount, files);
    }

    protected void assertOutputFileRecordsMatchInputFiles(Path outputFile, int skippedInputRecords, Path... files) throws IOException {
        assertOutputFileRecordsMatchInputFiles(outputFile, skippedInputRecords, -1, -1, files);
    }

    protected double assertOutputFileRecordsMatchInputFilesApproximately(Path outputFile, double maxMissingRatio, Path... files) throws IOException {
        return assertRecordsMatchApproximately(getLines(null, outputFile), getLines(null, files), maxMissingRatio);
    }


    protected void assertRecordsMatchInputFiles(List<? extends IRecord> actual, int maxRecordSize,
            int expectedOversizedRecordcount, Path... files) throws IOException {
        List<String> expected = new ArrayList<>(actual.size());
        getLines(expected, files);
        assertRecordsMatch(recordsToStrings(actual, maxRecordSize), expected, 0, maxRecordSize, expectedOversizedRecordcount);
    }

    protected void assertRegexParsedRecordsMatchInputFiles(List<? extends IRecord> actual, String pattern, int maxRecordSize, int expectedOversizedRecordcount, Path... files) throws IOException {
        List<String> expected = new ArrayList<>(actual.size());
        getRegexParsedExpectedRecords(expected, pattern, files);
        assertRecordsMatch(recordsToStrings(actual, maxRecordSize), expected, 0, maxRecordSize, expectedOversizedRecordcount);
    }


    protected void assertRecordsMatchInputFiles(List<? extends IRecord> records, Path... files) throws IOException {
        assertRecordsMatchInputFiles(records, -1, -1, files);
    }

    protected void assertRegexParsedRecordsMatchInputFiles(List<? extends IRecord> records, String pattern, Path... files) throws IOException {
        assertRegexParsedRecordsMatchInputFiles(records, pattern, -1, -1, files);
    }

    protected void assertSkipHeaderLinesRecordsMatchInputFiles(int skippedExpectedRecords, List<? extends IRecord> records, Path... files) throws IOException {
        assertSkipHeaderLinesRecordsMatchInputFiles(skippedExpectedRecords, records, -1, -1, files);
    }

    protected void assertSkipHeaderLinesRecordsMatchInputFiles(int skippedExpectedRecords, List<? extends IRecord> actual, int maxRecordSize, int expectedOversizedRecordcount, Path... files) throws IOException {
        List<String> expected = getLines(null, files);
        assertRecordsMatch(recordsToStrings(actual, maxRecordSize), expected, skippedExpectedRecords, maxRecordSize, expectedOversizedRecordcount);
    }

    protected void assertRecordsMatch(List<String> actual, List<String> expected) throws IOException {
        assertRecordsMatch(actual, expected, 0, -1, -1);
    }

    protected void assertRecordsMatch(List<String> actual, List<String> expected, int skippedExpectedRecords, int maxRecordSize, int expectedOversizedRecordcount) throws IOException {
        assertEquals(actual.size(), expected.size() - skippedExpectedRecords);
        // Sort both lists
        Collections.sort(expected);
        actual = dedupe(actual);
        Collections.sort(actual);

        // Now, the lists should be identical
        int actualOversizedRecordCount = 0;
        for(int i = 0; i < actual.size(); ++i) {
            if(maxRecordSize <= 0 || expected.get(i).length() <= maxRecordSize) {
                assertEquals(actual.get(i), expected.get(i + skippedExpectedRecords), "Record " + i + " does not match!");
            } else {
                ++actualOversizedRecordCount;
                // Only compare up to maxRecordSize
                assertEquals(actual.get(i), expected.get(i + skippedExpectedRecords).substring(0, maxRecordSize - 1) + "\n", "Record " + i + " does not match!");
            }
        }
        if(expectedOversizedRecordcount >= 0)
            assertEquals(actualOversizedRecordCount, expectedOversizedRecordcount);
    }

    protected double assertRecordsMatchApproximately(List<String> actual, List<String> expected, double maxMissingRatio) throws IOException {
        assertTrue(actual.size() <= expected.size(), "Actual size: " + actual.size() + " should be less than expected size: " + expected.size());
        // Sort both lists
        Collections.sort(expected);
        actual = dedupe(actual);
        Collections.sort(actual);

        // Skip the first records that don't match
        int index = 0;
        while(index < expected.size()) {
            if (expected.get(index).equals(actual.get(0)))
               break;
            ++index;
        }
        int missingAtBeginning = index;
        logger.debug("First {} expected records were not found", missingAtBeginning);

        // Now, count the records that match...
        int missingInMiddle = 0;
        for(; index < expected.size() && index - missingAtBeginning < actual.size(); ++index) {
            if(!actual.get(index - missingAtBeginning).equals(expected.get(index)))
                ++missingInMiddle;
        }
        int equal = index - missingAtBeginning - missingInMiddle;
        int missingAtEnd = expected.size() - index;
        logger.debug("After index {}, {} matched and {} did not, " +
        		"and the last {} records were missing " +
        		"(actual size: {}, expected size: {}).",
        		missingAtBeginning, equal, missingInMiddle,
        		missingAtEnd, actual.size(), expected.size());
        assertTrue(equal <= actual.size());

        double missingRatio = ((double)missingAtBeginning + missingInMiddle + missingAtEnd) / expected.size();
        assertTrue(missingRatio <= maxMissingRatio, String.format("Missing records (%d%%) exceed allowed (%d%%)", (int)(missingRatio*100), (int)(maxMissingRatio*100)));

        return missingRatio;
    }

    private List<String> dedupe(List<String> records) {
        Set<String> uniques = new HashSet<>(records);
        int dupes = records.size() - uniques.size();
        if (dupes > 0) {
            logger.debug("Found {} duplicate records out of {}", dupes, records.size());
            records = new ArrayList<>(uniques);
        }
        return records;
    }

    protected List<String> recordsToStrings(List<? extends IRecord> records, int maxRecordSize) {
        List<String> strings = new ArrayList<>(records.size());
        for(IRecord record : records) {
            String strRecord = ByteBuffers.toString(record.data(), StandardCharsets.UTF_8);
            // Truncate the record to the given size if it's too long
            if (maxRecordSize >= 0 && strRecord.length() > maxRecordSize) {
                strRecord = strRecord.substring(0, maxRecordSize - 1) + "\n";
            }
            strings.add(strRecord);
        }
        return strings;
    }

    protected List<String> getRegexParsedExpectedRecords(List<String> records, String pattern, Path... files) throws IOException, FileNotFoundException {
        records = records == null ? new ArrayList<String>() : records;
        for (Path file : files) {
            @Cleanup Scanner scanner = new Scanner(file);
            scanner.useDelimiter(pattern);
            String recordPattern;
            String recordData;
            while((recordPattern = scanner.findWithinHorizon(pattern, 0)) != null){
                if (scanner.hasNext() && (recordData = scanner.next()) != null) {
                    records.add(recordPattern + recordData);
                }
            }
        }
        logger.debug("Found {} records in files: {}", records.size(), Joiner.on(",").join(files));
        return records;
    }

    protected List<String> getLines(List<String> lines, Path... files) throws IOException, FileNotFoundException {
        lines = lines == null ? new ArrayList<String>() : lines;
        for (Path file : files) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file.toFile())))) {
                String line = reader.readLine();
                while (line != null) {
                    lines.add(line + "\n");
                    line = reader.readLine();
                }
            }
        }
        logger.debug("Found {} records in files: {}", lines.size(), Joiner.on(",").join(files));
        return lines;
    }
}
