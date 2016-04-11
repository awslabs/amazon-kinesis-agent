/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.processing.processors.AgentDataConverterChain;
import com.amazon.kinesis.streaming.agent.processing.processors.LogToJSONDataConverter;
import com.amazon.kinesis.streaming.agent.processing.processors.BracketsDataConverter;
import com.amazon.kinesis.streaming.agent.processing.processors.SingleLineDataConverter;
import com.amazon.kinesis.streaming.agent.tailing.AbstractParser;
import com.amazon.kinesis.streaming.agent.tailing.AbstractRecord;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.RegexSplitter;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow.InitialPosition;
import com.amazon.kinesis.streaming.agent.tailing.testing.RecordGenerator;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;

public abstract class AbstractParserTest<P extends AbstractParser<R>, R extends AbstractRecord> extends TailingTestBase {
    protected AgentContext context;
    protected FileFlow<R> flow;

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void setup() throws IOException {
        context = getTestAgentContext();
        flow = (FileFlow<R>) context.flows().get(0);
    }

    private List<R> parseAllRecords(P parser, List<R> records)
            throws IOException {
        return parseRecords(parser, -1, records);
    }

    private List<R> parseRecords(P parser, int maxRecords, List<R> records)
            throws IOException {
        records = records == null ? new ArrayList<R>() : records;
        R record = parser.readRecord();
        while (record != null) {
            // We don't expect empty records
            if (!record.shouldSkip()) {
                assertNotEquals(record.data().limit(), 0);
            }
            records.add(record);
            if(maxRecords > 0 && records.size() >= maxRecords)
                break;
            record = parser.readRecord();
        }
        return records;
    }

    @Test
    public void testStartParsingWithInitialPositionStartOfFile() throws IOException {
        flow = spy(flow);
        when(flow.getInitialPosition()).thenReturn(InitialPosition.START_OF_FILE);
        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        int expectedRecordCount = generator.appendDataToFile(testFile, getTestBytes());
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.startParsingFile(file));
        assertTrue(parser.isParsing());
        assertEquals(file.getCurrentOffset(), 0);
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), expectedRecordCount);
        assertTrue(records.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks
        assertRecordsMatchInputFiles(records, testFile);
        // Make sure no data was discarded
        verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());
    }

    @Test
    public void testStartParsingWithInitialPositionEndOfFile() throws IOException {
        flow = spy(flow);
        when(flow.getInitialPosition()).thenReturn(InitialPosition.END_OF_FILE);
        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        generator.appendDataToFile(testFile, getTestBytes());
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.startParsingFile(file));
        assertTrue(parser.isParsing());
        assertEquals(file.getCurrentOffset(), file.getSize());
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), 0);
        // Make sure no data was discarded
        verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());
    }

    @Test
    public void testStartParsingWithInitialPositionEndOfFileAndPartialRecord() throws IOException {
        flow = spy(flow);
        when(flow.getInitialPosition()).thenReturn(InitialPosition.END_OF_FILE);
        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        generator.appendDataToFile(testFile, getTestBytes());
        long recordBytes = generator.startAppendPartialRecordToFile(testFile);
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.startParsingFile(file));
        assertTrue(parser.isParsing());
        assertEquals(file.getCurrentOffset(), file.getSize());
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), 0);
        // Finish the partial record and continue reading
        recordBytes += generator.finishAppendPartialRecordToFile(testFile);
        records = parseAllRecords(parser, null);
        assertEquals(records.size(), 1);
        assertEquals(records.get(0).dataLength(), recordBytes);
        // Make sure no data was discarded
        verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());
    }

    @Test
    public void testReadRecordUntilEndOfFile() throws IOException {
        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        int expectedRecordCount = generator.appendDataToFile(testFile, getTestBytes());
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.switchParsingToFile(file));
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), expectedRecordCount);
        assertTrue(records.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks
        assertRecordsMatchInputFiles(records, testFile);
        // Make sure no data was discarded
        verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());
    }

    @Test
    public void testReadRecordWithPartialRecods() throws IOException {
        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        int expectedRecordCount = generator.appendDataToFile(testFile, getTestBytes());
        // Append a partial record
        generator.startAppendPartialRecordToFile(testFile);

        // Now start parsing the file, and observe the parser stop before the partial record
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.switchParsingToFile(file));
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), expectedRecordCount);
        assertTrue(records.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks

        // Now finish writing the partial record, and see the record count increase
        generator.finishAppendPartialRecordToFile(testFile);
        // Now continue reading records, and this time compare with file contents
        int oldSize = records.size();
        records = parseAllRecords(parser, records);
        // Make sure we read one more record
        assertEquals(records.size(), oldSize+1);
        assertRecordsMatchInputFiles(records, testFile);
        // Make sure no data was discarded
        verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());
    }

    @Test
    public void testReadRecordWithOversizedRecords() throws IOException {
        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        int expectedRecordCount = generator.appendDataToFile(testFile, getTestBytes());

        // Append few oversized records, then few more regular sized ones
        final int overSizedRecordCount = TestUtils.pickOne(1, 2, 3);
        final int extraRegularSizedRecords = 5;
        int oldAverageRecordSzie = generator.getAverageRecordSize();
        generator.setAverageRecordSize(5 * getTestBufferSize());
        generator.appendRecordsToFile(testFile, overSizedRecordCount);
        expectedRecordCount += overSizedRecordCount;
        generator.setAverageRecordSize(oldAverageRecordSzie);
        generator.appendRecordsToFile(testFile, extraRegularSizedRecords);
        expectedRecordCount += extraRegularSizedRecords;

        // Now start parsing the file
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.switchParsingToFile(file));
        List<R> records = parseAllRecords(parser, null);
        verify(parser, atLeast(1)).onDiscardedData(anyInt(), anyInt(), anyString());
        assertEquals(records.size(), expectedRecordCount);
        assertTrue(records.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks

        // Compare the records
        assertRecordsMatchInputFiles(records, flow.getMaxRecordSizeBytes(), overSizedRecordCount, testFile);
    }

    @Test
    public void testContinueParsingFromOffset() throws IOException {
        flow = spy(flow);
        // Doesn't matter what's the initial position is
        when(flow.getInitialPosition()).thenReturn(TestUtils.pickOne(InitialPosition.START_OF_FILE, InitialPosition.END_OF_FILE));
        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        generator.appendDataToFile(testFile, getTestBytes());

        // Open the file, pointing to the end
        TrackedFile file = new TrackedFile(flow, testFile);
        long offset = file.getSize();
        file.open(offset);

        // Write more data so that the offset is now the middle of the file
        int expectedRecordCount = generator.appendDataToFile(testFile, getTestBytes());

        // Initialize a new parser by continuing from a checkpoint
        P parser = buildParser();
        assertFalse(parser.isParsing());
        parser = spy(parser);
        assertTrue(parser.continueParsingWithFile(file));
        assertTrue(parser.isParsing());
        assertEquals(file.getCurrentOffset(), offset);
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), expectedRecordCount);

        // Make sure no data was discarded
        verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());
    }

    @Test
    public void testContinueWithDifferentFile() throws IOException {
        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        int expectedRecordCount = generator.appendDataToFile(testFile, getTestBytes());
        generator.startAppendPartialRecordToFile(testFile); // just for fun
        // Parse the file to the end
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file1 = new TrackedFile(flow, testFile);
        file1.open(0);
        assertTrue(parser.switchParsingToFile(file1));
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), expectedRecordCount);
        assertTrue(records.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks
        // Now write some more data, then rotate
        expectedRecordCount += generator.appendDataToFile(testFile, getTestBytes());
        // Simulate truncation by rotation
        Path testFile2 = testFiles.createTempFile();
        Files.copy(testFile, testFile2, StandardCopyOption.REPLACE_EXISTING);
        Files.write(testFile, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        TrackedFile file2 = new TrackedFile(flow, testFile2);
        file2.open(file1.getCurrentOffset());
        assertTrue(parser.continueParsingWithFile(file2));
        assertTrue(parser.isParsing());
        // Read remainder of file and compare
        records = parseAllRecords(parser, records);
        assertRecordsMatchInputFiles(records, file2.getPath());
        // Make sure no data was discarded
        verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());
    }

    @Test
    public void testContinueWithNullFile() throws IOException {
        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        generator.appendDataToFile(testFile, getTestBytes());
        generator.startAppendPartialRecordToFile(testFile); // just for fun

        // Parse few recrods from the file
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file1 = new TrackedFile(flow, testFile);
        file1.open(0);
        assertTrue(parser.switchParsingToFile(file1));
        parseRecords(parser, 10, null);

        // Keep track of how many bytes are left in current buffer, which will be discarded later
        int remainingInBuffer = parser.bufferedBytesRemaining();

        // Now set the current file to null
        assertFalse(parser.continueParsingWithFile(null));
        assertFalse(parser.isParsing());

        // The next call to parser.readRecord should return null
        assertNull(parser.readRecord());

        // Make sure expected amound of data was discarded
        verify(parser, times(1)).onDiscardedData(anyInt(), eq(remainingInBuffer), anyString());
    }

    @Test
    public void testContinueWhileParsingBuffer() throws IOException {
        Path testFile1 = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        generator.appendDataToFile(testFile1, getTestBytes());

        // Parse the file1 to the end
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file1 = new TrackedFile(flow, testFile1);
        file1.open(0);
        assertTrue(parser.switchParsingToFile(file1));
        List<R> records = parseAllRecords(parser, null);
        assertTrue(records.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks

        // Validate that we're at the end of the file, and the current buffer is empty
        assertFalse(parser.currentBuffer.hasRemaining());
        assertEquals(parser.currentFileChannel.position(), Files.size(testFile1));
        assertEquals(parser.bufferedBytesRemaining(), 0);

        // Add 2 more records, then read only 1
        generator.appendRecordsToFile(file1.getPath(), 2);
        R record1 = parser.readRecord();
        assertNotNull(record1);
        assertSame(record1.file(), file1);
        assertSame(parser.currentFile, file1);
        assertTrue(record1.startOffset() > 0);
        assertTrue(record1.endOffset() < Files.size(testFile1));
        assertTrue(parser.currentBuffer.hasRemaining()); // There's one more record in the buffer
        assertTrue(parser.bufferedBytesRemaining() > 0);
        assertEquals(parser.currentFileChannel.position(), Files.size(testFile1));

        // Now simulate rotation and write a single record into the new file
        Path testFile2 = testFiles.createTempFile();
        generator.appendRecordsToFile(testFile2, 1);
        TrackedFile file2 = new TrackedFile(flow, testFile2);
        file2.open(0);
        assertTrue(parser.continueParsingWithFile(file2));
        assertTrue(parser.isParsing());

        // Now read a single record from the parser... it should be the record from the previous file
        R record2 = parser.readRecord();
        assertNotNull(record2);
        assertSame(record2.file(), file1);
        assertSame(parser.currentFile, file2);
        assertTrue(record2.startOffset() > 0);
        assertEquals(record2.endOffset(), Files.size(testFile1));
        assertFalse(parser.currentBuffer.hasRemaining());

        // The following record should be the record from the new file
        R record3 = parser.readRecord();
        assertNotNull(record3);
        assertSame(record3.file(), file2);
        assertEquals(record3.startOffset(), 0);
        assertEquals(record3.endOffset(), Files.size(testFile2));
    }

    @Test
    public void testSwitchToNewFileDiscardsBuffer() throws IOException {
        Path testFile1 = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        int expectedRecordCount = generator.appendDataToFile(testFile1, getTestBytes());
        generator.startAppendPartialRecordToFile(testFile1); // just for fun
        // Parse the file to the end
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file1 = new TrackedFile(flow, testFile1);
        file1.open(0);
        assertTrue(parser.switchParsingToFile(file1));
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), expectedRecordCount);
        assertTrue(records.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks
        // Keep track of how many bytes are left in current buffer, which will be discarded later
        int remainingInBuffer = parser.bufferedBytesRemaining();

        // Now write some more data, then rotate, and reset parsing pointing to the new file
        generator.appendDataToFile(testFile1, getTestBytes());
        Path testFile2 = testFiles.createTempFile();
        generator.appendDataToFile(testFile2, getTestBytes());

        // Parse the latest file, instead of contiuing with older file
        TrackedFile file2 = new TrackedFile(flow, testFile2);
        file2.open(0);
        assertTrue(parser.switchParsingToFile(file2));

        // Read new file, and make sure that we read is exactly what is
        // expected in second file only and nothing was left over from
        // previous file.
        records = parseAllRecords(parser, null);
        assertRecordsMatchInputFiles(records, file2.getPath());

        // Make sure expected amound of data was discarded
        verify(parser, times(1)).onDiscardedData(anyInt(), eq(remainingInBuffer), anyString());
    }

    @Test
    public void testContiguousRecordBoundariesSingleFile() throws IOException {
        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        generator.appendDataToFile(testFile, getTestBytes());
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.switchParsingToFile(file));
        List<R> records = parseAllRecords(parser, null);
        assertTrue(records.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks
        // Check that record boundaries are contiguous
        assertEquals(records.get(0).startOffset(), 0);
        assertEquals(records.get(0).endOffset(), records.get(0).dataLength());
        for(int i = 1; i < records.size(); ++i) {
            assertEquals(records.get(i).endOffset(), records.get(i).startOffset() + records.get(i).dataLength());
            assertEquals(records.get(i).startOffset(), records.get(i-1).endOffset());
        }
        // Make sure no data was discarded
        verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());
    }

    @Test
    public void testParsingRecordsOnRegexFromGenerator() throws IOException {
        final String pattern = RecordGenerator.DEFAULT_TEST_RECORD_DATETIME_PATTERN;
        flow = spy(flow);
        when(flow.getRecordSplitter()).thenReturn(new RegexSplitter(pattern));
        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator(true);
        int expectedRecordCount = generator.appendDataToFile(testFile, getTestBytes());
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.switchParsingToFile(file));
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), expectedRecordCount);
        assertTrue(records.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks
        assertRegexParsedRecordsMatchInputFiles(records, pattern, testFile);
        // Make sure no data was discarded
        verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());
    }

    @Test
    public void testParsingRecordsOnRegexFromRealLog() throws IOException {
        // The test_service_log is taken from one Firehose Beta host and truncated to a reasonale size
        final String fileName = "test_service_log";
        final String pattern = "------------------------------------------------------------------------";
        // The number is counted by
        // grep PATTERN test_service_log | wc -l
        final int expectedRecordCount = 653;
        flow = spy(flow);
        when(flow.getRecordSplitter()).thenReturn(new RegexSplitter(pattern));
        Path testFile = FileSystems.getDefault().getPath(this.getClass().getResource(fileName).getFile());
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.switchParsingToFile(file));
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), expectedRecordCount);
        assertTrue(records.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks
        assertRegexParsedRecordsMatchInputFiles(records, pattern, testFile);
        // Make sure no data was discarded
        verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());
    }
    
    @Test
    public void testBuildingRecordOfConvertedData() throws IOException {
        flow = spy(flow);
        when(flow.getDataConverter()).thenReturn(new AgentDataConverterChain(new BracketsDataConverter()));
        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        final int expectedRecordCount = generator.appendDataToFile(testFile, getTestBytes());
        
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.switchParsingToFile(file));
        List<R> actualRecords = parseAllRecords(parser, null);
        List<String> expectedRecords = getLines(null, testFile);
        assertEquals(actualRecords.size(), expectedRecordCount);
        assertTrue(actualRecords.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks
        
        for(int i = 0; i < actualRecords.size(); i++) {
            String actualRecord = ByteBuffers.toString(actualRecords.get(i).data(), StandardCharsets.UTF_8);
            assertEquals(actualRecord, "{" + expectedRecords.get(i) + "}", "Record " + i + " does not match!");
        }
    }
    
    @Test
    public void testConvertingMultiLineDataFromRealLog() throws IOException {
        final String testfileName = "pretty_printed_json";
        final String expectedResultFileName = "parsed_singleline_json";
        final String pattern = "    \\{";
        flow = spy(flow);
        // Skip the first two lines
        when(flow.getSkipHeaderLines()).thenReturn(2);
        when(flow.getRecordSplitter()).thenReturn(new RegexSplitter(pattern));
        when(flow.getDataConverter()).thenReturn(new AgentDataConverterChain(new SingleLineDataConverter()));
        Path testFile = FileSystems.getDefault().getPath(this.getClass().getResource(testfileName).getFile());
        Path resultFile = FileSystems.getDefault().getPath(this.getClass().getResource(expectedResultFileName).getFile());
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.switchParsingToFile(file));
        List<R> actualRecords = parseAllRecords(parser, null);
        List<String> expectedRecords = getLines(null, resultFile);
        
        assertTrue(actualRecords.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks
        assertEquals(actualRecords.size(), expectedRecords.size());
        for(int i = 0; i < actualRecords.size(); i++) {
            String actualRecord = ByteBuffers.toString(actualRecords.get(i).data(), StandardCharsets.UTF_8);
            // getLines() preserves the NEWLINE at the end of each line
            assertEquals(actualRecord, 
                    expectedRecords.get(i).substring(0,  expectedRecords.get(i).length()-1) + "\n", 
                    "Record " + i + " does not match!");
        }
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testParsingApacheLogFromRealLog() throws IOException {
        final String testfileName = "apache_test_log";
        final String expectedResultFileName = "apache_test_log_json";
        // there are two records that don't match the log pattern
        final int expectedSkippedRecords = 2;
        flow = spy(flow);
        final Configuration config = new Configuration(new HashMap<String, Object>() {{
            put("optionName", "LOGTOJSON");
            put("logFormat", "COMMONAPACHELOG");
        }});
        when(flow.getDataConverter()).thenReturn(new AgentDataConverterChain(new LogToJSONDataConverter(config)));
        Path testFile = FileSystems.getDefault().getPath(this.getClass().getResource(testfileName).getFile());
        Path resultFile = FileSystems.getDefault().getPath(this.getClass().getResource(expectedResultFileName).getFile());
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.switchParsingToFile(file));
        List<R> actualRecords = parseAllRecords(parser, null);
        List<String> expectedRecords = getLines(null, resultFile);
        
        assertTrue(actualRecords.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks
        assertEquals(actualRecords.size(), expectedRecords.size() + expectedSkippedRecords);
        int numSkippedRecord = 0;
        for(int i = 0; i < actualRecords.size(); i++) {
            if (actualRecords.get(i).shouldSkip()) {
                numSkippedRecord++;
                continue;
            }
            String actualRecord = ByteBuffers.toString(actualRecords.get(i).data(), StandardCharsets.UTF_8);
            // getLines() preserves the NEWLINE at the end of each line
            assertEquals(actualRecord, 
                    expectedRecords.get(i - numSkippedRecord)
                    .substring(0,  expectedRecords.get(i - numSkippedRecord).length()-1) + "\n", 
                    "Record " + i + " does not match!");
        }
        assertEquals(numSkippedRecord, expectedSkippedRecords);
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testParsingSysLogFromRealLog() throws IOException {
        final String testfileName = "syslog_test";
        final String expectedResultFileName = "syslog_test_json";
        // there are two records that don't match the log pattern
        final int expectedSkippedRecords = 0;
        flow = spy(flow);
        final Configuration config = new Configuration(new HashMap<String, Object>() {{
            put("optionName", "LOGTOJSON");
            put("logFormat", "SYSLOG");
        }});
        when(flow.getDataConverter()).thenReturn(new AgentDataConverterChain(new LogToJSONDataConverter(config)));
        Path testFile = FileSystems.getDefault().getPath(this.getClass().getResource(testfileName).getFile());
        Path resultFile = FileSystems.getDefault().getPath(this.getClass().getResource(expectedResultFileName).getFile());
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.switchParsingToFile(file));
        List<R> actualRecords = parseAllRecords(parser, null);
        List<String> expectedRecords = getLines(null, resultFile);
        
        assertTrue(actualRecords.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks
        assertEquals(actualRecords.size(), expectedRecords.size() + expectedSkippedRecords);
        int numSkippedRecord = 0;
        for(int i = 0; i < actualRecords.size(); i++) {
            if (actualRecords.get(i).shouldSkip()) {
                numSkippedRecord++;
                continue;
            }
            String actualRecord = ByteBuffers.toString(actualRecords.get(i).data(), StandardCharsets.UTF_8);
            // getLines() preserves the NEWLINE at the end of each line
            assertEquals(actualRecord, 
                    expectedRecords.get(i - numSkippedRecord)
                    .substring(0,  expectedRecords.get(i - numSkippedRecord).length()-1) + "\n", 
                    "Record " + i + " does not match!");
        }
        assertEquals(numSkippedRecord, expectedSkippedRecords);
    }
    
    @Test
    public void testSkipHeadersSmallerThanBufferSize() throws IOException {
        final int bytesToSkip = (int) (0.6 * getTestBufferSize());
        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        // append data for skipping
        int skipHeaderLines = generator.appendDataToFile(testFile, bytesToSkip);
        // now append data for reading
        int expectedRecordCount = generator.appendDataToFile(testFile, getTestBytes());
        flow = spy(flow);
        when(flow.getSkipHeaderLines()).thenReturn(skipHeaderLines);
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.switchParsingToFile(file));
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), expectedRecordCount);
        assertTrue(records.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks
        assertSkipHeaderLinesRecordsMatchInputFiles(skipHeaderLines, records, testFile);
    }

    @Test
    public void testSkipHeadersLargerThanBufferSize() throws IOException {
        final int bytesToSkip = (int) (1.6 * getTestBufferSize());
        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        // append data for skipping
        generator.setRecordTag("HEADER");
        int skipHeaderLines = generator.appendDataToFile(testFile, bytesToSkip);
        // now append data for reading
        generator.setRecordTag("DATA");
        int expectedRecordCount = generator.appendDataToFile(testFile, getTestBytes());

        flow = spy(flow);
        when(flow.getSkipHeaderLines()).thenReturn(skipHeaderLines);
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.switchParsingToFile(file));
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), expectedRecordCount);
        assertTrue(records.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks
        assertSkipHeaderLinesRecordsMatchInputFiles(skipHeaderLines, records, testFile);
    }

    @Test
    public void testSkipEntireFile() throws IOException {
        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        generator.setRecordTag("HEADER");
        int numRecords = generator.appendDataToFile(testFile, getTestBytes());
        int skipHeaderLines = numRecords + 10;

        flow = spy(flow);
        when(flow.getSkipHeaderLines()).thenReturn(skipHeaderLines);
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.switchParsingToFile(file));
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), 0);
    }

    @Test
    public void testSkipHeadersUntilMultipleBatches() throws IOException {
        final int numRecordPerBatch = 2000;
        final int batch = 3;
        // To make sure we skip at least one batch
        final int skipHeaderLines = (int) 1.25 * numRecordPerBatch;

        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        flow = spy(flow);
        when(flow.getSkipHeaderLines()).thenReturn(skipHeaderLines);
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        assertTrue(parser.switchParsingToFile(file));
        List<R> records = new ArrayList<R>();

        for (int i = 1; i <= batch; i++) {
            generator.appendRecordsToFile(testFile, numRecordPerBatch);
            records = parseAllRecords(parser, records);
            int expectedRecordCount = Math.max(0, i * numRecordPerBatch - skipHeaderLines);
            assertEquals(records.size(), expectedRecordCount);
            if (expectedRecordCount > 0) {
                assertTrue(records.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks
                assertSkipHeaderLinesRecordsMatchInputFiles(skipHeaderLines, records, testFile);
            }
        }
    }

    @Test
    public void testSkipHeadersWithInitialPositionEndOfFile() throws IOException {
        final int skipHeaderLines = 5;
        flow = spy(flow);
        when(flow.getInitialPosition()).thenReturn(InitialPosition.END_OF_FILE);
        when(flow.getSkipHeaderLines()).thenReturn(skipHeaderLines);
        Path testFile = testFiles.createTempFile();
        RecordGenerator generator = new RecordGenerator();
        generator.setRecordTag("HEADER");
        int unreadRecordCount = generator.appendDataToFile(testFile, getTestBytes());
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        parser.startParsingFile(file);
        assertEquals(file.getCurrentOffset(), file.getSize());
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), 0);
        // Make sure no data was discarded
        verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());

        // Continue to write data into file so that parser will parse from the middle
        // We expect not to skip lines in this case
        generator.setRecordTag("DATA");
        int expectedRecordCount = generator.appendDataToFile(testFile, getTestBytes());
        records = parseAllRecords(parser, records);
        assertEquals(records.size(), expectedRecordCount);
        assertTrue(records.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks
        // This time it looks as if all the records written before first parse will be skipped during verification
        assertSkipHeaderLinesRecordsMatchInputFiles(unreadRecordCount, records, testFile);
        // Make sure no data was discarded
        verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());
    }

    @Test
    public void testSkipHeadersWithInitialPositionEndOfEmptyFile() throws IOException {
        final int skipHeaderLines = 5;
        flow = spy(flow);
        when(flow.getInitialPosition()).thenReturn(InitialPosition.END_OF_FILE);
        when(flow.getSkipHeaderLines()).thenReturn(skipHeaderLines);
        Path testFile = testFiles.createTempFile();
        P parser = buildParser();
        parser = spy(parser);
        TrackedFile file = new TrackedFile(flow, testFile);
        file.open(0);
        // Now this is just an empty file
        parser.startParsingFile(file);
        assertEquals(file.getCurrentOffset(), 0);
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), 0);
        // Make sure no data was discarded
        verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());

        // Start writing data into file so that parser will parse from the beginning
        // We expect to skip lines in this case

        // append data for skipping
        RecordGenerator generator = new RecordGenerator();
        generator.appendRecordsToFile(testFile, skipHeaderLines);
        int expectedRecordCount = generator.appendDataToFile(testFile, getTestBytes());
        records = parseAllRecords(parser, records);
        assertEquals(records.size(), expectedRecordCount);
        assertTrue(records.size() > 0);  // SANITYCHECK: Ensure we didn't shoot blanks
        // This time it looks as if all the records written before first parse will be skipped during verification
        assertSkipHeaderLinesRecordsMatchInputFiles(skipHeaderLines, records, testFile);
        // Make sure no data was discarded
        verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());
    }

    @Test
    public void testSkipHeadersWithNewFile() throws IOException {
        final int numRecords = 100;
        final int numHeaders = 2;

        flow = spy(flow);
        when(flow.getSkipHeaderLines()).thenReturn(numHeaders);

        RecordGenerator generator = new RecordGenerator();
        P parser = buildParser();
        Path testFile1 = testFiles.createTempFile();
        generator.setRecordTag("HEADER");
        generator.appendRecordsToFile(testFile1, numHeaders);
        generator.setRecordTag("DATA");
        generator.appendRecordsToFile(testFile1, numRecords);
        TrackedFile file1 = new TrackedFile(flow, testFile1);
        file1.open(0);
        assertTrue(parser.switchParsingToFile(file1));

        // Get and validate all records
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), numRecords);
        assertSkipHeaderLinesRecordsMatchInputFiles(numHeaders, records, testFile1);
        for(int i = 0; i < records.size(); ++i) {
            assertTrue(ByteBuffers.toString(records.get(i).data(), StandardCharsets.UTF_8).contains("\tDATA\t"));
            assertSame(records.get(i).file(), file1);
        }

        // Continue parsing with a new file and make sure headers are also skipped from second file
        int numRecords2 = 200;
        Path testFile2 = testFiles.createTempFile();
        generator.setRecordTag("HEADER");
        generator.appendRecordsToFile(testFile2, numHeaders);
        generator.setRecordTag("DATA");
        generator.appendRecordsToFile(testFile2, numRecords2);
        TrackedFile file2 = new TrackedFile(flow, testFile2);
        file2.open(0);
        // It shouldn't matter if we continue or switch...
        if (TestUtils.decide(0.5))
            assertTrue(parser.continueParsingWithFile(file2));
        else
            assertTrue(parser.switchParsingToFile(file2));
        records = parseAllRecords(parser, null);
        assertEquals(records.size(), numRecords2);
        assertSkipHeaderLinesRecordsMatchInputFiles(numHeaders, records, testFile2);
        for(int i = 0; i < records.size(); ++i) {
            assertTrue(ByteBuffers.toString(records.get(i).data(), StandardCharsets.UTF_8).contains("\tDATA\t"));
            assertSame(records.get(i).file(), file2);
        }
    }

    @Test
    public void testStopParsing() throws IOException {
        final int numRecords = 100;
        RecordGenerator generator = new RecordGenerator();
        P parser = buildParser();
        Path testFile1 = testFiles.createTempFile();
        generator.appendRecordsToFile(testFile1, numRecords);
        TrackedFile file1 = new TrackedFile(flow, testFile1);
        file1.open(0);
        assertTrue(parser.switchParsingToFile(file1));

        // Get and validate half the records
        List<R> records = parseRecords(parser, numRecords/2, null);
        assertEquals(records.size(), numRecords/2);

        // Now stop parsing, and validate state
        assertTrue(parser.stopParsing("Test"));
        assertFalse(parser.isParsing());
        assertNull(parser.currentBuffer);
        assertNull(parser.currentFile);
        assertNull(parser.readRecord());
    }

    @Test
    public void testIsAtEndOfCurrentFile() throws IOException {
        final int numRecords = 100;
        RecordGenerator generator = new RecordGenerator();
        P parser = buildParser();
        assertFalse(parser.isAtEndOfCurrentFile()); // before initialization
        Path testFile1 = testFiles.createTempFile();
        generator.appendRecordsToFile(testFile1, numRecords);
        // append a partial record
        generator.startAppendPartialRecordToFile(testFile1);
        TrackedFile file1 = new TrackedFile(flow, testFile1);
        file1.open(0);
        assertTrue(parser.switchParsingToFile(file1));

        // Get and validate al the records
        List<R> records = parseAllRecords(parser, null);
        assertEquals(records.size(), numRecords);
        // make sure we read to end of file
        assertNull(parser.readRecord());
        assertTrue(parser.bufferedBytesRemaining() > 0); // partial record
        // Now validate that we're at end of file
        assertTrue(parser.isAtEndOfCurrentFile());

        // Now write some more data to file, and see the last check changing
        generator.appendRecordsToFile(testFile1, numRecords);
        assertFalse(parser.isAtEndOfCurrentFile());
    }
    
    private P buildParser() {
    	return buildParser(flow, getTestBufferSize());
    }
    
    protected abstract int getTestBytes();
    protected abstract int getTestBufferSize();
    protected abstract P buildParser(FileFlow<R> flow, int bufferSize);
}
