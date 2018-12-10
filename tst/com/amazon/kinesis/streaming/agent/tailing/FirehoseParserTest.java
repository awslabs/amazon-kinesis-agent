/*
 * Copyright (c) 2014-2017 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import com.amazon.kinesis.streaming.agent.tailing.testing.RecordGenerator;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class FirehoseParserTest extends AbstractParserTest<FirehoseParser, FirehoseRecord> {
    private final static int BUNCH_OF_BYTES = 30 * 1024 * 1024;
    private final static int TEST_BUFFER_SIZE = BUNCH_OF_BYTES / 10;   // This guarantees that multiple buffers are needed to read the file

	@Test
	public void testParsingRecordsWithAggregationFromGeneratorGivenLargeRecordSizeHint() throws IOException {
		// Line length in the file (avg/min/max): 1024/768/1280
		// Each record should generally contain 4 ~ 6 lines (unless there are fewer lines remaining in the current buffer)
		// Number of records should be (ceil(<line_count> / 6)) ~ (ceil(<line_count> / 4))
		final int averageRecordSize = 1024;
		final double recordSizeJitter = 0.5;
		final int recordSizeHint = 5 * 1024;
		flow = spy(flow);
		when(flow.getRecordSplitter()).thenReturn(new AggregationSplitter(recordSizeHint));
		Path testFile = testFiles.createTempFile();
		RecordGenerator generator = new RecordGenerator(averageRecordSize, recordSizeJitter);
		int lineCount = generator.appendDataToFile(testFile, getTestBytes());
		FirehoseParser parser = buildParser();
		parser = spy(parser);
		TrackedFile file = new TrackedFile(flow, testFile);
		file.open(0);
		assertTrue(parser.switchParsingToFile(file));
		List<FirehoseRecord> records = parseAllRecords(parser, null);
		assertTrue(records.size() >= (int) Math.ceil(lineCount / 6.0));
		assertTrue(records.size() <= (int) Math.ceil(lineCount / 4.0));
		assertAggregationParsedRecordsMatchInputFiles(records, recordSizeHint, testFile);
		verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());
	}

	@Test
	public void testParsingRecordsWithAggregationFromGeneratorGivenSmallRecordSizeHint() throws IOException {
		// Line length in the file (avg/min/max): 1024/768/1280
		// Each record should exactly contain 1 line
		// Number of records should be <line_count>
		final int averageRecordSize = 1024;
		final double recordSizeJitter = 0.5;
		final int recordSizeHint = 1024;
		flow = spy(flow);
		when(flow.getRecordSplitter()).thenReturn(new AggregationSplitter(recordSizeHint));
		Path testFile = testFiles.createTempFile();
		RecordGenerator generator = new RecordGenerator(averageRecordSize, recordSizeJitter);
		int lineCount = generator.appendDataToFile(testFile, getTestBytes());
		FirehoseParser parser = buildParser();
		parser = spy(parser);
		TrackedFile file = new TrackedFile(flow, testFile);
		file.open(0);
		assertTrue(parser.switchParsingToFile(file));
		List<FirehoseRecord> records = parseAllRecords(parser, null);
		assertEquals(records.size(), lineCount);
		assertAggregationParsedRecordsMatchInputFiles(records, recordSizeHint, testFile);
		verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());
	}

	@Test
	public void testParsingRecordsWithAggregationFromRealLogGivenLargeRecordSizeHint() throws IOException {
		// Number of lines in the file: 58
		// Line length in the file (avg/min/max): 736/695/1390
		// Each record should generally contain ~7 (= 5 * 1024 / 736) lines (unless there are fewer lines remaining in the current buffer)
		// Expected number of records is 9 (= ceil(58 / 7))
		final String fileName = "parsed_singleline_json";
		final int recordSizeHint = 5 * 1024;
		final int expectedRecordCount = 9;
		flow = spy(flow);
		when(flow.getRecordSplitter()).thenReturn(new AggregationSplitter(recordSizeHint));
		Path testFile = FileSystems.getDefault().getPath(this.getClass().getResource(fileName).getFile());
		FirehoseParser parser = buildParser();
		parser = spy(parser);
		TrackedFile file = new TrackedFile(flow, testFile);
		file.open(0);
		assertTrue(parser.switchParsingToFile(file));
		List<FirehoseRecord> records = parseAllRecords(parser, null);
		assertEquals(records.size(), expectedRecordCount);
		assertAggregationParsedRecordsMatchInputFiles(records, recordSizeHint, testFile);
		verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());
	}

	@Test
	public void testParsingRecordsWithAggregationFromRealLogGivenSmallRecordSizeHint() throws IOException {
		// Number of lines in the file: 58
		// Line length in the file (avg/min/max): 736/695/1390
		// Each record should exactly contain 1 line
		// Expected number of records is 58
		final String fileName = "parsed_singleline_json";
		final int recordSizeHint = 1024;
		final int expectedRecordCount = 58;
		flow = spy(flow);
		when(flow.getRecordSplitter()).thenReturn(new AggregationSplitter(recordSizeHint));
		Path testFile = FileSystems.getDefault().getPath(this.getClass().getResource(fileName).getFile());
		FirehoseParser parser = buildParser();
		parser = spy(parser);
		TrackedFile file = new TrackedFile(flow, testFile);
		file.open(0);
		assertTrue(parser.switchParsingToFile(file));
		List<FirehoseRecord> records = parseAllRecords(parser, null);
		assertEquals(records.size(), expectedRecordCount);
		assertAggregationParsedRecordsMatchInputFiles(records, recordSizeHint, testFile);
		verify(parser, never()).onDiscardedData(anyInt(), anyInt(), anyString());
	}

	@Override
	protected FirehoseParser buildParser(FileFlow<FirehoseRecord> flow, int bufferSize) {
        return new FirehoseParser(flow, bufferSize);
	}

	@Override
	protected int getTestBytes() {
		return BUNCH_OF_BYTES;
	}

	@Override
	protected int getTestBufferSize() {
		return TEST_BUFFER_SIZE;
	}
}
