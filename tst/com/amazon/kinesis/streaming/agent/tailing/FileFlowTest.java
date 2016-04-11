/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.config.ConfigurationException;
import com.amazon.kinesis.streaming.agent.processing.processors.AgentDataConverterChain;
import com.amazon.kinesis.streaming.agent.processing.processors.CSVToJSONDataConverter;
import com.amazon.kinesis.streaming.agent.processing.processors.SingleLineDataConverter;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseFileFlow;
import com.amazon.kinesis.streaming.agent.tailing.RegexSplitter;
import com.amazon.kinesis.streaming.agent.tailing.SingleLineSplitter;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow.InitialPosition;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;

@SuppressWarnings("rawtypes")
public abstract class FileFlowTest<F extends FileFlow> extends TestBase {

    @SuppressWarnings("serial")
    public Configuration getConfiguration(final String fileName, final String destination) {
        return new Configuration(new HashMap<String, Object>() {{
            put("filePattern", fileName);
            put(getDestinationKey(), destination);
        }});
    }

    @Test
    public void testConfigurability() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("filePattern", "/tmp/testfile.log.*");
        configMap.put("pollPeriodMillis", 1234);
        configMap.put(getDestinationKey(), "testdes");
        configMap.put("maxBufferAgeMillis", 1230);
        configMap.put("maxBufferSizeRecords", 456);
        configMap.put("maxBufferSizeBytes", 789);
        configMap.put("retryInitialBackoffMillis", 123);
        configMap.put("retryMaxBackoffMillis", 567);
        configMap.put("initialPosition", "START_OF_FILE");
        configMap.put("multiLineStartPattern", ".+\\t\\d+\\t");
        configMap.put("skipHeaderLines", 5);
        configMap.put("truncatedRecordTerminator", "\nEOE\n");
        configMap.put("dataProcessingOptions", Arrays.asList(new Configuration(new HashMap<String, Object>() {{
                      put("optionName", "SINGLELINE");
                  }})));
        AgentContext context = TestUtils.getTestAgentContext();
        F ff = buildFileFlow(context, new Configuration(configMap));
        assertEquals(ff.getSourceFile().getDirectory(), Paths.get("/tmp"));
        assertEquals(ff.getSourceFile().getFilePattern(), Paths.get("testfile.log.*"));
        assertEquals(ff.getDestination(), "testdes");
        assertEquals(ff.getMaxBufferAgeMillis(), 1230);
        assertEquals(ff.getMaxBufferSizeRecords(), 456);
        assertEquals(ff.getMaxBufferSizeBytes(), 789);
        assertEquals(ff.getRetryInitialBackoffMillis(), 123);
        assertEquals(ff.getRetryMaxBackoffMillis(), 567);
        assertEquals(ff.getInitialPosition(), InitialPosition.START_OF_FILE);
        assertEquals(ff.getRecordSplitter().getClass(), RegexSplitter.class);
        assertEquals(((RegexSplitter) ff.getRecordSplitter()).getPattern(), ".+\\t\\d+\\t");
        assertEquals(ff.getSkipHeaderLines(), 5);
        assertEquals(ff.getRecordTerminatorBytes(), "\nEOE\n".getBytes(StandardCharsets.UTF_8));
        assertEquals(ff.getDataConverter().getClass(), AgentDataConverterChain.class);
        assertEquals(((AgentDataConverterChain) ff.getDataConverter()).getDataConverters().get(0).getClass(), SingleLineDataConverter.class);
    }

    @Test
    public void testConfigurationDefaults() {
        AgentContext context = TestUtils.getTestAgentContext();
        F ff = buildFileFlow(context, getConfiguration("/tmp/testfile.log.*", "testdes"));
        assertEquals(ff.getInitialPosition(), InitialPosition.END_OF_FILE);
        assertEquals(ff.getRecordSplitter().getClass(), SingleLineSplitter.class);
        assertEquals(ff.getSkipHeaderLines(), 0);
        assertEquals(ff.getRecordTerminatorBytes(), "\n".getBytes(StandardCharsets.UTF_8));
        assertNull(ff.getDataConverter());
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testDataConversionOptions() {
        AgentContext context = TestUtils.getTestAgentContext();
        final String file = "/var/log/message*";
        F ff1 = buildFileFlow(context, new Configuration(new HashMap<String, Object>() {{
            put("filePattern", file);
            put(getDestinationKey(), "des1");
            put(FileFlow.CONVERSION_OPTION_KEY, Arrays.asList(new Configuration(new HashMap<String, Object>() {{
                put("optionName", "SINGLELINE");
            }})));
        }}));
        F ff2 = buildFileFlow(context, new Configuration(new HashMap<String, Object>() {{
            put("filePattern", file);
            put(getDestinationKey(), "des2");
            put(FileFlow.CONVERSION_OPTION_KEY, Arrays.asList(new Configuration(new HashMap<String, Object>() {{
                put("optionName", "SINGLELINE");
            }}), new Configuration(new HashMap<String, Object>() {{
                put("optionName", "CSVTOJSON");
                put("customFieldNames", Arrays.asList("field1", "field2"));
            }})));
        }}));
        assertEquals(((AgentDataConverterChain) ff1.getDataConverter()).getDataConverters().get(0).getClass(), SingleLineDataConverter.class);
        assertEquals(((AgentDataConverterChain) ff2.getDataConverter()).getDataConverters().size(), 2);
        assertEquals(((AgentDataConverterChain) ff2.getDataConverter()).getDataConverters().get(0).getClass(), SingleLineDataConverter.class);
        assertEquals(((AgentDataConverterChain) ff2.getDataConverter()).getDataConverters().get(1).getClass(), CSVToJSONDataConverter.class);
    }
    
    @DataProvider(name="badDataConversionOptionInConfig")
    public Object[][] testDataConversionInConfigData(){
        return new Object[][] { 
                { new Configuration[] {new Configuration(new HashMap<String, Object>() {{
                    put("optionName", "SINGLELINE");
                }}), new Configuration(new HashMap<String, Object>() {{
                    put("optionName", "UNSUPPORTED");
                }})} }, 
                { new Configuration[] {new Configuration(new HashMap<String, Object>() {{
                    put("optionName", "singleline");
                }})} }, 
                { new Configuration[] {new Configuration(new HashMap<String, Object>() {{
                    put("optionName", "");
                }})} },
                { new Configuration[] {new Configuration(new HashMap<String, Object>() {{
                    put("optionName1", "SINGLELINE");
                }})} } 
               };
    }
    
    @SuppressWarnings("serial")
    @Test(dataProvider="badDataConversionOptionInConfig",
            expectedExceptions=ConfigurationException.class)
    public void testWrongDataConversionOption(final Configuration[] dataConversionOptions) {
        AgentContext context = TestUtils.getTestAgentContext();
        final String file = "/var/log/message*";
        F ff = buildFileFlow(context, new Configuration(new HashMap<String, Object>() {{
            put("filePattern", file);
            put(getDestinationKey(), "des");
            put(FileFlow.CONVERSION_OPTION_KEY, Arrays.asList(dataConversionOptions));
        }}));
        ff.getDataConverter();
    }

    // The data sources below use Firehose constants by default, 
    // any other concrete file flows should override the sources
    
    @DataProvider(name="badMaxBufferAgeMillisInConfig")
    public Object[][] testMaxBufferAgeMillisInConfigData(){
        return new Object[][] { { 0 }, { -1 },
                { FirehoseFileFlow.VALID_MAX_BUFFER_AGE_RANGE_MILLIS.lowerEndpoint() - 1 },
                { FirehoseFileFlow.VALID_MAX_BUFFER_AGE_RANGE_MILLIS.upperEndpoint() + 1 },
        };
    }

    @Test(dataProvider="badMaxBufferAgeMillisInConfig",
            expectedExceptions=ConfigurationException.class,
            expectedExceptionsMessageRegExp=".*maxBufferAgeMillis.*outside of valid range.*")
    public void testBadMaxBufferAgeMillisInConfig(long maxBufferAgeMillis) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("filePattern", "/tmp/testfile.log.*");
        configMap.put(getDestinationKey(), "testdes");
        configMap.put("maxBufferAgeMillis", maxBufferAgeMillis);
        AgentContext context = TestUtils.getTestAgentContext();
        buildFileFlow(context, new Configuration(configMap));
    }

    @DataProvider(name="badMaxBufferSizeRecordsInConfig")
    public Object[][] testBadMaxBufferSizeRecordsInConfigData(){
        return new Object[][] { { 0 }, { -1 },
                { FirehoseFileFlow.VALID_MAX_BUFFER_SIZE_RECORDS_RANGE.lowerEndpoint() - 1 },
                { FirehoseFileFlow.VALID_MAX_BUFFER_SIZE_RECORDS_RANGE.upperEndpoint() + 1 },
        };
    }

    @Test(dataProvider="badMaxBufferSizeRecordsInConfig",
            expectedExceptions=ConfigurationException.class,
            expectedExceptionsMessageRegExp=".*maxBufferSizeRecords.*outside of valid range.*")
    public void testBadMaxBufferSizeRecordsInConfig(long maxBufferSizeRecords) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("filePattern", "/tmp/testfile.log.*");
        configMap.put(getDestinationKey(), "testdes");
        configMap.put("maxBufferSizeRecords", maxBufferSizeRecords);
        AgentContext context = TestUtils.getTestAgentContext();
        buildFileFlow(context, new Configuration(configMap));
    }

    @DataProvider(name="badMaxBufferSizeBytesInConfig")
    public Object[][] testMaxBufferSizeBytesInConfigData(){
        return new Object[][] { { 0 }, { -1 },
                { FirehoseFileFlow.VALID_MAX_BUFFER_SIZE_BYTES_RANGE.lowerEndpoint() - 1 },
                { FirehoseFileFlow.VALID_MAX_BUFFER_SIZE_BYTES_RANGE.upperEndpoint() + 1 },
        };
    }

    @Test(dataProvider="badMaxBufferSizeBytesInConfig",
            expectedExceptions=ConfigurationException.class,
            expectedExceptionsMessageRegExp=".*maxBufferSizeBytes.*outside of valid range.*")
    public void testMaxBufferSizeBytesInConfig(long maxBufferSizeBytes) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("filePattern", "/tmp/testfile.log.*");
        configMap.put(getDestinationKey(), "testdes");
        configMap.put("maxBufferSizeBytes", maxBufferSizeBytes);
        AgentContext context = TestUtils.getTestAgentContext();
        buildFileFlow(context, new Configuration(configMap));
    }

    @Test
    public void testSameIdForSameFileAndDestination() {
        AgentContext context = TestUtils.getTestAgentContext();
        String file = "/var/log/message*";
        F ff1 = buildFileFlow(context, getConfiguration(file, "des1"));
        F ff2 = buildFileFlow(context, getConfiguration(file, "des1"));
        assertEquals(ff1.getId(), ff2.getId());
    }

    @Test
    public void testDifferentIdForSameFileButDifferentDestination() {
        AgentContext context = TestUtils.getTestAgentContext();
        String file = "/var/log/message*";
        F ff1 = buildFileFlow(context, getConfiguration(file, "des1"));
        F ff2 = buildFileFlow(context, getConfiguration(file, "des2"));
        assertNotEquals(ff1.getId(), ff2.getId());
    }

    @SuppressWarnings("serial")
    @Test
    public void testNullOrEmptyPatternInConfig() {
        AgentContext context = TestUtils.getTestAgentContext();
        final String file = "/var/log/message*";
        F ff1 = buildFileFlow(context, new Configuration(new HashMap<String, Object>() {{
            put("filePattern", file);
            put(getDestinationKey(), "des1");
            put("multiLineStartPattern", "");
        }}));
        F ff2 = buildFileFlow(context, new Configuration(new HashMap<String, Object>() {{
            put("filePattern", file);
            put(getDestinationKey(), "des2");
        }}));
        assertEquals(ff1.getRecordSplitter().getClass(), SingleLineSplitter.class);
        assertEquals(ff2.getRecordSplitter().getClass(), SingleLineSplitter.class);
    }

    @Test(enabled=false)
    public void testCreateFileTailer() {
        // TODO
    }
    
    protected abstract String getDestinationKey();
    protected abstract F buildFileFlow(AgentContext context, Configuration config);
}
