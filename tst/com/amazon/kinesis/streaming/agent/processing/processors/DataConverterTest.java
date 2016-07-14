/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.processing.processors;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IDataConverter;

import java.nio.*;
import java.util.Arrays;
import java.util.HashMap;

public class DataConverterTest {
    
    @Test
    public void testBracketsDataConverter() throws Exception {
        final IDataConverter converter = new BracketsDataConverter();
        final String dataStr = "hello there!! :)";
        final String expectedStr = "{" + dataStr + "}";
        
        Assert.assertEquals(dataStr.length()+2, expectedStr.length());
        verifyDataConversion(converter, dataStr.getBytes(), expectedStr.getBytes());
    }
    
    @Test
    public void testSingleLineDataConverter() throws Exception {
        final IDataConverter converter = new SingleLineDataConverter();
        final String dataStr = "{\n" +
                               "   \"a\": {\n" +
                               "           \"b\": 1,\n" +
                               "           \"c\": null,\n" +
                               "           \"d\": true\n" +
                               "    },\n" +
                               "   \"e\": \"f\",\n" +
                               "   \"g\": 2\n" +
                               "     " +
                               "}\n";
        final String expectedStr = "{\"a\": {\"b\": 1,\"c\": null,\"d\": true},\"e\": \"f\",\"g\": 2}\n";
        verifyDataConversion(converter, dataStr.getBytes(), expectedStr.getBytes()); 
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testCSVToJSONDataConverter() throws Exception {
        final Configuration config = new Configuration(new HashMap<String, Object>() {{
            put("optionName", "CSVTOJSON");
            put("customFieldNames", Arrays.asList("column1", "column2", "column3", "column4"));
        }});
        final IDataConverter converter = new CSVToJSONDataConverter(config);
        final String dataStr = "value1, value2,valu\ne3,    value4\n";
        final String expectedStr = "{\"column1\":\"value1\",\"column2\":\" value2\",\"column3\":\"valu\\ne3\",\"column4\":\"    value4\"}\n";
        verifyDataConversion(converter, dataStr.getBytes(), expectedStr.getBytes()); 
        
        final String dataStrValuesFewerThanColumns = "value1, value2,value3\n";
        final String expectedStrValuesFewerThanColumns = "{\"column1\":\"value1\",\"column2\":\" value2\",\"column3\":\"value3\",\"column4\":null}\n";
        verifyDataConversion(converter, dataStrValuesFewerThanColumns.getBytes(), expectedStrValuesFewerThanColumns.getBytes()); 
        
        final String dataStrMoreThanColumns = "value1,value2,value3,value4,value5\n";
        final String expectedStrMoreThanColumns = "{\"column1\":\"value1\",\"column2\":\"value2\",\"column3\":\"value3\",\"column4\":\"value4\"}\n";
        verifyDataConversion(converter, dataStrMoreThanColumns.getBytes(), expectedStrMoreThanColumns.getBytes()); 
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testCSVToJSONDataConverterConvertingTSV() throws Exception {
        final Configuration config = new Configuration(new HashMap<String, Object>() {{
            put("optionName", "CSVTOJSON");
            put("customFieldNames", Arrays.asList("column1", "column2", "column3", "column4"));
            put("delimiter", "\\t");
        }});
        final IDataConverter converter = new CSVToJSONDataConverter(config);
        final String dataStr = "value1\t value2\tvalue3\t    value4\n";
        final String expectedStr = "{\"column1\":\"value1\",\"column2\":\" value2\",\"column3\":\"value3\",\"column4\":\"    value4\"}\n";
        verifyDataConversion(converter, dataStr.getBytes(), expectedStr.getBytes()); 
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testApacheLogDataConverterCombinedApacheLog() throws Exception {
        final Configuration config = new Configuration(new HashMap<String, Object>() {{
            put("optionName", "LOGTOJSON");
            put("logFormat", "COMBINEDAPACHELOG");
        }});
        final IDataConverter converter = new LogToJSONDataConverter(config);

        final String dataStr = "123.45.67.89 - - [27/Oct/2000:09:27:09 -0400] \"GET /java/javaResources.html HTTP/1.0\" 200 10450 \"-\" \"Mozilla/4.6 [en] (X11; U; OpenBSD 2.8 i386; Nav)\" \"ANY_RANDOM_DATA\"";
        final String expectedStr = "{\"host\":\"123.45.67.89\",\"ident\":null,\"authuser\":null,\"datetime\":\"27/Oct/2000:09:27:09 -0400\",\"request\":\"GET /java/javaResources.html HTTP/1.0\",\"response\":\"200\",\"bytes\":\"10450\",\"referer\":null,\"agent\":\"Mozilla/4.6 [en] (X11; U; OpenBSD 2.8 i386; Nav)\"}\n";
        verifyDataConversion(converter, dataStr.getBytes(), expectedStr.getBytes()); 
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testApacheLogDataConverterCommonApacheLog() throws Exception {
        final Configuration config = new Configuration(new HashMap<String, Object>() {{
            put("optionName", "LOGTOJSON");
            put("logFormat", "COMMONAPACHELOG");
        }});
        final IDataConverter converter = new LogToJSONDataConverter(config);

        final String logWithAdditionalFields = "123.45.67.89 - - [27/Oct/2000:09:27:09 -0400] \"GET /java/javaResources.html HTTP/1.0\" 200 10450 \"-\" \"Mozilla/4.6 [en] (X11; U; OpenBSD 2.8 i386; Nav)\"";
        final String expectedStr = "{\"host\":\"123.45.67.89\",\"ident\":null,\"authuser\":null,\"datetime\":\"27/Oct/2000:09:27:09 -0400\",\"request\":\"GET /java/javaResources.html HTTP/1.0\",\"response\":\"200\",\"bytes\":\"10450\"}\n";
        verifyDataConversion(converter, logWithAdditionalFields.getBytes(), expectedStr.getBytes()); 
        
        final String logWithoutAdditionalFields = "123.45.67.89 - - [27/Oct/2000:09:27:09 -0400] \"GET /java/javaResources.html HTTP/1.0\" 200 10450";
        verifyDataConversion(converter, logWithoutAdditionalFields.getBytes(), expectedStr.getBytes()); 
        
        final String logWithHostname = "lj1036.inktomisearch.com - - [27/Oct/2000:09:27:09 -0400] \"GET /java/javaResources.html HTTP/1.0\" 200 10450";
        final String expectedStrWithHostname = "{\"host\":\"lj1036.inktomisearch.com\",\"ident\":null,\"authuser\":null,\"datetime\":\"27/Oct/2000:09:27:09 -0400\",\"request\":\"GET /java/javaResources.html HTTP/1.0\",\"response\":\"200\",\"bytes\":\"10450\"}\n";
        verifyDataConversion(converter, logWithHostname.getBytes(), expectedStrWithHostname.getBytes()); 
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testApacheLogDataConverterApacheErrorLog() throws Exception {
        final Configuration config = new Configuration(new HashMap<String, Object>() {{
            put("optionName", "LOGTOJSON");
            put("logFormat", "APACHEERRORLOG");
        }});
        final IDataConverter converter = new LogToJSONDataConverter(config);

        final String typicalLog = "[Fri Sep 09 10:42:29.902022 2011] [core:error] [pid 35708:tid 4328636416] [client 72.15.99.187] File does not exist: /usr/local/apache2/htdocs/favicon.ico";
        final String expectedStr = "{\"timestamp\":\"Fri Sep 09 10:42:29.902022 2011\",\"module\":\"core\",\"severity\":\"error\",\"processid\":\"35708\",\"threadid\":\"4328636416\",\"client\":\"72.15.99.187\",\"message\":\"File does not exist: /usr/local/apache2/htdocs/favicon.ico\"}\n";
        verifyDataConversion(converter, typicalLog.getBytes(), expectedStr.getBytes()); 
        
        final String logWithoutTid = "[Fri Sep 09 10:42:29 2011] [core:error] [pid 35708] [client 72.15.99.187] File does not exist: /usr/local/apache2/htdocs/favicon.ico";
        final String expectedStrWithoutTid = "{\"timestamp\":\"Fri Sep 09 10:42:29 2011\",\"module\":\"core\",\"severity\":\"error\",\"processid\":\"35708\",\"threadid\":null,\"client\":\"72.15.99.187\",\"message\":\"File does not exist: /usr/local/apache2/htdocs/favicon.ico\"}\n";;
        verifyDataConversion(converter, logWithoutTid.getBytes(), expectedStrWithoutTid.getBytes()); 
        
        final String logWithoutModule = "[Fri Sep 09 10:42:29.902022 2011] [error] [pid 35708:tid 4328636416] [client 72.15.99.187:12345] File does not exist: /usr/local/apache2/htdocs/favicon.ico";
        final String expectedStrWithoutModule = "{\"timestamp\":\"Fri Sep 09 10:42:29.902022 2011\",\"module\":null,\"severity\":\"error\",\"processid\":\"35708\",\"threadid\":\"4328636416\",\"client\":\"72.15.99.187:12345\",\"message\":\"File does not exist: /usr/local/apache2/htdocs/favicon.ico\"}\n";
        verifyDataConversion(converter, logWithoutModule.getBytes(), expectedStrWithoutModule.getBytes()); 
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testApacheLogDataConverterInvalidLog() throws Exception {
        final Configuration config = new Configuration(new HashMap<String, Object>() {{
            put("optionName", "LOGTOJSON");
            put("logFormat", "COMMONAPACHELOG");
        }});
        final IDataConverter converter = new LogToJSONDataConverter(config);
        final String logWithoutAdditionalFields = "123.45.67.89 - - [27/Oct/2000:09:27:09 -0400] \"GET /java/javaResources.html HTTP/1.0\" 200";
        
        // the record should be filtered out
        Assert.assertNull(converter.convert(ByteBuffer.wrap(logWithoutAdditionalFields.getBytes())));
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testApacheLogDataConverterCustomCommonApacheLog() throws Exception {
        final Configuration config = new Configuration(new HashMap<String, Object>() {{
            put("optionName", "LOGTOJSON");
            put("logFormat", "COMMONAPACHELOG");
            put("customFieldNames", Arrays.asList("column1", "column2", "column3", "column4", "column5", "column6", "column7"));
        }});
        final IDataConverter converter = new LogToJSONDataConverter(config);

        final String dataStr = "123.45.67.89 - - [27/Oct/2000:09:27:09 -0400] \"GET /java/javaResources.html HTTP/1.0\" 200 10450";
        final String expectedStr = "{\"column1\":\"123.45.67.89\",\"column2\":null,\"column3\":null,\"column4\":\"27/Oct/2000:09:27:09 -0400\",\"column5\":\"GET /java/javaResources.html HTTP/1.0\",\"column6\":\"200\",\"column7\":\"10450\"}\n";
        verifyDataConversion(converter, dataStr.getBytes(), expectedStr.getBytes()); 
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testApacheLogDataConverterCustomPattern() throws Exception {
        final Configuration config = new Configuration(new HashMap<String, Object>() {{
            put("optionName", "LOGTOJSON");
            put("logFormat", "COMMONAPACHELOG");
            put("matchPattern", "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3})");
            put("customFieldNames", Arrays.asList("column1", "column2", "column3", "column4", "column5", "column6"));
        }});
        final IDataConverter converter = new LogToJSONDataConverter(config);

        final String dataStr = "123.45.67.89 - - [27/Oct/2000:09:27:09 -0400] \"GET /java/javaResources.html HTTP/1.0\" 200";
        final String expectedStr = "{\"column1\":\"123.45.67.89\",\"column2\":null,\"column3\":null,\"column4\":\"27/Oct/2000:09:27:09 -0400\",\"column5\":\"GET /java/javaResources.html HTTP/1.0\",\"column6\":\"200\"}\n";
        verifyDataConversion(converter, dataStr.getBytes(), expectedStr.getBytes()); 
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testSysLogDataConverter() throws Exception {
        final Configuration config = new Configuration(new HashMap<String, Object>() {{
            put("optionName", "LOGTOJSON");
            put("logFormat", "SYSLOG");
        }});
        final IDataConverter converter = new LogToJSONDataConverter(config);

        final String logWithPID = "Mar 12 12:00:08 server2 rcd[308]: Loaded 12 packages in 'ximian-red-carpet|351' (0.01878 seconds) ";
        final String expectedLogWithPID = "{\"timestamp\":\"Mar 12 12:00:08\",\"hostname\":\"server2\",\"program\":\"rcd\",\"processid\":\"308\",\"message\":\"Loaded 12 packages in 'ximian-red-carpet|351' (0.01878 seconds) \"}\n";
        verifyDataConversion(converter, logWithPID.getBytes(), expectedLogWithPID.getBytes()); 
        
        final String logWithoutPID = "Mar 12 12:01:02 server4 snort: Ports to decode telnet on: 21 23 25 119";
        final String expectedLogWithoutPID = "{\"timestamp\":\"Mar 12 12:01:02\",\"hostname\":\"server4\",\"program\":\"snort\",\"processid\":null,\"message\":\"Ports to decode telnet on: 21 23 25 119\"}\n";
        verifyDataConversion(converter, logWithoutPID.getBytes(), expectedLogWithoutPID.getBytes()); 
    }
    
    private void verifyDataConversion(IDataConverter converter, byte[] dataBin, byte[] expectedBin) throws Exception {
        ByteBuffer data = ByteBuffer.wrap(dataBin);
        ByteBuffer res = converter.convert(data);
        byte[] resBin = new byte[res.remaining()];
        res.get(resBin);
        Assert.assertEquals(resBin, expectedBin);
    }
}
