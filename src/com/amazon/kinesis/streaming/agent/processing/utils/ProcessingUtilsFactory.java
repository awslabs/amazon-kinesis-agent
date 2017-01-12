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
package com.amazon.kinesis.streaming.agent.processing.utils;

import java.util.List;

import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.config.ConfigurationException;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IDataConverter;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IJSONPrinter;
import com.amazon.kinesis.streaming.agent.processing.interfaces.ILogParser;
import com.amazon.kinesis.streaming.agent.processing.parsers.ApacheLogParser;
import com.amazon.kinesis.streaming.agent.processing.parsers.SysLogParser;
import com.amazon.kinesis.streaming.agent.processing.processors.BracketsDataConverter;
import com.amazon.kinesis.streaming.agent.processing.processors.CSVToJSONDataConverter;
import com.amazon.kinesis.streaming.agent.processing.processors.LogToJSONDataConverter;
import com.amazon.kinesis.streaming.agent.processing.processors.SingleLineDataConverter;
import com.amazon.kinesis.streaming.agent.processing.processors.AddMetadataConverter;

/**
 * The factory to create: 
 * 
 * IDataConverter
 * ILogParser
 * IJSONPrinter
 * 
 * @author chaocheq
 *
 */
public class ProcessingUtilsFactory {
    
    public static enum DataConversionOption {
    	ADDMETADATA,
        SINGLELINE,
        CSVTOJSON,
        LOGTOJSON,
        ADDBRACKETS
    }
    
    public static enum LogFormat {
        COMMONAPACHELOG,
        COMBINEDAPACHELOG,
        APACHEERRORLOG,
        SYSLOG,
        RFC3339SYSLOG
    }
    
    public static enum JSONFormat {
        COMPACT,
        PRETTYPRINT
    }
    
    public static final String CONVERSION_OPTION_NAME_KEY = "optionName";
    public static final String LOGFORMAT_KEY = "logFormat";
    public static final String MATCH_PATTERN_KEY = "matchPattern";
    public static final String CUSTOM_FIELDS_KEY = "customFieldNames";
    public static final String JSONFORMAT_KEY = "jsonFormat";
    
    public static IDataConverter getDataConverter(Configuration config) throws ConfigurationException {
        if (config == null) {
            return null;
        }
        
        DataConversionOption option = config.readEnum(DataConversionOption.class, CONVERSION_OPTION_NAME_KEY);
        return buildConverter(option, config);
    }
    
    public static ILogParser getLogParser(Configuration config) throws ConfigurationException {
        // format must be specified
        LogFormat format = config.readEnum(LogFormat.class, LOGFORMAT_KEY);
        List<String> customFields = null;
        if (config.containsKey(CUSTOM_FIELDS_KEY))
            customFields = config.readList(CUSTOM_FIELDS_KEY, String.class);
        String matchPattern = config.readString(MATCH_PATTERN_KEY, null);
        
        return buildLogParser(format, matchPattern, customFields);
    }

    public static IJSONPrinter getPrinter(Configuration config) throws ConfigurationException {
        JSONFormat format = config.readEnum(JSONFormat.class, JSONFORMAT_KEY, JSONFormat.COMPACT);
        switch (format) {
            case COMPACT:
                return new SimpleJSONPrinter();
            case PRETTYPRINT:
                return new PrettyJSONPrinter();
            default:
                throw new ConfigurationException("JSON format " + format.name() + " is not accepted");
        }
    }
    
    private static ILogParser buildLogParser(LogFormat format, String matchPattern, List<String> customFields) {
        switch (format) {
            case COMMONAPACHELOG:
            case COMBINEDAPACHELOG:
            case APACHEERRORLOG:
                return new ApacheLogParser(format, matchPattern, customFields);
            case SYSLOG:
            case RFC3339SYSLOG:
                return new SysLogParser(format, matchPattern, customFields);
            default:
                throw new ConfigurationException("Log format " + format.name() + " is not accepted");
        }
    }
    
    private static IDataConverter buildConverter(DataConversionOption option, Configuration config) throws ConfigurationException {
        switch (option) {
	    case ADDMETADATA:
		return new AddMetadataConverter(config);
            case SINGLELINE:
                return new SingleLineDataConverter();
            case CSVTOJSON:
                return new CSVToJSONDataConverter(config);
            case LOGTOJSON:
                return new LogToJSONDataConverter(config);
            case ADDBRACKETS:
                return new BracketsDataConverter();
            default:
                throw new ConfigurationException(
                        "Specified option is not implemented yet: " + option);
        }
    }
}
