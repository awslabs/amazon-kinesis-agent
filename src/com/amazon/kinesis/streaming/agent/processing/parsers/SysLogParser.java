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
package com.amazon.kinesis.streaming.agent.processing.parsers;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazon.kinesis.streaming.agent.config.ConfigurationException;
import com.amazon.kinesis.streaming.agent.processing.exceptions.LogParsingException;
import com.amazon.kinesis.streaming.agent.processing.utils.ProcessingUtilsFactory.LogFormat;
import com.google.common.collect.ImmutableList;

/**
 * Class for parsing and transforming records of sys log files
 * 
 * Syslog format can vary a lot across platforms depending on the configurations.
 * We are using the most typical form which is composed of:
 * 
 * timestamp, hostname, program, processid, message
 * 
 * @author chaocheq
 *
 */
public class SysLogParser extends BaseLogParser {
    
    /**
     * The fields below are present in most syslogs
     * 
     * TODO: facility is currently omitted because it varies across platforms
     */
    public static final List<String> SYSLOG_FIELDS = 
            ImmutableList.of("timestamp",
                             "hostname",
                             "program",
                             "processid",
                             "message");
    
    public static final Pattern BASE_SYSLOG_PATTERN = 
            Pattern.compile(PatternConstants.SYSLOG_BASE);
    
    public static final Pattern RFC3339_SYSLOG_PATTERN =
            Pattern.compile(PatternConstants.RFC3339_SYSLOG_BASE);
    
    public SysLogParser(LogFormat format, String matchPattern, 
            List<String> customFields) {
        super(format, matchPattern, customFields);
    }

    @Override
    public Map<String, Object> parseLogRecord(String record, List<String> fields) throws LogParsingException {
        if (fields == null) {
            fields = getFields();
        }
        final Map<String, Object> recordMap = new LinkedHashMap<String, Object>();
        Matcher matcher = logEntryPattern.matcher(record);
        
        if (!matcher.matches()) {
            throw new LogParsingException("Invalid log entry given the entry pattern");
        }
        
        if (matcher.groupCount() != fields.size()) {
            throw new LogParsingException("The parsed fields don't match the given fields");
        }
        
        for (int i = 0; i < fields.size(); i++) {
            // FIXME: what do we deal with the field that's missing?
            //        shall we pass in as null or don't even pass in the result?
            recordMap.put(fields.get(i), matcher.group(i + 1));
        }
        
        return recordMap;
    }

    @Override
    protected void initializeByDefaultFormat(LogFormat format) {
        switch (format) {
            case SYSLOG:
                this.logEntryPattern = BASE_SYSLOG_PATTERN;
                this.fields = SYSLOG_FIELDS;
                return;
            case RFC3339SYSLOG:
                this.logEntryPattern = RFC3339_SYSLOG_PATTERN;
                this.fields = SYSLOG_FIELDS;
                return;
            default:
                throw new ConfigurationException("Log format is not accepted");
    }
    }

}
