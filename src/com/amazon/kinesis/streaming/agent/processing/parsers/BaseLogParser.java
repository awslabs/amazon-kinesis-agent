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

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.amazon.kinesis.streaming.agent.config.ConfigurationException;
import com.amazon.kinesis.streaming.agent.processing.exceptions.LogParsingException;
import com.amazon.kinesis.streaming.agent.processing.interfaces.ILogParser;
import com.amazon.kinesis.streaming.agent.processing.utils.ProcessingUtilsFactory.LogFormat;

/**
 * Base class for parsing log entries given fields.
 * 
 * TODO: support functions like filtering/adding/removing fields.
 * 
 * @author chaocheq
 *
 */
public abstract class BaseLogParser implements ILogParser {

    /**
     * The pattern used to filter the log entries
     */
    protected Pattern logEntryPattern;
    /**
     * The field names contained by each log entry
     * 
     * customField must be specified in the configuration if customer choose to use their own pattern to filter logs
     * 
     * customField can also be used to override the default field names in the pre-defined schemas (e.g. COMMONAPACHELOG, COMBINEDAPACHELOG)
     */
    protected List<String> fields;
    
    public BaseLogParser() {}
    
    public BaseLogParser(String pattern, List<String> fields) {
        setPattern(pattern);
        setFields(fields);
    }
    
    public BaseLogParser(LogFormat format, String matchPattern, 
            List<String> customFields) {
        // if matchPattern is specified, customFieldNames must be specified as well
        if (matchPattern != null && customFields == null) {
            throw new ConfigurationException(
                    "matchPattern must be specified when customFieldNames is specified");
        }
        if (matchPattern != null) {
            setPattern(matchPattern);
            setFields(customFields);
            return;
        }
        // if matchPattern is not specified, use default format
        initializeByDefaultFormat(format);
        // if customFieldNames is specified to override the default schema, we do a sanity check
        if (customFields != null) {
            if (customFields.size() != getFields().size()) {
                throw new ConfigurationException(
                        "Invalid custom field names for the selected log format. " +
                        "Please modify the field names or specify the matchPattern");
            }
            setFields(customFields);
        }
    }
    
    public List<String> getFields() {
        return fields;
    }
    
    public void setFields(List<String> fields) {
        this.fields = fields;
    }
    
    public String getPattern() {
        return logEntryPattern.pattern();
    }
    
    @Override
    public void setPattern(String pattern) {
        if (pattern == null) {
            throw new ConfigurationException("logPattern cannot be null");
        }
        this.logEntryPattern = Pattern.compile(pattern);
    }
    
    @Override
    public abstract Map<String, Object> parseLogRecord(String record, List<String> fields) throws LogParsingException;
    
    protected abstract void initializeByDefaultFormat(LogFormat format);
}
