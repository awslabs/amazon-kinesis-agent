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
package com.amazon.kinesis.streaming.agent.processing.processors;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

import com.amazon.kinesis.streaming.agent.processing.exceptions.InputOutPutTimeConversionException;
import com.amazon.kinesis.streaming.agent.processing.exceptions.WrongTimeZoneTimeConversionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.processing.exceptions.DataConversionException;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IDataConverter;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IJSONPrinter;
import com.amazon.kinesis.streaming.agent.processing.utils.ProcessingUtilsFactory;

/**
 * Convert a CSV record into JSON record.
 * 
 * customFieldNames is required. 
 * Optional delimiter other than comma can be configured.
 * Optional jsonFormat can be used for pretty printed json.
 * 
 * Configuration looks like:
 * {
 *     "optionName": "CSVTOJSON",
 *     "customFieldNames": [ "field1", "field2", ... ],
 *     "delimiter": "\\t"
 * }
 * 
 * @author chaocheq
 *
 */
public class CSVToJSONDataConverter implements IDataConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVToJSONDataConverter.class);
    
    private static String FIELDS_KEY = "customFieldNames";
    private static String DELIMITER_KEY = "delimiter";
    private final List<String> fieldNames;
    private final String delimiter;
    private final IJSONPrinter jsonProducer;
    private static String TIME_CONVERTER = "timeConverter";
    private static String FORMATTER_INPUT_KEY = "input";
    private static String FORMATTER_OUTPUT_KEY = "output";
    private static String TIMEZONE_KEY = "timezone";
    private final Map<String, Object> timeColumns;

    
    public CSVToJSONDataConverter(Configuration config) {
        fieldNames = config.readList(FIELDS_KEY, String.class);
        delimiter = config.readString(DELIMITER_KEY, ",");
        jsonProducer = ProcessingUtilsFactory.getPrinter(config);

        if (config.containsKey(TIME_CONVERTER)) {
            Configuration configuration2 = config.readConfiguration(TIME_CONVERTER);
            timeColumns = configuration2.getConfigMap();
        } else {
            timeColumns = new HashMap<String, Object>();
        }
    }

    @Override
    public ByteBuffer convert(ByteBuffer data) throws DataConversionException {
        final Map<String, Object> recordMap = new LinkedHashMap<String, Object>();
        String dataStr = ByteBuffers.toString(data, StandardCharsets.UTF_8);
        
        // Preserve the NEW_LINE at the end of the JSON record
        if (dataStr.endsWith(NEW_LINE)) {
            dataStr = dataStr.substring(0, (dataStr.length() - NEW_LINE.length()));
        }
        
        String[] columns = dataStr.split(delimiter);
        
        for (int i = 0; i < fieldNames.size(); i++) {
            try {
                recordMap.put(fieldNames.get(i), columns[i]);
            } catch (ArrayIndexOutOfBoundsException e) {
            	LoggerFactory.getLogger(getClass()).debug("Null field in CSV detected");
                recordMap.put(fieldNames.get(i), null);
            } catch (Exception e) {
                throw new DataConversionException("Unable to create the column map", e);
            }
        }

        for (String key : timeColumns.keySet()) {
            try {

                String timezone = (timeColumns.containsKey(TIMEZONE_KEY)) ?
                        timeColumns.get(TIMEZONE_KEY).toString()
                        : null;

                if(timezone!=null && !Arrays.asList(TimeZone.getAvailableIDs()).contains(timezone)) throw new WrongTimeZoneTimeConversionException("Such timezone does not exists");

                Object object = timeColumns.get(key);
                HashMap<String, String> hashMap = (object instanceof HashMap)
                        ? (HashMap) object
                        : new HashMap<String, String>();

                if (recordMap.containsKey(key)) {
                    if(!hashMap.containsKey(FORMATTER_INPUT_KEY)) throw new InputOutPutTimeConversionException("input is not defined");

                    SimpleDateFormat in = new SimpleDateFormat(hashMap.get(FORMATTER_INPUT_KEY));
                    Date date = in.parse(recordMap.get(key).toString());

                    if(!hashMap.containsKey(FORMATTER_OUTPUT_KEY)) throw new InputOutPutTimeConversionException("output is not defined");

                    SimpleDateFormat out = new SimpleDateFormat(hashMap.get(FORMATTER_OUTPUT_KEY));

                    if(timezone!=null) out.setTimeZone(TimeZone.getTimeZone(timezone));

                    String formattedDate = out.format(date);

                    recordMap.put(key, formattedDate);
                }
            } catch (InputOutPutTimeConversionException e) {
                throw new DataConversionException(e.getMessage());
            } catch (WrongTimeZoneTimeConversionException e) {
                throw new DataConversionException(e.getMessage());
            } catch (Exception e) {
                LOGGER.warn("Failed to convert date ", e);
            }
        }

        String dataJson = jsonProducer.writeAsString(recordMap) + NEW_LINE;
        
        return ByteBuffer.wrap(dataJson.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + 
               "{ delimiter: [" + delimiter + "], " +
               "fields: " + fieldNames.toString() + "}";
    }
}
