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

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.processing.exceptions.DataConversionException;
import com.amazon.kinesis.streaming.agent.processing.exceptions.LogParsingException;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IDataConverter;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IJSONPrinter;
import com.amazon.kinesis.streaming.agent.processing.utils.ProcessingUtilsFactory;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Convert a CSV record into JSON record.
 * <p>
 * customFieldNames is required.
 * Optional delimiter other than comma can be configured.
 * Optional jsonFormat can be used for pretty printed json.
 * <p>
 * Configuration looks like:
 * {
 * "optionName": "CSVTOJSON",
 * "customFieldNames": [ "field1", "field2", ... ],
 * "delimiter": "\\t"
 * }
 *
 * @author chaocheq
 */
public class CSVToJSONDataConverter implements IDataConverter {

    private static final String FIELDS_KEY = "customFieldNames";
    private static final String DELIMITER_KEY = "delimiter";
    private final List<String> fieldNames;
    private final String delimiter;
    private final IJSONPrinter jsonProducer;
    private final CSVParser csvParser;

    public CSVToJSONDataConverter(Configuration config) {
        fieldNames = config.readList(FIELDS_KEY, String.class);
        delimiter = config.readString(DELIMITER_KEY, ",");
        jsonProducer = ProcessingUtilsFactory.getPrinter(config);
        csvParser = new CSVParserBuilder()
                .withSeparator(StringEscapeUtils.unescapeJava(delimiter).charAt(0))
                .withQuoteChar('"')
                .withEscapeChar('\\')
                .build();
    }

    @Override
    public ByteBuffer convert(ByteBuffer data) throws DataConversionException {
        final Map<String, Object> recordMap = new LinkedHashMap<String, Object>();
        String dataStr = ByteBuffers.toString(data, StandardCharsets.UTF_8);

        // Preserve the NEW_LINE at the end of the JSON record
        if (dataStr.endsWith(NEW_LINE)) {
            dataStr = dataStr.substring(0, (dataStr.length() - NEW_LINE.length()));
        }

        try {
            String[] columns = csvParser.parseLine(dataStr);

            for (int i = 0; i < fieldNames.size(); ++i) {
                putRecord(recordMap, columns[i], i);
            }
        } catch (IOException e) {
            throw new LogParsingException("Could not parse line", e);
        }

        String dataJson = jsonProducer.writeAsString(recordMap) + NEW_LINE;

        return ByteBuffer.wrap(dataJson.getBytes(StandardCharsets.UTF_8));
    }

    private void putRecord(Map<String, Object> recordMap, String column, int i) throws DataConversionException {
        try {
            recordMap.put(fieldNames.get(i), column);
        } catch (ArrayIndexOutOfBoundsException e) {
            LoggerFactory.getLogger(getClass()).debug("Null field in CSV detected");
            recordMap.put(fieldNames.get(i), null);
        } catch (Exception e) {
            throw new DataConversionException("Unable to create the column map", e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() +
                "{ delimiter: [" + delimiter + "], " +
                "fields: " + fieldNames.toString() + "}";
    }
}
