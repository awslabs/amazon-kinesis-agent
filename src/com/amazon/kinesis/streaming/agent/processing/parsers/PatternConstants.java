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

/**
 * Common regex patterns used to construct log parsing format
 * 
 * @author chaocheq
 *
 */
public class PatternConstants {

    public static final String INT = "(?:[+-]?(?:[0-9]+))";
    public static final String POSINT = "(?:[1-9][0-9]*)";
    public static final String NONNEGINT = "(?:[0-9]+)";
    public static final String ANYDATA = ".*";
    public static final String WORD = "\\w+";
    public static final String NOTSPACE = "\\S+";
    public static final String NOQUOTE = "[^\"]+";
    
    // TODO: can be more restrict
    public static final String IP = "(?:[\\d\\.]+)";
    public static final String HOSTNAME = "(?:[\\w\\-]+)(?:\\.(?:[\\w\\-]+))*(?:\\.?|\\b)";
    public static final String IPORHOST = "(?:" + HOSTNAME + "|" + IP + ")";
    public static final String URIHOST = "(?:" + IPORHOST + "(?::" + POSINT + ")?)";
    public static final String USER = "(?:[a-zA-Z0-9._-]+)";
    
    public static final String MONTHDAY = "(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])";
    public static final String DAY = "(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)";
    public static final String MONTH = "\\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\\b";
    public static final String YEAR = "(?:[1-9]\\d*)";
    public static final String HOUR = "(?:2[0123]|[01]?[0-9])";
    public static final String MINUTE = "(?:[0-5][0-9])";
    public static final String SECOND = "(?:(?:[0-5]?[0-9]|60)(?:[:\\.,][0-9]+)?)";
    public static final String TIME = "(?:" + HOUR + ":" + MINUTE + ":" + SECOND + ")";
    public static final String HTTPDATE = "(?:" + MONTHDAY + "/" + MONTH + "/" + YEAR + ":" + TIME + "\\s" + INT + ")";
    public static final String APACHEERRORLOGTIMESTAMP = "(?:" + DAY + "\\s+" + MONTH + "\\s+" + MONTHDAY + "\\s+" + TIME + "\\s+" + YEAR + ")";
    public static final String SYSLOGTIMESTAMP = "(?:" + MONTH + "\\s+" + MONTHDAY + "\\s+" + TIME + ")";
    public static final String RFC3339 = "(?:\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?)(?:Z|(?:[-+]\\d{2}:\\d{2}))";
    
    public static final String SYSLOGFACILITY = "<" + NONNEGINT + "." + NONNEGINT + ">";
    public static final String PROG = "([\\w\\._/%-]+)";
    public static final String SYSLOGPROG = PROG + "(?:\\[(" + POSINT + ")\\])?";
    
    public static final String COMMON_APACHE_LOG = "^(" + IPORHOST + ") " +
                                                    "(" + USER + ") " +
                                                    "(" + USER + ") " +
                                                    "\\[(" + HTTPDATE + ")\\] " +
                                                    "\"(" + NOQUOTE + ")\" " +
                                                    "(" + INT + "|\\-) " +
                                                    "(" + INT + "|\\-)";
    
    public static final String COMBINED_APACHE_LOG = COMMON_APACHE_LOG + " " +
                                                    "\"(" + NOQUOTE + ")\" " +
                                                    "\"(" + NOQUOTE + ")\"";
    
    public static final String APACHE_ERROR_LOG = "^\\[(" + APACHEERRORLOGTIMESTAMP + ")\\] " +
                                                  "\\[(?:(" + WORD + "):)?(" + WORD + ")\\] " +
                                                  "\\[pid (" + NONNEGINT + ")(?::tid (" + NONNEGINT + "))?\\] " +
                                                  "\\[client (" + URIHOST + ")\\] " +
                                                  "(" + ANYDATA + ")";
    
    public static final String SYSLOG_BASE = "^(" + SYSLOGTIMESTAMP + ") " +
                                             "(?:" + SYSLOGFACILITY + " )?" +
                                             "(" + USER + ") " + 
                                             SYSLOGPROG + ": " +
                                             "(" + ANYDATA + ")";
    
    public static final String RFC3339_SYSLOG_BASE = "^(" + RFC3339 + ") " +
                                             "(?:" + SYSLOGFACILITY + " )?" +
                                             "(" + USER + ") " +
                                             SYSLOGPROG + ": " +
                                             "(" + ANYDATA + ")";
}
