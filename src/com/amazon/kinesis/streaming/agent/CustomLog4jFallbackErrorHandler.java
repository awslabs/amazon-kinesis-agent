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
package com.amazon.kinesis.streaming.agent;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.varia.FallbackErrorHandler;

public class CustomLog4jFallbackErrorHandler extends FallbackErrorHandler {
    private static StringBuilder errorHeader = new StringBuilder();

    /**
     * @return A string describing the error that triggered the fallback, or
     *         {@code null} if no errors occurred.
     */
    public static String getErrorHeader() {
        return errorHeader.length() == 0 ? null : errorHeader.toString();
    }

    public static String getFallbackLogFile() {
        return "/tmp/fallback-aws-kinesis-agent.log";
    }

    @Override
    public void error(String message, Exception e, int errorCode, LoggingEvent event) {
        errorHeader.append("# ********************************************************************************").append('\n');
        errorHeader.append("# ").append(new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z").format(new Date())).append('\n');
        errorHeader.append("# The following error was reported while initializing logging:").append('\n');
        errorHeader.append("#    " + message).append('\n');
        errorHeader.append("#    " + e.getClass().getName() + ": " + e.getMessage()).append('\n');
        errorHeader.append("# Please fix the problem and restart the application.").append('\n');
        errorHeader.append("# Logs will be redirected to " + getFallbackLogFile() + " in the meantime.").append('\n');
        errorHeader.append("# ********************************************************************************").append('\n');
        System.err.println(errorHeader.toString());
        super.error(message, e, errorCode, event);
    }

}
