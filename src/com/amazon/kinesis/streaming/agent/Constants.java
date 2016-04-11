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

import java.util.concurrent.TimeUnit;

public class Constants {
    public static final char NEW_LINE = '\n';
    public static final long DEFAULT_RETRY_INITIAL_BACKOFF_MILLIS = 100;
    public static final long DEFAULT_RETRY_MAX_BACKOFF_MILLIS = 10_000;
    public static final int DEFAULT_PUBLISH_QUEUE_CAPACITY = 100;
    public static final long DEFAULT_MAX_BUFFER_AGE_MILLIS = TimeUnit.MINUTES.toMillis(1);
    public static final long DEFAULT_WAIT_ON_FULL_PUBLISH_QUEUE_MILLIS = TimeUnit.MINUTES.toMillis(1);
    public static final long DEFAULT_WAIT_ON_EMPTY_PUBLISH_QUEUE_MILLIS = TimeUnit.MINUTES.toMillis(1);
}
