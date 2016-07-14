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
package com.amazon.kinesis.streaming.agent.tailing;

import com.amazon.kinesis.streaming.agent.Constants;

public final class FirehoseConstants extends Constants {
    public static final String DESTINATION_KEY = "deliveryStream";
    
    public static final int PER_RECORD_OVERHEAD_BYTES = 0;
    public static final int MAX_RECORD_SIZE_BYTES = 1000 * 1024;
    public static final int PER_BUFFER_OVERHEAD_BYTES = 0;
    public static final int MAX_BATCH_PUT_SIZE_RECORDS = 500;
    public static final int MAX_BATCH_PUT_SIZE_BYTES = 4 * 1024 * 1024;
    public static final int MAX_BUFFER_SIZE_RECORDS = MAX_BATCH_PUT_SIZE_RECORDS;
    public static final int MAX_BUFFER_SIZE_BYTES = MAX_BATCH_PUT_SIZE_BYTES;
    public static final int DEFAULT_PARSER_BUFFER_SIZE_BYTES = MAX_BATCH_PUT_SIZE_BYTES;
}
