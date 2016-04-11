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

import org.slf4j.Logger;

import com.amazon.kinesis.streaming.agent.config.AgentConfiguration;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.google.common.base.Strings;

public class AgentAWSCredentialsProvider implements AWSCredentialsProvider {
    private static final Logger LOGGER = Logging.getLogger(AgentAWSCredentialsProvider.class);
    private final AgentConfiguration config;
    
    public AgentAWSCredentialsProvider(AgentConfiguration config) {
        this.config = config;
    }
    
    public AWSCredentials getCredentials() {
        String accessKeyId = config.accessKeyId();
        String secretKey = config.secretKey();
        
        if (!Strings.isNullOrEmpty(accessKeyId) && !Strings.isNullOrEmpty(secretKey)) {
            LOGGER.debug("Loading credentials from agent config");
            return new BasicAWSCredentials(accessKeyId, secretKey);
        }
        
        throw new AmazonClientException("Unable to load credentials from agent config. Missing entries: " + 
                (Strings.isNullOrEmpty(accessKeyId) ? AgentConfiguration.CONFIG_ACCESS_KEY : "") +
                " " + (Strings.isNullOrEmpty(secretKey) ? AgentConfiguration.CONFIG_SECRET_KEY : ""));
    }

    public void refresh() {}

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
