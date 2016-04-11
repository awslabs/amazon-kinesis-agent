/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent;

import java.util.HashMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.config.AgentConfiguration;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;

public class AgentAWSCredentialsProviderChainTest extends TestBase {
    @Test
    public void testGetCredentialsFromAgentConfig() {
        AWSCredentialsProvider credentialsProvider = getCredentialsProviderByKeyPair("CONFIG_ACCESS_ID", "CONFIG_SECRET_KEY");
        Assert.assertEquals(credentialsProvider.getCredentials().getAWSAccessKeyId(), "CONFIG_ACCESS_ID");
        Assert.assertEquals(credentialsProvider.getCredentials().getAWSSecretKey(), "CONFIG_SECRET_KEY");
    }

    @Test
    public void testGetCredentialsFromDefaultProvider() {
        // If the credentials read from agent config are invalid,
        // it will fall back to DefaultAWSCredentialsProviderChain.
        // We use system properties to test this
        String originAccessKeyProperty = System.setProperty(SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY, "PROPERTY_ACCESS_ID");
        String originSecretKeyProperty = System.setProperty(SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY, "PROPERTY_SECRET_KEY");

        try {
            AWSCredentialsProvider credentialsProvider = getCredentialsProviderByKeyPair("", "");
            Assert.assertEquals(credentialsProvider.getCredentials().getAWSAccessKeyId(), "PROPERTY_ACCESS_ID");
            Assert.assertEquals(credentialsProvider.getCredentials().getAWSSecretKey(), "PROPERTY_SECRET_KEY");
        } finally {
         // set the system properties back after testing
            if (originAccessKeyProperty != null)
                System.setProperty(SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY, originAccessKeyProperty);
            if (originSecretKeyProperty != null)
                System.setProperty(SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY, originSecretKeyProperty);
        }
    }

    private AWSCredentialsProvider getCredentialsProviderByKeyPair (final String accessKeyId, final String secretKey) {
        @SuppressWarnings("serial")
        AgentContext context = TestUtils.getTestAgentContext(new HashMap<String, Object>() {{
            put(AgentConfiguration.CONFIG_ACCESS_KEY, accessKeyId);
            put(AgentConfiguration.CONFIG_SECRET_KEY, secretKey);
        }});
        return context.getAwsCredentialsProvider();
    }
}
