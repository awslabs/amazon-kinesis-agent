/*
 * Copyright (c) 2014-2017 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent;

import java.util.HashMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.config.AgentConfiguration;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;

public class AgentAWSCredentialsProviderChainTest extends TestBase {
    private static final String CREDENTIALS_PROVIDER_LOCATION = "userDefinedCredentialsProvider.location";
    private static final String CREDENTIALS_PROVIDER_CLASS = "userDefinedCredentialsProvider.classname";

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

    @Test
    public void testGetCredentialsFromUserDefinedCredentialsProvider() {
        AWSCredentialsProvider awsCredentialsProvider = getUserDefinedCredentialsProvider();
        Assert.assertEquals(awsCredentialsProvider.getCredentials().getAWSAccessKeyId(), "CUSTOM_CREDENTIALS_ACCESS_KEY");
        Assert.assertEquals(awsCredentialsProvider.getCredentials().getAWSSecretKey(), "CUSTOM_CREDENTIALS_SECRET_KEY");
    }

    @Test
    public void testGetCredentials_whenJarNotFound_ForUserDefinedCredentialsProvider() {
        AWSCredentialsProvider awsCredentialsProvider = getUserDefinedCredentialsProvider_WhenJarIsIncorrect();
        Assert.assertNotEquals(awsCredentialsProvider.getCredentials().getAWSAccessKeyId(), "CUSTOM_CREDENTIALS_ACCESS_KEY");
        Assert.assertNotEquals(awsCredentialsProvider.getCredentials().getAWSSecretKey(), "CUSTOM_CREDENTIALS_SECRET_KEY");
    }

    @Test
    public void testGetCredentials_whenClassIncorrect_ForUserDefinedCredentialsProvider() {
        AWSCredentialsProvider awsCredentialsProvider = getUserDefinedCredentialsProvider_WhenClassNameIsIncorrect();
        Assert.assertNotEquals(awsCredentialsProvider.getCredentials().getAWSAccessKeyId(), "CUSTOM_CREDENTIALS_ACCESS_KEY");
        Assert.assertNotEquals(awsCredentialsProvider.getCredentials().getAWSSecretKey(), "CUSTOM_CREDENTIALS_SECRET_KEY");

    }

    private AWSCredentialsProvider getUserDefinedCredentialsProvider() {
        AgentContext context = TestUtils.getTestAgentContext(new HashMap<String, Object>() {{
            put(CREDENTIALS_PROVIDER_CLASS, "com.amazon.kinesis.streaming.agent.TestUserDefinedCredentialsProvider");
            put(CREDENTIALS_PROVIDER_LOCATION, System.getProperty("user.dir"));
        }});
        return context.getAwsCredentialsProvider();
    }

    private AWSCredentialsProvider getUserDefinedCredentialsProvider_WhenClassNameIsIncorrect() {
        AgentContext context = TestUtils.getTestAgentContext(new HashMap<String, Object>() {{
            put(CREDENTIALS_PROVIDER_LOCATION, "com.amazon.kinesis.streaming.agent.blahblahProvider");
            put(CREDENTIALS_PROVIDER_CLASS, System.getProperty("user.dir") + "/TestUserDefinedCredentialsProvider.jar");
        }});
        return context.getAwsCredentialsProvider();
    }

    private AWSCredentialsProvider getUserDefinedCredentialsProvider_WhenJarIsIncorrect() {
        AgentContext context = TestUtils.getTestAgentContext(new HashMap<String, Object>() {{
            put(CREDENTIALS_PROVIDER_LOCATION, "com.amazon.kinesis.streaming.agent.TestUserDefinedCredentialsProvider");
            put(CREDENTIALS_PROVIDER_CLASS, "blahblahProvider.jar");
        }});
        return context.getAwsCredentialsProvider();
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
