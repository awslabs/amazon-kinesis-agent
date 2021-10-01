package com.amazon.kinesis.streaming.agent;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUserDefinedCredentialsProvider implements AWSCredentialsProvider{
    private static final Logger LOGGER = LoggerFactory.getLogger(UserDefinedCredentialsProvider.class);

    public TestUserDefinedCredentialsProvider() {
    }

    @Override
    public AWSCredentials getCredentials() {
        LOGGER.info("Generating credentials from CustomCredentials Provider");
        return new BasicAWSCredentials("CUSTOM_CREDENTIALS_ACCESS_KEY", "CUSTOM_CREDENTIALS_SECRET_KEY");
    }

    @Override
    public void refresh() {}
}