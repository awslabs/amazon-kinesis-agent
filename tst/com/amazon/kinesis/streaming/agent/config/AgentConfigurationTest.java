/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.config;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.util.Strings;

import com.amazon.kinesis.streaming.agent.config.AgentConfiguration;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazonaws.ClientConfiguration;

public class AgentConfigurationTest {

    private AgentConfiguration getTestConfiguration(String fileName)
            throws IOException {
        try (InputStream i = this.getClass().getResourceAsStream(fileName)) {
            return new AgentConfiguration(Configuration.get(i));
        }
    }

    @Test
    public void testDefaultConfigs() {
        AgentConfiguration config = new AgentConfiguration(Collections.<String, Object> emptyMap());
        Assert.assertNull(config.accessKeyId());
        Assert.assertNull(config.secretKey());
        Assert.assertTrue(config.cloudwatchEmitMetrics());
        Assert.assertEquals(config.logEmitInternalMetrics(), false);
        Assert.assertEquals(config.logStatusReportingPeriodSeconds(), AgentConfiguration.DEFAULT_LOG_STATUS_REPORTING_PERIOD_SECONDS);
        Assert.assertEquals(config.sendingThreadsKeepAliveMillis(), AgentConfiguration.DEFAULT_SENDING_THREADS_KEEPALIVE_MILLIS);
        Assert.assertEquals(config.sendingThreadsMaxQueueSize(), AgentConfiguration.DEFAULT_SENDING_THREADS_MAX_QUEUE_SIZE);
        Assert.assertEquals(config.maxSendingThreadsPerCore(), AgentConfiguration.DEFAULT_MAX_SENDING_THREADS_PER_CORE);
        Assert.assertEquals(config.maxConnections(), config.defaultMaxConnections());
        Assert.assertEquals(config.connectionTimeoutMillis(), ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT);
        Assert.assertEquals(config.connectionTTLMillis(), ClientConfiguration.DEFAULT_CONNECTION_TTL);
        Assert.assertEquals(config.socketTimeoutMillis(), ClientConfiguration.DEFAULT_SOCKET_TIMEOUT);
        Assert.assertEquals(config.useHttpGzip(), ClientConfiguration.DEFAULT_USE_GZIP);
        Assert.assertEquals(config.useTcpKeepAlive(), ClientConfiguration.DEFAULT_TCP_KEEP_ALIVE);
        Assert.assertNull(config.logFile());
        Assert.assertNull(config.logLevel());
        Assert.assertEquals(config.logMaxBackupIndex(), -1);
        Assert.assertEquals(config.logMaxFileSize(), -1);
    }

    @Test
    public void testParseJSONConfigs() throws IOException {
        AgentConfiguration config = getTestConfiguration("agentconfig1.json");
        Assert.assertEquals(config.accessKeyId(), "ACCESS_ID");
        Assert.assertEquals(config.secretKey(), "SECRET_KEY");
        Assert.assertFalse(config.cloudwatchEmitMetrics());
        Assert.assertEquals(config.cloudwatchMetricsBufferTimeMillis(), 25);
        Assert.assertEquals(config.cloudwatchMetricsQueueSize(), 100);
        Assert.assertEquals(config.logEmitInternalMetrics(), true);
        Assert.assertEquals(config.logStatusReportingPeriodSeconds(), 12);
        Assert.assertEquals(config.sendingThreadsKeepAliveMillis(), 123000L);
        Assert.assertEquals(config.sendingThreadsMaxQueueSize(), 21);
        Assert.assertEquals(config.maxSendingThreadsPerCore(), 1);
        Assert.assertEquals(config.maxSendingThreads(), 8);
        Assert.assertEquals(config.maxConnections(), 1);
        Assert.assertEquals(config.connectionTimeoutMillis(), 12);
        Assert.assertEquals(config.connectionTTLMillis(), 123);
        Assert.assertEquals(config.socketTimeoutMillis(), 1234);
        Assert.assertEquals(config.useHttpGzip(), true);
        Assert.assertEquals(config.useTcpKeepAlive(), true);
        Assert.assertEquals(config.logLevel(), "ERROR");
        Assert.assertEquals(config.logFile(), Paths.get("/tmp/agent-test.log"));
        Assert.assertEquals(config.logMaxBackupIndex(), 12);
        Assert.assertEquals(config.logMaxFileSize(), 1234);
    }

    @Test
    public void testDefaultMaxSendingThreads() {
        AgentConfiguration config = new AgentConfiguration(Collections.<String, Object> emptyMap());
        int maxSendingThreadsPerCore = config.maxSendingThreadsPerCore();
        int numCores = Runtime.getRuntime().availableProcessors();
        Assert.assertEquals(config.defaultMaxSendingThreads(), numCores * maxSendingThreadsPerCore);
        Assert.assertEquals(config.maxSendingThreads(), numCores * maxSendingThreadsPerCore);
    }

    @Test
    public void testDefaultMaxConnections() {
        int highMaxThreads = 2 * ClientConfiguration.DEFAULT_MAX_CONNECTIONS;
        int lowMaxThreads = ClientConfiguration.DEFAULT_MAX_CONNECTIONS / 2;

        AgentConfiguration config = new AgentConfiguration(Collections.<String, Object> emptyMap());
        config = spy(config);
        when(config.maxSendingThreads()).thenReturn(highMaxThreads);
        Assert.assertEquals(config.defaultMaxConnections(), highMaxThreads);
        Assert.assertEquals(config.maxConnections(), highMaxThreads);

        config = new AgentConfiguration(Collections.<String, Object> emptyMap());
        config = spy(config);
        when(config.maxSendingThreads()).thenReturn(lowMaxThreads);
        Assert.assertEquals(config.defaultMaxConnections(), ClientConfiguration.DEFAULT_MAX_CONNECTIONS);
        Assert.assertEquals(config.maxConnections(), ClientConfiguration.DEFAULT_MAX_CONNECTIONS);
    }
    
    @Test
    public void testEndpointConfig() throws IOException {
    	AgentConfiguration config = getTestConfiguration("agentconfig1.json");
        Assert.assertEquals(config.firehoseEndpoint(), "https://firehose.us-east-1.amazonaws.com");
        Assert.assertTrue(Strings.isNullOrEmpty(config.kinesisEndpoint()));
        Assert.assertTrue(Strings.isNullOrEmpty(config.cloudwatchEndpoint()));
        Assert.assertTrue(Strings.isNullOrEmpty(config.stsEndpoint()));
    }
}
