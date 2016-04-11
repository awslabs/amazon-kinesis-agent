/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.amazonaws.ClientConfiguration;

public class AgentContextTest {

    private Configuration getTestConfiguration(String fileName)
            throws IOException {
        try (InputStream i = this.getClass().getResourceAsStream(fileName)) {
            return Configuration.get(i);
        }
    }

    @Test
    public void testVersion() throws IOException {
        AgentContext context = new AgentContext(getTestConfiguration("agentconfig1.json"));
        assertNotNull(context.version());
        assertNotEquals(context.version(), "x.x");
    }

    @Test
    public void testDefaultUserAgent() throws IOException {
        AgentContext context = new AgentContext(getTestConfiguration("agentconfig1.json"));
        context = spy(context);
        when(context.version()).thenReturn("0.1");
        ClientConfiguration config = new ClientConfiguration();
        String userAgent = context.userAgent(config);
        assertTrue(userAgent.startsWith(AgentContext.DEFAULT_USER_AGENT + "/0.1"));
    }

    @Test
    public void testCustomUserAgent() throws IOException {
        AgentContext context = new AgentContext(TestUtils.getTestConfiguration(new Object[] {"userAgent", "test-ua"}));
        context = spy(context);
        when(context.version()).thenReturn("0.1");
        ClientConfiguration config = new ClientConfiguration();
        String userAgent = context.userAgent(config);
        assertTrue(userAgent.startsWith("test-ua " + AgentContext.DEFAULT_USER_AGENT + "/0.1"));
    }

    @Test
    public void testUserAgentOverride() throws IOException {
        AgentContext context = new AgentContext(TestUtils.getTestConfiguration(new Object[] {"userAgentOverride", "test-ua"}));
        ClientConfiguration config = new ClientConfiguration();
        assertEquals(context.userAgent(config), "test-ua");
    }

    @Test
    public void testGetAWSClientConfiguration() throws IOException {
        AgentContext context = new AgentContext(getTestConfiguration("agentconfig1.json"));
        context = spy(context);
        when(context.version()).thenReturn("0.1");
        ClientConfiguration config = context.getAwsClientConfiguration();
        assertTrue(config.getUserAgent().startsWith(AgentContext.DEFAULT_USER_AGENT + "/0.1"));
        assertEquals(config.getMaxConnections(), context.maxConnections());
        assertEquals(config.getConnectionTimeout(), context.connectionTimeoutMillis());
        assertEquals(config.getConnectionTTL(), context.connectionTTLMillis());
        assertEquals(config.getSocketTimeout(), context.socketTimeoutMillis());
        assertEquals(config.useTcpKeepAlive(), context.useTcpKeepAlive());
        assertEquals(config.useGzip(), context.useHttpGzip());
    }

    @Test
    public void testFlows() throws IOException {
        AgentContext context = new AgentContext(getTestConfiguration("agentconfig1.json"));

        assertEquals(context.flows().size(), 2);
        assertEquals(context.flows().get(0).getDestination(), "fh1");
        assertEquals(context.flows().get(0).getSourceFile().getDirectory().toString(), "/var/log");
        assertEquals(context.flows().get(0).getSourceFile().getFilePattern().toString(), "prefix1*");
        assertEquals(context.flows().get(0).getMaxBufferAgeMillis(), 10000);

        assertEquals(context.flows().get(1).getDestination(), "fh2");
        assertEquals(context.flows().get(1).getSourceFile().getDirectory().toString(), "/");
        assertEquals(context.flows().get(1).getSourceFile().getFilePattern().toString(), "prefix2*");
    }

}
