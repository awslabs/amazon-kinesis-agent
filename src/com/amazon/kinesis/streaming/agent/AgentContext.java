/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.kinesis.streaming.agent.config.AgentConfiguration;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.config.ConfigurationException;
import com.amazon.kinesis.streaming.agent.metrics.IMetricsContext;
import com.amazon.kinesis.streaming.agent.metrics.IMetricsScope;
import com.amazon.kinesis.streaming.agent.metrics.Metrics;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FileFlowFactory;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.util.EC2MetadataUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Global context for the agent, including configuration, caches and transient
 * state.
 */
public class AgentContext extends AgentConfiguration implements IMetricsContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(AgentContext.class);

    @VisibleForTesting
    static final String DEFAULT_USER_AGENT = "aws-kinesis-agent";

    @VisibleForTesting
    public final FileFlowFactory fileFlowFactory;

    /** The listing of flows, ordered in order of appearance in configuration */
    private final Map<String, FileFlow<?>> flows = new LinkedHashMap<>();
    private AmazonKinesisFirehose firehoseClient;
    private AmazonKinesisClient kinesisClient;
    private AmazonCloudWatch cloudwatchClient;
    private IMetricsContext metrics;
    private String instanceTag = null;
    /**
     *
     * @param configuration
     * @throws ConfigurationException
     */
    public AgentContext(Configuration configuration) {
        this(configuration, new FileFlowFactory());
    }

    /**
     * @param configuration
     * @param fileFlowFactory
     * @throws ConfigurationException
     */
    public AgentContext(Configuration configuration, FileFlowFactory fileFlowFactory) {
        super(configuration);
        this.fileFlowFactory = fileFlowFactory;
        if (cloudwatchTagInstance()) {
            instanceTag = EC2MetadataUtils.getInstanceId();
            if (Strings.isNullOrEmpty(instanceTag)) {
                try {
                    instanceTag = InetAddress.getLocalHost().getHostName();
                } catch (UnknownHostException e) {
                    LOGGER.error("Cannot determine host name, instance tagging in CloudWatch metrics will be skipped.");
                }
            }
        }
        if (containsKey("flows")) {
            for (Configuration c : readList("flows", Configuration.class)) {
                FileFlow<?> flow = fileFlowFactory.getFileFlow(this, c);
                if (flows.containsKey(flow.getId()))
                    throw new ConfigurationException("Duplicate flow: " + flow.getId());
                flows.put(flow.getId(), flow);
            }
        }
    }


    /**
     * @return the version of this build.
     */
    public String version() {
        final String VERSION_INFO_FILE = "versionInfo.properties";
        try (InputStream versionInfoStream = Agent.class.getResourceAsStream(VERSION_INFO_FILE)) {
            Properties versionInfo = new Properties();
            versionInfo.load(versionInfoStream);
            return versionInfo.getProperty("version");
        } catch (IOException e) {
            LOGGER.error("Failed to read agent version from " + VERSION_INFO_FILE, e);
            return "x.x";
        }
    }

    /**
     * @return A new instance of a threadpool executor for sending data to
     *         destination.
     */
    public ThreadPoolExecutor createSendingExecutor() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("sender-%d").build();
        ThreadPoolExecutor tp = new ThreadPoolExecutor(maxSendingThreads(),
                maxSendingThreads(), sendingThreadsKeepAliveMillis(), TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(sendingThreadsMaxQueueSize()), threadFactory,
                new ThreadPoolExecutor.AbortPolicy());
        tp.allowCoreThreadTimeOut(true);
        return tp;
    }

    /**
     * @param config
     * @return the user agent component for this build.
     */
    public String userAgent(ClientConfiguration config) {
        if (containsKey("userAgentOverride")) {
            return readString("userAgentOverride");
        } else {
            String userAgentString = DEFAULT_USER_AGENT + "/" + version();
            if (config != null) {
                userAgentString += " " + config.getUserAgent();
            }
            String customAgent = readString("userAgent", null);
            if (customAgent != null && !customAgent.trim().isEmpty()) {
                userAgentString = customAgent.trim() + " " + userAgentString;
            }
            return userAgentString;
        }
    }

    @VisibleForTesting
    public synchronized AmazonKinesisFirehose getFirehoseClient() {
        if (firehoseClient == null) {
            firehoseClient = new AmazonKinesisFirehoseClient(
            		getAwsCredentialsProvider(), getAwsClientConfiguration());
            if (!Strings.isNullOrEmpty(firehoseEndpoint()))
                firehoseClient.setEndpoint(firehoseEndpoint());
        }
        return firehoseClient;
    }
    
    public synchronized AmazonKinesisClient getKinesisClient() {
        if (kinesisClient == null) {
            AmazonKinesisClientBuilder kinesisClientBuilder = AmazonKinesisClientBuilder.standard()
                    .withCredentials(getAwsCredentialsProvider())
                    .withClientConfiguration(getAwsClientConfiguration());
            if (!Strings.isNullOrEmpty(kinesisEndpoint()) && !Strings.isNullOrEmpty(kinesisRegion())) {
                kinesisClientBuilder.withEndpointConfiguration(new EndpointConfiguration(
                        kinesisEndpoint(), kinesisRegion()));
            }
            else if (!Strings.isNullOrEmpty(kinesisRegion())) {
                kinesisClientBuilder.withRegion(kinesisRegion());
            }
            kinesisClient = (AmazonKinesisClient) kinesisClientBuilder.build();

            // Edge case to maintain backwards compatability
            // A previous commit allowed setting kinesis.endpoint without kinesis.region; however, this overrides
            // the region if both of the properties are set. This maintains that functionality so users who have
            // only provided kinesis.endpoint will not break when upgrading to this commit
            if (!Strings.isNullOrEmpty(kinesisEndpoint()) && Strings.isNullOrEmpty(kinesisRegion())) {
                kinesisClient.setEndpoint(kinesisEndpoint());
            }
        }
        return kinesisClient;
    }

    private synchronized IMetricsContext getMetricsContext() {
        if(metrics == null) {
            metrics = new Metrics(this);
        }
        return metrics;
    }

    public AmazonCloudWatch getCloudWatchClient() {
        if (cloudwatchClient == null) {
            cloudwatchClient = new AmazonCloudWatchClient(
                    getAwsCredentialsProvider(), getAwsClientConfiguration());
            if (!Strings.isNullOrEmpty(cloudwatchEndpoint()))
                cloudwatchClient.setEndpoint(cloudwatchEndpoint());
        }
        return cloudwatchClient;
    }

    public AWSCredentialsProvider getAwsCredentialsProvider() {
    	AWSCredentialsProvider credentialsProvider = new AgentAWSCredentialsProviderChain(this);
    	final String assumeRoleARN = readString(ASSUME_ROLE_ARN, null);
    	if (!Strings.isNullOrEmpty(assumeRoleARN)) {
    		credentialsProvider = 
    				getSTSAssumeRoleSessionCredentialsProvider(assumeRoleARN, 
    						credentialsProvider);
    	}
    	return credentialsProvider;
    }
    
    public STSAssumeRoleSessionCredentialsProvider getSTSAssumeRoleSessionCredentialsProvider(
    		String roleARN, AWSCredentialsProvider credentialsProvider) {
    	Preconditions.checkNotNull(credentialsProvider);
    	final String stsEndpoint = stsEndpoint();
    	final String roleExternalId = readString(ASSUME_ROLE_EXTERNAL_ID, null);
    	
    	STSAssumeRoleSessionCredentialsProvider.Builder builder = 
    			new STSAssumeRoleSessionCredentialsProvider.Builder(roleARN, ASSUME_ROLE_SESSION)
    					.withLongLivedCredentialsProvider(credentialsProvider)
    					.withRoleSessionDurationSeconds(DEFAULT_ASSUME_ROLE_DURATION_SECONDS);
    	if (!Strings.isNullOrEmpty(roleExternalId)) {
    		builder = builder.withExternalId(roleExternalId);
    	}
    	if (!Strings.isNullOrEmpty(stsEndpoint)) {
    		builder = builder.withServiceEndpoint(stsEndpoint);
    	}
    	
    	return builder.build();
    }

    public ClientConfiguration getAwsClientConfiguration() {
        ClientConfiguration config = new ClientConfiguration();
        config.setUserAgent(userAgent(config));
        config.setMaxConnections(maxConnections());
        config.setConnectionTimeout(connectionTimeoutMillis());
        config.setSocketTimeout(socketTimeoutMillis());
        config.setUseTcpKeepAlive(useTcpKeepAlive());
        config.setConnectionTTL(connectionTTLMillis());
        config.setUseGzip(useHttpGzip());
        return config;
    }

    public synchronized FileFlow<?> flow(String flowId) {
        return flows.get(flowId);
    }

    public synchronized List<FileFlow<?>> flows() {
        return new ArrayList<>(flows.values());
    }

    @Override
    public IMetricsScope beginScope() {
        return getMetricsContext().beginScope();
    }
    
    public String getInstanceTag() {
    	return instanceTag;
    }
}
