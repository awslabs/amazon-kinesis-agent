package com.amazon.kinesis.streaming.agent;

import com.amazon.kinesis.streaming.agent.config.AgentConfiguration;
import com.amazon.kinesis.streaming.agent.config.ConfigurationException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class UserDefinedCredentialsProvider implements AWSCredentialsProvider{
    private static final String CREDENTIALS_PROVIDER_LOCATION = "userDefinedCredentialsProvider.location";
    private static final String CREDENTIALS_PROVIDER_CLASS = "userDefinedCredentialsProvider.classname";
    private static final String AGENT_LIB_DIRECTORY = "/usr/share/aws-kinesis-agent/lib/";
    private static final Logger LOGGER = LoggerFactory.getLogger(UserDefinedCredentialsProvider.class);
    private static final String AGENT_USER = "aws-kinesis-agent-user";


    private final AgentConfiguration configuration;
    private AWSCredentialsProvider awsCredentialsProvider = null;

    public UserDefinedCredentialsProvider(AgentConfiguration configuration) {
        this.configuration = configuration;
        try {
            instantiateCredentialsProvider();
        } catch (Exception e) {
             LOGGER.warn("Error instantiating custom credentials provider.  Falling back to the standard credentials provider." + e);
        }
    }

    @Override
    public AWSCredentials getCredentials() {
        try {
            if (awsCredentialsProvider != null) {
                return awsCredentialsProvider.getCredentials();
            }
        } catch (Exception e) {
            LOGGER.error("There is a problem with the User defined credentials provider. " + e);
        }
        throw new AmazonClientException("Unable to create credentials from user defined credentials provider");
    }

    @Override
    public void refresh() {
        try {
            if (awsCredentialsProvider != null) {
                awsCredentialsProvider.refresh();
                return;
            }
        } catch (Exception e) {
            LOGGER.error("There is a problem with refreshing the user defined credentials provider. " +  e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    private void instantiateCredentialsProvider () {
        Class<AWSCredentialsProvider> awsCredentialsProviderClass = loadUserDefinedCredentialsProvider();
        if (awsCredentialsProviderClass != null) {
            Class<AWSCredentialsProvider> credentialsProviderClass = awsCredentialsProviderClass;

            if (!AWSCredentialsProvider.class.isAssignableFrom(credentialsProviderClass)) {
                LOGGER.error("The loaded credential provider " +  credentialsProviderClass.getName() +
                        " does not extend com.amazonaws.auth.AWSCredentialsProvider.");
                return;
            }

            Constructor<AWSCredentialsProvider> constructor;
            try {
                constructor = credentialsProviderClass.getConstructor();
            } catch (NoSuchMethodException e) {
                LOGGER.error("Error initializing user defined credentials provider. No valid constructor. "+ e);
                return;
            }

            try {
                awsCredentialsProvider = constructor.newInstance();
                LOGGER.info("Instantiated the user defined credentials provider.");
            } catch (Exception e) {
                LOGGER.error("Exception while instantiating user defined " +
                        "credentials provider. " + e);
            }
        }
    }

    private Class<AWSCredentialsProvider> loadUserDefinedCredentialsProvider() {
            Pair<String, String> customCredentialsProvider = getUserDefinedCredentialsProviderFromConfig();
            if (customCredentialsProvider != null) {
                try {
                    List<URL> classPathList = new ArrayList<>();

                    File libDirPath = new File(AGENT_LIB_DIRECTORY);
                    classPathList.add(libDirPath.toURI().toURL());

                    if (customCredentialsProvider.getRight() != null && customCredentialsProvider.getRight().length() > 0) {
                        addCustomCredentialsJarToClassPath(customCredentialsProvider, classPathList);
                    }
                    URL[] urlArray = new URL[classPathList.size()];
                    URLClassLoader urlClassLoader = new URLClassLoader(classPathList.toArray(urlArray),
                            ClassLoader.getSystemClassLoader());
                    Class classToLoad = Class.forName(customCredentialsProvider.getLeft(), true,
                            urlClassLoader);
                    return (Class<AWSCredentialsProvider>) classToLoad;
                } catch (Exception e) {
                    LOGGER.error("Error loading user defined credentials provider. " + e);
                }
            }
            return null;
    }

    private void addCustomCredentialsJarToClassPath(Pair<String, String> customCredentialsProvider,
                                                    List<URL> classPathList) throws IOException {
        File customCredentialProviderJar = new File(customCredentialsProvider.getRight());
        classPathList.add(customCredentialProviderJar.toURI().toURL());
        checkIfOwnerIsKinesisAgentUser(customCredentialProviderJar);
    }

    private void checkIfOwnerIsKinesisAgentUser(File fileOrDirectory) {
        try {
            if (!(Files.getOwner(fileOrDirectory.toPath()).getName().equals(AGENT_USER))) {
                LOGGER.warn("Warning: A possible insecure configuration - the lib directory or the credentials provider jar" +
                        " itself is owned by a different user other than the kinesis agent user. This could to lead " +
                        "to a potential privilege escalation if the kinesis agent user has higher privileges. " +
                        "Make sure that the lib directory and the custom credentials provider jar file are owned by " +
                        "'aws-kinesis-agent-user'.");
            }
        } catch (Exception e) {
            LOGGER.error("Error while inspecting the owner of the of the jar file", e);
        }
    }

    private Pair<String, String> getUserDefinedCredentialsProviderFromConfig() {
        String credentialsProviderClass = "";
        String credentialsProviderLocation = "";
        try {
            credentialsProviderClass = configuration.readString(CREDENTIALS_PROVIDER_CLASS);
            credentialsProviderLocation = configuration.readString(CREDENTIALS_PROVIDER_LOCATION);
        } catch (ConfigurationException e) {
            if (credentialsProviderClass.length() == 0) {
                LOGGER.info("No custom implementation of credentials provider present in the config file");
                return null;
            }
        }
        return Pair.of(credentialsProviderClass, credentialsProviderLocation);
    }
}
