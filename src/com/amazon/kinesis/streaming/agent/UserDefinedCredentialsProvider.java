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
import java.util.Optional;

public class UserDefinedCredentialsProvider implements AWSCredentialsProvider{
    private static final String CREDENTIALS_PROVIDER_LOCATION = "userDefinedCredentialsProvider.location";
    private static final String CREDENTIALS_PROVIDER_CLASS = "userDefinedCredentialsProvider.classname";
    private static final String AGENT_LIB_DIRECTORY = "/usr/share/aws-kinesis-agent/lib/";
    private static final Logger LOGGER = LoggerFactory.getLogger(UserDefinedCredentialsProvider.class);
    private static final String AGENT_USER = "aws-kinesis-agent-user";


    private final AgentConfiguration configuration;
    private Optional<AWSCredentialsProvider> awsCredentialsProvider = Optional.empty();

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
            if (awsCredentialsProvider.isPresent()) {
                return awsCredentialsProvider.get().getCredentials();
            }
        } catch (Exception e) {
            LOGGER.error("There is a problem with the User defined credentials provider. " + e);
        }
        throw new AmazonClientException("Unable to create credentials from user defined credentials provider");
    }

    @Override
    public void refresh() {
        try {
            if (awsCredentialsProvider.isPresent()) {
                awsCredentialsProvider.get().refresh();
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
        Optional<Class<AWSCredentialsProvider>> awsCredentialsProviderClass = loadUserDefinedCredentialsProvider();
        if (awsCredentialsProviderClass.isPresent()) {
            Class<AWSCredentialsProvider> credentialsProviderClass = awsCredentialsProviderClass.get();

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
                awsCredentialsProvider = Optional.of(constructor.newInstance());
                LOGGER.info("Instantiated the user defined credentials provider.");
            } catch (Exception e) {
                LOGGER.error("Exception while instantiating user defined " +
                        "credentials provider. " + e);
            }
        }
    }

    private Optional<Class<AWSCredentialsProvider>> loadUserDefinedCredentialsProvider() {
            Optional<Pair<String, Optional<String>>> customCredentialsProvider = getUserDefinedCredentialsProviderFromConfig();
            if (customCredentialsProvider.isPresent()) {
                try {
                    List<URL> classPathList = new ArrayList<>();

                    File libDirPath = new File(AGENT_LIB_DIRECTORY);
                    classPathList.add(libDirPath.toURI().toURL());

                    if (customCredentialsProvider.get().getRight().isPresent()) {
                        addCustomCredentialsJarToClassPath(customCredentialsProvider, classPathList);
                    }
                    URL[] urlArray = new URL[classPathList.size()];
                    URLClassLoader urlClassLoader = new URLClassLoader(classPathList.toArray(urlArray),
                            ClassLoader.getSystemClassLoader());
                    Class classToLoad = Class.forName(customCredentialsProvider.get().getLeft(), true,
                            urlClassLoader);
                    return Optional.of((Class<AWSCredentialsProvider>) classToLoad);
                } catch (Exception e) {
                    LOGGER.error("Error loading user defined credentials provider. " + e);
                }
            }
            return Optional.empty();
    }

    private void addCustomCredentialsJarToClassPath(Optional<Pair<String, Optional<String>>> customCredentialsProvider,
                                                    List<URL> classPathList) throws IOException {
        File customCredentialProviderJar = new File(customCredentialsProvider.get().getRight().get());
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

    private Optional<Pair<String, Optional<String>>> getUserDefinedCredentialsProviderFromConfig() {
        String credentialsProviderClass = "";
        Optional<String> credentialsProviderLocation = Optional.empty();
        try {
            credentialsProviderClass = configuration.readString(CREDENTIALS_PROVIDER_CLASS);
            credentialsProviderLocation = Optional.of(configuration.readString(CREDENTIALS_PROVIDER_LOCATION));
        } catch (ConfigurationException e) {
            if (credentialsProviderClass.length() == 0) {
                LOGGER.info("No custom implementation of credentials provider present in the config file");
                return Optional.empty();
            }
        }
        return Optional.of(Pair.of(credentialsProviderClass, credentialsProviderLocation));
    }
}
