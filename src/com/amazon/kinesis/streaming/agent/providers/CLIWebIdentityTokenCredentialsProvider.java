package com.amazon.kinesis.streaming.agent.providers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;

/**
 * This provider uses the AWS CLI to call assume-role-with-web-identity to retrieve credentials.
 * Refresh on expiration is implemented.
 */
public class CLIWebIdentityTokenCredentialsProvider implements AWSCredentialsProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(CLIWebIdentityTokenCredentialsProvider.class);
    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
        .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
        .appendTimeZoneOffset("+00:00", true, 2, 4)
        .toFormatter();

    private final String roleArn = System.getenv("AWS_ROLE_ARN");
    private String webIdentityToken = "";

    private String accessKey = "";
    private String secretKey = "";
    private String sessionToken = "";
    private DateTime expiration = DateTime.now();

    public CLIWebIdentityTokenCredentialsProvider() {
        this.refresh();
        if (accessKey.isEmpty() || secretKey.isEmpty() || sessionToken.isEmpty()) {
            LOGGER.warn("Error instantiating CLI Web Identity Token credentials provider. Falling back to the standard provider.");
        }
    }

    @Override
    public AWSCredentials getCredentials() {
        if (accessKey.isEmpty() || secretKey.isEmpty() || sessionToken.isEmpty()) {
            throw new IllegalStateException("Unable to retrieve Web Token Identity credentials from CLI.");
        }

        // Refresh credentials when expiration is within 1 minute of current time
        if (DateTime.now(DateTimeZone.UTC).plusMinutes(1).getMillis() >= expiration.getMillis()) {
            this.refresh();
        }

        return new BasicSessionCredentials(this.accessKey, this.secretKey, this.sessionToken);
    }

    @Override
    public void refresh() {
        try {
            LOGGER.info("Refreshing Web Identity Token credentials from CLI...");
            this.getToken();
            final String cmd = "aws sts assume-role-with-web-identity --role-session-name kinesis_agent --role-arn "
                + this.roleArn + " --web-identity-token " + this.webIdentityToken + " --query Credentials --output yaml";
            this.getAssumedCredentials(Runtime.getRuntime().exec(cmd));
            LOGGER.info("Finished credentials refresh");
            if (this.accessKey.isEmpty()) {
                LOGGER.error("Access key is empty after credentials refresh");
            }
            if (this.secretKey.isEmpty()) {
                LOGGER.error("Secret key is empty after credentials refresh");
            }
            if (this.sessionToken.isEmpty()) {
                LOGGER.error("Session token is empty after credentials refresh");
            }
        } catch (IOException e) {
            this.resetCredentials();
            LOGGER.error("Unable to refresh Web Identity Token credentials from CLI.");
        }
    }

    private void resetCredentials() {
        this.accessKey = "";
        this.secretKey = "";
        this.sessionToken = "";
        this.expiration = DateTime.now();
    }

    private void getToken() throws IOException {
        final BufferedReader reader = new BufferedReader(new FileReader(System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE")));
        this.webIdentityToken = reader.readLine();
        reader.close();
        if (this.webIdentityToken == null || this.webIdentityToken.isEmpty()) {
            throw new IOException("Error during Web Identity Token retrieval");
        }
    }

    private void getAssumedCredentials(Process process) throws IOException {
        this.resetCredentials();
        final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("AccessKeyId")) {
                this.accessKey = line.split(" ")[1].trim();
            } else if (line.startsWith("SecretAccessKey")) {
                this.secretKey = line.split(" ")[1];
            } else if (line.startsWith("SessionToken")) {
                this.sessionToken = line.split(" ")[1];
            } else if (line.startsWith("Expiration")) {
                line = line.split(" ")[1];
                this.expiration = DateTime.parse(line.substring(1, line.length() - 1), DATE_TIME_FORMATTER);
            }
        }
        reader.close();
    }
}
