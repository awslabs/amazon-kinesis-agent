# Amazon Kinesis Agent

The **Amazon Kinesis Agent** is a stand-alone Java software application that offers an easier way to collect and ingest data into [Amazon Kinesis][kinesis] services, including [Amazon Kinesis Streams][kinesis-stream] and [Amazon Kinesis Firehose][kinesis-firehose].

* [Amazon Kinesis details][kinesis]
* [Forum][kinesis-forum]
* [Issues][kinesis-agent-issues]

## Features

* Monitors file patterns and sends new data records to delivery streams
* Handles file rotation, checkpointing, and retry upon failure
* Delivers all data in a reliable, timely, and simpler manner
* Emits Amazon CloudWatch metrics to help you better monitor and troubleshoot the streaming process

## Getting started

1. **Sign up for AWS** &mdash; Before you begin, you need an AWS account. For more information about creating an AWS account and retrieving your AWS credentials, see [AWS Account and Credentials][docs-signup] in the AWS SDK for Java Developer Guide.
2. **Sign up for Amazon Kinesis** &mdash; Go to the Amazon Kinesis console to sign up for the service and create a Amazon Kinesis stream or Firehose delivery stream. For more information, see [Create an Amazon Kinesis Stream][kinesis-stream-create] in the *Amazon Kinesis Streams Developer Guide* or [Create an Amazon Kinesis Firehose Delivery Stream][kinesis-firehose-create] in the *Amazon Kinesis Firehose Developer Guide*.
3. **Minimum requirements** &mdash; To start the Amazon Kinesis Agent, you need **Java 1.7+**.
4. **Using the Amazon Kinesis Agent** &mdash; For more information about using the Amazon Kinesis Agent to deliver data to Streams and Firehose, see [Writing to Amazon Kinesis with Agents][kinesis-stream-agent-guide] and [Writing to Delivery Streams with Agents][kinesis-firehose-agent-guide].

## Installing Amazon Kinesis Agent
After you've downloaded the code from GitHub, you can install the Amazon Kinesis Agent with the following command:

```sh
sudo ./setup --install
```

This setup script downloads all the dependencies and bootstraps the environment for running the Java program.

## Configuring and starting Amazon Kinesis Agent
After the agent is installed, the configuration file can be found in `/etc/aws-kinesis/agent.json`. You need to modify this configuration file to set the data destinations and AWS credentials, and to point the agent to the files to push. After you complete the configuration, you can make the agent start automatically at system startup with the following command:

```sh
sudo chkconfig aws-kinesis-agent on
```

If you do not want the agent running at system startup, turn it off with the following command:

```sh
sudo chkconfig aws-kinesis-agent off
```

To start the agent manually, use the following command:

```sh
sudo service aws-kinesis-agent start
```

You can make sure the agent is running with the following command:

```sh
sudo service aws-kinesis-agent status
```

You may see messages such as `aws-kinesis-agent (pid [PID]) is running...`

To stop the agent, use the following command:

```sh
sudo service aws-kinesis-agent stop
```

## Viewing the Amazon Kinesis Agent log file
The agent writes its logs to `/var/log/aws-kinesis-agent/aws-kinesis-agent.log`

## Uninstalling Amazon Kinesis Agent
To uninstall the agent, use the following command:

```sh
sudo ./setup --uninstall
```

## Building from Source

The setup script is only tested on **Red Hat Enterprise Linux** version 7 or later and **Amazon Linux AMI** version 2015.09 or later. For other distributions or platforms, you can build the Java project with the following command:

```sh
sudo ./setup --build
```

You need to download all the dependencies listed in pom.xml before building the Java program. **DEPENDENCY_DIR** is the directory where you download and store the dependencies. By default, the Amazon Kinesis Agent reads the configuration file from /etc/aws-kinesis/agent.json. You need to create such a file if it does not already exist. A sample configuration can be found at ./configuration/release/aws-kinesis-agent.json

To start the program, use the following command:

```sh
java -cp CLASSPATH "com.amazon.kinesis.streaming.agent.Agent"
```

**CLASSPATH** is the classpath to your dependencies and the target JAR file that you built from the step above.

## Release Notes
### Release 1.0 (October 7, 2015)
* This is the first release.

[kinesis]: http://aws.amazon.com/kinesis
[kinesis-forum]: http://developer.amazonwebservices.com/connect/forum.jspa?forumID=169
[kinesis-agent-issues]: https://github.com/awslabs/amazon-kinesis-agent/issues
[docs-signup]: http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html
[kinesis-stream]: https://aws.amazon.com/kinesis/streams/
[kinesis-firehose]: https://aws.amazon.com/kinesis/firehose/
[kinesis-stream-create]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-create-stream.html
[kinesis-firehose-create]: http://docs.aws.amazon.com/firehose/latest/dev/basic-create.html
[kinesis-stream-agent-guide]: http://docs.aws.amazon.com/kinesis/latest/dev/writing-with-agents.html
[kinesis-firehose-agent-guide]: http://docs.aws.amazon.com/firehose/latest/dev/writing-with-agents.html
