/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

import lombok.Cleanup;

import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.Agent;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileSender;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;
import com.amazon.kinesis.streaming.agent.tailing.testing.TestAgentContext;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileSender.FileSenderFactory;

public class AgentEnduranceTest extends TailingTestBase {

    @Test(enabled=false, groups={"endurance"})
    public void testAgent() throws IOException, InterruptedException  {
        final int targetFileSize = 2 * 1024 * 1024;
        FileRotatorFactory rotatorFactory = new TruncateFileRotatorFactory(targetFileSize);
        FileSenderFactory<FirehoseRecord> fileSenderFactory = new FileSender.FileSenderWithPartialFailuresAndErrorsBeforeCommitFactory<FirehoseRecord>();

        @Cleanup InputStream configStream = getClass().getResourceAsStream("AgentEnduranceTest.json");
        TestAgentContext context = new TestAgentContext(
                configStream, testFiles, rotatorFactory, fileSenderFactory);
        Files.deleteIfExists(context.checkpointFile());
        Agent agent = new Agent(context);
        context.startFileGenerators();
        agent.startAsync();
        agent.awaitRunning();

        while(agent.isRunning()) {
            Thread.sleep(2_000);
        }
    }
}
