/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.Agent;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileSender;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;
import com.amazon.kinesis.streaming.agent.tailing.testing.TestAgentContext;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileSender.FileSenderFactory;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.google.common.base.Stopwatch;

public class AgentEndToEndTest extends TailingTestBase {
    private static final long TEST_DURATION_MILLIS = TimeUnit.MINUTES.toMillis(6);

    @DataProvider(name = "rotatorsAndSenders")
    public Object[][] getRotatorsSendersAndTailersData() throws IOException {
        // NOTE: Comment any of the elements in the arrays below to "zoom in" on
        //       any particular combination that you want to analyze further.

        final int fileSize = 2 * 1024 * 1024;
        Object[][] rotators = new Object[][] {
                {new TruncateFileRotatorFactory(fileSize)},
                //{new RenameFileRotatorFactory(fileSize)},
                //{new CreateFileRotatorFactory(fileSize)},
                //{new CopyFileRotatorFactory(fileSize)},
        };

        Object[][] senders = new Object[][] {
                //{new FileSender.PerfectFileSenderFactory<FirehoseRecord>()},
                //{new FileSender.FileSenderWithHighLatencyFactory<FirehoseRecord>()},
                //{new FileSender.FileSenderWithPartialFailuresFactory<FirehoseRecord>()},
                //{new FileSender.FileSenderWithErrorsBeforeCommitFactory<FirehoseRecord>()},
                {new FileSender.FileSenderWithPartialFailuresAndErrorsBeforeCommitFactory<FirehoseRecord>()},

                // TODO: The following two senders will produce duplicate records and need the tests need to be modified to handle those
                //{new FileSender.FileSenderWithErrorsAfterPartialCommitFactory<FirehoseRecord>()},
                //{new FileSender.MisbehavingFileSenderFactory<FirehoseRecord>()},
        };

        return TestUtils.crossTestParameters(rotators, senders);
    }

    @Test(enabled=false, groups={"integration"},
            dataProvider="rotatorsAndSenders",
            description="Long running test that writes tons of records to multiple flows and validates that all is well.")
    public void testLongRunningEndToEndAgent(
            final FileRotatorFactory rotatorFactory,
            final FileSenderFactory<FirehoseRecord> fileSenderFactory) throws Exception {
        runTest(TEST_DURATION_MILLIS, rotatorFactory, fileSenderFactory, "AgentEndToEndTest.json", 0.10);
    }

    @Test(enabled=false, groups={"integration"},
            dataProvider="rotatorsAndSenders",
            description="Long running test that writes tons and tons of records to multiple flows and validates that all is well.")
    public void testLongRunningEndToEndAgentHighThroughput(
            final FileRotatorFactory rotatorFactory,
            final FileSenderFactory<FirehoseRecord> fileSenderFactory) throws Exception {
        runTest(TEST_DURATION_MILLIS, rotatorFactory, fileSenderFactory, "AgentEndToEndTestHighThroughput.json", 0.90);
    }

    private void runTest(
            final long duration,
            final FileRotatorFactory rotatorFactory,
            final FileSenderFactory<FirehoseRecord> fileSenderFactory,
            final String configFile,
            final double maxMissingRatio) throws IOException, InterruptedException {

        @Cleanup InputStream configStream = getClass().getResourceAsStream(configFile);
        TestAgentContext context = new TestAgentContext(
                configStream, testFiles, rotatorFactory, fileSenderFactory);
        Files.deleteIfExists(context.checkpointFile());
        Agent agent = new Agent(context);
        context.startFileGenerators();

        agent.startAsync();
        agent.awaitRunning();

        Stopwatch timer = Stopwatch.createStarted();
        while(timer.elapsed(TimeUnit.MILLISECONDS) < duration) {
            Thread.sleep(10_000);
        }
        context.stopFileGenerators();

        // Give agent some extra time to finish
        Thread.sleep(context.shutdownTimeoutMillis());
        agent.stopAsync();
        agent.awaitTerminated();
        for(FileFlow<?> flow : context.flows()) {
            double missingRatio = assertOutputFileRecordsMatchInputFilesApproximately(
                    context.getOutputFile(flow), maxMissingRatio,
                    context.getInputFiles(flow));
            logger.debug("{}: Missing percent: {}%", flow.getId(), (int)(missingRatio * 100));
        }
    }
}
