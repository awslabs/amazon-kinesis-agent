/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.io.InputStream;
import java.nio.file.Files;

import lombok.Cleanup;

import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.tailing.FileTailer;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseFileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.SQLiteFileCheckpointStore;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileSender;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;
import com.amazon.kinesis.streaming.agent.tailing.testing.TestAgentContext;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileSender.FileSenderFactory;

public class FileTailerEnduranceTest extends TailingTestBase {

    @Test(enabled=false, groups = { "endurance" })
    public void testFileTailer() throws Exception {
        final int targetFileSize = 10 * 1024 * 1024;
        FileRotatorFactory rotatorFactory = new TruncateFileRotatorFactory(targetFileSize);
        FileSenderFactory<FirehoseRecord> senderFactory = new FileSender.FileSenderWithPartialFailuresAndErrorsBeforeCommitFactory<FirehoseRecord>();

        @Cleanup InputStream configStream = getClass().getResourceAsStream("FileTailerEnduranceTest.json");
        TestAgentContext context = new TestAgentContext(
                configStream, testFiles, rotatorFactory, senderFactory);
        Files.deleteIfExists(context.checkpointFile());
        FirehoseFileFlow flow = (FirehoseFileFlow) context.flows().get(0);
        SQLiteFileCheckpointStore checkpoints = new SQLiteFileCheckpointStore(context);
        FileTailer<FirehoseRecord> tailer = flow.createNewTailer(checkpoints, context.createSendingExecutor());
        tailer.initialize();

        context.startFileGenerators();

        // start tailing
        tailer.startAsync();
        tailer.awaitRunning();

        // Run indefinitely
        while(tailer.isRunning()) {
            tailer.heartbeat(context);
            //logger.debug("Tailer bytes behind: {}, Files behind: {}", tailer.bytesBehind(), tailer.filesBehind());
            Thread.sleep(1000);
        }
    }
}
