/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.testing;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FileFlowFactory;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseConstants;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseFileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;
import com.amazon.kinesis.streaming.agent.tailing.ISender;
import com.amazon.kinesis.streaming.agent.tailing.SourceFile;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileSender.FileSenderFactory;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase.FileRotatorFactory;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestFiles;

public class TestAgentContext extends AgentContext {
    public TestAgentContext(
            InputStream configStream,
            TestFiles testFiles,
            FileRotatorFactory rotatorFactory,
            FileSenderFactory<FirehoseRecord> fileSenderFactory) throws IOException {
        super(Configuration.get(configStream),
                new TestFileFlowFactory(testFiles, rotatorFactory, fileSenderFactory));
    }

    public static class TestFileFlowFactory extends FileFlowFactory {
        final Map<FileFlow<?>, Path> outputFiles = new HashMap<>();
        final Map<FileFlow<?>, FileRotator> rotators = new HashMap<>();
        final Map<FileFlow<?>, RotatingFileGenerator> fileGenerators = new HashMap<>();
        final TestFiles testFiles;
        final FileRotatorFactory rotatorFactory;
        final FileSenderFactory<FirehoseRecord> fileSenderFactory;

        public TestFileFlowFactory(
                TestFiles testFiles,
                FileRotatorFactory rotatorFactory,
                FileSenderFactory<FirehoseRecord> fileSenderFactory) {
            this.testFiles = testFiles;
            this.rotatorFactory = rotatorFactory;
            this.fileSenderFactory = fileSenderFactory;
        }

        @Override
        protected FirehoseFileFlow getFirehoseFileflow(AgentContext context, Configuration config) {
            String[] sourceSpec = config.readString(FileFlow.FILE_PATTERN_KEY).split(",");
            String sourceFileNamePrefix = sourceSpec[0];
            double sourceRecordsPerSecond = Double.parseDouble(sourceSpec[1]);
            long fileWriteIntervalMillis = Long.parseLong(sourceSpec[2]);
            long fileRotationIntervalMillis = Long.parseLong(sourceSpec[3]);
            int maxFileRotations = Integer.parseInt(sourceSpec[4]);
            String outputFilePrefix = config.readString(FirehoseConstants.DESTINATION_KEY);
            rotatorFactory.withPrefix(sourceFileNamePrefix);
            rotatorFactory.withMaxFilesToKeepOnDisk(maxFileRotations);

            final FileRotator rotator = rotatorFactory.create();
            final Path outputFile = testFiles.createTempFileWithPrefix(outputFilePrefix);
            FirehoseFileFlow flow = buildFirehoseFileFlow(context, config, rotator, outputFile);
            outputFiles.put(flow, outputFile);
            rotators.put(flow, rotator);
            RotatingFileGenerator fileGenerator = new RotatingFileGenerator(
                    rotator, sourceRecordsPerSecond,
                    fileWriteIntervalMillis, fileRotationIntervalMillis);
            fileGenerators.put(flow, fileGenerator);
            return flow;
        }

        private FirehoseFileFlow buildFirehoseFileFlow(AgentContext context, Configuration config, final FileRotator rotator,
                final Path outputFile) {
            FirehoseFileFlow flow = new FirehoseFileFlow(context, config) {
                @Override
                protected SourceFile buildSourceFile() {
                    return new SourceFile(this, rotator.getInputFileGlob());
                }

                @Override
                protected ISender<FirehoseRecord> buildSender() {
                    return fileSenderFactory.create(agentContext, outputFile);
                }
            };
            return flow;
        }
    }

    public void startFileGenerators() {
        for(RotatingFileGenerator gen : ((TestFileFlowFactory)this.fileFlowFactory).fileGenerators.values()) {
            gen.startAsync();
            gen.awaitRunning();
        }
    }

    public void stopFileGenerators() {
        for(RotatingFileGenerator gen : ((TestFileFlowFactory)this.fileFlowFactory).fileGenerators.values()) {
            gen.stopAsync();
            gen.awaitTerminated();
        }
    }

    public Path[] getInputFiles(FileFlow<?> flow) {
        FileRotator rotator = ((TestFileFlowFactory)this.fileFlowFactory).rotators.get(flow);
        ArrayList<Path> input = new ArrayList<>(rotator.getInputFiles());
        input.addAll(rotator.getDeletedFiles());
        return input.toArray(new Path[input.size()]);
    }

    public Path getOutputFile(FileFlow<?> flow) {
        return ((TestFileFlowFactory)this.fileFlowFactory).outputFiles.get(flow);
    }
}
