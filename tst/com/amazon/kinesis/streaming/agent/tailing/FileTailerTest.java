/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseRecord;
import com.amazon.kinesis.streaming.agent.tailing.IRecord;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow.InitialPosition;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpoint;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpointStore;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.SQLiteFileCheckpointStore;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileRotator;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileSender;
import com.amazon.kinesis.streaming.agent.tailing.testing.RememberedTrackedFile;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;
import com.amazon.kinesis.streaming.agent.tailing.testing.TestableFileTailer;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileSender.FileSenderFactory;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.google.common.base.Preconditions;

public class FileTailerTest extends TailingTestBase {
    private static final int TEST_TIMEOUT = 60_000;
    private static final int TEST_REPS = 1;

    @Test(dataProvider="rotatorsSendersAndTailers", timeOut=TEST_TIMEOUT, skipFailedInvocations=true, invocationCount=TEST_REPS)
    public void testStartWithCheckpoint(
            FileRotatorFactory rotatorFactory,
            TestableFileTailerFactory<FirehoseRecord> tailerFactory,
            FileSenderFactory<FirehoseRecord> senderFactory) throws Exception {
        final Path checkpointsFile = testFiles.createTempFile();

        new TailerTestRunner(true, rotatorFactory, senderFactory, tailerFactory, checkpointsFile, TestUtils.pickOne(InitialPosition.START_OF_FILE, InitialPosition.END_OF_FILE)) {
            private FileCheckpoint checkpoint;

            @Override
            protected void initializeInputFile() throws IOException {
                super.initializeInputFile();
                checkpoint = getCheckpointStore().saveCheckpoint(new TrackedFile(flow, rotator.getLatestFile()), Files.size(rotator.getLatestFile()));
            }

            @Override
            protected void initializeTailer() throws IOException {
                super.initializeTailer();
                assertNotNull(tailer.fileTracker.getCurrentOpenFile());
                assertTrue(tailer.fileTracker.getCurrentOpenFile().isSameAs(checkpoint.getFile()));
                assertEquals(tailer.fileTracker.getCurrentOpenFile().getCurrentOffset(), checkpoint.getOffset());
                assertTrue(tailer.parser.getCurrentFile().isSameAs(checkpoint.getFile()));
                assertEquals(tailer.parser.getCurrentFile().getCurrentOffset(), checkpoint.getOffset());
            }

            @Override
            protected void doRun() throws Exception {
                // Process records...
                processAllInputFiles();
            }

            @Override
            protected void validateOutput(Path[] inputFiles) throws IOException {
                assertOutputFileRecordsMatchInputFiles(outputFile, inputRecordsBeforeStart, inputFiles);
            }
        }.call();
    }

    @Test(dataProvider="rotatorsSendersAndTailers", timeOut=TEST_TIMEOUT, skipFailedInvocations=true, invocationCount=TEST_REPS)
    public void testStartWithCheckpointAndRotation(
            FileRotatorFactory rotatorFactory,
            TestableFileTailerFactory<FirehoseRecord> tailerFactory,
            FileSenderFactory<FirehoseRecord> senderFactory) throws Exception {
        final Path checkpointsFile = testFiles.createTempFile();

        new TailerTestRunner(true, rotatorFactory, senderFactory, tailerFactory, checkpointsFile, TestUtils.pickOne(InitialPosition.START_OF_FILE, InitialPosition.END_OF_FILE)) {
            private FileCheckpoint checkpoint;

            @Override
            protected void doRun() throws Exception {
                // Process records and stop...
                processAllInputFiles();
                stopTailer();

                // Note checkpoint at which the tailer stopped
                checkpoint = tailer.getCheckpoints().getCheckpointForFlow(flow);
                assertNotNull(checkpoint);
                RememberedTrackedFile remembered = new RememberedTrackedFile(checkpoint.getFile());

                // Rotate
                rotator.rotate();

                // Recreate and initialize the file tailer, checking that it picked up the checkpointed file
                tailer.stopAsync();
                tailer.awaitTerminated();
                tailer.getCheckpoints().close();

                createTailer();
                initializeTailer();
                assertNotNull(tailer.fileTracker.getCurrentOpenFile());
                assertTrue(remembered.hasSameContentHash(tailer.fileTracker.getCurrentOpenFile().getPath()));
                assertEquals(tailer.fileTracker.getCurrentOpenFile().getCurrentOffset(), checkpoint.getOffset());
                assertTrue(tailer.fileTracker.newerFilesPending());
                assertEquals(tailer.parser.getCurrentFile().getCurrentOffset(), checkpoint.getOffset());
                startTailer();
                // Process everything that's left...
                processAllInputFiles();
            }
        }.call();
    }

    @Test(dataProvider="rotatorsSendersAndTailers", timeOut=TEST_TIMEOUT, skipFailedInvocations=true, invocationCount=TEST_REPS)
    public void testStartWithCheckpointThatDoesntMatchAndNoExistingFile(
            FileRotatorFactory rotatorFactory,
            TestableFileTailerFactory<FirehoseRecord> tailerFactory,
            FileSenderFactory<FirehoseRecord> senderFactory) throws Exception {
        final Path checkpointsFile = testFiles.createTempFile();

        new TailerTestRunner(false, rotatorFactory, senderFactory, tailerFactory, checkpointsFile, TestUtils.pickOne(InitialPosition.START_OF_FILE, InitialPosition.END_OF_FILE)) {

            @Override
            protected void start() throws IOException {
                // Create a "bad" checkpoint
                Path tmp = testFiles.createTempFile();
                rotator.getRecordGenerator().appendDataToFile(tmp, BUNCH_OF_BYTES);
                getCheckpointStore().saveCheckpoint(new TrackedFile(flow, tmp), Files.size(tmp));
                super.start();
            }

            @Override
            protected void initializeTailer() throws IOException {
                super.initializeTailer();
                assertNull(tailer.fileTracker.getCurrentOpenFile());
                assertNull(tailer.parser.getCurrentFile());
            }

            @Override
            protected void doRun() throws Exception {
                // Process records...
                processAllInputFiles();
            }

            @Override
            protected void validateInputNotEmpty(Path[] inputFiles) throws IOException {
                // no-op
            }
        }.call();
    }

    @Test(dataProvider="rotatorsSendersTailersInitialPosition", timeOut=TEST_TIMEOUT, skipFailedInvocations=true, invocationCount=TEST_REPS)
    public void testStartWithCheckpointThatDoesntMatch(
            FileRotatorFactory rotatorFactory,
            TestableFileTailerFactory<FirehoseRecord> tailerFactory,
            FileSenderFactory<FirehoseRecord> senderFactory,
            InitialPosition initialPosition) throws Exception {
        final Path checkpointsFile = testFiles.createTempFile();

        new TailerTestRunner(true, rotatorFactory, senderFactory, tailerFactory, checkpointsFile, initialPosition) {
            @Override
            protected void start() throws IOException {
                // Create a "bad" checkpoint
                Path tmp = testFiles.createTempFile();
                rotator.getRecordGenerator().appendDataToFile(tmp, BUNCH_OF_BYTES);
                getCheckpointStore().saveCheckpoint(new TrackedFile(flow, tmp), Files.size(tmp));
                super.start();
            }

            @Override
            protected void initializeTailer() throws IOException {
                super.initializeTailer();
                assertNotNull(tailer.fileTracker.getCurrentOpenFile());
                assertEquals(tailer.fileTracker.getCurrentOpenFile().getPath(), rotator.getLatestFile());
                assertNotNull(tailer.parser.getCurrentFile());
            }

            @Override
            protected void doRun() throws Exception {
                // Process records...
                processAllInputFiles();
            }
        }.call();
    }

    @Test(dataProvider="rotatorsSendersTailersInitialPosition", timeOut=TEST_TIMEOUT, skipFailedInvocations=true, invocationCount=TEST_REPS)
    public void testStartWithPreexistingFileNoRotations(
            FileRotatorFactory rotatorFactory,
            TestableFileTailerFactory<FirehoseRecord> tailerFactory,
            FileSenderFactory<FirehoseRecord> senderFactory,
            InitialPosition initialPosition) throws Exception {

        new TailerTestRunner(true, rotatorFactory, senderFactory, tailerFactory,
                null, initialPosition) {
            @Override
            protected void doRun() throws Exception {
                // Process records...
                processAllInputFiles();
            }
        }.call();
    }

    @Test(dataProvider="rotatorsSendersAndTailers", timeOut=TEST_TIMEOUT, skipFailedInvocations=true, invocationCount=TEST_REPS)
    public void testStartWithNonExistingFile(
            FileRotatorFactory rotatorFactory,
            TestableFileTailerFactory<FirehoseRecord> tailerFactory,
            FileSenderFactory<FirehoseRecord> senderFactory) throws Exception {

        new TailerTestRunner(false, rotatorFactory, senderFactory, tailerFactory, null, TestUtils.pickOne(InitialPosition.START_OF_FILE, InitialPosition.END_OF_FILE)) {
            @Override
            protected void doRun() throws Exception {
                // Process records... Nothing should happen.
                processAllInputFiles();
                processAllInputFiles();
                processAllInputFiles();
            }

            @Override
            protected void validateInputNotEmpty(Path[] inputFiles) throws IOException {
                // No-op: No input in this test
            }
        }.call();
    }

    @Test(dataProvider="rotatorsSendersTailersInitialPosition", timeOut=TEST_TIMEOUT, skipFailedInvocations=true, invocationCount=TEST_REPS)
    public void testInputFilesAppearingAfterStart(
            FileRotatorFactory rotatorFactory,
            TestableFileTailerFactory<FirehoseRecord> tailerFactory,
            FileSenderFactory<FirehoseRecord> senderFactory,
            InitialPosition initialPosition) throws Exception {

        new TailerTestRunner(false, rotatorFactory, senderFactory, tailerFactory, null, initialPosition) {
            @Override
            protected void doRun() throws Exception {
                // Process records... Nothing should happen.
                processAllInputFiles();
                assertTrue(rotator.getActiveFiles().isEmpty());
                assertTrue(rotator.getDeletedFiles().isEmpty());

                // Following will create an input file and tailing should pick it up
                rotator.rotate();
                processAllInputFiles();
            }
        }.call();
    }


    @Test(dataProvider="rotatorsSendersTailersInitialPosition", timeOut=TEST_TIMEOUT, skipFailedInvocations=true, invocationCount=TEST_REPS)
    public void testStartWithPreexistingFileAndRotation(
            FileRotatorFactory rotatorFactory,
            TestableFileTailerFactory<FirehoseRecord> tailerFactory,
            FileSenderFactory<FirehoseRecord> senderFactory,
            InitialPosition initialPosition) throws Exception {
        new TailerTestRunner(true, rotatorFactory, senderFactory, tailerFactory, null, initialPosition) {
            @Override
            protected void doRun() throws Exception {
                // Process records...
                processAllInputFiles();
                // Rotate once, add data
                rotator.rotate();
                // Then process more records
                processAllInputFiles();
                // Rotate once more and add data
                rotator.rotate();
                // Then process more records
                processAllInputFiles();
            }
        }.call();
    }

    @DataProvider(name = "rotatorsSendersTailersInitialPosition")
    public Object[][] getrotatorsSendersTailersInitialPositionData() throws IOException {
        Object[][] initialPositions = new Object[][] {
                {InitialPosition.START_OF_FILE},
                {InitialPosition.END_OF_FILE},
        };

        return TestUtils.crossTestParameters(getRotatorsSendersAndTailersData(), initialPositions);
    }

    @DataProvider(name = "rotatorsSendersAndTailers")
    public Object[][] getRotatorsSendersAndTailersData() throws IOException {
        // NOTE: Comment any of the elements in the arrays below to "zoom in" on
        //       any particular combination that you want to analyze further.

        final int fileSize = 2 * 1024 * 1024;
        Object[][] rotatorTailers = new Object[][] {
                {new TruncateFileRotatorFactory(fileSize), new AsyncFileTailerFactory()},
                //{new RenameFileRotatorFactory(fileSize), new AsyncFileTailerFactory()},
                //{new CreateFileRotatorFactory(fileSize), new AsyncFileTailerFactory()},
                //{new CopyFileRotatorFactory(fileSize), new AsyncFileTailerFactory()},
        };

        Object[][] senders = getSendersData();

        return TestUtils.crossTestParameters(rotatorTailers, senders);
    }

    @DataProvider(name = "senders")
    private Object[][] getSendersData() {
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
        return senders;
    }

    public abstract class TailerTestRunner implements Callable<Void> {
        protected static final int BUNCH_OF_BYTES = 10*1014;
        final FileRotatorFactory rotatorFactory;
        final TestableFileTailerFactory<FirehoseRecord> tailerFactory;
        final FileRotator rotator;
        final FileSenderFactory<FirehoseRecord> senderFactory;
        final FileSender<FirehoseRecord> sender;
        final Path outputFile;
        final boolean preCreateInputFile;
        final AgentContext agentContext;
        final FileFlow<FirehoseRecord> flow;
        final InitialPosition initialPosition;
        final boolean startPaused;
        TestableFileTailer<FirehoseRecord> tailer;
        int inputRecordsBeforeStart = 0;

        public TailerTestRunner(
                boolean preCreateInputFile,
                FileRotatorFactory rotatorFactory,
                FileSenderFactory<FirehoseRecord> senderFactory,
                TestableFileTailerFactory<FirehoseRecord> tailerFactory,
                Path checkpointsFile,
                InitialPosition initialPosition) throws IOException {
            this(true, preCreateInputFile, rotatorFactory, senderFactory,
                    tailerFactory, checkpointsFile, initialPosition);
        }

        @SuppressWarnings("unchecked")
        public TailerTestRunner(
                boolean startPaused,
                boolean preCreateInputFile,
                FileRotatorFactory rotatorFactory,
                FileSenderFactory<FirehoseRecord> senderFactory,
                TestableFileTailerFactory<FirehoseRecord> tailerFactory,
                Path checkpointsFile,
                InitialPosition initialPosition) throws IOException {
            this.rotatorFactory = rotatorFactory;
            this.tailerFactory = tailerFactory;
            this.preCreateInputFile = preCreateInputFile;
            this.senderFactory = senderFactory;
            this.outputFile = testFiles.createTempFile();
            this.rotator = rotatorFactory.create();
            this.initialPosition = initialPosition;
            this.startPaused = startPaused;
            // Create a context with high shutdown timeout
            Map<String, Object> agentConfig = getAgentConfig(checkpointsFile);
            agentContext = getTestAgentContext(this.rotator.getInputFileGlob(), agentConfig);
            flow = spy((FileFlow<FirehoseRecord>) this.agentContext.flows().get(0));
            when(flow.getInitialPosition()).thenReturn(this.initialPosition);
            sender = senderFactory.create(this.agentContext, this.outputFile);
        }

        protected Map<String, Object> getAgentConfig(Path checkpointsFile) {
            Map<String, Object> agentConfig = new HashMap<>();
            if(checkpointsFile != null)
                agentConfig.put("checkpointFile", checkpointsFile);
            return agentConfig;
        }

        protected void createTailer()
                throws IOException {
            tailer = tailerFactory.create(this.agentContext, this.flow, this.sender, getCheckpointStore());
        }

        protected SQLiteFileCheckpointStore getCheckpointStore() {
            return new SQLiteFileCheckpointStore(agentContext);
        }

        protected void initializeTailer() throws IOException {
            tailer.initialize();
        }

        protected void initializeInputFile() throws IOException {
            if (preCreateInputFile) {
                rotator.rotate();
                inputRecordsBeforeStart = rotator.getRecordsWrittenToFile(0);
            }
        }

        protected void startTailer() {
            tailer.startAsync(startPaused);
            tailer.awaitRunning();
        }

        protected void start() throws IOException {
            initializeInputFile();
            createTailer();
            initializeTailer();
            startTailer();
            if (preCreateInputFile) {
                rotator.appendDataToLatestFile(BUNCH_OF_BYTES);
            }
        }

        protected void end() throws IOException {
            stopTailer();
            if(senderFactory.producesDuplicates()) {
                // TODO: support duplicate records when testing
                throw new UnsupportedOperationException("Testing for duplicate records not implemented yet.");
            }
            Path[] inputFiles = rotator.getInputFiles().toArray(new Path[0]);
            validateInputNotEmpty(inputFiles);
            validateOutput(inputFiles);
        }

        protected void stopTailer() {
            tailer.waitForIdle();
            tailer.stopAsync();
            tailer.awaitTerminated();
        }

        protected void validateOutput(Path[] inputFiles) throws IOException {
            if(initialPosition == InitialPosition.END_OF_FILE)
                assertOutputFileRecordsMatchInputFiles(outputFile, inputRecordsBeforeStart, inputFiles);
            else
                assertOutputFileRecordsMatchInputFiles(outputFile, 0, inputFiles);
        }

        protected void validateInputNotEmpty(Path[] inputFiles) throws IOException {
            // SANITYCHECK: Make sure we didn't shoot blanks...
            for(Path f : inputFiles)
                assertTrue(Files.size(f) > 0);
            assertTrue(Files.size(outputFile) > 0);
        }

        protected void processAllInputFiles() throws Exception {
            Preconditions.checkState(tailer.isRunning());
            tailer.processAllRecords();
        }

        @Override
        public Void call() throws Exception {
          start();
          doRun();
          end();
          return null;
        }

        protected abstract void doRun() throws Exception;
    }

    public abstract class TestableFileTailerFactory<R extends IRecord> {
        protected abstract TestableFileTailer<R> create(AgentContext context, FileFlow<R> flow, FileSender<R> sender, FileCheckpointStore checkpoints) throws IOException;
    }

    public class AsyncFileTailerFactory extends TestableFileTailerFactory<FirehoseRecord> {
        @Override
        protected TestableFileTailer<FirehoseRecord> create(AgentContext context, FileFlow<FirehoseRecord> flow, FileSender<FirehoseRecord> sender, FileCheckpointStore checkpoints) throws IOException {
            return TestableFileTailer.createFirehoseTailer(context, flow, sender, checkpoints);
        }
    }
}
