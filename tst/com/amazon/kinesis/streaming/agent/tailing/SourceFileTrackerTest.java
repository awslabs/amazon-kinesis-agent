/*
 * Copyright (c) 2014-2017 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;

import lombok.Cleanup;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FileFlowFactory;
import com.amazon.kinesis.streaming.agent.tailing.FileId;
import com.amazon.kinesis.streaming.agent.tailing.SourceFileTracker;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFileList;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpoint;
import com.amazon.kinesis.streaming.agent.tailing.testing.CopyFileRotator;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileRotator;
import com.amazon.kinesis.streaming.agent.tailing.testing.RememberedTrackedFile;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;
import com.amazon.kinesis.streaming.agent.tailing.testing.TestableSourceFileTracker;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;

public class SourceFileTrackerTest extends TailingTestBase {
    private static final int TEST_REPS = 2;

    private AgentContext context;

    @BeforeMethod
    public void setup() throws IOException {
        context = getTestAgentContext();
    }

    protected TestableSourceFileTracker getTracker(FileRotator rotator) throws IOException {
        // create the tracker for these files
        FileFlow<?> flow = getFlow(rotator);
        return new TestableSourceFileTracker(rotator, flow, context);
    }

    protected FileFlow<?> getFlow(FileRotator rotator) throws IOException {
        Configuration flowConfig = new Configuration(getTestFlowConfig(rotator.getInputFileGlob()));
        return new FileFlowFactory().getFileFlow(context, flowConfig);
    }

    @DataProvider(name="rotators")
    public Object[][] rotatorsProvider() {
        return new Object[][] {
                { new RenameFileRotatorFactory() },
                { new CreateFileRotatorFactory() },
                { new CopyFileRotatorFactory() },
                { new TruncateFileRotatorFactory() },
        };
    }

    @Test(dataProvider="rotators")
    public void testInitializeWithoutCheckpointOpensLatestFile(FileRotatorFactory rotatorFactory) throws IOException {
        FileRotator rotator = rotatorFactory.create();
        rotator.rotate(2);
        SourceFileTracker tracker = getTracker(rotator);
        tracker.initialize();
        Assert.assertNotNull(tracker.getCurrentOpenFile());
        Assert.assertEquals(tracker.getCurrentOpenFile().getPath(), rotator.getLatestFile());
        Assert.assertTrue(tracker.getCurrentOpenFile().isOpen());
        Assert.assertEquals(tracker.getCurrentOpenFile().getCurrentOffset(), 0);
        Assert.assertFalse(tracker.newerFilesPending());
    }

    @Test(dataProvider="rotators")
    public void testInitializeWithCheckpointAndNoRotations(FileRotatorFactory rotatorFactory) throws IOException {
        FileRotator rotator = rotatorFactory.create();
        SourceFileTracker tracker = getTracker(rotator);
        rotator.rotate();
        long offset = Files.size(rotator.getLatestFile());
        rotator.appendDataToLatestFile(10000);
        Path cpFile = rotator.getLatestFile();
        TrackedFile firstFile = new TrackedFile(tracker.flow, cpFile);
        RememberedTrackedFile rememberedFirstFile = new RememberedTrackedFile(firstFile);

        // Then initialize with the CP and make sure that the file was picked up
        FileCheckpoint cp = new FileCheckpoint(firstFile, offset);
        Assert.assertTrue(tracker.initialize(cp));

        Assert.assertNotNull(tracker.getCurrentOpenFile());
        Assert.assertEquals(tracker.getCurrentOpenFile().getPath(), cpFile);
        Assert.assertTrue(rememberedFirstFile.hasSameContentHash(tracker.getCurrentOpenFile().getPath()));
        Assert.assertTrue(tracker.getCurrentOpenFile().isOpen());
        Assert.assertEquals(tracker.getCurrentOpenFile().getCurrentOffset(), offset);
        Assert.assertFalse(tracker.newerFilesPending());
    }

    @Test(dataProvider="rotators")
    public void testInitializeWithCheckpointAndRotation(FileRotatorFactory rotatorFactory) throws IOException {
        final int maxFilesToKeepOnDisk = 3;
        FileRotator rotator = rotatorFactory.create();
        rotator.setMaxFilesToKeepOnDisk(maxFilesToKeepOnDisk);
        SourceFileTracker tracker = getTracker(rotator);
        rotator.rotate();
        long offset = Files.size(rotator.getLatestFile());
        rotator.appendDataToLatestFile(10000);
        Path cpFile = rotator.getLatestFile();
        TrackedFile firstFile = new TrackedFile(tracker.flow, cpFile);
        FileCheckpoint cp = new FileCheckpoint(firstFile, offset);

        // Rotate one or more times, as long as the file current file doesn't get deleted
        RememberedTrackedFile rememberedFirstFile = new RememberedTrackedFile(firstFile);
        final int maxRotations = maxFilesToKeepOnDisk - 1;
        rotator.rotate(ThreadLocalRandom.current().nextInt(1, maxRotations));

        // Then initialize with the CP and make sure that the same file was picked up
        Assert.assertTrue(tracker.initialize(cp));
        Assert.assertNotNull(tracker.getCurrentOpenFile());
        Assert.assertTrue(tracker.getCurrentOpenFile().isOpen());
        Assert.assertEquals(tracker.getCurrentOpenFile().getCurrentOffset(), offset);
        if (!(rotator instanceof CopyFileRotator)) {
            Assert.assertTrue(tracker.newerFilesPending());
        }
        Assert.assertTrue(rememberedFirstFile.hasSameContentHash(tracker.getCurrentOpenFile().getPath()));
    }

    @Test(dataProvider="rotators")
    public void testInitializeWithCheckpointAndFileDisappearedAndAnotherFilePresent(FileRotatorFactory rotatorFactory) throws IOException {
        FileRotator rotator = rotatorFactory.create();
        SourceFileTracker tracker = getTracker(rotator);
        rotator.rotate();
        long offset = Files.size(rotator.getLatestFile());
        rotator.appendDataToLatestFile(10000);
        Path cpFile = rotator.getLatestFile();
        TrackedFile firstFile = new TrackedFile(tracker.flow, cpFile);
        FileCheckpoint cp = new FileCheckpoint(firstFile, offset);

        // Delete the file, and create a new one
        RememberedTrackedFile rememberedFirstFile = new RememberedTrackedFile(firstFile);
        moveFileToTrash(rotator.getFile(0));
        rotator.rotate();

        // Then initialize with the CP and make sure that tracker will recover
        Assert.assertFalse(tracker.initialize(cp));
        Assert.assertNotNull(tracker.getCurrentOpenFile());
        Assert.assertTrue(tracker.getCurrentOpenFile().isOpen());
        Assert.assertEquals(tracker.getCurrentOpenFile().getPath(), rotator.getLatestFile());
        Assert.assertEquals(tracker.getCurrentOpenFile().getCurrentOffset(), 0);
        Assert.assertFalse(rememberedFirstFile.hasSameContentHash(tracker.getCurrentOpenFile().getPath()));
    }

    @Test(dataProvider="rotators")
    public void testInitializeWithCheckpointAndFileDisappearedAndNoMoreFilesPresent(FileRotatorFactory rotatorFactory) throws IOException {
        FileRotator rotator = rotatorFactory.create();
        SourceFileTracker tracker = getTracker(rotator);
        rotator.rotate();
        long offset = Files.size(rotator.getLatestFile());
        rotator.appendDataToLatestFile(10000);
        Path cpFile = rotator.getLatestFile();
        TrackedFile firstFile = new TrackedFile(tracker.flow, cpFile);
        FileCheckpoint cp = new FileCheckpoint(firstFile, offset);

        // Delete the file
        moveFileToTrash(rotator.getFile(0));

        // Then initialize with the CP and make sure that tracker will recover
        Assert.assertFalse(tracker.initialize(cp));
        Assert.assertNull(tracker.getCurrentOpenFile());
    }

    @Test(dataProvider="rotators")
    public void testRefreshWithNoChangesInFiles(FileRotatorFactory rotatorFactory) throws IOException {
        FileRotator rotator = rotatorFactory.create();
        rotator.rotate(2);
        TestableSourceFileTracker tracker = getTracker(rotator);
        tracker.initialize();
        Path f0 = rotator.getLatestFile();
        FileId f0Id = FileId.get(f0);
        Assert.assertEquals(tracker.getCurrentOpenFile().getPath(), f0);
        Assert.assertEquals(tracker.getCurrentOpenFile().getId(), f0Id);
        Assert.assertTrue(tracker.getCurrentOpenFile().isOpen());
        Assert.assertEquals(tracker.getCurrentOpenFile().getCurrentOffset(), 0);
        Assert.assertFalse(tracker.newerFilesPending());
        tracker.rememberCurrentOpenFile();

        // call refresh, and make sure no changes happened
        tracker.refresh();
        tracker.assertStillTrackingRememberedFileWithoutRotation();

        // call refresh again, and make sure nothing changed this time either
        tracker.refresh();
        tracker.assertStillTrackingRememberedFileWithoutRotation();
    }

    @Test(dataProvider="rotators")
    public void testRefreshWithNoFilesFound(FileRotatorFactory rotatorFactory) throws IOException {
        FileRotator rotator = rotatorFactory.create();
        SourceFileTracker tracker = getTracker(rotator);
        tracker.initialize();

        // call refresh when there are no files to be tracked
        tracker.refresh();
        Assert.assertNull(tracker.getCurrentOpenFile());
        Assert.assertTrue(tracker.currentSnapshot.isEmpty());
        Assert.assertTrue(tracker.pendingFiles.isEmpty());
        Assert.assertFalse(tracker.newerFilesPending());

        // call it again and validate for good measure
        tracker.refresh();
        Assert.assertNull(tracker.getCurrentOpenFile());
        Assert.assertTrue(tracker.currentSnapshot.isEmpty());
        Assert.assertTrue(tracker.pendingFiles.isEmpty());
        Assert.assertFalse(tracker.newerFilesPending());
    }

    @Test(dataProvider="rotators", invocationCount=TEST_REPS, skipFailedInvocations=true)
    public void testRefreshAfterRotation(FileRotatorFactory rotatorFactory) throws IOException {
        FileRotator rotator = rotatorFactory.create();
        // create few files
        rotator.rotate(2);

        // create the tracker for these files
        TestableSourceFileTracker tracker = getTracker(rotator);
        tracker.initialize();

        // make sure we are tracking latest file as expected
        Assert.assertFalse(tracker.newerFilesPending());
        Assert.assertEquals(tracker.getCurrentOpenFileIndex(), 0);
        Assert.assertEquals(tracker.getCurrentOpenFile().getPath(), rotator.getLatestFile());
        Assert.assertEquals(tracker.getCurrentOpenFile().getId(), FileId.get(rotator.getLatestFile()));

        // read a few bytes to advance offset
        ByteBuffer buff = ByteBuffer.allocate((int) (rotator.getMinNewFileSize() / 2));
        tracker.getCurrentOpenFile().getChannel().read(buff);
        Assert.assertEquals(tracker.getCurrentOpenFile().getCurrentOffset(), buff.limit());

        // save pre-rotation information
        tracker.rememberCurrentOpenFile();

        // now rotate file and refresh the tracker
        rotator.rotate();
        tracker.refresh();

        // we should see that the tracker is still tracking the same file
        tracker.assertStillTrackingRememberedFile();

        // write more data to latest file and refresh... nothing should change
        final long newBytes = 1024;
        rotator.appendDataToLatestFile(newBytes);
        tracker.refresh();
        tracker.assertStillTrackingRememberedFile();
    }

    @Test(dataProvider="rotators", invocationCount=TEST_REPS, skipFailedInvocations=true)
    public void testRefreshAfterMultipleRotationsButBeforeCurrentFileIsDeleted(FileRotatorFactory rotatorFactory) throws IOException {
        FileRotator rotator = rotatorFactory.create();
        // create new files
        rotator.rotate();

        // create the tracker for these files
        TestableSourceFileTracker tracker = getTracker(rotator);
        tracker.initialize();

        // make sure we are tracking latest/only file as expected
        Assert.assertFalse(tracker.newerFilesPending());
        Assert.assertEquals(tracker.getCurrentOpenFileIndex(), 0);
        Assert.assertEquals(tracker.getCurrentOpenFile().getPath(), rotator.getLatestFile());
        Assert.assertEquals(tracker.getCurrentOpenFile().getId(), FileId.get(rotator.getLatestFile()));

        // read a few bytes to advance offset
        ByteBuffer buff = ByteBuffer.allocate((int) (rotator.getMinNewFileSize() / 2));
        tracker.getCurrentOpenFile().getChannel().read(buff);
        Assert.assertEquals(tracker.getCurrentOpenFile().getCurrentOffset(), buff.limit());

        // save pre-rotation information
        tracker.rememberCurrentOpenFile();

        // now rotate file multiple times just but make sure the file isn't cleaned up by rotator
        final int rotations = rotator.getMaxFilesToKeepOnDisk() - 2;
        rotator.rotate(rotations);
        tracker.refresh();

        // we should see that the tracker is still tracking the same file
        tracker.assertStillTrackingRememberedFileAfterRotation();

        // write data to latest file and refresh... nothing should change
        final long newBytes = 1024;
        rotator.appendDataToLatestFile(newBytes);
        tracker.refresh();
        tracker.assertStillTrackingRememberedFileAfterRotation();
    }

    @Test(dataProvider="rotators")
    public void testCurrentFileDeletedCausesTailingToBeReset(FileRotatorFactory rotatorFactory) throws IOException {
        FileRotator rotator = rotatorFactory.create();
        // create new files
        rotator.rotate(2);

        // create the tracker for these files
        TestableSourceFileTracker tracker = getTracker(rotator);
        tracker.initialize();

        // make sure we are tracking latest/only file as expected
        Assert.assertFalse(tracker.newerFilesPending());
        Assert.assertEquals(tracker.getCurrentOpenFileIndex(), 0);
        Assert.assertEquals(tracker.getCurrentOpenFile().getPath(), rotator.getLatestFile());
        Assert.assertEquals(tracker.getCurrentOpenFile().getId(), FileId.get(rotator.getLatestFile()));

        // save pre-deletion information
        tracker.rememberCurrentOpenFile();

        // now delete the file and refresh
        moveFileToTrash(rotator.getFile(0));
        tracker.refresh();

        // validate that the tailer was reset to latest file
        tracker.assertCurrentFileWasNotFound();
        tracker.assertTailingWasReset();

        // validate that the initial channel was closed
        tracker.assertRememberedFileWasClosed();
    }

    @Test(dataProvider="rotators")
    public void testDeletingAllFilesWillStopTailingCompletely(FileRotatorFactory rotatorFactory) throws IOException {
        FileRotator rotator = rotatorFactory.create();
        // create new files
        rotator.rotate(2);

        // create the tracker for these files
        TestableSourceFileTracker tracker = getTracker(rotator);
        tracker.initialize();

        // save pre-deletion information
        tracker.rememberCurrentOpenFile();

        // now delete the file and refresh
        for (Path file : rotator.getActiveFiles())
            moveFileToTrash(file);
        tracker.refresh();
        tracker.assertTailingWasStopped();

        // validate that the initial channel was closed
        tracker.assertRememberedFileWasClosed();
    }

    /**
     * Regression test for a file-rotation race where the fallback file is deleted
     * between {@code listFiles()} and {@code open()}. Before the fix,
     * {@code startTailingNewFile} reassigned {@code currentOpenFile} before opening,
     * so when {@code open()} threw the tracker was left with {@code currentOpenFile}
     * no longer contained in {@code currentSnapshot}. Every subsequent refresh then
     * threw {@code IllegalArgumentException} ("Current file is not contained in
     * current snapshot!") forever, with no self-recovery. This test simulates the
     * race and asserts the tracker recovers on later refreshes.
     */
    @Test
    public void testRaceWhenFallbackFileDeletedDuringOpenIsRecoverable() throws IOException {
        // Use the create-new-file rotation mode: it produces distinct, on-demand
        // files (matching the real-world scenario this race was observed in) and
        // lets us delete individual files without disturbing rotator bookkeeping.
        FileRotator rotator = new CreateFileRotatorFactory().create();
        rotator.rotate(2);
        FileFlow<?> flow = getFlow(rotator);

        // One-shot injection: simulate the file vanishing between the directory
        // listing and the open() that startTailingNewFile performs on it.
        final boolean[] injectRace = { false };
        TestableSourceFileTracker tracker = new TestableSourceFileTracker(rotator, flow, context) {
            @Override
            protected void startTailingNewFile(TrackedFileList newSnapshot, int index) throws IOException {
                if (injectRace[0]) {
                    injectRace[0] = false;
                    moveFileToTrash(newSnapshot.get(index).getPath());
                }
                super.startTailingNewFile(newSnapshot, index);
            }
        };
        tracker.initialize();
        Assert.assertNotNull(tracker.getCurrentOpenFile());
        TrackedFile originalOpenFile = tracker.getCurrentOpenFile();

        // Delete the current open file so the next refresh falls back to the other
        // file, then make that fallback vanish during open() (the race).
        moveFileToTrash(rotator.getFile(0));
        injectRace[0] = true;
        try {
            tracker.refresh();
        } catch (IOException expected) {
            // The vanished-file open() surfaces as a transient IOException; the agent
            // logs and retries. The bug being fixed is the PERMANENT failure that
            // followed it, not this transient error.
        }

        // Fix A: state was not corrupted by the failed open(), so the next refresh
        // must NOT throw IllegalArgumentException.
        tracker.refresh();

        // With both files gone the tracker should have cleanly stopped tailing.
        Assert.assertNull(tracker.getCurrentOpenFile());

        // And it self-recovers: once a new file appears it resumes tailing.
        rotator.rotate();
        tracker.refresh();
        Assert.assertNotNull(tracker.getCurrentOpenFile());
        Assert.assertTrue(tracker.getCurrentOpenFile().isOpen());
        Assert.assertNotSame(tracker.getCurrentOpenFile(), originalOpenFile);
    }

    /**
     * Regression test for the recovery guard: if the tracker's internal state ever
     * becomes inconsistent and {@code updateCurrentFile} throws an
     * {@code IllegalArgumentException}/{@code IllegalStateException}, {@code refresh()}
     * must reset to a recoverable state rather than propagating the exception on every
     * poll forever.
     */
    @Test
    public void testRefreshRecoversFromInconsistentTrackerState() throws IOException {
        FileRotator rotator = new CreateFileRotatorFactory().create();
        rotator.rotate(2);
        FileFlow<?> flow = getFlow(rotator);

        final boolean[] failOnce = { false };
        SourceFileTracker tracker = new SourceFileTracker(context, flow) {
            @Override
            boolean updateCurrentFile(TrackedFileList newSnapshot) throws IOException {
                if (failOnce[0]) {
                    failOnce[0] = false;
                    throw new IllegalArgumentException("Current file is not contained in current snapshot!");
                }
                return super.updateCurrentFile(newSnapshot);
            }
        };
        tracker.initialize();
        Assert.assertNotNull(tracker.getCurrentOpenFile());

        // First refresh hits the simulated inconsistency. It must be swallowed and
        // the tracker reset to the no-current-file state, not propagated.
        failOnce[0] = true;
        tracker.refresh();
        Assert.assertNull(tracker.getCurrentOpenFile());

        // Subsequent refresh re-initializes from disk and resumes tailing.
        tracker.refresh();
        Assert.assertNotNull(tracker.getCurrentOpenFile());
        Assert.assertTrue(tracker.getCurrentOpenFile().isOpen());
    }

    @Test(description = "Test behavior of an open channel when more data is appeneded.")
    public void testFileChannelWithMoreDataAppended() throws IOException {
        Path tmp = testFiles.createTempFile();
        final String testLine1 = "test1\n";
        final String testLine2 = "test22\n";
        final String testLine3 = "test333\n";
        TestUtils.appendToFile(testLine1, tmp);
        TestUtils.appendToFile(testLine2, tmp);
        @Cleanup FileChannel tmpChannel = FileChannel.open(tmp, StandardOpenOption.READ);
        Assert.assertEquals(tmpChannel.size(), testLine1.length() + testLine2.length());
        ByteBuffer buffer = ByteBuffer.allocate(testLine1.length());
        tmpChannel.read(buffer);
        buffer.flip();
        String result = ByteBuffers.toString(buffer, StandardCharsets.UTF_8);
        Assert.assertEquals(result, testLine1);
        Assert.assertEquals(tmpChannel.position(), testLine1.length());
        Assert.assertEquals(tmpChannel.size() - tmpChannel.position(), testLine2.length());

        // append more lines and see the data show up in the channel
        TestUtils.appendToFile(testLine3, tmp);
        Assert.assertEquals(tmpChannel.position(), testLine1.length()); // position shouldn't change
        Assert.assertEquals(tmpChannel.size(), testLine1.length() + testLine2.length() + testLine3.length());
        Assert.assertEquals(tmpChannel.size() - tmpChannel.position(), testLine2.length() + testLine3.length());
        buffer = ByteBuffer.allocate(testLine2.length() + testLine3.length());
        tmpChannel.read(buffer);
        buffer.flip();
        result = ByteBuffers.toString(buffer, StandardCharsets.UTF_8);
        Assert.assertEquals(result, testLine2 + testLine3);
    }

    @Test(description = "Test behavior of an open channel when underlying file is truncated.")
    public void testFileChannelWhenFileIsTruncated() throws IOException {
        Path tmp = testFiles.createTempFile();
        final String testLine1 = "test1\n";
        final String testLine2 = "test22\n";
        final String testLine3 = "test333\n";
        TestUtils.appendToFile(testLine1, tmp);
        TestUtils.appendToFile(testLine2, tmp);
        TestUtils.appendToFile(testLine3, tmp);
        @Cleanup FileChannel tmpChannel = FileChannel.open(tmp, StandardOpenOption.READ);
        ByteBuffer buffer = ByteBuffer.allocate(testLine1.length());
        tmpChannel.read(buffer);
        buffer.flip();
        String result = ByteBuffers.toString(buffer, StandardCharsets.UTF_8);
        Assert.assertEquals(result, testLine1);
        long oldPosition = tmpChannel.position();
        // now truncate the file externally
        TestUtils.truncateFile(tmp);
        Assert.assertEquals(Files.size(tmp), 0);
        // make sure size is reflected in old channel, but position remains intact
        Assert.assertEquals(tmpChannel.size(), 0);
        Assert.assertEquals(tmpChannel.position(), oldPosition);
        // try reading some more from original channel
        tmpChannel.read(buffer);
        buffer.flip();
        result = ByteBuffers.toString(buffer, StandardCharsets.UTF_8);
        Assert.assertEquals(result, "");
    }
}
