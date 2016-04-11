/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.testing;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.tailing.FileId;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;
import com.google.common.base.Throwables;

/**
 * Unit tests expected to pass for all implementations of {@link FileRotator}.
 */
public abstract class FileRotatorTestBase extends TestBase {
    static final int TEST_REPS = 1;
    static final String DEFAULT_ROTATOR_DIR_NAME = "logs";
    static final String DEFAULT_ROTATOR_PREFIX = "app.log";

    protected abstract FileRotator getFileRotator(Path root, String prefix);

    protected FileRotator getFileRotator(Path root) {
        return getFileRotator(root, DEFAULT_ROTATOR_PREFIX);
    }

    protected FileRotator getFileRotator(int maxFiles) {
        FileRotator rotator = getFileRotator(testFiles.getTmpDir().resolve(DEFAULT_ROTATOR_DIR_NAME), DEFAULT_ROTATOR_PREFIX);
        rotator.setMaxFilesToKeepOnDisk(maxFiles);
        return rotator;
    }

    protected FileRotator getFileRotator() {
        return getFileRotator(testFiles.getTmpDir().resolve(DEFAULT_ROTATOR_DIR_NAME));
    }

    private void assertRotatorPoliciesAfterRotation(FileRotator rotator,
            Path latestPathBeforeRotation,
            FileId latestIdBeforeRotation) throws IOException {
        // latest file
        Path latestPath = rotator.getLatestFile();
        FileId latestId = FileId.get(latestPath);
        if(rotator.latestFileKeepsSameId()) {
            Assert.assertEquals(latestId, latestIdBeforeRotation);
        } else {
            Assert.assertNotEquals(latestId, latestIdBeforeRotation);
        }
        if(rotator.latestFileKeepsSamePath()) {
            Assert.assertEquals(latestPath, latestPathBeforeRotation);
        } else {
            Assert.assertNotEquals(latestPath, latestPathBeforeRotation);
        }

        // rotated file
        Path rotatedPath = rotator.getFile(1);
        FileId rotatedId = FileId.get(rotatedPath);
        if(rotator.rotatedFileKeepsSameId()) {
            Assert.assertEquals(rotatedId, latestIdBeforeRotation);
        } else {
            Assert.assertNotEquals(rotatedId, latestIdBeforeRotation);
        }
        if(rotator.rotatedFileKeepsSamePath()) {
            Assert.assertEquals(rotatedPath, latestPathBeforeRotation);
        } else {
            Assert.assertNotEquals(rotatedPath, latestPathBeforeRotation);
        }

        // timestamp should not decrease
        long latestTimestamp = Files.getLastModifiedTime(latestPath).toMillis();
        long rotatedTimestamp = Files.getLastModifiedTime(rotatedPath).toMillis();
        Assert.assertTrue(rotatedTimestamp <= latestTimestamp);
    }

    @Test(invocationCount=TEST_REPS, skipFailedInvocations=true)
    public void testCreateFirstFile() throws IOException {
        FileRotator rotator = getFileRotator();
        Path rotated = rotator.rotate();
        // first rotation returns null, because no existing files were there
        Assert.assertNull(rotated);

        // check the properties of the new file
        Path actualNewFilePath = rotator.getLatestFile();
        Assert.assertTrue(Files.exists(actualNewFilePath));
        Assert.assertEquals(rotator.getMaxInputFileIndex(), 0);

        // make sure some data is written to the new file
        Assert.assertTrue(Files.size(actualNewFilePath) > 0);
    }

    @Test(invocationCount=TEST_REPS, skipFailedInvocations=true)
    public void testRotation() throws IOException {
        FileRotator rotator = getFileRotator();

        // First file
        rotator.rotate();
        Path latestPathBeforeRotation = rotator.getLatestFile();
        FileId latestIdBeforeRotation = FileId.get(latestPathBeforeRotation);

        // Now rotate and check the return value and the state of the files
        rotator.rotate();
        Assert.assertEquals(rotator.getActiveFiles().size(), 2);
        assertRotatorPoliciesAfterRotation(rotator, latestPathBeforeRotation, latestIdBeforeRotation);

        // check the new/old file are both still there
        Assert.assertTrue(Files.exists(rotator.getFile(0)));
        Assert.assertTrue(Files.exists(rotator.getFile(1)));

        // make sure some data is written to the new file
        Assert.assertTrue(Files.size(rotator.getLatestFile()) > 0);
    }

    @Test(invocationCount=TEST_REPS, skipFailedInvocations=true)
    public void testFilesDeletedWhenMaxFilesReached() throws IOException {
        final int maxFiles = 4;
        FileRotator rotator = getFileRotator(maxFiles);

        // rotate up to max files and check state of files
        rotator.rotate(maxFiles);
        Assert.assertEquals(rotator.getActiveFiles().size(), maxFiles);

        // keep track of the last 2 files
        Path lastFile = rotator.getFile(maxFiles-1);
        FileId lastFileId = FileId.get(lastFile);

        Path nextToLastFile = rotator.getFile(maxFiles - 2);
        FileId nextToLastFileId = FileId.get(nextToLastFile);
        long nextToLastTimestamp = Files.getLastModifiedTime(nextToLastFile).toMillis();

        // add one more file
        rotator.rotate();
        Assert.assertEquals(rotator.getActiveFiles().size(), maxFiles);

        // and last file now is what used to be next-to-last
        Path newLastFile = rotator.getFile(maxFiles-1);
        FileId newLastFileId = FileId.get(newLastFile);
        long newLastTimestamp = Files.getLastModifiedTime(newLastFile).toMillis();
        Assert.assertEquals(newLastFileId, nextToLastFileId);
        Assert.assertEquals(newLastTimestamp, nextToLastTimestamp);

        // make sure the old file is nowhere to be found on the filesystem
        for(Path p : listFiles(rotator.getDir())) {
            Assert.assertNotEquals(
                    FileId.get(p),
                    lastFileId,
                    String.format("%s: Found deleted file: Deleted file: %s (%s), Found file: %s (%s)",
                            rotator.getClass().getSimpleName(), lastFile.toString(),
                            lastFileId.toString(), p.toString(), FileId.get(p).toString()));
        }
    }

    @Test(invocationCount=TEST_REPS, skipFailedInvocations=true)
    public void testMultipleRotations() throws IOException {
        final int maxFiles = 4;
        int rotations = maxFiles - 1;
        FileRotator rotator = getFileRotator(maxFiles);
        rotator.rotate(rotations);

        // make sure all files are as expected, and last file is not empty
        Assert.assertEquals(rotator.getActiveFiles().size(), rotations);
        Assert.assertTrue(Files.size(rotator.getLatestFile()) > 0);
        for (int i = 0; i < rotator.getMaxInputFileIndex(); ++i) {
            Assert.assertTrue(
                    TrackedFile.isNewer(rotator.getFile(i), rotator.getFile(i + 1)),
                    String.format("%s: Expecting %s@%s (%d bytes) to be newer than %s@%s (%d bytes)",
                            rotator.getClass().getSimpleName(),
                            rotator.getFile(i), Files.getLastModifiedTime(rotator.getFile(i)).toString(),
                            Files.size(rotator.getFile(i)), rotator.getFile(i + 1),
                            Files.getLastModifiedTime(rotator.getFile(i + 1)).toString(),
                            Files.size(rotator.getFile(i + 1))));
        }
    }

    @Test(invocationCount=TEST_REPS, skipFailedInvocations=true)
    public void testGetMaxInputFileIndex() throws IOException {
        // This test is valid for all except CopyFileRotator. See CopyFileRotatorTest.
        FileRotator rotator = getFileRotator();
        Assert.assertEquals(rotator.getMaxInputFileIndex(), -1);

        // First file
        rotator.rotate();
        Assert.assertEquals(rotator.getMaxInputFileIndex(), 0);

        // Now rotate another time and check
        rotator.rotate();
        Assert.assertEquals(rotator.getMaxInputFileIndex(), 1);

        // Now rotate another time and check
        rotator.rotate();
        Assert.assertEquals(rotator.getMaxInputFileIndex(), 2);
    }

    // List all files in directory
    protected List<Path> listFiles(Path dir) {
        List<Path> result = new ArrayList<>();
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(dir)) {
            for (Path p : directoryStream) {
                if(!Files.isDirectory(p)) {
                    result.add(p);
                }
            }
            return result;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
