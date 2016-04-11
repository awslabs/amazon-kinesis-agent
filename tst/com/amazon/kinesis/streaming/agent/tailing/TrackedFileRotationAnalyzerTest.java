/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.io.IOException;
import java.nio.file.Path;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.tailing.SourceFile;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFileList;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFileRotationAnalyzer;
import com.amazon.kinesis.streaming.agent.tailing.testing.CreateFileRotator;
import com.amazon.kinesis.streaming.agent.tailing.testing.FileRotator;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;
import com.amazon.kinesis.streaming.agent.tailing.testing.TruncateFileRotator;

public class TrackedFileRotationAnalyzerTest extends TailingTestBase {
    private SourceFile getSourceFile(FileRotator rotator) {
        return new SourceFile(null, rotator.getInputFileGlob());
    }

    @Test
    public void testInitializeAnalyzer() throws IOException {
        final int maxFilesToKeep = 6;
        final int initialFiles = 2;
        CreateFileRotator rotator = (CreateFileRotator) new CreateFileRotatorFactory().withMaxFilesToKeepOnDisk(maxFilesToKeep).create();
        SourceFile source = getSourceFile(rotator);
        // create few files and take snapshot
        rotator.rotate(initialFiles);
        TrackedFileList current = source.listFiles();
        // now rotate one more time and take another snapshot
        rotator.rotate();
        TrackedFileList incoming = source.listFiles();
        // create the analyzer which computes the counterparts
        TrackedFileRotationAnalyzer analyzer = new TrackedFileRotationAnalyzer(current, incoming, null);

        // check the current counterparts
        for(int i = 0; i < current.size(); ++i) {
            Assert.assertTrue(analyzer.hasCounterpart(current.get(i)));
            Assert.assertEquals(analyzer.getCounterpartIndex(current.get(i)), i+1);
            Assert.assertEquals(analyzer.getCounterpart(current.get(i)).getId(), incoming.get(i+1).getId());
        }
        // check the incoming counterparts
        Assert.assertFalse(analyzer.hasCounterpart(incoming.get(0)));
        for(int i = 1; i < incoming.size(); ++i) {
            Assert.assertTrue(analyzer.hasCounterpart(incoming.get(i)));
            Assert.assertEquals(analyzer.getCounterpartIndex(incoming.get(i)), i-1);
            Assert.assertEquals(analyzer.getCounterpart(incoming.get(i)).getId(), current.get(i-1).getId());
        }
    }

    @Test
    public void testNoRotationCheckWhenTrue() throws IOException {
        final int maxFilesToKeep = 6;
        final int initialFiles = 2;
        CreateFileRotator rotator = (CreateFileRotator) new CreateFileRotatorFactory().withMaxFilesToKeepOnDisk(maxFilesToKeep).create();
        SourceFile source = getSourceFile(rotator);

        // create few files and take snapshot
        rotator.rotate(initialFiles);
        TrackedFileList current = source.listFiles();
        TrackedFile currentOpenFile = current.get(0);

        // Write some data, then take another snapshot without rotating.. repeat few more time
        for(int i = 0; i < 10; ++i) {
            rotator.appendDataToLatestFile(1024);
            TrackedFileList incoming = source.listFiles();
            Assert.assertEquals(incoming.size(), current.size());
            // Create the analyzer and make sure that it will detect that no rotation happened
            TrackedFileRotationAnalyzer analyzer = new TrackedFileRotationAnalyzer(current, incoming, currentOpenFile);
            Assert.assertTrue(analyzer.checkNoRotation());
            // after next rotation newer snapshot becomes current
            current = incoming;
            currentOpenFile = analyzer.getCounterpart(currentOpenFile);
        }
    }

    @Test
    public void testNoRotationCheckWhenOldFilesAreCleanedUp() throws IOException {
        final int maxFilesToKeep = 6;
        final int initialFiles = 4;
        CreateFileRotator rotator = (CreateFileRotator) new CreateFileRotatorFactory().withMaxFilesToKeepOnDisk(maxFilesToKeep).create();
        SourceFile source = getSourceFile(rotator);

        // create few files and take snapshot
        rotator.rotate(initialFiles);
        TrackedFileList current = source.listFiles();
        Assert.assertEquals(current.size(), initialFiles);
        TrackedFile currentOpenFile = current.get(0);

        // Delete the oldest file and take another snapshot and see that deleting old files does not change our logic
        moveFileToTrash(rotator.getFile(rotator.getMaxInputFileIndex()));
        TrackedFileList incoming = source.listFiles();
        Assert.assertEquals(incoming.size(), initialFiles-1);
        TrackedFileRotationAnalyzer analyzer = new TrackedFileRotationAnalyzer(current, incoming, currentOpenFile);
        Assert.assertTrue(analyzer.checkNoRotation());
    }

    @Test
    public void testNoRotationCheckWhenFalse() throws IOException {
        final int maxFilesToKeep = 6;
        final int initialFiles = 2;
        final int additionalRotations = 5;
        CreateFileRotator rotator = (CreateFileRotator) new CreateFileRotatorFactory().withMaxFilesToKeepOnDisk(maxFilesToKeep).create();
        SourceFile source = getSourceFile(rotator);
        // create few files and take snapshot
        rotator.rotate(initialFiles);
        TrackedFileList current = source.listFiles();
        TrackedFile currentOpenFile = current.get(0);
        // Rotate, keeping same current snapshot, and validate that a rotatation is detected every time
        for(int i = 0; i < additionalRotations;  ++i) {
            rotator.rotate();
            TrackedFileList incoming = source.listFiles();
            // Create the analyzer and make sure that it will detect the rotation
            TrackedFileRotationAnalyzer analyzer = new TrackedFileRotationAnalyzer(current, incoming, currentOpenFile);
            Assert.assertFalse(analyzer.checkNoRotation());
        }

        // Rotate, updating current snalshot, and validate that a rotatation is detected every time
        for(int i = 0; i < additionalRotations;  ++i) {
            rotator.rotate();
            TrackedFileList incoming = source.listFiles();
            // Create the analyzer and make sure that it will detect the rotation
            TrackedFileRotationAnalyzer analyzer = new TrackedFileRotationAnalyzer(current, incoming, currentOpenFile);
            Assert.assertFalse(analyzer.checkNoRotation());
            // after next rotation newer snapshot becomes current
            current = incoming;
            currentOpenFile = analyzer.getCounterpart(currentOpenFile);
        }
    }

    @Test
    public void testAllFilesHaveDisappeared() throws IOException {
        final int maxFilesToKeep = 6;
        final int initialFiles = 2;
        final int additionalRotations = 3;
        CreateFileRotator rotator = (CreateFileRotator) new CreateFileRotatorFactory().withMaxFilesToKeepOnDisk(maxFilesToKeep).create();
        SourceFile source = getSourceFile(rotator);
        // create few files and take snapshot
        rotator.rotate(initialFiles);
        TrackedFileList current = source.listFiles();

        // rotate and check allFilesHaveDisappeared
        rotator.rotate();
        TrackedFileList incoming = source.listFiles();
        TrackedFileRotationAnalyzer analyzer = new TrackedFileRotationAnalyzer(current, incoming, null);
        Assert.assertFalse(analyzer.allFilesHaveDisappeared());

        // delete one file and check it'll remain false
        current = incoming;
        moveFileToTrash(rotator.getFile(1));
        incoming = source.listFiles();
        analyzer = new TrackedFileRotationAnalyzer(current, incoming, null);
        Assert.assertFalse(analyzer.allFilesHaveDisappeared());

        // delete all file and see that it's now true (keep old current)
        for (Path file : rotator.getActiveFiles())
            moveFileToTrash(file);
        incoming = source.listFiles();
        analyzer = new TrackedFileRotationAnalyzer(current, incoming, null);
        Assert.assertTrue(analyzer.allFilesHaveDisappeared());

        // even if we create fre more file, it'll remain true since they are different files
        rotator.rotate(additionalRotations);
        incoming = source.listFiles();
        analyzer = new TrackedFileRotationAnalyzer(current, incoming, null);
        Assert.assertTrue(analyzer.allFilesHaveDisappeared());
    }

    @DataProvider(name="currentOpenFileTruncatedAfterRotation")
    public Object[][] getCurrentOpenFileTruncatedAfterRotationData() {
        return new Object[][] {
                {6,2,1},    // single rotation
                {6,2,3}     // multiple rotations
        };
    }

    @Test(dataProvider="currentOpenFileTruncatedAfterRotation")
    public void testCurrentOpenFileTruncatedAfterRotation(int maxFilesToKeep, int initialFiles, int additionalRotations) throws IOException {
        TruncateFileRotator rotator = (TruncateFileRotator) new TruncateFileRotatorFactory().withMaxFilesToKeepOnDisk(maxFilesToKeep).create();
        SourceFile source = getSourceFile(rotator);
        // create few files and take snapshot
        rotator.rotate(initialFiles);
        TrackedFileList current = source.listFiles();

        // get another snapshot and check that no truncation will be detected
        TrackedFileList incoming = source.listFiles();
        TrackedFile currentOpenFile = current.get(0);
        TrackedFileRotationAnalyzer analyzer = new TrackedFileRotationAnalyzer(current, incoming, currentOpenFile);
        Assert.assertFalse(analyzer.currentOpenFileWasTruncated());

        // now rotate and check that truncation was detected
        current = incoming;
        currentOpenFile = current.get(0);
        rotator.rotate(additionalRotations);
        incoming = source.listFiles();
        analyzer = new TrackedFileRotationAnalyzer(current, incoming, currentOpenFile);
        Assert.assertTrue(analyzer.currentOpenFileWasTruncated());

        // get another snapshot and without rotation and check that no truncation will be detected
        current = incoming;
        incoming = source.listFiles();
        currentOpenFile = current.get(0);
        analyzer = new TrackedFileRotationAnalyzer(current, incoming, currentOpenFile);
        Assert.assertFalse(analyzer.currentOpenFileWasTruncated());
    }

    @DataProvider(name="findCurrentOpenFileAfterTruncation")
    public Object[][] getFindCurrentOpenFileAfterTruncationData() {
        return new Object[][] {
                {6,2,1},    // single rotation
                {6,2,3}     // multiple rotations
        };
    }

    @Test(dataProvider="findCurrentOpenFileAfterTruncation")
    public void testFindCurrentOpenFileAfterTruncation(int maxFilesToKeep, int initialFiles,int additionalRotations) throws IOException {
        TruncateFileRotator rotator = (TruncateFileRotator) new TruncateFileRotatorFactory().withMaxFilesToKeepOnDisk(maxFilesToKeep).create();
        SourceFile source = getSourceFile(rotator);

        // create few files and take snapshot
        rotator.rotate(initialFiles);
        TrackedFileList current = source.listFiles();
        TrackedFile currentOpenFile = current.get(0);


        // now rotate and check that truncation was detected
        rotator.rotate(additionalRotations);
        TrackedFileList incoming = source.listFiles();
        TrackedFileRotationAnalyzer analyzer = new TrackedFileRotationAnalyzer(current, incoming, currentOpenFile);
        Assert.assertTrue(analyzer.currentOpenFileWasTruncated());
        // see if analyzer will figure out the right file
        Assert.assertEquals(analyzer.findCurrentOpenFileAfterTruncate(), additionalRotations);
    }
}