/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.concurrent.NotThreadSafe;

import lombok.ToString;

import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Copies content of current file to a new file and truncates current file.
 * Simulates behavior of <code>logrotate</code> unix tool with
 * <code>copytruncate</code> option.
 */
@NotThreadSafe
@ToString(callSuper=true)
public class TruncateFileRotator extends FileRotator {
    private long lastPreRotationSize = -1;

    public TruncateFileRotator(Path dir, String prefix) {
        super(dir, prefix);
    }

    @Override
    protected Path rotateOrCreateNewFile() throws IOException {
        // if there are old files, rotate their names
        if(activeFiles.size() > 1) {
            rotateFileName(1);
        }
        if(!activeFiles.isEmpty() && Files.exists(getFile(0))) {
            // if there's an existing file, copy/truncate it
            Path currentPath = getFile(0);
            Path rotatedPath = getFilePathAfterRotation(0);
            logger.debug("Copying contents of {} to {}", currentPath, rotatedPath);
            Files.copy(currentPath, rotatedPath);
            activeFiles.set(0, rotatedPath);
            logger.debug("Truncating contents of {}", currentPath);
            TestUtils.truncateFile(currentPath);
            return currentPath;
        } else {
            // create a new file
            Path newPath = getNewFilePath();
            logger.debug("Creating new file {}", newPath);
            Files.createFile(newPath);
            return newPath;
        }
    }

    @Override
    protected int writeDataToLatestFileBeforeRotation() {
        if(!activeFiles.isEmpty()) {
            try {
                if(Files.exists(getLatestFile()))
                    lastPreRotationSize = Files.size(getLatestFile());
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
        return super.writeDataToLatestFileBeforeRotation();
    }

    @Override
    protected int writeDataToNewFileAfterRotation() {
        // After truncation, the file is required to be smaller than it was
        // before rotation.
        // If we don't do that, this will cause lots of trouble in the unit tests.
        // This is not un-realistic because the normal situation we are trying to
        // simulate here (Truncate rotation) behaves this way except in *very* rare
        // occasions.
        // Our code does not currently handle the case where the truncated file
        // is filled up to a size larger than its previous size.
        long dataSizeToWrite = (long) (minNewFileSize * ThreadLocalRandom.current().nextDouble(1.5, 2));
        if(lastPreRotationSize > 0 && dataSizeToWrite >= lastPreRotationSize) {
            dataSizeToWrite = lastPreRotationSize - 1;
            logger.debug("Limiting new file size to {} to make sure it's smaller than size before rotation ({})",
                    dataSizeToWrite, lastPreRotationSize);
            // This is not expected to happen before DEFAULT_MIN_NEW_FILE_SIZE_BYTES rotations
            // But check for it defensively, nevertheless.
            Preconditions.checkState(dataSizeToWrite > 0, "New file cannot be created with size 0");
        }
        int result = appendDataToLatestFile(dataSizeToWrite);
        try {
            Preconditions.checkState(lastPreRotationSize < 0 || Files.size(getFile(0)) < lastPreRotationSize, "New file created with size larger than previously rotated file. This will cause rotation detection impossible.");
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        return result;
    }

    @Override
    boolean latestFileKeepsSamePath() {
        return true;
    }

    @Override
    boolean latestFileKeepsSameId() {
        return true;
    }
}