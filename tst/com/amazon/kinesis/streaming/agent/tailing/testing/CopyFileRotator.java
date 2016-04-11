/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import lombok.ToString;

import com.amazon.kinesis.streaming.agent.tailing.SourceFile;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.google.common.collect.ImmutableList;

/**
 * Copies the contents of the current file to a new file, but does not truncate
 * the current file. The current file remains the latest file.
 * Simulates behavior of <code>logrotate</code> unix tool with
 * <code>copy</code> option.
 * NOTE: Files produced by this rotator do not work with
 *       {@link TrackedFile#isNewer(TrackedFile, TrackedFile)} because the latest
 *       file is not truncated, and therefore is of equal or larger size as the
 *       file that was just copied, and likely the same timestamp.
 *       The only way this rotation mode is supported is if the {@link SourceFile}
 *       instance uses a specific file to track, as opposed to a glob; e.g.
 *       <code>/var/log/app.log</code> instead of <code>/var/log/app.log*</code>
 */
@NotThreadSafe
@ToString(callSuper=true)
public class CopyFileRotator extends FileRotator {

//    public CopyFileRotator(Path dir, String prefix, int maxFilesToKeep) {
//        super(dir, prefix, maxFilesToKeep);
//    }
//
    public CopyFileRotator(Path dir, String prefix) {
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
    public String getInputFileGlob() {
        return String.format("%s/%s", dir.toString(), prefix);
    }

    @Override
    public List<Path> getInputFiles() {
        return !activeFiles.isEmpty() ? ImmutableList.of(getFile(0)) : Collections.<Path> emptyList();
    }

    @Override
    public int getMaxInputFileIndex() {
        return !activeFiles.isEmpty() ? 0 : -1;
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