/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.annotation.concurrent.NotThreadSafe;

import lombok.ToString;

/**
 * Generates files and rotates them by renaming the existing files.
 */
@NotThreadSafe
@ToString(callSuper=true)
public class RenameFileRotator extends FileRotator {

    public RenameFileRotator(Path dir, String prefix) {
        super(dir, prefix);
    }

    @Override
    protected Path rotateOrCreateNewFile() throws IOException {
        // if there's an existing file, rotate it
        if(!activeFiles.isEmpty() && Files.exists(getFile(0)))
            rotateFileName(0);
        Path newPath = getNewFilePath();
        logger.debug("Creating new file {}", newPath);
        Files.createFile(newPath);
        return newPath;
    }

    @Override
    boolean latestFileKeepsSamePath() {
        return true;
    }

    @Override
    boolean latestFileKeepsSameId() {
        return false;
    }
}