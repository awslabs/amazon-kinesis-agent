/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.annotation.concurrent.NotThreadSafe;

import lombok.Getter;
import lombok.ToString;

/**
 * A rotator that simply creates a new file with a new name every time.
 */
@NotThreadSafe
@ToString(callSuper=true)
public class CreateFileRotator extends FileRotator {
    /** The index to be appended to next new file name. */
    @Getter private int nextFileIndex = 1;

//    public CreateFileRotator(Path dir, String prefix, int maxFilesToKeep) {
//        super(dir, prefix, maxFilesToKeep);
//    }

    public CreateFileRotator(Path dir, String prefix) {
        super(dir, prefix);
    }

    @Override
    protected Path rotateOrCreateNewFile() throws IOException {
        Path newPath = getNewFilePath();
        nextFileIndex += 1;
        logger.debug("Creating new file {}", newPath);
        Files.createFile(newPath);
        return newPath;
    }

    @Override
    Path getNewFilePath() {
        return dir.resolve(prefix + "." + nextFileIndex);
    }

    @Override
    Path getFilePathAfterRotation(int index) {
        return getFile(index);
    }

    @Override
    boolean latestFileKeepsSamePath() {
        return false;
    }

    @Override
    boolean latestFileKeepsSameId() {
        return false;
    }
}
