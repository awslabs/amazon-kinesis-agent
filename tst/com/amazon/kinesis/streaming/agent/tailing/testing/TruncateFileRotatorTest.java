/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link TruncateFileRotator}.
 */
public class TruncateFileRotatorTest extends FileRotatorTestBase {

    @Override
    protected FileRotator getFileRotator(Path root, String prefix) {
        return new TruncateFileRotator(root, prefix);
    }

    @Test(invocationCount=TEST_REPS, skipFailedInvocations=true)
    public void testTruncatedFileSizeDecreasesAfterRotation() throws IOException {
        FileRotator rotator = getFileRotator();
        rotator.rotate(2);
        long preRotationSize = Files.size(rotator.getLatestFile());
        rotator.rotate();
        Assert.assertTrue(Files.size(rotator.getLatestFile()) < preRotationSize,
                String.format("File size (%d) was expected to be smaller than previous file size (%d)",
                        Files.size(rotator.getLatestFile()), preRotationSize));
    }
}