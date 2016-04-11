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
 * Unit tests for {@link CopyFileRotator}.
 */
public class CopyFileRotatorTest extends FileRotatorTestBase {
    @Override
    protected FileRotator getFileRotator(Path root, String prefix) {
        return new CopyFileRotator(root, prefix);
    }

    @Test(invocationCount=TEST_REPS, skipFailedInvocations=true)
    public void testLatestFileSizeDoesNotDecreaseAfterRotation() throws IOException {
        FileRotator rotator = getFileRotator();
        rotator.rotate(2);
        long preRotationSize = Files.size(rotator.getLatestFile());
        rotator.rotate();
        Assert.assertTrue(Files.size(rotator.getLatestFile()) >= preRotationSize,
                String.format("File size (%d) was expected not to decrease after rotation (was %d)",
                        Files.size(rotator.getLatestFile()), preRotationSize));
    }


    @Override
    @Test(invocationCount=TEST_REPS, skipFailedInvocations=true)
    public void testGetMaxInputFileIndex() throws IOException {
        FileRotator rotator = getFileRotator();
        Assert.assertEquals(rotator.getMaxInputFileIndex(), -1);

        // First file
        rotator.rotate();
        Assert.assertEquals(rotator.getMaxInputFileIndex(), 0);

        // Now rotate another time and check that it wasn't changed
        rotator.rotate();
        Assert.assertEquals(rotator.getMaxInputFileIndex(), 0);

        // Now rotate another time and check that it wasn't changed
        rotator.rotate();
        Assert.assertEquals(rotator.getMaxInputFileIndex(), 0);
    }
}