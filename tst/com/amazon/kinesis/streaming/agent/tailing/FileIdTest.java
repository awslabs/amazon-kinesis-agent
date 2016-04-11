/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.io.IOException;
import java.nio.file.Path;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.tailing.FileId;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;

public class FileIdTest extends TestBase {

    @Test
    public void testToString() throws IOException {
        Path tmp = testFiles.createTempFile();
        FileId id = FileId.get(tmp);
        Assert.assertEquals(id.toString(), id.getId());
    }

    @Test
    public void testEqualsAndHashCode() throws IOException {
        Path tmp = testFiles.createTempFile();
        FileId id1 = FileId.get(tmp);
        FileId id2 = FileId.get(tmp);
        Assert.assertEquals(id1, id2);
        Assert.assertEquals(id1.hashCode(), id2.hashCode());
    }
    @Test
    public void testFileIdDoesntChangeWhenLastModifiedTimeIsChanged() throws IOException {
        Path tmp = testFiles.createTempFile();
        FileId id1 = FileId.get(tmp);
        // now increase lastModifiedTime
        TestUtils.ageFile(tmp, -2);
        FileId id2 = FileId.get(tmp);
        Assert.assertEquals(id2, id1);
        // now decreate lastModifiedTime
        TestUtils.ageFile(tmp, 10);
        FileId id3 = FileId.get(tmp);
        Assert.assertEquals(id3, id1);
    }
}
