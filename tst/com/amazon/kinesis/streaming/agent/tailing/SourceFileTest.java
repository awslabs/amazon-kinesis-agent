/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.tailing.SourceFile;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFileList;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class SourceFileTest extends TestBase {
    @DataProvider(name = "constructorFileNameParsing")
    public Object[][] testConstructorFileNameParsingData() {
        return new Object[][] { { "/var/log/message*", "/var/log", "message*" }, { "/log.*", "/", "log.*" }, };
    }

    @Test(dataProvider = "constructorFileNameParsing")
    public void testConstructorFileNameParsing(String fileName, String directory, String file) {
        SourceFile src = new SourceFile(null, fileName);
        Assert.assertEquals(src.getDirectory().toString(), directory);
        Assert.assertEquals(src.getFilePattern().toString(), file);
    }

    @Test
    public void testConfigurability() {
        SourceFile src = new SourceFile(null, "/tmp/testfile.log.*");
        Assert.assertEquals(src.getDirectory(), Paths.get("/tmp"));
        Assert.assertEquals(src.getFilePattern(), Paths.get("testfile.log.*"));
    }

    @Test(expectedExceptions=IllegalArgumentException.class, expectedExceptionsMessageRegExp="Directory component is empty.*")
    public void testConfigurationWithEmptyDirectory() {
        new SourceFile(null, "messages*");
    }

    @Test(expectedExceptions=IllegalArgumentException.class, expectedExceptionsMessageRegExp="File name component is empty.*")
    public void testConfigurationWithEmptyFileName() {
        new SourceFile(null, "/var/log/");
    }

    @DataProvider(name = "listAndCountFiles")
    public Object[][] testListAndCountFileData() {
        return new Object[][] {
                {
                    "app.log*",
                    new String[] { "app.log.1", "app.log.2", "app.log.3", "app.log.4", "app.log" },
                    new String[] { "another.log", "app.log.1.gz", "app.log.gz", "app.log.zip" }
                },
                {   // single file
                    "app.log*",
                    new String[] { "app.log" },
                    new String[] { "another.log", "app.log.1.gz", "app.log.bz2" }
                },
                {
                    "app[01][123456789].log",
                    new String[] { "app05.log", "app04.log", "app03.log", "app02.log", "app01.log" },
                    new String[] { "another.log", "app.log.1", "app.log.2", "app05.log.1.gz" }
                },
        };
    }

    public List<Path> toPathList(TrackedFileList files) {
        return Lists.transform(files, new Function<TrackedFile, Path>() {
            @Override
            public Path apply(@Nullable TrackedFile input) {
                return input.getPath();
            }
        });
    }

    @Test(dataProvider = "listAndCountFiles")
    public void testListFiles(String fileGlob, String[] matchingNames, String[] nonMatchingNames)
            throws InterruptedException, IOException {
        List<Path> matchingFiles = new ArrayList<>(matchingNames.length);
        for (String fileName : matchingNames) {
            matchingFiles.add(testFiles.createTempFileWithName(fileName));
        }
        TestUtils.ensureIncreasingLastModifiedTime(matchingFiles.toArray(new Path[matchingFiles.size()]));
        // now we have most recent file last... need to reverse
        matchingFiles = Lists.reverse(matchingFiles);
        // create the non-matching files
        for (String fileName : nonMatchingNames) {
            testFiles.createTempFileWithName(fileName);
        }
        SourceFile src = new SourceFile(null, testFiles.getTmpDir().toString() + "/" + fileGlob);
        TrackedFileList result = src.listFiles();

        // result should match order of lastModifiedTime set above
        Assert.assertEquals(toPathList(result).toArray(), matchingFiles.toArray());

        // sanity check the lastModified time
        if(result.size() > 1) {
            for(int i = 0; i < result.size()-1; ++i) {
                Assert.assertTrue(result.get(i).getLastModifiedTime() >= result.get(i+1).getLastModifiedTime());
            }
        }

        // change the time of the last file and see it climb to the top
        Path lastFile = matchingFiles.remove(matchingFiles.size() - 1);
        Files.setLastModifiedTime(lastFile, FileTime.fromMillis(System.currentTimeMillis()));
        matchingFiles.add(0, lastFile);
        result = src.listFiles();
        Assert.assertEquals(toPathList(result).toArray(), matchingFiles.toArray());
    }

    @Test
    public void testListFilesReturnsEmptyListIfDirectoryDoesNotExist() throws IOException {
        String nonExistingDirectory = "/somenonexistingdirectoryforsure" + System.currentTimeMillis();
        String fileGlob = "app.log*";
        SourceFile src = new SourceFile(null, nonExistingDirectory + "/" + fileGlob);
        Assert.assertEquals(src.listFiles().size(), 0);
    }

    @Test
    public void testListFilesReturnsEmptyListIfNoFilesMatch() throws IOException {
        String fileGlob = "app.log*";
        testFiles.createTempFile();
        testFiles.createTempFile();
        testFiles.createTempFile();
        SourceFile src = new SourceFile(null, testFiles.getTmpDir().toString() + "/" + fileGlob);
        Assert.assertEquals(src.listFiles().size(), 0);
    }

    @Test(dataProvider = "listAndCountFiles")
    public void testCountFiles(String fileGlob, String[] matchingNames, String[] nonMatchingNames)
            throws InterruptedException, IOException {
        List<Path> matchingFiles = new ArrayList<>(matchingNames.length);
        for (String fileName : matchingNames) {
            matchingFiles.add(testFiles.createTempFileWithName(fileName));
        }
        // create the non-matching files
        for (String fileName : nonMatchingNames) {
            testFiles.createTempFileWithName(fileName);
        }

        SourceFile src = new SourceFile(null, testFiles.getTmpDir().toString() + "/" + fileGlob);
        Assert.assertEquals(src.countFiles(), matchingFiles.size());
    }

    @Test
    public void testCountFilesReturnsZeroIfDirectoryDoesNotExist() throws IOException {
        String nonExistingDirectory = "/somenonexistingdirectoryforsure" + System.currentTimeMillis();
        String fileGlob = "app.log*";
        SourceFile src = new SourceFile(null, nonExistingDirectory + "/" + fileGlob);
        Assert.assertEquals(src.countFiles(), 0);
    }

    @Test
    public void testCountFilesReturnsZeroIfNoFilesMatch() throws IOException {
        String fileGlob = "app.log*";
        testFiles.createTempFile();
        testFiles.createTempFile();
        testFiles.createTempFile();
        SourceFile src = new SourceFile(null, testFiles.getTmpDir().toString() + "/" + fileGlob);
        Assert.assertEquals(src.countFiles(), 0);
    }
}
