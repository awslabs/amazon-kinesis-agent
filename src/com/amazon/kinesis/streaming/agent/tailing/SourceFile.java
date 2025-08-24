/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file.
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Specification of the file(s) to be tailed.
 */
@EqualsAndHashCode(exclude = {"pathMatcher"})
public class SourceFile {
    @Getter
    private final FileFlow<?> flow;
    @Getter
    private final Path directory;
    @Getter
    private final Path filePattern;
    private final PathMatcher pathMatcher;

    public SourceFile(FileFlow<?> flow, String filePattern) {
        this.flow = flow;

        Preconditions.checkArgument(!filePattern.endsWith("/"), "File name component is empty!");
        Path filePath = FileSystems.getDefault().getPath(filePattern);

        this.directory = findNotGlobParent(filePath);

        validateDirectory(this.directory);

        // Absolute pattern
        this.filePattern = this.directory.relativize(filePath);

        this.pathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + this.directory.resolve(this.filePattern).toString());
    }

    private boolean isGlobName(String pathPart) {
        return pathPart.matches(".*(?<!\\\\)\\*.*")
                || pathPart.matches(".*(?<!\\\\)\\?.*")
                || pathPart.matches(".*(?<!\\\\)\\[.*");
    }

    private boolean isGlobName(Path pathPart, int index) {
        return isGlobName(pathPart.getName(index).toString());
    }

    private Path findNotGlobParent(Path filePath) {
        int nameCount = filePath.getNameCount();

        if (nameCount <= 1 || isGlobName(filePath, 0))
            return filePath.getRoot();

        for (int i = 0; i < nameCount; ++i) { // walk through directories
            if (isGlobName(filePath, i)) {
                return filePath.getRoot().resolve(filePath.subpath(0, i));
            }
        }
        return filePath.getParent();
    }

    /**
     * @return List of {@link Path} objects contained in the given directory and subdirectories
     * and that match the file path pattern, sorted by {@code lastModifiedTime}
     * descending (newest at the top). An empty list is returned if
     * {@link #directory} does not exist, or if there are no files
     * that match the pattern.
     * @throws IOException If there was an error reading the directory or getting
     *                     the {@code lastModifiedTime} of a directory. Note that if
     *                     the {@link #directory} doesn't exist no exception will be thrown
     *                     but an empty list is returned instead.
     */
    public TrackedFileList listFiles() throws IOException {
        return listFilesImpl(this.directory);
    }

    /**
     * @return List of {@link Path} objects contained in {@code directory} and subdirectories
     * and that match the file path pattern, sorted by {@code lastModifiedTime}
     * descending (newest at the top). An empty list is returned if
     * {@link #directory} does not exist, or if there are no files
     * that match the pattern.
     * @throws IOException If there was an error reading the directory or getting
     *                     the {@code lastModifiedTime} of a directory. Note that if
     *                     the {@link #directory} doesn't exist no exception will be thrown
     *                     but an empty list is returned instead.
     */
    private TrackedFileList listFilesImpl(Path directory) throws IOException {
        if (!Files.exists(directory))
            return TrackedFileList.emptyList();

        List<TrackedFile> files = new ArrayList<>();

        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory)) {
            for (Path path : directoryStream) {
                if (Files.isDirectory(path)) {
                    files.addAll(listFilesImpl(path));
                } else if (this.pathMatcher.matches(path) && validateFile(path)) {
                    files.add(new TrackedFile(flow, path));
                }
            }
        }
        // sort the files by descending last modified time and return
        Collections.sort(files, new TrackedFile.NewestFirstComparator());
        return new TrackedFileList(files);
    }

    /**
     * @return The number of files on the file system that match the given input
     * pattern. More lightweight than {@link #listFiles()}.
     * @throws IOException If there was an error reading the directory or getting
     *                     the {@code lastModifiedTime} of a directory. Note that if
     *                     the {@link #directory} doesn't exist no exception will be thrown
     *                     but {@code 0} is returned instead.
     */
    public int countFiles() throws IOException {
        return countFilesImpl(this.directory);
    }

    /**
     * The number of files in directory and subdirectories on the file system that match the given input
     * pattern.
     *
     * @param directory Directory
     * @return The number of files in {@code directory} and subdirectories on the file system that match the given input
     * pattern.
     * @throws IOException If there was an error reading the directory or getting
     *                     the {@code lastModifiedTime} of a directory. Note that if
     *                     the {@code directory} doesn't exist no exception will be thrown
     *                     but {@code 0} is returned instead.
     */
    private int countFilesImpl(Path directory) throws IOException {
        int count = 0;
        if (Files.exists(directory)) {
            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory)) {
                for (Path path : directoryStream) {
                    if (Files.isDirectory(path)) {
                        count += countFilesImpl(path);
                    } else if (this.pathMatcher.matches(path) && validateFile(path)) {
                        ++count;
                    }
                }
            }
        }
        return count;
    }

    @Override
    public String toString() {
        return this.directory.resolve(this.filePattern).toString();
    }

    /**
     * Performs basic validation on the directory parameter making sure it fits
     * within the supported functionality of this class.
     * @param dir
     */
    private void validateDirectory(Path dir) {
        Preconditions.checkArgument(dir != null, "Directory component is empty!");
    }

    /**
     * Make sure to ignore invalid file for streaming
     * e.g. well known compressed file extensions
     * @param file
     */
    private boolean validateFile(Path file) {
        List<String> ignoredExtensions = ImmutableList.of(".gz", ".bz2", ".zip");

        for (String extension : ignoredExtensions) {
            if (file.toString().toLowerCase().endsWith(extension))
                return false;
        }

        return true;
    }
}
