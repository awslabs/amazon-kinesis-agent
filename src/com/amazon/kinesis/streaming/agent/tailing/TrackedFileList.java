/*
 * Copyright 2014-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.nio.file.Path;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;

/**
 * A snapshot of a a collection of files on the filesystem, ordered by last
 * modified time (newer-first) as would be retrieved by {@link SourceFile#listFiles().
 */
public class TrackedFileList extends AbstractList<TrackedFile> {

    public static TrackedFileList emptyList() {
        return new TrackedFileList(Collections.<TrackedFile> emptyList());
    }

    private final List<TrackedFile> snapshot;

    public TrackedFileList(List<TrackedFile> snapshot) {
        this.snapshot = new ArrayList<>(validate(snapshot));
    }

    @Override
    public TrackedFile get(int index) {
        return snapshot.get(index);
    }

    @Override
    public int size() {
        return snapshot.size();
    }

    @Override
    public TrackedFileList subList(int fromIndex, int toIndex) {
        return new TrackedFileList(snapshot.subList(fromIndex, toIndex));
    }

    public int indexOfPath(Path path) {
        int i = 0;
        for(TrackedFile f : snapshot) {
            if(path.equals(f.getPath()))
                return i;
            ++i;
        }
        return -1;
    }

    public int indexOfFileId(FileId id) {
        int i = 0;
        for(TrackedFile f : snapshot) {
            if(id.equals(f.getId()))
                return i;
            ++i;
        }
        return -1;
    }

    /**
     * Performs following validations:
     * <ol>
     *   <li>{@code files} is not {@code null}</li>
     *   <li>No two files have the same path</li>
     *   <li>No two files have the same {@code FileId}</li>
     *   <li>Files are ordered newer-first</li>
     * </ol>
     * @param files
     * @return The input parameter, if all is valid.
     * @throws NullPointerException if {@code files} is {@code null}.
     * @throws IllegalArgumentException if any of the other conditions are found.
     */
    private List<TrackedFile> validate(List<TrackedFile> files) {
        Preconditions.checkNotNull(files);
        // Unique Path
        Set<Path> seenPaths = new HashSet<>();
        for(TrackedFile f : files) {
            Path p = f.getPath().toAbsolutePath();
            Preconditions.checkArgument(!seenPaths.contains(p),
                    "File with path '" + p + "' shows up multiple times!");
            seenPaths.add(p);
        }
        // Unique FileId
        Set<FileId> seenIds = new HashSet<>();
        for(TrackedFile f : files) {
            Preconditions.checkArgument(!seenIds.contains(f.getId()),
                    "File with path '" + f.getPath() + "' has an ID " + f.getId() + " that shows up multiple times!");
            seenIds.add(f.getId());
        }
        // Order by lastModifiedTime descending
        long previousLastModifiedTime = -1;
        for(TrackedFile f : files) {
            if(previousLastModifiedTime >= 0) {
                Preconditions.checkArgument(f.getLastModifiedTime() <= previousLastModifiedTime,
                        "File with path '" + f.getPath() + "' is older than previous file in the list.");
            }
            previousLastModifiedTime = f.getLastModifiedTime();
        }

        // All good
        return files;
    }

}
