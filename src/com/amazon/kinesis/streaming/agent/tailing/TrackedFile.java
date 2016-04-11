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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;

import javax.annotation.concurrent.NotThreadSafe;

import lombok.Getter;
import lombok.ToString;

import org.slf4j.Logger;

import com.amazon.kinesis.streaming.agent.Logging;
import com.google.common.base.Preconditions;

/**
 * A class that encapsulates a snapshot of the state of a file at a point in
 * time, along with an optional channel that can be used to read from the file.
 */
@NotThreadSafe
@ToString(exclude={"channel", "flow"})
public class TrackedFile {
    private static final Logger LOGGER = Logging.getLogger(TrackedFile.class);
    @Getter protected final FileFlow<?> flow;
    @Getter protected final FileId id;
    @Getter protected final Path path;

    @Getter protected final long lastModifiedTime;
    @Getter protected final long size;
    @Getter protected FileChannel channel;

    public TrackedFile(FileFlow<?> flow, Path path, FileId id, long lastModifiedTime, long size) {
        this.flow = flow;
        this.path = path;
        this.id = id;
        this.channel = null;
        this.lastModifiedTime = lastModifiedTime;
        this.size = size;
    }

    public TrackedFile(FileFlow<?> flow, Path path) throws IOException {
        this(flow, path, FileId.get(path), Files.getLastModifiedTime(path).toMillis(), Files.size(path));
    }

    protected TrackedFile(TrackedFile original) {
        this(original.flow, original.path, original.id, original.lastModifiedTime, original.size);
    }

    public void inheritChannel(TrackedFile oldOpenFile) throws IOException {
        Preconditions.checkState(oldOpenFile.channel != null, "Existing file does not have an open channel.");
        Preconditions.checkState(channel == null, "This file has an open channel already.");
        Preconditions.checkState(oldOpenFile.id.equals(id), "File ID differ (old: %s, new: %s)", id.toString(), oldOpenFile.id.toString());
        channel = oldOpenFile.channel;
    }

    public long getCurrentOffset() throws IOException {
        return channel == null ? 0 : channel.position();
    }

    public long getCurrentSize() throws IOException {
        return channel == null ? (Files.exists(path) ? Files.size(path) : 0) : channel.size();
    }

    public long getRemainingBytes() throws IOException {
         return getSize() - getCurrentOffset();
    }

    public void open(long offset) throws IOException {
        Preconditions.checkState(channel == null, "File already open.");
        Preconditions.checkArgument(offset >= 0, "Offset must be a non-negative number.");
        channel = FileChannel.open(path, StandardOpenOption.READ);
        if(offset > 0) {
            channel.position(offset);
        }
    }

    /**
     * Closes the channel that's open to this file.
     */
    public void close() {
        Preconditions.checkState(channel != null, "File not open.");
        try {
            if(channel.isOpen())
                channel.close();
        } catch (IOException e) {
            LOGGER.error("Failed to closed file channel for {}", path, e);
        } finally {
            channel = null;
        }
    }

    /**
     * @return {@code true} if there's an open channel to this file,
     *         {@code false} otherwise.
     */
    public boolean isOpen() {
        return channel != null && channel.isOpen();
    }

    /**
     * @param f
     * @return {@code true} if the two files have the same ID,
     *         and {@code false} otherwise.
     */
    public boolean isSameAs(TrackedFile f) {
        return id.equals(f.id);
    }

    /**
     * @param other The file to compare to.
     * @return @see #isNewer(long, long, long, long)
     */
    public boolean isNewer(TrackedFile other) {
        return isNewer(this, other);
    }

    /**
     * @param tsOther The timestamp of the file to compare to.
     * @param sizeOther The size of the file to compare to.
     * @return @see #isNewer(long, long, long, long)
     */
    public boolean isNewer(long tsOther, long sizeOther) {
        return isNewer(this, tsOther, sizeOther);
    }

    /**
     * @return A shorter representation than {@link #toString()}
     */
    public String toShortString() {
        return path + ":" + id + ":" + size + ":" + lastModifiedTime;
    }



    /**
     * @see #isNewer(long, long, long, long)
     * @param f1 The first file to compare.
     * @param f2 The second file to compare.
     * @return @see #isNewer(long, long, long, long)
     */
    public static boolean isNewer(TrackedFile f1, TrackedFile f2) {
        return isNewer(f1.lastModifiedTime, f1.size, f2.lastModifiedTime, f2.size);
    }

    /**
     * @see #isNewer(long, long, long, long)
     * @param f1 The first file to compare.
     * @param ts2 The timestamp of the second file.
     * @param size2 The size of the second file.
     * @return @see #isNewer(long, long, long, long)
     */
    public static boolean isNewer(TrackedFile f1, long ts2, long size2) {
        return isNewer(f1.lastModifiedTime, f1.size, ts2, size2);
    }

    /**
     * @see #isNewer(long, long, long, long)
     * @param p1 The first file to compare.
     * @param p2 The second file to compare.
     * @return @see #isNewer(long, long, long, long)
     * @throws IOException
     */
    public static boolean isNewer(Path p1, Path p2) throws IOException {
        return isNewer(p1, Files.getLastModifiedTime(p2).toMillis(), Files.size(p2));
    }

    /**
     * @see #isNewer(long, long, long, long)
     * @param p1 The path of the file to compare.
     * @param ts2 The timestamp of the second file.
     * @param size2 The size of the second file.
     * @return @see #isNewer(long, long, long, long)
     * @throws IOException
     */
    public static boolean isNewer(Path p1, long ts2, long size2) throws IOException {
        return isNewer(Files.getLastModifiedTime(p1).toMillis(), Files.size(p1),
                ts2, size2);
    }

    /**
     * Determines if a file is newer than another by comparing the timestamps
     * and sizes. This heuristic is consisten with the behavior of rotating log
     * files: at the instant the file is rotated there will likely exist two files
     * with the same timestamp; in that case, knowing nothing else about the files,
     * the old file is almost certainly the one which has more data, since the newer
     * file has just been created (or truncated) and would not have enough data yet.
     * TODO: The edge case where the new file fills so quickly (within 1 second)
     *       to become as large as the old file will escape this heuristic. This
     *       is *exceedingly rare* to happen in reality however, and will soon
     *       rectify itself (within 1 second) since the timestamp of the file
     *       that's being written to will change, and the sizes won't matter
     *       anymore.
     * @param ts1 Timestamp of first file.
     * @param size1 Size of first file.
     * @param ts2 Timestamp of second file.
     * @param size2 Size of second file.
     * @return {@code true} if the first file is newer than the second file
     *         ({@code ts1 &gt; ts2}); if the two files have the same timestamp,
     *         then {@code true} is returned if the first file's size is
     *         smaller than the second's; otherwise, {@code false} is returned.
     */
    public static boolean isNewer(long ts1, long size1, long ts2, long size2) {
        return ts1 > ts2 || (ts1 == ts2 && size1 < size2);
    }

    /**
     * A comparison operator that uses the heuristic defined in
     * {@link TrackedFile#isNewer(long, long, long, long)} to comapre two {@link TrackedFile}
     * instances and order them in descending order of recency (newest first).
     */
    public static class NewestFirstComparator implements Comparator<TrackedFile> {
        @Override
        public int compare(TrackedFile f1, TrackedFile f2) {
            return f1.isSameAs(f2) ? 0 : (isNewer(f1, f2) ? -1 : 1);
        }
    }
}
