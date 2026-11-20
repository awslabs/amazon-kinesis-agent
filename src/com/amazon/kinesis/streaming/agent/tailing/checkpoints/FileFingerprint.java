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
package com.amazon.kinesis.streaming.agent.tailing.checkpoints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.amazon.kinesis.streaming.agent.tailing.FileId;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * A content fingerprint of the bytes a checkpoint covers, used to tell whether a file found by
 * {@link FileId} (device + inode) is really the checkpointed file or a new file that reused its inode.
 * <p>
 * Two murmur3_128 hashes are kept, both over bytes before the checkpoint offset (data already sent):
 * <ul>
 *   <li><b>head</b>: bytes {@code [0, headLength)}</li>
 *   <li><b>tail</b>: bytes {@code [offset - tailLength, offset)}, the window just before the offset. It sits past
 *       any shared header or common record prefix, so it still differs when two files start the same way.</li>
 * </ul>
 * Both lengths are {@code min(sizeBytes, offset)}, so any file with at least {@code offset} bytes can be checked.
 */
@ToString
@EqualsAndHashCode
public final class FileFingerprint {
    public static final int DEFAULT_SIZE_BYTES = 1024;
    public static final int MIN_SIZE_BYTES = 64;
    public static final int MAX_SIZE_BYTES = 64 * 1024;
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

    /** The checkpoint offset this fingerprint was taken for. */
    @Getter private final long offset;
    @Getter private final int headLength;
    @Getter private final String headHash;
    @Getter private final int tailLength;
    @Getter private final String tailHash;

    public FileFingerprint(long offset, int headLength, String headHash, int tailLength, String tailHash) {
        Preconditions.checkArgument(offset > 0, "Fingerprint offset (%s) must be positive", offset);
        Preconditions.checkArgument(headLength > 0 && headLength <= offset,
                "Fingerprint head length (%s) must be in (0, %s]", headLength, offset);
        Preconditions.checkArgument(tailLength > 0 && tailLength <= offset,
                "Fingerprint tail length (%s) must be in (0, %s]", tailLength, offset);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(headHash), "Fingerprint head hash is empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tailHash), "Fingerprint tail hash is empty");
        this.offset = offset;
        this.headLength = headLength;
        this.headHash = headHash;
        this.tailLength = tailLength;
        this.tailHash = tailHash;
    }

    /**
     * Fingerprints the bytes before {@code offset} in an open channel. The channel's position is not changed.
     *
     * @return the fingerprint, or {@code null} if {@code offset <= 0} or the channel has fewer than {@code offset} bytes
     */
    public static FileFingerprint compute(FileChannel channel, long offset, int sizeBytes) throws IOException {
        Preconditions.checkNotNull(channel);
        Preconditions.checkArgument(sizeBytes > 0, "Fingerprint size (%s) must be positive", sizeBytes);
        if (offset <= 0 || channel.size() < offset) {
            return null;
        }
        int length = (int) Math.min(sizeBytes, offset);
        String head = hash(channel, 0, length);
        String tail = hash(channel, offset - length, length);
        if (head == null || tail == null) {
            return null;
        }
        return new FileFingerprint(offset, length, head, length, tail);
    }

    /**
     * Fingerprints the file at {@code path}, but only if it is still the file identified by {@code expectedId}.
     * A path can point to a different file after rotation; fingerprinting that file would be wrong.
     *
     * @return the fingerprint, or {@code null} if the file is gone, has a different ID, is too short or cannot be read
     */
    public static FileFingerprint compute(Path path, FileId expectedId, long offset, int sizeBytes) {
        if (path == null || expectedId == null || offset <= 0) {
            return null;
        }
        try {
            if (!expectedId.equals(FileId.get(path))) {
                return null;
            }
            FileFingerprint fingerprint;
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                fingerprint = compute(channel, offset, sizeBytes);
            }
            // The path may have been rotated while it was read: keep the result only if it is still the same file.
            return expectedId.equals(FileId.get(path)) ? fingerprint : null;
        } catch (IOException | IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * @return {@code true} if the channel has at least {@link #getOffset()} bytes and both hashed ranges match
     */
    public boolean matches(FileChannel channel) throws IOException {
        Preconditions.checkNotNull(channel);
        if (channel.size() < offset) {
            return false;
        }
        return headHash.equals(hash(channel, 0, headLength))
                && tailHash.equals(hash(channel, offset - tailLength, tailLength));
    }

    /**
     * @return {@code true} if the file at {@code path} matches, {@code false} if its content genuinely differs
     * @throws IOException if the file cannot be read (missing, permissions, or a short read). The caller must not
     *         treat an unreadable file as a content mismatch: it propagates so the agent fails and restarting
     *         the agent attempts to restart from the previous checkpoint.
     */
    public boolean matches(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            return matches(channel);
        }
    }

    /** Hashes {@code length} bytes at {@code position} using positional reads; throws on a short read. */
    private static String hash(FileChannel channel, long position, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        long readPosition = position;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, readPosition);
            if (read < 0) {
                throw new IOException("Short read hashing " + length + " bytes at " + position
                        + "; file ended at " + readPosition);
            }
            readPosition += read;
        }
        return HASH_FUNCTION.hashBytes(buffer.array()).toString();
    }
}
