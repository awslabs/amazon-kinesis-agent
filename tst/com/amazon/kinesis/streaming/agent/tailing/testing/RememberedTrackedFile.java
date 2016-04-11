/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import lombok.Getter;
import lombok.ToString;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.google.common.base.Throwables;

/**
 * A class that encapsulates a snapshot of the state of a file at a
 * point in time.
 */
@ToString(callSuper = true)
public class RememberedTrackedFile extends TrackedFile {
    static final long MAX_CONTENT_HASH_BYTES = 2 * 1024 * 1024;
    private final long offset;
    @Getter private final String md5;
    @Getter private final int index;
    @Getter private int md5Size;

    public RememberedTrackedFile(TrackedFile original) throws IOException {
        this(original, 0);
    }

    public RememberedTrackedFile(TrackedFile original, int index) throws IOException {
        super(original);
        this.channel = original.getChannel();
        this.offset = original.getCurrentOffset();
        this.md5Size = (int) Math.min(size, RememberedTrackedFile.MAX_CONTENT_HASH_BYTES);
        this.md5 = TestUtils.getContentMD5(path, md5Size);
        this.index = index;
    }

    /**
     * @return The offset frozen in time (when this file was remembered).
     */
    @Override
    public long getCurrentOffset() {
        return offset;
    }

    public boolean hasSameContentHash(Path other) {
        try {
            if (Files.size(other) < size)
                return false;
            String otherMD5 = TestUtils.getContentMD5(other, md5Size);   // use remembered size
            return (getMd5() == null && otherMD5 == null)
                    || (getMd5() != null && otherMD5 != null && getMd5().equals(otherMD5));

        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public void inheritChannel(TrackedFile oldOpenFile) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public void open(long offset) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }
}
