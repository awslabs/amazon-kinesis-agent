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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import com.google.common.base.Preconditions;

/**
 * Abstraction of a file identifier (independent from path). Current implementation
 * relies on {@link BasicFileAttributes#fileKey()}.
 */
@EqualsAndHashCode
public class FileId {
    @Getter private final String id;

    /**
     * @see #get(BasicFileAttributes)
     */
    public static FileId get(Path file) throws IOException {
        if (!Files.exists(file)) {
            return null;
        }
        Preconditions.checkArgument(Files.isRegularFile(file),
                "Can only get ID for real files (no directories and symlinks): "
                        + file);
        BasicFileAttributes attr = Files.readAttributes(file,
                BasicFileAttributes.class);
        return get(attr);
    }

    /**
     * TODO: this might not be portable as we rely on inner representation of
     *       the {@link BasicFileAttributes#fileKey()}
     *       ({@linkplain UnixFileKey http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/7u40-b43/sun/nio/fs/UnixFileKey.java}
     *       on Linux), which is not defined canonically anywhere.
     * @param attr
     * @return
     * @throws IOException
     */
    public static FileId get(BasicFileAttributes attr) throws IOException {
        return new FileId(attr.fileKey().toString());
    }

    public FileId(String id) {
        Preconditions.checkNotNull(id);
        this.id = id;
    }

    @Override
    public String toString() {
        return this.id;
    }
}
