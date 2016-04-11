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
package com.amazon.kinesis.streaming.agent.tailing.checkpoints;

import com.amazon.kinesis.streaming.agent.tailing.FileId;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.google.common.base.Preconditions;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * A checkpoint in a file.
 * When doing equality comparison and computing hashcode, only the file ID and
 * flow ID are considered, making this class work with files that change in size
 * (and potentially path).
 */
@ToString
@EqualsAndHashCode(exclude={"file"})
public class FileCheckpoint {
    @Getter private final TrackedFile file;
    @Getter private final long offset;
    @Getter private final FileId fileId;
    @Getter private final String flowId;

    public FileCheckpoint(TrackedFile file, long offset) {
        Preconditions.checkNotNull(file);
        Preconditions.checkArgument(offset >= 0, "The offset (%s) must be a non-negative integer", offset);
        this.file = file;
        this.offset = offset;
        fileId = file.getId();
        flowId = file.getFlow().getId();
    }
}
