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

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;

/**
 * Base interface for a checkpoing store to track progress of file tailers.
 */
public interface FileCheckpointStore {

    /**
     * Creates or updates the checkpoint for the given file.
     * @param file
     * @param offset
     * @return The checkpoint that was just created/updated.
     */
    public FileCheckpoint saveCheckpoint(TrackedFile file, long offset);

    /**
     *
     * @param flow
     * @param p
     * @return The checkpoint for the given path in the given flow, or
     *         {@code null} if the flow/path combination has no checkpoints in
     *         the store.
     */
    public FileCheckpoint getCheckpointForPath(FileFlow<?> flow, Path p);

    /**
     * @param flow
     * @return The latest checkpoint for the given flow if any, or {@code null}
     *         if the flow has no checkpoints in the store.
     */
    public FileCheckpoint getCheckpointForFlow(FileFlow<?> flow);

    /**
     * Cleans up any resources used up by this store.
     */
    public void close();

    public List<Map<String, Object>> dumpCheckpoints();
}
