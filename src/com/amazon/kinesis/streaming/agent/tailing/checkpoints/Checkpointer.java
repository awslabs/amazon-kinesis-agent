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

import org.slf4j.Logger;

import lombok.Getter;

import com.amazon.kinesis.streaming.agent.Logging;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.IRecord;
import com.amazon.kinesis.streaming.agent.tailing.RecordBuffer;
import com.google.common.base.Preconditions;

/**
 * Class that manages checkpoint updates per {@link FileFlow flow}.
 * Current implementation only makes sure that for a given {@link FileFlow flow}
 * an older checkpoint arriving late does not overwrite a newer checkpoint.
 *
 * This is not ideal because it could potentially create gaps and lead to data
 * loss, but in normal cases this should happen only rarely.
 *
 * TODO: A future iteration could ensure that checkpoints are created in
 *       sequence such that no gaps are left. This entails waiting for
 *       late-arriving buffers, and could lead to data duplication but
 *       guarantees no data loss.
 *
 * @param <R>
 */
public class Checkpointer<R extends IRecord> {
    private static final Logger LOGGER = Logging.getLogger(Checkpointer.class);

    @Getter private final FileCheckpointStore store;
    @Getter private final FileFlow<R> flow;
    private long committed = -1;

    public Checkpointer(FileFlow<R> flow, FileCheckpointStore store) {
        this.flow = flow;
        this.store = store;
    }

    /**
     * @param buffer The buffer that was sent and that triggered the checkpoint
     *               update.
     * @return The checkpoint that was created, or {@code null} if the buffer
     *         would overwrite a previous checkpoint.
     */
    public synchronized FileCheckpoint saveCheckpoint(RecordBuffer<?> buffer) {
        // SANITYCHECK: Can remove when done with debugging
        Preconditions.checkArgument(buffer.id() != committed);
        // Only store the checkpoint if it has an increasing sequence number
        if(buffer.id() > committed) {
            committed = buffer.id();
            return store.saveCheckpoint(buffer.checkpointFile(), buffer.checkpointOffset());
        } else {
            LOGGER.trace("Buffer {} has lower sequence number than the last committed value {}. " +
            		"No checkpoints will be updated.", buffer, committed);
            // TODO: Add metrics?
            return null;
        }
    }
}
