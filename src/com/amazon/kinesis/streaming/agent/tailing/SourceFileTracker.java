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
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import lombok.Getter;

import org.slf4j.Logger;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.Logging;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpoint;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Component responsible for tracking a collection source files specified by
 * a {@link FileFlow}. It maintains an internal snapshot of the current list
 * of files that belong to the flow (that match the {@code file} pattern)
 * as well as a pointer (and open channel) to the file currently tailed.
 * Every time the {@link #refresh()} method is called, the internal snapshot
 * is refreshed making sure the file currently tailed does not change to
 * maintain continuity, but keeping track of any rotations and new files that
 * might have showed up since the last snapshot.
 *
 * Once the current file is fully read by some consumer {@link #onEndOfCurrentFile()}
 * must be called so that the current file is advanced to the next pending
 * file if any.
 *
 * When {@link #refresh()} is called, it will do the following:
 *
 * <ul>
 *   <li>If there's a file currently tailed ({@code {@link #currentFile()} != null}):
 *     <ol>
 *       <li>List all exisisting files that belong to the flow, and order them by
 *           last modified date with most-recently modified file on top (see
 *           {@link SourceFile#listFiles()}).</li>
 *       <li>Compare file path, {@link FileId id}, size and last modified date
 *           to heuristically determine if a file rotation has happened (see
 *           {@link #updateCurrentFile(List)} for details of logic).</li>
 *       <li>If file rotation is detected, it modifies the {@link #currentFile()}
 *           to make sure that it remains pointing to the same file at the same
 *           offset.</li>
 *       <li>Updated its internal snapshot of the current files, including any
 *           new files that have showed up after the rotation.</li>
 *     </ol>
 *   </li>
 *   <li>If there isn't a file currently tailed ({@code {@link #currentFile()} == null}),
 *       or if the instance if being initialized:
 *       <ol>
 *         <li>If a checkpoint was found for the given flow, it reads it and uses it
 *             as the current file.</li>
 *         <li>Re-runs the heuristics described above and opens the appropriate
 *           file for tailing.</li>
 *       </ol>
 *   </li>
 * </ul>
 *
 * File rotations supported (see
 * {@linkplain http://linuxcommand.org/man_pages/logrotate8.html logrotate} man
 * page for some information on typical rotation configuration):
 *
 * <ul>
 *   <li>Rename: close current file + rename + create new file with same name.
 *       Example: file {@code access.log} is closed then renamed
 *       {@code access.log.1} or {@code access.log.2014-12-29-10-00-00},
 *       then a new file named {@code access.log} is created. This is the
 *       default behavior of <code>logrotate<utility></li>
 *   <li>Truncate: copy contents of current file to new file + truncate
 *       current file (keeping same name and inode). Example: contents of file
 *       {@code access.log} are copied into {@code access.log.1} or
 *       {@code access.log.2014-12-29-10-00-00}, then {@code access.log}
 *       is truncated (size reset to 0). This is similar to {@code logrotate}'s
 *       {@code copytruncate} configuration option.<br />
 *       Note: The edge case where a file is truncated and then populated
 *             by same (or larger) number of bytes as original file between
 *             calls to {@link #refresh()} is not handled by this implementation.
 *             If this happens, the rotation will not be detected by the current
 *             heuristic. This is thought to be a very rare edge case in typical
 *             systems. To handle it, we need to use a fingerprinting scheme
 *             and use a hash of the content of the file as a file identifier.
 *   </li>
 *   <li>Create: the file name is typically constructed with a timestamp
 *       providing time-based rotation. Example: to rotate a new file
 *       {@code access.log.2014-12-29-10-00-00}, a new file
 *       {@code access.log.2014-12-29-11-00-00} is created.</li>
 *   <li>Copy: copy contents of current file to new
 *       file but don't truncate the current file. This is similar to
 *       {@code logrotate}'s {@code copy} configuration option.
 *       The way to handle this rotation mode is to disable rotation
 *       tracking completely by specifying a specific file name (rather than
 *       a glob) to track. The tests of this class include cases that cover this
 *       mode.</li>
 * </ul>
 *
 * Notes:
 *
 * <ul>
 *   <li>This class does not modify the file checkpoints.</li>
 *   <li>Behavior is undefined if rotation occurs <strong>while</strong>
 *      {@link #refresh()} is running. TODO: investigate ability to lock latest
 *      file in {@code newSnapshot} as a way to prevent rotation.</li>
 *   <li>If any anomaly (something unexpected by rotation-detection heuristic)
 *       is detected, it assumes the worse and resets the tailing to the latest
 *       file (by modification time).</li>
 *   <li>The assumption made by this class is once a file is rotatoted, it
 *       can only be deleted, but cannot be modified, and will keep the ID (inode).
 *       The following entails:
 *       <ol>
 *         <li>If {@code newerFilesPending() == true} then {@code currentFile().size()}
 *             is the final size of the file. When the reader finishes reading
 *             the file up to its size, they can move to the next pending file.</li>
 *       </ol>
 *   </li>
 * </ul>
 */
@NotThreadSafe
public class SourceFileTracker {
    private static final Logger LOGGER = Logging.getLogger(SourceFileTracker.class);
    @VisibleForTesting
    final FileFlow<?> flow;
    private final SourceFile sourceFile;

    @Getter private TrackedFile currentOpenFile = null;
    @Getter private int currentOpenFileIndex = -1;
    @Getter protected TrackedFileList pendingFiles;
    protected TrackedFileList currentSnapshot;
    @Getter protected long lastRefreshTimestamp = 0;

    public SourceFileTracker(AgentContext agentContext, FileFlow<?> flow) throws IOException {
        this.flow = flow;
        this.sourceFile = flow.getSourceFile();
    }

    public void initialize() throws IOException {
        TrackedFileList newSnapshot = sourceFile.listFiles();
        initializeCurrentFile(newSnapshot);
    }

    /**
     * @param checkpoint
     * @return {@code true} if the checkpoint was successfully picked up,
     *         and {@code false} otherwise (e.g. checkpointed file was not
     *         found).
     * @throws IOException
     */
    public boolean initialize(FileCheckpoint checkpoint) throws IOException {
        Preconditions.checkNotNull(checkpoint);
        // Start with the file in the checkpoint as the current open file
        currentOpenFile = checkpoint.getFile();
        currentSnapshot = new TrackedFileList(Collections.singletonList(currentOpenFile));
        currentOpenFileIndex = 0;
        pendingFiles = TrackedFileList.emptyList();
        TrackedFileList newSnapshot = sourceFile.listFiles();
        if(updateCurrentFile(newSnapshot)) {
            // means that the current file was tracked... we just need to change the offset
            if (currentOpenFile.getSize() >= checkpoint.getOffset()) {
                LOGGER.debug("File from checkpoint was tracked successfully ({}). " +
                		"Setting offset to {}.",
                		currentOpenFile.getPath(), checkpoint.getOffset());
                closeCurrentFileIfOpen();
                currentOpenFile.open(checkpoint.getOffset());
                return true;
            } else {
                onTrackingAnomaly(
                        "Checkpoint offset (" + checkpoint.getOffset() + ") is beyound the end of the current file. " +
                        		"This suggests that we failed to track the correct file referenced in the checkpoint.",
                        newSnapshot, true);
                return false;
            }
        } else
            return false;
    }

    public long remainingBytes() throws IOException {
        long remaining = currentOpenFile == null ? 0 : currentOpenFile.getRemainingBytes();
        for(TrackedFile pending : pendingFiles)
            remaining += pending.getRemainingBytes();
        return remaining;
    }

    public boolean newerFilesPending() {
        return !pendingFiles.isEmpty();
    }

    /**
     * @return {@code true} if current open file was changed, otherwise
     *         {@code false}.
     * @throws IOException
     */
    public boolean onEndOfCurrentFile() throws IOException {
        if(currentOpenFile != null
                && currentOpenFile.getCurrentOffset() < currentOpenFile.getCurrentSize()) {
            LOGGER.debug("Current file offset ({}) is smaller than its size ({}), " +
                    "and consumer reported it has ended! ({})",
                    currentOpenFile.getCurrentOffset(),
                    currentOpenFile.getCurrentSize(),
                    currentOpenFile);
            return false;
        }
        if(newerFilesPending()) {
            LOGGER.trace("More files are pending so moving to next file. Current index: {}, Pending files: {}", currentOpenFileIndex, pendingFiles.size());
            startTailingNewFile(currentSnapshot, currentOpenFileIndex - 1);
            return true;
        } else {
            LOGGER.trace("No newer files are pending. Nothing to do. Current index: {}.", currentOpenFileIndex);
            return false;
        }
    }

    /**
     * This method performs a series of quick checks on the current open file
     * to see if we need to refresh. This is much faster than doing a full
     * refresh.
     * @return {@code true} if anything about the current open file has
     *         changed that warrants a full refresh of tracked files (e.g. file
     *         rotation, truncation, deletion, etc...); otherwise,
     *         {@code false}.
     * @throws IOException
     */
    public boolean mustRefresh() {
        try {
            // Some files appeared/disappeared
            if(currentSnapshot == null || sourceFile.countFiles() != currentSnapshot.size())
                return true;
            if(currentOpenFile != null) {
                if(!Files.exists(currentOpenFile.getPath())) {
                    LOGGER.debug("Current file {} does not exist anymore. Must refresh.", currentOpenFile.getPath());
                    return true;
                }
                BasicFileAttributes newAttr = Files.readAttributes(currentOpenFile.getPath(), BasicFileAttributes.class);
                // Check if the file ID changed
                FileId newId = FileId.get(newAttr);
                if(!newId.equals(currentOpenFile.getId())) {
                    LOGGER.debug("ID for current file ({}) changed from {} to {}. Must refresh.", currentOpenFile.getPath(), currentOpenFile.getId(), newId);
                    return true;
                }
                // Check if there are indications of truncation
                if(newAttr.size() < currentOpenFile.getSize()) {
                    LOGGER.debug("Current file ({}) shrunk in size from {} to {}. Must refresh.", currentOpenFile.getPath(), currentOpenFile.getSize(), newAttr.size());
                    return true;
                }
            }
        } catch (IOException e) {
            LOGGER.error("Error while status of current file snapshot. Must refresh.", e);
            return true;
        }
        return false;
    }

    /**
     * If this method returns {@code true} then any parsers reading the
     * curent open file can continue reading the same file at the same offset
     * (provided they update to the most recent {@link #getCurrentOpenFile()}).
     * If it returns {@code false}, this means we could not track the current
     * open file and parsing needs to be reset with the new file if one is
     * present.
     * @return {@code true} if current open file was tracked and any readers
     *         of that file can continue reading from the same offset; otherwise
     *         {@code false}.
     * @throws IOException
     */
    public boolean refresh() throws IOException {
        TrackedFileList newSnapshot = sourceFile.listFiles();
        boolean currentFileTracked = false;
        if(currentOpenFile == null) {
            initializeCurrentFile(newSnapshot);
            currentFileTracked = false;
        } else {
            currentFileTracked = updateCurrentFile(newSnapshot);
        }
        lastRefreshTimestamp = System.currentTimeMillis();
        return currentFileTracked;
    }

    private boolean initializeCurrentFile(TrackedFileList newSnapshot) throws IOException {
        Preconditions.checkState(currentOpenFile == null);
        if(newSnapshot.isEmpty()) {
            currentSnapshot = pendingFiles = TrackedFileList.emptyList();
            return false;
        } else {
            // TODO: get latest checkpoint for this flow
            startTailingNewFile(newSnapshot, 0);
            return true;
        }
    }

    /**
     * TODO: If a Truncate/Rename rotation happen while this method is running,
     *       behavior is undefined. Should we lock the current file? Would
     *       this prevent truncation and renaming?
     * @param newSnapshot
     * @throws IOException
     */
    @VisibleForTesting
    boolean updateCurrentFile(TrackedFileList newSnapshot) throws IOException {
        Preconditions.checkNotNull(currentSnapshot);
        Preconditions.checkNotNull(currentOpenFile);
        TrackedFileList oldSnapshot = currentSnapshot;
        TrackedFile oldCurrentOpenFile = currentOpenFile;

        // Initialize the analysis and log any anomalies
        TrackedFileRotationAnalyzer analyzer = new TrackedFileRotationAnalyzer(currentSnapshot, newSnapshot, currentOpenFile);
        if(!analyzer.getIncomingAnomalies().isEmpty()) {
            for(Map.Entry<TrackedFile, String> anomaly : analyzer.getIncomingAnomalies().entrySet()) {
                onUnexpectedChangeDetected(newSnapshot, anomaly.getKey(), anomaly.getValue());
            }
        }

        // Flag to tell if we managed to track the current open file
        boolean currentFileTracked = false;
        if(!analyzer.checkNoRotation()) {
            onPossibleRotationDetected(newSnapshot);
            if (!analyzer.allFilesHaveDisappeared()) {
                if(analyzer.hasCounterpart(currentOpenFile)) {
                    if(analyzer.currentOpenFileWasTruncated()) {
                        // If current file was truncated, we need to do something.
                        int newIndex = analyzer.findCurrentOpenFileAfterTruncate();
                        if(newIndex >= 0) {
                            currentFileTracked = onRotationByTruncate(newSnapshot, newIndex);
                        } else {
                            currentFileTracked = onTruncationWithoutRotation(newSnapshot);
                        }
                    } else {
                        // Otherwise, all is well. We resume tailing same file.
                        currentFileTracked = continueTailingCurrentFile(newSnapshot, analyzer.getCounterpartIndex(currentOpenFile));
                    }
                } else {
                    // Current open file disappeared somehow.
                    // None of our supported rotations methods could explain this.
                    currentFileTracked = onCurrentFileNotFound(newSnapshot);
                }
            } else {
                currentFileTracked = onAllFilesDisappeared(newSnapshot);
            }
        } else {
            currentFileTracked = onNoRotationDetected(newSnapshot, analyzer.getCounterpartIndex(currentOpenFile));
        }

        // SANITYCHECK: After we run, the current snapshot and current file must have changed.
        if(currentSnapshot == oldSnapshot) // identity intended
            throw new IllegalStateException("The current tracked file snapshot was not updated as expected.");
        if(currentOpenFile == oldCurrentOpenFile)  // identity intended
            throw new IllegalStateException("The current open file was not updated as expected.");

        return currentFileTracked;
    }

    private void closeCurrentFileIfOpen() {
        if(currentOpenFile != null && currentOpenFile.isOpen())
            closeCurrentFile();
    }

    private void closeCurrentFile() {
        currentOpenFile.close();
    }

    protected void resumeTailingRotatedFileWithNewId(TrackedFileList newSnapshot, int index) throws IOException {
        long offset = currentOpenFile.getCurrentOffset();  // note: FileChannel.position() does not change if underlying file was truncated
        closeCurrentFileIfOpen();
        currentOpenFile = newSnapshot.get(index);
        currentOpenFile.open(offset);
        currentOpenFileIndex = index;
        LOGGER.trace("Resumed tailing file (index {}): {}", currentOpenFileIndex, currentOpenFile);
        updateSnapshot(newSnapshot, index);
    }

    protected void startTailingNewFile(TrackedFileList newSnapshot, int index) throws IOException {
        closeCurrentFileIfOpen();
        currentOpenFile = newSnapshot.get(index);
        currentOpenFile.open(0);
        currentOpenFileIndex = index;
        LOGGER.trace("Started tailing new file (index {}): {}", currentOpenFileIndex, currentOpenFile);
        updateSnapshot(newSnapshot, index);
    }

    protected boolean continueTailingCurrentFile(TrackedFileList newSnapshot, int currentFileIndex) throws IOException {
        TrackedFile newCurrentFile = newSnapshot.get(currentFileIndex);
        if(currentOpenFile.isOpen())
            newCurrentFile.inheritChannel(currentOpenFile);
        currentOpenFile = newCurrentFile;
        currentOpenFileIndex = currentFileIndex;
        LOGGER.trace("Continuing to tail current file (index {}): {}", currentFileIndex, currentOpenFile);
        updateSnapshot(newSnapshot, currentFileIndex);
        return true;
    }

    protected void stopTailing(TrackedFileList newSnapshot) throws IOException {
        closeCurrentFileIfOpen();
        currentOpenFile = null;
        currentOpenFileIndex = -1;
        currentSnapshot = newSnapshot;
        pendingFiles = TrackedFileList.emptyList();
    }

    protected void resetTailing(TrackedFileList newSnapshot) throws IOException {
        if (!newSnapshot.isEmpty()) {
            LOGGER.trace("Retstarting anew tailing the latest file.");
            startTailingNewFile(newSnapshot, 0);
        } else {
            LOGGER.trace("No available files to tail. Stopping.");
            stopTailing(newSnapshot);
        }
    }

    protected void updateSnapshot(TrackedFileList newSnapshot, int currentFileIndex) throws IOException {
        if(LOGGER.isTraceEnabled() && newSnapshot != currentSnapshot) { // identity intended
            LOGGER.trace("Updating file snapshot:\n\tCurrent File Index: {}\n\tExisting snapshot:\n\t\t{}\n\tNew snapshot:\n\t\t{}",
                    currentFileIndex,
                    currentSnapshot == null || currentSnapshot.isEmpty() ? "<none>" : Joiner.on("\n\t\t").join(currentSnapshot),
                    newSnapshot == null || newSnapshot.isEmpty() ? "<none>" : Joiner.on("\n\t\t").join(newSnapshot));
        }
        TrackedFileList newPendingFiles = newSnapshot.subList(0, currentFileIndex);
        if(LOGGER.isTraceEnabled()) {
            LOGGER.trace("Updating pendig files:\n\tCurrent File Index: {}\n\tExisting pending:\n\t\t{}\n\tNew pending:\n\t\t{}",
                    currentFileIndex,
                    pendingFiles == null || pendingFiles.isEmpty() ? "<none>" : Joiner.on("\n\t\t").join(pendingFiles),
                    newPendingFiles == null || newPendingFiles.isEmpty() ? "<none>" : Joiner.on("\n\t\t").join(newPendingFiles));
        }

        // finally, update the files
        this.currentSnapshot = newSnapshot;
        this.pendingFiles = newPendingFiles;
    }

    protected boolean onTrackingAnomaly(String message, TrackedFileList newSnapshot, boolean resetTailingRequested) throws IOException {
        message = message.replace("{}", "\\{}");
        LOGGER.error("Error while tracking file rotation for {}: " +
                message +
                "\n\tCurrent file: {}" +
                "\n\tCurrent file snapshot (newest first):\n\t\t{}" +
                "\n\tNew file snapshot (newest first):\n\t\t{}",
                flow.getSourceFile(), currentOpenFile,
                currentSnapshot == null || currentSnapshot.isEmpty() ? "<none>" : Joiner.on("\n\t\t").join(currentSnapshot),
                newSnapshot == null || newSnapshot.isEmpty() ? "<none>" : Joiner.on("\n\t\t").join(newSnapshot));
        if(resetTailingRequested) {
            resetTailing(newSnapshot);
            return false;
        }
        return true;
    }

    protected void onUnexpectedChangeDetected(TrackedFileList newSnapshot, TrackedFile file, String message) throws IOException {
        onTrackingAnomaly(message + ": " + file, newSnapshot, false);
    }

    protected boolean onAllFilesDisappeared(TrackedFileList newSnapshot) throws IOException {
        return onTrackingAnomaly("All files have gone away!", newSnapshot, true);
    }

    protected boolean onTruncationWithoutRotation(TrackedFileList newSnapshot) throws IOException {
        return onTrackingAnomaly("Current file was apparently truncated but can't find file with older data to track.", newSnapshot, true);
    }

    private boolean onNoRotationDetected(TrackedFileList newSnapshot, int newIndex) throws IOException {
        LOGGER.trace("No changes in tracked files were detected. Will continue to tail the current file.");
        return continueTailingCurrentFile(newSnapshot, newIndex);
    }

    private void onPossibleRotationDetected(TrackedFileList newSnapshot) {
        LOGGER.trace("Possible rotation detected in tracked files. Looking for best current file to continue tracking.");
    }

    protected boolean onRotationByTruncate(TrackedFileList newSnapshot, int newIndex) throws IOException {
        TrackedFile resumed = newSnapshot.get(newIndex);
        LOGGER.debug("Current file ({}) was apparently rotated by Truncate; " +
                "resuming with old file ({}).",
                currentOpenFile.toShortString(), resumed.toShortString());
        resumeTailingRotatedFileWithNewId(newSnapshot, newIndex);
        return true;
    }

    protected boolean onCurrentFileNotFound(TrackedFileList newSnapshot) throws IOException {
        String msg = "Current file was not found";
        if(newSnapshot.indexOfPath(currentOpenFile.getPath()) != -1)
            msg += " (path exists, but file ID is different)";
        msg += ".";
        return onTrackingAnomaly(msg, newSnapshot, true);
    }
}