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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import lombok.Getter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * A class encapsulating various heuristics and methods to assist in tracking
 * tailed files, to be used by {@link SourceFileTracker}. A few definitions:
 * <ul>
 *   <li>current snapshot: the last snapshot of files processed by
 *       {@link SourceFileTracker#refresh()}.</li>
 *   <li>current file: a file in the current snapshot</li>
 *   <li>incoming snapshot: the snapshot of files currently on the file system</li>
 *   <li>incoming file: a file in the incoming snapshot</li>
 *   <li>file counterpart: the incoming (current) file with the same
 *       {@code FileId} as a current (incoming) file (respectively).</li>
 *   <li>matched file (M): a file whose counterpart was found.</li>
 *   <li>un-matched file (X): a file (current or incoming) whose counterpart was
 *       not found.</li>
 * </ul>
 */
@NotThreadSafe
public class TrackedFileRotationAnalyzer {
    @Getter private final TrackedFileList current;
    @Getter private final TrackedFile currentOpenFile;
    @Getter private final TrackedFileList incoming;
    private final Map<TrackedFile, TrackedFile> counterparts = new HashMap<>();
    private final Map<TrackedFile, Integer> counterpartIndices = new HashMap<>();
    private Map<TrackedFile, String> incomingAnomalies;

    public TrackedFileRotationAnalyzer(TrackedFileList current, TrackedFileList incoming, TrackedFile currentOpenFile) {
        Preconditions.checkNotNull(current);
        Preconditions.checkNotNull(incoming);
        Preconditions.checkArgument(currentOpenFile == null || current.contains(currentOpenFile), "Current file is not contained in current snapshot!");
        Preconditions.checkArgument(incoming != current, "Current and incoming are the same list!"); // identitiy intended
        this.current = current;
        this.currentOpenFile = currentOpenFile;
        this.incoming = incoming;
        syncCounterpartsByFileId();
    }

    /**
     * No rotatoin is assumed to have happended if the following are true:
     * <ol>
     *   <li>All files in incoming snapshot have counterparts</li>
     *   <li>All counterparts of incoming snapshot have same path and FileId</li>
     *   <li>Only tail files in current snapshot are allowed not to have counterparts: older files deleted by rotation</li>
     *   <li>currentOpenFile has an incoming counterpart</li>
     *   <li>currentOpenFile: Incoming lastModifiedTime &gt;= current lastModifiedFile</li>
     *   <li>currentOpenFile: Incoming size &gt;= current size (currentOpenFile)</li>
     *   <li>All incoming files: Incoming lastModifiedTime == current counterpart lastModifiedFile</li>
     *   <li>All incoming files: Incoming size == current counterpart size</li>
     *   <li>No anomalies reported when synching current/incoming snapshots (implied by all above)</li>
     * </ol>
     * @return {@code true} if no rotation appears to be have happeneded,
     *          otherwise {@code false}.
     */
    public boolean checkNoRotation() {
        for(TrackedFile incomingFile : incoming) {
            // All files in incoming snapshot have counterparts
            if(!hasCounterpart(incomingFile))
                return false;
            TrackedFile currentCounterpart = getCounterpart(incomingFile);
            // All counterparts of incoming files have same path and FileId
            if(!incomingFile.isSameAs(currentCounterpart) ||
                    !incomingFile.getPath().equals(currentCounterpart.getPath()))
                return false;
            if(currentCounterpart == currentOpenFile) { // identity intended
                // currentOpenFile: Incoming lastModifiedTime >= current lastModifiedFile
                // currentOpenFile: Incoming size >= current size (currentOpenFile)
                if(incomingFile.getSize() < currentCounterpart.getSize() ||
                        incomingFile.getLastModifiedTime() < currentCounterpart.getLastModifiedTime())
                    return false;
            } else {
                // All incoming files: Incoming lastModifiedTime == current counterpart lastModifiedFile
                // All incoming files: Incoming size == current counterpart size
                if(incomingFile.getSize() != currentCounterpart.getSize() ||
                        incomingFile.getLastModifiedTime() != currentCounterpart.getLastModifiedTime())
                    return false;
            }
        }

        // Only tail files in current snapshot are allowed not to have counterparts: older files deleted by rotation
        if(incoming.size() > current.size())
            return false;
        else if(incoming.size() < current.size()) {
            // verify that only tail files are missing from current
            int firstMissingFileIndex = incoming.size();
            // all files before this index have counterparts
            for(int i = 0; i < firstMissingFileIndex; ++i)
                if(!hasCounterpart(current.get(i)))
                    return false;
            // all files at or after this index don't
            for(int i = firstMissingFileIndex; i < current.size(); ++i)
                if(hasCounterpart(current.get(i)))
                    return false;
        }

        // currentOpenFile has an incoming counterpart
        if(currentOpenFile != null && !hasCounterpart(currentOpenFile))
                return false;

        // No anomalies reported when synching current/incoming snapshots (implied by all above)
        if(!getIncomingAnomalies().isEmpty())
            return false;

        // everythoing ok? hypothesis must be true
        return true;
    }

    /**
     * After rotation by Truncate (assuming no anomalies), we might
     * end up with any of following in the incoming snapshot (X=un-matched,
     * M=matched):
     * <ol>
     *   <li>Single rotation: M1,X1</li>
     *   <li>Multiple rotations: M1,X1,...,Xn</li>
     *   <li>Single rotation with prior rotation(s): M1,X1,M2,...Mn</li>
     *   <li>Multiple rotations with prior rotation(s): M1,X1,...,Xn,M2,M3,...Mq</li>
     * </ol>
     * In cases 1 and 2 (no prior rotations), we assume that the current file is
     * now the oldest unmatched file.
     *
     * In cases 3 and 4 (with one or more prior rotations):
     * <ul>
     *   <li>If current file is matched to any of the prior rotated files (Mi),
     *       then the current remains the same</li>
     *   <li>Otherwise, the oldest un-matched file is assumed to be the current
     *       file</li>
     * </ul>
     * TODO: For the cases where the oldest un-matched (Xn) is assumed to be the
     *       current file, a comparision of content hash can be useful to confirm
     *       this.
     * @return The best guess of the index of the current file in the incoming
     *         list, or {@code -1} if no such guess was possible.
     */
    public int findCurrentOpenFileAfterTruncate() {
        Preconditions.checkNotNull(currentOpenFile);
        Preconditions.checkState(incoming.size() > 1); // always true after rotation by Truncate
        Preconditions.checkState(hasCounterpart(incoming.get(0))); // always true for rotation by Truncate
        // In case of rotation by Truncate, the current open file
        // MUST have a counterpart: either the latest file, or a previously
        // rotated file. If not, this is an anomaly (could happen if
        // previously rotated file have been deleted).
        if(hasCounterpart(currentOpenFile)) {
            int counterpartIndex = getCounterpartIndex(currentOpenFile);
            if(counterpartIndex == -1) {
                // Should never happen really if we are this far!!
                // Defensively, return -1.
                return -1;
            } else if(counterpartIndex == 0) {
                // We were still at the top file; find the oldest un-matched file.
                int lastUntracked = 1;
                while(lastUntracked < incoming.size()) {
                    if(hasCounterpart(incoming.get(lastUntracked))) {
                        break;
                    }
                    ++lastUntracked;
                }
                lastUntracked -= 1;

                if(lastUntracked == incoming.size()) {
                    // No unmatched files were found
                    return -1;
                } else {
                    // Now, depending on the situation, the current file could be any
                    // of the unmatched files, not necessarily the oldest. Starting
                    // with the assumption that it's the oldest, double check...
                    // TODO: Here would be the right place to compare file hashes
                    //       to double check that's the right file being tracked.
                    //       See #couldHaveBeenCurrentOpenFile().
                    for(; lastUntracked > 0 && !hasCounterpart(incoming.get(lastUntracked)); --lastUntracked) {
                        if(couldHaveBeenCurrentOpenFile(incoming.get(lastUntracked)))
                            break;
                    }
                    if(lastUntracked == 0)
                        // Couldn't find any good candidates
                        return -1;
                    else
                        return lastUntracked;
                }
            } else {
                // The counterpart must be a previously rotated file. We should
                // just keep tracking it.
                return counterpartIndex;
            }
        }
        return -1;
    }

    public Map<TrackedFile, String> getIncomingAnomalies() {
        if(incomingAnomalies == null) {
            incomingAnomalies = new LinkedHashMap<>();
            // For any matched files after the first one (to guard against Truncate rotation),
            // the order, timestamp and size should remain the same.
            int previousCounterpartIndex = -1;
            List<String> otherAnomalies = new ArrayList<>();
            boolean foundFirstMatch = false;
            for(int i = 1; i < incoming.size(); ++i) {
                otherAnomalies.clear();
                TrackedFile f = incoming.get(i);
                if(hasCounterpart(f)) {
                    int counterpartIndex = getCounterpartIndex(f);
                    if(foundFirstMatch) {
                        TrackedFile counterpart = getCounterpart(f);
                        if(counterpart.getSize() != f.getSize())
                            otherAnomalies.add("size changed from " + counterpart.getSize() + " to " + f.getSize());
                        if(counterpart.getLastModifiedTime() != f.getLastModifiedTime())
                            otherAnomalies.add("last modified time changed from " + counterpart.getLastModifiedTime() + " to " + f.getLastModifiedTime());
                        if(previousCounterpartIndex >= 0) {
                            if(counterpartIndex < previousCounterpartIndex)
                                otherAnomalies.add("file relative position changed (expected to be after position " + previousCounterpartIndex + ")");
                        }
                    } else {
                        foundFirstMatch = true;
                    }
                    previousCounterpartIndex = counterpartIndex;
                }
                if(!otherAnomalies.isEmpty())
                    incomingAnomalies.put(f, Joiner.on("; ").join(otherAnomalies));
            }
        }
        return incomingAnomalies;
    }

    /**
     * Checks if the current open file has been truncated, according to a
     * heuristic (see implementation).
     * TODO: The heuristic has a weakness: even if file has same/increased size,
     *       it could still be truncated and then popualated by the same/more
     *       amount of bytes. To make this more robust and handle such edge
     *       cases we could do a content comparision by hash.
     * @return {@code true} if the current open file has likely been
     *         truncated, otherwise {@code false}.
     */
    public boolean currentOpenFileWasTruncated() {
        TrackedFile counterpart = getCounterpart(currentOpenFile);
        return counterpart != null &&
                counterpart.getPath().equals(currentOpenFile.getPath()) &&
                counterpart.isSameAs(currentOpenFile) &&
                counterpart.getLastModifiedTime() >= currentOpenFile.getLastModifiedTime() &&
                counterpart.getSize() < currentOpenFile.getSize();
    }

    /**
     * @return {@code true} if all currently tracked files have
     *         disappeared, {@code false} otherwise.
     */
    public boolean allFilesHaveDisappeared() {
        return !current.isEmpty() &&
                (incoming.isEmpty() || !hasMatchedFilesAtOrAfter(current, 0));
    }

    /**
     * @param f
     * @return {@code true} if {@code getCounterpart(f)} would return
     *         non-{@code null}, {@code false} otherwise.
     */
    public boolean hasCounterpart(TrackedFile f) {
        return counterparts.containsKey(f);
    }

    /**
     * The "counterpart" of file {@code f}, defined as the file with the
     * same {@code FileId} in the reciprocal snapshot (if {@code f}
     * is in {@code current}, then the counterpart is in {@code incoming}
     * and vice-versa).
     * @param f
     * @return
     */
    public TrackedFile getCounterpart(TrackedFile f) {
        return counterparts.get(f);
    }

    /**
     * @param f
     * @return The index of the counterpart (see {@link #getCounterpart(TrackedFile)})
     *         if in the corresponding snapshot ({@code current} or
     *         {@code incoming}), or {@code -1} if {@code f}
     *         has no counterpart.
     */
    public int getCounterpartIndex(TrackedFile f) {
        Integer ix = counterpartIndices.get(f);
        return ix == null ? -1 : ix;
    }

    /**
     * Compares the attributes of {@code incomingFile} (I) to those of
     * {@code currentOpenFile} (C) and determines whether I could have been C
     * a while a ago. This heuristic assumes that I could have been C if all
     * the following are true:
     * <ol>
     *   <li>I.getLastModifiedTime() &gt;= C.getLastModifiedTime()</li>
     *   <li>I.getSize() &gt;= C.getSize()</li>
     *   <li>TODO: I.getContentHash() == C.getContentHash()</li>
     * </ol>
     * The logic being that files are being appended to, and therefore
     * their size is going to increase and their timestamp going to be updated
     * with every write. If we find that the size if I decreased or its
     * timestamp rolled back (relative to C), then I could not have been C.
     *
     * @param incomingFile
     * @return {@code true} if the {@code incomingFile} could have
     *         been the same as {@code currentFile} at a previous
     *         time, otherwise {@code false}.
     */
    private boolean couldHaveBeenCurrentOpenFile(TrackedFile incomingFile) {
        //Preconditions.checkNotNull(currentOpenFile);
        //Preconditions.checkArgument(incoming.contains(incomingFile));
        return incomingFile.getLastModifiedTime() >= currentOpenFile.getLastModifiedTime()
                && incomingFile.getSize() >= currentOpenFile.getSize();
    }

    private boolean hasMatchedFilesAtOrAfter(TrackedFileList files, int index) {
        for(int i = index; i < files.size(); ++i)
            if(hasCounterpart(files.get(i)))
                return true;
        return false;
    }

    /**
     * Matches all files in {@code newSnapshot} (except the top two) to
     * the {@code currentSnapshot}. {@code FileId} is used for the
     * matching.
     * @param other
     */
    @VisibleForTesting
    void syncCounterpartsByFileId() {
        // start with clean slate
        counterparts.clear();
        counterpartIndices.clear();
        // Use file ID to find the counterpart of new files in current
        for(int i = 0; i < incoming.size(); ++i) {
            TrackedFile incomingFile = incoming.get(i);
            int currentIndex = current.indexOfFileId(incomingFile.getId());
            if(currentIndex >= 0) {
                TrackedFile currentFile = current.get(currentIndex);
                counterparts.put(incomingFile, currentFile);
                counterpartIndices.put(incomingFile, currentIndex);
                counterparts.put(currentFile, incomingFile);
                counterpartIndices.put(currentFile, i);
            }
        }
    }
}
