/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.testing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.ToString;

import org.slf4j.Logger;
import org.testng.Assert;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.SourceFileTracker;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFileList;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpoint;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class TestableSourceFileTracker extends SourceFileTracker {
    private static final Logger LOGGER = TestUtils.getLogger(TestableSourceFileTracker.class);
    private final FileRotator rotator;
    List<RefreshEvent> refreshEvents;
    RememberedTrackedFile rememberedFile;

    public TestableSourceFileTracker(FileRotator rotator, FileFlow<?> flow, AgentContext agentContext) throws IOException {
        super(agentContext, flow);
        this.rotator = rotator;
    }

    public void rememberCurrentOpenFile() throws IOException {
        if(getCurrentOpenFile() != null)
            rememberedFile = new RememberedTrackedFile(getCurrentOpenFile(), getCurrentOpenFileIndex());
    }

    public void forgetgetCurrentOpenFile() {
        rememberedFile = null;
    }

    public void assertStillTrackingRememberedFileAfterRotation() throws IOException {
        if(!(rotator instanceof CopyFileRotator)) {
            Assert.assertTrue(newerFilesPending());
        }
        assertStillTrackingRememberedFile();
    }

    public void assertStillTrackingRememberedFileWithoutRotation() throws IOException {
        Assert.assertEquals(getCurrentOpenFileIndex(), rememberedFile.getIndex());
        Assert.assertEquals(getCurrentOpenFile().getPath(), rememberedFile.getPath());
        Assert.assertEquals(getCurrentOpenFile().getId(), rememberedFile.getId());
        Assert.assertSame(getCurrentOpenFile().getChannel(), rememberedFile.getChannel());
        Assert.assertFalse(newerFilesPending());
        assertStillTrackingRememberedFile();
    }

    public void assertStillTrackingRememberedFile() throws IOException {
        Preconditions.checkNotNull(rememberedFile, "Must call remembergetCurrentOpenFile() first!");
        Preconditions.checkState(getCurrentOpenFile() != rememberedFile, "Must call refresh() after calling remembergetCurrentOpenFile()!");
        Assert.assertTrue(getCurrentOpenFile().getSize() >= rememberedFile.getSize());
        Assert.assertTrue(getCurrentOpenFile().getLastModifiedTime() >= rememberedFile.getLastModifiedTime());
        Assert.assertEquals(getCurrentOpenFile().getCurrentOffset(), rememberedFile.getCurrentOffset());
        Assert.assertEquals(getCurrentOpenFileIndex(), pendingFiles.size());
        // check that the content is the same...
        boolean sameContent = rememberedFile.hasSameContentHash(getCurrentOpenFile().getPath());
        Assert.assertTrue(sameContent, String.format(
                "File hashes for first %d bytes of %s (previous) and %s (current) are different", rememberedFile.getMd5Size(),
                rememberedFile.toString(), getCurrentOpenFile().toString()));
    }

    public void assertRememberedFileWasClosed() {
        Assert.assertFalse(rememberedFile.getChannel().isOpen());
    }

    public void assertTailingWasReset() throws IOException {
        assertEvents("resetTailing");
        Assert.assertFalse(newerFilesPending());
        if(getCurrentOpenFile() != null) {
            Assert.assertEquals(getCurrentOpenFileIndex(), 0);
            Assert.assertTrue(getCurrentOpenFile().isOpen());
            Assert.assertEquals(getCurrentOpenFile().getCurrentOffset(), 0);
        } else {
            Assert.assertEquals(getCurrentOpenFileIndex(), -1);
        }
    }

    public void assertCurrentFileWasNotFound() {
        Assert.assertTrue(
                findEvent("currentFileNotFound", 0) != -1
                || findEvent("allFilesDisappeared", 0) != -1);
    }

    public void assertTailingWasStopped() {
        assertEvents("stopTailing");
        Assert.assertFalse(newerFilesPending());
        Assert.assertNull(getCurrentOpenFile());
        Assert.assertEquals(getCurrentOpenFileIndex(), -1);
    }

    public int findEventByType(String type) {
        int i = 0;
        for(RefreshEvent event : refreshEvents) {
            if(event.getType().equals(type))
                return i;
            ++i;
        }
        return -1;
    }

    public void assertEvents(String... types) {
        int i = 0;
        int eventIndex = 0;
        while(i < types.length && eventIndex < refreshEvents.size()) {
            String type = types[i];
            eventIndex = findEvent(type, eventIndex);
            if (eventIndex != -1) {
                LOGGER.trace("Event found: {}", refreshEvents.get(eventIndex));
                ++i;
            } else {
                Assert.fail("Event '" + type + "(" + i + ")' was not found!");
            }
        }
    }

    public int findEvent(String type, int start) {
        int j = start;
        while(j < refreshEvents.size()) {
            if(refreshEvents.get(j).getType().equals(type)) {
                return j;
            }
            ++j;
        }
        return -1;
    }

    public String dumpEvents() {
        return "Tracker Events:\n\t" + Joiner.on("\n\t").join(refreshEvents);
    }

    @Override
    public void initialize() throws IOException {
        refreshEvents = new ArrayList<>();
        recordRefreshEvent("initialize");
        super.initialize();
    }


    @Override
    public boolean initialize(FileCheckpoint checkpoint) throws IOException {
        refreshEvents = new ArrayList<>();
        recordRefreshEvent("initialize", "checkpoint", checkpoint);
        return super.initialize(checkpoint);
    }

    @Override
    public boolean refresh() throws IOException {
        recordRefreshEvent("refresh");
        return super.refresh();
    }

    @Override
    protected boolean onTrackingAnomaly(String message, TrackedFileList newSnapshot, boolean resetTailingRequested)
            throws IOException {
        recordRefreshEvent("tackingAnomaly",
                "message", message,
                "resetTailingRequested", resetTailingRequested);
        return super.onTrackingAnomaly(message, newSnapshot, resetTailingRequested);
    }

    @Override
    protected void onUnexpectedChangeDetected(TrackedFileList newSnapshot, TrackedFile file, String message)
            throws IOException {
        recordRefreshEvent("unexpectedChangeDetected",
                "filePattern", file,
                "message", message);
        super.onUnexpectedChangeDetected(newSnapshot, file, message);
    }

    @Override
    protected void stopTailing(TrackedFileList newSnapshot) throws IOException {
        recordRefreshEvent("stopTailing");
        super.stopTailing(newSnapshot);
    }

    @Override
    protected void startTailingNewFile(TrackedFileList newSnapshot, int index) throws IOException {
        recordRefreshEvent("startTailingNewFile", "newIndex", index);
        super.startTailingNewFile(newSnapshot, index);
    }

    @Override
    protected boolean onAllFilesDisappeared(TrackedFileList newSnapshot) throws IOException {
        recordRefreshEvent("allFilesDisappeared");
        return super.onAllFilesDisappeared(newSnapshot);
    }

    @Override
    protected boolean onRotationByTruncate(TrackedFileList newSnapshot, int newIndex) throws IOException {
        recordRefreshEvent("rotationByTruncate", "newIndex", newIndex);
        return super.onRotationByTruncate(newSnapshot, newIndex);
    }

    @Override
    protected boolean onTruncationWithoutRotation(TrackedFileList newSnapshot) throws IOException {
        recordRefreshEvent("truncationWithoutRotation");
        return super.onTruncationWithoutRotation(newSnapshot);
    }

    @Override
    protected void resumeTailingRotatedFileWithNewId(TrackedFileList newSnapshot, int index) throws IOException {
        recordRefreshEvent("resumeTailingRotatedFileWithNewId", "newIndex", index);
        super.resumeTailingRotatedFileWithNewId(newSnapshot, index);
    }

    @Override
    protected void resetTailing(TrackedFileList newSnapshot) throws IOException {
        recordRefreshEvent("resetTailing");
        super.resetTailing(newSnapshot);
    }

    @Override
    protected boolean onCurrentFileNotFound(TrackedFileList newSnapshot) throws IOException {
        recordRefreshEvent("currentFileNotFound");
        return super.onCurrentFileNotFound(newSnapshot);
    }

    private RefreshEvent recordRefreshEvent(String type, Object... kvpairs) {
        Preconditions.checkArgument(kvpairs.length % 2 == 0);
        RefreshEvent e = new RefreshEvent(type);
        e.put("currentFile", getCurrentOpenFile());
        for(int i = 0; i+1 < kvpairs.length; i+=2) {
            e.put(kvpairs[i].toString(), kvpairs[i+1]);
        }
        LOGGER.trace("Event: " + e);
        refreshEvents.add(e);
        return e;
    }

    /**
     * An event that happened during call to {@link SourceFileTracker#refresh()}.
     */
    @ToString
    public static class RefreshEvent {
        @Getter private final String type;
        private final Map<String, Object> data;
        @Getter private final long timestamp = System.currentTimeMillis();

        public RefreshEvent(String type) {
            this(type, Collections.<String, Object> emptyMap());
        }

        public RefreshEvent(String type, Map<String, Object> data) {
            this.type = type;
            this.data = new HashMap<>(data);
        }

        public void put(String key, Object value) {
            this.data.put(key, value);
        }

        public Object get(String key) {
            return this.data.get(key);
        }
    }

}
