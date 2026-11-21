/*
 * Copyright (c) 2014-2017 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.checkpoints;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.config.ConfigurationException;
import com.amazon.kinesis.streaming.agent.tailing.FileId;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseConstants;
import com.amazon.kinesis.streaming.agent.tailing.FirehoseFileFlow;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpoint;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.FileCheckpointStore;
import com.amazon.kinesis.streaming.agent.tailing.checkpoints.SQLiteFileCheckpointStore;
import com.amazon.kinesis.streaming.agent.tailing.testing.TailingTestBase;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.google.common.base.Strings;

public class SQLiteFileCheckpointStoreTest extends TailingTestBase {
    private AgentContext context;

    @BeforeMethod
    public void setup() throws IOException {
        context = getTestAgentContext();
    }

    protected FileFlow<?> getTestFlow(String flow, String file) {
        Configuration config = TestUtils.getTestConfiguration(new Object[][] { {"filePattern", file + "*"}, {FirehoseConstants.DESTINATION_KEY, flow} });
        return new FirehoseFileFlow(context, config);
    }

    protected FileCheckpointStore getTestCheckpointStore() {
        return getTestCheckpointStore(new HashMap<String, Object>());
    }

    @SuppressWarnings("serial")
    private FileCheckpointStore getTestCheckpointStore(final Path dbFile) {
        return getTestCheckpointStore(new HashMap<String, Object>() {{
            put("checkpointFile", dbFile);
        }});
    }

    protected FileCheckpointStore getTestCheckpointStore(Map<String, Object> config) {
        config = new HashMap<>(config);
        if (!config.containsKey("checkpointFile")) {
            Path dbFile = testFiles.getTempFilePath();
            config.put("checkpointFile", dbFile);
        }
        AgentContext context = TestUtils.getTestAgentContext(config);
        return new SQLiteFileCheckpointStore(context);
    }

    @DataProvider(name = "saveCheckpointThenRetrieveIt")
    public Object[][] testSaveCheckpointThenRetrieveItData() {
        return new Object[][] { { "testflow1", globalTestFiles.createTempFile().toString(), 123 },
                { RandomStringUtils.random(25), globalTestFiles.createTempFile().toString(), 123 },
                { "testflow2", globalTestFiles.createTempFile().toString(), 0 },
                { RandomStringUtils.random(10), globalTestFiles.createTempFile().toString(), 0 },
                // test that large values that don't fit in Integer work well
                { "testflow3", globalTestFiles.createTempFile().toString(), 10_000_000_000L },
                // test that long flow names work
                { Strings.repeat("testflow3", 1000), globalTestFiles.createTempFile().toString(), 10_000_000_000L }, };
    }

    @Test(dataProvider = "saveCheckpointThenRetrieveIt")
    public void testSaveCheckpointThenRetrieveItByPath(String flow, String file, long position) throws IOException {
        FileCheckpointStore store = getTestCheckpointStore();
        TrackedFile tf = new TrackedFile(getTestFlow(flow, file), Paths.get(file));
        FileCheckpoint cp = store.saveCheckpoint(tf, position);
        FileCheckpoint cp2 = store.getCheckpointForPath(tf.getFlow(), tf.getPath());
        Assert.assertEquals(cp2, cp);
    }

    @Test(invocationCount=5, skipFailedInvocations=true)
    public void testSaveCheckpointThenRetrieveItByFlow() throws Exception {
        FileCheckpointStore store = getTestCheckpointStore();
        final String filePrefix = "logfile";
        final FileFlow<?> flow = getTestFlow("testflow", testFiles.getTempFilePath().resolve(filePrefix).toString());
        Path file1 = testFiles.createTempFileWithPrefix(filePrefix);
        TrackedFile tf1 = new TrackedFile(flow, file1);
        store.saveCheckpoint(tf1, 1000L);
        Thread.sleep(100);
        // Add a second checkpoint for the same flow, but different file
        Path file2 = testFiles.createTempFileWithPrefix(filePrefix);
        TrackedFile tf2 = new TrackedFile(flow, file2);
        FileCheckpoint cp2 = store.saveCheckpoint(tf2, 2000L);

        // Should get the latest checkpoint for the flow
        FileCheckpoint flowCp = store.getCheckpointForFlow(flow);
        Assert.assertEquals(flowCp, cp2);
    }

    @Test
    public void testSamePathWithDifferentFlows() throws IOException {
        FileCheckpointStore store = getTestCheckpointStore();
        Path file = testFiles.createTempFile();

        FileFlow<?> flow1 = getTestFlow("fh1", file.toString());
        FileCheckpoint cp1 = store.saveCheckpoint(new TrackedFile(flow1, file), 1000);

        FileFlow<?> flow2 = getTestFlow("fh2", file.toString());
        FileCheckpoint cp2 = store.saveCheckpoint(new TrackedFile(flow2, file), 12323423234L);

        FileCheckpoint cp1_2 = store.getCheckpointForPath(flow1, file);
        Assert.assertNotNull(cp1_2);

        FileCheckpoint cp2_2 = store.getCheckpointForPath(flow2, file);
        Assert.assertNotNull(cp2_2);

        Assert.assertNotEquals(cp1_2, cp2_2);
        Assert.assertEquals(cp1_2, cp1);
        Assert.assertEquals(cp2_2, cp2);
    }

    @Test
    public void testClosingAndReopeningStore() throws IOException {
        final Path tmpFile = testFiles.createTempFile();
        final FileFlow<?> testFlow = getTestFlow("testflow", tmpFile.toString());

        Path dbFile = testFiles.getTempFilePath();

        SQLiteFileCheckpointStore store1 = (SQLiteFileCheckpointStore) getTestCheckpointStore(dbFile);
        FileCheckpoint cp = store1.saveCheckpoint(new TrackedFile(testFlow, tmpFile), 1000);
        store1.close();

        SQLiteFileCheckpointStore store2 = (SQLiteFileCheckpointStore) getTestCheckpointStore(dbFile);
        FileCheckpoint cp2 = store2.getCheckpointForPath(testFlow, tmpFile);
        Assert.assertEquals(cp2, cp);
        store2.close();
    }

    @Test
    public void testEqualityOfCheckpoint() throws IOException {
        Path dbFile = testFiles.getTempFilePath();
        SQLiteFileCheckpointStore store = (SQLiteFileCheckpointStore) getTestCheckpointStore(dbFile);

        final Path tmpFile = testFiles.createTempFile();
        final FileFlow<?> testFlow = getTestFlow("testflow", tmpFile.toString());
        final long offset = 1000L;
        FileCheckpoint cp = new FileCheckpoint(new TrackedFile(testFlow, tmpFile), offset);

        FileCheckpoint savedCp = store.saveCheckpoint(new TrackedFile(testFlow, tmpFile), offset);
        Assert.assertEquals(savedCp, cp);
        FileCheckpoint retrievedCp = store.getCheckpointForPath(testFlow, tmpFile);
        Assert.assertEquals(retrievedCp, cp);
    }

    @Test
    public void testDeleteOldData() throws IOException, SQLException {
        SQLiteFileCheckpointStore store = (SQLiteFileCheckpointStore) getTestCheckpointStore();
        Path[] tmpFiles = new Path[4];
        FileFlow<?>[] flows = new FileFlow<?>[4];

        FileCheckpoint[] checkpoints = new FileCheckpoint[4];
        // create checkpoints
        for(int i = 0; i < 4; ++i){
            tmpFiles[i] = testFiles.createTempFile();
            flows[i] = getTestFlow("flow"+i, tmpFiles[i].toString());
            checkpoints[i] = store.saveCheckpoint(new TrackedFile(flows[i], tmpFiles[i]), 1000L + i^2);
        }
        // change the timestamp on half the checkpoints
        PreparedStatement update = store.connection.prepareStatement(
                "update FILE_CHECKPOINTS " +
                "set lastUpdated = datetime(?) " +
                "where flow=?");
        // try a very old date (>1 year as of writing)
        update.setString(1, "2014-01-01");
        update.setString(2, flows[0].getId());
        Assert.assertEquals(update.executeUpdate(), 1);
        store.connection.commit();
        // try date slightly longer than checkpointTimeToLiveDays
        DateTime newTimestamp = new DateTime(DateTimeZone.UTC).minusDays(store.agentContext.checkpointTimeToLiveDays()).minusHours(1);
        update.setString(1, newTimestamp.toString("yyyy-MM-dd HH:mm:ss"));
        update.setString(2, flows[1].getId());
        Assert.assertEquals(update.executeUpdate(), 1);
        store.connection.commit();
        // now trigger the cleanup
        store.deleteOldData();
        // make sure the checkpoints were deleted
        Assert.assertNull(store.getCheckpointForPath(flows[0], tmpFiles[0]));
        Assert.assertNull(store.getCheckpointForPath(flows[1], tmpFiles[1]));
        // makre sure the recent checkpoints remain
        Assert.assertEquals(store.getCheckpointForPath(flows[2], tmpFiles[2]), checkpoints[2]);
        Assert.assertEquals(store.getCheckpointForPath(flows[3], tmpFiles[3]), checkpoints[3]);
    }

    @Test
    public void testStoreWillCreateParentDirectoryIfMissing() {
        Path parentDir = testFiles.getTmpDir().resolve("dir1/dir2/dir3");
        Assert.assertFalse(Files.exists(parentDir));
        Path dbFile = parentDir.resolve("checkpoints");
        getTestCheckpointStore(dbFile);
        Assert.assertTrue(Files.exists(parentDir));
        Assert.assertTrue(Files.isDirectory(parentDir));
        Assert.assertTrue(Files.exists(dbFile));
        Assert.assertTrue(Files.isRegularFile(dbFile));
    }

    // ---------------------------------------------------------------- fingerprints

    private Path writeLines(String prefix, int count) throws IOException {
        Path file = testFiles.createTempFile();
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= count; i++) {
            sb.append("{\"file\":\"").append(prefix).append("\",\"n\":").append(i).append("}\n");
        }
        Files.write(file, sb.toString().getBytes(StandardCharsets.UTF_8));
        return file;
    }

    @SuppressWarnings("serial")
    private SQLiteFileCheckpointStore getStore(final Path dbFile) {
        return (SQLiteFileCheckpointStore) getTestCheckpointStore(dbFile);
    }

    private static Connection rawConnection(Path dbFile) throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + dbFile);
    }

    private static int countFingerprints(Connection connection) throws SQLException {
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery("select count(*) from FILE_FINGERPRINTS")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Test
    public void testFingerprintSavedAndLoaded() throws IOException {
        FileCheckpointStore store = getTestCheckpointStore();
        Path file = writeLines("A", 200);
        long offset = Files.size(file) - 50;
        TrackedFile tf = new TrackedFile(getTestFlow("fpflow", file.toString()), file);
        FileCheckpoint cp = store.saveCheckpoint(tf, offset);

        FileFingerprint expected = FileFingerprint.compute(file, FileId.get(file), offset, FileFingerprint.DEFAULT_SIZE_BYTES);
        Assert.assertNotNull(expected);
        Assert.assertEquals(cp.getFingerprint(), expected);
        Assert.assertEquals(store.getCheckpointForPath(tf.getFlow(), file).getFingerprint(), expected);
        Assert.assertEquals(store.getCheckpointForFlow(tf.getFlow()).getFingerprint(), expected);
    }

    @Test
    public void testNoFingerprintWhenOffsetPastEndOfFile() throws IOException {
        FileCheckpointStore store = getTestCheckpointStore();
        Path file = writeLines("A", 5);
        TrackedFile tf = new TrackedFile(getTestFlow("fpflow", file.toString()), file);
        FileCheckpoint cp = store.saveCheckpoint(tf, Files.size(file) + 1);
        Assert.assertNotNull(cp);
        Assert.assertNull(cp.getFingerprint());
        Assert.assertNull(store.getCheckpointForPath(tf.getFlow(), file).getFingerprint());
    }

    @Test
    public void testCheckpointWithoutFingerprintReplacesOldFingerprint() throws Exception {
        Path dbFile = testFiles.getTempFilePath();
        SQLiteFileCheckpointStore store = getStore(dbFile);
        Path file = writeLines("A", 200);
        TrackedFile tf = new TrackedFile(getTestFlow("fpflow", file.toString()), file);
        Assert.assertNotNull(store.saveCheckpoint(tf, 500).getFingerprint());
        // Same path, but the file is now a different file: no fingerprint, and the old one must not linger
        Files.delete(file);
        Assert.assertNull(store.saveCheckpoint(tf, 600).getFingerprint());
        Assert.assertEquals(countFingerprints(store.connection), 0);
        Assert.assertNull(store.getCheckpointForPath(tf.getFlow(), file).getFingerprint());
    }

    /** A database created by an older agent has no FILE_FINGERPRINTS table: it must load, with no fingerprints. */
    @Test
    public void testOpensDatabaseFromOlderAgent() throws Exception {
        Path dbFile = testFiles.getTempFilePath();
        Path file = writeLines("A", 200);
        FileFlow<?> flow = getTestFlow("legacyflow", file.toString());
        try (Connection old = rawConnection(dbFile); Statement st = old.createStatement()) {
            st.executeUpdate("create table FILE_CHECKPOINTS(flow text, path text, fileId text, lastModifiedTime bigint, "
                    + "size bigint, offset bigint, lastUpdated datetime, primary key (flow, path))");
            try (PreparedStatement insert = old.prepareStatement(
                    "insert into FILE_CHECKPOINTS values(?, ?, ?, ?, ?, ?, strftime('%Y-%m-%d %H:%M:%f', 'now'))")) {
                insert.setString(1, flow.getId());
                insert.setString(2, file.toAbsolutePath().toString());
                insert.setString(3, FileId.get(file).toString());
                insert.setLong(4, 0);
                insert.setLong(5, Files.size(file));
                insert.setLong(6, 1234);
                insert.executeUpdate();
            }
        }
        SQLiteFileCheckpointStore store = getStore(dbFile);
        FileCheckpoint cp = store.getCheckpointForPath(flow, file);
        Assert.assertNotNull(cp);
        Assert.assertEquals(cp.getOffset(), 1234);
        Assert.assertNull(cp.getFingerprint());
        Assert.assertNull(store.getCheckpointForFlow(flow).getFingerprint());
        // and new checkpoints get fingerprints from now on
        Assert.assertNotNull(store.saveCheckpoint(new TrackedFile(flow, file), 2000).getFingerprint());
    }

    /**
     * After a downgrade an older agent writes FILE_CHECKPOINTS with its positional insert and plain updates. Its
     * insert must still work with the new database, and a fingerprint left behind for an updated checkpoint must
     * not be applied to the new offset.
     */
    @Test
    public void testOlderAgentWritesAfterUpgrade() throws Exception {
        Path dbFile = testFiles.getTempFilePath();
        SQLiteFileCheckpointStore store = getStore(dbFile);
        Path file1 = writeLines("A", 200);
        Path file2 = writeLines("B", 200);
        FileFlow<?> flow = getTestFlow("downgradeflow", file1.toString());
        Assert.assertNotNull(store.saveCheckpoint(new TrackedFile(flow, file1), 500).getFingerprint());

        try (Connection old = rawConnection(dbFile)) {
            try (PreparedStatement update = old.prepareStatement(
                    "update FILE_CHECKPOINTS set offset=?, lastUpdated=strftime('%Y-%m-%d %H:%M:%f', 'now') where flow=? and path=?")) {
                update.setLong(1, 900);
                update.setString(2, flow.getId());
                update.setString(3, file1.toAbsolutePath().toString());
                Assert.assertEquals(update.executeUpdate(), 1);
            }
            try (PreparedStatement insert = old.prepareStatement(
                    "insert or ignore into FILE_CHECKPOINTS values(?, ?, ?, ?, ?, ?, strftime('%Y-%m-%d %H:%M:%f', 'now'))")) {
                insert.setString(1, flow.getId());
                insert.setString(2, file2.toAbsolutePath().toString());
                insert.setString(3, FileId.get(file2).toString());
                insert.setLong(4, 0);
                insert.setLong(5, Files.size(file2));
                insert.setLong(6, 300);
                Assert.assertEquals(insert.executeUpdate(), 1);
            }
        }

        FileCheckpoint cp1 = store.getCheckpointForPath(flow, file1);
        Assert.assertEquals(cp1.getOffset(), 900);
        Assert.assertNull(cp1.getFingerprint(), "fingerprint for offset 500 must not apply to offset 900");
        FileCheckpoint cp2 = store.getCheckpointForPath(flow, file2);
        Assert.assertEquals(cp2.getOffset(), 300);
        Assert.assertNull(cp2.getFingerprint());

        // The cleanup drops the stale fingerprint row
        store.deleteOldData();
        Assert.assertEquals(countFingerprints(store.connection), 0);
    }

    @Test
    public void testDeleteOldDataRemovesFingerprintsOfDeletedCheckpoints() throws Exception {
        SQLiteFileCheckpointStore store = (SQLiteFileCheckpointStore) getTestCheckpointStore();
        Path file = writeLines("A", 200);
        FileFlow<?> flow = getTestFlow("ttlflow", file.toString());
        store.saveCheckpoint(new TrackedFile(flow, file), 500);
        Assert.assertEquals(countFingerprints(store.connection), 1);

        PreparedStatement update = store.connection.prepareStatement(
                "update FILE_CHECKPOINTS set lastUpdated = datetime(?) where flow=?");
        update.setString(1, "2014-01-01");
        update.setString(2, flow.getId());
        Assert.assertEquals(update.executeUpdate(), 1);
        store.connection.commit();

        store.deleteOldData();
        Assert.assertNull(store.getCheckpointForPath(flow, file));
        Assert.assertEquals(countFingerprints(store.connection), 0);
    }

    @SuppressWarnings("serial")
    @Test
    public void testFingerprintSizeIsConfigurable() throws IOException {
        FileCheckpointStore store = getTestCheckpointStore(new HashMap<String, Object>() {{
            put(SQLiteFileCheckpointStore.FINGERPRINT_SIZE_KEY, 100);
        }});
        Path file = writeLines("A", 200);
        FileCheckpoint cp = store.saveCheckpoint(new TrackedFile(getTestFlow("sizeflow", file.toString()), file), 1000);
        Assert.assertEquals(cp.getFingerprint().getHeadLength(), 100);
        Assert.assertEquals(cp.getFingerprint().getTailLength(), 100);
    }

    @DataProvider(name = "badFingerprintSizes")
    public Object[][] badFingerprintSizes() {
        return new Object[][] { { 0 }, { FileFingerprint.MIN_SIZE_BYTES - 1 }, { FileFingerprint.MAX_SIZE_BYTES + 1 } };
    }

    @SuppressWarnings("serial")
    @Test(dataProvider = "badFingerprintSizes", expectedExceptions = ConfigurationException.class)
    public void testFingerprintSizeOutOfRange(final int size) {
        getTestCheckpointStore(new HashMap<String, Object>() {{
            put(SQLiteFileCheckpointStore.FINGERPRINT_SIZE_KEY, size);
        }});
    }
}
