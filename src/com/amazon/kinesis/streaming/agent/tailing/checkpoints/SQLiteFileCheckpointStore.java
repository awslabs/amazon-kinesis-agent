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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import lombok.Cleanup;
import lombok.ToString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FileId;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;

/**
 * Checkpoint store backed by a SQLite database file.
 * This class is thread-safe.
 */
@ToString(exclude={"agentContext", "connection"})
public class SQLiteFileCheckpointStore implements FileCheckpointStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteFileCheckpointStore.class);
    private static final int DEFAULT_DB_CONNECTION_TIMEOUT_SECONDS = 5;
    private static final int DEFAULT_DB_QUERY_TIMEOUT_SECONDS = 30;
    @VisibleForTesting
    static final String FINGERPRINT_SIZE_KEY = "checkpoints.fingerprintSizeBytes";
    private static final Range<Integer> VALID_FINGERPRINT_SIZE_RANGE =
            Range.closed(FileFingerprint.MIN_SIZE_BYTES, FileFingerprint.MAX_SIZE_BYTES);

    // Fingerprints live in their own table so FILE_CHECKPOINTS keeps the exact schema older agents read and write
    // (they insert positionally). Older agents ignore this table. A fingerprint row is only trusted while its
    // fileId and offset still equal the checkpoint row's, so a checkpoint updated by an older agent simply has
    // no fingerprint.
    private static final String FINGERPRINT_COLUMNS =
            "f.headLength as headLength, f.headHash as headHash, f.tailLength as tailLength, f.tailHash as tailHash";
    private static final String CHECKPOINTS_WITH_FINGERPRINTS =
            "from FILE_CHECKPOINTS c left join FILE_FINGERPRINTS f " +
            "on f.flow = c.flow and f.path = c.path and f.fileId = c.fileId and f.offset = c.offset ";

    private final Path dbFile;
    @VisibleForTesting
    final AgentContext agentContext;
    @VisibleForTesting
    Connection connection;
    private final int dbQueryTimeoutSeconds;
    private final int dbConnectionTimeoutSeconds;
    private final int fingerprintSizeBytes;

    public SQLiteFileCheckpointStore(AgentContext agentContext) {
        this.agentContext = agentContext;
        this.dbFile = this.agentContext.checkpointFile();
        this.dbQueryTimeoutSeconds = this.agentContext.readInteger("checkpoints.queryTimeoutSeconds", DEFAULT_DB_QUERY_TIMEOUT_SECONDS);
        this.dbConnectionTimeoutSeconds = this.agentContext.readInteger("checkpoints.connectionTimeoutSeconds", DEFAULT_DB_CONNECTION_TIMEOUT_SECONDS);
        this.fingerprintSizeBytes = this.agentContext.readInteger(FINGERPRINT_SIZE_KEY, FileFingerprint.DEFAULT_SIZE_BYTES);
        Configuration.validateRange(this.fingerprintSizeBytes, VALID_FINGERPRINT_SIZE_RANGE, FINGERPRINT_SIZE_KEY);
        connect();
        // Every time we connect, try cleaning up the database
        deleteOldData();
    }

    private synchronized boolean isConnected() {
        return connection != null;
    }

    @Override
    public synchronized void close() {
        if (isConnected()) {
            try {
                LOGGER.debug("Closing connection to database {}...", dbFile);
                connection.close();
            } catch (SQLException e) {
                LOGGER.error("Failed to cleanly close the database {}", dbFile, e);
            }
        }
        connection = null;
    }

    private synchronized void connect() {
        if (!isConnected()) {
            try {
                Class.forName("org.sqlite.JDBC");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Failed to load SQLite driver.", e);
            }

            try {
                LOGGER.debug("Connecting to database {}...", dbFile);
                if(!Files.isDirectory(dbFile.getParent())) {
                    Files.createDirectories(dbFile.getParent());
                }
                String connectionString = String.format("jdbc:sqlite:%s", dbFile.toString());
                connection = DriverManager.getConnection(connectionString);
                connection.setAutoCommit(false);
            } catch (SQLException | IOException e) {
                throw new RuntimeException("Failed to create or connect to the checkpoint database.", e);
            }
            try {
                @Cleanup Statement statement = connection.createStatement();
                statement.executeUpdate("create table if not exists FILE_CHECKPOINTS("
                        + "       flow text,"
                        + "       path text,"
                        + "       fileId text,"
                        + "       lastModifiedTime bigint,"
                        + "       size bigint,"
                        + "       offset bigint,"
                        + "       lastUpdated datetime,"
                        + "       primary key (flow, path))"
                        );
                statement.executeUpdate("create table if not exists FILE_FINGERPRINTS("
                        + "       flow text,"
                        + "       path text,"
                        + "       fileId text,"
                        + "       offset bigint,"
                        + "       headLength integer,"
                        + "       headHash text,"
                        + "       tailLength integer,"
                        + "       tailHash text,"
                        + "       primary key (flow, path))"
                        );
                connection.commit();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to configure the checkpoint database.", e);
            }
        }
    }

    protected boolean ensureConnected() {
        if (isConnected()) {
            return true;
        } else {
            try {
                connect();
                return true;
            } catch(Exception e) {
                LOGGER.error("Failed to open a connection to the checkpoint database.", e);
                return false;
            }
        }
    }

    @Override
    public FileCheckpoint saveCheckpoint(TrackedFile file, long offset) {
        Preconditions.checkNotNull(file);
        FileFingerprint fingerprint = FileFingerprint.compute(file.getPath(), file.getId(), offset, fingerprintSizeBytes);
        if (fingerprint == null && offset > 0) {
            LOGGER.trace("No fingerprint for checkpoint {}@{}: file not readable at its path as the same file.", file, offset);
        }
        return createOrUpdateCheckpoint(new FileCheckpoint(file, offset, fingerprint));
    }

    private synchronized FileCheckpoint createOrUpdateCheckpoint(FileCheckpoint cp) {
        if (!ensureConnected())
            return null;
        try {
            @Cleanup PreparedStatement update = connection.prepareStatement(
                    "update FILE_CHECKPOINTS " +
                    "set fileId=?, offset=?, lastModifiedTime=?, size=?, " +
                    "lastUpdated=strftime('%Y-%m-%d %H:%M:%f', 'now') " +
                    "where flow=? and path=?");
            update.setString(1, cp.getFile().getId().toString());
            update.setLong(2, cp.getOffset());
            update.setLong(3, cp.getFile().getLastModifiedTime());
            update.setLong(4, cp.getFile().getSize());
            update.setString(5, cp.getFile().getFlow().getId());
            update.setString(6, cp.getFile().getPath().toAbsolutePath().toString());
            int affected = update.executeUpdate();
            if (affected == 0) {
                @Cleanup PreparedStatement insert = connection.prepareStatement(
                        "insert or ignore into FILE_CHECKPOINTS " +
                        "values(?, ?, ?, ?, ?, ?, strftime('%Y-%m-%d %H:%M:%f', 'now'))");
                insert.setString(1, cp.getFile().getFlow().getId());
                insert.setString(2, cp.getFile().getPath().toAbsolutePath().toString());
                insert.setString(3, cp.getFile().getId().toString());
                insert.setLong(4, cp.getFile().getLastModifiedTime());
                insert.setLong(5, cp.getFile().getSize());
                insert.setLong(6, cp.getOffset());
                affected = insert.executeUpdate();
                if (affected == 1) {
                    LOGGER.trace("Created new database checkpoint: {}@{}", cp.getFile(), cp.getOffset());
                } else {
                    // SANITYCHECK: This should never happen since method is synchronized.
                    // TODO: Remove when done debugging.
                    LOGGER.error("Did not update or create checkpoint because of race condition: {}@{}", cp.getFile(), cp.getOffset());
                    throw new RuntimeException("Race condition detected when setting checkpoint for file: " + cp.getFile().getPath());
                }
            } else {
                LOGGER.trace("Updated database checkpoint: {}@{}", cp.getFile(), cp.getOffset());
            }
            saveFingerprint(cp);
            connection.commit();
            return cp;
        } catch (SQLException e) {
            LOGGER.error("Failed to create the checkpoint {}@{} in database {}", cp.getFile(), cp.getOffset(), dbFile);
            try {
                connection.rollback();
            } catch (SQLException e2) {
                LOGGER.error("Failed to rollback checkpointing transaction: {}@{}", cp.getFile(), cp.getOffset());
                LOGGER.info("Reinitializing connection to database {}", dbFile);
                close();
            }
            return null;
        }
    }

    /**
     * Stores the checkpoint's fingerprint in the same transaction as the checkpoint, or removes the old one
     * when there is none, so a fingerprint row never describes a different offset than its checkpoint.
     */
    private void saveFingerprint(FileCheckpoint cp) throws SQLException {
        String flowId = cp.getFile().getFlow().getId();
        String path = cp.getFile().getPath().toAbsolutePath().toString();
        FileFingerprint fp = cp.getFingerprint();
        if (fp == null) {
            @Cleanup PreparedStatement delete = connection.prepareStatement(
                    "delete from FILE_FINGERPRINTS where flow=? and path=?");
            delete.setString(1, flowId);
            delete.setString(2, path);
            delete.executeUpdate();
            return;
        }
        @Cleanup PreparedStatement upsert = connection.prepareStatement(
                "insert or replace into FILE_FINGERPRINTS " +
                "(flow, path, fileId, offset, headLength, headHash, tailLength, tailHash) " +
                "values(?, ?, ?, ?, ?, ?, ?, ?)");
        upsert.setString(1, flowId);
        upsert.setString(2, path);
        upsert.setString(3, cp.getFile().getId().toString());
        upsert.setLong(4, cp.getOffset());
        upsert.setInt(5, fp.getHeadLength());
        upsert.setString(6, fp.getHeadHash());
        upsert.setInt(7, fp.getTailLength());
        upsert.setString(8, fp.getTailHash());
        upsert.executeUpdate();
    }

    private static FileFingerprint readFingerprint(ResultSet result, long offset) throws SQLException {
        String headHash = result.getString("headHash");
        String tailHash = result.getString("tailHash");
        if (headHash == null || tailHash == null) {
            return null;
        }
        try {
            return new FileFingerprint(offset, result.getInt("headLength"), headHash, result.getInt("tailLength"), tailHash);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Ignoring invalid checkpoint fingerprint: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public FileCheckpoint getCheckpointForPath(FileFlow<?> flow, Path p) {
        Preconditions.checkNotNull(flow);
        Preconditions.checkNotNull(p);
        if (!ensureConnected())
            return null;
        try {
            @Cleanup PreparedStatement statement = connection.prepareStatement(
                    "select c.fileId as fileId, c.lastModifiedTime as lastModifiedTime, c.size as size, " +
                    "c.offset as offset, " + FINGERPRINT_COLUMNS + " " +
                    CHECKPOINTS_WITH_FINGERPRINTS +
                    "where c.flow=? and c.path=?");
            statement.setString(1, flow.getId());
            statement.setString(2, p.toAbsolutePath().toString());
            statement.setMaxRows(1);
            @Cleanup ResultSet result = statement.executeQuery();
            if (result.next()) {
                TrackedFile file = new TrackedFile(
                        flow, p, new FileId(result.getString("fileId")),
                        result.getLong("lastModifiedTime"),
                        result.getLong("size"));
                long offset = result.getLong("offset");
                return new FileCheckpoint(file, offset, readFingerprint(result, offset));
            } else
                return null;
        } catch (SQLException e) {
            LOGGER.error("Failed when getting checkpoint for path {} in flow {}", p, flow.getId(), e);
            return null;
        }
    }

    @Override
    public FileCheckpoint getCheckpointForFlow(FileFlow<?> flow) {
        Preconditions.checkNotNull(flow);
        if (!ensureConnected())
            return null;
        try {
            @Cleanup PreparedStatement statement = connection.prepareStatement(
                    "select c.path as path, c.fileId as fileId, c.lastModifiedTime as lastModifiedTime, " +
                    "c.size as size, c.offset as offset, " + FINGERPRINT_COLUMNS + " " +
                    CHECKPOINTS_WITH_FINGERPRINTS +
                    "where c.flow=? " +
                    "order by c.lastUpdated desc " +
                    "limit 1");
            statement.setString(1, flow.getId());
            statement.setMaxRows(1);
            @Cleanup ResultSet result = statement.executeQuery();
            if (result.next()) {
                TrackedFile file = new TrackedFile(
                        flow, Paths.get(result.getString("path")),
                        new FileId(result.getString("fileId")),
                        result.getLong("lastModifiedTime"),
                        result.getLong("size"));
                long offset = result.getLong("offset");
                return new FileCheckpoint(file, offset, readFingerprint(result, offset));
            } else
                return null;
        } catch (SQLException e) {
            LOGGER.error("Failed when getting checkpoint for flow {}", flow.getId(), e);
            return null;
        }
    }

    @Override
    public List<Map<String, Object>> dumpCheckpoints() {
        if (!ensureConnected())
            return Collections.emptyList();
        try {
            @Cleanup PreparedStatement statement = this.connection.prepareStatement(
                    "select * from FILE_CHECKPOINTS order by lastUpdated desc");
            @Cleanup ResultSet result = statement.executeQuery();
            List<Map<String, Object>> checkpoints = new ArrayList<>();
            ResultSetMetaData md = result.getMetaData();
            int columns = md.getColumnCount();
            while (result.next()) {
                Map<String, Object> row = new LinkedHashMap<>(columns);
                for (int i = 1; i <= columns; ++i) {
                    row.put(md.getColumnName(i), result.getObject(i));
                }
                checkpoints.add(row);
            }
            return checkpoints;
        } catch (SQLException e) {
            LOGGER.error("Failed when dumping checkpoints from db.", e);
            return Collections.emptyList();
        }
    }

    @VisibleForTesting
    synchronized void deleteOldData() {
        if (!ensureConnected())
            return;
        try {
            String query = String.format(
                    "delete from FILE_CHECKPOINTS " +
                    "where datetime(lastUpdated, '+%d days') < CURRENT_TIMESTAMP",
                    agentContext.checkpointTimeToLiveDays());
            @Cleanup PreparedStatement statement = connection.prepareStatement(query);
            statement.setQueryTimeout(dbQueryTimeoutSeconds);
            int affectedCount = statement.executeUpdate();
            // Drop fingerprints whose checkpoint is gone or was changed by an agent that does not write them.
            @Cleanup PreparedStatement fingerprints = connection.prepareStatement(
                    "delete from FILE_FINGERPRINTS where not exists (" +
                    "select 1 from FILE_CHECKPOINTS c where c.flow = FILE_FINGERPRINTS.flow " +
                    "and c.path = FILE_FINGERPRINTS.path and c.fileId = FILE_FINGERPRINTS.fileId " +
                    "and c.offset = FILE_FINGERPRINTS.offset)");
            fingerprints.setQueryTimeout(dbQueryTimeoutSeconds);
            int fingerprintCount = fingerprints.executeUpdate();
            connection.commit();
            LOGGER.info("Deleted {} old checkpoints and {} stale fingerprints.", affectedCount, fingerprintCount);
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException e2) {
                LOGGER.error("Failed to rollback cleanup transaction.", e2);
                LOGGER.info("Reinitializing connection to database {}", dbFile);
                close();
            }
            LOGGER.error("Failed to delete old checkpoints.", e);
        }
    }
}
