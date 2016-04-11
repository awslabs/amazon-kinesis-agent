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

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.Logging;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.FileId;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Checkpoint store backed by a SQLite database file.
 * This class is thread-safe.
 */
@ToString(exclude={"agentContext", "connection"})
public class SQLiteFileCheckpointStore implements FileCheckpointStore {
    private static final Logger LOGGER = Logging.getLogger(SQLiteFileCheckpointStore.class);
    private static final int DEFAULT_DB_CONNECTION_TIMEOUT_SECONDS = 5;
    private static final int DEFAULT_DB_QUERY_TIMEOUT_SECONDS = 30;

    private final Path dbFile;
    @VisibleForTesting
    final AgentContext agentContext;
    @VisibleForTesting
    Connection connection;
    private final int dbQueryTimeoutSeconds;
    private final int dbConnectionTimeoutSeconds;

    public SQLiteFileCheckpointStore(AgentContext agentContext) {
        this.agentContext = agentContext;
        this.dbFile = this.agentContext.checkpointFile();
        this.dbQueryTimeoutSeconds = this.agentContext.readInteger("checkpoints.queryTimeoutSeconds", DEFAULT_DB_QUERY_TIMEOUT_SECONDS);
        this.dbConnectionTimeoutSeconds = this.agentContext.readInteger("checkpoints.connectionTimeoutSeconds", DEFAULT_DB_CONNECTION_TIMEOUT_SECONDS);
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
    public synchronized FileCheckpoint saveCheckpoint(TrackedFile file, long offset) {
        Preconditions.checkNotNull(file);
        return createOrUpdateCheckpoint(new FileCheckpoint(file, offset));
    }

    private FileCheckpoint createOrUpdateCheckpoint(FileCheckpoint cp) {
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

    @Override
    public FileCheckpoint getCheckpointForPath(FileFlow<?> flow, Path p) {
        Preconditions.checkNotNull(flow);
        Preconditions.checkNotNull(p);
        if (!ensureConnected())
            return null;
        try {
            @Cleanup PreparedStatement statement = connection.prepareStatement(
                    "select fileId, lastModifiedTime, size, offset " +
                    "from FILE_CHECKPOINTS " +
                    "where flow=? and path=?");
            statement.setString(1, flow.getId());
            statement.setString(2, p.toAbsolutePath().toString());
            statement.setMaxRows(1);
            @Cleanup ResultSet result = statement.executeQuery();
            if (result.next()) {
                TrackedFile file = new TrackedFile(
                        flow, p, new FileId(result.getString("fileId")),
                        result.getLong("lastModifiedTime"),
                        result.getLong("size"));
                return new FileCheckpoint(file, result.getLong("offset"));
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
                    "select path, fileId, lastModifiedTime, size, offset " +
                    "from FILE_CHECKPOINTS " +
                    "where flow=? " +
                    "order by lastUpdated desc " +
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
                return new FileCheckpoint(file, result.getLong("offset"));
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
            connection.commit();
            LOGGER.info("Deleted {} old checkpoints.", affectedCount);
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
