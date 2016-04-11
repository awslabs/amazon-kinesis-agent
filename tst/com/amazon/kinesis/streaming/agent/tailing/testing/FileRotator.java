/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing.testing;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.NotThreadSafe;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import org.slf4j.Logger;

import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

/**
 * Base class for all file-rotation simulators.
 * Subclasses need to implement the specific rotation logic (e.g rename/recreate,
 * copy/truncate, etc...).
 */
@NotThreadSafe
@ToString(exclude = { "logger", "deletedDir", "deletedFiles" })
public abstract class FileRotator {
    public static final int DEFAULT_MIN_NEW_FILE_SIZE_BYTES = 50 * 1024;
    public static final int DEFAULT_MIN_ROTATED_FILE_SIZE_BYTES = 10 * DEFAULT_MIN_NEW_FILE_SIZE_BYTES;
    public static final int DEFAULT_MAX_FILES_TO_KEEP_ON_DISK = 5;

    protected final Logger logger;
    @Getter protected final Path dir;
    @Getter protected final Path deletedDir;
    @Getter protected final String prefix;
    @Getter @Setter protected int maxFilesToKeepOnDisk;
    @Getter protected final List<Path> activeFiles;
    @Getter protected final List<Path> deletedFiles;
    @Getter protected final List<AtomicInteger> recordsWritten;
    @Getter protected final RecordGenerator recordGenerator;
    @Getter @Setter protected long minNewFileSize;
    @Getter @Setter protected long minRotatedFileSize;
    @Getter private long lastRotationTime = 0;
    private AtomicLong totalRecordsWritten = new AtomicLong();
    private AtomicLong totalBytesWritten = new AtomicLong();

    public FileRotator(Path dir, String prefix, RecordGenerator recordGenerator) {
        this.dir = dir;
        this.deletedDir = this.dir.resolve(".deleted");
        if (!Files.isDirectory(this.deletedDir)) {
            try {
                Files.createDirectories(this.deletedDir);
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        }
        this.prefix = prefix;
        this.maxFilesToKeepOnDisk = DEFAULT_MAX_FILES_TO_KEEP_ON_DISK;
        this.minNewFileSize = DEFAULT_MIN_NEW_FILE_SIZE_BYTES;
        this.minRotatedFileSize = DEFAULT_MIN_ROTATED_FILE_SIZE_BYTES;
        this.activeFiles = new ArrayList<>();
        this.deletedFiles = new ArrayList<>();
        this.recordsWritten = new ArrayList<>();
        this.logger = TestUtils.getLogger(getClass());
        this.recordGenerator = recordGenerator;
    }

    public FileRotator(Path dir, String prefix, boolean multiline) {
        this(dir, prefix, new RecordGenerator(multiline));
    }

    public FileRotator(Path dir, String prefix) {
        this(dir, prefix, new RecordGenerator());
    }

    /**
     * @param index
     *            The index of the file to return, where 0 points to the most
     *            recent file, and {@link #getMaxInputFileIndex()} points to the oldest file.
     * @return The path of the file at the given inde.
     * @throws IndexOutOfBoundsException
     *             If the index is less than 0 or more than {@link #getMaxInputFileIndex()}.
     */
    public Path getFile(int index) {
        return activeFiles.get(index);
    }

    /**
     *
     * @param index
     * @return The number of records written via this rotator to the file
     *         at the given index. This only captures data writted by this
     *         rotator by calling the {@link #appendDataToLatestFile(long)}.
     */
    public int getRecordsWrittenToFile(int index) {
        return recordsWritten.get(index).intValue();
    }

    /**
     * @return The latest file (<code>getFile(0)</code>) or <code>null</code> if there are no files created yet.
     */
    public Path getLatestFile() {
        return activeFiles.isEmpty() ? null : getFile(0);
    }

    /**
     * @return The index of the last (oldest) file.
     */
    public int getMaxInputFileIndex() {
        return Math.min(activeFiles.size(), maxFilesToKeepOnDisk) - 1;
    }

    /**
     * Creates a number of files, one at a time, simulating rotation. All files
     * will contain data, except the last one which is governed by <code>writeData</code> parameters.
     *
     * @param rotationCount
     *            Number of files to create.
     * @return The path of the last file created.
     */
    public Path rotate(int rotationCount) {
        while (rotationCount-- > 1) {
            rotate();
        }
        return rotate();
    }

    /**
     * @return The new path of the file that used to be the latest file (now <code>getFile(1)</code>), or
     *         <code>null</code> if no files
     *         existed before this call.
     */
    public Path rotate() {
        try {
            writeDataToLatestFileBeforeRotation();
            waitForNextRotation();
            Path newPath = rotateOrCreateNewFile();
            activeFiles.add(0, newPath);
            recordsWritten.add(0, new AtomicInteger(0));
            lastRotationTime = System.currentTimeMillis();
            writeDataToNewFileAfterRotation();
            while(activeFiles.size() > maxFilesToKeepOnDisk) {
                Path toDelete = activeFiles.remove(activeFiles.size()-1);
                deletedFiles.add(TestUtils.moveFileToTrash(toDelete, deletedDir));
                logger.trace("Deleted {} since maxFilesToKeep ({}) exceeded", toDelete, maxFilesToKeepOnDisk);
            }
            return activeFiles.size() > 1 ? getFile(1) : null;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Before we rotate, we generate some more data in the latest file as well.
     * This is important to simulate real-life rotations: the new file
     * is expected to be much smaller than the files that got rotated.
     * See {@link TrackedFile#isNewer(long, long, long, long)} for details
     * on why this is important to simulate a real-life file rotation.
     *
     * @return
     * @see TrackedFile#isNewer(long, long, long, long)
     */
    protected int writeDataToLatestFileBeforeRotation() {
        if (!activeFiles.isEmpty()) {
            // write some random amount of data > minRotatedFileSize
            long dataSizeToWrite = (long) (minRotatedFileSize * ThreadLocalRandom.current().nextDouble(1, 2));
            return appendDataToLatestFile(dataSizeToWrite);
        } else
            return 0;
    }

    /**
     * After we rotate, we write some data to the new file.
     *
     * @return
     */
    protected int writeDataToNewFileAfterRotation() {
        // write some random amount of data > minNewFileSize
        long dataSizeToWrite = (long) (minNewFileSize * ThreadLocalRandom.current().nextDouble(1, 2));
        return appendDataToLatestFile(dataSizeToWrite);
    }

    /**
     * Rotates a file by renaming it.
     *
     * @param index
     *            Index of the file to rotate.
     * @throws IOException
     */
    protected Path rotateFileName(int index) throws IOException {
        if (index < activeFiles.size() - 1) {
            rotateFileName(index + 1);
        }
        Path oldPath = getFile(index);
        Path newPath = getFilePathAfterRotation(index);
        if (!newPath.equals(oldPath)) {
            logger.trace("Moving (rotation) {} to {}", oldPath, newPath);
            Files.move(oldPath, newPath);
            activeFiles.set(index, newPath);
        }
        return newPath;
    }

    /**
     * @param maxBytes
     *            Maximum number of bytes to be written. The actual number of
     *            bytes written with be higher, but close to this minimum.
     * @return Number of records written to file.
     */
    public int appendDataToLatestFile(long maxBytes) {
        Path file = getFile(0);
        try {
            if (!Files.exists(file))
                return 0;
            long oldSize = Files.size(file);
            int records = finishAppendPartialRecordToLatestFile();
            try (FileChannel output = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                records = recordGenerator.appendDataToChannel(output, maxBytes);
            }
            long newSize = Files.size(file);
            long bytesWritten = newSize - oldSize;
            logger.trace("Generated {} records ({} bytes) of data into {} (new size {})", records, bytesWritten, file,
                    newSize);
            recordsWritten.get(0).addAndGet(records);
            totalRecordsWritten.addAndGet(records);
            totalBytesWritten.addAndGet(bytesWritten);
            return records;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * @param recordCount
     *            Number of records to add to the latest file.
     *
     * @return Number of records written to file.
     */
    public int appendRecordsToLatestFile(int recordCount) {
        Path file = getFile(0);
        try {
            if (!Files.exists(file))
                return 0;
            long oldSize = Files.size(file);
            int partialRecordCount = finishAppendPartialRecordToLatestFile();
            try (FileChannel output = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                recordGenerator.appendRecordsToChannel(output, recordCount - partialRecordCount);
            }
            long newSize = Files.size(file);
            long bytesWritten = newSize - oldSize;
            recordsWritten.get(0).addAndGet(recordCount);
            totalRecordsWritten.addAndGet(recordCount);
            totalBytesWritten.addAndGet(bytesWritten);
            return recordCount;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public int startAppendPartialRecordToLatestFile() {
        Path file = getFile(0);
        try {
            if (!Files.exists(file))
                return 0;
            recordGenerator.startAppendPartialRecordToFile(file);
            logger.trace("Wrote half a record to ", file, Files.size(file));
            return 0;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public int finishAppendPartialRecordToLatestFile() {
        Path file = getFile(0);
        try {
            if (!Files.exists(file)) {
                recordGenerator.discardPartialRecord();
                return 0;
            }
            if (recordGenerator.finishAppendPartialRecordToFile(file) > 0) {
                logger.trace("Finished writing partial record to {} (new size {})", file, Files.size(file));
                recordsWritten.get(0).addAndGet(1);
                return 1;
            } else
                return 0;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Imposes a minimum time between rotations to make sure file times
     * are different. The wait time is based on {@link TestUtils#getFileTimeResolution()}.
     */
    protected void waitForNextRotation() {
        long waitTime = lastRotationTime > 0 ? (lastRotationTime + (long) (TestUtils.getFileTimeResolution() * 1.5) - System
                .currentTimeMillis()) : 0;
        if (waitTime > 0) {
            logger.trace("Waiting {} milliseconds before performing rotation.", waitTime);
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // just return
                logger.trace("Thread interrupted.", e);
            }
        }
    }

    /**
     * @return The glob expression that can be used to track the files generated
     *         by this rotator.
     */
    public String getInputFileGlob() {
        return String.format("%s/%s*", dir.toString(), prefix);
    }

    /**
     * @return The list of all files where data was generated by this rotator.
     */
    public List<Path> getInputFiles() {
        return activeFiles.subList(0, getMaxInputFileIndex() + 1);
    }

    /**
     * @param index
     * @return The path of the file at given index after it's rotated once.
     */
    @VisibleForTesting
    Path getFilePathAfterRotation(int index) {
        return Paths.get(getNewFilePath().toString() + "." + (index + 1));
    }

    /**
     *
     * @return The path of the file that's to be created.
     */
    @VisibleForTesting
    Path getNewFilePath() {
        return dir.resolve(prefix);
    }

    public long getTotalRecordsWritten() {
        return totalRecordsWritten.get();
    }

    public long getTotalBytesWritten() {
        return totalBytesWritten.get();
    }

    /**
     * Perform the actual file rotation. Each subclass will implement its own
     * rotation mechanism.
     *
     * @param index
     * @return The path of the latest file.
     * @throws IOException
     */
    protected abstract Path rotateOrCreateNewFile() throws IOException;

    /**
     * @return <code>true</code> if after calling {@link #rotate()} the
     *         file that was just rotated keeps the same <code>FileId</code> as
     *         before the rotation, otherwise <code>false</code>.
     */
    @VisibleForTesting
    boolean rotatedFileKeepsSameId() {
        return !latestFileKeepsSameId();
    }

    /**
     * @return <code>true</code> if after calling {@link #rotate()} the
     *         file that was just rotated keeps the same path as before the
     *         rotation, otherwise <code>false</code>.
     */
    @VisibleForTesting
    boolean rotatedFileKeepsSamePath() {
        return !latestFileKeepsSamePath();
    }

    /**
     * @return <code>true</code> if after calling {@link #rotate()} the
     *         latest file (if it exists) will have the same path, otherwise <code>false</code>.
     */
    @VisibleForTesting
    abstract boolean latestFileKeepsSamePath();

    /**
     * @return <code>true</code> if after calling {@link #rotate()} the
     *         latest file (if it exists) will have the same ID, otherwise <code>false</code>.
     */
    @VisibleForTesting
    abstract boolean latestFileKeepsSameId();

}
