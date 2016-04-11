/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.testing;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;

import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.IClass;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;

import com.amazon.kinesis.streaming.agent.AgentContext;
import com.amazon.kinesis.streaming.agent.Logging;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * A collection of utilities ot use with unit tests.
 */
public final class TestUtils {
    private static final Logger LOGGER = Logging.getLogger(TestUtils.class);
    private static final int MIN_FILE_TIME_RESOLUTION = 1000;
    private static AtomicLong atomicCounter = new AtomicLong();

    /**
     * Static initializer that initializes logging.
     * Logging specification is stored in resource file <code>test.log4j.xml</code>
     * at the same path as this class.
     */
    static {
        try (InputStream stream = TestUtils.class.getResourceAsStream("test.log4j.xml")) {
            Logging.initialize(stream, null, null, -1, -1);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * @return A number that increases with every invokation. No two invokations
     *         of this method during the lifetime of the JVM will return the same
     *         number.
     */
    public static long uniqueCounter() {
        return atomicCounter.getAndIncrement();
    }

    /**
     * @return the minimum positive difference between two {@link FileTime}
     *         values. System dependent, though it seems to be 1 second on most
     *         OSes.
     */
    public static long getFileTimeResolution() {
        return MIN_FILE_TIME_RESOLUTION;
    }

    /**
     * Should be used by all unit tests to ensure logging is initialized with
     * the test configuration.
     *
     * @param clazz
     * @return
     */
    public static Logger getLogger(Class<?> clazz) {
        return Logging.getLogger(clazz);
    }

    /**
     * @see #getLogger(Class)
     */
    public static Logger getLogger() {
        return Logging.getLogger(TestUtils.class);
    }

    /**
     * @param items
     * @return a randomly-selected item from provided list
     */
    @SafeVarargs
    public static <T> T pickOne(T... items) {
        return items[ThreadLocalRandom.current().nextInt(items.length)];
    }

    /**
     * Flips the case of some characters in the input (about 50% of characters).
     *
     * @param input
     * @return
     */
    public static String randomCase(String input) {
        if (input != null) {
            StringBuilder output = new StringBuilder();
            for (int i = 0; i < input.length(); ++i) {
                char c = input.charAt(i);
                if (Character.isAlphabetic(c) && ThreadLocalRandom.current().nextDouble() < 0.5) {
                    if (Character.isLowerCase(c))
                        output.append(Character.toUpperCase(c));
                    else if (Character.isUpperCase(c))
                        output.append(Character.toLowerCase(c));
                } else
                    output.append(c);
            }
            return output.toString();
        } else
            return null;

    }

    /**
     * Moves the deleted file to a trash directory to avoid recycling inodes when
     * files are deleted and recreated quickly.
     * This method does not remove the file from the <code>files</code> list.
     * @param file
     * @param trashDir
     * @return new path of the file, or {@code null} if file was not deleted.
     * @throws IOException
     */
    public static Path moveFileToTrash(Path file, Path trashDir) throws IOException {
        if(Files.exists(file)) {
            String deletedName = TestUtils.uniqueCounter() + "-" + file.getFileName();
            Path deleted = trashDir.resolve(deletedName);
            Files.move(file, deleted);
            return deleted;
        } else
            return null;
    }

    /**
     * Recursively delete a directory.
     *
     * @param dir
     */
    public static void deleteDirectory(Path dir) {
        try {
            SimpleFileVisitor<Path> deleter = new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attr) throws IOException {
                    try {
                        Files.delete(file);
                    } catch(IOException e) {
                        LOGGER.debug("Error deleting file {}.", file, e);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException attr) throws IOException {
                    try {
                        Files.delete(dir);
                    } catch(IOException e) {
                        LOGGER.debug("Error deleting directory {}.", dir, e);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    if(file.toString().endsWith("-journal"))
                        return FileVisitResult.CONTINUE;
                    return super.visitFileFailed(file, exc);
                }
            };
            if(Files.exists(dir))
                Files.walkFileTree(dir, deleter);
        } catch (IOException e) {
            Throwables.propagate(e);
        }
    }

    /**
     * Appends a string to a file (opens the file, appends, then closes it).
     * @param text
     * @param file
     */
    public static void appendToFile(String text, Path file) {
        try {
            Files.write(file, StandardCharsets.UTF_8.encode(text).array(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            Throwables.propagate(e);
        }
    }

    public static Configuration getTestConfiguration(Object[]... kvpairs) {
        Map<String, Object> config = new HashMap<>();
        for (int i = 0; i < kvpairs.length; i += 1) {
            config.put(kvpairs[i][0].toString(), kvpairs[i][1]);
        }
        return getTestAgentContext(config);
    }

    /**
     * Creates an {@link AgentContext} instance for tests based on configuration
     * values provided by caller in a {@link Map}.
     * @param config
     * @return
     */
    public static AgentContext getTestAgentContext(Map<String, Object> config) {
        // make copy of map, since we might change it
        config = new HashMap<>(config);
        // ensure minimal valid configuration
        if (!config.containsKey("flows"))
            config.put("flows", Collections.emptyList());
        return new AgentContext(new Configuration(config));
    }

    /**
     * Creates an {@link AgentContext} instance for tests based on configuration
     * values provided by caller as an array of key/value pairs. Example:
     * <code><pre>
     *   testContext = TestUtils.getTestAgentContext(new Object[] {"tailedFiles", Collections.emptyList()}, new Object[] {"checkpointFile", "/tmp/mytest"}});
     * </pre></code>
     * @param kvpairs
     * @return
     */
    public static AgentContext getTestAgentContext(Object[]... kvpairs) {
        Map<String, Object> config = new HashMap<>();
        for (int i = 0; i < kvpairs.length; i += 1) {
            config.put(kvpairs[i][0].toString(), kvpairs[i][1]);
        }
        return getTestAgentContext(config);
    }

    /**
    /**
     * Creates an {@link AgentContext} instance for tests based on configuration
     * values provided by caller as a JSON String. Example:
     * <code><pre>
     *   testContext = TestUtils.getTestAgentContext("{\"tailedFiles\":[],\"checkpointFile\":\"/tmp/mytest\"}");
     * </pre></code>
     * @param json
     * @return
     */
    @SuppressWarnings("unchecked")
    public static AgentContext getTestAgentContext(String json) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return getTestAgentContext((HashMap<String, Object>) mapper.readValue(json,
                    new TypeReference<HashMap<String, Object>>() {}));
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Truncates the file (setting its length to 0).
     * @param path
     * @throws IOException
     */
    public static void truncateFile(Path path) throws IOException {
        FileChannel.open(path, StandardOpenOption.WRITE).truncate(0).close();
    }

    /**
     * Age a file by the given number of <code>seconds</code>.
     * If <code>seconds</code> is negative, the file timestamp
     * will be decreased, and the file will appear newer instead..
     * @param path the file to make appear older
     * @param seconds Number of seconds to make the file appear older by. To
     *                make the file look newer, pass a negative value.
     */
    public static void ageFile(Path path, int seconds) throws IOException {
        long currentTime = Files.getLastModifiedTime(path).toMillis();
        long newTime = currentTime - TimeUnit.SECONDS.toMillis(seconds);
        Files.setLastModifiedTime(path, FileTime.fromMillis(newTime));
    }

    /**
     * Changes the <code>lastModifiedTime</code> attr of given files such that
     * they vary in increasing order. The first of <code>files</code> will have
     * the oldest <code>lastModifiedTime</code>, and the last will have the
     * newest. The remaining files will have their <code>lastModifiedTime</code>
     * set in between spaced by something between 1 and 10 seconds.
     *
     * @param files the files to act upon.
     */
    public static void ensureIncreasingLastModifiedTime(Path... files) {
        final int maxDelta = (int) (2 * getFileTimeResolution());
        final int minDelta = (int) getFileTimeResolution();
        // Go enough in past to avoid setting lastModifiedTime in future
        long fileTime = System.currentTimeMillis() - files.length * maxDelta;
        try {
            for (Path file : files) {
                // TODO: Should we adjust creation time and access as well time
                //       so they don't occur after lastModifiedTime?
                Files.setLastModifiedTime(file, FileTime.fromMillis(fileTime));
                fileTime += ThreadLocalRandom.current().nextInt(minDelta, maxDelta);
            }
        } catch (IOException e) {
            Throwables.propagate(e);
        }
    }

    /**
     * Asserts that both arrays are same length and that the sub-arrays
     * are "equal" to each other.
     * @param array1
     * @param array2
     */
    public static <T> void assert2DArrayEquals(T[][] array1, T[][] array2) {
        Assert.assertEquals(array1.length, array2.length);
        for(int i = 0; i < array1.length; ++i) {
            Assert.assertEquals(array1[i], array2[i]);
        }
    }


    public static Object[][] toTestParameters(Object param) {
        return new Object[][] { { param } };
    }

    public static Object[][] toTestParameters(Object[]... params) {
        Object[][] newParams = new Object[params.length][];
        for(int i = 0; i < params.length; ++i)
            newParams[i] = params[i];
        return newParams;
    }

    public static Object[][] toTestParameters(Object... params) {
        Object[][] newParams = new Object[params.length][];
        for(int i = 0; i < params.length; ++i)
            newParams[i] = new Object[] { params[i] };
        return newParams;
    }

    public static Object[][] concatTestParameters(Object[][] params1, Object[][] params2) {
        if(params1 == null || params1.length == 0)
            return params2 == null ? new Object[0][] : params2;
        else if(params2 == null || params2.length == 0)
            return params1 == null ? new Object[0][] : params1;
        else {
            Object[][] result = new Object[params1.length + params2.length][];
            System.arraycopy(params1, 0, result, 0, params1.length);
            System.arraycopy(params2, 0, result, params1.length, params2.length);
            return result;
        }
    }

    public static Object[][] crossTestParameters(Object[][] params1, Object[][] params2) {
        if(params1 == null || params1.length == 0)
            return params2 == null ? new Object[0][] : params2;
        else if(params2 == null || params2.length == 0)
            return params1 == null ? new Object[0][] : params1;
        else {
            List<Object[]> cross = new ArrayList<>(params1.length * params2.length);
            for(int i = 0; i < params1.length; ++i) {
                for(int j = 0; j < params2.length; ++j) {
                    // concatenate params1[i] and params2[j] in one array
                    Object[] newParams = new Object[params1[i].length + params2[j].length];
                    System.arraycopy(params1[i], 0, newParams, 0, params1[i].length);
                    System.arraycopy(params2[j], 0, newParams, params1[i].length, params2[j].length);
                    cross.add(newParams);
                }
            }
            return cross.toArray(new Object[cross.size()][]);
        }
    }

    public static Object[][] crossTestParameters(Object[][]... params) {
        Object[][] cross = null;
        for(Object[][] param : params)
            cross = crossTestParameters(cross, param);
        return cross;
    }

    public static String getContentMD5(Path p, long bytes) {
        try {
            Preconditions.checkArgument(bytes <= Files.size(p),
                    "Requested bytes to hash (" + bytes + ") " +
                    "exceed the size of file " + p + " (" + Files.size(p) + ")");
            if (bytes > 0) {
                try (FileChannel newChannel = FileChannel.open(p, StandardOpenOption.READ)) {
                    return getMD5(newChannel, bytes);
                }
            }
            return null;
        } catch (FileNotFoundException | NoSuchFileException nsf) {
            return null;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static String getMD5(ReadableByteChannel channel, long size) {
        final int bufferSize = 1024*1024;
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            ByteBuffer buff = ByteBuffer.allocate((int)Math.min(size, bufferSize));
            long remaining = size;
            while (remaining != 0) {
                if (remaining < buff.capacity())
                    buff.limit((int)remaining);
                int read = channel.read(buff);
                if (read == -1)
                    remaining = 0;
                else
                    remaining -= read;
                buff.flip();
                digest.update(buff);
                buff.flip();
            }
            return new BigInteger(1, digest.digest()).toString(16);
        } catch (NoSuchAlgorithmException | IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static long sleep(long millis, double jitter) throws InterruptedException {
        Preconditions.checkArgument(jitter >= 0.0);
        Preconditions.checkArgument(jitter <= 1.0);
        if(millis > 0) {
            long sleepMillis = millis;
            if(jitter > 0.0) {
                sleepMillis = (long) (sleepMillis * ThreadLocalRandom.current().nextDouble(1.0 - jitter, 1.0 + jitter));
            }
            if(sleepMillis > 0) {
                Thread.sleep(sleepMillis);
            }
            return sleepMillis;
        } else
            return 0;
    }

    public static void throwOccasionalError(double rate, RuntimeException error)  {
        if(decide(rate))
            throw error;
    }

    public static boolean decide(double yesRate) {
        Preconditions.checkArgument(yesRate >= 0.0);
        Preconditions.checkArgument(yesRate <= 1.0);
        return yesRate > 0.0
                && (yesRate == 1.0 || ThreadLocalRandom.current().nextDouble() <= yesRate);
    }

    /**
     * A temp file manager to be used in unit/integration tests.
     * It makes it simple to create temp files or temp file paths and to
     * clean them up.
     */
    public static class TestFiles {
        @Getter private Path tmpDir;
        private final Class<?> testClass;
        private final boolean keepAfterTeardown;
        private long lastCreateTime = 0;

        public TestFiles(Class<?> testClass, boolean keep) {
            this.testClass = testClass;
            this.keepAfterTeardown = keep;
            initialize();
        }

        public TestFiles(Class<?> testClass) {
            this(testClass, false);
        }

        public void initialize() {
            try {
                this.tmpDir = Files.createTempDirectory(testClass.getSimpleName());
                if (this.keepAfterTeardown) {
                    LOGGER.info("Temp directory will not be deleted at end of test. Remember to delete manually: {}",
                            tmpDir.toAbsolutePath());
                }
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        teardown();
                    }
                });
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        public void teardown() {
            if (!this.keepAfterTeardown) {
                try {
                    deleteDirectory(tmpDir);
                } catch(Exception e) {
                    LOGGER.debug("Failed while deleting a temp directory: {}", tmpDir, e);
                }
            }
        }

        public Path createTempFile() {
            return createTempFileWithPrefix(null);
        }

        public Path createTempFileWithPrefix(String prefix) {
            try {
                Path tmp = Files.createTempFile(tmpDir, prefix, null);
                lastCreateTime = System.currentTimeMillis();
                return tmp;
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        public Path createTempFileWithName(String name) {
            Path fname = getTempFilePathWithName(name);
            Preconditions.checkState(!Files.exists(fname), "File already exists: " + fname);
            try {
                Files.createFile(fname);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            return fname;
        }

        public Path getTempFilePath() {
            try {
                Path tmp = Files.createTempFile(tmpDir, null, null);
                Files.delete(tmp);
                return tmp;
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        public Path getTempFilePathWithName(String name) {
            return Paths.get(tmpDir.toString(), name);
        }

        public void appendToFile(String text, Path file) {
            TestUtils.appendToFile(text, file);
        }

        public void waitForNewTimestamp() {
            long waitTime = lastCreateTime > 0 ? (lastCreateTime + getFileTimeResolution() - System.currentTimeMillis()) : 0;
            if(waitTime > 0) {
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // just return when interrupted
                }
            }
        }
    }

    /**
     * A class that logs the start/end of each tests so that test results can
     * make more sense in the log file.
     */
    public static class LoggingTestNGListener implements IInvokedMethodListener {

        private String getMethodName(IInvokedMethod m, ITestResult testResult) {
            ITestNGMethod method = m.getTestMethod();
            IClass klass = method.getTestClass();
            StringBuilder sb = new StringBuilder();
            if (m.isTestMethod()) {
                sb.append("Test ");
            } else {
                sb.append("Method ");
            }
            sb.append(klass.getRealClass().getSimpleName())
                .append(".")
                .append(method.getMethodName());
            if (method.getInvocationCount() > 1) {
                sb.append(" (")
                    .append(method.getCurrentInvocationCount() + 1)
                    .append(" of ")
                    .append(method.getInvocationCount())
                    .append(")");
            }
            return sb.toString();
        }

        @Override
        public void beforeInvocation(IInvokedMethod method, ITestResult testResult) {
            LOGGER.info("=== Starting {}", getMethodName(method, testResult));
            int i = 0;
            for (Object param : testResult.getParameters()) {
                LOGGER.info("===== Parameter {}: {}", i++, param);
            }
        }

        @Override
        public void afterInvocation(IInvokedMethod method, ITestResult testResult) {
            if (!testResult.isSuccess()) {
                LOGGER.error("=====  Test Error: ", testResult.getThrowable());
                LOGGER.error("=== {} Failed: {}", getMethodName(method, testResult), testResult.getThrowable()
                        .getMessage());
            } else {
                LOGGER.info("=== {} Succeeded", getMethodName(method, testResult));
            }
        }
    }

    /**
     * Base class to be used for all unit tests to make sure logging is
     * initialized correctly and that test invocations are logged right.
     */
    @Listeners(LoggingTestNGListener.class)
    public static class TestBase {
        protected final Logger logger = Logging.getLogger(getClass());
        protected TestFiles globalTestFiles;
        protected TestFiles testFiles;
        protected Path globalTrashDir;

        @BeforeClass(alwaysRun=true)
        public void setupGlobalTestFiles() {
            globalTestFiles = new TestFiles(getClass());
            try {
                globalTrashDir = Files.createTempDirectory(getClass().getSimpleName() + "-trash");
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        }

        @BeforeMethod(alwaysRun=true)
        public void setupTestFiles() {
            testFiles = new TestFiles(getClass());
        }

        @AfterMethod(alwaysRun=true)
        public void teardownTestFiles(){
            testFiles.teardown();
            testFiles = null;
        }

        @AfterClass(alwaysRun=true)
        public void teardownGlobaleTestFiles(){
            globalTestFiles.teardown();
            globalTestFiles = null;
        }

        public Path moveFileToTrash(Path file) throws IOException {
            return moveFileToTrash(file, globalTrashDir);
        }

        /**
         * Moves the deleted file to a trash directory to avoid recycling inodes when
         * files are deleted and recreated quickly.
         * This method does not remove the file from the <code>files</code> list.
         * @param file
         * @param trashDir
         * @return new path of the file, or {@code null} if file was not deleted.
         * @throws IOException
         */
        public Path moveFileToTrash(Path file, Path trashDir) throws IOException {
            return TestUtils.moveFileToTrash(file, trashDir);
        }
    }
}
