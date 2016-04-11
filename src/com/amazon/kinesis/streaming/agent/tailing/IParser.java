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

import java.util.Map;

/**
 * The inteface for a record parser that defines the protocol or parsing records
 * from an open channel.
 *
 * To start reading from a file channel, the user of this interface needs to call
 * {@link #setCurrentFile(TrackedFile, boolean) setCurrentFile} with an open file.
 * This file can be called at a later time as needed as the underlying file rotates.
 * After that, successive calls to {@link #readRecord()} will yield the successive
 * records read from the file channel. When no more complete records can be
 * read from the underlying channel, {@code null} will be returned. The caller
 * is then responsible to figure out what's the reason (e.g. simple EOF or
 * rotation happened), and either keep reading from the same file or switching
 * to another file by calling {@code setCurrentFile(TrackedFile, boolean) setCurrentFile}
 * again.
 *
 * @param <R> The record type that this parser recognizes and produces.
 */
public interface IParser<R extends IRecord> {
    /**
     * Sets up the parser to read records from the given {@code file}, and
     * advances to the initial position specified in the
     * {@link FileFlow#getInitialPosition()} configuration parameters.
     * Any existing parsing state is discarded (e.g. buffered data from a
     * previous file).
     *
     * If {@code file == null}, this is equivalent to calling {@link #stopParsing()}.
     *
     * @param file {@code null} or a file to parse data from. The file should have
     *             @{code file.isOpen() == true}, and its current offset is
     *             assumed to be at the beginning of a record (assumed but not
     *             explicitly checked).
     * @return {@code true} if the new file is opened successfully, or
     *         {@code false} if {@code file == null} or if an error occurred
     *         while opening the new file. If {@code false} is returned, it
     *         means that parsing was stopped.
     */
    boolean startParsingFile(TrackedFile file);

    /**
     * Sets up the parser to read records from the given {@code file}, assuming
     * no discontinuity in the data. Any buffered data from a previous
     * file will be preserved.
     * Call this method if the new file is the "same" as the current file
     * or if the current file was successfully parsed to its end.
     *
     * If there's no file currently being parsed, this method is identical to
     * {@link #switchParsingToFile(TrackedFile)}.
     *
     * If {@code file == null}, this is equivalent to calling {@link #stopParsing()}.
     * @param file {@code null} or a file to continue parsing data from.
     *             If there's already a file being parsed, continuity is assumed
     *             and is not explicitly checked.
     * @return {@code true} if the new file is opened successfully, or
     *         {@code false} if {@code file == null} or if an error occurred
     *         while opening the new file. If {@code false} is returned, it
     *         means that parsing was stopped.
     */
    boolean continueParsingWithFile(TrackedFile file);

    /**
     * Sets up the parser to read records from the given {@code file}, assuming
     * there's no continuity in the data between the current file (if any) and
     * the new file. Any buffered data from a previous file will be
     * discarded.
     *
     * If {@code file == null}, this is equivalent to calling {@link #stopParsing()}.
     *
     * @param file {@code null} or a file to start parsing data from.
     * @return {@code true} if the new file is opened successfully, or
     *         {@code false} if {@code file == null} or if an error occurred
     *         while opening the new file. If {@code false} is returned, it
     *         means that parsing was stopped.
     */
    boolean switchParsingToFile(TrackedFile file);


    /**
     * @return {@code true} if there's a file currently being parsed, and
     *         {@code false} otherwise.
     */
    boolean isParsing();

    /**
     * Forcibly stops parsing the current file (if any), discarding any buffered
     * data. Subsequent calls to {@link #readRecord()} will return {@code null}.
     * @param reason message describing the reason we're stopping.
     * @return {@code true} if there was a file being parsed, and {@code false}
     *         otherwise.
     */
    boolean stopParsing(String reason);

    /**
     * @return The current open file being parsed by this instance.
     */
    public TrackedFile getCurrentFile();

    /**
     * @return The number of bytes remaining in the internal buffer.
     */
    public int bufferedBytesRemaining();

    /**
     * NOTE: A {@code null} return should indicate to the caller that we reached
     *       the end of file for the underlying channel. It's up to the caller to
     *       decide what to do: whether to wait for more data to be written to
     *       current file or to open a new file (in case of rotation e.g.).
     * @return The next record read from the current open file, or
     *         {@code null} if no COMPLETE records could be read from the
     *         underlying channel at this time.
     */
    public R readRecord();

    /**
     * @return {@code true} if the parser has read all bytes found in the
     *         current file, and {@code false} otherwise. The return value might
     *         change over time if more data is written to the file.
     */
    public boolean isAtEndOfCurrentFile();

    public Map<String, Object> getMetrics();
}
