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
package com.amazon.kinesis.streaming.agent;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.IllegalBlockingModeException;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;

/**
 * Provides utility methods for working with {@link ByteBuffer}s. Methods are
 * generally optimized to avoid making unnecessary copies when using
 * byte[]-backed buffers. None of the methods in this class will modify a
 * buffer's position; all will respect the buffer position and limit.
 */
public final class ByteBuffers {
    private static final int BUFFER_SIZE = 0x1000;

    /**
     * @param input
     * @return the input string with tab, newline and carriage return characters
     * converted to literals: {@code \t}, {@code \n} and {@code \r} respectively.
     */
    public static String visibleWhiteSpaces(CharSequence input) {
        if (input == null)
            return null;
        StringBuffer output = new StringBuffer();
        for(int i = 0; i < input.length(); ++i) {
            char c = input.charAt(i);
            switch(c) {
            case '\t':
                output.append("\\t");
                break;
            case '\n':
                output.append("\\n");
                output.append(c);
                break;
            case '\r':
                output.append("\\r");
                break;
            default:
                output.append(c);
            }
        }
        return output.toString();
    }

    /**
     * Returns an InputStream that is backed by the same data as the provided
     * ByteBuffer {@code buffer}, but has an independent mark and position.
     * Functionally the same as
     * {@code new ByteBufferInputStream(buffer.duplicate())}, but if the
     * provided buffer has an accessible backing array, the byte array itself
     * will be directly wrapped in a ByteArrayInputStream to avoid a layer of
     * indirection.
     * <p>
     * In either case, the returned InputStream will have an
     * {@link InputStream#available() available()} method that returns the exact
     * number of bytes remaining.
     */
    public static InputStream asInputStream(ByteBuffer buffer) {
        if (buffer.hasArray()) {
            return new ByteArrayInputStream(buffer.array(),
                    buffer.arrayOffset() + buffer.position(),
                    buffer.remaining());
        } else {
            return new ByteBufferInputStream(buffer.duplicate());
        }
    }

    /**
     * Copies all remaining bytes from the byte buffer to the output stream.
     * Does not modify the buffer or flush or close the output stream.
     *
     * @throws IOException
     *             if an I/O error occurs when writing to the stream
     */
    public static void copy(ByteBuffer buffer, OutputStream out) throws IOException {
        if (buffer.hasArray()) {
            out.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
                    buffer.remaining());
        } else {
            byte[] arr = new byte[BUFFER_SIZE];
            ByteBuffer in = buffer.duplicate();
            while (in.hasRemaining()) {
                int len = Math.min(in.remaining(), arr.length);
                in.get(arr, 0, len);
                out.write(arr, 0, len);
            }
        }
    }

    /**
     * Return a ByteBuffer representation of the String per given charset
     * @param str
     * @param charset
     */
    public static ByteBuffer fromString(String str, Charset charset) {
        return ByteBuffer.wrap(str.getBytes(charset));
    }

    /**
     * Read the remaining data in the provided ByteBuffer into a new byte array
     * and return it. The buffer's position will not be modified.
     *
     * @param buffer
     * @return The remaining contents of {@code buffer} as a new byte array.
     */
    public static byte[] toArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }

    /**
     * Return the result of decoding the remaining contents of the provided
     * ByteBuffer. The buffer's position will not be modified.
     * @param buffer
     * @param charset
     */
    public static String toString(ByteBuffer buffer, Charset charset) {
        return toString(buffer, buffer.remaining(), charset);
    }

    /**
     * Returns a String obtained by decoding {@code size} bytes or
     * remaining bytes in the ByteBuffer, whichever is lower, starting from the
     * position in the ByteBuffer. The buffer's position will not be modified.
     * @param buffer
     * @param size
     *            The number of bytes starting from the position of the buffer
     *            to convert to a String. If this is more than the remaining
     *            bytes in the buffer, only the remaining bytes will be
     *            converted to a String.
     * @param charset
     */
    public static String toString(ByteBuffer buffer, int size, Charset charset) {
        int arraySize = Math.min(buffer.remaining(), size);
        if (buffer.hasArray()) {
            return new String(buffer.array(),
                    buffer.arrayOffset() + buffer.position(),
                    arraySize, charset);
        } else {
            byte[] data = new byte[arraySize];
            buffer.duplicate().get(data);
            return new String(data, charset);
        }
    }

    /**
     * Reads a sequence of bytes from a channel into a newly allocated buffer,
     * starting at the given file position. This method does not modify the
     * channel position or close the channel.
     *
     * @param channel
     *            The FileChannel to read from. See
     *            {@link FileChannel#read(ByteBuffer, long)}.
     * @param fileOffset
     *            The file position at which reading is to begin
     * @param bytesToRead
     *            The number of bytes to read past the offset
     * @return A newly allocated ByteBuffer with the position set to zero
     * @throws EOFException
     *             if EOF is reached before reading the requested number of
     *             bytes
     * @throws IOException
     *             if reading from the channel fails for any reason
     */
    public static ByteBuffer readFully(FileChannel channel, long fileOffset, int bytesToRead)
            throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(bytesToRead);
        int totalBytesRead = 0;
        while (totalBytesRead < bytesToRead) {
            int bytesRead = channel.read(buffer, fileOffset + totalBytesRead);
            if (bytesRead < 0) {
                throw new EOFException("Expected to read " + bytesToRead + " bytes; only read "
                        + totalBytesRead + " before EOF");
            }
            totalBytesRead += bytesRead;
        }
        buffer.flip();
        return buffer;
    }

    /**
     * Reads a sequence of bytes from a channel into the given buffer. Acts like
     * {@link ReadableByteChannel#read(ByteBuffer)}, except it guarantees that
     * the buffer will be filled or an exception thrown.
     *
     * @param channel
     *            The channel to read from. The channel must not be in
     *            non-blocking mode, or else
     *            {@link IllegalBlockingModeException} may be thrown.
     * @param dst
     *            The buffer to fill with bytes from the channel. The position
     *            of the buffer will be left where reading finished; generally,
     *            after reading finishes, you will call
     *            {@link ByteBuffer#flip()} to read data out of it.
     * @return The number of bytes read into the buffer--on a successful return,
     *         this will always be the number of {@code remaining} bytes on the
     *         {@code dst} buffer before this method was called.
     * @throws EOFException
     *             if EOF is reached before filling the buffer
     * @throws IOException
     *             if reading from the channel fails for any reason
     */
    public static int readFully(ReadableByteChannel channel, ByteBuffer dst)
            throws IOException {
        int bytesToRead = dst.remaining();
        while (dst.hasRemaining()) {
            int bytesRead = channel.read(dst);
            if (bytesRead == 0) {
                throw new IllegalBlockingModeException();
            } else if (bytesRead < 0) {
                throw new EOFException("Expected to read " + bytesToRead + " bytes; only read "
                        + (bytesToRead - dst.remaining()) + " before EOF");
            }
        }
        return bytesToRead;
    }

    /**
     * Returns a view of the original buffer between {@code [offset, offset + length)}.
     * This is different from {@link ByteBuffer#wrap(byte[], int, int)} in that
     * the position of the new buffer is 0, and it starts at the given {@code offset}.
     * The limit of the new buffer is {@code length}. The capacity is what
     * remains of the original buffer after offset.
     *
     * @param original
     * @param offset
     * @param length
     * @return A view of the original buffer starting at {@code offset} (position 0)
     *         and with a limit == to {@code length}.
     */
    public static ByteBuffer getPartialView(ByteBuffer original, int offset, int length) {
        int oldPosition = original.position();
        original.position(offset);
        ByteBuffer view = original.slice();
        view.limit(length);
        original.position(oldPosition);
        return view;
    }

    private ByteBuffers() {
        throw new UnsupportedOperationException("Should never be called");
    }

    /**
     * Advances the buffer current position to be at the index following the
     * next newline, or else the end of the buffer if there is no final newline.
     *
     * @return {@code position} of the buffer at the starting index of the next newline;
     *         {@code -1} if the end of the buffer was reached.
     */
    public static int advanceBufferToNextLine(ByteBuffer buffer) {
        while (buffer.hasRemaining()) {
            if (buffer.get() == Constants.NEW_LINE) {
                return buffer.position();
            }
        }
        return -1;
    }
}
