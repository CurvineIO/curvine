// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.curvine;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

import javax.annotation.Nonnull;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CurvineInputStream extends FSInputStream {
    private long nativeHandle;
    private byte[] oneByte = new byte[1];
    private final CurvineFsMount libFs;
    private boolean closed;
    private long pos;
    private final long fileSize;

    private final long[] tmp = new long[] {0, 0};
    private ByteBuffer buffer;

    private FileSystem.Statistics statistics;

    public CurvineInputStream(CurvineFsMount libFs, long nativeHandle, long fileSize, FileSystem.Statistics statistics) {
        this.libFs = libFs;
        this.nativeHandle = nativeHandle;
        this.fileSize = fileSize;
        this.statistics = statistics;
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new EOFException("stream has been closed!");
        }
    }

    @Override
    public int read() throws IOException {
        checkClosed();
        if (read(oneByte, 0, oneByte.length) == -1) {
            return -1;
        }
        return oneByte[0] & 0xff;
    }

    @Override
    public int read(@Nonnull byte[] buf) throws IOException {
        checkClosed();
        return read(buf, 0, buf.length);
    }

    @Override
    public int read(@Nonnull byte[] buf, int offset, int length) throws IOException {
        if (length == 0) {
            return 0;
        } else if (pos < 0) {
            throw new IOException("position is negative");
        } else if (pos >= fileSize) {
            return -1;
        }

        checkClosed();

        if (buf.length - offset < length) {
            throw new IndexOutOfBoundsException(
                    FSExceptionMessages.TOO_MANY_BYTES_FOR_DEST_BUFFER
                            + ": request length=" + length
                            + ", with offset ="+ offset
                            + "; buffer capacity =" + (buf.length - offset));
        }

        if (buffer == null || !buffer.hasRemaining()) {
            libFs.read(nativeHandle, tmp);
            if (tmp[1] <= 0) { // read end
                return -1;
            }
            buffer = CurvineNative.createBuffer(tmp);
            buffer.position((int) tmp[1]);
            buffer.flip();
        }

        int size = Math.min(buffer.remaining(), length);
        buffer.get(buf, offset, size);
        pos += size;

        if (statistics != null) {
            statistics.incrementBytesRead(size);
        }

        return size;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        super.close();
        try {
            libFs.closeReader(nativeHandle);
        } finally {
            nativeHandle = 0;
            buffer = null;
            closed = true;
            oneByte = null;
        }

    }

    @Override
    public void seek(long targetPos) throws IOException {
        checkClosed();

        if (targetPos < 0L) {
            throw new IOException("Cannot seek to negative offset");
        } else if (targetPos == pos) {
            return;
        }

        long toSkip = pos - targetPos;
        if (buffer != null && toSkip >= 0 && toSkip <= buffer.remaining()) {
            buffer.position((int) (buffer.position() + toSkip));
        } else {
            // Discard buffer data.
            buffer = null;
            libFs.seek(nativeHandle, targetPos);
        }
        pos = targetPos;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0) {
            return n < 0 ? -1 : 0;
        }

        long toSkip = Math.min(n, fileSize - pos);
        seek(pos + toSkip);
        return toSkip;
    }

    @Override
    public long getPos() {
        return pos;
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
        throw new IOException("This method seekToNewSource is not supported.");
    }

    @Override
    public int available() {
        return (int) (fileSize - pos);
    }
}
