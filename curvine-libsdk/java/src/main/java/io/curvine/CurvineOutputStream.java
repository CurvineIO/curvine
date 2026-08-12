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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

import org.apache.hadoop.fs.Syncable;

public class CurvineOutputStream extends OutputStream implements Syncable {
    private long nativeHandle;
    private volatile byte[] oneByte;
    private final CurvineFsMount libFs;
    private boolean closed;

    private ByteBuffer buffer;

    private final long[] tmp = new long[] {0, 0};

    private long pos = 0;

    public CurvineOutputStream(CurvineFsMount libFs, long nativeHandle, long pos) {
        this.libFs = libFs;
        this.nativeHandle = nativeHandle;
        this.oneByte = new byte[1];
        this.pos = pos;
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("Stream has been closed");
        }
    }

    @Override
    public void write(int b) throws IOException {
        checkClosed();
        oneByte[0] = (byte) b;
        write(oneByte, 0, oneByte.length);
    }

    @Override
    public void write(@Nonnull byte[] buf) throws IOException {
        checkClosed();
        write(buf, 0, buf.length);
    }

    @Override
    public void write(@Nonnull byte[] buf, int offset, int length) throws IOException {
        checkClosed();
        if (length == 0) {
            return;
        }

        if (offset > buf.length || offset + length > buf.length) {
            throw new IndexOutOfBoundsException("offset is out of buffer length.");
        }

        while (length > 0) {
            ensureWritableBuffer();

            int writeLen = Math.min(length, buffer.remaining());
            buffer.put(buf, offset, writeLen);
            offset += writeLen;
            length -= writeLen;
            pos += writeLen;
        }
    }

    private void flushBuffer() throws IOException {
        if (buffer != null && buffer.position() > 0) {
            libFs.write(nativeHandle, CurvineNative.getAddress(buffer), buffer.position(), tmp);
            buffer = CurvineNative.createBuffer(tmp);
        }
    }

    private void ensureWritableBuffer() throws IOException {
        if (buffer == null) {
            libFs.allocChunk(nativeHandle, tmp);
            buffer = CurvineNative.createBuffer(tmp);
        }

        if (!buffer.hasRemaining()) {
            flushBuffer();
            if (buffer == null || !buffer.hasRemaining()) {
                throw new IOException("write buffer has no remaining capacity after flush");
            }
        }
    }

    @Override
    public void flush() throws IOException {
        checkClosed();
        flushBuffer();
        libFs.flush(nativeHandle);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            flushBuffer();
            libFs.closeWriter(nativeHandle);
        } finally {
            nativeHandle = 0;
            buffer = null;
            closed = true;
            this.oneByte = null;
        }
    }

    public long pos() {
        return pos;
    }

    @Override
    public void hflush() throws IOException {
        flush();
    }

    @Override
    public void hsync() throws IOException {
        // HDFS protocol requires hsync() to guarantee data durability
        // Flush all buffered data to ensure persistence
        flush();
    }
}
