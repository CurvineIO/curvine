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

package io.curvine.bench;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class NIOFileOutputStream extends OutputStream {
    private final RandomAccessFile file;
    private final FileChannel channel;
    private final ByteBuffer buffer;

    public NIOFileOutputStream(File path, int chunkSize) throws IOException {
        file = new RandomAccessFile(path, "rw");
        channel = file.getChannel();
        buffer = ByteBuffer.allocateDirect(chunkSize);
    }

    @Override
    public void write(int b) throws IOException {
        throw new IOException();
    }

    @Override
    public void write(byte[] b) throws IOException {
       write(b, 0, b.length);
    }

    private void flushBuffer() throws IOException {
        if (buffer.position() > 0) {
            buffer.flip();
            channel.write(buffer);
            buffer.clear();
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        while (len > 0) {
            if (!buffer.hasRemaining()) {
                flushBuffer();
            }

            int writeLen = Math.min(len, buffer.remaining());
            buffer.put(b, off, writeLen);
            off += writeLen;
            len -= writeLen;
        }
    }

    @Override
    public void close() throws IOException {
        flushBuffer();
        channel.close();
        file.close();
    }
}
