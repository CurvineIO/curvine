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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class NIOFileInputStream extends InputStream  {
    private final RandomAccessFile file;
    private final FileChannel channel;
    private final ByteBuffer buffer;

    public NIOFileInputStream(File path, int chunkSize) throws IOException {
        file = new RandomAccessFile(path, "r");
        channel = file.getChannel();
        buffer = ByteBuffer.allocateDirect(chunkSize);
        buffer.flip();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (!buffer.hasRemaining()) {
            buffer.clear();
            channel.read(buffer);
            buffer.flip();
        }
        int size = Math.min(buffer.remaining(), len);
        buffer.get(b, off, size);
        return size;
    }

    @Override
    public void close() throws IOException {
        channel.close();
        file.close();
    }

    @Override
    public int read() throws IOException {
       throw new IOException();
    }
}
