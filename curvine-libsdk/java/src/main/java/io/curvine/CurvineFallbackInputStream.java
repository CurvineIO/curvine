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

import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.EOFException;
import java.io.IOException;
import java.util.function.Supplier;

public class CurvineFallbackInputStream extends FSInputStream {
    public static final Logger LOGGER = LoggerFactory.getLogger(CurvineFallbackInputStream.class);

    private final FSInputStream cvInputStream;

    private FSDataInputStream ufsInputStream;

    private final Supplier<FSDataInputStream> ufsInputStreamSupplier;

    private final byte[] oneByte = new byte[1];

    private boolean closed;

    public CurvineFallbackInputStream(FSInputStream cvInputStream, Supplier<FSDataInputStream> ufsInputStreamSupplier) {
        this.cvInputStream = cvInputStream;
        this.ufsInputStreamSupplier = ufsInputStreamSupplier;
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
        if (ufsInputStream != null) {
            return ufsInputStream.read(buf, offset, length);
        }
        try {
            return cvInputStream.read(buf, offset, length);
        } catch (IOException e) {
            LOGGER.warn("An error occurred while reading data from Curvine." +
                    " Use ufs to continue reading data. pos = {}", cvInputStream.getPos(), e);
            ufsInputStream = ufsInputStreamSupplier.get();
            ufsInputStream.seek(cvInputStream.getPos());
            return ufsInputStream.read(buf, offset, length);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            if (cvInputStream != null) {
                try {
                    cvInputStream.close();
                } catch (IOException e) {
                    if (ufsInputStream == null) {
                        throw e;
                    }
                }
            }

            if (ufsInputStream != null) {
                ufsInputStream.close();
            }
        } finally {
            closed = true;
        }
    }

    @Override
    public void seek(long targetPos) throws IOException {
        checkClosed();
        if (ufsInputStream != null) {
            ufsInputStream.seek(targetPos);
        } else {
            cvInputStream.seek(targetPos);
        }
    }

    @Override
    public long skip(long n) throws IOException {
        checkClosed();
        if (ufsInputStream != null) {
            return ufsInputStream.skip(n);
        } else {
            return cvInputStream.skip(n);
        }
    }

    @Override
    public long getPos() throws IOException {
        if (ufsInputStream != null) {
            return ufsInputStream.getPos();
        } else {
            return cvInputStream.getPos();
        }
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
        throw new IOException();
    }

    @Override
    public int available() throws IOException {
        if (ufsInputStream != null) {
            return ufsInputStream.available();
        } else {
            return cvInputStream.available();
        }
    }
}
