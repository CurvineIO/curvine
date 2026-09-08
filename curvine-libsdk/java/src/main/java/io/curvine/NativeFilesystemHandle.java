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

import io.curvine.exception.CurvineException;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Serializes native filesystem destruction with calls that use its handle. */
final class NativeFilesystemHandle {
    @FunctionalInterface
    interface Operation<T> {
        T run(long nativeHandle) throws IOException;
    }

    private final long nativeHandle;
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private boolean closed;

    NativeFilesystemHandle(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    <T> T withOpen(Operation<T> operation) throws IOException {
        lifecycleLock.readLock().lock();
        try {
            if (closed) {
                throw new IOException("Curvine filesystem is closed");
            }
            return operation.run(nativeHandle);
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    void close(String message) throws IOException {
        lifecycleLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            long errno = CurvineNative.closeFilesystem(nativeHandle);
            if (errno < 0) {
                throw CurvineException.create((int) errno, message);
            }
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }
}
