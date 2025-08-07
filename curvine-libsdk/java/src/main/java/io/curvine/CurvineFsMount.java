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
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

public class CurvineFsMount {
    private final long nativeHandle;

    public static long SUCCESS = 0;

    public CurvineFsMount(FilesystemConf conf) throws IOException {
        try {
            nativeHandle = CurvineNative.newFilesystem(conf.toToml());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        checkError(nativeHandle, "Curvine fs mount error, new curvine client failed, conf: " + conf);
    }

    public void checkError(long errno, String msg) throws IOException {
        if (errno < SUCCESS) {
            throw new CurvineException((int) errno, msg);
        }
    }

    public void checkError(long errno) throws IOException {
        if (errno < SUCCESS) {
            throw new CurvineException((int) errno, "lib error");
        }
    }

    public void checkError(Object res) throws IOException {
        if (res == null) {
            throw new CurvineException("lib error");
        }
    }

    public long create(String path, boolean overwrite) throws IOException {
        long handle = CurvineNative.create(nativeHandle, path, overwrite);
        checkError(handle);
        return handle;
    }

    public long append(String path, long[] tmp) throws IOException {
        long handle = CurvineNative.append(nativeHandle, path, tmp);
        checkError(handle);
        return handle;
    }

    public void write(long writerHandle, ByteBuffer buffer) throws IOException {
        long res = CurvineNative.write(writerHandle, ((DirectBuffer) buffer).address(), buffer.position());
        checkError(res);
    }

    public void flush(long writerHandle) throws IOException {
        checkError(CurvineNative.flush(writerHandle));
    }

    public void closeWriter(long writerHandle) throws IOException {
        checkError(CurvineNative.closeWriter(writerHandle));
    }

    public long open(String path, long[] tmp) throws IOException {
        long handle = CurvineNative.open(nativeHandle, path, tmp);
        checkError(handle);
        return handle;
    }

    public void read(long nativeHandle, long[] tmp) throws IOException {
        long res = CurvineNative.read(nativeHandle, tmp);
        checkError(res);
    }

    public void seek(long nativeHandle, long pos) throws IOException {
        long res = CurvineNative.seek(nativeHandle, pos);
        checkError(res);
    }

    public void closeReader(long readerHandle) throws IOException {
        checkError(CurvineNative.closeReader(readerHandle));
    }

    public void close() throws IOException {
        checkError(CurvineNative.closeFilesystem(nativeHandle));
    }

    public void mkdir(String path,  boolean createParent) throws IOException {
        checkError(CurvineNative.mkdir(nativeHandle, path, createParent));
    }

    public byte[] getFileStatus(String path) throws IOException {
        byte[] bytes = CurvineNative.getFileStatus(nativeHandle, path);
        checkError(bytes);
        return bytes;
    }

    public byte[] listStatus(String path) throws IOException {
        byte[] bytes = CurvineNative.listStatus(nativeHandle, path);
        checkError(bytes);
        return bytes;
    }

    public void rename(String src, String dst) throws IOException {
        checkError(CurvineNative.rename(nativeHandle, src, dst));
    }

    public void delete(String path, boolean recursive) throws IOException {
        checkError(CurvineNative.delete(nativeHandle, path, recursive));
    }

    public byte[] getMasterInfo() throws IOException {
        byte[] bytes = CurvineNative.getMasterInfo(nativeHandle);
        checkError(bytes);
        return bytes;
    }

    public byte[] getMountPoint(String path) throws IOException {
        byte[] bytes = CurvineNative.getMountPoint(nativeHandle, path);
        checkError(bytes);
        return bytes;
    }
}
