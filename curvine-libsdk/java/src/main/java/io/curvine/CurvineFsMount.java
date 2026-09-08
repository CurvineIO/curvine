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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.curvine.exception.CurvineException;
import io.curvine.proto.FreeResponse;

public class CurvineFsMount {
    private final NativeFilesystemHandle filesystem;

    public static long SUCCESS = 0;

    private static final ThreadLocal<Map<String, String>> CLIENT_CONTEXT =
            ThreadLocal.withInitial(HashMap::new);

    public CurvineFsMount(FilesystemConf conf) throws IOException {
        long nativeHandle;
        try {
            nativeHandle = CurvineNative.newFilesystem(conf.toToml());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        checkError(nativeHandle, "Curvine fs mount error, new curvine client failed, conf: " + conf);
        filesystem = new NativeFilesystemHandle(nativeHandle);
    }

    public static void setClientContext(Map<String, String> context) throws IOException {
        Map<String, String> copy =
                context == null ? Collections.emptyMap() : new HashMap<>(context);
        CLIENT_CONTEXT.set(copy);
        CurvineNative.setClientContext(toJson(copy));
    }

    public static void clearClientContext() throws IOException {
        CLIENT_CONTEXT.remove();
        CurvineNative.clearClientContext();
    }

    public static Map<String, String> getClientContext() {
        return new HashMap<>(CLIENT_CONTEXT.get());
    }

    private static String toJson(Map<String, String> context) {
        StringBuilder builder = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : context.entrySet()) {
            if (!first) {
                builder.append(',');
            }
            first = false;
            builder.append('"')
                    .append(escapeJson(entry.getKey()))
                    .append("\":\"")
                    .append(escapeJson(entry.getValue()))
                    .append('"');
        }
        return builder.append('}').toString();
    }

    private static String escapeJson(String value) {
        String input = value == null ? "" : value;
        StringBuilder builder = new StringBuilder(input.length());
        for (int i = 0; i < input.length(); i++) {
            char ch = input.charAt(i);
            switch (ch) {
                case '\\':
                    builder.append("\\\\");
                    break;
                case '"':
                    builder.append("\\\"");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    if (ch < 0x20) {
                        builder.append(String.format("\\u%04x", (int) ch));
                    } else {
                        builder.append(ch);
                    }
            }
        }
        return builder.toString();
    }

    public void checkError(long errno, String msg) throws IOException {
        if (errno < SUCCESS) {
            throw CurvineException.create((int) errno, msg);
        }
    }

    public void checkError(long errno) throws IOException {
        if (errno < SUCCESS) {
            throw CurvineException.create((int) errno, "lib error");
        }
    }

    public void checkError(Object res) throws IOException {
        if (res == null) {
            throw new CurvineException("lib error");
        }
    }

    public long create(String path, boolean overwrite) throws IOException {
        return filesystem.withOpen(nativeHandle -> {
            long handle = CurvineNative.create(nativeHandle, path, overwrite);
            checkError(handle);
            return handle;
        });
    }

    public long append(String path, long[] tmp) throws IOException {
        return filesystem.withOpen(nativeHandle -> {
            long handle = CurvineNative.append(nativeHandle, path, tmp);
            checkError(handle);
            return handle;
        });
    }

    public void allocChunk(long writerHandle, long[] tmp) throws IOException {
        checkError(CurvineNative.allocChunk(writerHandle, tmp));
    }

    public void write(long writerHandle, long address, int len, long[] tmp) throws IOException {
        checkError(CurvineNative.write(writerHandle, address, len, tmp));
    }

    public void flush(long writerHandle) throws IOException {
        checkError(CurvineNative.flush(writerHandle));
    }

    public void closeWriter(long writerHandle) throws IOException {
        checkError(CurvineNative.closeWriter(writerHandle));
    }

    public long open(String path, long[] tmp) throws IOException {
        return filesystem.withOpen(nativeHandle -> {
            long handle = CurvineNative.open(nativeHandle, path, tmp);
            checkError(handle);
            return handle;
        });
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
        filesystem.close("close CurvineFsMount failed");
    }

    public void mkdir(String path,  boolean createParent) throws IOException {
        filesystem.withOpen(nativeHandle -> {
            checkError(CurvineNative.mkdir(nativeHandle, path, createParent));
            return null;
        });
    }

    public byte[] getFileStatus(String path) throws IOException {
        return filesystem.withOpen(nativeHandle -> {
            byte[] bytes = CurvineNative.getFileStatus(nativeHandle, path);
            checkError(bytes);
            return bytes;
        });
    }

    public byte[] setAttr(String path, SetAttrOpts opts) throws IOException {
        Objects.requireNonNull(opts, "opts");
        return filesystem.withOpen(nativeHandle -> {
            byte[] bytes = CurvineNative.setAttr(nativeHandle, path, opts.toByteArray());
            checkError(bytes);
            return bytes;
        });
    }

    public byte[] listStatus(String path) throws IOException {
        return filesystem.withOpen(nativeHandle -> {
            byte[] bytes = CurvineNative.listStatus(nativeHandle, path);
            checkError(bytes);
            return bytes;
        });
    }

    public void rename(String src, String dst) throws IOException {
        filesystem.withOpen(nativeHandle -> {
            checkError(CurvineNative.rename(nativeHandle, src, dst));
            return null;
        });
    }

    public void delete(String path, boolean recursive) throws IOException {
        filesystem.withOpen(nativeHandle -> {
            checkError(CurvineNative.delete(nativeHandle, path, recursive));
            return null;
        });
    }

    /** Release Curvine metadata and blocks for a path and return the released totals. */
    public FreeResult free(String path, boolean recursive) throws IOException {
        return filesystem.withOpen(nativeHandle -> {
            byte[] bytes = CurvineNative.free(nativeHandle, path, recursive);
            checkError(bytes);
            return FreeResult.fromProto(FreeResponse.parseFrom(bytes).getRes());
        });
    }

    public byte[] getFilesystemInfo() throws IOException {
        return filesystem.withOpen(nativeHandle -> {
            byte[] bytes = CurvineNative.getFilesystemInfo(nativeHandle);
            checkError(bytes);
            return bytes;
        });
    }

    /**
     * @deprecated renamed to {@link #getFilesystemInfo()}; this RPC returns
     * whole-filesystem statistics, not master-process information. Kept for
     * source compatibility and will be removed in a future release.
     */
    @Deprecated
    public byte[] getMasterInfo() throws IOException {
        return getFilesystemInfo();
    }

    public byte[] getMountInfo(String path) throws IOException {
        return filesystem.withOpen(nativeHandle -> {
            byte[] bytes = CurvineNative.getMountInfo(nativeHandle, path);
            checkError(bytes);
            return bytes;
        });
    }

    public Optional<String> togglePath(String path, boolean checkCache) throws IOException {
        return filesystem.withOpen(nativeHandle -> {
            String ufsPath = CurvineNative.togglePath(nativeHandle, path, checkCache);
            return Optional.ofNullable(ufsPath);
        });
    }
}
