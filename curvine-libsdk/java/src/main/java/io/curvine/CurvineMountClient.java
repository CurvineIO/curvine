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
import io.curvine.proto.GetMountInfoResponse;
import io.curvine.proto.GetMountTableResponse;
import io.curvine.proto.MountInfoProto;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Explicit Java client for Curvine UFS mount lifecycle operations.
 *
 * <p>Mounting is intentionally separate from {@link CurvineLoadClient}: callers decide when
 * cluster-wide UFS metadata is created, updated, or removed.
 */
public final class CurvineMountClient implements Closeable {
    private static final long SUCCESS = 0;

    private final NativeFilesystemHandle filesystem;

    private CurvineMountClient(long nativeHandle) {
        this.filesystem = new NativeFilesystemHandle(nativeHandle);
    }

    public static CurvineMountClient from(FilesystemConf conf) throws IOException {
        Objects.requireNonNull(conf, "conf");
        try {
            long handle = CurvineNative.newFilesystem(conf.toToml());
            if (handle < SUCCESS) {
                throw CurvineException.create((int) handle, "failed to create CurvineMountClient");
            }
            return new CurvineMountClient(handle);
        } catch (IllegalAccessException e) {
            throw new IOException("failed to serialize FilesystemConf", e);
        }
    }

    public static CurvineMountClient from(Configuration conf) throws IOException {
        try {
            return from(new FilesystemConf(conf));
        } catch (IllegalAccessException e) {
            throw new IOException("failed to build FilesystemConf", e);
        }
    }

    /** Create or update a UFS mount. */
    public void mount(MountRequest request) throws IOException {
        Objects.requireNonNull(request, "request");
        filesystem.withOpen(nativeHandle -> {
            long errno = CurvineNative.mount(
                    nativeHandle,
                    request.getUfsPath(),
                    request.getCvPath(),
                    request.getOptions().toByteArray());
            checkError(errno, "mount failed: " + request.getCvPath());
            return null;
        });
    }

    /** Remove the UFS mount at {@code cvPath}. */
    public void unmount(String cvPath) throws IOException {
        String path = requirePath(cvPath, "cvPath");
        filesystem.withOpen(nativeHandle -> {
            long errno = CurvineNative.unmount(nativeHandle, path);
            checkError(errno, "unmount failed: " + path);
            return null;
        });
    }

    /** Find the mount that contains either a Curvine or UFS path. */
    public Optional<MountInfoProto> getMountInfo(String path) throws IOException {
        String checkedPath = requirePath(path, "path");
        return filesystem.withOpen(nativeHandle -> {
            byte[] bytes = CurvineNative.getMountInfo(nativeHandle, checkedPath);
            checkBytes(bytes, "getMountInfo");
            return parseMountInfo(bytes);
        });
    }

    /** List every mount in the server's mount-table order. */
    public List<MountInfoProto> listMounts() throws IOException {
        return filesystem.withOpen(nativeHandle -> {
            byte[] bytes = CurvineNative.getMountTable(nativeHandle);
            checkBytes(bytes, "getMountTable");
            return parseMountTable(bytes);
        });
    }

    @Override
    public void close() throws IOException {
        filesystem.close("close CurvineMountClient failed");
    }

    static Optional<MountInfoProto> parseMountInfo(byte[] bytes) throws IOException {
        GetMountInfoResponse response = GetMountInfoResponse.parseFrom(bytes);
        return response.hasMountInfo() ? Optional.of(response.getMountInfo()) : Optional.empty();
    }

    static List<MountInfoProto> parseMountTable(byte[] bytes) throws IOException {
        return GetMountTableResponse.parseFrom(bytes).getMountTableList();
    }

    private static void checkError(long errno, String message) throws IOException {
        if (errno < SUCCESS) {
            throw CurvineException.create((int) errno, message);
        }
    }

    private static void checkBytes(byte[] bytes, String operation) throws IOException {
        if (bytes == null) {
            throw new CurvineException("native " + operation + " call returned null");
        }
    }

    private static String requirePath(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " cannot be empty");
        }
        return value.trim();
    }
}
