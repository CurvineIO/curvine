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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryNotEmptyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.curvine.exception.CurvineException;
import io.curvine.proto.FileStatusProto;
import io.curvine.proto.GetFileStatusResponse;
import io.curvine.proto.GetFilesystemInfoResponse;
import io.curvine.proto.GetMountInfoResponse;
import io.curvine.proto.ListStatusResponse;
import io.curvine.proto.MountInfoProto;

/****************************************************************
 * Implement the Hadoop FileSystem API for Curvine
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class CurvineFileSystem extends FileSystem {
    public static final Logger LOGGER = LoggerFactory.getLogger(CurvineFileSystem.class);

    /**
     * Global cache for CurvineFsMount instances to avoid creating too many Tokio runtimes.
     * Key: master_addrs plus client mode (enable_unified_fs / enable_rust_read_ufs).
     * Native conf is frozen at mount construction, so different modes cannot share a handle.
     * Value: CachedMount containing the shared CurvineFsMount and reference count
     */
    private static final ConcurrentHashMap<String, CachedMount> MOUNT_CACHE = new ConcurrentHashMap<>();

    private static class CachedMount {
        final CurvineFsMount mount;
        int refCount;
        private boolean closed;

        CachedMount(CurvineFsMount mount) {
            this.mount = mount;
            this.refCount = 1;
        }

        /**
         * Close the underlying mount exactly once; later calls are no-ops.
         * The lock makes it idempotent against concurrent closes from multiple
         * CurvineFileSystem instances sharing the mount, so the native handle
         * is never released twice (double free). Failures are propagated to
         * the caller so an explicit close can never silently leak the mount.
         */
        synchronized void closeOnce() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            mount.close();
        }
    }

    private CurvineFsMount libFs;
    private FilesystemConf filesystemConf;
    private Path workingDir;
    private URI uri;
    private String cacheKey;  // Key used for mount cache lookup

    public final static String SCHEME = "cv";

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public URI getUri() {
        return this.uri;
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
        workingDir = newDir;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    public FilesystemConf getFilesystemConf() {
        return filesystemConf;
    }

    public static void setClientContext(Map<String, String> context) throws IOException {
        CurvineFsMount.setClientContext(context);
    }

    public static void clearClientContext() throws IOException {
        CurvineFsMount.clearClientContext();
    }

    public static Map<String, String> getClientContext() {
        return CurvineFsMount.getClientContext();
    }

    String getMasterAddrs(String name) throws IOException {
        String key = String.format("%s.%s.master_addrs", FilesystemConf.PREFIX, name);
        String addrs = getConf().get(key);
        if (StringUtils.isEmpty(addrs)) {
            throw new IOException(key + " not set");
        }
        return FilesystemConf.normalizeCsv(addrs);
    }

    /**
     * Resolve master addresses for native client:
     * <ol>
     *   <li>{@code fs.cv.<authority>.master_addrs} when URI has authority</li>
     *   <li>otherwise {@code fs.cv.master_addrs} already loaded by {@link FilesystemConf}</li>
     * </ol>
     */
    private void resolveMasterAddrs(String authority) throws IOException {
        if (StringUtils.isNotEmpty(authority)) {
            String authorityKey = String.format("%s.%s.master_addrs", FilesystemConf.PREFIX, authority);
            String authorityAddrs = getConf().get(authorityKey);
            if (StringUtils.isNotEmpty(authorityAddrs)) {
                filesystemConf.master_addrs = FilesystemConf.normalizeCsv(authorityAddrs);
                return;
            }
            if (StringUtils.isNotEmpty(filesystemConf.master_addrs)) {
                return;
            }
            throw new IOException(authorityKey + " not set, and "
                    + FilesystemConf.PREFIX + ".master_addrs not set");
        }
        if (StringUtils.isEmpty(filesystemConf.master_addrs)) {
            throw new IOException(FilesystemConf.PREFIX + ".master_addrs not set");
        }
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);
        String authority = name.getAuthority();
        try {
            filesystemConf = new FilesystemConf(conf);
            resolveMasterAddrs(authority);
            if (StringUtils.isEmpty(authority)) {
                authority = "/";
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }

        this.uri = URI.create(name.getScheme() + "://" + authority);
        this.workingDir = getHomeDirectory();

        this.cacheKey = mountCacheKey(filesystemConf);
        this.libFs = getOrCreateMount(filesystemConf);
    }

    /**
     * Isolate native mounts by the exact configuration frozen into native.
     *
     * <p>{@code CurvineFsMount} passes {@code conf.toToml()} to
     * {@code CurvineNative.newFilesystem}, so the serialized config is what
     * actually defines a mount. A hand-maintained field list silently missed
     * fields and let two different configs share one native mount. Hashing the
     * serialized form keeps the key correct by construction and avoids putting
     * credentials or addresses in log lines.
     */
    static String mountCacheKey(FilesystemConf conf) {
        try {
            byte[] toml = conf.toToml().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            return hex(MessageDigest.getInstance("SHA-256").digest(toml));
        } catch (IllegalAccessException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("cannot derive mount cache key", e);
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    /**
     * Get or create a cached CurvineFsMount instance.
     * Lookup and refcount update both happen under the cache lock: an entry that is
     * still in the cache is always open, so no caller can ever observe a closed
     * mount (no use-after-free). One instance is created per cache key (Master + client mode).
     */
    private CurvineFsMount getOrCreateMount(FilesystemConf conf) throws IOException {
        String key = mountCacheKey(conf);

        synchronized (MOUNT_CACHE) {
            CachedMount cached = MOUNT_CACHE.get(key);
            if (cached != null) {
                cached.refCount++;
                LOGGER.debug("Reusing cached CurvineFsMount for {}, refCount={}", key, cached.refCount);
                return cached.mount;
            }

            LOGGER.info("Creating new cached CurvineFsMount for {}", key);
            CurvineFsMount newMount = new CurvineFsMount(conf);
            MOUNT_CACHE.put(key, new CachedMount(newMount));
            return newMount;
        }
    }

    private String formatPath(Path path) {
        return makeQualified(path).toUri().getPath();
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        if (statistics != null) {
            statistics.incrementReadOps(1);
        }

        long[] tmp = new long[] {0, 0};
        long nativeHandle = libFs.open(formatPath(path), tmp);
        FSInputStream inputStream = new CurvineInputStream(libFs, nativeHandle, tmp[0], statistics);
        return new FSDataInputStream(inputStream);
    }

    @Override
    public FSDataOutputStream create(
            Path path,
            FsPermission fsPermission,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress
    ) throws IOException {
        if (statistics != null) {
            statistics.incrementWriteOps(1);
        }
        long nativeHandle = this.libFs.create(formatPath(path), overwrite);
        CurvineOutputStream output = new CurvineOutputStream(libFs, nativeHandle, 0);
        return new FSDataOutputStream(output, statistics);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
        if (statistics != null) {
            statistics.incrementWriteOps(1);
        }

        long[] tmp = new long[] {0};
        long nativeHandle = this.libFs.append(formatPath(path), tmp);
        CurvineOutputStream output = new CurvineOutputStream(libFs, nativeHandle, tmp[0]);
        return new FSDataOutputStream(output, statistics, output.pos());
    }

    @Override
    public boolean exists(Path f) throws IOException {
        if (statistics != null) {
            statistics.incrementReadOps(1);
        }
        try {
            getFileStatus(f);
            return true;
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        if (statistics != null) {
            statistics.incrementWriteOps(1);
        }
        try {
            String srcPath = formatPath(src);
            String dstPath = formatPath(dst);
            if (srcPath.equals(dstPath)) {
                return true;
            }
            try {
                libFs.rename(srcPath, dstPath);
            } catch (IOException e) {
                if (!shouldMoveIntoDirectory(e)) {
                    throw e;
                }
                // Hadoop semantics: dst is an existing directory, move src into it.
                dstPath = formatPath(new Path(dst, src.getName()));
                if (srcPath.equals(dstPath)) {
                    return true;
                }
                libFs.rename(srcPath, dstPath);
            }
            return true;
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    /**
     * Hadoop rename onto an existing directory: file-to-dir returns EISDIR;
     * dir-to-non-empty-dir returns ENOTEMPTY on the POSIX backend.
     *
     * <p>{@link CurvineException#create(int, String)} maps {@code IS_A_DIRECTORY} to a plain
     * {@code IOException("Is a directory: ...")}, so errno checks alone are not enough.
     */
    private static boolean shouldMoveIntoDirectory(IOException e) {
        if (e instanceof DirectoryNotEmptyException) {
            return true;
        }
        if (e instanceof CurvineException) {
            int errno = ((CurvineException) e).getErrno();
            return errno == CurvineException.IS_A_DIRECTORY || errno == CurvineException.DIRECTORY_NOT_EMPTY;
        }
        String msg = e.getMessage();
        if (msg != null) {
            String lowerMsg = msg.toLowerCase();
            return lowerMsg.contains("is a directory") || lowerMsg.contains("directory not empty");
        }
        return false;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        if (statistics != null) {
            statistics.incrementWriteOps(1);
        }
        try {
            libFs.delete(formatPath(f), recursive);
            return true;
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        if (statistics != null) {
            statistics.incrementWriteOps(1);
        }
        try {
            libFs.mkdir(formatPath(f), true);
            return true;
        } catch (IOException e) {
            // mkdir may fail if parent doesn't exist or other reasons
            LOGGER.warn("mkdirs failed for path: {}", f, e);
            return false;
        }
    }

    /**
     * Detach this instance from the shared mount. The mount itself stays cached.
     *
     * <p>Closing the native mount on the last reference used to tear down the
     * Curvine tokio runtime. CurvineClient owns that runtime and hands clones of
     * it to background write tasks, so the teardown could run on a runtime
     * worker thread and abort the whole process ("Cannot drop a runtime in a
     * context where blocking is not allowed"). Keeping the mount alive also
     * preserves the client connection pool and its worker threads across
     * statements, which is the fast path for repeated open/write/close.
     *
     * <p>Use {@link #closeCachedMounts} to release unreferenced mounts
     * explicitly, e.g. in
     * tests or before class loader shutdown.
     */
    @Override
    public void close() throws IOException {
        if (libFs != null && cacheKey != null) {
            synchronized (MOUNT_CACHE) {
                CachedMount cached = MOUNT_CACHE.get(cacheKey);
                if (cached != null && cached.refCount > 0) {
                    cached.refCount--;
                }
            }
            libFs = null;
        }
        super.close();
    }

    /**
     * Close the cached mounts that no live {@code CurvineFileSystem} still
     * references. Intended for tests and explicit shutdown, not for the normal
     * per-instance close path.
     *
     * <p>Mounts whose {@code refCount} is still positive are left in the cache:
     * closing them would invalidate live instances and make their next call
     * use a released native handle.
     */
    static int closeCachedMounts() throws IOException {
        java.util.List<CachedMount> mounts = new java.util.ArrayList<>();
        synchronized (MOUNT_CACHE) {
            java.util.Iterator<Map.Entry<String, CachedMount>> it = MOUNT_CACHE.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, CachedMount> entry = it.next();
                if (entry.getValue().refCount <= 0) {
                    mounts.add(entry.getValue());
                    it.remove();
                }
            }
        }
        IOException failure = null;
        for (CachedMount mount : mounts) {
            try {
                mount.closeOnce();
            } catch (IOException e) {
                failure = e;
            }
        }
        if (failure != null) {
            throw failure;
        }
        return mounts.size();
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        if (statistics != null) {
            statistics.incrementReadOps(1);
        }

        byte[] bytes = libFs.getFileStatus(formatPath(f));
        GetFileStatusResponse proto = GetFileStatusResponse.parseFrom(bytes);
        return toHadoop(proto.getStatus(), f);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        if (statistics != null) {
            statistics.incrementReadOps(1);
        }

        byte[] bytes = libFs.listStatus(formatPath(f));
        ListStatusResponse proto = ListStatusResponse.parseFrom(bytes);
        FileStatus[] statuses = new FileStatus[proto.getStatusesList().size()];
        for (int i = 0; i < statuses.length; i++) {
            Path path = new Path(f, proto.getStatuses(i).getName());
            statuses[i] = toHadoop(proto.getStatuses(i), path);
        }
        return statuses;
    }

    /**
     * Update file or directory attributes using the native {@code set_attr} RPC.
     *
     * @return updated Hadoop {@link FileStatus} for the target path
     */
    public FileStatus setAttr(Path path, SetAttrOpts opts) throws IOException {
        return applySetAttr(path, opts, true);
    }

    private FileStatus applySetAttr(Path path, SetAttrOpts opts, boolean countWriteOp) throws IOException {
        if (countWriteOp && statistics != null) {
            statistics.incrementWriteOps(1);
        }
        byte[] bytes = libFs.setAttr(formatPath(path), opts);
        GetFileStatusResponse proto = GetFileStatusResponse.parseFrom(bytes);
        return toHadoop(proto.getStatus(), path);
    }

    @Override
    public void setOwner(Path path, String username, String groupname) throws IOException {
        if (statistics != null) {
            statistics.incrementWriteOps(1);
        }
        SetAttrOpts.Builder builder = SetAttrOpts.builder();
        if (username != null) {
            builder.owner(username);
        }
        if (groupname != null) {
            builder.group(groupname);
        }
        applySetAttr(path, builder.build(), false);
    }

    @Override
    public void setPermission(Path path, FsPermission permission) throws IOException {
        if (statistics != null) {
            statistics.incrementWriteOps(1);
        }
        applySetAttr(
                path,
                SetAttrOpts.builder().mode(permission.toShort() & 0xFFFF).build(),
                false);
    }

    @Override
    public void setTimes(Path path, long mtime, long atime) throws IOException {
        if (statistics != null) {
            statistics.incrementWriteOps(1);
        }
        SetAttrOpts.Builder builder = SetAttrOpts.builder();
        if (mtime >= 0) {
            builder.mtime(mtime);
        }
        if (atime >= 0) {
            builder.atime(atime);
        }
        applySetAttr(path, builder.build(), false);
    }

    public FileStatus toHadoop(FileStatusProto proto, Path path) {
        FsPermission permission = new FsPermission((short) proto.getMode());
        return new org.apache.hadoop.fs.FileStatus(
                proto.getLen(),
                proto.getIsDir(),
                (short) proto.getReplicas(),
                proto.getBlockSize(),
                proto.getMtime(),
                proto.getAtime(),
                permission,
                proto.getOwner(),
                proto.getGroup(),
                makeQualified(path)
        );
    }

    // curvine currently does not have a directory capacity setting function, so this interface always returns the capacity information of the root directory.
    @Override
    public FsStatus getStatus(Path p) throws IOException {
        return getFsStat();
    }

    public CurvineFsStat getFsStat() throws IOException {
        if (statistics != null) {
            statistics.incrementReadOps(1);
        }

        byte[] bytes = libFs.getFilesystemInfo();
        GetFilesystemInfoResponse info = GetFilesystemInfoResponse.parseFrom(bytes);
        return new CurvineFsStat(info);
    }

    /** Release Curvine metadata and blocks for a path and return the released totals. */
    public FreeResult free(Path path, boolean recursive) throws IOException {
        if (statistics != null) {
            statistics.incrementWriteOps(1);
        }
        return libFs.free(formatPath(path), recursive);
    }

    public Optional<MountInfoProto> getMountInfo(Path path) throws IOException {
        if (statistics != null) {
            statistics.incrementReadOps(1);
        }

        byte[] bytes = libFs.getMountInfo(path.toString());
        GetMountInfoResponse response = GetMountInfoResponse.parseFrom(bytes);
        if (response.hasMountInfo()) {
            return Optional.of(response.getMountInfo());
        }  else {
            return Optional.empty();
        }
    }

    private boolean isCv(Path path) {
        String scheme = path.toUri().getScheme();
        return StringUtils.isEmpty(scheme) || scheme.equals(getScheme());
    }
    public Optional<Path> togglePath(Path path, boolean checkCache) throws IOException {
        String pathString;
        if (isCv(path)) {
            pathString = formatPath(path);
        } else {
            pathString = path.toString();
        }
        return togglePath(pathString, checkCache);
    }

    public Optional<Path> togglePath(String path, boolean checkCache) throws IOException {
        return libFs.togglePath(path, checkCache).map(Path::new);
    }
}
