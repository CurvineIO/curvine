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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    String getMasterAddrs(String name) throws IOException {
        String key = String.format("%s.%s.master_addrs", FilesystemConf.PREFIX, name);
        String addrs = getConf().get(key);
        if (StringUtils.isEmpty(addrs)) {
            throw new IOException(key + " not set");
        } else {
            return FilesystemConf.normalizeCsv(addrs);
        }
    }
    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);
        String authority = name.getAuthority();
        try {
            filesystemConf = new FilesystemConf(conf);
            if (StringUtils.isNotEmpty(authority)) {
                filesystemConf.master_addrs = getMasterAddrs(authority);
            } else {
                authority = "/";
            }
        } catch (Exception e) {
            throw new IOException(e);
        }

        this.uri = URI.create(name.getScheme() + "://" + authority);
        this.workingDir = getHomeDirectory();

        this.cacheKey = mountCacheKey(filesystemConf);
        this.libFs = getOrCreateMount(filesystemConf);
    }

    /**
     * Isolate native mounts by Master address and client mode.
     * Same master_addrs with different enable_unified_fs / enable_rust_read_ufs
     * must not reuse a CurvineFsMount whose conf was already frozen into native.
     */
    static String mountCacheKey(FilesystemConf conf) {
        return conf.master_addrs
                + "|unified=" + conf.enable_unified_fs
                + "|rust_ufs=" + conf.enable_rust_read_ufs;
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

    /**
     * Hadoop {@code append}: the file must already exist.
     *
     * <p>Hadoop's FileSystem specification and HDFS fail on a missing file. The native append
     * behind this method ({@code UnifiedFileSystem::append_existing}) opens without create, so a
     * file removed by another client after the lookup below fails there instead of being
     * recreated. The lookup itself gives the Hadoop exceptions: {@link FileNotFoundException}
     * for a missing path (also on mounted paths, where the UFS append decides) and for a
     * directory.
     */
    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
        if (statistics != null) {
            statistics.incrementWriteOps(1);
        }

        if (getFileStatus(path).isDirectory()) {
            throw new FileNotFoundException("Cannot append to a directory: " + path);
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

    /**
     * Hadoop {@code rename} on top of the master's POSIX {@code rename(2)}.
     *
     * <p>The master replaces an existing file, or an existing empty directory, at {@code dst}
     * (#854). Hadoop's FileSystem specification does not: a rename onto an existing directory
     * moves {@code src} into it as {@code dst/srcName}, and a rename onto an existing file
     * changes nothing and returns {@code false}. The destination is looked up first so that
     * all four cases (file or directory onto a file or a directory) follow Hadoop, including
     * the two that the master would carry out as a replacement.
     *
     * <p>This is best effort against concurrent writers: the lookup happens on the client, so a
     * destination another client creates between the lookup and the native rename is still
     * replaced by the master's POSIX rename. Do not build an atomic "first writer wins"
     * publish on it; that needs a no-replace rename on the master side.
     */
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        if (statistics != null) {
            statistics.incrementWriteOps(1);
        }
        String srcPath = formatPath(src);
        String dstPath = formatPath(dst);
        if (srcPath.equals(dstPath)) {
            return true;
        }
        FileStatus target = statusOrNull(dst);
        if (target != null && target.isDirectory()) {
            dst = new Path(dst, src.getName());
            dstPath = formatPath(dst);
            if (srcPath.equals(dstPath)) {
                return true;
            }
            target = statusOrNull(dst);
        }
        if (target != null) {
            LOGGER.debug("Not renaming {} to {}: the destination exists", src, dst);
            return false;
        }
        try {
            libFs.rename(srcPath, dstPath);
            return true;
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    private FileStatus statusOrNull(Path path) throws IOException {
        try {
            return getFileStatus(path);
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return delete(f, recursive, false);
    }

    /**
     * Delete a path, optionally through the master's async lane.
     *
     * <p>With {@code asyncDelete} the path leaves the namespace immediately and the master
     * reclaims the blocks in the background, so the call does not pay for walking the subtree.
     * Mounted paths stay synchronous, because their backing UFS object is deleted first and there
     * would be nothing left to restore.
     */
    public boolean delete(Path f, boolean recursive, boolean asyncDelete) throws IOException {
        if (statistics != null) {
            statistics.incrementWriteOps(1);
        }
        try {
            if (asyncDelete) {
                libFs.deleteAsync(formatPath(f), recursive);
            } else {
                libFs.delete(formatPath(f), recursive);
            }
            return true;
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    /**
     * Hadoop {@code mkdirs}: create {@code f} and any missing parents.
     *
     * <p>Returns {@code true} when {@code f} is a directory afterwards, also when it already
     * was. A failure is thrown, as Hadoop's FileSystem specification and HDFS do: a file at
     * {@code f} or at one of its parents, a permission error or an unreachable master must not
     * turn into a {@code false} that most callers never check.
     */
    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        if (statistics != null) {
            statistics.incrementWriteOps(1);
        }
        String path = formatPath(f);
        if ("/".equals(path)) {
            // The root always exists, and the master refuses to create it.
            return true;
        }
        if (libFs.mkdir(path, true)) {
            return true;
        }
        // The native mkdir reports "already exists" as false rather than an error. With
        // createParent an existing directory is not reported that way by the master, but a
        // mounted path may be, so look before failing.
        if (getFileStatus(f).isDirectory()) {
            return true;
        }
        throw new FileAlreadyExistsException(path + " already exists and is not a directory");
    }

    @Override
    public void close() throws IOException {
        if (libFs != null && cacheKey != null) {
            CachedMount cached;
            boolean last;
            synchronized (MOUNT_CACHE) {
                cached = MOUNT_CACHE.get(cacheKey);
                last = cached != null && --cached.refCount <= 0;
                if (last) {
                    MOUNT_CACHE.remove(cacheKey);
                }
            }
            try {
                if (last) {
                    cached.closeOnce();
                }
            } finally {
                // Idempotent per instance even if the native close fails: the
                // cache entry is already removed, so a retry is a no-op.
                libFs = null;
            }
        }
        super.close();
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

    /**
     * Lists the children of a directory, or returns the file itself when {@code f} is a file
     * (Hadoop's {@code listStatus} contract).
     *
     * <p>A listed file is recognised by its entry carrying the listed path, which holds for
     * Curvine paths: the master builds that status from the listed path. Under a UFS mount the
     * listing comes from the UFS layer, whose entries carry UFS paths, so listing a file there
     * is not covered by this check and still follows the UFS listing.
     */
    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        if (statistics != null) {
            statistics.incrementReadOps(1);
        }

        String listed = formatPath(f);
        byte[] bytes = libFs.listStatus(listed);
        ListStatusResponse proto = ListStatusResponse.parseFrom(bytes);
        FileStatus[] statuses = new FileStatus[proto.getStatusesList().size()];
        for (int i = 0; i < statuses.length; i++) {
            FileStatusProto status = proto.getStatuses(i);
            // Listing a Curvine file returns that file (Hadoop's listStatus contract, and what
            // the master sends): its path is f itself, not f/<its own name>.
            Path path = listed.equals(status.getPath()) ? f : new Path(f, status.getName());
            statuses[i] = toHadoop(status, path);
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
