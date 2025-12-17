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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageSize;
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

import io.curvine.proto.FileStatusProto;
import io.curvine.proto.GetFileStatusResponse;
import io.curvine.proto.GetMasterInfoResponse;
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
     * Key: master_addrs (e.g., "master-0:8995")
     * Value: CachedMount containing the shared CurvineFsMount and reference count
     */
    private static final ConcurrentHashMap<String, CachedMount> MOUNT_CACHE = new ConcurrentHashMap<>();
    
    private static class CachedMount {
        final CurvineFsMount mount;
        final AtomicInteger refCount;
        
        CachedMount(CurvineFsMount mount) {
            this.mount = mount;
            this.refCount = new AtomicInteger(1);
        }
    }
    
    static {
        // Register shutdown hook to clean up all cached mounts
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down CurvineFileSystem, closing {} cached mounts", MOUNT_CACHE.size());
            for (CachedMount cached : MOUNT_CACHE.values()) {
                try {
                    cached.mount.close();
                } catch (IOException e) {
                    LOGGER.warn("Error closing cached mount", e);
                }
            }
            MOUNT_CACHE.clear();
        }));
    }

    private CurvineFsMount libFs;
    private FilesystemConf filesystemConf;
    private Path workingDir;
    private URI uri;
    private String cacheKey;  // Key used for mount cache lookup

    private int writeChunkSize;
    private int writeChunkNum;

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

    private String getMasterAddrs(String name) throws IOException {
        String key = String.format("%s.%s.master_addrs", FilesystemConf.PREFIX, name);
        String addrs = getConf().get(key);
        if (StringUtils.isEmpty(addrs)) {
            throw new IOException(key + " not set");
        } else {
            return addrs;
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
        
        this.cacheKey = filesystemConf.master_addrs;
        this.libFs = getOrCreateMount(filesystemConf);

        StorageSize size = StorageSize.parse(filesystemConf.write_chunk_size);
        this.writeChunkSize = (int) size.getUnit().toBytes(size.getValue());
        this.writeChunkNum = filesystemConf.write_chunk_num;
    }
    
    /**
     * Get or create a cached CurvineFsMount instance.
     * This method is thread-safe and ensures only one mount is created per master_addrs.
     */
    private CurvineFsMount getOrCreateMount(FilesystemConf conf) throws IOException {
        String key = conf.master_addrs;
        
        CachedMount cached = MOUNT_CACHE.get(key);
        if (cached != null) {
            cached.refCount.incrementAndGet();
            LOGGER.debug("Reusing cached CurvineFsMount for {}, refCount={}", key, cached.refCount.get());
            return cached.mount;
        }
        
        synchronized (MOUNT_CACHE) {
            cached = MOUNT_CACHE.get(key);
            if (cached != null) {
                cached.refCount.incrementAndGet();
                LOGGER.debug("Reusing cached CurvineFsMount for {} (after lock), refCount={}", key, cached.refCount.get());
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
        CurvineOutputStream output = new CurvineOutputStream(libFs, nativeHandle, 0, writeChunkSize, writeChunkNum);
        return new FSDataOutputStream(output, statistics);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
        if (statistics != null) {
            statistics.incrementWriteOps(1);
        }

        long[] tmp = new long[] {0};
        long nativeHandle = this.libFs.append(formatPath(path), tmp);
        CurvineOutputStream output = new CurvineOutputStream(libFs, nativeHandle, tmp[0], writeChunkSize, writeChunkNum);
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
            libFs.rename(formatPath(src), formatPath(dst));
            return true;
        } catch (FileNotFoundException e) {
            return false;
        }
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

    @Override
    public void close() throws IOException {
        // Don't close the shared mount, just decrement reference count
        if (libFs != null && cacheKey != null) {
            CachedMount cached = MOUNT_CACHE.get(cacheKey);
            if (cached != null) {
                int remaining = cached.refCount.decrementAndGet();
                LOGGER.debug("Closing CurvineFileSystem for {}, remaining refCount={}", cacheKey, remaining);
                // Note: We don't remove from cache even when refCount reaches 0
                // because new FileSystem instances may be created later.
                // The mount will be cleaned up by the shutdown hook.
            }
            libFs = null;
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

    public FileStatus toHadoop(FileStatusProto proto, Path path) {
        return new org.apache.hadoop.fs.FileStatus(
                proto.getLen(),
                proto.getIsDir(),
                proto.getReplicas(),
                proto.getBlockSize(),
                proto.getMtime(),
                proto.getAtime(),
                FsPermission.getDefault(),
                System.getProperty("user.name"),
                System.getProperty("user.group"),
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

        byte[] bytes = libFs.getMasterInfo();
        GetMasterInfoResponse info = GetMasterInfoResponse.parseFrom(bytes);
        return new CurvineFsStat(info);
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
