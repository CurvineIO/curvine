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
import io.curvine.proto.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageSize;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/****************************************************************
 * Implement the Hadoop FileSystem API for Curvine
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class CurvineFileSystem extends FileSystem {
    public static final Logger LOGGER = LoggerFactory.getLogger(CurvineFileSystem.class);

    private CurvineFsMount libFs;
    private FilesystemConf filesystemConf;
    private Path workingDir;
    private URI uri;

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
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);
        String authority;
        try {
            filesystemConf = new FilesystemConf(conf);
            // cv://host:name format, host:name is the curvine cluster address.
            if (StringUtils.isNotEmpty(name.getAuthority())) {
                filesystemConf.master_addrs = name.getAuthority();
                authority = name.getAuthority();
            } else {
                authority = "/";
            }
        } catch (Exception e) {
            throw new IOException(e);
        }

        this.uri = URI.create(name.getScheme() + "://" + authority);
        this.workingDir = getHomeDirectory();
        this.libFs = new CurvineFsMount(filesystemConf);


        StorageSize size = StorageSize.parse(filesystemConf.write_chunk_size);
        this.writeChunkSize = (int) size.getUnit().toBytes(size.getValue());
        this.writeChunkNum = filesystemConf.write_chunk_num;
    }

    private String formatPath(Path path) {
        return makeQualified(path).toUri().getPath();
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        long[] tmp = new long[] {0, 0};
        try {
            long nativeHandle = libFs.open(formatPath(path), tmp);
            return new FSDataInputStream(new CurvineInputStream(libFs, nativeHandle, tmp[0]));
        } catch (CurvineException e) {
            if (e.isExpired()) {
                LOGGER.info("The {} data is either cached or has expired, " +
                        "data will be read from the ufs.", path);
                return FileSystem.get(path.toUri(), getConf()).open(path, bufferSize);
            } else {
                throw e;
            }
        }
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
        long nativeHandle = this.libFs.create(formatPath(path), overwrite);
        CurvineOutputStream output = new CurvineOutputStream(libFs, nativeHandle, 0, writeChunkSize, writeChunkNum);
        return new FSDataOutputStream(output, statistics);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
        long[] tmp = new long[] {0};
        long nativeHandle = this.libFs.append(formatPath(path), tmp);
        CurvineOutputStream output = new CurvineOutputStream(libFs, nativeHandle, tmp[0], writeChunkSize, writeChunkNum);
        return new FSDataOutputStream(output, statistics, output.pos());
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        libFs.rename(formatPath(src), formatPath(dst));
        return true;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        libFs.delete(formatPath(f), recursive);
        return true;
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
        workingDir = newDir;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        libFs.mkdir(formatPath(f), true);
        return true;
    }

    @Override
    public void close() throws IOException {
        if (libFs != null) {
            libFs.close();
            libFs = null;
        }
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        byte[] bytes = libFs.getFileStatus(formatPath(f));
        GetFileStatusResponse proto = GetFileStatusResponse.parseFrom(bytes);
        return toHadoop(proto.getStatus(), f);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
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
        byte[] bytes = libFs.getMasterInfo();
        GetMasterInfoResponse info = GetMasterInfoResponse.parseFrom(bytes);
        return new CurvineFsStat(info);
    }

    public MountPointInfo getMountPoint(Path path) throws IOException {
        byte[] bytes = libFs.getMountPoint(path.toString());
        GetMountPointInfoResponse response = GetMountPointInfoResponse.parseFrom(bytes);
        if (response.hasMountPoint()) {
            return response.getMountPoint();
        }  else {
            return null;
        }
    }
}
