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

import io.curvine.proto.FileStatusProto;
import io.curvine.proto.FileTypeProto;
import io.curvine.proto.ListStatusResponse;
import io.curvine.proto.StoragePolicyProto;
import io.curvine.proto.StorageStateProto;
import io.curvine.proto.StorageTypeProto;
import io.curvine.proto.TtlActionProto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;

import sun.misc.Unsafe;

public class CurvineFileSystemToHadoopTest {

    @Test
    public void toHadoopMapsOwnerGroupAndModeFromProto() throws Exception {
        CurvineFileSystem fs = new CurvineFileSystem();
        fs.setConf(new Configuration());
        Field uriField = CurvineFileSystem.class.getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(fs, URI.create("cv://localhost"));

        FileStatusProto proto = FileStatusProto.newBuilder()
                .setId(1)
                .setPath("/demo/file")
                .setName("file")
                .setIsDir(false)
                .setMtime(1_700_000_000_000L)
                .setAtime(1_700_000_001_000L)
                .setChildrenNum(0)
                .setIsComplete(true)
                .setLen(42)
                .setReplicas(2)
                .setBlockSize(128L * 1024L * 1024L)
                .setFileType(FileTypeProto.FILE_TYPE_PROTO_FILE)
                .setStoragePolicy(StoragePolicyProto.newBuilder()
                        .setStorageType(StorageTypeProto.STORAGE_TYPE_PROTO_DISK)
                        .setTtlMs(0)
                        .setTtlAction(TtlActionProto.TTL_ACTION_PROTO_NONE)
                        .setUfsMtime(0)
                        .setState(StorageStateProto.STORAGE_STATE_PROTO_CV)
                        .build())
                .setOwner("alice")
                .setGroup("staff")
                .setMode(0644)
                .setNlink(1)
                .build();

        FileStatus status = fs.toHadoop(proto, new Path("/demo/file"));
        Assert.assertEquals("alice", status.getOwner());
        Assert.assertEquals("staff", status.getGroup());
        Assert.assertEquals(new FsPermission((short) 0644), status.getPermission());
        Assert.assertEquals(42, status.getLen());
        Assert.assertEquals(1_700_000_000_000L, status.getModificationTime());
        Assert.assertEquals(1_700_000_001_000L, status.getAccessTime());
    }

    /**
     * Hadoop: listStatus on a file returns that one file. The master sends its status with
     * the listed path, and the entry must keep that path instead of f/<name>.
     */
    @Test
    public void listStatusOfAFileKeepsTheFilePath() throws Exception {
        ListingMount mount = listingMount(entry("/demo/file", "file", false));
        CurvineFileSystem fs = fileSystem(mount);

        FileStatus[] statuses = fs.listStatus(new Path("/demo/file"));

        Assert.assertEquals("/demo/file", mount.listed);
        Assert.assertEquals(1, statuses.length);
        Assert.assertEquals(new Path("cv://localhost/demo/file"), statuses[0].getPath());
        Assert.assertTrue(statuses[0].isFile());
    }

    @Test
    public void listStatusOfADirectoryNamesEachChildUnderIt() throws Exception {
        ListingMount mount = listingMount(
                entry("/demo/file", "file", false),
                entry("/demo/sub", "sub", true));
        CurvineFileSystem fs = fileSystem(mount);

        FileStatus[] statuses = fs.listStatus(new Path("/demo"));

        Assert.assertEquals(2, statuses.length);
        Assert.assertEquals(new Path("cv://localhost/demo/file"), statuses[0].getPath());
        Assert.assertEquals(new Path("cv://localhost/demo/sub"), statuses[1].getPath());
        Assert.assertTrue(statuses[1].isDirectory());
    }

    /** A directory holding a file with the directory's own name is still a directory listing. */
    @Test
    public void listStatusOfADirectoryWithAChildOfTheSameName() throws Exception {
        ListingMount mount = listingMount(entry("/demo/demo", "demo", false));
        CurvineFileSystem fs = fileSystem(mount);

        FileStatus[] statuses = fs.listStatus(new Path("/demo"));

        Assert.assertEquals(1, statuses.length);
        Assert.assertEquals(new Path("cv://localhost/demo/demo"), statuses[0].getPath());
    }

    private static FileStatusProto entry(String path, String name, boolean isDir) {
        return FileStatusProto.newBuilder()
                .setId(1)
                .setPath(path)
                .setName(name)
                .setIsDir(isDir)
                .setMtime(0)
                .setAtime(0)
                .setChildrenNum(0)
                .setIsComplete(true)
                .setLen(0)
                .setReplicas(1)
                .setBlockSize(128L * 1024L * 1024L)
                .setFileType(isDir ? FileTypeProto.FILE_TYPE_PROTO_DIR
                        : FileTypeProto.FILE_TYPE_PROTO_FILE)
                .setStoragePolicy(StoragePolicyProto.newBuilder()
                        .setStorageType(StorageTypeProto.STORAGE_TYPE_PROTO_DISK)
                        .setTtlMs(0)
                        .setTtlAction(TtlActionProto.TTL_ACTION_PROTO_NONE)
                        .setUfsMtime(0)
                        .setState(StorageStateProto.STORAGE_STATE_PROTO_CV)
                        .build())
                .setOwner("alice")
                .setGroup("staff")
                .setMode(0644)
                .setNlink(1)
                .build();
    }

    private static ListingMount listingMount(FileStatusProto... entries) throws Exception {
        Field field = Unsafe.class.getDeclaredField("theUnsafe");
        field.setAccessible(true);
        ListingMount mount = (ListingMount) ((Unsafe) field.get(null))
                .allocateInstance(ListingMount.class);
        ListStatusResponse.Builder response = ListStatusResponse.newBuilder();
        for (FileStatusProto entry : entries) {
            response.addStatuses(entry);
        }
        mount.response = response.build().toByteArray();
        return mount;
    }

    private static CurvineFileSystem fileSystem(CurvineFsMount mount) throws Exception {
        CurvineFileSystem fs = new CurvineFileSystem();
        fs.setConf(new Configuration());
        setField(fs, "uri", URI.create("cv://localhost"));
        setField(fs, "workingDir", new Path("/"));
        setField(fs, "libFs", mount);
        return fs;
    }

    private static void setField(CurvineFileSystem fs, String name, Object value) throws Exception {
        Field field = CurvineFileSystem.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(fs, value);
    }

    /** Fake native mount answering listStatus; created via Unsafe, so no native code runs. */
    static class ListingMount extends CurvineFsMount {
        byte[] response;
        String listed;

        private ListingMount() throws IOException {
            super((FilesystemConf) null);
        }

        @Override
        public byte[] listStatus(String path) {
            listed = path;
            return response;
        }
    }
}
