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
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.curvine.proto.FileStatusProto;
import io.curvine.proto.FileTypeProto;
import io.curvine.proto.GetFileStatusResponse;
import io.curvine.proto.StoragePolicyProto;
import io.curvine.proto.StorageStateProto;
import io.curvine.proto.StorageTypeProto;
import io.curvine.proto.TtlActionProto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import sun.misc.Unsafe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Hadoop append semantics of {@link CurvineFileSystem#append(Path)}, pure Java. Like the native
 * append the Java SDK uses ({@code UnifiedFileSystem::append_existing}), {@link FakeMount#append}
 * opens without create and fails on a missing file.
 */
public class CurvineFileSystemAppendTest {

    private FakeMount mount;
    private CurvineFileSystem fs;

    @Before
    public void setUp() throws Exception {
        mount = (FakeMount) unsafe().allocateInstance(FakeMount.class);
        mount.entries = new TreeMap<>();
        mount.appends = new ArrayList<>();

        fs = new CurvineFileSystem();
        fs.setConf(new Configuration());
        setField(fs, "uri", URI.create("cv://test-master"));
        setField(fs, "workingDir", new Path("/"));
        setField(fs, "libFs", mount);
    }

    @Test
    public void appendingToAMissingFileFailsAndCreatesNothing() {
        assertThrows(FileNotFoundException.class, () -> fs.append(new Path("/d/missing")));
        assertTrue("the native append must not be called", mount.appends.isEmpty());
        assertTrue(mount.entries.isEmpty());
    }

    @Test
    public void appendingToADirectoryFails() {
        mount.entries.put("/d", -1L);
        assertThrows(FileNotFoundException.class, () -> fs.append(new Path("/d")));
        assertTrue(mount.appends.isEmpty());
    }

    @Test
    public void aFileRemovedAfterTheLookupIsNotRecreated() {
        mount.entries.put("/d/log", 42L);
        mount.removeBeforeAppend = true;

        assertThrows(FileNotFoundException.class, () -> fs.append(new Path("/d/log")));
        assertTrue("the file must not come back", mount.entries.isEmpty());
    }

    @Test
    public void appendingToAFileContinuesAtItsEnd() throws IOException {
        mount.entries.put("/d/log", 42L);

        FSDataOutputStream out = fs.append(new Path("/d/log"));

        assertEquals(42, out.getPos());
        assertEquals(1, mount.appends.size());
        assertEquals("/d/log", mount.appends.get(0));
    }

    // --- helpers ---------------------------------------------------------

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = CurvineFileSystem.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Unsafe unsafe() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Fake native mount; created via {@link Unsafe} so the native constructor never runs.
     * {@code entries} maps a path to its length, or to {@code -1} for a directory.
     */
    static class FakeMount extends CurvineFsMount {
        Map<String, Long> entries;
        List<String> appends;
        /** Another client removes the file between the lookup and the append. */
        boolean removeBeforeAppend;

        private FakeMount() throws IOException {
            super((FilesystemConf) null);
        }

        @Override
        public long append(String path, long[] tmp) throws IOException {
            appends.add(path);
            if (removeBeforeAppend) {
                entries.remove(path);
            }
            // Like the native append_existing: no create, so a missing file is an error.
            Long len = entries.get(path);
            if (len == null) {
                throw new FileNotFoundException(path);
            }
            tmp[0] = len;
            return 7;
        }

        @Override
        public byte[] getFileStatus(String path) throws IOException {
            Long len = entries.get(path);
            if (len == null) {
                throw new FileNotFoundException(path);
            }
            boolean isDir = len < 0;
            FileStatusProto status = FileStatusProto.newBuilder()
                    .setId(1)
                    .setPath(path)
                    .setName(path.substring(path.lastIndexOf('/') + 1))
                    .setIsDir(isDir)
                    .setMtime(0)
                    .setAtime(0)
                    .setChildrenNum(0)
                    .setIsComplete(true)
                    .setLen(isDir ? 0 : len)
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
                    .setOwner("test")
                    .setGroup("test")
                    .setMode(0644)
                    .setNlink(1)
                    .build();
            return GetFileStatusResponse.newBuilder().setStatus(status).build().toByteArray();
        }
    }
}
