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
import java.nio.file.NotDirectoryException;
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
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import sun.misc.Unsafe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * {@link CurvineFileSystem#mkdirs(Path)} without the native library. {@link FakeMount} answers
 * {@code mkdir} the way the native layer does: an existing directory is a success, while an
 * existing file comes back as {@code false} ({@code UnifiedFileSystem::mkdir} maps
 * {@code FileAlreadyExists} to {@code Ok(false)}), not as an error.
 */
public class CurvineFileSystemMkdirsTest {

    private FakeMount mount;
    private CurvineFileSystem fs;

    @Before
    public void setUp() throws Exception {
        mount = (FakeMount) unsafe().allocateInstance(FakeMount.class);
        mount.calls = new ArrayList<>();
        mount.entries = new TreeMap<>();

        fs = new CurvineFileSystem();
        fs.setConf(new Configuration());
        setField(fs, "uri", URI.create("cv://test-master"));
        setField(fs, "workingDir", new Path("/"));
        setField(fs, "libFs", mount);
    }

    @Test
    public void createsTheDirectoryWithItsParents() throws IOException {
        assertTrue(fs.mkdirs(new Path("/a/b")));
        assertEquals(1, mount.calls.size());
        assertEquals("/a/b createParent=true", mount.calls.get(0));
    }

    @Test
    public void anExistingDirectoryIsASuccess() throws IOException {
        mount.entries.put("/a", true);
        assertTrue(fs.mkdirs(new Path("/a")));
    }

    @Test
    public void aFileAtThePathIsAnErrorNotTrue() {
        mount.entries.put("/a/file", false);
        FileAlreadyExistsException e =
                assertThrows(FileAlreadyExistsException.class, () -> fs.mkdirs(new Path("/a/file")));
        assertTrue(e.getMessage(), e.getMessage().contains("/a/file"));
    }

    @Test
    public void aDirectoryReportedAsExistingIsStillASuccess() throws IOException {
        // A mounted path may answer "already exists" for a directory too.
        mount.entries.put("/mnt/dir", true);
        mount.existingIsFalse = true;
        assertTrue(fs.mkdirs(new Path("/mnt/dir")));
    }

    @Test
    public void aFileAsAParentIsThrownNotReportedAsFalse() {
        mount.failure = new NotDirectoryException("/a/file");
        assertThrows(NotDirectoryException.class, () -> fs.mkdirs(new Path("/a/file/sub")));
    }

    @Test
    public void otherFailuresAreThrownNotReportedAsFalse() {
        mount.failure = new IOException("Connection refused: master-0:8995");
        IOException e = assertThrows(IOException.class, () -> fs.mkdirs(new Path("/a/b")));
        assertEquals("Connection refused: master-0:8995", e.getMessage());
    }

    @Test
    public void theRootAlwaysExists() throws IOException {
        assertTrue(fs.mkdirs(new Path("/")));
        assertTrue(fs.mkdirs(new Path("cv://test-master/")));
        assertEquals("the master refuses to create the root, so it is not asked",
                0, mount.calls.size());
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

    /** Fake native mount; created via {@link Unsafe} so the native constructor never runs. */
    static class FakeMount extends CurvineFsMount {
        List<String> calls;
        /** Existing paths, mapped to "is a directory". */
        Map<String, Boolean> entries;
        /** Answer "already exists" (false) for existing directories too. */
        boolean existingIsFalse;
        IOException failure;

        private FakeMount() throws IOException {
            super((FilesystemConf) null);
        }

        @Override
        public boolean mkdir(String path, boolean createParent) throws IOException {
            calls.add(path + " createParent=" + createParent);
            if (path.equals("/")) {
                // MasterFilesystem::mkdir_with_opts
                throw new IOException("Not allowed to create existing root path: /");
            }
            if (failure != null) {
                throw failure;
            }
            Boolean isDir = entries.get(path);
            if (isDir == null) {
                entries.put(path, true);
                return true;
            }
            return isDir && !existingIsFalse;
        }

        @Override
        public byte[] getFileStatus(String path) throws IOException {
            Boolean isDir = entries.get(path);
            if (isDir == null) {
                throw new FileNotFoundException(path);
            }
            FileStatusProto status = FileStatusProto.newBuilder()
                    .setId(1)
                    .setPath(path)
                    .setName(path.substring(path.lastIndexOf('/') + 1))
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
                    .setOwner("test")
                    .setGroup("test")
                    .setMode(0755)
                    .setNlink(1)
                    .build();
            return GetFileStatusResponse.newBuilder().setStatus(status).build().toByteArray();
        }
    }
}
