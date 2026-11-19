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
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import sun.misc.Unsafe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Hadoop rename semantics of {@link CurvineFileSystem#rename(Path, Path)}, pure Java.
 *
 * <p>{@link PosixMount} stands in for the native mount and renames the way the master does
 * since #854: an existing file or empty directory at the destination is replaced, a file onto
 * a directory fails with EISDIR, a directory onto a non-empty directory with ENOTEMPTY. The
 * tests pin what Hadoop's FileSystem specification asks of {@code rename} on top of that.
 */
public class CurvineFileSystemRenameTest {

    private PosixMount mount;
    private CurvineFileSystem fs;

    @Before
    public void setUp() throws Exception {
        mount = (PosixMount) unsafe().allocateInstance(PosixMount.class);
        mount.entries = new TreeMap<>();
        mount.entries.put("/", true);
        mount.renames = new ArrayList<>();

        fs = new CurvineFileSystem();
        fs.setConf(new Configuration());
        setField(fs, "uri", URI.create("cv://test-master"));
        setField(fs, "workingDir", new Path("/"));
        setField(fs, "libFs", mount);
    }

    @Test
    public void renameToAFreePathMovesTheFile() throws IOException {
        mount.file("/d/a");

        assertTrue(fs.rename(new Path("/d/a"), new Path("/d/b")));
        assertTrue(mount.isFile("/d/b"));
        assertFalse(mount.exists("/d/a"));
    }

    @Test
    public void renameOntoAnExistingFileFailsAndChangesNothing() throws IOException {
        mount.file("/d/a");
        mount.file("/d/b");

        assertFalse(fs.rename(new Path("/d/a"), new Path("/d/b")));
        assertTrue(mount.isFile("/d/a"));
        assertTrue(mount.isFile("/d/b"));
        assertEquals("the master must not be asked to replace /d/b", 0, mount.renames.size());
    }

    @Test
    public void renameOfADirectoryOntoAnEmptyDirectoryMovesItInside() throws IOException {
        mount.file("/src/part-0");
        mount.dir("/dst");

        assertTrue(fs.rename(new Path("/src"), new Path("/dst")));
        assertTrue(mount.isDir("/dst"));
        assertTrue(mount.isFile("/dst/src/part-0"));
        assertFalse(mount.exists("/src"));
    }

    @Test
    public void renameOfADirectoryOntoANonEmptyDirectoryMovesItInside() throws IOException {
        mount.file("/src/part-0");
        mount.file("/dst/keep");

        assertTrue(fs.rename(new Path("/src"), new Path("/dst")));
        assertTrue(mount.isFile("/dst/keep"));
        assertTrue(mount.isFile("/dst/src/part-0"));
        assertFalse(mount.exists("/src"));
    }

    @Test
    public void renameOfAFileOntoADirectoryMovesItInside() throws IOException {
        mount.file("/f");
        mount.dir("/dir");

        assertTrue(fs.rename(new Path("/f"), new Path("/dir")));
        assertTrue(mount.isFile("/dir/f"));
        assertFalse(mount.exists("/f"));
    }

    @Test
    public void renameIntoADirectoryWhoseEntryIsTakenFails() throws IOException {
        mount.file("/f");
        mount.file("/dir/f");

        assertFalse(fs.rename(new Path("/f"), new Path("/dir")));
        assertTrue(mount.isFile("/f"));
        assertTrue(mount.isFile("/dir/f"));
        assertEquals(0, mount.renames.size());
    }

    @Test
    public void renameOfAMissingSourceReturnsFalse() throws IOException {
        mount.dir("/d");

        assertFalse(fs.rename(new Path("/d/missing"), new Path("/d/b")));
        assertFalse(mount.exists("/d/b"));
    }

    @Test
    public void renameOntoItselfIsANoOp() throws IOException {
        mount.file("/d/a");

        assertTrue(fs.rename(new Path("/d/a"), new Path("/d/a")));
        assertTrue(fs.rename(new Path("/d/a"), new Path("/d")));
        assertTrue(mount.isFile("/d/a"));
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
     * In-memory namespace that renames like the master's POSIX {@code rename(2)}.
     * Created via {@link Unsafe}, so the native constructor never runs.
     */
    static class PosixMount extends CurvineFsMount {
        /** Path to "is a directory". */
        Map<String, Boolean> entries;
        /** Every rename the file system asked for, as "src -> dst". */
        List<String> renames;

        private PosixMount() throws IOException {
            super((FilesystemConf) null);
        }

        void dir(String path) {
            String parent = parent(path);
            if (parent != null) {
                dir(parent);
            }
            entries.put(path, true);
        }

        void file(String path) {
            dir(parent(path));
            entries.put(path, false);
        }

        boolean exists(String path) {
            return entries.containsKey(path);
        }

        boolean isDir(String path) {
            return Boolean.TRUE.equals(entries.get(path));
        }

        boolean isFile(String path) {
            return Boolean.FALSE.equals(entries.get(path));
        }

        private boolean hasChildren(String dir) {
            String prefix = dir + "/";
            for (String path : entries.keySet()) {
                if (path.startsWith(prefix)) {
                    return true;
                }
            }
            return false;
        }

        private static String parent(String path) {
            if (path.equals("/")) {
                return null;
            }
            int slash = path.lastIndexOf('/');
            return slash == 0 ? "/" : path.substring(0, slash);
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

        @Override
        public void rename(String src, String dst) throws IOException {
            renames.add(src + " -> " + dst);
            Boolean srcIsDir = entries.get(src);
            if (srcIsDir == null) {
                throw new FileNotFoundException(src);
            }
            if (!isDir(parent(dst))) {
                throw new FileNotFoundException(parent(dst));
            }
            Boolean dstIsDir = entries.get(dst);
            if (dstIsDir != null) {
                if (!srcIsDir && dstIsDir) {
                    throw new IOException("Is a directory: " + dst);
                }
                if (srcIsDir && !dstIsDir) {
                    throw new IOException("Not a directory: " + dst);
                }
                if (dstIsDir && hasChildren(dst)) {
                    throw new IOException("Directory not empty: " + dst);
                }
                // POSIX: an existing file, or an empty directory, is replaced.
                entries.remove(dst);
            }
            Map<String, Boolean> moved = new TreeMap<>();
            String prefix = src + "/";
            for (Map.Entry<String, Boolean> e : new TreeMap<>(entries).entrySet()) {
                String path = e.getKey();
                if (path.equals(src)) {
                    moved.put(dst, e.getValue());
                    entries.remove(path);
                } else if (path.startsWith(prefix)) {
                    moved.put(dst + path.substring(src.length()), e.getValue());
                    entries.remove(path);
                }
            }
            entries.putAll(moved);
        }
    }
}
