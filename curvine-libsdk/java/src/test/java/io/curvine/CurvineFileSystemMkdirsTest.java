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
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NotDirectoryException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import sun.misc.Unsafe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * {@link CurvineFileSystem#mkdirs(Path)} without the native library: {@link FakeMount} answers
 * {@code mkdir} the way the master does and records the calls.
 */
public class CurvineFileSystemMkdirsTest {

    private FakeMount mount;
    private CurvineFileSystem fs;

    @Before
    public void setUp() throws Exception {
        mount = (FakeMount) unsafe().allocateInstance(FakeMount.class);
        mount.calls = new ArrayList<>();

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
    public void aFileAtThePathIsThrownNotReportedAsFalse() {
        mount.failure = new FileAlreadyExistsException("/a/file");
        assertThrows(FileAlreadyExistsException.class, () -> fs.mkdirs(new Path("/a/file")));
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
        IOException failure;

        private FakeMount() throws IOException {
            super((FilesystemConf) null);
        }

        @Override
        public void mkdir(String path, boolean createParent) throws IOException {
            calls.add(path + " createParent=" + createParent);
            if (path.equals("/")) {
                // MasterFilesystem::mkdir_with_opts
                throw new IOException("Not allowed to create existing root path: /");
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}
