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
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.Test;

import sun.misc.Unsafe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Pure-Java lifecycle tests for the cached mount lifecycle in CurvineFileSystem.
 *
 * <p>The native library is not required: a {@link FakeMount} created via {@link Unsafe}
 * replaces the real CurvineFsMount, and the private MOUNT_CACHE / CachedMount state is
 * driven through reflection. These tests pin the release contract:
 * <ol>
 *   <li>closing one of several CurvineFileSystem instances sharing a mount must not
 *       release the native mount (no premature release);</li>
 *   <li>closing the last instance releases the native mount exactly once;</li>
 *   <li>a failed native close is propagated to the caller instead of being swallowed;</li>
 *   <li>reads on a closed stream throw IOException instead of returning EOF or crashing.</li>
 * </ol>
 */
public class CurvineFileSystemLifecycleTest {

    private static final String KEY = "test-master-1:8995";

    private final ConcurrentHashMap<String, Object> cache = mountCache();

    private FakeMount fakeMount;
    private Object cachedMount;

    @After
    public void tearDown() {
        cache.remove(KEY);
    }

    @Test
    public void intermediateCloseMustNotReleaseSharedMount() throws Exception {
        fakeMount = newFakeMount();
        cachedMount = newCachedMount(fakeMount, 2);

        CurvineFileSystem fs1 = newInstance("fs1");
        CurvineFileSystem fs2 = newInstance("fs2");

        fs1.close();

        assertEquals("intermediate close must not call closeFilesystem", 0, fakeMount.closeCalls);
        assertTrue("mount must stay cached while another instance holds it", cache.containsKey(KEY));
        assertEquals("refcount must drop by one only", 1, refCount(cachedMount));
        assertFalse("mount must not be marked closed", isClosed(cachedMount));
        fs2.close(); // cleanup
    }

    @Test
    public void lastCloseReleasesSharedMountExactlyOnce() throws Exception {
        fakeMount = newFakeMount();
        cachedMount = newCachedMount(fakeMount, 2);

        CurvineFileSystem fs1 = newInstance("fs1");
        CurvineFileSystem fs2 = newInstance("fs2");

        fs1.close();
        fs2.close();

        assertEquals("last close must release the native mount exactly once", 1, fakeMount.closeCalls);
        assertNull("cache entry must be removed after last close", cache.get(KEY));
    }

    @Test
    public void closeFailureIsPropagatedToCaller() throws Exception {
        fakeMount = newFakeMount();
        fakeMount.failClose = true;
        cachedMount = newCachedMount(fakeMount, 1);

        CurvineFileSystem fs = newInstance("fs");

        IOException e = assertThrows(IOException.class, fs::close);
        assertTrue(e.getMessage().contains("fake close failure"));
        // A failed close must not break per-instance idempotency: retry is a no-op.
        fs.close();
    }

    @Test
    public void readOnClosedStreamThrowsIOException() throws Exception {
        CurvineInputStream in = new CurvineInputStream(null, 0, 0, null);
        setClosed(in, true);

        assertThrows(IOException.class, () -> in.read(new byte[16], 0, 16));
        // close on an already closed stream stays a no-op and must not throw
        in.close();
    }

    @Test
    public void readOnClosedStreamDoesNotDependOnFileSize() throws Exception {
        // Regression: checkClosed must run before the pos >= fileSize EOF short-circuit.
        CurvineInputStream in = new CurvineInputStream(null, 0, 0, null);
        setClosed(in, true);

        assertThrows(IOException.class, () -> in.read(new byte[16], 0, 16));
    }

    // --- helpers ---------------------------------------------------------

    private CurvineFileSystem newInstance(String name) throws Exception {
        CurvineFileSystem fs = new CurvineFileSystem();
        setField(fs, "libFs", fakeMount);
        setField(fs, "cacheKey", KEY);
        return fs;
    }

    private Object newCachedMount(CurvineFsMount mount, int refCount) throws Exception {
        Class<?> cachedMountClass = Class.forName("io.curvine.CurvineFileSystem$CachedMount");
        Constructor<?> ctor = cachedMountClass.getDeclaredConstructor(CurvineFsMount.class);
        ctor.setAccessible(true);
        Object cm = ctor.newInstance(mount);
        Field refCountField = cachedMountClass.getDeclaredField("refCount");
        refCountField.setAccessible(true);
        refCountField.setInt(cm, refCount);
        cache.put(KEY, cm);
        return cm;
    }

    private static int refCount(Object cachedMount) throws Exception {
        Field refCountField = cachedMount.getClass().getDeclaredField("refCount");
        refCountField.setAccessible(true);
        return refCountField.getInt(cachedMount);
    }

    private static boolean isClosed(Object cachedMount) throws Exception {
        Field closedField = cachedMount.getClass().getDeclaredField("closed");
        closedField.setAccessible(true);
        return closedField.getBoolean(cachedMount);
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<String, Object> mountCache() {
        try {
            Field cacheField = CurvineFileSystem.class.getDeclaredField("MOUNT_CACHE");
            cacheField.setAccessible(true);
            return (ConcurrentHashMap<String, Object>) cacheField.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static void setClosed(CurvineInputStream in, boolean closed) throws Exception {
        setField(in, "closed", closed);
    }

    private static FakeMount newFakeMount() throws InstantiationException {
        return (FakeMount) unsafe().allocateInstance(FakeMount.class);
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

    /** Fake native mount; created via {@link Unsafe} so the native constructor is never run. */
    static class FakeMount extends CurvineFsMount {
        int closeCalls;
        boolean failClose;

        private FakeMount() throws IOException {
            // Never invoked: instances are allocated via Unsafe.allocateInstance.
            super((FilesystemConf) null);
        }

        @Override
        public void close() throws IOException {
            closeCalls++;
            if (failClose) {
                throw new IOException("fake close failure");
            }
        }
    }
}
