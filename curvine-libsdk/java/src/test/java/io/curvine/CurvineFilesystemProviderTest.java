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
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Lifecycle tests for {@link CurvineFilesystemProvider} driven by real {@link Configuration}
 * values. The native library is not required: {@link TestProvider} replaces only the
 * filesystem factory.
 */
public class CurvineFilesystemProviderTest {

    @Test
    public void proxyDisabledByConfigNeverCreatesCurvine() throws Exception {
        Configuration conf = new Configuration(false);
        conf.setBoolean(CurvineFilesystemProvider.PROXY_ENABLED, false);
        TestProvider provider = new TestProvider(conf, () -> new FakeFs(true, false));

        assertNull(provider.getFs());
        assertEquals(0, provider.created.get());
        provider.close();
    }

    @Test
    public void validInstanceIsReturnedAndClosedWithProvider() throws Exception {
        FakeFs fake = new FakeFs(true, false);
        TestProvider provider = new TestProvider(new Configuration(false), () -> fake);

        assertSame(fake, provider.getFs());
        assertSame(fake, provider.getFs());
        assertEquals(1, provider.created.get());
        assertFalse(fake.closed.getCount() == 0);

        provider.close();
        assertEquals(0, fake.closed.getCount());
    }

    @Test
    public void instanceWithoutUnifiedFsIsClosed() throws Exception {
        assertRejectedAndClosed(new FakeFs(false, false));
    }

    @Test
    public void instanceWithRustUfsReadIsClosed() throws Exception {
        assertRejectedAndClosed(new FakeFs(true, true));
    }

    @Test
    public void instanceCreatedAfterTimeoutIsClosed() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set(CurvineFilesystemProvider.PROXY_TIMEOUT, "100ms");
        CountDownLatch release = new CountDownLatch(1);
        FakeFs late = new FakeFs(true, false);
        TestProvider provider = new TestProvider(conf, () -> {
            release.await();
            return late;
        });

        assertNull(provider.getFs());
        release.countDown();
        assertTrue("late instance must be closed", late.closed.await(10, TimeUnit.SECONDS));
        provider.close();
    }

    @Test
    public void instanceCreatedWhileProviderClosesIsClosed() throws Exception {
        CountDownLatch creating = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        FakeFs inFlight = new FakeFs(true, false);
        TestProvider provider = new TestProvider(new Configuration(false), () -> {
            creating.countDown();
            release.await();
            return inFlight;
        });

        AtomicReference<CurvineFileSystem> result = new AtomicReference<>();
        Thread getter = new Thread(() -> result.set(provider.getFs()));
        getter.start();
        assertTrue(creating.await(10, TimeUnit.SECONDS));

        provider.close();
        release.countDown();
        getter.join(10_000);

        assertFalse(getter.isAlive());
        assertNull(result.get());
        assertEquals(0, inFlight.closed.getCount());
        assertNull(provider.getFs());
    }

    private static void assertRejectedAndClosed(FakeFs fake) throws Exception {
        TestProvider provider = new TestProvider(new Configuration(false), () -> fake);

        assertNull(provider.getFs());
        assertEquals(0, fake.closed.getCount());
        // The failure disables the proxy; no further instances are created.
        assertNull(provider.getFs());
        assertEquals(1, provider.created.get());
        provider.close();
    }

    @FunctionalInterface
    private interface Factory {
        FileSystem create() throws Exception;
    }

    private static class TestProvider extends CurvineFilesystemProvider {
        private final Factory factory;
        final AtomicInteger created = new AtomicInteger();

        TestProvider(Configuration conf, Factory factory) {
            super(conf);
            this.factory = factory;
        }

        @Override
        FileSystem newFileSystem(URI uri, Configuration conf) throws IOException {
            created.incrementAndGet();
            try {
                return factory.create();
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    private static class FakeFs extends CurvineFileSystem {
        private final FilesystemConf conf;
        final CountDownLatch closed = new CountDownLatch(1);

        FakeFs(boolean unifiedFs, boolean rustReadUfs) throws Exception {
            conf = new FilesystemConf(new Configuration(false));
            conf.enable_unified_fs = unifiedFs;
            conf.enable_rust_read_ufs = rustReadUfs;
        }

        @Override
        public FilesystemConf getFilesystemConf() {
            return conf;
        }

        @Override
        public void close() {
            closed.countDown();
        }
    }
}
