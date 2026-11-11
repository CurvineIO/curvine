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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import io.curvine.exception.CurvineException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Pure-Java tests for the transparent HDFS proxy read path. The native library is not
 * required: {@link FakeCurvineFs} replaces mount lookup and Curvine reads.
 */
public class HdfsProxyFileSystemTest {
    private static final String UFS_ROOT = "hdfs://ns1/warehouse";
    private static final String CV_ROOT = "/hdfs-cache/warehouse";

    private static final byte[] CV_DATA = "curvine-data".getBytes(StandardCharsets.UTF_8);
    private static final byte[] UFS_DATA = "hdfs!!-data!".getBytes(StandardCharsets.UTF_8);

    @Test
    public void disabledProxyUsesNativeFilesystem() throws Exception {
        assertNull(CurvineProxyOpener.tryOpen(null, new Path(UFS_ROOT + "/a"), 4096, failingUfs()));
    }

    @Test
    public void unmountedPathUsesNativeFilesystem() throws Exception {
        FakeCurvineFs fs = new FakeCurvineFs(true);
        assertNull(CurvineProxyOpener.tryOpen(fs, new Path("hdfs://ns1/other/a"), 4096, failingUfs()));
        assertTrue(fs.opened.isEmpty());
    }

    @Test
    public void mountedPathIsServedFromCurvine() throws Exception {
        FakeCurvineFs fs = new FakeCurvineFs(true);
        FSDataInputStream in = CurvineProxyOpener.tryOpen(fs, new Path(UFS_ROOT + "/a"), 4096, failingUfs());

        assertEquals(1, fs.opened.size());
        assertEquals(new Path(CV_ROOT + "/a"), fs.opened.get(0));
        assertTrue(in.getWrappedStream() instanceof CurvineFallbackInputStream);
        assertArrayEquals(CV_DATA, readAll(in));
    }

    @Test
    public void fallbackWrapperIsSkippedWhenDisabled() throws Exception {
        FakeCurvineFs fs = new FakeCurvineFs(false);
        FSDataInputStream in = CurvineProxyOpener.tryOpen(fs, new Path(UFS_ROOT + "/a"), 4096, failingUfs());
        assertTrue(in.getWrappedStream() instanceof BytesInputStream);
    }

    @Test
    public void curvineOpenFailureUsesNativeFilesystem() throws Exception {
        FakeCurvineFs fs = new FakeCurvineFs(true);
        fs.openError = new IOException("curvine unavailable");
        assertNull(CurvineProxyOpener.tryOpen(fs, new Path(UFS_ROOT + "/a"), 4096, failingUfs()));
    }

    @Test
    public void cacheMissUsesNativeFilesystem() throws Exception {
        FakeCurvineFs fs = new FakeCurvineFs(true);
        fs.openError = CurvineException.create(
                CurvineException.UNSUPPORTED_UFS_READ, "File /hdfs-cache/warehouse/a unsupported ufs read");
        assertNull(CurvineProxyOpener.tryOpen(fs, new Path(UFS_ROOT + "/a"), 4096, failingUfs()));
    }

    @Test
    public void onlyUnsupportedUfsReadIsCacheMiss() {
        assertTrue(CurvineProxyOpener.isCacheMiss(CurvineException.create(
                CurvineException.UNSUPPORTED_UFS_READ, "File /a unsupported ufs read")));
        assertFalse(CurvineProxyOpener.isCacheMiss(
                new CurvineException(CurvineException.TIMEOUT, "rpc timeout")));
        assertFalse(CurvineProxyOpener.isCacheMiss(new IOException("worker down")));
    }

    @Test
    public void curvineReadFailureContinuesFromUfsAtSamePosition() throws Exception {
        FakeCurvineFs fs = new FakeCurvineFs(true);
        fs.failReadAt = 4;
        FSDataInputStream in = CurvineProxyOpener.tryOpen(fs, new Path(UFS_ROOT + "/a"), 4096,
                () -> new FSDataInputStream(new BytesInputStream(UFS_DATA, -1)));

        byte[] out = readAll(in);
        // First 4 bytes come from Curvine, the rest from HDFS starting at offset 4.
        assertEquals(UFS_DATA.length, out.length);
        assertEquals("curv",new String(out, 0, 4, StandardCharsets.UTF_8));
        assertEquals("!!-data!", new String(out, 4, out.length - 4, StandardCharsets.UTF_8));
    }

    @Test
    public void proxyQualifiesPathAndFallsBackToHdfs() throws Exception {
        FakeCurvineFs fs = new FakeCurvineFs(true);
        TestProxy proxy = new TestProxy(fs);

        FSDataInputStream mounted = proxy.open(new Path("/warehouse/a"), 4096);
        assertArrayEquals(CV_DATA, readAll(mounted));
        assertEquals(new Path(CV_ROOT + "/a"), fs.opened.get(0));
        assertEquals(0, proxy.ufsOpens);

        FSDataInputStream unmounted = proxy.open(new Path("/other/a"), 4096);
        assertArrayEquals(UFS_DATA, readAll(unmounted));
        assertEquals(1, proxy.ufsOpens);
    }

    @Test
    public void proxyUsesHdfsWhenCurvineDisabled() throws Exception {
        TestProxy proxy = new TestProxy(null);
        assertArrayEquals(UFS_DATA, readAll(proxy.open(new Path("/warehouse/a"), 4096)));
        assertEquals(1, proxy.ufsOpens);
    }

    private static CurvineProxyOpener.UfsOpener failingUfs() {
        return () -> {
            throw new AssertionError("native filesystem must not be opened");
        };
    }

    private static byte[] readAll(FSDataInputStream in) throws IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] buf = new byte[3];
        int n;
        while ((n = in.read(buf, 0, buf.length)) != -1) {
            out.write(buf, 0, n);
        }
        in.close();
        return out.toByteArray();
    }

    /** Proxy with the HDFS client and Curvine provider replaced; no cluster needed. */
    private static class TestProxy extends HdfsProxyFileSystem {
        private final CurvineFileSystem cv;
        int ufsOpens;

        TestProxy(CurvineFileSystem cv) {
            this.cv = cv;
        }

        @Override
        public URI getUri() {
            return URI.create("hdfs://ns1");
        }

        @Override
        CurvineFileSystem curvineFs() {
            return cv;
        }

        @Override
        FSDataInputStream openUfs(Path path, int bufferSize) {
            ufsOpens++;
            return new FSDataInputStream(new BytesInputStream(UFS_DATA, -1));
        }
    }

    /** Mounts {@link #UFS_ROOT} at {@link #CV_ROOT} and serves {@link #CV_DATA}. */
    private static class FakeCurvineFs extends CurvineFileSystem {
        private final FilesystemConf conf;
        final List<Path> opened = new ArrayList<>();
        IOException openError;
        int failReadAt = -1;

        FakeCurvineFs(boolean fallback) throws Exception {
            conf = new FilesystemConf(new Configuration(false));
            conf.enable_fallback_read_ufs = fallback;
        }

        @Override
        public FilesystemConf getFilesystemConf() {
            return conf;
        }

        @Override
        public Optional<Path> togglePath(Path path, boolean checkCache) {
            String p = path.toString();
            if (p.equals(UFS_ROOT) || p.startsWith(UFS_ROOT + "/")) {
                return Optional.of(new Path(CV_ROOT + p.substring(UFS_ROOT.length())));
            }
            return Optional.empty();
        }

        @Override
        public FSDataInputStream open(Path path, int bufferSize) throws IOException {
            if (openError != null) {
                throw openError;
            }
            opened.add(path);
            return new FSDataInputStream(new BytesInputStream(CV_DATA, failReadAt));
        }
    }

    /** In-memory seekable stream that throws once {@code failAt} bytes have been read. */
    private static class BytesInputStream extends FSInputStream {
        private final byte[] data;
        private final int failAt;
        private int pos;

        BytesInputStream(byte[] data, int failAt) {
            this.data = data;
            this.failAt = failAt;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            return read(one, 0, 1) == -1 ? -1 : one[0] & 0xff;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            if (failAt >= 0 && pos >= failAt) {
                throw new IOException("worker down");
            }
            if (pos >= data.length) {
                return -1;
            }
            int limit = failAt >= 0 ? Math.min(data.length, failAt) : data.length;
            int n = Math.min(len, limit - pos);
            System.arraycopy(data, pos, buf, off, n);
            pos += n;
            return n;
        }

        @Override
        public void seek(long target) {
            pos = (int) target;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public boolean seekToNewSource(long target) {
            return false;
        }
    }
}
