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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Read path shared by the transparent proxy filesystems ({@link S3aProxyFileSystem},
 * {@link HdfsProxyFileSystem}): a UFS path that is mounted in Curvine is served from
 * Curvine; anything else is left to the native filesystem.
 */
final class CurvineProxyOpener {
    private static final Logger LOGGER = LoggerFactory.getLogger(CurvineProxyOpener.class);

    /** Opens the same file through the native (UFS) filesystem. */
    @FunctionalInterface
    interface UfsOpener {
        FSDataInputStream open() throws IOException;
    }

    private CurvineProxyOpener() {
    }

    /**
     * Try to open {@code ufsPath} through Curvine.
     *
     * @param fs        the Curvine filesystem, or {@code null} when the proxy is disabled
     * @param ufsPath   fully qualified UFS path (e.g. {@code hdfs://ns/warehouse/a.parquet})
     * @param ufsOpener reopens the file natively when a Curvine read fails mid-stream
     * @return a stream served by Curvine, or {@code null} when the caller must use the
     *         native filesystem (proxy disabled, path not mounted, or Curvine open failed)
     */
    static FSDataInputStream tryOpen(CurvineFileSystem fs, Path ufsPath, int bufferSize,
                                     UfsOpener ufsOpener) {
        if (fs == null) {
            return null;
        }

        Path cvPath = null;
        try {
            cvPath = fs.togglePath(ufsPath, true).orElse(null);
            if (cvPath == null) {
                return null;
            }

            FSInputStream inputStream = (FSInputStream) fs.open(cvPath, bufferSize).getWrappedStream();
            if (fs.getFilesystemConf().enable_fallback_read_ufs) {
                inputStream = new CurvineFallbackInputStream(inputStream, () -> {
                    try {
                        return ufsOpener.open();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
            return new FSDataInputStream(inputStream);
        } catch (Exception e) {
            if (isCacheMiss(e)) {
                // Mounted but not cached (or cache invalid): the normal miss path.
                LOGGER.debug("Curvine cache miss {} -> {}", ufsPath, cvPath);
            } else {
                LOGGER.warn("Failed to open curvine file {} -> {}: {}", cvPath, ufsPath, e.getMessage());
            }
            return null;
        }
    }

    /**
     * The proxy requires {@code enable_rust_read_ufs=false}, so on a miss Curvine refuses to
     * read the UFS itself and reports {@code UNSUPPORTED_UFS_READ}; the caller then reads natively.
     */
    static boolean isCacheMiss(Exception e) {
        return e instanceof CurvineException
                && ((CurvineException) e).getErrno() == CurvineException.UNSUPPORTED_UFS_READ;
    }
}
