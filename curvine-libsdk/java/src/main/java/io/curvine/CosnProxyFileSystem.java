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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosNFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Transparent acceleration for COSN: Hive tables keep their {@code cosn://}
 * locations and jobs opt in per-job through {@code fs.cv.proxy.enabled}.
 *
 * Reads of paths mounted in Curvine are served from Curvine (with optional
 * fallback to COSN on cache miss); everything else - unmounted reads and all
 * writes - goes to the native CosNFileSystem.
 *
 * Configure with {@code fs.cosn.impl=io.curvine.CosnProxyFileSystem}. The
 * FileSystem.openFile() builder path also lands here because the Hadoop
 * base-class default of openFileWithOptions delegates to open(Path, int).
 */
public class CosnProxyFileSystem extends CosNFileSystem {
    public static final Logger LOGGER = LoggerFactory.getLogger(CosnProxyFileSystem.class);

    public CurvineFilesystemProvider provider;

    @Override
    public void initialize(URI name, Configuration originalConf) throws IOException {
        super.initialize(name, originalConf);
        provider = new CurvineFilesystemProvider(originalConf);
    }

    FSDataInputStream openCvInputStream(Path path, int bufferSize) {
        Path cvPath = null;
        try {
            CurvineFileSystem fs = provider.getFs();
            if (fs == null) {
                // Cache functionality is not enabled.
                return null;
            }
            cvPath = fs.togglePath(path, true).orElse(null);
            if (cvPath == null) {
                // Path is not mounted
                return null;
            }

            FSInputStream inputStream = (FSInputStream) fs.open(cvPath, bufferSize).getWrappedStream();
            if (fs.getFilesystemConf().enable_fallback_read_ufs) {
                inputStream = new CurvineFallbackInputStream(inputStream, () -> {
                    try {
                        return super.open(path, bufferSize);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            return new FSDataInputStream(inputStream);
        } catch (Exception e) {
            LOGGER.warn("Failed to open curvine file {} -> {}: {}", cvPath, path, e.getMessage());
            return null;
        }
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        FSDataInputStream cvInputStream = openCvInputStream(path, bufferSize);
        if (cvInputStream != null) {
            return cvInputStream;
        } else {
            return super.open(path, bufferSize);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        provider.close();
    }
}
