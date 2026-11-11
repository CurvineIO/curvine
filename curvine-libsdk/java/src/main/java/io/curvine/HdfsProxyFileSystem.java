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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * Transparent Curvine read acceleration for {@code hdfs://} paths.
 *
 * <p>Enable with {@code fs.hdfs.impl=io.curvine.HdfsProxyFileSystem}. Reads of files under a
 * Curvine mount of the HDFS path are served from Curvine; reads of unmounted paths, and all
 * writes, metadata operations and delegation tokens, use the native {@link DistributedFileSystem}.
 * If Curvine is disabled or fails to open a file, the read falls back to HDFS.
 *
 * <p>{@code openFile()} needs no override: {@link DistributedFileSystem} does not override
 * {@code openFileWithOptions}, whose default implementation delegates to {@link #open(Path, int)}.
 */
public class HdfsProxyFileSystem extends DistributedFileSystem {
    private CurvineFilesystemProvider provider;

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        provider = new CurvineFilesystemProvider(conf);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        // Mount points are keyed by the full UFS URI, so resolve relative paths first.
        FSDataInputStream cvInputStream = CurvineProxyOpener.tryOpen(
                curvineFs(), makeQualified(path), bufferSize, () -> openUfs(path, bufferSize));
        return cvInputStream != null ? cvInputStream : openUfs(path, bufferSize);
    }

    /** The Curvine filesystem, or {@code null} when the proxy is disabled. */
    CurvineFileSystem curvineFs() {
        return provider == null ? null : provider.getFs();
    }

    FSDataInputStream openUfs(Path path, int bufferSize) throws IOException {
        return super.open(path, bufferSize);
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }
}
