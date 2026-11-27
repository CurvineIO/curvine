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
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

public class S3aProxyFileSystem extends S3AFileSystem {
    public CurvineFilesystemProvider provider;
    @Override
    public void initialize(URI name, Configuration originalConf) throws IOException {
        super.initialize(name, originalConf);
        provider = new CurvineFilesystemProvider(originalConf);
    }

    FSDataInputStream openCvInputStream(Path path, int bufferSize) {
        return CurvineProxyOpener.tryOpen(provider.getFs(), path, bufferSize,
                () -> super.open(path, bufferSize));
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
    public CompletableFuture<FSDataInputStream> openFileWithOptions(Path rawPath, OpenFileParameters parameters) throws IOException {
        FSDataInputStream cvInputStream = openCvInputStream(rawPath, parameters.getBufferSize());
        if (cvInputStream != null) {
            return CompletableFuture.completedFuture(cvInputStream);
        } else {
            return super.openFileWithOptions(rawPath, parameters);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        provider.close();
    }
}
