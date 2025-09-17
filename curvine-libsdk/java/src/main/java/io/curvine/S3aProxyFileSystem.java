package io.curvine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

public class S3aProxyFileSystem extends S3AFileSystem {
    public static final Logger LOGGER = LoggerFactory.getLogger(S3aProxyFileSystem.class);

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
