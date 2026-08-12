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

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.github.luben.zstd.util.Native;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

public class SparkShuffleTransferToMmapTest {
    private static final String TEST_DIR_PROPERTY = "curvine.shuffle.test.dir";
    private static final String ZSTD_JNI_LIB_BASE = "libzstd-jni-1.5.6-3";
    private static final String ZSTD_JNI_LIB_NAME = "zstd-jni-1.5.6-3";
    private static final int RAW_DATA_SIZE = 8 * 1024 * 1024;
    private static final int ZSTD_LEVEL = 3;

    public static void main(String[] args) throws Exception {
        checkZstdNative();
        Path testDir = createTestDir(args);
        Path shuffleOutput = run(testDir);
        // System.out.println("Spark shuffle transferTo mmap check passed: " + shuffleOutput);
    }

    private static void checkZstdNative() throws IOException {
        loadZstdNativeWithPath();
        int magicNumber = Zstd.magicNumber();
        // System.out.println("zstd native loaded, magic number: 0x" + Integer.toHexString(magicNumber));
    }

    private static void loadZstdNativeWithPath() throws IOException {
        if (Native.isLoaded()) {
            return;
        }

        String extension = zstdNativeExtension();
        String resourceName = zstdNativeResourceName(extension);

        String overridePath = System.getProperty("ZstdNativePath");
        if (overridePath != null) {
            System.load(overridePath);
            Native.assumeLoaded();
            return;
        }

        try {
            Class.forName("org.osgi.framework.BundleEvent");
            System.loadLibrary(ZSTD_JNI_LIB_NAME);
            Native.assumeLoaded();
            return;
        } catch (Throwable ignored) {
            // Follow zstd-jni Native.load(): fall back to unpacking from jar resources.
        }

        InputStream input = SparkShuffleTransferToMmapTest.class.getResourceAsStream(resourceName);
        if (input == null) {
            try {
                System.loadLibrary(ZSTD_JNI_LIB_NAME);
                Native.assumeLoaded();
                return;
            } catch (UnsatisfiedLinkError e) {
                throw new IOException("Cannot find zstd native resource " + resourceName
                        + " or load " + ZSTD_JNI_LIB_NAME + " from system libraries: " + e.getMessage(), e);
            }
        }

        File nativeFile = null;
        FileOutputStream output = null;
        try {
            nativeFile = File.createTempFile(ZSTD_JNI_LIB_BASE, "." + extension, null);
            nativeFile.deleteOnExit();
            // System.out.println("zstd native resource: " + resourceName);
            // System.out.println("zstd native extract path: " + nativeFile.getAbsolutePath());

            output = new FileOutputStream(nativeFile);
            byte[] buffer = new byte[4096];
            while (true) {
                int read = input.read(buffer);
                if (read == -1) {
                    break;
                }
                output.write(buffer, 0, read);
            }
            output.flush();
            output.close();
            output = null;

            try {
                System.load(nativeFile.getAbsolutePath());
            } catch (UnsatisfiedLinkError first) {
                try {
                    System.loadLibrary(ZSTD_JNI_LIB_NAME);
                } catch (UnsatisfiedLinkError second) {
                    throw new IOException(first.getMessage() + "\n" + second.getMessage(), second);
                }
            }

            Native.assumeLoaded();
        } catch (IOException e) {
            String nativePath = nativeFile == null
                    ? new File(System.getProperty("java.io.tmpdir")).getAbsolutePath()
                    : nativeFile.getAbsolutePath();
            throw new IOException("Cannot unpack " + ZSTD_JNI_LIB_BASE + " to "
                    + nativePath + ": " + e.getMessage(), e);
        } finally {
            try {
                input.close();
                if (output != null) {
                    output.close();
                }
                if (nativeFile != null && nativeFile.exists()) {
                    nativeFile.delete();
                }
            } catch (IOException ignored) {
                // Same cleanup behavior as zstd-jni Native.load(): ignore cleanup failures.
            }
        }
    }

    private static String zstdNativeResourceName(String extension) {
        return "/" + zstdNativeOsName() + "/" + System.getProperty("os.arch")
                + "/" + ZSTD_JNI_LIB_BASE + "." + extension;
    }

    private static String zstdNativeOsName() {
        String osName = System.getProperty("os.name").toLowerCase().replace(' ', '_');
        if (osName.startsWith("win")) {
            return "win";
        }
        if (osName.startsWith("mac")) {
            return "darwin";
        }
        return osName;
    }

    private static String zstdNativeExtension() {
        String osName = zstdNativeOsName();
        if (osName.contains("os_x") || osName.contains("darwin")) {
            return "dylib";
        }
        if (osName.contains("win")) {
            return "dll";
        }
        return "so";
    }

    public static Path run(Path testDir) throws Exception {
        testDir = testDir.toAbsolutePath().normalize();
        Files.createDirectories(testDir);
        check(Files.isDirectory(testDir), "test path is not a directory: " + testDir);
        check(Files.isWritable(testDir), "test directory is not writable: " + testDir);

        String uuid = UUID.randomUUID().toString();
        Path compressedSource = testDir.resolve("shuffle-block-" + uuid + ".zst.source");
        Path shuffleTemp = testDir.resolve("shuffle-output-" + uuid + ".data.tmp");
        Path shuffleRenamed = testDir.resolve("shuffle-output-" + uuid + ".data.renamed");
        Path shuffleOutput = testDir.resolve("shuffle-output-" + uuid + ".data");
        // System.out.println("test dir: " + testDir);
        // System.out.println("source file: " + compressedSource);
        // System.out.println("temp file: " + shuffleTemp);
        // System.out.println("final file: " + shuffleOutput);

        byte[] expected = createShuffleData(RAW_DATA_SIZE);

        try (FileChannel channel = FileChannel.open(compressedSource,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
             ZstdOutputStream zstdOutput = new ZstdOutputStream(Channels.newOutputStream(channel), ZSTD_LEVEL)) {
            int offset = 0;
            while (offset < expected.length) {
                int length = Math.min(64 * 1024, expected.length - offset);
                zstdOutput.write(expected, offset, length);
                offset += length;
            }
        }

        check(Files.exists(compressedSource), "compressed source file does not exist after write: " + compressedSource);
        long compressedSize = Files.size(compressedSource);
        check(compressedSize > 0, "zstd compressed data should not be empty");

        Exception original = null;
        try {
            writeByTransferTo(compressedSource, shuffleTemp);
            moveToFinalFile(shuffleTemp, shuffleRenamed);
            moveToFinalFile(shuffleRenamed, shuffleOutput);

            check(Files.notExists(shuffleTemp), "temporary shuffle file should be renamed away");
            check(Files.notExists(shuffleRenamed), "intermediate shuffle file should be renamed away");
            check(Files.size(shuffleOutput) == compressedSize, "final shuffle file size mismatch");

            byte[] actual = readZstdFileByMmap(shuffleOutput, expected.length);
            check(Arrays.equals(expected, actual), "mmap read data is different from original shuffle data");
            Files.delete(shuffleOutput);
            check(Files.notExists(shuffleOutput), shuffleOutput + "  final shuffle file should be deleted");
        } catch (Exception e) {
            original = e;
        }

        cleanupFiles(original, compressedSource, shuffleTemp, shuffleRenamed, shuffleOutput);

        if (original != null) {
            throw original;
        }
        return shuffleOutput;
    }

    private static Path createTestDir(String[] args) throws IOException {
        if (args != null && args.length > 0 && args[0] != null && !args[0].trim().isEmpty()) {
            Path rootPath = Paths.get(args[0]);
            Files.createDirectories(rootPath);
            return rootPath;
        }

        String root = System.getProperty(TEST_DIR_PROPERTY);
        if (root == null || root.trim().isEmpty()) {
            return Files.createTempDirectory("curvine-shuffle-mmap-");
        }

        Path rootPath = Paths.get(root);
        Files.createDirectories(rootPath);
        return rootPath;
    }

    private static byte[] createShuffleData(int size) {
        byte[] data = new byte[size];
        byte[] rowPrefix = "partition=00042,map=00007,record=".getBytes(StandardCharsets.UTF_8);
        Random random = new Random(20250627L);

        int offset = 0;
        int recordId = 0;
        while (offset < data.length) {
            for (int i = 0; i < rowPrefix.length && offset < data.length; i++) {
                data[offset++] = rowPrefix[i];
            }

            int value = recordId++;
            for (int i = 0; i < 16 && offset < data.length; i++) {
                data[offset++] = (byte) ('0' + Math.abs(value % 10));
                value /= 10;
            }

            for (int i = 0; i < 96 && offset < data.length; i++) {
                data[offset++] = (byte) ('a' + random.nextInt(8));
            }

            if (offset < data.length) {
                data[offset++] = '\n';
            }
        }
        return data;
    }

    private static void writeByTransferTo(Path sourcePath, Path targetPath) throws IOException {
        try (FileChannel source = FileChannel.open(sourcePath, StandardOpenOption.READ);
             FileChannel target = FileChannel.open(targetPath,
                     StandardOpenOption.CREATE_NEW,
                     StandardOpenOption.WRITE)) {
            long position = 0;
            long size = source.size();
            while (position < size) {
                long written = source.transferTo(position, size - position, target);
                if (written <= 0) {
                    throw new IOException("FileChannel.transferTo made no progress at position " + position);
                }
                position += written;
            }
            target.force(true);
            check(size == position, "transferTo should copy the whole compressed block");
        }
    }

    private static void moveToFinalFile(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static byte[] readZstdFileByMmap(Path path, int decompressedSize) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            MappedByteBuffer mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            byte[] data = new byte[decompressedSize];
            int offset = 0;
            try (ZstdInputStream zstdInput = new ZstdInputStream(new ByteBufferBackedInputStream(mapped))) {
                while (offset < data.length) {
                    int read = zstdInput.read(data, offset, data.length - offset);
                    if (read == -1) {
                        break;
                    }
                    offset += read;
                }
                check(zstdInput.read() == -1, "zstd stream has more decompressed bytes than expected");
            }

            check(offset == decompressedSize, "decompressed size mismatch");
            return data;
        }
    }

    private static void cleanupFiles(Exception original, Path... paths) throws IOException {
        IOException cleanupError = null;
        for (Path path : paths) {
            if (path == null) {
                continue;
            }
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                if (cleanupError == null) {
                    cleanupError = e;
                } else {
                    cleanupError.addSuppressed(e);
                }
            }
        }
        if (cleanupError != null) {
            if (original != null) {
                original.addSuppressed(cleanupError);
            } else {
                throw cleanupError;
            }
        }
    }

    private static void check(boolean condition, String message) throws IOException {
        if (!condition) {
            throw new IOException(message);
        }
    }

    private static final class ByteBufferBackedInputStream extends InputStream {
        private final ByteBuffer buffer;

        private ByteBufferBackedInputStream(ByteBuffer buffer) {
            this.buffer = buffer.slice();
        }

        @Override
        public int read() {
            if (!buffer.hasRemaining()) {
                return -1;
            }
            return buffer.get() & 0xff;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) {
            if (length == 0) {
                return 0;
            }
            if (!buffer.hasRemaining()) {
                return -1;
            }
            int read = Math.min(length, buffer.remaining());
            buffer.get(bytes, offset, read);
            return read;
        }

        @Override
        public long skip(long n) {
            if (n <= 0) {
                return 0;
            }
            int skipped = (int) Math.min(n, buffer.remaining());
            buffer.position(buffer.position() + skipped);
            return skipped;
        }

        @Override
        public int available() {
            return buffer.remaining();
        }
    }
}
