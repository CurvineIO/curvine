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

import java.nio.Buffer;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class CurvineNative {
    public static final Logger LOGGER = LoggerFactory.getLogger(CurvineNative.class);
    private static final Constructor<?> DBB_CONSTRUCTOR;
    private static final Field DBB_ADDRESS;
    private static final File WORKDIR;

    public static final String LIBRARY_PATH = "java.library.path";
    public static final String NATIVE_WORKDIR = "curvine.native.workdir";

    public static String OS_RELEASE_FILE = CurvineNativeLibraryResolver.OS_RELEASE_FILE;
    public static final String LINUX_ID_PREFIX = CurvineNativeLibraryResolver.LINUX_ID_PREFIX;
    public static final String LINUX_VERSION_PREFIX = CurvineNativeLibraryResolver.LINUX_VERSION_PREFIX;
    private static final String JINDOSDK_SONAME = "libjindosdk_c.so.6";

    // Split java.version on non-digit chars:
    private static final int majorVersion =
            Integer.parseInt(System.getProperty("java.version").split("\\D+")[0]);

    static {
        try {
            Class<?> cls = Class.forName("java.nio.DirectByteBuffer");
            Constructor<?> constructor = (majorVersion < 21) ?
                    cls.getDeclaredConstructor(Long.TYPE, Integer.TYPE) :
                    cls.getDeclaredConstructor(Long.TYPE, Long.TYPE);
            constructor.setAccessible(true);
            Field cleanerField = cls.getDeclaredField("cleaner");
            cleanerField.setAccessible(true);
            DBB_CONSTRUCTOR = constructor;

            Field unsafeField = Buffer.class.getDeclaredField("address");
            unsafeField.setAccessible(true);
            DBB_ADDRESS = unsafeField;

            WORKDIR = getWorkerDir();
        } catch (ClassNotFoundException | NoSuchMethodException | NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }


        load();
    }

    static ByteBuffer createBuffer(long[] tmp) throws IOException {
        try {
            return (ByteBuffer) DBB_CONSTRUCTOR.newInstance(tmp[0], (int) tmp[1]);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static long getAddress(ByteBuffer buf) throws IOException {
        if (!buf.isDirect()) {
            throw new IllegalArgumentException("only direct buffer");
        }
        try {
            return DBB_ADDRESS.getLong(buf);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static String getLibraryName() {
        return getLibraryNames()[0];
    }

    public static String[] getLibraryNames() {
        String sysOs = System.getProperty("os.name").toLowerCase();
        String arch = getNativeArch();
        String osVersion = isLinux() ? getOsVersion() : null;

        return getLibraryNames(sysOs, osVersion, arch);
    }

    static String[] getLibraryNames(String sysOs, String osVersion, String arch) {
        return CurvineNativeLibraryResolver.getLibraryNames(sysOs, osVersion, arch);
    }

    private static String getNativeArch() {
        return CurvineNativeLibraryResolver.getNativeArch(
                System.getProperty("os.arch").toLowerCase());
    }

    private static boolean isLinux() {
        return CurvineNativeLibraryResolver.isLinux(System.getProperty("os.name").toLowerCase());
    }

    private static String getJindoLibraryResourceName() {
        if (!isLinux()) {
            return null;
        }
        return String.format("libjindosdk_c_linux_%s_64.so.6", getNativeArch());
    }

    public static String getOsVersion() {
        return getOsVersion(OS_RELEASE_FILE);
    }

    public static String getOsVersion(String path) {
        return CurvineNativeLibraryResolver.getOsVersion(path);
    }

    /**
     * Name passed to {@link System#loadLibrary(String)}: JVM maps {@code foo} -> {@code libfoo.so}. Linux
     * artifacts are {@code libfoo.so}, so strip the {@code lib} prefix from the basename.
     */
    private static String loadLibraryLookupName(String libraryFileName) {
        String base = FilenameUtils.getBaseName(libraryFileName);
        if (libraryFileName.endsWith(".so") && base.startsWith("lib")) {
            return base.substring(3);
        }
        return base;
    }

    /**
     * Try {@link System#load(String)} for each directory in {@code java.library.path} ({@link File#pathSeparator}-separated).
     */
    private static boolean loadFromLibraryPathDirectories(String libraryName) {
        String pathProp = System.getProperty(LIBRARY_PATH);
        if (StringUtils.isEmpty(pathProp)) {
            return false;
        }
        for (String dir : pathProp.split(Pattern.quote(File.pathSeparator))) {
            if (StringUtils.isBlank(dir)) {
                continue;
            }
            File candidate = new File(dir.trim(), libraryName);
            if (!candidate.isFile()) {
                continue;
            }
            System.load(candidate.getAbsolutePath());
            LOGGER.info("Loaded native library {} via System.load ({})", libraryName,
                    candidate.getAbsolutePath());
            return true;
        }
        return false;
    }

    /**
     * Resolves JNI: try {@code loadLibrary}, then concrete paths under {@code java.library.path}, then jar extract.
     * Order avoids broken {@code new File(entire_java.library.path, name)} when multiple dirs are listed, and prefers
     * loading from a real filesystem path before copying to tmp (helps some TLS / dlopen cases).
     */
    public static void load() {
        String[] libraryNames = getLibraryNames();
        Throwable lastFailure = null;
        List<String> failures = new ArrayList<>();

        for (String libraryName : libraryNames) {
            try {
                System.loadLibrary(loadLibraryLookupName(libraryName));
                LOGGER.info("Loaded native library {} via System.loadLibrary", libraryName);
                return;
            } catch (UnsatisfiedLinkError e) {
                lastFailure = e;
                failures.add("System.loadLibrary(" + libraryName + "): " + e);
                LOGGER.debug("System.loadLibrary failed for {}: {}", libraryName, e.toString());
            }
        }

        for (String libraryName : libraryNames) {
            try {
                if (loadFromLibraryPathDirectories(libraryName)) {
                    return;
                }
            } catch (Throwable e) {
                lastFailure = e;
                failures.add("java.library.path(" + libraryName + "): " + e);
                LOGGER.warn("java.library.path directory scan failed for {}", libraryName, e);
            }
        }

        try {
            File extractionDir = createNativeExtractionDir();
            String jindoPath = loadJindoLibraryFromJar(extractionDir);
            if (jindoPath != null) {
                System.load(jindoPath);
                LOGGER.info("Loaded JindoSDK native dependency from jar extract {}", jindoPath);
            }

            for (String libraryName : libraryNames) {
                if (!hasJarResource(libraryName)) {
                    LOGGER.debug("Native library {} was not found inside JAR", libraryName);
                    continue;
                }
                try {
                    File extracted = extractLibraryFromJar(libraryName, new File(extractionDir, libraryName));
                    System.load(extracted.getAbsolutePath());
                    LOGGER.info("Loaded native library {} from jar extract {}", libraryName,
                            extracted.getAbsolutePath());
                    return;
                } catch (Throwable e) {
                    lastFailure = e;
                    failures.add("jar(" + libraryName + "): " + e);
                    LOGGER.warn("Failed to load {} from jar", libraryName, e);
                }
            }
        } catch (Throwable e) {
            lastFailure = e;
            failures.add("jar extraction setup: " + e);
            LOGGER.warn("Failed to load native libraries from jar", e);
        }

        RuntimeException rte = new RuntimeException(
                "Could not load native library. Tried [" + StringUtils.join(libraryNames, ", ")
                        + "]. Failures: " + failures, lastFailure);
        LOGGER.error(rte.getMessage(), lastFailure);
        throw rte;
    }

    public static String loadLibraryFromJar(String libraryName) throws IOException {
        return extractLibraryFromJar(
                libraryName,
                new File(createNativeExtractionDir(), libraryName)
        ).getAbsolutePath();
    }

    private static File createNativeExtractionDir() throws IOException {
        File dir = Files.createTempDirectory(WORKDIR.toPath(), "curvine-native-").toFile();
        dir.deleteOnExit();
        return dir;
    }

    private static String loadJindoLibraryFromJar(File extractionDir) throws IOException {
        String resourceName = getJindoLibraryResourceName();
        if (resourceName == null) {
            return null;
        }

        if (!hasJarResource(resourceName)) {
            LOGGER.debug("JindoSDK native dependency {} was not found inside JAR", resourceName);
            return null;
        }

        return extractLibraryFromJar(resourceName, new File(extractionDir, JINDOSDK_SONAME))
                .getAbsolutePath();
    }

    private static boolean hasJarResource(String resourceName) {
        return CurvineNative.class.getClassLoader().getResource(resourceName) != null;
    }

    private static File extractLibraryFromJar(String libraryName, File outputFile) throws IOException {
        // Load from jar package.
        final File parent = outputFile.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new IOException("Native extraction directory could not be created: "
                    + parent.getAbsolutePath());
        }

        File temp = File.createTempFile(outputFile.getName(), ".tmp", parent);
        temp.deleteOnExit();
        try (final InputStream is = CurvineNative.class.getClassLoader().getResourceAsStream(libraryName)) {
            if (is == null) {
                throw new RuntimeException(libraryName + " was not found inside JAR.");
            }
            copyUninterruptibly(is, temp);
            if (outputFile.exists() && !outputFile.delete()) {
                throw new IOException("Native output file could not be replaced: "
                        + outputFile.getAbsolutePath());
            }
            if (!temp.renameTo(outputFile)) {
                try (FileInputStream in = new FileInputStream(temp)) {
                    copyUninterruptibly(in, outputFile);
                }
            }
        } finally {
            if (temp.exists() && !temp.delete()) {
                LOGGER.debug("Failed to delete temporary native extraction file {}", temp.getAbsolutePath());
            }
        }

        outputFile.setReadable(true);
        outputFile.setExecutable(true);
        outputFile.deleteOnExit();
        return outputFile;
    }

    /**
     * Copy with {@code java.io} so {@link Thread#interrupt()} cannot abort the write via
     * {@link java.nio.channels.ClosedByInterruptException} the way {@code Files.copy} /
     * {@code FileChannel} can. Deletes {@code dest} if the copy fails so a truncated
     * library is not later treated as valid.
     */
    private static void copyUninterruptibly(InputStream in, File dest) throws IOException {
        try (FileOutputStream out = new FileOutputStream(dest)) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) >= 0) {
                out.write(buf, 0, n);
            }
        } catch (IOException e) {
            if (!dest.delete()) {
                dest.deleteOnExit();
            }
            throw e;
        }
    }

    public static File getWorkerDir() {
        String workdir = System.getProperty(NATIVE_WORKDIR);
        if (workdir != null) {
            File f = new File(workdir);
            f.mkdirs();

            try {
                f = f.getAbsoluteFile();
            } catch (Exception ignored) {
                // Good to have an absolute path, but it's OK.
            }
            return f;
        } else {
            return new File(System.getProperty("java.io.tmpdir"));
        }
    }

    static ByteBuffer createBuffer(int len) {
        return ByteBuffer.allocateDirect(len);
    }

    public static String normalizeOsReleaseVariableValue(String value) {
        return CurvineNativeLibraryResolver.normalizeOsReleaseVariableValue(value);
    }

    public static native long newFilesystem(String conf) throws IOException;

    public static native long create(long fs, String path, boolean overwrite) throws IOException;

    public static native long append(long fs, String path, long[] tmp) throws IOException;

    public static native long allocChunk(long nativeHandle, long[] tmp) throws IOException;

    public static native long write(long nativeHandle, long address, int len, long[] tmp) throws IOException;

    public static native long flush(long nativeHandle) throws IOException;

    public static native long closeWriter(long nativeHandle) throws IOException;

    public static native long open(long nativeHandle, String path, long[] tmp) throws IOException;

    public static native long read(long nativeHandle, long[] buf) throws IOException;

    public static native long seek(long nativeHandle, long pos) throws IOException;

    public static native long closeReader(long nativeHandle) throws IOException;

    public static native long closeFilesystem(long nativeHandle) throws IOException;

    public static native long mkdir(long nativeHandle, String path, boolean createParent) throws IOException;

    public static native byte[] getFileStatus(long nativeHandle, String path) throws IOException;

    /** Apply attribute updates and return serialized {@code GetFileStatusResponse} bytes. */
    public static native byte[] setAttr(
            long nativeHandle, String path, byte[] setAttrOptions) throws IOException;

    public static native byte[] listStatus(long nativeHandle, String path) throws IOException;

    public static native long rename(long nativeHandle, String src, String dst) throws IOException;

    public static native long delete(long nativeHandle, String path, boolean recursive) throws IOException;

    /** Release Curvine blocks and metadata for a path and return the released totals. */
    public static native byte[] free(long nativeHandle, String path, boolean recursive) throws IOException;

    public static native byte[] getFilesystemInfo(long nativeHandle) throws IOException;

    /**
     * @deprecated renamed to {@link #getFilesystemInfo(long)}; this RPC returns
     * whole-filesystem statistics, not master-process information. Kept as a
     * non-native forwarder for source compatibility; will be removed in a
     * future release.
     */
    @Deprecated
    public static byte[] getMasterInfo(long nativeHandle) throws IOException {
        return getFilesystemInfo(nativeHandle);
    }

    public static native byte[] getMountInfo(long nativeHandle, String path) throws IOException;

    /** Create or update a UFS mount with a serialized {@code MountOptionsProto}. */
    public static native long mount(
            long nativeHandle, String ufsPath, String cvPath, byte[] mountOptions) throws IOException;

    /** Remove a UFS mount by Curvine path. */
    public static native long unmount(long nativeHandle, String cvPath) throws IOException;

    /** Return a serialized {@code GetMountTableResponse} protobuf. */
    public static native byte[] getMountTable(long nativeHandle) throws IOException;

    public static native String togglePath(long nativeHandle, String path, boolean checkCache) throws IOException;

    /**
     * Submit a UFS-to-Curvine load job.
     *
     * @param nativeHandle filesystem native handle
     * @param sourcePath UFS path or mounted CV path
     * @param targetPath optional explicit CV target path; may be null
     * @param overwrite whether to overwrite existing target
     * @return serialized {@code SubmitJobResponse} protobuf bytes
     */
    public static native byte[] submitLoadJob(
            long nativeHandle, String sourcePath, String targetPath, boolean overwrite)
            throws IOException;

    /** Submit a Curvine-to-UFS export job. */
    public static native byte[] submitExportJob(
            long nativeHandle, String sourcePath, boolean overwrite) throws IOException;

    /**
     * Query load job status by job id.
     *
     * @return serialized {@code GetJobStatusResponse} protobuf bytes
     */
    public static native byte[] getJobStatus(long nativeHandle, String jobId) throws IOException;

    /** Cancel a load job by job id. */
    public static native long cancelJob(long nativeHandle, String jobId) throws IOException;

    /** Retry a failed, partial-success, or canceled transfer job. */
    public static native String retryJob(long nativeHandle, String jobId) throws IOException;
}
