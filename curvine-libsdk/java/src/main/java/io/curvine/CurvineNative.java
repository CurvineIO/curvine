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

import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.regex.Pattern;

public class CurvineNative {
    public static final Logger LOGGER = LoggerFactory.getLogger(CurvineNative.class);
    private static final Constructor<?> DBB_CONSTRUCTOR;
    private static final File WORKDIR;


    public static final String LIBRARY_PATH = "java.library.path";
    public static final String NATIVE_WORKDIR = "curvine.native.workdir";

    public static String OS_RELEASE_FILE = "/etc/os-release";
    public static final String LINUX_ID_PREFIX = "ID=";
    public static final String LINUX_VERSION_PREFIX = "VERSION_ID=";

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

    public static String getLibraryName() {
        String sysOs = System.getProperty("os.name").toLowerCase();
        String sysArch = System.getProperty("os.arch").toLowerCase();

        // Determine platform type
        String arch;
        if (sysArch.contains("arm") || sysArch.contains("aarch")) {
            arch = "aarch";
        } else if (sysArch.contains("x86") || sysArch.contains("amd")) {
            arch = "x86";
        } else {
            throw new RuntimeException("Unsupported CPU architecture: " + sysArch);
        }

        if (!sysArch.contains("64")) {
            throw new RuntimeException("Currently only supports 64-bit systems");
        }

        if (sysOs.contains("win")) {
            return "curvine_libsdk.dll";
        } else if (sysOs.contains("linux")) {
            String osVersion = getOsVersion();
            return String.format("libcurvine_libsdk_%s_%s_64.so", osVersion, arch);
        } else {
            throw new RuntimeException("Unsupported operating systems: " + sysOs);
        }
    }

    public static String getOsVersion() {
        return getOsVersion(OS_RELEASE_FILE);
    }

    public static String getOsVersion(String path) {
        File file = new File(path);
        if (!file.exists()) {
            return "unknown";
        }

        // Use try-with-resources to ensure BufferedReader is properly closed
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            String line;
            String id = null;
            String version = null;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith(LINUX_ID_PREFIX)) {
                    id = normalizeOsReleaseVariableValue(line.substring(LINUX_ID_PREFIX.length()));
                } else if (line.startsWith(LINUX_VERSION_PREFIX)) {
                    version = normalizeOsReleaseVariableValue(line.substring(LINUX_VERSION_PREFIX.length()));
                    String[] split = version.split("\\.");
                    if (split.length > 0) {
                        version = split[0];
                    }
                }
            }

            if (id == null || version == null) {
                throw new RuntimeException("No os version was parsed");
            }
            return id.toLowerCase() + version;
        } catch (Exception e) {
            LOGGER.warn("Failed to parse the os version", e);
            return "unknown";
        }
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
        String libraryName = getLibraryName();
        Throwable lastFailure = null;

        try {
            System.loadLibrary(loadLibraryLookupName(libraryName));
            LOGGER.info("Loaded native library {} via System.loadLibrary", libraryName);
            return;
        } catch (UnsatisfiedLinkError e) {
            LOGGER.debug("System.loadLibrary failed for {}: {}", libraryName, e.toString());
        }

        try {
            if (loadFromLibraryPathDirectories(libraryName)) {
                return;
            }
        } catch (Throwable e) {
            lastFailure = e;
            LOGGER.warn("java.library.path directory scan failed for {}", libraryName, e);
        }

        try {
            String extracted = loadLibraryFromJar(libraryName);
            System.load(extracted);
            LOGGER.info("Loaded native library {} from jar extract {}", libraryName, extracted);
            return;
        } catch (Throwable e) {
            lastFailure = e;
            LOGGER.warn("Failed to load {} from jar", libraryName, e);
        }

        RuntimeException rte = new RuntimeException(
                "Could not load native library " + libraryName, lastFailure);
        LOGGER.error(rte.getMessage(), lastFailure);
        throw rte;
    }

    public static String loadLibraryFromJar(String libraryName) throws IOException {
        // Load from jar package.
        final File temp = File.createTempFile(
                FilenameUtils.getBaseName(libraryName),
                "." + FilenameUtils.getExtension(libraryName),
                WORKDIR
        );
        if (temp.exists() && !temp.delete()) {
            throw new RuntimeException("File: " + temp.getAbsolutePath()
                    + " already exists and cannot be removed.");
        }
        if (!temp.createNewFile()) {
            throw new RuntimeException("File: " + temp.getAbsolutePath()
                    + " could not be created.");
        }

        if (!temp.exists()) {
            throw new RuntimeException("File " + temp.getAbsolutePath() + " does not exist.");
        } else {
            temp.deleteOnExit();
        }

        try (final InputStream is = CurvineNative.class.getClassLoader().getResourceAsStream(libraryName)) {
            if (is == null) {
                throw new RuntimeException(libraryName + " was not found inside JAR.");
            } else {
                Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }
        return temp.getAbsolutePath();
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
        // Variable assignment values may be enclosed in double or single quotes.
        return value.trim().replaceAll("[\"']", "");
    }

    public static native long newFilesystem(String conf) throws IOException;

    public static native long create(long fs, String path, boolean overwrite) throws IOException;

    public static native long append(long fs, String path, long[] tmp) throws IOException;

    public static native long write(long nativeHandle, long address, int len) throws IOException;

    public static native long flush(long nativeHandle) throws IOException;

    public static native long closeWriter(long nativeHandle) throws IOException;

    public static native long open(long nativeHandle, String path, long[] tmp) throws IOException;

    public static native long read(long nativeHandle, long[] buf) throws IOException;

    public static native long seek(long nativeHandle, long pos) throws IOException;

    public static native long closeReader(long nativeHandle) throws IOException;

    public static native long closeFilesystem(long nativeHandle) throws IOException;

    public static native long mkdir(long nativeHandle, String path, boolean createParent) throws IOException;

    public static native byte[] getFileStatus(long nativeHandle, String path) throws IOException;

    public static native byte[] listStatus(long nativeHandle, String path) throws IOException;

    public static native long rename(long nativeHandle, String src, String dst) throws IOException;

    public static native long delete(long nativeHandle, String path, boolean recursive) throws IOException;

    public static native byte[] getMasterInfo(long nativeHandle) throws IOException;

    public static native byte[] getMountInfo(long nativeHandle, String path) throws IOException;

    public static native String togglePath(long nativeHandle, String path, boolean checkCache) throws IOException;
}
