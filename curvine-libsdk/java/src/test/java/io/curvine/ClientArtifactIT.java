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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.jar.JarFile;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;

/** Verifies the package installed directly into a Hadoop host, not project classes. */
public class ClientArtifactIT {
    @Test
    public void clientJarEmbedsRelocatedProtobufWithoutS3aProxyRuntime() throws Exception {
        File jar = findClientArtifact();

        try (JarFile clientJar = new JarFile(jar)) {
            Assert.assertNotNull(clientJar.getJarEntry("curvine_shaded/com/google/protobuf/MessageLite.class"));
            Assert.assertNull(clientJar.getJarEntry("com/google/protobuf/MessageLite.class"));
            Assert.assertNull(clientJar.getJarEntry("org/apache/commons/io/FilenameUtils.class"));
            Assert.assertNull(clientJar.getJarEntry("org/apache/commons/lang3/StringUtils.class"));
            Assert.assertNull(clientJar.getJarEntry("org/apache/hadoop/fs/FileSystem.class"));
            Assert.assertNull(clientJar.getJarEntry("io/curvine/S3aProxyFileSystem.class"));
            Assert.assertNull(clientJar.getJarEntry("io/curvine/CurvineFilesystemProvider.class"));
            Assert.assertNull(clientJar.getJarEntry("io/curvine/CurvineFallbackInputStream.class"));
            Assert.assertNull(clientJar.getJarEntry("io/curvine/CurvineShell.class"));
            Assert.assertNull(clientJar.getJarEntry("org/apache/curator/framework/CuratorFramework.class"));
            Assert.assertNull(clientJar.getJarEntry("org/apache/zookeeper/Watcher.class"));
            Assert.assertNull(clientJar.getJarEntry("curvine_shaded/org/apache/curator/framework/CuratorFramework.class"));
            Assert.assertNull(clientJar.getJarEntry("curvine_shaded/org/apache/zookeeper/Watcher.class"));
            assertClassDoesNotReference(clientJar, "io/curvine/CurvineInputStream.class", "javax/annotation/");
            assertClassDoesNotReference(clientJar, "io/curvine/CurvineOutputStream.class", "javax/annotation/");
        }

        try (URLClassLoader loader = new URLClassLoader(new URL[] {jar.toURI().toURL()}, null)) {
            Assert.assertEquals(
                    "io.curvine.proto.GetMountTableResponse",
                    Class.forName("io.curvine.proto.GetMountTableResponse", true, loader).getName());
        }
    }

    @Test
    public void clientJarLoadsEmbeddedNativeDependencies() throws Exception {
        Assume.assumeTrue(
                "native smoke test runs in the Linux x86_64 release packager",
                System.getProperty("os.name").toLowerCase().contains("linux")
                        && (System.getProperty("os.arch").contains("amd64")
                                || System.getProperty("os.arch").contains("x86_64")));
        File jar = findClientArtifact();
        try (JarFile clientJar = new JarFile(jar)) {
            Assume.assumeTrue(
                    "native resources are added by the release packaging job",
                    clientJar.getJarEntry("libcurvine_libsdk_alinux3_x86_64.so") != null);
        }

        File testClasses = new File(
                ClientArtifactNativeSmoke.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        File slf4jApi = new File(Logger.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        String classpath = testClasses.getAbsolutePath()
                + File.pathSeparator + jar.getAbsolutePath()
                + File.pathSeparator + slf4jApi.getAbsolutePath();
        Process process = new ProcessBuilder(
                new File(System.getProperty("java.home"), "bin/java").getAbsolutePath(),
                "-cp", classpath, ClientArtifactNativeSmoke.class.getName())
                .redirectErrorStream(true)
                .start();
        String output = readAll(process.getInputStream());
        Assert.assertEquals("native smoke test failed:\n" + output, 0, process.waitFor());
    }

    private static File findClientArtifact() {
        File[] clientArtifacts = new File("target").listFiles(
                (directory, name) -> name.startsWith("curvine-hadoop-") && name.endsWith("-client.jar"));
        Assert.assertNotNull("could not list Maven target directory", clientArtifacts);
        Assert.assertEquals("expected exactly one client artifact", 1, clientArtifacts.length);
        return clientArtifacts[0];
    }

    private static void assertClassDoesNotReference(JarFile jar, String className, String reference)
            throws IOException {
        try (InputStream input = jar.getInputStream(jar.getJarEntry(className));
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int length;
            while ((length = input.read(buffer)) >= 0) {
                output.write(buffer, 0, length);
            }
            Assert.assertFalse(
                    className + " references " + reference,
                    new String(output.toByteArray(), StandardCharsets.ISO_8859_1).contains(reference));
        }
    }

    private static String readAll(InputStream input) throws IOException {
        try (InputStream stream = input; ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int length;
            while ((length = stream.read(buffer)) >= 0) {
                output.write(buffer, 0, length);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        }
    }
}
