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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NativeDependencyLoaderTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void packagedDependencyIsExtractedToUniqueDirectoryAndLoadedWithExpectedSoname() throws Exception {
        AtomicReference<String> loadedPath = new AtomicReference<>();

        boolean loaded = NativeDependencyLoader.loadFromJar(
                getClass().getClassLoader(),
                temporaryFolder.getRoot(),
                "native/test-jindo-runtime.so",
                "libjindosdk_c.so.6",
                absolutePath -> {
                    loadedPath.set(absolutePath);
                    try {
                        assertEquals(
                                "test Jindo runtime\n",
                                new String(
                                        Files.readAllBytes(new File(absolutePath).toPath()),
                                        StandardCharsets.UTF_8));
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                });

        assertTrue(loaded);
        assertNotNull(loadedPath.get());

        File extracted = new File(loadedPath.get());
        assertEquals("libjindosdk_c.so.6", extracted.getName());
        assertEquals(temporaryFolder.getRoot(), extracted.getParentFile().getParentFile());
        assertEquals("test Jindo runtime\n", new String(Files.readAllBytes(extracted.toPath()), StandardCharsets.UTF_8));
    }

    @Test
    public void independentExtractionsUseDifferentDirectories() throws Exception {
        AtomicReference<String> firstPath = new AtomicReference<>();
        AtomicReference<String> secondPath = new AtomicReference<>();

        assertTrue(NativeDependencyLoader.loadFromJar(
                getClass().getClassLoader(),
                temporaryFolder.getRoot(),
                "native/test-jindo-runtime.so",
                "libjindosdk_c.so.6",
                firstPath::set));
        assertTrue(NativeDependencyLoader.loadFromJar(
                getClass().getClassLoader(),
                temporaryFolder.getRoot(),
                "native/test-jindo-runtime.so",
                "libjindosdk_c.so.6",
                secondPath::set));

        assertFalse(firstPath.get().equals(secondPath.get()));
        assertFalse(new File(firstPath.get()).getParent().equals(new File(secondPath.get()).getParent()));
    }

    @Test
    public void missingOptionalDependencyDoesNotExtractOrLoad() throws Exception {
        AtomicReference<String> loadedPath = new AtomicReference<>();

        boolean loaded = NativeDependencyLoader.loadFromJar(
                getClass().getClassLoader(),
                temporaryFolder.getRoot(),
                "native/missing-jindo-runtime.so",
                "libjindosdk_c.so.6",
                loadedPath::set);

        assertFalse(loaded);
        assertNull(loadedPath.get());
        assertFalse(new File(temporaryFolder.getRoot(), "libjindosdk_c.so.6").exists());
    }

    @Test
    public void loadedDependencySatisfiesDependentLibrarySoname() throws Exception {
        Assume.assumeTrue(isLinuxWithCCompiler());

        File buildDir = temporaryFolder.newFolder("native-build");
        File dependencySource = new File(buildDir, "jindo.c");
        File dependency = new File(buildDir, "libjindosdk_c.so.6");
        File consumerSource = new File(buildDir, "curvine.c");
        File consumer = new File(buildDir, "libcurvine-test.so");

        Files.write(dependencySource.toPath(),
                "int jindo_symbol(void) { return 42; }\n".getBytes(StandardCharsets.UTF_8));
        runCompiler(buildDir,
                "-shared",
                "-fPIC",
                "-Wl,-soname,libjindosdk_c.so.6",
                "-o",
                dependency.getAbsolutePath(),
                dependencySource.getAbsolutePath());

        Files.write(consumerSource.toPath(),
                "extern int jindo_symbol(void);\n"
                        .concat("int curvine_symbol(void) { return jindo_symbol(); }\n")
                        .getBytes(StandardCharsets.UTF_8));
        runCompiler(buildDir,
                "-shared",
                "-fPIC",
                "-Wl,-z,defs",
                "-Wl,-z,now",
                "-o",
                consumer.getAbsolutePath(),
                consumerSource.getAbsolutePath(),
                "-L",
                buildDir.getAbsolutePath(),
                "-Wl,-l:libjindosdk_c.so.6");

        byte[] dependencyBytes = Files.readAllBytes(dependency.toPath());
        assertTrue(dependency.delete());
        ClassLoader resourceClassLoader = new ClassLoader(null) {
            @Override
            public InputStream getResourceAsStream(String name) {
                return "native/test-jindo-runtime.so".equals(name)
                        ? new ByteArrayInputStream(dependencyBytes)
                        : null;
            }
        };

        assertTrue(NativeDependencyLoader.loadFromJar(
                resourceClassLoader,
                temporaryFolder.newFolder("native-workdir"),
                "native/test-jindo-runtime.so",
                "libjindosdk_c.so.6",
                System::load));
        System.load(consumer.getAbsolutePath());
    }

    private static boolean isLinuxWithCCompiler() {
        if (!System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("linux")) {
            return false;
        }
        try {
            return new ProcessBuilder("cc", "--version").start().waitFor() == 0;
        } catch (Exception ignored) {
            return false;
        }
    }

    private static void runCompiler(File workdir, String... args) throws Exception {
        List<String> command = new ArrayList<>();
        command.add("cc");
        for (String arg : args) {
            command.add(arg);
        }

        Process process = new ProcessBuilder(command)
                .directory(workdir)
                .inheritIO()
                .start();
        assertEquals("native compiler failed: " + command, 0, process.waitFor());
    }
}
