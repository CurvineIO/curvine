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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

final class NativeDependencyLoader {
    @FunctionalInterface
    interface LibraryLoader {
        void load(String absolutePath);
    }

    private NativeDependencyLoader() {
    }

    /**
     * Extract and load an optional native dependency before loading the JNI library that needs it.
     *
     * @return {@code true} when the dependency resource was present and loaded
     */
    static boolean loadFromJar(
            ClassLoader classLoader,
            File workdir,
            String resourceName,
            String targetFileName,
            LibraryLoader libraryLoader) throws IOException {
        try (InputStream input = classLoader.getResourceAsStream(resourceName)) {
            if (input == null) {
                return false;
            }

            if (!workdir.isDirectory() && !workdir.mkdirs()) {
                throw new IOException("Could not create native work directory: " + workdir.getAbsolutePath());
            }

            File extractionDir = Files.createTempDirectory(workdir.toPath(), "curvine-native-").toFile();
            extractionDir.deleteOnExit();
            File target = new File(extractionDir, targetFileName);
            copyUninterruptibly(input, target);
            target.deleteOnExit();
            libraryLoader.load(target.getAbsolutePath());
            return true;
        }
    }

    private static void copyUninterruptibly(InputStream input, File target) throws IOException {
        try (FileOutputStream output = new FileOutputStream(target)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = input.read(buffer)) >= 0) {
                output.write(buffer, 0, read);
            }
        } catch (IOException e) {
            if (!target.delete()) {
                target.deleteOnExit();
            }
            throw e;
        }
    }
}
