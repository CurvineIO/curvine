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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Set;

final class CurvineNativeLibraryResolver {
    private static final Logger LOGGER = LoggerFactory.getLogger(CurvineNativeLibraryResolver.class);

    static final String OS_RELEASE_FILE = "/etc/os-release";
    static final String LINUX_ID_PREFIX = "ID=";
    static final String LINUX_VERSION_PREFIX = "VERSION_ID=";

    private CurvineNativeLibraryResolver() {
    }

    static String[] getLibraryNames(String sysOs, String osVersion, String arch) {
        if (sysOs.contains("win")) {
            return new String[] {"curvine_libsdk.dll"};
        } else if (sysOs.contains("linux")) {
            String archSuffix = arch + "_64";
            Set<String> libraryNames = new LinkedHashSet<>();
            if (StringUtils.isNotBlank(osVersion) && !"unknown".equalsIgnoreCase(osVersion)) {
                libraryNames.add(String.format("libcurvine_libsdk_%s_%s.so", osVersion, archSuffix));
            }
            libraryNames.add(String.format("libcurvine_libsdk_linux_%s.so", archSuffix));
            libraryNames.add(String.format("libcurvine_libsdk_centos7_%s.so", archSuffix));
            if ("x86".equals(arch)) {
                libraryNames.add("libcurvine_libsdk_rocky9_x86_64.so");
                libraryNames.add("libcurvine_libsdk.so");
            }
            return libraryNames.toArray(new String[0]);
        } else {
            throw new RuntimeException("Unsupported operating systems: " + sysOs);
        }
    }

    static String getNativeArch(String sysArch) {
        if (!sysArch.contains("64")) {
            throw new RuntimeException("Currently only supports 64-bit systems");
        }

        if (sysArch.contains("arm") || sysArch.contains("aarch")) {
            return "aarch";
        } else if (sysArch.contains("x86") || sysArch.contains("amd")) {
            return "x86";
        } else {
            throw new RuntimeException("Unsupported CPU architecture: " + sysArch);
        }
    }

    static boolean isLinux(String sysOs) {
        return sysOs.contains("linux");
    }

    static String getOsVersion(String path) {
        File file = new File(path);
        if (!file.exists()) {
            return "unknown";
        }

        // Use try-with-resources to ensure BufferedReader is properly closed.
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

    static String normalizeOsReleaseVariableValue(String value) {
        // Variable assignment values may be enclosed in double or single quotes.
        return value.trim().replaceAll("[\"']", "");
    }
}
