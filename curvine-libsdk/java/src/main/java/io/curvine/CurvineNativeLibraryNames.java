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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

final class CurvineNativeLibraryNames {
    private CurvineNativeLibraryNames() {
    }

    static List<String> getLibraryNames(String sysOs, String sysArch, String osVersion) {
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
            return Collections.singletonList("curvine_libsdk.dll");
        } else if (sysOs.contains("linux")) {
            return buildLinuxLibraryCandidates(osVersion, arch);
        } else {
            throw new RuntimeException("Unsupported operating systems: " + sysOs);
        }
    }

    static String getDependencyLibraryName(String libraryName) {
        if (libraryName.contains("_aarch_64")) {
            return "libjindosdk_c_linux_aarch_64.so.6";
        } else if (libraryName.contains("_x86_64")) {
            return "libjindosdk_c_linux_x86_64.so.6";
        }
        throw new RuntimeException("Unsupported native library name: " + libraryName);
    }

    static String getDependencySoname(String libraryName) {
        return "libjindosdk_c.so.6";
    }

    private static List<String> buildLinuxLibraryCandidates(String osVersion, String arch) {
        LinkedHashSet<String> candidates = new LinkedHashSet<>();
        candidates.add(String.format("libcurvine_libsdk_%s_%s_64.so", osVersion, arch));

        if (osVersion.startsWith("centos")) {
            candidates.add(String.format("libcurvine_libsdk_centos7_%s_64.so", arch));
        } else if (osVersion.startsWith("rocky")) {
            candidates.add(String.format("libcurvine_libsdk_centos7_%s_64.so", arch));
        }

        candidates.add(String.format("libcurvine_libsdk_linux_%s_64.so", arch));
        return new ArrayList<>(candidates);
    }
}
