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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CurvineNativeLibraryNamesTest {

    @Test
    public void centos8FallsBackToCentos7ThenLinux() {
        List<String> names = CurvineNativeLibraryNames.getLibraryNames("linux", "x86_64", "centos8");

        assertEquals(Arrays.asList(
                "libcurvine_libsdk_centos8_x86_64.so",
                "libcurvine_libsdk_centos7_x86_64.so",
                "libcurvine_libsdk_linux_x86_64.so"
        ), names);
    }

    @Test
    public void ubuntuFallsBackToLinuxOnly() {
        List<String> names = CurvineNativeLibraryNames.getLibraryNames("linux", "x86_64", "ubuntu20");

        assertEquals(Arrays.asList(
                "libcurvine_libsdk_ubuntu20_x86_64.so",
                "libcurvine_libsdk_linux_x86_64.so"
        ), names);
    }

    @Test
    public void curvineLibraryUsesMatchingJindoDependencyForX86() {
        assertEquals("libjindosdk_c_linux_x86_64.so.6",
                CurvineNativeLibraryNames.getDependencyLibraryName("libcurvine_libsdk_linux_x86_64.so"));
        assertEquals("libjindosdk_c.so.6",
                CurvineNativeLibraryNames.getDependencySoname("libcurvine_libsdk_linux_x86_64.so"));
    }

    @Test
    public void curvineLibraryUsesMatchingJindoDependencyForArm() {
        assertEquals("libjindosdk_c_linux_aarch_64.so.6",
                CurvineNativeLibraryNames.getDependencyLibraryName("libcurvine_libsdk_linux_aarch_64.so"));
        assertEquals("libjindosdk_c.so.6",
                CurvineNativeLibraryNames.getDependencySoname("libcurvine_libsdk_linux_aarch_64.so"));
    }

    @Test
    public void centos8StillUsesMatchingJindoDependency() {
        assertEquals("libjindosdk_c_linux_x86_64.so.6",
                CurvineNativeLibraryNames.getDependencyLibraryName("libcurvine_libsdk_centos8_x86_64.so"));
    }
}
