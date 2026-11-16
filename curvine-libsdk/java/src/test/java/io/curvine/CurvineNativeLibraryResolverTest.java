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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/** Pure-logic tests for native library name resolution; needs no native library or cluster. */
public class CurvineNativeLibraryResolverTest {
    @Test
    public void macLibraryNamesPreferBundledDylib() {
        Assert.assertArrayEquals(new String[] {"libcurvine_libsdk_mac_aarch_64.dylib", "libcurvine_libsdk.dylib"},
                CurvineNativeLibraryResolver.getLibraryNames("mac os x", null, "aarch"));
        Assert.assertArrayEquals(new String[] {"libcurvine_libsdk_mac_x86_64.dylib", "libcurvine_libsdk.dylib"},
                CurvineNativeLibraryResolver.getLibraryNames("mac os x", null, "x86"));
        // "darwin" contains "win" and must not be treated as Windows.
        Assert.assertArrayEquals(new String[] {"libcurvine_libsdk_mac_aarch_64.dylib", "libcurvine_libsdk.dylib"},
                CurvineNativeLibraryResolver.getLibraryNames("darwin", null, "aarch"));
    }

    @Test
    public void windowsLibraryNames() {
        Assert.assertArrayEquals(new String[] {"curvine_libsdk.dll"},
                CurvineNativeLibraryResolver.getLibraryNames("windows 10", null, "x86"));
        Assert.assertArrayEquals(new String[] {"curvine_libsdk.dll"},
                CurvineNativeLibraryResolver.getLibraryNames("windows server 2019", null, "x86"));
    }

    @Test
    public void loadLibraryLookupNameStripsLibPrefix() {
        Assert.assertEquals("curvine_libsdk", CurvineNativeLibraryResolver.loadLibraryLookupName("libcurvine_libsdk.dylib"));
        Assert.assertEquals("curvine_libsdk_linux_x86_64",
                CurvineNativeLibraryResolver.loadLibraryLookupName("libcurvine_libsdk_linux_x86_64.so"));
        Assert.assertEquals("curvine_libsdk", CurvineNativeLibraryResolver.loadLibraryLookupName("curvine_libsdk.dll"));
    }

    @Test
    public void linuxLibraryNamesFallBackToGenericLinux() {
        List<String> x86 = Arrays.asList(CurvineNativeLibraryResolver.getLibraryNames("linux", "ubuntu20", "x86"));
        Assert.assertEquals("libcurvine_libsdk_ubuntu20_x86_64.so", x86.get(0));
        Assert.assertTrue(x86.contains("libcurvine_libsdk_linux_x86_64.so"));
        Assert.assertTrue(x86.contains("libcurvine_libsdk_centos7_x86_64.so"));
        Assert.assertTrue(x86.contains("libcurvine_libsdk_amzn2_x86_64.so"));
        Assert.assertTrue(x86.contains("libcurvine_libsdk_alinux3_x86_64.so"));
        Assert.assertTrue(x86.contains("libcurvine_libsdk_rocky9_x86_64.so"));
        Assert.assertTrue(x86.contains("libcurvine_libsdk.so"));

        List<String> arm = Arrays.asList(CurvineNativeLibraryResolver.getLibraryNames("linux", "ubuntu20", "aarch"));
        Assert.assertEquals("libcurvine_libsdk_ubuntu20_aarch_64.so", arm.get(0));
        Assert.assertTrue(arm.contains("libcurvine_libsdk_linux_aarch_64.so"));
        Assert.assertTrue(arm.contains("libcurvine_libsdk_centos7_aarch_64.so"));
        Assert.assertTrue(arm.contains("libcurvine_libsdk_amzn2_aarch_64.so"));
        Assert.assertTrue(arm.contains("libcurvine_libsdk_alinux3_aarch_64.so"));
        Assert.assertTrue(arm.contains("libcurvine_libsdk_rocky9_aarch_64.so"));
        Assert.assertFalse(arm.contains("libcurvine_libsdk.so"));
    }
}
