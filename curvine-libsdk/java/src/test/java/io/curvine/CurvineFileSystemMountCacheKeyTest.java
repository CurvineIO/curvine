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

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Pure-Java tests for {@link CurvineFileSystem#mountCacheKey(FilesystemConf)}.
 * Does not load JNI.
 */
public class CurvineFileSystemMountCacheKeyTest {

    @Test
    public void sameMasterDifferentUnifiedFsProducesDifferentKeys() throws Exception {
        FilesystemConf unified = conf("localhost:8995", true, false);
        FilesystemConf pureCv = conf("localhost:8995", false, false);

        String unifiedKey = CurvineFileSystem.mountCacheKey(unified);
        String pureCvKey = CurvineFileSystem.mountCacheKey(pureCv);

        assertEquals("localhost:8995|unified=true|rust_ufs=false", unifiedKey);
        assertEquals("localhost:8995|unified=false|rust_ufs=false", pureCvKey);
        assertNotEquals(unifiedKey, pureCvKey);
    }

    @Test
    public void sameMasterDifferentRustUfsProducesDifferentKeys() throws Exception {
        FilesystemConf cacheLookup = conf("localhost:8995", true, false);
        FilesystemConf rustUfs = conf("localhost:8995", true, true);

        assertNotEquals(
                CurvineFileSystem.mountCacheKey(cacheLookup),
                CurvineFileSystem.mountCacheKey(rustUfs));
    }

    @Test
    public void defaultSingleClientKeepsStableKeyPerMaster() throws Exception {
        FilesystemConf first = conf("master-0:8995", true, false);
        FilesystemConf second = conf("master-0:8995", true, false);

        assertEquals(
                "master-0:8995|unified=true|rust_ufs=false",
                CurvineFileSystem.mountCacheKey(first));
        assertEquals(
                CurvineFileSystem.mountCacheKey(first),
                CurvineFileSystem.mountCacheKey(second));
    }

    private static FilesystemConf conf(String masterAddrs, boolean unified, boolean rustUfs)
            throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.cv.master_addrs", masterAddrs);
        conf.set("fs.cv.enable_unified_fs", Boolean.toString(unified));
        conf.set("fs.cv.enable_rust_read_ufs", Boolean.toString(rustUfs));
        return new FilesystemConf(conf);
    }
}
