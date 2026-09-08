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
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CurvineJavaUtilsTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void byteFromStringParsesBinaryUnitsWithoutHadoop3RuntimeClasses() {
        assertEquals(0L, CurvineJavaUtils.byteFromString("0"));
        assertEquals(42L, CurvineJavaUtils.byteFromString("42"));
        assertEquals(128L * 1024L, CurvineJavaUtils.byteFromString("128KB"));
        assertEquals(10L * 1024L * 1024L, CurvineJavaUtils.byteFromString("10MB"));
        assertEquals(10L * 1024L * 1024L * 1024L, CurvineJavaUtils.byteFromString("10g"));
        assertEquals(1536L, CurvineJavaUtils.byteFromString("1.5 KB"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void byteFromStringRejectsUnknownUnits() {
        CurvineJavaUtils.byteFromString("10XB");
    }

    @Test
    public void getCurvineConfSetsHadoopFileSystemImplementations() throws Exception {
        File confDir = temporaryFolder.newFolder("curvine-conf");
        File confFile = new File(confDir, "curvine-site.xml");
        Files.write(confFile.toPath(), Arrays.asList("<configuration>", "</configuration>"),
                StandardCharsets.UTF_8);

        String previous = System.getProperty("curvine.conf.dir");
        try {
            System.setProperty("curvine.conf.dir", confDir.getAbsolutePath());
            Configuration conf = CurvineJavaUtils.getCurvineConf();

            assertEquals("io.curvine.CurvineFileSystem", conf.get("fs.cv.impl"));
            assertEquals("io.curvine.CurvineAbstractFileSystem",
                    conf.get("fs.AbstractFileSystem.cv.impl"));
            assertNull(conf.get("fs.curvine.impl"));
        } finally {
            restoreProperty("curvine.conf.dir", previous);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void getCurvineConfRequiresConfigurationDirectory() {
        String previous = System.getProperty("curvine.conf.dir");
        try {
            System.clearProperty("curvine.conf.dir");
            CurvineJavaUtils.getCurvineConf();
        } finally {
            restoreProperty("curvine.conf.dir", previous);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void getCurvineConfRequiresCurvineSiteXml() throws Exception {
        File confDir = temporaryFolder.newFolder("missing-curvine-conf");
        String previous = System.getProperty("curvine.conf.dir");
        try {
            System.setProperty("curvine.conf.dir", confDir.getAbsolutePath());
            CurvineJavaUtils.getCurvineConf();
        } finally {
            restoreProperty("curvine.conf.dir", previous);
        }
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }
}
