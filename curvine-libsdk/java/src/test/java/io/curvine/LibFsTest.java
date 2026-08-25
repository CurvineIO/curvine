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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.File;

public class LibFsTest {
    @Test
    public void conf() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.cv.master_addrs", "localhost:9001,localhost:9002");
        conf.set("fs.cv.io_threads", "12");
        conf.set("fs.cv.rpc_timeout_ms", "300");
        conf.set("fs.cv.short_circuit", "false");

        FilesystemConf filesystemConf = new FilesystemConf(conf);
        System.out.println(filesystemConf);

        assert filesystemConf.master_addrs.equals("localhost:9001,localhost:9002");
        assert filesystemConf.io_threads == 12;
        assert filesystemConf.rpc_timeout_ms == 300;
        assert !filesystemConf.short_circuit;
    }

    @Test
    public void trimsMasterAddrsWhitespace() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.cv.master_addrs", "localhost:9001, localhost:9002,localhost:9003");
        conf.set("fs.cv.io_threads", " 16 ");

        FilesystemConf filesystemConf = new FilesystemConf(conf);

        assert filesystemConf.master_addrs.equals("localhost:9001,localhost:9002,localhost:9003");
        assert filesystemConf.io_threads == 16;
    }

    @Test
    public void namespacedMasterAddrsAreNormalized() throws Exception {
        // initialize() overwrites constructor master_addrs via getMasterAddrs(authority).
        // Exercise that helper directly so a revert of normalizeCsv there fails the test.
        CurvineFileSystem fs = new CurvineFileSystem();
        Configuration conf = new Configuration();
        conf.set("fs.cv.ns1.master_addrs", "h1:8995, h2:8995,h3:8995");
        fs.setConf(conf);

        assert fs.getMasterAddrs("ns1").equals("h1:8995,h2:8995,h3:8995");
    }

    @Test
    public void clientHostnameEnvValuesAreTrimmed() {
        assert FilesystemConf.trimEnvHostname(" 10.0.0.8 \n").equals("10.0.0.8");
        assert FilesystemConf.trimEnvHostname("  ") == null;
        assert FilesystemConf.trimEnvHostname(null) == null;
    }

    @Test
    public void jni1() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.cv.master_addrs", "localhost:6995");
        conf.set("fs.cv.io_threads", "12");
        conf.set("fs.cv.rpc_timeout_ms", "300");
        conf.set("fs.cv.short_circuit", "false");


        FilesystemConf filesystemConf = new FilesystemConf(conf);

        long h = CurvineNative.newFilesystem(filesystemConf.toToml());
        long open = CurvineNative.open(h, "/test", new long[0]);
        System.out.println(open);
    }

    @Test
    public void osVersion() throws Exception {
        String ver = CurvineNative.getOsVersion("src/test/resources/os-version");
        System.out.println(ver);
    }
}
