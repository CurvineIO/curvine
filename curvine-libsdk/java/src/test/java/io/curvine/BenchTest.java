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

import io.curvine.bench.CurvineBenchV2;
import io.curvine.bench.NNBenchWithoutMR;
import io.curvine.bench.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class BenchTest {

    static {
        String useDir = System.getProperty("user.dir");
        System.setProperty("java.library.path", useDir + "/../../target/debug");
        System.setProperty("curvine.conf.dir", useDir + "/../../etc");

        System.setProperty("fs.cv.master_addrs", "localhost:60705");
        System.setProperty("fs.cv.log_level", "debug");
        // System.setProperty("fs.cv.client_hostname", "localhost");
    }

    @Test
    public void bytesUnit() {
        long l = Utils.byteFromString("10MB");
        System.out.println(l);
        assert l == 10 * 1024 * 1024;

        System.out.println(Utils.bytesToString(l));
    }

    @Test
    public void benchV2() {
        String userNIO = "true";
        String[] writerArgs = {
                "-action", "fs.write",
                "-useNIO", userNIO,
                "-dataDir", "/target/bench",
                "-fileNum", "1",
                "-fileSize", "10MB",
        };
        CurvineBenchV2.main(writerArgs);

        String[] readArgs = {
                "-action", "fs.read",
                "-useNIO", userNIO,
                "-dataDir", "/target/bench",
                "-fileNum", "1",
                "-fileSize", "10MB",
        };
        CurvineBenchV2.main(readArgs);
    }

    @Test
    public void conf() throws Exception {
        Configuration conf = Utils.getCurvineConf();
        FilesystemConf fsConf = new FilesystemConf(conf);

        System.out.println(fsConf);
    }

    @Test
    public void metaCreateWrite() throws Exception {
        String[] args = {
                "-confDir", System.getProperty("curvine.conf.dir"),
                "-baseDir", "cv:///fs-meta",
                "-numFiles", "100",
                "-threads", "10",
                "-operation", "createWrite",
                "-bytesToWrite", "0"
        };

        NNBenchWithoutMR.main(args);
    }

    @Test
    public void metaRename() throws Exception {
        String[] args = {
                "-confDir", System.getProperty("curvine.conf.dir"),
                "-baseDir", "cv:///fs-meta",
                "-numFiles", "100",
                "-threads", "10",
                "-operation", "rename",
                "-bytesToWrite", "0"
        };

        NNBenchWithoutMR.main(args);
    }

    @Test
    public void metaDelete() throws Exception {
        String[] args = {
                "-confDir", System.getProperty("curvine.conf.dir"),
                "-baseDir", "cv:///fs-meta",
                "-numFiles", "100",
                "-threads", "10",
                "-operation", "delete",
                "-bytesToWrite", "0"
        };

        NNBenchWithoutMR.main(args);
    }

    @Test
    public void hdfsShell() throws Exception {
        Configuration conf = Utils.getCurvineConf();
        conf.setQuietMode(false);

        FsShell shell = new FsShell(conf);

        int res;
        try {
            String[] args = {
                    "-ls", "/fs-meta/N80338435"
            };
            res = ToolRunner.run(shell, args);
        } finally {
            shell.close();
        }
        System.exit(res);
    }

    @Test
    public void fsShell() throws Exception {
        String[] args = {
                "fs", "-ls", "/"
        };

        CurvineShell.main(args);
    }
}
