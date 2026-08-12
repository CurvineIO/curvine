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

import io.curvine.bench.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

// Test curvine command line tool
public class ShellTest {

    static {
        String useDir = System.getProperty("user.dir");
        System.setProperty("java.library.path", useDir + "/../../target/debug");
        System.setProperty("curvine.conf.dir", useDir + "/../../etc");
        // System.setProperty("fs.curvine.master_addrs", "localhost:2001,localhost:1001,localhost:3001");
    }

    public void run(String[] args) throws Exception {
        Configuration conf = Utils.getCurvineConf();
        conf.setQuietMode(false);

        FsShell shell = new FsShell(conf);
        try {
            ToolRunner.run(shell, args);
        } finally {
            shell.close();
        }
    }

    @Test
    public void put() throws Exception {
        String[] args = {
                "-put", "src/main/resources/log4j.properties", "/3.log"
        };
        run(args);

        String[] args1 = {
                "-cat", "/3.log"
        };
        run(args1);
    }

    @Test
    public void mkdir() throws Exception {
        String[] args = {
               "-mkdir", "/user"
        };
        run(args);
    }

    @Test
    public void df() throws Exception {
        String[] args = {
                "-df", "-h"
        };
        run(args);
    }

    @Test
    public void report() throws Exception {
        String[] args = {
               "report", "info"
        };
        CurvineShell.main(args);

    }
}
