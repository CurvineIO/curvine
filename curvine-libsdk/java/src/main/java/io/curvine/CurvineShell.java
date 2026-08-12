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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.util.ToolRunner;
import com.google.protobuf.util.JsonFormat;

// curvine command line tool, is a wrapper class for hdfs fs shell
public class CurvineShell {
    public static void main(String[] args) throws Exception {
        String cmd;
        if (args.length == 0) {
            cmd = "";
        } else {
            cmd = args[0];
        }

        String[] params = new String[] {};
        if (args.length > 1) {
            params = new String[args.length - 1];
            System.arraycopy(args, 1, params, 0, args.length - 1);
        }

        switch (cmd) {
            case "fs":
                runFsShell(params);
                break;

            case "report":
                runReport(params);
                break;

            default:
                throw new RuntimeException("Unsupported operation type:" + args[0]);
        }
    }

    public static void runFsShell(String[] args) throws Exception {
        Configuration conf = Utils.getCurvineConf();
        conf.setQuietMode(false);

        FsShell shell = new FsShell(conf);
        int res;
        try {
            res = ToolRunner.run(shell, args);
        } finally {
            shell.close();
        }
        System.exit(res);
    }

    public static void runReport(String[] args) throws Exception {
        Configuration conf = Utils.getCurvineConf();
        conf.setQuietMode(false);

        try (FileSystem fs = FileSystem.get(conf)) {
            CurvineFsStat stat = ((CurvineFileSystem) fs).getFsStat();
            if (args.length == 0) {
                System.out.println(stat.simple(false));
            } else {
                String action = args[0].toLowerCase();
                switch (action) {
                    case "json":
                        System.out.println(JsonFormat.printer().print(stat.getInfo()));
                        break;
                    case "info":
                        System.out.println(stat.simple(true));
                        break;
                    case "capacity":
                        System.out.println(stat.capacity());
                        break;
                    case "used":
                        System.out.println(stat.used());
                        break;
                    case "available":
                        System.out.println(stat.available());
                        break;
                    default:
                        System.out.println(stat.simple(false));
                        break;
                }
            }
        }
        System.exit(0);
    }
}
