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

package io.curvine.bench;

import org.apache.commons.cli.*;

import java.io.IOException;

public class BenchParams {
    public String action = "write";
    public String dataDir = "cv:///fs-bench";
    public int threads = 10;
    public int bufferSize = 64 * 1024;
    public long fileSize = 100 * 1024 * 1024;
    public int fileNum = 10;
    public boolean checksum = true;
    public boolean clearDir = false;
    public boolean useNIO = true;

    public static BenchParams buildFromArgs(String[] args) throws IOException {
Options options = new Options();
        options.addOption("action", true, "Operation Type")
                .addOption("dataDir", true, "Directory for reading and writing of data")
                .addOption("threads", true, "Number of Concurrent Threads")
                .addOption("bufferSize", true, "data size per read and write")
                .addOption("fileSize", true, "file size")
                .addOption("fileNum", true, "file number")
                .addOption("checksum", true, "Whether to check checksum")
                .addOption("clearDir", true, "Whether to clean the directory")
                .addOption("useNIO", true, "Whether to read and write local disk to use java nio");

        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("java [OPTION]...", options);
            System.exit(1);
        }

        BenchParams params = new BenchParams();
        params.action = cmd.getOptionValue("action", params.action);
        params.dataDir = cmd.getOptionValue("dataDir", params.dataDir);
        params.threads = Integer.parseInt(cmd.getOptionValue("threads", params.threads + ""));
        params.bufferSize = (int) Utils.byteFromString(cmd.getOptionValue("bufferSize", params.bufferSize + "B"));
        params.fileSize = Utils.byteFromString(cmd.getOptionValue("fileSize", params.fileSize + "B"));
        params.fileNum = Integer.parseInt(cmd.getOptionValue("fileNum", params.fileNum + ""));
        params.checksum = Boolean.parseBoolean(cmd.getOptionValue("checksum", params.checksum + ""));
        params.clearDir = Boolean.parseBoolean(cmd.getOptionValue("clearDir", params.clearDir + ""));
        params.useNIO = Boolean.parseBoolean(cmd.getOptionValue("useNIO", params.useNIO + ""));

        return params;
    }

    public long totalSize() {
        return fileSize * fileNum;
    }

    public String totalSizeString() {
        return Utils.bytesToString(fileSize * fileNum);
    }

    @Override
    public String toString() {
        return "BenchParams{" +
                "action='" + action + '\'' +
                ", dataDir='" + dataDir + '\'' +
                ", threads=" + threads +
                ", bufferSize=" + Utils.bytesToString(bufferSize) +
                ", fileSize=" + Utils.bytesToString(fileSize) +
                ", fileNum=" + fileNum +
                ", clearDir=" + clearDir +
                '}';
    }
}