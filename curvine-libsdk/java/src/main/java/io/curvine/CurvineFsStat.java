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
import io.curvine.proto.GetMasterInfoResponse;
import io.curvine.proto.WorkerInfoProto;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FsStatus;

public class CurvineFsStat extends FsStatus {
    private final GetMasterInfoResponse info;

    public CurvineFsStat(GetMasterInfoResponse info) {
        super(info.getCapacity(), info.getFsUsed(), info.getAvailable());
        this.info = info;
    }

    public GetMasterInfoResponse getInfo() {
        return info;
    }

    public double getPercent(long base, long capacity) {
        if (capacity <= 0) {
            return 0;
        } else {
            return (double) base / (double) capacity * 100;
        }
    }


    public String simple(boolean showWorkers) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("%20s: %s\n", "active_master", info.getActiveMaster()));

        builder.append(String.format("%20s: ", "journal_nodes"));
        for (int i = 0; i < info.getJournalNodesCount(); i++) {
            if (i == 0) {
                builder.append(String.format("%s\n", info.getJournalNodes(i)));
            } else {
                builder.append(String.format("%s%s\n", StringUtils.leftPad(" ", 22), info.getJournalNodes(i)));
            }
        }
        if (info.getJournalNodesCount() == 0) {
            builder.append("\n");
        }

        builder.append(String.format("%20s: %s\n", "capacity", Utils.bytesToString(info.getCapacity())));

        String available = String.format(
                "%20s: %s (%.2f%%)\n",
                "available",
                Utils.bytesToString(info.getAvailable()),
                getPercent(info.getAvailable(), info.getCapacity())
        );
        builder.append(available);

        String used = String.format(
                "%20s: %s (%.2f%%)\n",
                "fs_used",
                Utils.bytesToString(info.getFsUsed()),
                getPercent(info.getFsUsed(), info.getCapacity())
        );
        builder.append(used);

        builder.append(String.format("%20s: %s\n", "non_fs_used", Utils.bytesToString(info.getNonFsUsed())));
        builder.append(String.format("%20s: %s\n", "live_worker_num", info.getLiveWorkersCount()));
        builder.append(String.format("%20s: %s\n", "lost_worker_num", info.getLostWorkersCount()));
        builder.append(String.format("%20s: %s\n", "inode_dir_num", info.getInodeDirNum()));
        builder.append(String.format("%20s: %s\n", "inode_file_num", info.getInodeFileNum()));
        builder.append(String.format("%20s: %s\n", "block_num", info.getBlockNum()));

        if (!showWorkers) {
            return builder.toString();
        }

        // Output worker details
        builder.append(String.format("%20s: ", "live_worker_list"));
        for (int i = 0; i < info.getLiveWorkersCount(); i++) {
            WorkerInfoProto worker = info.getLiveWorkers(i);
            String str = String.format(
                    "%s:%s,%s/%s (%.2f%%)",
                    worker.getAddress().getHostname(),
                    worker.getAddress().getRpcPort(),
                    Utils.bytesToString(worker.getAvailable()),
                    Utils.bytesToString(worker.getCapacity()),
                    getPercent(worker.getAvailable(), worker.getCapacity())
            );

            if (i == 0) {
                builder.append(String.format("%s\n", str));
            } else {
                builder.append(String.format("%s%s\n", StringUtils.leftPad(" ", 22), str));
            }
        }

        if (info.getLiveWorkersCount() == 0) {
            builder.append("\n");
        }

        // Output lost worker details
        builder.append(String.format("%20s: ", "lost_worker_list"));
        for (int i = 0; i < info.getLostWorkersCount(); i++) {
            WorkerInfoProto worker = info.getLostWorkers(i);
            String str = String.format(
                    "%s:%s",
                    worker.getAddress().getHostname(),
                    worker.getAddress().getRpcPort()
            );

            if (i == 0) {
                builder.append(String.format("%s\n", str));
            } else {
                builder.append(String.format("%s%s\n", StringUtils.leftPad(" ", 22), str));
            }
        }
        return builder.toString();
    }

    public String capacity() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < info.getLiveWorkersCount(); i++) {
            WorkerInfoProto worker = info.getLiveWorkers(i);
            String str = String.format(
                    "%s:%s  %s",
                    worker.getAddress().getHostname(),
                    worker.getAddress().getRpcPort(),
                    Utils.bytesToString(worker.getCapacity())
            );
            builder.append(String.format("%s\n", str));
        }

        return builder.toString();
    }

    public String used() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < info.getLiveWorkersCount(); i++) {
            WorkerInfoProto worker = info.getLiveWorkers(i);
            String str = String.format(
                    "%s:%s  %s",
                    worker.getAddress().getHostname(),
                    worker.getAddress().getRpcPort(),
                    Utils.bytesToString(worker.getFsUsed())
            );
            builder.append(String.format("%s\n", str));
        }

        return builder.toString();
    }

    public String available() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < info.getLiveWorkersCount(); i++) {
            WorkerInfoProto worker = info.getLiveWorkers(i);
            String str = String.format(
                    "%s:%s  %s",
                    worker.getAddress().getHostname(),
                    worker.getAddress().getRpcPort(),
                    Utils.bytesToString(worker.getAvailable())
            );
            builder.append(String.format("%s\n", str));
        }

        return builder.toString();
    }
}
