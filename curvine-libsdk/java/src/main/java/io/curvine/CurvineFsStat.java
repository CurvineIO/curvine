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
import io.curvine.proto.GetFilesystemInfoResponse;
import io.curvine.proto.WorkerInfoProto;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FsStatus;

public class CurvineFsStat extends FsStatus {
    private final GetFilesystemInfoResponse info;

    public CurvineFsStat(GetFilesystemInfoResponse info) {
        // super(...) must be the first statement, so the allocatable fallback
        // and used derivation live in static helpers below. See the comment on
        // allocatableCapacity/allocatableAvailable for the rationale.
        super(allocatableCapacity(info), allocatableUsed(info), allocatableRemaining(info));
        this.info = info;
    }

    public GetFilesystemInfoResponse getInfo() {
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

        // Allocatable (writable) view: capacity/available eligible for new
        // writes (Live workers only). Absent on legacy masters, so guard with
        // hasAllocatableCapacity() to avoid printing a misleading 0.
        if (info.hasAllocatableCapacity()) {
            builder.append(String.format("%20s: %s\n", "allocatable_capacity", Utils.bytesToString(info.getAllocatableCapacity())));
            builder.append(String.format(
                    "%20s: %s (%.2f%%)\n",
                    "allocatable_available",
                    Utils.bytesToString(info.getAllocatableAvailable()),
                    getPercent(info.getAllocatableAvailable(), info.getAllocatableCapacity())
            ));
        }

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

    // The allocatable (writable) view: capacity/available eligible for new
    // writes (Live workers only). Legacy masters omit tags 15/16; protobuf
    // returns 0 for an absent optional int64 with default=0, so we must guard
    // with hasAllocatableCapacity()/hasAllocatableAvailable() and fall back to
    // the aggregate totals — otherwise a new client against an old master
    // would report zero free space.
    //
    // Used is derived as Capacity - Remaining rather than info.getFsUsed()
    // (which sums every non-lost worker, including Blacklist/Decommission).
    // Capacity/Remaining are Live-only, so deriving Used keeps the FsStatus
    // triple self-consistent (Used == Capacity - Remaining) even when
    // non-writable workers still hold data — otherwise getUsed() could
    // exceed getCapacity() - getRemaining() and report a misleading ratio.
    // The master-reported total fs_used is still surfaced in simple().
    // These are static so they can run before super(...) completes.
    private static long allocatableCapacity(GetFilesystemInfoResponse info) {
        return info.hasAllocatableCapacity() ? info.getAllocatableCapacity() : info.getCapacity();
    }

    private static long allocatableRemaining(GetFilesystemInfoResponse info) {
        return info.hasAllocatableAvailable() ? info.getAllocatableAvailable() : info.getAvailable();
    }

    private static long allocatableUsed(GetFilesystemInfoResponse info) {
        long capacity = allocatableCapacity(info);
        long remaining = allocatableRemaining(info);
        return Math.max(0, capacity - remaining);
    }
}
