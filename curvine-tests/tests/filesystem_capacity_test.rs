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

#![cfg(target_os = "linux")]

use curvine_client::file::FsContext;
use curvine_fuse::fs::operator::StatFs;
use curvine_fuse::fs::{CurvineFileSystem as FuseFileSystem, FileSystem};
use curvine_fuse::raw::fuse_abi::fuse_in_header;
use curvine_fuse::FUSE_UNKNOWN_INODES;
use curvine_model::{FilesystemCapacity, WorkerAddress, WorkerInfo, WorkerStatus};
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::time::Duration;

#[test]
fn filesystem_capacity_rpc_and_fuse_statfs_use_allocatable_bytes() {
    let testing = Testing::builder().workers(0).build().unwrap();
    let cluster = testing.start_cluster().unwrap();
    let master = cluster.get_active_master_fs();
    let mut live = WorkerInfo::new(
        WorkerAddress {
            worker_id: 1,
            ..Default::default()
        },
        1,
    );
    live.capacity = 5 * 4096 + 1;
    live.available = 3 * 4096 + 17;
    live.scheduled_bytes = 4096;
    master.add_test_worker(live);
    let mut blacklisted = WorkerInfo::new(
        WorkerAddress {
            worker_id: 2,
            ..Default::default()
        },
        1,
    );
    blacklisted.status = WorkerStatus::Blacklist;
    blacklisted.capacity = 1 << 30;
    blacklisted.available = 1 << 29;
    master.add_test_worker(blacklisted);

    let rt = cluster.clone_client_rt();
    let fs = cluster.new_fs();
    let fuse = FuseFileSystem::new(cluster.master_conf().clone(), rt.clone()).unwrap();
    rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let expected = FilesystemCapacity {
                capacity: 5 * 4096 + 1,
                available: 2 * 4096 + 17,
            };
            assert_eq!(fs.get_filesystem_capacity().await.unwrap(), expected);
            let info = fs.get_filesystem_info().await.unwrap();
            assert!(info.capacity > expected.capacity);
            assert_eq!(info.allocatable_capacity, expected.capacity);
            assert_eq!(info.allocatable_available, expected.available);

            let metrics = &FsContext::get_metrics().metadata_operation_duration;
            let full_info = metrics.with_label_values(&["GetFilesystemInfo"]);
            let capacity = metrics.with_label_values(&["GetFilesystemCapacity"]);
            let before_full_info = full_info.get_sample_count();
            let before_capacity = capacity.get_sample_count();
            let header = fuse_in_header::default();
            for _ in 0..2 {
                let stat = fuse.stat_fs(StatFs { header: &header }).await.unwrap();
                assert_eq!(stat.blocks, 5);
                assert_eq!(stat.bfree, 2);
                assert_eq!(stat.bavail, 2);
                assert_eq!(stat.bsize, 4096);
                assert_eq!(stat.frsize, 4096);
                assert_eq!(stat.files, FUSE_UNKNOWN_INODES);
                assert_eq!(stat.ffree, FUSE_UNKNOWN_INODES);
            }
            assert_eq!(full_info.get_sample_count(), before_full_info);
            assert_eq!(capacity.get_sample_count(), before_capacity + 2);
        })
        .await
        .expect("capacity RPC and statfs deadline");
    });
}
