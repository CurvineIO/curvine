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

use super::super::BlockLocationLockManager;
use crate::master::Master;

#[test]
fn block_lock_metrics_count_each_acquisition_once() {
    const ENV: &str = "CURVINE_BLOCK_LOCK_METRIC_TEST";
    if std::env::var_os(ENV).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "master::meta::tests::block_lock_metrics_test::block_lock_metrics_count_each_acquisition_once",
            ])
            .env(ENV, "1")
            .status()
            .unwrap();
        assert!(status.success(), "isolated metric test failed: {status}");
        return;
    }

    Master::init_test_metrics();
    let metrics = Master::get_metrics().unwrap();
    let read = metrics.block_lock_acquire_us.with_label_values(&["read"]);
    let write = metrics.block_lock_acquire_us.with_label_values(&["write"]);
    let (read_before, write_before) = (read.get_sample_count(), write.get_sample_count());
    let locks = BlockLocationLockManager::new(16, 16);
    std::thread::scope(|scope| {
        for id in 0..8 {
            let locks = &locks;
            scope.spawn(move || drop(locks.write_worker(id)));
        }
    });
    drop(locks.read_blocks(&[1]));
    drop(locks.write_blocks(&[2, 3]));
    drop(locks.write_workers_blocks(&[3, 4], &[5]));
    assert_eq!(read.get_sample_count() - read_before, 1);
    assert_eq!(write.get_sample_count() - write_before, 10);
}
