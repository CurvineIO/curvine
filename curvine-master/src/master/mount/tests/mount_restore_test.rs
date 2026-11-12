// Copyright 2026 OPPO
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

//! Mount restore must follow the namespace error contract: a closed view
//! (cold window) is retryable, a FAILED handle is terminal, and neither may
//! panic the master during startup or recovery.

#[test]
fn restore_reports_a_closed_namespace_instead_of_panicking() {
    use super::MountManager;
    use crate::master::journal::JournalSystem;
    use crate::master::meta::fs_dir_handle::DEFAULT_COLD_TIMEOUT;
    use crate::master::Master;
    use curvine_config::ClusterConf;
    use curvine_runtime::common::Utils;

    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.journal.enable = false;
    conf.change_test_meta_dir(format!("mount-restore-cold-{}", Utils::rand_str(8)));
    let journal = JournalSystem::fs_only_for_test(&conf).unwrap();
    let manager = MountManager::new(journal);

    // A cold window closes the shared view: recovery must surface a
    // retryable error and the best-effort variant must log and return.
    let cold = manager
        .master_fs
        .fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap();
    let err = manager.restore().unwrap_err();
    assert!(
        matches!(err, curvine_error::FsError::InProgress(_)),
        "a closed namespace must surface as retryable, got {err:?}"
    );
    manager.restore_best_effort();

    drop(cold);
    manager.restore().unwrap();
}
