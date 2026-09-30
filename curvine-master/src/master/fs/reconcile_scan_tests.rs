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

use super::*;
use curvine_rocksdb::RocksUtils;
use curvine_runtime::common::{SerdeUtils as Serde, Utils};

#[test]
fn reconcile_scan_error_preserves_locations_and_retry_cleans_them() -> CommonResult<()> {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    let suffix = Utils::rand_str(6);
    conf.master.meta_dir = Utils::test_sub_dir(format!("reconcile-scan/meta-{suffix}"));
    conf.journal.journal_dir = Utils::test_sub_dir(format!("reconcile-scan/journal-{suffix}"));
    let fs = JournalSystem::fs_only_for_test(&conf)?;
    let worker = 101;
    let ids = [1, 2];
    let broken_key = RocksUtils::u32_i64_to_bytes(worker, ids[1]);
    {
        let fs_dir = fs.fs_dir.write();
        let mut batch = fs_dir.store.new_batch();
        for id in ids {
            batch.add_location(id, &BlockLocation::with_id(worker))?;
        }
        batch.commit()?;
        fs_dir.store.store.db.put_cf(
            crate::master::meta::store::RocksInodeStore::CF_LOCATION,
            broken_key,
            [],
        )?;
    }
    fs.full_block_reconciles.lock().insert(
        worker,
        FullBlockReconcileState {
            running: true,
            generation: 1,
            pending: None,
        },
    );

    assert!(fs
        .reconcile_full_block_report(worker, 1, HashSet::new())
        .is_err());
    {
        let fs_dir = fs.fs_dir.write();
        for id in ids {
            let locations = fs_dir.get_block_locations(id)?;
            assert_eq!(locations.len(), 1);
            assert_eq!(locations[0].worker_id, worker);
        }
        fs_dir.store.store.db.put_cf(
            crate::master::meta::store::RocksInodeStore::CF_LOCATION,
            broken_key,
            Serde::serialize(&ids[1])?,
        )?;
        assert_eq!(fs_dir.get_worker_block_ids(worker)?, ids);
    }

    assert_eq!(
        fs.reconcile_full_block_report(worker, 1, HashSet::new())?,
        ids
    );
    let fs_dir = fs.fs_dir.read();
    assert!(fs_dir.get_worker_block_ids(worker)?.is_empty());
    for id in ids {
        assert!(fs_dir.get_block_locations(id)?.is_empty());
    }
    Ok(())
}
