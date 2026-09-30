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
use curvine_runtime::common::Utils;

fn store(name: &str) -> CommonResult<RocksInodeStore> {
    RocksInodeStore::new(
        DBConf::new(Utils::test_sub_dir(format!(
            "worker-block-scan/{name}-{}",
            Utils::rand_str(6)
        ))),
        true,
    )
}

#[test]
fn raw_worker_scan_preserves_encoded_order_and_worker_bounds() -> CommonResult<()> {
    let store = store("bounds")?;
    let input = [i64::MIN, -1, 0, 1, i64::MAX];
    let expected = vec![0, 1, i64::MAX, i64::MIN, -1];
    let workers = [0, 7, 8, u32::MAX];
    let mut batch = store.new_batch();
    for worker in workers {
        for id in input {
            batch.add_location(id, &BlockLocation::with_id(worker))?;
        }
    }
    batch.commit()?;

    assert!(store.get_block_ids(6)?.is_empty());
    assert!(store.get_block_ids(u32::MAX - 1)?.is_empty());
    for worker in workers {
        for capacity in [0, 1, usize::MAX] {
            assert_eq!(
                store.get_block_ids_with_capacity(worker, capacity)?,
                expected
            );
        }
        if worker != u32::MAX {
            let boxed: CommonResult<Vec<_>> = store
                .db
                .prefix_scan(
                    RocksInodeStore::CF_LOCATION,
                    RocksUtils::u32_to_bytes(worker),
                )?
                .map(|row| {
                    let (_, value) = row?;
                    Serde::deserialize::<i64>(&value)
                })
                .collect();
            assert_eq!(store.get_block_ids(worker)?, boxed?);
        }
    }
    Ok(())
}

#[test]
fn raw_worker_scan_keeps_one_snapshot_across_location_changes() -> CommonResult<()> {
    let store = store("snapshot")?;
    let worker = 7;
    let mut initial = store.new_batch();
    initial.add_location(1, &BlockLocation::with_id(worker))?;
    initial.add_location(3, &BlockLocation::with_id(worker))?;
    initial.commit()?;

    let prefix = RocksUtils::u32_to_bytes(worker);
    let mut scan = store
        .db
        .raw_prefix_scan(RocksInodeStore::CF_LOCATION, prefix)?;
    assert_eq!(Serde::deserialize::<i64>(scan.value().unwrap())?, 1);
    let mut changed = store.new_batch();
    changed.delete_location(1, worker)?;
    changed.add_location(2, &BlockLocation::with_id(worker))?;
    changed.commit()?;

    let mut observed = Vec::new();
    while let Some((key, value)) = scan.item() {
        assert!(key.starts_with(&prefix));
        observed.push(Serde::deserialize::<i64>(value)?);
        scan.next();
    }
    scan.status()?;
    assert_eq!(observed, vec![1, 3]);
    drop(scan);
    assert_eq!(store.get_block_ids(worker)?, vec![2, 3]);
    Ok(())
}
