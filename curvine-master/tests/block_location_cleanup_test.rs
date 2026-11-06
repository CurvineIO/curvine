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

use curvine_master::master::meta::store::RocksInodeStore;
use curvine_model::BlockLocation;
use curvine_rocksdb::DBConf;
use curvine_runtime::common::Utils;

#[test]
fn removing_worker_locations_preserves_other_worker_across_batches() {
    let conf = DBConf::new(Utils::test_sub_dir(format!(
        "worker-location-cleanup-{}",
        Utils::rand_str(8)
    )));
    let store = RocksInodeStore::new(conf, true).unwrap();
    let mut batch = store.new_batch();
    for id in 0..4100 {
        batch.add_location(id, &BlockLocation::with_id(1)).unwrap();
        batch.add_location(id, &BlockLocation::with_id(2)).unwrap();
    }
    batch.commit().unwrap();
    let removed = store.delete_locations(1).unwrap();
    assert_eq!(removed.len(), 4100);
    assert!(store.get_block_ids(1).unwrap().is_empty());
    assert_eq!(store.get_block_ids(2).unwrap().len(), 4100);
    for id in 0..4100 {
        let locations = store.get_locations(id).unwrap();
        assert_eq!(locations.len(), 1);
        assert_eq!(locations[0].worker_id, 2);
    }
    assert!(store.delete_locations(1).unwrap().is_empty());
}
