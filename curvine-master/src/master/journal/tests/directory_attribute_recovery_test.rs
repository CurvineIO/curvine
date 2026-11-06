// Copyright 2026 OPPO.
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

use super::recovery::{classify_apply_error, ApplyDisposition};

#[test]
fn invalid_directory_attributes_do_not_retry_forever() {
    for message in [
        "invalid directory attributes for inode 42",
        "invalid directory attributes record for inode 42",
        // RocksDB returns this when the directory merge operator rejects an
        // underflow or malformed operand.
        "Corruption: Merge operator failed",
    ] {
        assert_eq!(
            classify_apply_error(message),
            ApplyDisposition::EnterSafeMode
        );
    }
}

#[test]
fn inode_record_id_mismatch_enters_safe_mode() {
    use crate::master::meta::store::RocksInodeStore;
    use crate::master::meta::FsDir;
    use crate::master::metadata_codec::MetadataCodec;
    use curvine_config::DBConf;
    use curvine_rocksdb::RocksUtils;
    use curvine_runtime::common::Utils;

    let path = Utils::test_sub_dir(format!("journal-inode-mismatch-{}", Utils::rand_str(8)));
    let store = RocksInodeStore::new(DBConf::new(&path), true).unwrap();
    let mut batch = store.new_batch();
    batch
        .put_cf_raw(
            RocksInodeStore::CF_INODES,
            RocksUtils::i64_to_bytes(123),
            MetadataCodec::encode(&FsDir::create_root()).unwrap(),
        )
        .unwrap();
    batch.commit().unwrap();
    let error = store.get_inode(123).err().unwrap();
    assert_eq!(
        classify_apply_error(&error.to_string()),
        ApplyDisposition::EnterSafeMode
    );
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
