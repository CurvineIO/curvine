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

//! Regression tests for #1966: a partial `[master.rocksdb]` /
//! `[journal.rocksdb]` table must overlay the component-specific RocksDB
//! defaults, not the generic `DBConf::default()`.

use curvine_config::validation::ConfValidate;
use curvine_config::{ClusterConf, DBConf};
use curvine_runtime::common::ByteUnit;

const PARTIAL_TABLE: &str = r#"
[cluster]
name = "cv1966"
nodes = ["localhost"]

[master]
meta_dir = "testing/meta"

# Only one key written: everything else must keep MasterConf::rocksdb_default().
[master.rocksdb]
block_cache_size = "1GB"

[journal]
journal_dir = "testing/journal"

# Only one key written: everything else must keep JournalConf::rocksdb_default().
[journal.rocksdb]
block_size = "32KB"
"#;

fn conf_from(text: &str) -> ClusterConf {
    let doc = curvine_config::pipeline::build_document(text, &[]).unwrap();
    let mut conf: ClusterConf = doc.try_into().unwrap();
    conf.validate().unwrap();
    conf
}

#[test]
fn partial_master_rocksdb_table_overlays_component_defaults() {
    let conf = conf_from(PARTIAL_TABLE);

    // Written key wins.
    assert_eq!(conf.master.rocksdb.block_cache_size, ByteUnit::gb(1));

    // Unset keys keep the component baseline (issue #1966 regression):
    assert!(
        conf.master.rocksdb.disable_wal,
        "disable_wal must stay true"
    );
    assert_eq!(
        conf.master.rocksdb.compression_type,
        DBConf::COMPRESSION_LZ4,
        "compression_type must stay lz4"
    );
    assert_eq!(conf.master.rocksdb.max_background_jobs, 6);
    assert_eq!(conf.master.rocksdb.max_subcompactions, 2);
    assert_eq!(
        conf.master.rocksdb.max_bytes_for_level_base,
        ByteUnit::gb(2)
    );
    assert!(conf.master.rocksdb.enable_statistics);
    assert!(conf.master.rocksdb.use_bloom_filter);
    assert!(conf.master.rocksdb.cache_index_and_filter_blocks);
    assert!(conf.master.rocksdb.pin_l0_filter_and_index_blocks_in_cache);
}

#[test]
fn partial_journal_rocksdb_table_overlays_component_defaults() {
    let conf = conf_from(PARTIAL_TABLE);

    // Written key wins.
    assert_eq!(conf.journal.rocksdb.block_size, ByteUnit::kb(32));

    // Unset keys keep the journal baseline.
    assert!(conf.journal.rocksdb.use_bloom_filter);
    assert!(!conf.journal.rocksdb.disable_wal);
}

#[test]
fn empty_rocksdb_tables_equal_component_defaults() {
    let text = r#"
[cluster]
name = "cv1966"
nodes = ["localhost"]

[master]
meta_dir = "testing/meta"

[master.rocksdb]

[journal]
journal_dir = "testing/journal"

[journal.rocksdb]
"#;
    let conf = conf_from(text);
    let master_default = &conf.master.rocksdb;
    assert!(master_default.disable_wal);
    assert_eq!(master_default.compression_type, DBConf::COMPRESSION_LZ4);
    assert_eq!(master_default.block_cache_size, ByteUnit::gb(4));
    let journal_default = &conf.journal.rocksdb;
    assert_eq!(journal_default.block_size, ByteUnit::kb(16));
    assert!(journal_default.use_bloom_filter);
}
