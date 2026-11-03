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

//! Component-scoped RocksDB config deserialization helpers (#1966).
//!
//! `[master.rocksdb]` and `[journal.rocksdb]` carry per-component tuning on
//! top of a component-specific baseline (`MasterConf::rocksdb_default` /
//! `JournalConf::rocksdb_default`), which differs from the generic
//! `DBConf::default()`. Field-level `#[serde(default = ...)]` only kicks in
//! when the whole table is missing, so a partial table would otherwise lose
//! the component baseline (e.g. master `disable_wal = true`,
//! `compression_type = "lz4"`, 4GB cache). This module deserializes the raw
//! table over the component defaults instead.

use crate::{DBConf, JournalConf, MasterConf};
use curvine_runtime::common::ByteUnit;
use serde::{Deserialize, Deserializer};

/// Deserializes a partial `[master.rocksdb]` table over
/// `MasterConf::rocksdb_default()`. Keys absent from the table keep the
/// component default; an empty or missing table yields the component default.
pub fn deserialize_master_rocksdb<'de, D>(deserializer: D) -> Result<DBConf, D::Error>
where
    D: Deserializer<'de>,
{
    let overlay: DBConfOverlay = Deserialize::deserialize(deserializer)?;
    Ok(overlay.apply_over(MasterConf::rocksdb_default()))
}

/// Deserializes a partial `[journal.rocksdb]` table over
/// `JournalConf::rocksdb_default()`. Semantics match
/// [`deserialize_master_rocksdb`].
pub fn deserialize_journal_rocksdb<'de, D>(deserializer: D) -> Result<DBConf, D::Error>
where
    D: Deserializer<'de>,
{
    let overlay: DBConfOverlay = Deserialize::deserialize(deserializer)?;
    Ok(overlay.apply_over(JournalConf::rocksdb_default()))
}

/// Deserialization shape of a partial `[<component>.rocksdb]` table: every
/// key is optional, so only the keys an operator actually wrote are captured.
#[derive(Default, Deserialize)]
#[serde(default)]
struct DBConfOverlay {
    base_dir: Option<String>,
    data_dir: Option<String>,
    checkpoint_dir: Option<String>,
    family_list: Option<Vec<String>>,
    disable_wal: Option<bool>,
    compression_type: Option<String>,
    db_write_buffer_size: Option<ByteUnit>,
    block_size: Option<ByteUnit>,
    block_cache_size: Option<ByteUnit>,
    disable_block_cache: Option<bool>,
    write_buffer_size: Option<ByteUnit>,
    max_write_buffer_number: Option<usize>,
    use_bloom_filter: Option<bool>,
    max_background_jobs: Option<usize>,
    max_subcompactions: Option<usize>,
    bloom_filter_bits_per_key: Option<usize>,
    block_based_bloom_filter: Option<bool>,
    level0_file_num_compaction_trigger: Option<usize>,
    level0_slowdown_writes_trigger: Option<usize>,
    level0_stop_writes_trigger: Option<usize>,
    max_bytes_for_level_base: Option<ByteUnit>,
    target_file_size_base: Option<ByteUnit>,
    max_total_wal_size: Option<ByteUnit>,
    bytes_per_sync: Option<ByteUnit>,
    wal_bytes_per_sync: Option<ByteUnit>,
    compaction_readahead_size: Option<ByteUnit>,
    use_direct_reads: Option<bool>,
    use_direct_io_for_flush_and_compaction: Option<bool>,
    cache_index_and_filter_blocks: Option<bool>,
    pin_l0_filter_and_index_blocks_in_cache: Option<bool>,
    enable_statistics: Option<bool>,
}

impl DBConfOverlay {
    fn apply_over(self, base: DBConf) -> DBConf {
        DBConf {
            base_dir: self.base_dir.unwrap_or(base.base_dir),
            data_dir: self.data_dir.unwrap_or(base.data_dir),
            checkpoint_dir: self.checkpoint_dir.unwrap_or(base.checkpoint_dir),
            family_list: self.family_list.unwrap_or(base.family_list),
            disable_wal: self.disable_wal.unwrap_or(base.disable_wal),
            compression_type: self.compression_type.unwrap_or(base.compression_type),
            db_write_buffer_size: self
                .db_write_buffer_size
                .unwrap_or(base.db_write_buffer_size),
            block_size: self.block_size.unwrap_or(base.block_size),
            block_cache_size: self.block_cache_size.unwrap_or(base.block_cache_size),
            disable_block_cache: self.disable_block_cache.unwrap_or(base.disable_block_cache),
            write_buffer_size: self.write_buffer_size.unwrap_or(base.write_buffer_size),
            max_write_buffer_number: self
                .max_write_buffer_number
                .unwrap_or(base.max_write_buffer_number),
            use_bloom_filter: self.use_bloom_filter.unwrap_or(base.use_bloom_filter),
            max_background_jobs: self.max_background_jobs.unwrap_or(base.max_background_jobs),
            max_subcompactions: self.max_subcompactions.unwrap_or(base.max_subcompactions),
            bloom_filter_bits_per_key: self
                .bloom_filter_bits_per_key
                .unwrap_or(base.bloom_filter_bits_per_key),
            block_based_bloom_filter: self
                .block_based_bloom_filter
                .unwrap_or(base.block_based_bloom_filter),
            level0_file_num_compaction_trigger: self
                .level0_file_num_compaction_trigger
                .unwrap_or(base.level0_file_num_compaction_trigger),
            level0_slowdown_writes_trigger: self
                .level0_slowdown_writes_trigger
                .unwrap_or(base.level0_slowdown_writes_trigger),
            level0_stop_writes_trigger: self
                .level0_stop_writes_trigger
                .unwrap_or(base.level0_stop_writes_trigger),
            max_bytes_for_level_base: self
                .max_bytes_for_level_base
                .unwrap_or(base.max_bytes_for_level_base),
            target_file_size_base: self
                .target_file_size_base
                .unwrap_or(base.target_file_size_base),
            max_total_wal_size: self.max_total_wal_size.unwrap_or(base.max_total_wal_size),
            bytes_per_sync: self.bytes_per_sync.unwrap_or(base.bytes_per_sync),
            wal_bytes_per_sync: self.wal_bytes_per_sync.unwrap_or(base.wal_bytes_per_sync),
            compaction_readahead_size: self
                .compaction_readahead_size
                .unwrap_or(base.compaction_readahead_size),
            use_direct_reads: self.use_direct_reads.unwrap_or(base.use_direct_reads),
            use_direct_io_for_flush_and_compaction: self
                .use_direct_io_for_flush_and_compaction
                .unwrap_or(base.use_direct_io_for_flush_and_compaction),
            cache_index_and_filter_blocks: self
                .cache_index_and_filter_blocks
                .unwrap_or(base.cache_index_and_filter_blocks),
            pin_l0_filter_and_index_blocks_in_cache: self
                .pin_l0_filter_and_index_blocks_in_cache
                .unwrap_or(base.pin_l0_filter_and_index_blocks_in_cache),
            enable_statistics: self.enable_statistics.unwrap_or(base.enable_statistics),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{deserialize_journal_rocksdb, deserialize_master_rocksdb};
    use crate::{DBConf, JournalConf, MasterConf};
    use curvine_runtime::common::ByteUnit;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct MasterWrapper {
        #[serde(deserialize_with = "deserialize_master_rocksdb")]
        rocksdb: DBConf,
    }

    #[derive(Deserialize)]
    struct JournalWrapper {
        #[serde(deserialize_with = "deserialize_journal_rocksdb")]
        rocksdb: DBConf,
    }

    fn parse_master(s: &str) -> DBConf {
        toml::from_str::<MasterWrapper>(s).unwrap().rocksdb
    }

    fn parse_journal(s: &str) -> DBConf {
        toml::from_str::<JournalWrapper>(s).unwrap().rocksdb
    }

    #[test]
    fn empty_master_table_equals_component_default() {
        let conf = parse_master("[rocksdb]");
        let expected = MasterConf::rocksdb_default();
        assert_eq!(conf.disable_wal, expected.disable_wal);
        assert_eq!(conf.compression_type, expected.compression_type);
        assert_eq!(conf.block_cache_size, expected.block_cache_size);
        assert_eq!(conf.max_background_jobs, expected.max_background_jobs);
        assert_eq!(conf.block_size, expected.block_size);
    }

    #[test]
    fn partial_master_table_keeps_component_defaults() {
        let conf = parse_master(
            r#"
            [rocksdb]
            block_cache_size = "1GB"
        "#,
        );
        assert_eq!(conf.block_cache_size, ByteUnit::gb(1));
        assert!(conf.disable_wal, "component default must survive");
        assert_eq!(conf.compression_type, DBConf::COMPRESSION_LZ4);
        assert_eq!(conf.max_background_jobs, 6);
        assert_eq!(conf.max_bytes_for_level_base, ByteUnit::gb(2));
        assert!(conf.enable_statistics);
        assert!(conf.use_bloom_filter);
    }

    #[test]
    fn written_master_keys_win_over_defaults() {
        let conf = parse_master(
            r#"
            [rocksdb]
            disable_wal = false
            compression_type = "none"
        "#,
        );
        assert!(!conf.disable_wal);
        assert_eq!(conf.compression_type, DBConf::COMPRESSION_NONE);
        assert_eq!(conf.block_cache_size, ByteUnit::gb(4));
    }

    #[test]
    fn partial_journal_table_keeps_component_defaults() {
        let conf = parse_journal(
            r#"
            [rocksdb]
            block_size = "32KB"
        "#,
        );
        assert_eq!(conf.block_size, ByteUnit::kb(32));
        assert_eq!(
            conf.block_size,
            ByteUnit::kb(32),
            "written key must be honored"
        );
        // Remaining keys fall back to JournalConf::rocksdb_default(), not
        // DBConf::default().
        assert!(conf.use_bloom_filter);
        assert_eq!(conf.block_cache_size, ByteUnit::mb(64));
    }

    #[test]
    fn journal_default_without_table_uses_journal_baseline() {
        // With no table at all, serde's field `default` would kick in; here we
        // verify the helper matches the component default when applied over an
        // empty table, which is the same end state.
        let conf = parse_journal("[rocksdb]");
        let expected = JournalConf::rocksdb_default();
        assert_eq!(conf.disable_wal, expected.disable_wal);
        assert_eq!(conf.block_size, expected.block_size);
        assert_eq!(conf.use_bloom_filter, expected.use_bloom_filter);
    }
}
