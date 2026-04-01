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

use orpc::common::{ByteUnit, FileUtils, Utils};
use rocksdb::*;
use serde::Deserialize;
use serde_with::serde_derive::Serialize;
use std::ffi::c_int;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct DBConf {
    pub base_dir: String,
    pub data_dir: String,
    pub checkpoint_dir: String,
    pub family_list: Vec<String>,

    // Whether to disable wal, default to false, enable wal.
    pub disable_wal: bool,

    // Data compression type, value type: none, Lz4. Currently, only lz4 dependencies are compiled, and all compressed formats only support lz4
    // Default: none
    pub compression_type: String,

    // The amount of data to accumulate in the memory tables of all column families before writing to disk.
    // Default: 0
    pub db_write_buffer_size: ByteUnit,

    pub block_size: ByteUnit,

    pub block_cache_size: ByteUnit,

    pub disable_block_cache: bool,

    pub write_buffer_size: ByteUnit,

    pub max_write_buffer_number: usize,

    pub use_bloom_filter: bool,

    pub max_background_jobs: usize,

    pub max_subcompactions: usize,

    pub bloom_filter_bits_per_key: usize,
    pub block_based_bloom_filter: bool,

    pub level0_file_num_compaction_trigger: usize,
    pub level0_slowdown_writes_trigger: usize,
    pub level0_stop_writes_trigger: usize,
    pub max_bytes_for_level_base: ByteUnit,
    pub target_file_size_base: ByteUnit,
    pub max_total_wal_size: ByteUnit,
    pub bytes_per_sync: ByteUnit,
    pub wal_bytes_per_sync: ByteUnit,
    pub compaction_readahead_size: ByteUnit,
    pub use_direct_reads: bool,
    pub use_direct_io_for_flush_and_compaction: bool,
    pub cache_index_and_filter_blocks: bool,
    pub pin_l0_filter_and_index_blocks_in_cache: bool,

    pub enable_statistics: bool,
}

impl DBConf {
    pub const DATA_DIR: &'static str = "data";

    pub const CHECKPOINT_DIR: &'static str = "checkpoint";

    pub const COMPRESSION_NONE: &'static str = "none";
    pub const COMPRESSION_LZ4: &'static str = "lz4";
    pub const DEFAULT_FAMILY: &'static str = "default";

    pub fn new<T: AsRef<str>>(dir: T) -> Self {
        Self {
            base_dir: dir.as_ref().to_string(),
            data_dir: FileUtils::join_path(dir.as_ref(), Self::DATA_DIR),
            checkpoint_dir: FileUtils::join_path(dir.as_ref(), Self::CHECKPOINT_DIR),
            ..Default::default()
        }
    }

    pub fn set_dir(mut self, base_dir: impl AsRef<str>) -> Self {
        let base_dir = base_dir.as_ref();
        self.base_dir = base_dir.to_owned();
        self.data_dir = FileUtils::join_path(base_dir, Self::DATA_DIR);
        self.checkpoint_dir = FileUtils::join_path(base_dir, Self::CHECKPOINT_DIR);
        self
    }

    // Add column family.
    pub fn add_cf<T: AsRef<str>>(mut self, cf: T) -> Self {
        let cf = cf.as_ref().to_string();
        if !self.family_list.contains(&cf) {
            self.family_list.push(cf);
        }
        self
    }

    pub fn set_disable_wal(mut self, disable_wal: bool) -> Self {
        self.disable_wal = disable_wal;
        self
    }

    pub fn set_db_write_buffer_size(mut self, size: impl AsRef<str>) -> Self {
        self.db_write_buffer_size = Self::byte_unit_from_str("db_write_buffer_size", size);
        self
    }

    pub fn set_write_buffer_size(mut self, size: impl AsRef<str>) -> Self {
        self.write_buffer_size = Self::byte_unit_from_str("write_buffer_size", size);
        self
    }

    pub fn set_compress_type(mut self, t: impl AsRef<str>) -> Self {
        let t = t.as_ref();
        match t {
            Self::COMPRESSION_LZ4 | Self::COMPRESSION_NONE => {
                self.compression_type = t.to_string();
            }
            s => panic!("Unsupported compression type：{}", s),
        }
        self
    }

    pub fn set_block_size(mut self, size: impl AsRef<str>) -> Self {
        self.block_size = Self::byte_unit_from_str("block_size", size);
        self
    }

    pub fn set_block_cache_size(mut self, size: impl AsRef<str>) -> Self {
        self.block_cache_size = Self::byte_unit_from_str("block_cache_size", size);
        self
    }

    pub fn set_disable_block_cache(mut self, disable_block_cache: bool) -> Self {
        self.disable_block_cache = disable_block_cache;
        self
    }

    pub fn set_max_write_buffer_number(mut self, n: usize) -> Self {
        self.max_write_buffer_number = n;
        self
    }

    pub fn set_use_bloom_filter(mut self, use_bloom_filter: bool) -> Self {
        self.use_bloom_filter = use_bloom_filter;
        self
    }

    pub fn set_max_background_jobs(mut self, n: usize) -> Self {
        self.max_background_jobs = n;
        self
    }

    pub fn set_max_subcompactions(mut self, n: usize) -> Self {
        self.max_subcompactions = n;
        self
    }

    pub fn set_bloom_filter_bits_per_key(mut self, n: usize) -> Self {
        self.bloom_filter_bits_per_key = n;
        self
    }

    pub fn set_block_based_bloom_filter(mut self, block_based_bloom_filter: bool) -> Self {
        self.block_based_bloom_filter = block_based_bloom_filter;
        self
    }

    pub fn set_level0_file_num_compaction_trigger(mut self, n: usize) -> Self {
        self.level0_file_num_compaction_trigger = n;
        self
    }

    pub fn set_level0_slowdown_writes_trigger(mut self, n: usize) -> Self {
        self.level0_slowdown_writes_trigger = n;
        self
    }

    pub fn set_level0_stop_writes_trigger(mut self, n: usize) -> Self {
        self.level0_stop_writes_trigger = n;
        self
    }

    pub fn set_max_bytes_for_level_base(mut self, size: impl AsRef<str>) -> Self {
        self.max_bytes_for_level_base = Self::byte_unit_from_str("max_bytes_for_level_base", size);
        self
    }

    pub fn set_target_file_size_base(mut self, size: impl AsRef<str>) -> Self {
        self.target_file_size_base = Self::byte_unit_from_str("target_file_size_base", size);
        self
    }

    pub fn set_max_total_wal_size(mut self, size: impl AsRef<str>) -> Self {
        self.max_total_wal_size = Self::byte_unit_from_str("max_total_wal_size", size);
        self
    }

    pub fn set_bytes_per_sync(mut self, size: impl AsRef<str>) -> Self {
        self.bytes_per_sync = Self::byte_unit_from_str("bytes_per_sync", size);
        self
    }

    pub fn set_wal_bytes_per_sync(mut self, size: impl AsRef<str>) -> Self {
        self.wal_bytes_per_sync = Self::byte_unit_from_str("wal_bytes_per_sync", size);
        self
    }

    pub fn set_compaction_readahead_size(mut self, size: impl AsRef<str>) -> Self {
        self.compaction_readahead_size =
            Self::byte_unit_from_str("compaction_readahead_size", size);
        self
    }

    pub fn set_use_direct_reads(mut self, use_direct_reads: bool) -> Self {
        self.use_direct_reads = use_direct_reads;
        self
    }

    pub fn set_use_direct_io_for_flush_and_compaction(
        mut self,
        use_direct_io_for_flush_and_compaction: bool,
    ) -> Self {
        self.use_direct_io_for_flush_and_compaction = use_direct_io_for_flush_and_compaction;
        self
    }

    pub fn set_cache_index_and_filter_blocks(
        mut self,
        cache_index_and_filter_blocks: bool,
    ) -> Self {
        self.cache_index_and_filter_blocks = cache_index_and_filter_blocks;
        self
    }

    pub fn set_pin_l0_filter_and_index_blocks_in_cache(
        mut self,
        pin_l0_filter_and_index_blocks_in_cache: bool,
    ) -> Self {
        self.pin_l0_filter_and_index_blocks_in_cache = pin_l0_filter_and_index_blocks_in_cache;
        self
    }

    pub fn set_enable_statistics(mut self, enable_statistics: bool) -> Self {
        self.enable_statistics = enable_statistics;
        self
    }

    fn byte_unit_from_str(label: &'static str, size: impl AsRef<str>) -> ByteUnit {
        ByteUnit::from_str(size).unwrap_or_else(|e| panic!("Invalid {}: {}", label, e))
    }

    pub fn create_cache(&self) -> Cache {
        Cache::new_lru_cache(self.block_cache_size.as_byte() as usize)
    }

    pub fn create_block_opts(&self, cache: &Cache) -> BlockBasedOptions {
        let mut opts = BlockBasedOptions::default();
        opts.set_block_size(self.block_size.as_byte() as usize);
        if self.disable_block_cache {
            opts.disable_cache();
        } else {
            opts.set_block_cache(cache);
        }
        if self.use_bloom_filter {
            if self.block_based_bloom_filter {
                opts.set_bloom_filter(self.bloom_filter_bits_per_key as f64, true);
            } else {
                opts.set_ribbon_filter(self.bloom_filter_bits_per_key as f64);
            }
            opts.set_whole_key_filtering(true);
        }
        opts.set_cache_index_and_filter_blocks(self.cache_index_and_filter_blocks);
        opts.set_pin_l0_filter_and_index_blocks_in_cache(
            self.pin_l0_filter_and_index_blocks_in_cache,
        );
        opts.set_format_version(2);
        opts.set_checksum_type(ChecksumType::XXH3);
        opts.set_optimize_filters_for_memory(false);
        opts
    }

    pub fn create_cf_opts(&self, block_opts: &BlockBasedOptions) -> Options {
        let mut opts = self.create_db_opts();
        opts.set_block_based_table_factory(block_opts);
        opts.set_write_buffer_size(self.write_buffer_size.as_byte() as usize);
        opts.set_max_write_buffer_number(self.max_write_buffer_number as c_int);
        opts.set_level_zero_file_num_compaction_trigger(
            self.level0_file_num_compaction_trigger as c_int,
        );
        opts.set_level_zero_slowdown_writes_trigger(self.level0_slowdown_writes_trigger as c_int);
        opts.set_level_zero_stop_writes_trigger(self.level0_stop_writes_trigger as c_int);
        opts.set_max_bytes_for_level_base(self.max_bytes_for_level_base.as_byte());
        opts.set_target_file_size_base(self.target_file_size_base.as_byte());
        opts.set_compaction_readahead_size(self.compaction_readahead_size.as_byte() as usize);

        opts
    }

    // Build a rocksdb database configuration.
    pub fn create_db_opts(&self) -> Options {
        let mut opts = Options::default();
        opts.set_allow_concurrent_memtable_write(true);
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_max_open_files(-1);
        let compression_type = match self.compression_type.as_str() {
            Self::COMPRESSION_LZ4 => DBCompressionType::Lz4,
            _ => DBCompressionType::None,
        };
        opts.set_compression_type(compression_type);
        opts.set_max_background_jobs(self.max_background_jobs as c_int);
        opts.set_max_subcompactions(self.max_subcompactions as u32);
        opts.set_max_total_wal_size(self.max_total_wal_size.as_byte());
        opts.set_bytes_per_sync(self.bytes_per_sync.as_byte());
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync.as_byte());
        opts.set_use_direct_reads(self.use_direct_reads);
        opts.set_use_direct_io_for_flush_and_compaction(
            self.use_direct_io_for_flush_and_compaction,
        );

        // Exposes `rocksdb.options-statistics` (e.g. block cache hit/miss tickers) via DB::property_value.
        if self.enable_statistics {
            opts.enable_statistics();
        }

        if self.db_write_buffer_size.as_byte() > 0 {
            opts.set_db_write_buffer_size(self.db_write_buffer_size.as_byte() as usize);
        }
        opts.set_write_buffer_size(self.write_buffer_size.as_byte() as usize);

        opts
    }

    // Get the column family you want to create.
    pub fn get_cf_with_opts(&self) -> Vec<(String, Options)> {
        let cache = self.create_cache();
        let block_opts = self.create_block_opts(&cache);
        let opts = self.create_cf_opts(&block_opts);

        let mut cfs = Vec::new();
        for family_name in &self.family_list {
            cfs.push((family_name.to_string(), opts.clone()));
        }
        cfs
    }

    // Read configuration.
    pub fn create_read_opt(&self) -> ReadOptions {
        ReadOptions::default()
    }

    pub fn create_iterator_opt(&self) -> ReadOptions {
        let mut opt = ReadOptions::default();
        opt.set_readahead_size(64 * 1024 * 1024);
        opt
    }

    // Write configuration
    pub fn create_write_opt(&self) -> WriteOptions {
        let mut opt = WriteOptions::default();
        opt.disable_wal(self.disable_wal);
        opt
    }
}

impl Default for DBConf {
    fn default() -> Self {
        let dir = Utils::test_sub_dir(format!("testing/db-{}", Utils::rand_id()));
        let base_dir = dir.as_str();
        Self {
            base_dir: base_dir.to_string(),
            data_dir: FileUtils::join_path(base_dir, Self::DATA_DIR),
            checkpoint_dir: FileUtils::join_path(base_dir, Self::CHECKPOINT_DIR),
            family_list: vec![Self::DEFAULT_FAMILY.to_string()],
            disable_wal: false,
            compression_type: Self::COMPRESSION_NONE.to_string(),
            db_write_buffer_size: ByteUnit::new(0),
            block_size: ByteUnit::kb(4),
            block_cache_size: ByteUnit::mb(64),
            disable_block_cache: false,
            write_buffer_size: ByteUnit::mb(64),
            max_write_buffer_number: 2,
            use_bloom_filter: false,
            max_background_jobs: 2,
            max_subcompactions: 1,

            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_bytes_for_level_base: ByteUnit::mb(256),
            target_file_size_base: ByteUnit::mb(64),
            max_total_wal_size: ByteUnit::new(0),
            bytes_per_sync: ByteUnit::new(0),
            wal_bytes_per_sync: ByteUnit::new(0),
            compaction_readahead_size: ByteUnit::new(0),
            use_direct_reads: false,
            use_direct_io_for_flush_and_compaction: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks_in_cache: false,

            enable_statistics: false,
        }
    }
}
