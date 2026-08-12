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

pub use curvine_config::DBConf;

use rocksdb::*;
use std::ffi::c_int;

pub trait DBConfExt {
    fn create_cache(&self) -> Cache;

    fn create_block_opts(&self, cache: &Cache) -> BlockBasedOptions;

    fn create_cf_opts(&self, block_opts: &BlockBasedOptions) -> Options;

    fn create_db_opts(&self) -> Options;

    fn get_cf_with_opts(&self) -> Vec<(String, Options)>;

    fn create_read_opt(&self) -> ReadOptions;

    fn create_iterator_opt(&self) -> ReadOptions;

    fn create_bulk_scan_opt(&self) -> ReadOptions;

    fn create_write_opt(&self) -> WriteOptions;
}

impl DBConfExt for DBConf {
    fn create_cache(&self) -> Cache {
        Cache::new_lru_cache(self.block_cache_size.as_byte() as usize)
    }

    fn create_block_opts(&self, cache: &Cache) -> BlockBasedOptions {
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

    fn create_cf_opts(&self, block_opts: &BlockBasedOptions) -> Options {
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
    fn create_db_opts(&self) -> Options {
        let mut opts = Options::default();
        opts.set_allow_concurrent_memtable_write(true);
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_max_open_files(-1);
        let compression_type = match self.compression_type.as_str() {
            DBConf::COMPRESSION_LZ4 => DBCompressionType::Lz4,
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
    fn get_cf_with_opts(&self) -> Vec<(String, Options)> {
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
    fn create_read_opt(&self) -> ReadOptions {
        ReadOptions::default()
    }

    fn create_iterator_opt(&self) -> ReadOptions {
        let mut opt = ReadOptions::default();
        opt.set_readahead_size(64 * 1024 * 1024);
        opt
    }

    /// ReadOptions tuned for one-shot bulk scans during snapshot restore.
    ///
    /// - `total_order_seek(true)`: required for correctness with hash memtables;
    ///   without it, a full-CF scan can silently miss keys (see db_engine.rs
    ///   `scan()` comment).
    /// - `fill_cache(false)`: the scan data is one-shot and won't be reused;
    ///   avoid polluting the block cache during restore.
    /// - `readahead_size(64 MiB)`: maximise sequential I/O throughput.
    fn create_bulk_scan_opt(&self) -> ReadOptions {
        let mut opt = self.create_iterator_opt();
        opt.set_total_order_seek(true);
        opt.fill_cache(false);
        opt
    }

    // Write configuration
    fn create_write_opt(&self) -> WriteOptions {
        let mut opt = WriteOptions::default();
        opt.disable_wal(self.disable_wal);
        opt
    }
}
