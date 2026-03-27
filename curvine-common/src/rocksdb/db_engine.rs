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

use crate::rocksdb::{DBConf, RocksUtils};
use log::{info, warn};
use orpc::common::{FileUtils, Utils};
use orpc::{err_box, try_err, CommonResult};
use rocksdb::checkpoint::Checkpoint;
use rocksdb::properties;
use rocksdb::*;
use std::collections::HashMap;

pub type KVBytes = (Box<[u8]>, Box<[u8]>);

pub struct DBEngine {
    db: DB,
    write_opt: WriteOptions,
    conf: DBConf,
}

impl DBEngine {
    pub fn new(conf: DBConf, format: bool) -> CommonResult<Self> {
        // Do you need to retry formatting? If format is true, the directory will be deleted.
        Self::format(format, &conf)?;

        let db_path = &conf.data_dir;
        let db_opt = conf.create_db_opt();
        let write_opt = conf.create_write_opt();
        let cfs = conf.create_cf_opt();

        let db = try_err!(DB::open_cf_with_opts(&db_opt, db_path, cfs));
        info!(
            "Create rocksdb success, format: {}, conf: {:?}",
            format, conf
        );
        Ok(Self {
            db,
            write_opt,
            conf,
        })
    }

    pub fn from_dir<T: AsRef<str>>(dir: T, format: bool) -> CommonResult<Self> {
        let conf = DBConf::new(dir);
        Self::new(conf, format)
    }

    pub fn restore<T: AsRef<str>>(&mut self, checkpoint: T) -> CommonResult<()> {
        let db_path = self.conf.data_dir.clone();
        let db_opt = self.conf.create_db_opt();
        let cfs = self.conf.create_cf_opt();
        let checkpoint = checkpoint.as_ref();

        // The database points to a temporary directory while we prepare the restore.
        let tmp_path = Utils::temp_file();
        self.db = try_err!(DB::open(&db_opt, &tmp_path));
        // Remove the original database directory before linking the checkpoint.
        FileUtils::delete_path(&db_path, true)?;

        try_err!(RocksUtils::link_dir(checkpoint, &db_path));
        self.db = try_err!(DB::open_cf_with_opts(&db_opt, &db_path, cfs));

        // Now that self.db points to the restored DB and the temporary DB handle is dropped,
        // it is safe to delete the temporary directory. Propagate any deletion error.
        FileUtils::delete_path(&tmp_path, true)?;

        Ok(())
    }

    // Get a reference to the column family.
    pub fn cf(&self, name: &str) -> CommonResult<&ColumnFamily> {
        match self.db.cf_handle(name) {
            Some(v) => Ok(v),
            None => err_box!("cf {} not exists", name),
        }
    }

    pub fn put_cf<K, V>(&self, cf: &str, key: K, value: V) -> CommonResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let cf = self.cf(cf)?;
        try_err!(self.db.put_cf_opt(cf, key, value, &self.write_opt));
        Ok(())
    }

    pub fn put<K, V>(&self, key: K, value: V) -> CommonResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        try_err!(self.db.put_opt(key, value, &self.write_opt));
        Ok(())
    }

    pub fn get_cf<K>(&self, cf: &str, key: K) -> CommonResult<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let cf = self.cf(cf)?;
        let cf_bytes = try_err!(self.db.get_cf(cf, key));
        Ok(cf_bytes)
    }

    pub fn get<K>(&self, key: K) -> CommonResult<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let bytes = try_err!(self.db.get(key));
        Ok(bytes)
    }

    // Delete data.
    pub fn delete_cf<K>(&self, cf: &str, key: K) -> CommonResult<()>
    where
        K: AsRef<[u8]>,
    {
        let cf = self.cf(cf)?;
        try_err!(self.db.delete_cf_opt(cf, key, &self.write_opt));
        Ok(())
    }

    pub fn delete<K>(&self, key: K) -> CommonResult<()>
    where
        K: AsRef<[u8]>,
    {
        try_err!(self.db.delete_opt(key, &self.write_opt));
        Ok(())
    }

    // Scan a column family
    // Note: If the mem table is of hash type, then the method cannot obtain data. It needs to set set_total_order_seek to true
    pub fn scan(&self, cf: &str) -> CommonResult<RocksIterator<'_>> {
        let opt = self.conf.create_read_opt();
        let cf = self.cf(cf)?;

        let mode = IteratorMode::Start;
        let iter = self.db.iterator_cf_opt(cf, opt, mode);
        Ok(RocksIterator { inner: iter })
    }

    // Describe the data in the specified key range of column families.
    // If the mem table is of hash type, then the method may not get the correct data. It needs to set set_total_order_seek to true
    pub fn range_scan<K>(&self, cf: &str, start: K, end: K) -> CommonResult<RocksIterator<'_>>
    where
        K: AsRef<[u8]>,
    {
        let mut opt = self.conf.create_read_opt();
        opt.set_iterate_lower_bound(start.as_ref());
        opt.set_iterate_upper_bound(end.as_ref());

        let cf = self.cf(cf)?;
        let mode = IteratorMode::From(start.as_ref(), Direction::Forward);
        let iter = self.db.iterator_cf_opt(cf, opt, mode);
        Ok(RocksIterator { inner: iter })
    }

    // Prefix matcher.
    // lower_bound: key contains.
    // upper_bound: key + 1, not included.
    pub fn prefix_scan<K>(&self, cf: &str, key: K) -> CommonResult<RocksIterator<'_>>
    where
        K: AsRef<[u8]>,
    {
        let mut opt = self.conf.create_read_opt();
        opt.set_prefix_same_as_start(true);

        let start = key.as_ref();
        let end = RocksUtils::calculate_end_bytes(start);
        opt.set_iterate_lower_bound(start);
        opt.set_iterate_upper_bound(end);

        let cf = self.cf(cf)?;
        let mode = IteratorMode::From(start, Direction::Forward);
        let iter = self.db.iterator_cf_opt(cf, opt, mode);
        Ok(RocksIterator { inner: iter })
    }

    pub fn iter_cf_opt<'a: 'b, 'b>(
        &'a self,
        cf: &str,
    ) -> CommonResult<DBIteratorWithThreadMode<'b, DB>> {
        let cf = self.cf(cf)?;
        let opt = self.conf.create_iterator_opt();
        let iter = self.db.iterator_cf_opt(cf, opt, IteratorMode::Start);
        Ok(iter)
    }

    pub fn get_db(&self) -> &DB {
        &self.db
    }

    pub fn get_db_path(&self) -> &str {
        &self.conf.data_dir
    }

    // Create a checkpoint.
    pub fn create_checkpoint(&self, id: u64) -> CommonResult<String> {
        self.flush(true)?;

        let checkpoint_path = self.get_checkpoint_path(id);
        let existed = FileUtils::exists(&checkpoint_path);

        if existed {
            warn!(
                "checkpoint directory already exists, will be deleted and recreated: {}, id: {}",
                checkpoint_path, id
            );
            FileUtils::delete_path(&checkpoint_path, true)?;
        }

        FileUtils::create_parent_dir(&checkpoint_path, true)?;
        let checkpoint = try_err!(Checkpoint::new(&self.db));
        checkpoint.create_checkpoint(&checkpoint_path)?;

        info!(
            "created checkpoint successfully, id: {}, path: {}, existed_before: {}",
            id, checkpoint_path, existed
        );

        Ok(checkpoint_path)
    }

    // Whether to recreate a database. Delete the previous directory and create a new directory.
    fn format(format: bool, conf: &DBConf) -> CommonResult<()> {
        let base_dir = &conf.base_dir;
        if format && FileUtils::exists(base_dir) {
            FileUtils::delete_path(base_dir, true)?;
            info!("Delete(format) exists db path {:?}", base_dir)
        }

        if FileUtils::exists(base_dir) {
            FileUtils::create_dir(base_dir, true)?;
        }

        Ok(())
    }

    pub fn db(&self) -> &DB {
        &self.db
    }

    pub fn flush_mem(&self, sync: bool) -> CommonResult<()> {
        let mut opts = FlushOptions::default();
        opts.set_wait(sync);
        self.db.flush_opt(&opts)?;
        Ok(())
    }

    pub fn flush_wal(&self, sync: bool) -> CommonResult<()> {
        self.db.flush_wal(sync)?;
        Ok(())
    }

    pub fn flush(&self, sync: bool) -> CommonResult<()> {
        if !self.conf.disable_wal {
            self.flush_wal(sync)?;
        }
        self.flush_mem(sync)?;
        Ok(())
    }

    pub fn write_batch(&self, batch: WriteBatchWithTransaction<false>) -> CommonResult<()> {
        try_err!(self.db.write_opt(batch, &self.write_opt));
        Ok(())
    }

    // Delete all data with the specified prefix.
    pub fn prefix_delete<K>(&self, cf: &str, prefix: K) -> CommonResult<()>
    where
        K: AsRef<[u8]>,
    {
        let cf = self.cf(cf)?;
        let start = prefix.as_ref();
        let end = RocksUtils::calculate_end_bytes(start);

        self.db.delete_range_cf(cf, start, &end)?;
        Ok(())
    }

    pub fn multi_get_cf<'a, K, I>(&self, cf: &str, keys: I) -> CommonResult<Vec<Vec<u8>>>
    where
        K: AsRef<[u8]> + 'a,
        I: IntoIterator<Item = &'a K>,
    {
        let cf = self.cf(cf)?;
        let mut res = Vec::with_capacity(16);

        let keys_iter = keys.into_iter().map(|x| (cf, x));
        let multi = self.db.multi_get_cf(keys_iter);

        for item in multi {
            let value_bytes = try_err!(item);
            if let Some(v) = value_bytes {
                res.push(v)
            }
        }

        Ok(res)
    }

    pub fn get_checkpoint_path(&self, id: u64) -> String {
        format!("{}/ck-{}", self.conf.checkpoint_dir, id)
    }

    /// RocksDB statistics (memory, compaction/flush pressure, snapshots, versions), as `HashMap<metric key, value>`.
    ///
    /// **Key naming** (`.` in property names becomes `_`)
    /// - **DB scope**: `db_{name.replace('.', '_')}`, e.g. `db_rocksdb_block-cache-usage`.
    /// - **CF scope**: `cf_{cf_name}_{name.replace('.', '_')}`, e.g. `cf_inodes_rocksdb_cur-size-all-mem-tables`.
    ///
    /// **`db_*` vs real “whole DB” semantics**
    ///
    /// Values under `db_*` are read with `DB::property_int_value` (RocksDB C API **without** a column-family
    /// handle). For several int properties—including `rocksdb.size-all-mem-tables` and
    /// `rocksdb.estimate-table-readers-mem`—that path reports metrics for the **default column family only**,
    /// **not** the sum over all column families. Example: `db_rocksdb_size-all-mem-tables` can be tiny while
    /// `cf_inodes_rocksdb_size-all-mem-tables` is large. For **memtable** and **table-reader** totals on a
    /// multi-CF database, **sum the corresponding `cf_*` keys** across your column families (and do **not**
    /// add those `db_*` keys on top).
    ///
    /// **Shared block cache** properties (`block-cache-*`) are typically **DB-wide**; `db_*` is appropriate there.
    ///
    /// **Units** (properties not listed below are usually **bytes**)
    /// - **Counts**: `rocksdb.num-immutable-mem-table`, `rocksdb.num-snapshots`, `rocksdb.num-live-versions`,
    ///   `rocksdb.num-running-flushes`, `rocksdb.num-running-compactions`.
    /// - **0/1 flags**: `rocksdb.compaction-pending`, `rocksdb.mem-table-flush-pending`.
    /// - **Time**: `rocksdb.oldest-snapshot-time` is **Unix seconds** (may be 0 when no snapshot, depending on RocksDB version).
    ///
    /// **DB-level metrics (via `property_int_value`; mostly shared cache + process-wide counters)**
    ///
    /// | Property suffix | Meaning |
    /// |-----------------|---------|
    /// | `rocksdb.block-cache-capacity` | Block cache **configured capacity** (bytes), not current usage. |
    /// | `rocksdb.block-cache-usage` | Block cache **current usage** (bytes), hot read-path cache. |
    /// | `rocksdb.block-cache-pinned-usage` | Bytes of **pinned** entries in the block cache. |
    /// | `rocksdb.size-all-mem-tables` | **Default CF only** when exposed as `db_*` (see note above); memtable bytes for that CF. |
    /// | `rocksdb.estimate-table-readers-mem` | **Default CF only** when exposed as `db_*`; use `cf_*` sum for all CFs. |
    ///
    /// **DB-level metrics (compaction / flush in flight)**
    ///
    /// | Property suffix | Meaning |
    /// |-----------------|---------|
    /// | `rocksdb.num-running-flushes` | Number of **flushes** currently running. |
    /// | `rocksdb.num-running-compactions` | Number of **compactions** currently running. |
    ///
    /// **DB-level metrics (snapshots, whole DB)**
    ///
    /// | Property suffix | Meaning |
    /// |-----------------|---------|
    /// | `rocksdb.num-snapshots` | Unreleased **Snapshot** count (holding snapshots pins old SSTs). |
    /// | `rocksdb.oldest-snapshot-time` | **Unix timestamp (seconds)** of the oldest snapshot. |
    ///
    /// **CF-level metrics (per column family)**
    ///
    /// | Property suffix | Meaning |
    /// |-----------------|---------|
    /// | `rocksdb.cur-size-all-mem-tables` | This CF: active + unflushed immutable memtables (bytes). |
    /// | `rocksdb.cur-size-active-mem-table` | This CF: **active** memtable size (bytes). |
    /// | `rocksdb.size-all-mem-tables` | This CF: memtable size including pinned immutable (bytes). |
    /// | `rocksdb.num-immutable-mem-table` | This CF: count of unflushed **immutable** memtables (not bytes). |
    /// | `rocksdb.estimate-table-readers-mem` | This CF: estimated table reader memory (bytes). |
    ///
    /// **CF-level metrics (compaction / flush pressure)**
    ///
    /// | Property suffix | Meaning |
    /// |-----------------|---------|
    /// | `rocksdb.compaction-pending` | Whether compaction is pending (**0/1**). |
    /// | `rocksdb.mem-table-flush-pending` | Whether memtable flush is pending (**0/1**). |
    /// | `rocksdb.estimate-pending-compaction-bytes` | Estimated bytes to rewrite for pending compaction (level-style, **bytes**). |
    ///
    /// **CF-level metrics (versions / long-lived readers)**
    ///
    /// | Property suffix | Meaning |
    /// |-----------------|---------|
    /// | `rocksdb.num-live-versions` | Number of **live Version** structs; high values often correlate with unreleased iterators, snapshots, or unfinished compactions. |
    ///
    /// **Approximate “RocksDB internal accounted memory” (bytes, not process RSS)**
    ///
    /// Use **three parts** (do **not** use `db_rocksdb_size-all-mem-tables` / `db_rocksdb_estimate-table-readers-mem`
    /// for multi-CF totals—they track **default CF only**; see `db_*` note above):
    ///
    /// 1. **Block cache (once, DB-wide):** `db_rocksdb_block-cache-usage`.
    /// 2. **Memtables (sum all CFs):** add every `cf_{cf}_rocksdb_size-all-mem-tables` (same property as
    ///    `rocksdb.size-all-mem-tables` per column family).
    /// 3. **Table readers (sum all CFs):** add every `cf_{cf}_rocksdb_estimate-table-readers-mem`.
    ///
    /// **Do not** add `block-cache-pinned-usage` on top of `block-cache-usage` (pinned is usually part of usage);
    /// **do not** treat `block-cache-capacity` as used bytes. **Do not** add both `db_*` and `cf_*` memtable
    /// or table-reader figures for the same scope.
    ///
    /// This sum is a coarse RocksDB-internal estimate; it **excludes** WAL, OS page cache, etc., and **does not equal** process memory in `top`/`ps`.
    pub fn get_rocksdb_metrics(&self) -> CommonResult<HashMap<String, u64>> {
        let mut info = HashMap::new();

        // DB scope: block cache; default-CF memtable/table-reader ints (see doc); flush/compaction; snapshots.
        let db_keys = vec![
            properties::BLOCK_CACHE_CAPACITY,
            properties::BLOCK_CACHE_USAGE,
            properties::BLOCK_CACHE_PINNED_USAGE,
            properties::SIZE_ALL_MEM_TABLES,
            properties::ESTIMATE_TABLE_READERS_MEM,
            properties::NUM_RUNNING_FLUSHES,
            properties::NUM_RUNNING_COMPACTIONS,
            properties::NUM_SNAPSHOTS,
            properties::OLDEST_SNAPSHOT_TIME,
        ];
        for key in db_keys {
            if let Some(value) = self.db.property_int_value(key)? {
                info.insert(format!("db_{}", key.as_str().replace(".", "_")), value);
            }
        }

        // CF scope: memtables, immutables, table readers; compaction/flush backlog; live versions.
        let cf_keys = vec![
            properties::CUR_SIZE_ALL_MEM_TABLES,
            properties::CUR_SIZE_ACTIVE_MEM_TABLE,
            properties::SIZE_ALL_MEM_TABLES,
            properties::NUM_IMMUTABLE_MEM_TABLE,
            properties::ESTIMATE_TABLE_READERS_MEM,
            properties::COMPACTION_PENDING,
            properties::MEM_TABLE_FLUSH_PENDING,
            properties::ESTIMATE_PENDING_COMPACTION_BYTES,
            properties::NUM_LIVE_VERSIONS,
        ];

        for cf_name in &self.conf.family_list {
            if let Some(cf) = self.db.cf_handle(cf_name) {
                for &key in &cf_keys {
                    if let Some(value) = self.db.property_int_value_cf(cf, key)? {
                        info.insert(
                            format!("cf_{}_{}", cf_name, key.as_str().replace(".", "_")),
                            value,
                        );
                    }
                }
            }
        }

        Ok(info)
    }

    pub fn conf(&self) -> &DBConf {
        &self.conf
    }
}

pub struct RocksIterator<'a> {
    inner: DBIteratorWithThreadMode<'a, DB>,
}

impl Iterator for RocksIterator<'_> {
    type Item = Result<KVBytes, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
