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
use log::info;
use orpc::common::{FileUtils, Utils};
use orpc::{err_box, try_err, CommonResult};
use rocksdb::checkpoint::Checkpoint;
use rocksdb::*;

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
        let db_path = &self.conf.data_dir;
        let db_opt = self.conf.create_db_opt();
        let cfs = self.conf.create_cf_opt();

        //The database points to a temporary directory.
        let tmp_path = Utils::temp_file();
        self.db = try_err!(DB::open(&db_opt, &tmp_path));

        // Delete the original file and move the checkpoint to the data directory.
        FileUtils::delete_path(db_path, true)?;
        FileUtils::rename(checkpoint.as_ref(), db_path)?;

        // Retry instantiating db and delete the temporary directory.
        self.db = try_err!(DB::open_cf_with_opts(&db_opt, db_path, cfs));
        let _ = FileUtils::delete_path(tmp_path, true);

        Ok(())
    }

    // Get a reference to the column family.
    pub fn cf(&self, name: &str) -> CommonResult<&ColumnFamily> {
        match self.db.cf_handle(name) {
            Some(v) => Ok(v),
            None => err_box!("cf {} not exits", name),
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
    pub fn scan(&self, cf: &str) -> CommonResult<RocksIterator> {
        let opt = self.conf.create_read_opt();
        let cf = self.cf(cf)?;

        let mode = IteratorMode::Start;
        let iter = self.db.iterator_cf_opt(cf, opt, mode);
        Ok(RocksIterator { inner: iter })
    }

    // Describe the data in the specified key range of column families.
    // If the mem table is of hash type, then the method may not get the correct data. It needs to set set_total_order_seek to true
    pub fn range_scan<K>(&self, cf: &str, start: K, end: K) -> CommonResult<RocksIterator>
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
    pub fn prefix_scan<K>(&self, cf: &str, key: K) -> CommonResult<RocksIterator>
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
        let checkpoint_path = self.get_checkpoint_path(id);
        if FileUtils::exists(&checkpoint_path) {
            FileUtils::delete_path(&checkpoint_path, true)?;
        }

        FileUtils::create_parent_dir(&checkpoint_path, true)?;
        let checkpoint = try_err!(Checkpoint::new(&self.db));
        checkpoint.create_checkpoint(&checkpoint_path)?;

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

    pub fn flush(&self) -> CommonResult<()> {
        self.db.flush()?;
        Ok(())
    }

    pub fn flush_wal(&self, sync: bool) -> CommonResult<()> {
        self.db.flush_wal(sync)?;
        Ok(())
    }

    pub fn write_batch(&self, batch: WriteBatchWithTransaction<false>) -> CommonResult<()> {
        try_err!(self.db.write(batch));
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

    pub fn get_rocksdb_memory(&self) -> CommonResult<Vec<(String, u64)>> {
        let mut memory_info = Vec::with_capacity(4);

        let properties = [
            ("rocksdb.cur-size-all-mem-tables", "mem-tables"),
            ("rocksdb.block-cache-usage", "block-cache"),
            ("rocksdb.block-cache-pinned-usage", "block-cache-pinned"),
            ("rocksdb.estimate-table-readers-mem", "table-readers"),
        ];

        for (property, name) in properties {
            if let Some(value) = self.db.property_value(property)? {
                if let Ok(size) = value.parse::<u64>() {
                    memory_info.push((name.to_string(), size));
                }
            }
        }

        Ok(memory_info)
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
