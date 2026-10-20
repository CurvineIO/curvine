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

use crate::master::meta::inode::InodeView;
use crate::master::meta::LockMeta;
use crate::master::MetadataCodec;
use curvine_core_error::CommonResult;
use curvine_model::{BlockLocation, FileLock, MountInfo};
use curvine_rocksdb::{
    DBConf, DBEngine, DbReadSnapshot as DBReadSnapshot, RocksIterator, RocksUtils,
};
use curvine_rocksdb::{
    DBIteratorWithThreadMode, DBPinnableSlice, Error, WriteBatchWithTransaction, DB,
};
use std::collections::HashMap;

pub struct RocksInodeStore {
    pub(crate) db: DBEngine,
}

impl RocksInodeStore {
    pub const CF_INODES: &'static str = "inodes";
    pub const CF_EDGES: &'static str = "edges";
    pub const CF_BLOCK: &'static str = "block";
    pub const CF_LOCATION: &'static str = "location";
    pub const CF_COMMON: &'static str = "common";

    pub const PREFIX_MOUNT: u8 = 0x01;
    pub const PREFIX_LOCK: u8 = 0x02;
    pub const PREFIX_UFS_DEFERRED: u8 = 0x03;

    pub fn new(conf: DBConf, format: bool) -> CommonResult<Self> {
        let conf = conf
            .set_disable_wal(true)
            .add_cf(Self::CF_INODES)
            .add_cf(Self::CF_EDGES)
            .add_cf(Self::CF_BLOCK)
            .add_cf(Self::CF_LOCATION)
            .add_cf(Self::CF_COMMON);
        let db = DBEngine::new(conf, format)?;
        Ok(Self { db })
    }

    pub fn get_child_ids(
        &self,
        id: i64,
        prefix: Option<&str>,
    ) -> CommonResult<InodeChildrenIter<'_>> {
        let iter = match prefix {
            None => {
                let key = RocksUtils::i64_to_bytes(id);
                self.db.prefix_scan(Self::CF_EDGES, key)?
            }

            Some(v) => {
                let key = RocksUtils::i64_str_to_bytes(id, v);
                self.db.prefix_scan(Self::CF_EDGES, key)?
            }
        };

        Ok(InodeChildrenIter { inner: iter })
    }

    // Get all location information for all block ids.
    pub fn get_locations(&self, block_id: i64) -> CommonResult<Vec<BlockLocation>> {
        let prefix = RocksUtils::i64_to_bytes(block_id);
        let iter = self.db.prefix_scan(Self::CF_BLOCK, prefix)?;

        let mut vec = Vec::with_capacity(8);
        for item in iter {
            let bytes = item?;
            let location = MetadataCodec::decode::<BlockLocation>(&bytes.1)?;
            vec.push(location);
        }

        Ok(vec)
    }

    pub fn new_batch(&self) -> InodeWriteBatch<'_> {
        InodeWriteBatch::new(&self.db)
    }

    pub fn inodes_iter(&self) -> CommonResult<RocksIterator<'_>> {
        self.db.scan(Self::CF_INODES)
    }

    pub fn edges_iter(&self, id: i64) -> CommonResult<RocksIterator<'_>> {
        self.db
            .prefix_scan(Self::CF_EDGES, RocksUtils::i64_to_bytes(id))
    }

    pub fn get_inode(&self, id: i64) -> CommonResult<Option<InodeView>> {
        let bytes = self
            .db
            .get_cf(Self::CF_INODES, RocksUtils::i64_to_bytes(id))?;
        match bytes {
            None => Ok(None),

            Some(v) => {
                let inode: InodeView = MetadataCodec::decode(&v)?;
                Ok(Some(inode))
            }
        }
    }

    pub fn batched_multi_get_inodes<'a, K, I>(
        &'a self,
        keys: I,
        sorted_input: bool,
    ) -> CommonResult<Vec<Result<Option<DBPinnableSlice<'a>>, Error>>>
    where
        K: AsRef<[u8]> + 'a + ?Sized,
        I: IntoIterator<Item = &'a K>,
    {
        self.db
            .batched_multi_get_cf(Self::CF_INODES, keys, sorted_input)
    }

    pub fn iter_cf<'a: 'b, 'b>(
        &'a self,
        cf: &str,
    ) -> CommonResult<DBIteratorWithThreadMode<'b, DB>> {
        self.db.iter_cf_opt(cf)
    }

    /// Bulk-scan the entire `inodes` CF with tuned ReadOptions (total_order_seek +
    /// fill_cache(false) + 64 MiB readahead).  Use for snapshot restore.
    pub fn bulk_scan_inodes<'a: 'b, 'b>(
        &'a self,
    ) -> CommonResult<DBIteratorWithThreadMode<'b, DB>> {
        self.db.bulk_scan(Self::CF_INODES)
    }

    /// Bulk-scan the entire `edges` CF with tuned ReadOptions.
    pub fn bulk_scan_edges<'a: 'b, 'b>(&'a self) -> CommonResult<DBIteratorWithThreadMode<'b, DB>> {
        self.db.bulk_scan(Self::CF_EDGES)
    }

    /// Create a consistent multi-CF read snapshot for background scans.
    pub fn read_snapshot(&self) -> DBReadSnapshot<'_> {
        self.db.read_snapshot()
    }

    pub fn delete_locations(&self, worker_id: u32) -> CommonResult<Vec<i64>> {
        let block_ids = self.get_block_ids(worker_id)?;

        // delete all worker_id -> block_ids
        let prefix = RocksUtils::u32_to_bytes(worker_id);
        self.db.prefix_delete(Self::CF_LOCATION, prefix)?;

        // delete all block_id -> worker_ids
        for block_id in &block_ids {
            let key = RocksUtils::i64_u32_to_bytes(*block_id, worker_id);
            self.db.delete_cf(Self::CF_BLOCK, key)?;
        }

        Ok(block_ids)
    }

    pub fn get_block_ids(&self, worker_id: u32) -> CommonResult<Vec<i64>> {
        let prefix = RocksUtils::u32_to_bytes(worker_id);
        let iter = self.db.prefix_scan(Self::CF_LOCATION, prefix)?;

        let mut vec = Vec::with_capacity(8);
        for item in iter {
            let bytes = item?;
            let location = MetadataCodec::decode::<i64>(&bytes.1)?;
            vec.push(location);
        }

        Ok(vec)
    }

    pub fn add_mountpoint(&self, id: u32, entry: &MountInfo) -> CommonResult<()> {
        let key = RocksUtils::u8_u32_to_bytes(Self::PREFIX_MOUNT, id);
        let value = MetadataCodec::encode(entry)?;
        self.db.put_cf(Self::CF_COMMON, key, value)
    }

    pub fn remove_mountpoint(&self, id: u32) -> CommonResult<()> {
        let key = RocksUtils::u8_u32_to_bytes(Self::PREFIX_MOUNT, id);
        self.db.delete_cf(Self::CF_COMMON, key)
    }

    pub fn get_mount_info(&self, id: u32) -> CommonResult<Option<MountInfo>> {
        let key = RocksUtils::u8_u32_to_bytes(Self::PREFIX_MOUNT, id);

        let bytes = self.db.get_cf(Self::CF_COMMON, key)?;

        match bytes {
            None => Ok(None),

            Some(v) => {
                let info: MountInfo = MetadataCodec::decode(&v)?;
                Ok(Some(info))
            }
        }
    }

    pub fn get_mount_table(&self) -> CommonResult<Vec<MountInfo>> {
        let iter = self.db.prefix_scan(Self::CF_COMMON, [Self::PREFIX_MOUNT])?;
        let mut vec = Vec::with_capacity(8);
        for item in iter {
            let bytes = item?;
            let mnt: MountInfo = MetadataCodec::decode(&bytes.1)?;
            vec.push(mnt);
        }

        Ok(vec)
    }

    pub fn get_locks(&self, id: i64) -> CommonResult<LockMeta> {
        let key = RocksUtils::u8_i64_to_bytes(Self::PREFIX_LOCK, id);
        let bytes = self.db.get_cf(Self::CF_COMMON, key)?;

        if let Some(bytes) = bytes {
            let locks: Vec<FileLock> = MetadataCodec::decode(&bytes)?;
            Ok(LockMeta::with_vec(locks))
        } else {
            Ok(LockMeta::default())
        }
    }

    pub fn set_locks(&self, id: i64, lock: &[FileLock]) -> CommonResult<()> {
        let key = RocksUtils::u8_i64_to_bytes(Self::PREFIX_LOCK, id);
        if lock.is_empty() {
            self.db.delete_cf(RocksInodeStore::CF_COMMON, key)
        } else {
            let value = MetadataCodec::encode(&lock.to_vec())?;
            self.db.put_cf(RocksInodeStore::CF_COMMON, key, value)
        }
    }

    pub fn get_rocksdb_metrics(&self) -> CommonResult<HashMap<String, u64>> {
        self.db.get_rocksdb_metrics()
    }

    fn ufs_deferred_key(index: u64) -> [u8; 9] {
        let mut key = [0u8; 9];
        key[0] = Self::PREFIX_UFS_DEFERRED;
        key[1..].copy_from_slice(&index.to_be_bytes());
        key
    }

    pub fn put_ufs_deferred(&self, index: u64, payload: &[u8]) -> CommonResult<()> {
        self.db
            .put_cf(Self::CF_COMMON, Self::ufs_deferred_key(index), payload)
    }

    pub fn delete_ufs_deferred(&self, index: u64) -> CommonResult<()> {
        self.db
            .delete_cf(Self::CF_COMMON, Self::ufs_deferred_key(index))
    }

    pub fn ufs_deferred_records(&self) -> CommonResult<Vec<(u64, Vec<u8>)>> {
        let iter = self
            .db
            .prefix_scan(Self::CF_COMMON, [Self::PREFIX_UFS_DEFERRED])?;
        let mut records = Vec::new();
        for item in iter {
            let (key, value) = item?;
            if key.len() < 9 {
                continue;
            }
            let mut index_bytes = [0u8; 8];
            index_bytes.copy_from_slice(&key[1..9]);
            records.push((u64::from_be_bytes(index_bytes), value.to_vec()));
        }
        Ok(records)
    }
}

pub struct InodeChildrenIter<'a> {
    inner: RocksIterator<'a>,
}

impl Iterator for InodeChildrenIter<'_> {
    type Item = CommonResult<i64>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(v) = self.inner.next() {
            match v {
                Err(e) => Some(Err(e.into())),

                Ok(bytes) => {
                    let id = match RocksUtils::i64_from_bytes(&bytes.1) {
                        Ok(id) => id,
                        Err(e) => return Some(Err(e)),
                    };
                    Some(Ok(id))
                }
            }
        } else {
            None
        }
    }
}

pub struct InodeWriteBatch<'a> {
    db: &'a DBEngine,
    batch: WriteBatchWithTransaction<false>,
}

impl<'a> InodeWriteBatch<'a> {
    pub fn new(db: &'a DBEngine) -> Self {
        Self {
            db,
            batch: WriteBatchWithTransaction::<false>::default(),
        }
    }

    /// Raw CF put, for tests that inject corrupted records.
    #[cfg(test)]
    pub fn put_cf_raw<K, V>(&mut self, cf: &str, key: K, value: V) -> CommonResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_cf(cf, key, value)
    }

    fn put_cf<K, V>(&mut self, cf: &str, key: K, value: V) -> CommonResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let cf = self.db.cf(cf)?;
        self.batch.put_cf(cf, key, value);
        Ok(())
    }

    fn delete_cf<K>(&mut self, cf: &str, key: K) -> CommonResult<()>
    where
        K: AsRef<[u8]>,
    {
        let cf = self.db.cf(cf)?;
        self.batch.delete_cf(cf, key);
        Ok(())
    }

    pub fn add_location(&mut self, id: i64, loc: &BlockLocation) -> CommonResult<()> {
        // store with the key of  (block_id, worker_id)
        let key = RocksUtils::i64_u32_to_bytes(id, loc.worker_id);
        let value = MetadataCodec::encode(loc)?;
        self.put_cf(RocksInodeStore::CF_BLOCK, key, value)?;

        // store with the key of (worker_id, block_id)
        let key = RocksUtils::u32_i64_to_bytes(loc.worker_id, id);
        let value = MetadataCodec::encode(&id)?;
        self.put_cf(RocksInodeStore::CF_LOCATION, key, value)
    }

    // Add an inode.
    pub fn write_inode(&mut self, inode: &InodeView) -> CommonResult<()> {
        let key = RocksUtils::i64_to_bytes(inode.id());
        let value = MetadataCodec::encode(inode)?;
        self.put_cf(RocksInodeStore::CF_INODES, key, value)
    }

    // Add an edge to identify the subordinate relationship between inodes
    pub fn add_child(
        &mut self,
        parent_id: i64,
        child_name: &str,
        child_id: i64,
    ) -> CommonResult<()> {
        let key = RocksUtils::i64_str_to_bytes(parent_id, child_name);
        let value = RocksUtils::i64_to_bytes(child_id);
        self.put_cf(RocksInodeStore::CF_EDGES, key, value)
    }

    pub fn delete_inode(&mut self, id: i64) -> CommonResult<()> {
        let key = RocksUtils::i64_to_bytes(id);
        self.delete_cf(RocksInodeStore::CF_INODES, key)
    }

    // Delete a subordinate relationship between an inode
    pub fn delete_child(&mut self, parent_id: i64, child_name: &str) -> CommonResult<()> {
        let key = RocksUtils::i64_str_to_bytes(parent_id, child_name);
        self.delete_cf(RocksInodeStore::CF_EDGES, key)
    }

    // Delete 1 block to store information
    pub fn delete_location(&mut self, id: i64, worker_id: u32) -> CommonResult<()> {
        let key = RocksUtils::i64_u32_to_bytes(id, worker_id);
        self.delete_cf(RocksInodeStore::CF_BLOCK, key)?;

        let key = RocksUtils::u32_i64_to_bytes(worker_id, id);
        self.delete_cf(RocksInodeStore::CF_LOCATION, key)
    }

    pub fn commit(self) -> CommonResult<()> {
        self.db.write_batch(self.batch)
    }
}
