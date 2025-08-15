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
use crate::master::meta::BlockMeta;
use curvine_common::rocksdb::{DBConf, DBEngine, RocksIterator, RocksUtils};
use curvine_common::state::{BlockLocation, MountInfo};
use curvine_common::utils::SerdeUtils as Serde;
use orpc::CommonResult;
use rocksdb::{DBIteratorWithThreadMode, WriteBatchWithTransaction, DB};

pub struct RocksInodeStore {
    pub(crate) db: DBEngine,
}

impl RocksInodeStore {
    pub const CF_INODES: &'static str = "inodes";
    pub const CF_EDGES: &'static str = "edges";
    pub const CF_BLOCK: &'static str = "block";
    pub const CF_LOCATION: &'static str = "location";
    pub const CF_MOUNTPOINT: &'static str = "mountpoints";

    pub fn new(conf: DBConf, format: bool) -> CommonResult<Self> {
        let conf = conf
            .add_cf(Self::CF_INODES)
            .add_cf(Self::CF_EDGES)
            .add_cf(Self::CF_BLOCK)
            .add_cf(Self::CF_LOCATION)
            .add_cf(Self::CF_MOUNTPOINT);
        let db = DBEngine::new(conf, format)?;
        Ok(Self { db })
    }

    pub fn get_child_ids(&self, id: i64, prefix: Option<&str>) -> CommonResult<InodeChildrenIter> {
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
    pub fn get_locations(&self, id: i64) -> CommonResult<Vec<BlockLocation>> {
        let prefix = RocksUtils::i64_to_bytes(id);
        let iter = self.db.prefix_scan(Self::CF_LOCATION, prefix)?;

        let mut vec = Vec::with_capacity(8);
        for item in iter {
            let bytes = item?;
            let location = Serde::deserialize::<BlockLocation>(&bytes.1)?;
            vec.push(location);
        }

        Ok(vec)
    }

    pub fn new_batch(&self) -> InodeWriteBatch {
        InodeWriteBatch::new(&self.db)
    }

    pub fn inodes_iter(&self) -> CommonResult<RocksIterator> {
        self.db.scan(Self::CF_INODES)
    }

    pub fn edges_iter(&self, id: i64) -> CommonResult<RocksIterator> {
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
                let inode: InodeView = Serde::deserialize(&v)?;
                Ok(Some(inode))
            }
        }
    }

    pub fn iter_cf<'a: 'b, 'b>(
        &'a self,
        cf: &str,
    ) -> CommonResult<DBIteratorWithThreadMode<'b, DB>> {
        self.db.iter_cf_opt(cf)
    }

    pub fn delete_locations(&self, worker_id: u32) -> CommonResult<()> {
        let prefix = RocksUtils::u32_to_bytes(worker_id);
        self.db.prefix_delete(Self::CF_LOCATION, prefix)
    }

    pub fn add_mountpoint(&self, id: u32, entry: &MountInfo) -> CommonResult<()> {
        let key = RocksUtils::u32_to_bytes(id);
        let value = Serde::serialize(entry).unwrap();
        self.db.put_cf(RocksInodeStore::CF_MOUNTPOINT, key, value)
    }

    pub fn remove_mountpoint(&self, id: u32) -> CommonResult<()> {
        let key = RocksUtils::u32_to_bytes(id);
        self.db.delete_cf(RocksInodeStore::CF_MOUNTPOINT, key)
    }

    pub fn get_mount_info(&self, id: u32) -> CommonResult<Option<MountInfo>> {
        let bytes = self
            .db
            .get_cf(Self::CF_MOUNTPOINT, RocksUtils::u32_to_bytes(id))?;

        match bytes {
            None => Ok(None),

            Some(v) => {
                let info: MountInfo = Serde::deserialize(&v)?;
                Ok(Some(info))
            }
        }
    }

    pub fn get_mount_table(&self) -> CommonResult<Vec<MountInfo>> {
        let iter = self.db.scan(Self::CF_MOUNTPOINT)?;
        let mut vec = Vec::with_capacity(8);
        for item in iter {
            let bytes = item?;
            let mnt = Serde::deserialize::<MountInfo>(&bytes.1)?;
            vec.push(mnt);
        }

        Ok(vec)
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
                    let id = RocksUtils::i64_from_bytes(&bytes.1).unwrap();
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
        let key = RocksUtils::i64_u32_to_bytes(id, loc.worker_id);
        let value = Serde::serialize(loc).unwrap();
        self.put_cf(RocksInodeStore::CF_LOCATION, key, value)
    }

    // Add an inode.
    pub fn write_inode(&mut self, inode: &InodeView) -> CommonResult<()> {
        let key = RocksUtils::i64_to_bytes(inode.id());
        let value = Serde::serialize(inode)?;
        self.put_cf(RocksInodeStore::CF_INODES, key, value)
    }

    pub fn write_block(&mut self, meta: &BlockMeta) -> CommonResult<()> {
        let key = RocksUtils::i64_to_bytes(meta.id);
        let value = Serde::serialize(meta)?;
        self.put_cf(RocksInodeStore::CF_BLOCK, key, value)
    }

    pub fn delete_block(&mut self, meta: &BlockMeta) -> CommonResult<()> {
        let key = RocksUtils::i64_to_bytes(meta.id);
        self.delete_cf(RocksInodeStore::CF_BLOCK, key)
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
        self.delete_cf(RocksInodeStore::CF_LOCATION, key)
    }

    pub fn commit(self) -> CommonResult<()> {
        self.db.write_batch(self.batch)
    }
}
