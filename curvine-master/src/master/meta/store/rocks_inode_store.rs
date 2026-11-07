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
use curvine_config::CfMergeOperator;
use curvine_core_error::{err_box, CommonResult};
use curvine_model::{
    encode_matching, BlockLocation, DirectoryAttributeDelta, DirectoryAttributes, FileLock,
    MountInfo,
};
use curvine_raft::proto::raft::AppliedIndex;
use curvine_rocksdb::{
    DBConf, DBEngine, DbReadSnapshot as DBReadSnapshot, ReadOptions, RocksIterator, RocksUtils,
};
use curvine_rocksdb::{
    DBIteratorWithThreadMode, DBPinnableSlice, Error, WriteBatchWithTransaction, DB,
};
use prost::Message as _;
use std::collections::HashMap;

#[cfg(test)]
thread_local! {
    static LOCATION_FALLBACKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub struct RocksInodeStore {
    pub(crate) db: DBEngine,
}

impl RocksInodeStore {
    pub const CF_INODES: &'static str = "inodes";
    pub const CF_EDGES: &'static str = "edges";
    pub const CF_BLOCK: &'static str = "block";
    pub const CF_LOCATION: &'static str = "location";
    pub const CF_COMMON: &'static str = "common";
    pub const CF_DIR_ATTRS: &'static str = "dir_attrs";

    pub const PREFIX_MOUNT: u8 = 0x01;
    pub const PREFIX_LOCK: u8 = 0x02;
    pub const PREFIX_UFS_DEFERRED: u8 = 0x03;
    pub const PREFIX_APPLIED_REQ: u8 = 0x04;

    // Single-key marker for mount table version negotiation. It shares the
    // applied-request prefix but not its nine-byte key layout; prefix-range
    // deletion must exclude this one-byte key.
    pub const PREFIX_MOUNT_TABLE_VERSION: u8 = 0x04;

    /// System row key for the journal applied cursor (see `put_common_kv`).
    pub const JOURNAL_APPLIED_KEY: &str = "journal_applied";
    const PREFIX_SCHEMA: u8 = 0x05;
    const DIRECTORY_ATTRIBUTE_SCHEMA: u8 = 0x01;
    const DIRECTORY_ATTRIBUTE_SCHEMA_VERSION: u8 = 0x01;

    pub fn new(conf: DBConf, format: bool) -> CommonResult<Self> {
        // Cursor WAL sync must also cover earlier namespace batches; writes
        // without WAL can otherwise disappear while the cursor survives.
        let conf = conf
            .set_disable_wal(false)
            .add_cf(Self::CF_INODES)
            .add_cf(Self::CF_EDGES)
            .add_cf(Self::CF_BLOCK)
            .add_cf(Self::CF_LOCATION)
            .add_cf(Self::CF_COMMON)
            .add_cf(Self::CF_DIR_ATTRS)
            .set_cf_merge_operator(Self::CF_DIR_ATTRS, CfMergeOperator::DirectoryAttributes);
        let db = DBEngine::new(conf, format)?;
        Ok(Self { db })
    }

    /// Reset namespace rows so the committed Raft log alone determines the
    /// recovered tree.
    ///
    /// Local rows may contain mutations that never reached Raft commit (the
    /// write path materializes rows before the journal commit receipt). When
    /// no registered snapshot exists and the log still starts at index 1,
    /// replaying `[1..commit]` onto an empty namespace reproduces exactly the
    /// committed state (the same convergence the follower path already
    /// guarantees). This clears every namespace column family, drops pending
    /// UFS-deferred records (their journal entries are replayed again), and
    /// zeroes the applied cursor so replay starts at 1.
    pub fn reset_namespace_for_replay(&self) -> CommonResult<()> {
        for cf in [
            Self::CF_INODES,
            Self::CF_EDGES,
            Self::CF_BLOCK,
            Self::CF_LOCATION,
            Self::CF_DIR_ATTRS,
        ] {
            self.db.clear_cf(cf)?;
        }
        self.db
            .prefix_delete(Self::CF_COMMON, [Self::PREFIX_UFS_DEFERRED])?;
        // Preserve the one-byte mount table version key sharing this prefix.
        let cf = self.db.cf(Self::CF_COMMON)?;
        let mut batch = WriteBatchWithTransaction::<false>::default();
        batch.delete_range_cf(
            cf,
            &[Self::PREFIX_APPLIED_REQ, 0][..],
            &[Self::PREFIX_APPLIED_REQ + 1][..],
        );
        self.db.write_batch(batch)?;
        let zero = AppliedIndex::default();
        self.db.put_cf_sync_wal(
            Self::CF_COMMON,
            Self::JOURNAL_APPLIED_KEY,
            &zero.encode_to_vec(),
        )?;
        Ok(())
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

    /// Reuse one iterator only within the caller's namespace guard.
    /// Results are owned; no iterator, pinned value, or cache escapes the call.
    pub fn get_locations_many(
        &self,
        ids: impl IntoIterator<Item = i64>,
    ) -> CommonResult<HashMap<i64, Vec<BlockLocation>>> {
        let ids: Vec<i64> = ids.into_iter().collect();
        // Bound the shared iterator to the requested range so seeking past the
        // last requested prefix cannot walk tombstones of neighbouring blocks.
        // Keys are big-endian, so byte order is the unsigned order of the id.
        let mut opt = ReadOptions::default();
        let bounded = ids.iter().copied().filter(|id| *id != -1);
        if let (Some(min), Some(max)) = (
            bounded.clone().min_by_key(|id| *id as u64),
            bounded.max_by_key(|id| *id as u64),
        ) {
            opt.set_iterate_lower_bound(RocksUtils::i64_to_bytes(min));
            let upper = RocksUtils::calculate_end_bytes(&RocksUtils::i64_to_bytes(max));
            if !upper.is_empty() {
                opt.set_iterate_upper_bound(upper);
            }
        }
        // DBConf currently uses the default ordered memtable and no prefix extractor.
        let mut iter = self
            .db
            .get_db()
            .raw_iterator_cf_opt(self.db.cf(Self::CF_BLOCK)?, opt);
        let mut result = HashMap::with_capacity(ids.len());
        for id in ids {
            // Preserve the existing all-0xff prefix boundary behavior.
            if id == -1 {
                result.insert(id, self.get_locations(id)?);
                continue;
            }
            let prefix = RocksUtils::i64_to_bytes(id);
            iter.seek(prefix);
            let mut locations = Vec::with_capacity(8);
            while let Some((key, value)) = iter.item() {
                if !key.starts_with(&prefix) {
                    break;
                }
                locations.push(MetadataCodec::decode::<BlockLocation>(value)?);
                iter.next();
            }
            if iter.status().is_err() {
                // The shared iterator spans every requested prefix and may touch
                // a corrupt SST outside this one. Retry the per-block query to preserve its
                // error scope; corruption inside the requested prefix still
                // propagates. No partial location list is published.
                #[cfg(test)]
                LOCATION_FALLBACKS.with(|count| count.set(count.get() + 1));
                locations = self.get_locations(id)?;
            }
            result.insert(id, locations);
        }
        Ok(result)
    }

    pub fn new_batch(&self) -> InodeWriteBatch<'_> {
        InodeWriteBatch::new(&self.db)
    }

    pub fn inodes_iter(&self) -> CommonResult<RocksIterator<'_>> {
        self.db.scan(Self::CF_INODES)
    }

    /// Scan the registered dir_attrs column family.
    pub fn scan_directory_attributes(&self) -> CommonResult<RocksIterator<'_>> {
        self.db.scan(Self::CF_DIR_ATTRS)
    }

    pub fn edges_iter(&self, id: i64) -> CommonResult<RocksIterator<'_>> {
        self.db
            .prefix_scan(Self::CF_EDGES, RocksUtils::i64_to_bytes(id))
    }

    pub fn get_directory_attributes(&self, id: i64) -> CommonResult<Option<DirectoryAttributes>> {
        let value = self
            .db
            .get_pinned_cf(Self::CF_DIR_ATTRS, RocksUtils::i64_to_bytes(id))?;
        Self::decode_directory_attributes(id, value.as_deref())
    }

    pub fn initialize_root_directory_attributes(&self) -> CommonResult<()> {
        use crate::master::meta::inode::ROOT_INODE_ID;

        if self.get_directory_attributes(ROOT_INODE_ID)?.is_none() {
            let mut batch = self.new_batch();
            batch.write_directory_attributes(ROOT_INODE_ID, DirectoryAttributes::new(0, 0, 2))?;
            batch.commit()?;
        }
        Ok(())
    }

    pub fn directory_attributes_migrated(&self) -> CommonResult<bool> {
        let key = [Self::PREFIX_SCHEMA, Self::DIRECTORY_ATTRIBUTE_SCHEMA];
        Ok(self
            .db
            .get_cf(Self::CF_COMMON, key)?
            .is_some_and(|value| value == [Self::DIRECTORY_ATTRIBUTE_SCHEMA_VERSION]))
    }

    pub fn mark_directory_attributes_migrated(&self) -> CommonResult<()> {
        self.db.put_cf(
            Self::CF_COMMON,
            [Self::PREFIX_SCHEMA, Self::DIRECTORY_ATTRIBUTE_SCHEMA],
            [Self::DIRECTORY_ATTRIBUTE_SCHEMA_VERSION],
        )
    }

    pub fn directory_attribute_ids(&self) -> CommonResult<Vec<i64>> {
        self.db
            .bulk_scan(Self::CF_DIR_ATTRS)?
            .map(|entry| {
                let (key, _) = entry?;
                RocksUtils::i64_from_bytes(&key)
            })
            .collect()
    }

    pub(super) fn decode_directory_attributes(
        id: i64,
        value: Option<&[u8]>,
    ) -> CommonResult<Option<DirectoryAttributes>> {
        match value {
            Some(value) => match DirectoryAttributes::decode(value) {
                Some(attributes) => Ok(Some(attributes)),
                None => err_box!("invalid directory attributes for inode {}", id),
            },
            None => Ok(None),
        }
    }

    pub(super) fn decode_inode(id: i64, bytes: &[u8]) -> CommonResult<InodeView> {
        let inode: InodeView = MetadataCodec::decode(bytes)?;
        if inode.id() != id {
            return err_box!(
                "inode record id mismatch: expected {}, got {}",
                id,
                inode.id()
            );
        }
        Ok(inode)
    }

    pub fn get_inode(&self, id: i64) -> CommonResult<Option<InodeView>> {
        let bytes = self
            .db
            .get_pinned_cf(Self::CF_INODES, RocksUtils::i64_to_bytes(id))?;
        match bytes {
            None => Ok(None),

            Some(v) => {
                let inode = Self::decode_inode(id, &v)?;
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

        // Keep both indexes consistent even if a later batch fails. Bounded
        // batches avoid building a single write batch for a large worker.
        for chunk in block_ids.chunks(4096) {
            let mut batch = self.new_batch();
            for block_id in chunk {
                batch.delete_location(*block_id, worker_id)?;
            }
            batch.commit()?;
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

    pub fn save_mount_table_version(&self, version: u64) -> CommonResult<()> {
        let key = [Self::PREFIX_MOUNT_TABLE_VERSION];
        let value = MetadataCodec::encode(&version)?;
        self.db.put_cf(Self::CF_COMMON, key, value)
    }

    /// Missing key (fresh cluster / pre-upgrade store) reads as version 0.
    pub fn get_mount_table_version(&self) -> CommonResult<u64> {
        let key = [Self::PREFIX_MOUNT_TABLE_VERSION];
        match self.db.get_cf(Self::CF_COMMON, key)? {
            None => Ok(0),
            Some(v) => MetadataCodec::decode(&v),
        }
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

    fn applied_req_key(req_id: i64) -> [u8; 9] {
        let mut key = [0u8; 9];
        key[0] = Self::PREFIX_APPLIED_REQ;
        key[1..].copy_from_slice(&req_id.to_be_bytes());
        key
    }

    /// Persist which request IDs already applied, with their entry index, so a
    /// duplicate proposal that reaches a new leader after a restart or
    /// failover cannot double-apply namespace mutations.
    pub fn put_applied_req(&self, req_id: i64, index: u64, term: u64) -> CommonResult<()> {
        let applied = AppliedIndex {
            term,
            index,
            ..Default::default()
        };
        self.db.put_cf(
            Self::CF_COMMON,
            Self::applied_req_key(req_id),
            &applied.encode_to_vec(),
        )
    }

    pub fn get_applied_req(&self, req_id: i64) -> CommonResult<Option<AppliedIndex>> {
        match self
            .db
            .get_cf(Self::CF_COMMON, Self::applied_req_key(req_id))?
        {
            Some(bytes) => Ok(Some(AppliedIndex::decode(bytes.as_slice())?)),
            None => Ok(None),
        }
    }

    /// Drop applied-request records whose entry index is below the retention
    /// window; they can no longer collide with a live retry.
    pub fn prune_applied_req_below(&self, first_kept_index: u64) -> CommonResult<()> {
        let cf = self.db.cf(Self::CF_COMMON)?;
        let mut batch = WriteBatchWithTransaction::<false>::default();
        let mut dropped = 0usize;
        for item in self
            .db
            .prefix_scan(Self::CF_COMMON, [Self::PREFIX_APPLIED_REQ])?
        {
            let (key, value) = match item {
                Ok(item) => item,
                Err(_) => continue,
            };
            if key.len() != 9 {
                continue;
            }
            let expired = match AppliedIndex::decode(value.as_ref()) {
                Ok(applied) => applied.index < first_kept_index,
                // A corrupted record can never match a live retry again.
                Err(_) => true,
            };
            if expired {
                batch.delete_cf(cf, &key);
                dropped += 1;
            }
        }
        if dropped > 0 {
            self.db.write_batch(batch)?;
        }
        Ok(())
    }

    /// Synchronize the cursor WAL record after preceding namespace writes.
    /// This does not remove uncommitted namespace writes after a crash.
    pub fn put_common_kv(&self, key: &str, value: &[u8]) -> CommonResult<()> {
        self.db
            .put_cf_sync_wal(Self::CF_COMMON, key.as_bytes(), value)
    }

    pub fn get_common_kv(&self, key: &str) -> CommonResult<Option<Vec<u8>>> {
        self.db.get_cf(Self::CF_COMMON, key.as_bytes())
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

    pub fn write_directory_attributes(
        &mut self,
        id: i64,
        attributes: DirectoryAttributes,
    ) -> CommonResult<()> {
        let key = RocksUtils::i64_to_bytes(id);
        let existing = self.db.get_cf(RocksInodeStore::CF_DIR_ATTRS, key)?;
        if existing
            .as_deref()
            .is_some_and(|value| DirectoryAttributes::decode(value).is_none())
        {
            return err_box!("invalid directory attributes record for inode {}", id);
        }
        self.put_cf(
            RocksInodeStore::CF_DIR_ATTRS,
            key,
            encode_matching(
                existing.as_deref(),
                &attributes.encode(),
                &attributes.encode_legacy(),
            ),
        )
    }

    /// Backfills an attribute row for an existing database without claiming a
    /// child count that was never stored. Migration records must remain
    /// readable by older replicas even if a newer process has enabled CDA4.
    pub fn write_legacy_directory_attributes(
        &mut self,
        id: i64,
        attributes: DirectoryAttributes,
    ) -> CommonResult<()> {
        self.put_cf(
            RocksInodeStore::CF_DIR_ATTRS,
            RocksUtils::i64_to_bytes(id),
            attributes.encode_legacy(),
        )
    }

    pub fn merge_directory_attributes(
        &mut self,
        id: i64,
        delta: DirectoryAttributeDelta,
    ) -> CommonResult<()> {
        let cf = self.db.cf(RocksInodeStore::CF_DIR_ATTRS)?;
        self.batch.merge_cf(
            cf,
            RocksUtils::i64_to_bytes(id),
            encode_matching(None, &delta.encode(), &delta.encode_legacy()),
        );
        Ok(())
    }

    pub fn delete_directory_attributes(&mut self, id: i64) -> CommonResult<()> {
        self.delete_cf(RocksInodeStore::CF_DIR_ATTRS, RocksUtils::i64_to_bytes(id))
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

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::StorageType;
    use curvine_rocksdb::perf::{set_perf_stats, PerfContext, PerfMetric, PerfStatsLevel};
    use curvine_runtime::common::Utils;

    #[test]
    fn reused_location_iterator_does_not_expand_corrupt_sst_scope() {
        use std::io::{Read, Seek, SeekFrom, Write};
        for compact in [false, true] {
            let mut conf = DBConf::new(Utils::test_sub_dir(format!(
                "location-corrupt-neighbor-{}",
                Utils::rand_str(8)
            )));
            conf.disable_block_cache = true;
            let store = RocksInodeStore::new(conf.clone(), true).unwrap();
            for id in [100, 5000, 10000] {
                let mut batch = store.new_batch();
                batch
                    .add_location(id, &BlockLocation::new(100, StorageType::Disk))
                    .unwrap();
                batch.commit().unwrap();
                store.db.flush(true).unwrap();
                if compact {
                    store.db.get_db().compact_range_cf(
                        store.db.cf(RocksInodeStore::CF_BLOCK).unwrap(),
                        Some(RocksUtils::i64_to_bytes(id)),
                        Some(RocksUtils::i64_to_bytes(id + 1)),
                    );
                }
            }
            // Keep the corrupt key in the iterator's range, but in a separate
            // SST from both requested prefixes.
            let bad_key = RocksUtils::i64_u32_to_bytes(5000, 100);
            let live = store.db.get_db().live_files().unwrap();
            let bad = live
                .iter()
                .find(|file| {
                    file.column_family_name == RocksInodeStore::CF_BLOCK
                        && file.start_key.as_deref() == Some(bad_key.as_slice())
                        && file.end_key.as_deref() == Some(bad_key.as_slice())
                })
                .unwrap();
            let bad_level = bad.level;
            if compact {
                assert!(bad_level > 0, "test must exercise a non-L0 SST");
            }
            let bad_path = store
                .db
                .get_db()
                .path()
                .join(bad.name.trim_start_matches('/'));
            drop(store);
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&bad_path)
                .unwrap();
            let mut byte = [0u8];
            file.read_exact(&mut byte).unwrap();
            byte[0] ^= 0x80;
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&byte).unwrap();
            file.sync_all().unwrap();
            drop(file);
            let reopened = RocksInodeStore::new(conf, false).unwrap();
            assert!(
                reopened.get_locations(5000).is_err(),
                "fault must be detected"
            );
            let outside_a = reopened.get_locations(100);
            let outside_b = reopened.get_locations(101);
            let outside = reopened.get_locations_many([100, 101]);
            assert_eq!(
                outside.is_ok(),
                outside_a.is_ok() && outside_b.is_ok(),
                "corruption beyond the upper bound must preserve per-block error scope"
            );
            let a = reopened.get_locations(100);
            let b = reopened.get_locations(10000);
            let baseline_ok = a.is_ok() && b.is_ok();
            assert!(
                reopened.get_locations_many([100, 5000]).is_err(),
                "requested SST corruption must propagate"
            );
            LOCATION_FALLBACKS.with(|count| count.set(0));
            let candidate = reopened.get_locations_many([100, 10000]);
            assert_eq!(
                candidate.is_ok(),
                baseline_ok,
                "corruption between requested blocks must preserve per-block error scope"
            );
            if compact {
                assert!(baseline_ok, "compacted good SSTs must remain readable");
                assert!(
                    LOCATION_FALLBACKS.with(|count| count.get()) > 0,
                    "the in-range corrupt SST must exercise the per-block fallback"
                );
            }
            let workers = |list: &[BlockLocation]| {
                list.iter()
                    .map(|x| (x.worker_id, x.storage_type))
                    .collect::<Vec<_>>()
            };
            if let Ok(outside) = outside {
                assert_eq!(workers(&outside[&100]), workers(&outside_a.unwrap()));
                assert_eq!(workers(&outside[&101]), workers(&outside_b.unwrap()));
            }
            if let Ok(candidate) = candidate {
                assert_eq!(workers(&candidate[&100]), workers(&a.unwrap()));
                assert_eq!(workers(&candidate[&10000]), workers(&b.unwrap()));
            }
        }
    }

    #[test]
    fn reused_location_iterator_matches_prefix_queries() {
        let conf = DBConf::new(Utils::test_sub_dir(format!(
            "location-iterator-{}",
            Utils::rand_str(8)
        )));
        let store = RocksInodeStore::new(conf, true).unwrap();
        let stored = [i64::MIN, -42, -1, 0, 1, 8192, i64::MAX];
        let mut batch = store.new_batch();
        for (i, id) in stored.iter().enumerate() {
            batch
                .add_location(*id, &BlockLocation::new(100, StorageType::Ssd))
                .unwrap();
            if i % 2 == 0 {
                batch
                    .add_location(*id, &BlockLocation::new(101, StorageType::Disk))
                    .unwrap();
            }
        }
        batch.commit().unwrap();
        let requested = [8192, -42, -1, 2, i64::MAX, i64::MIN, 0, 1, 8192];
        for flush in [false, true] {
            if flush {
                store.db.flush(true).unwrap();
            }
            let actual = store.get_locations_many(requested).unwrap();
            for id in requested {
                let expected = store.get_locations(id).unwrap();
                let pairs = |v: &[BlockLocation]| {
                    v.iter()
                        .map(|x| (x.worker_id, x.storage_type))
                        .collect::<Vec<_>>()
                };
                assert_eq!(pairs(&actual[&id]), pairs(&expected));
            }
        }
        assert!(store.get_locations_many([]).unwrap().is_empty());
        store.delete_locations(100).unwrap();
        let actual = store.get_locations_many(requested).unwrap();
        for id in requested {
            let expected = store.get_locations(id).unwrap();
            assert_eq!(
                actual[&id].iter().map(|x| x.worker_id).collect::<Vec<_>>(),
                expected.iter().map(|x| x.worker_id).collect::<Vec<_>>()
            );
        }
        // Corrupt a requested value: the candidate must propagate decode failure.
        store
            .db
            .put_cf(
                RocksInodeStore::CF_BLOCK,
                RocksUtils::i64_u32_to_bytes(2, 101),
                [0xff],
            )
            .unwrap();
        assert!(store.get_locations(2).is_err());
        assert!(store.get_locations_many([2]).is_err());
    }

    #[test]
    fn reused_location_iterator_skips_no_trailing_tombstones() {
        let conf = DBConf::new(Utils::test_sub_dir(format!(
            "location-trailing-tombstones-{}",
            Utils::rand_str(8)
        )));
        let store = RocksInodeStore::new(conf, true).unwrap();
        let block_id = |inode: i64, seq: i64| (inode << 20) | seq;
        let requested = (0..4).map(|seq| block_id(1000, seq)).collect::<Vec<_>>();
        let mut batch = store.new_batch();
        for id in &requested {
            batch
                .add_location(*id, &BlockLocation::new(100, StorageType::Disk))
                .unwrap();
        }
        for seq in 0..1024 {
            batch
                .add_location(
                    block_id(1001, seq),
                    &BlockLocation::new(100, StorageType::Disk),
                )
                .unwrap();
        }
        batch
            .add_location(
                block_id(1002, 0),
                &BlockLocation::new(100, StorageType::Disk),
            )
            .unwrap();
        batch.commit().unwrap();
        let mut batch = store.new_batch();
        for seq in 0..1024 {
            batch.delete_location(block_id(1001, seq), 100).unwrap();
        }
        batch.commit().unwrap();

        set_perf_stats(PerfStatsLevel::EnableCount);
        let mut perf = PerfContext::default();
        perf.reset();
        let actual = store.get_locations_many(requested.iter().copied()).unwrap();
        let skipped = perf.metric(PerfMetric::InternalDeleteSkippedCount);
        set_perf_stats(PerfStatsLevel::Disable);

        assert_eq!(actual.len(), requested.len());
        for id in &requested {
            assert_eq!(actual[id].len(), 1);
        }
        assert_eq!(skipped, 0, "iterator walked past the requested blocks");
    }
}
