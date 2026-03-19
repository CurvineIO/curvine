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

use crate::proto::raft::SnapshotData;
use crate::raft::storage::AppStorage;
use crate::raft::{RaftResult, RaftUtils};
use crate::rocksdb::DBEngine;
use crate::utils::SerdeUtils;
use orpc::common::LocalTime;
use orpc::{try_err, try_option_ref, CommonResult};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Clone)]
pub struct HashAppStorage<K, V> {
    map: Arc<RwLock<HashMap<K, V>>>,
}

impl<K, V> Default for HashAppStorage<K, V>
where
    K: Clone + Hash + Eq,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> HashAppStorage<K, V>
where
    K: Clone + Hash + Eq,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn write(&self) -> CommonResult<RwLockWriteGuard<'_, HashMap<K, V>>> {
        let map = try_err!(self.map.write());
        Ok(map)
    }

    fn read(&self) -> CommonResult<RwLockReadGuard<'_, HashMap<K, V>>> {
        let map = try_err!(self.map.read());
        Ok(map)
    }

    pub fn get(&self, k: &K) -> CommonResult<Option<V>> {
        let map = try_err!(self.map.read());
        Ok(map.get(k).cloned())
    }

    pub fn len(&self) -> usize {
        let map = self.map.read().unwrap();
        map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<K, V> AppStorage for HashAppStorage<K, V>
where
    K: DeserializeOwned + Sized + Serialize + Clone + Hash + Eq + Send + Sync + 'static,
    V: DeserializeOwned + Sized + Serialize + Clone + Send + Sync + 'static,
{
    async fn apply(&self, _: bool, message: &[u8]) -> RaftResult<()> {
        let mut map = self.write()?;
        let pairs: (K, V) = SerdeUtils::deserialize(message)?;
        map.insert(pairs.0, pairs.1);
        Ok(())
    }

    fn create_snapshot(&self, node_id: u64, snapshot_id: u64) -> RaftResult<SnapshotData> {
        let map = self.read()?;
        let bytes = SerdeUtils::serialize(&*map)?;
        let data = SnapshotData {
            snapshot_id,
            node_id,
            create_time: LocalTime::mills(),
            bytes_data: Some(bytes),
            files_data: None,
        };
        Ok(data)
    }

    fn apply_snapshot(&self, data: &SnapshotData) -> RaftResult<()> {
        let data = try_option_ref!(data.bytes_data);
        let new: HashMap<K, V> = SerdeUtils::deserialize(data)?;
        let mut map = self.write()?;
        let _ = std::mem::replace(&mut *map, new);
        Ok(())
    }

    fn snapshot_dir(&self, _snapshot_id: u64) -> RaftResult<String> {
        panic!()
    }
}

#[derive(Clone)]
pub struct RocksAppStorage<K, V> {
    db: Arc<Mutex<DBEngine>>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K, V> RocksAppStorage<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    pub fn new<T: AsRef<str>>(dir: T) -> Self {
        let db = DBEngine::from_dir(dir, true).unwrap();
        Self {
            db: Arc::new(Mutex::new(db)),
            _k: Default::default(),
            _v: Default::default(),
        }
    }

    pub fn lock(&self) -> CommonResult<MutexGuard<'_, DBEngine>> {
        let db = try_err!(self.db.lock());
        Ok(db)
    }

    pub fn get(&self, k: &K) -> CommonResult<Option<V>> {
        let db = self.lock()?;
        let k_bytes = SerdeUtils::serialize(k)?;
        let bytes = db.get(k_bytes)?;

        match bytes {
            None => Ok(None),
            Some(v) => {
                let val = SerdeUtils::deserialize(&v)?;
                Ok(Some(val))
            }
        }
    }
}

impl<K, V> AppStorage for RocksAppStorage<K, V>
where
    K: Serialize + DeserializeOwned + Clone + Sync + Send + 'static,
    V: Serialize + DeserializeOwned + Clone + Sync + Send + 'static,
{
    async fn apply(&self, _: bool, message: &[u8]) -> RaftResult<()> {
        let db = self.lock()?;
        let pairs: (K, V) = SerdeUtils::deserialize(message)?;
        let k = SerdeUtils::serialize(&pairs.0)?;
        let v = SerdeUtils::serialize(&pairs.1)?;
        db.put(k, v)?;
        Ok(())
    }

    // Create a snapshot.
    fn create_snapshot(&self, node_id: u64, snapshot_id: u64) -> RaftResult<SnapshotData> {
        let db = self.lock()?;
        let dir = db.create_checkpoint(snapshot_id)?;
        let data = RaftUtils::create_file_snapshot(dir, node_id, snapshot_id)?;
        Ok(data)
    }

    fn apply_snapshot(&self, data: &SnapshotData) -> RaftResult<()> {
        let mut db = self.lock()?;
        let files = try_option_ref!(data.files_data);
        RaftUtils::apply_rocks_snapshot(&mut db, files)
    }

    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String> {
        let db = self.lock()?;
        Ok(db.get_checkpoint_path(snapshot_id))
    }
}
