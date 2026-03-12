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

use crate::rocksdb::DBEngine;
use orpc::CommonResult;
use rocksdb::WriteBatchWithTransaction;

pub struct WriteBatch<'a> {
    db: &'a DBEngine,
    batch: WriteBatchWithTransaction<false>,
}

impl<'a> WriteBatch<'a> {
    pub fn new(db: &'a DBEngine) -> Self {
        Self {
            db,
            batch: WriteBatchWithTransaction::<false>::default(),
        }
    }

    pub fn put_cf<K, V>(&mut self, cf: &str, key: K, value: V) -> CommonResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let cf = self.db.cf(cf)?;
        self.batch.put_cf(cf, key, value);
        Ok(())
    }

    pub fn delete_cf<K>(&mut self, cf: &str, key: K) -> CommonResult<()>
    where
        K: AsRef<[u8]>,
    {
        let cf = self.db.cf(cf)?;
        self.batch.delete_cf(cf, key);
        Ok(())
    }

    pub fn commit(self) -> CommonResult<()> {
        self.db.write_batch(self.batch)
    }

    pub fn commit_flush(self, sync: bool) -> CommonResult<()> {
        self.db.write_batch(self.batch)?;
        self.db.flush(sync)?;
        Ok(())
    }

    pub fn commit_sync(self) -> CommonResult<()> {
        self.commit_flush(true)
    }
}
