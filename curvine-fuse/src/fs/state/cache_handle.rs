//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use curvine_fs_api::{StateReader, StateWriter};
use curvine_model::{CreateFileOpts, FileAllocOpts, FileStatus, LockFlags, OpenFlags};
use curvine_runtime::sync::{AsyncMutex, AtomicCounter};
use std::sync::{Arc, Mutex};

use crate::fs::operator::{Read, Write};
use crate::fs::pcache::InodeCache;
use crate::fs::state::backend_handle::HandleLock;
use crate::fs::state::NodeState;
use crate::fs::FuseReader;
use crate::raw::fuse_abi::fuse_write_out;
use crate::session::FuseResponse;
use crate::{FuseError, FuseMetrics, FuseResult};

pub struct CacheHandle {
    pub ino: u64,
    pub fh: u64,
    pub cache: Arc<InodeCache>,
    pub status: FileStatus,

    pub flags: OpenFlags,
    pub opts: CreateFileOpts,
    pub reader: AsyncMutex<Option<FuseReader>>,

    fh_locks: Mutex<HandleLock>,
    read_ver: AtomicCounter,
}

impl CacheHandle {
    pub fn new(
        ino: u64,
        fh: u64,
        cache: Arc<InodeCache>,
        status: FileStatus,
        flags: OpenFlags,
        opts: CreateFileOpts,
    ) -> Self {
        Self {
            ino,
            fh,
            cache,
            status,
            flags,
            opts,
            reader: AsyncMutex::new(None),
            fh_locks: Mutex::new(HandleLock::default()),
            read_ver: AtomicCounter::new(0),
        }
    }

    pub async fn read(
        &self,
        state: &NodeState,
        op: Read<'_>,
        reply: FuseResponse,
    ) -> FuseResult<()> {
        let cached = self.cache.read(op.arg.offset, op.arg.size as usize).await?;
        if let Some(v) = cached {
            let got: usize = v.iter().map(|s| s.len()).sum();
            if got == op.arg.size as usize || self.cache.reached_eof(op.arg.offset, got) {
                FuseMetrics::with(|m| {
                    m.write_back_io_bytes
                        .with_label_values(&["read_cache"])
                        .inc_by(got as i64);
                });

                reply.send_data(Ok(v)).await?;
                return Ok(());
            } else {
                FuseMetrics::with(|m| {
                    m.write_back_io_bytes
                        .with_label_values(&["read_half"])
                        .inc_by(got as i64);
                });
            }
        }

        let flush_ver = self.cache.ensure_flushed().await?;
        let mut lock = self.reader.lock().await;
        let read_ver = self.read_ver.get();
        let reader = match lock.as_mut() {
            Some(r) if read_ver >= flush_ver => r,

            _ => {
                if read_ver < flush_ver {
                    state.invalidate_file_blocks(self.ino);
                }

                let path = state.get_path(self.ino)?;
                let reader = state.open_reader(Some(self.ino), &path).await?;
                self.read_ver.set(flush_ver);
                lock.insert(reader)
            }
        };

        reader.read(op, reply).await?;
        Ok(())
    }

    pub async fn write(&self, op: Write<'_>, reply: FuseResponse) -> FuseResult<()> {
        if op.data.is_empty() {
            let res: FuseResult<fuse_write_out> = Ok(fuse_write_out {
                size: 0,
                padding: 0,
            });
            reply.send_rep(res).await?;
            return Ok(());
        }

        let res = self
            .cache
            .write(op.arg.offset, op.data)
            .await
            .map(|size| fuse_write_out {
                size: size as u32,
                padding: 0,
            });
        reply.send_rep(res).await?;
        Ok(())
    }

    pub async fn resize(&self, opts: FileAllocOpts) -> FuseResult<FileStatus> {
        self.cache.resize(opts).await
    }

    pub async fn flush(&self, reply: Option<FuseResponse>) -> FuseResult<()> {
        if self.flags.write() {
            self.cache.queue_flush()?;
        }
        if let Some(reply) = reply {
            reply.send_rep(Ok::<(), FuseError>(())).await?;
        }
        Ok(())
    }

    pub async fn complete(&self, reply: Option<FuseResponse>) -> FuseResult<()> {
        self.cache.queue_complete().await?;

        if let Some(reply) = reply {
            reply.send_rep(Ok::<(), FuseError>(())).await?;
        }
        Ok(())
    }

    pub fn status(&self) -> &FileStatus {
        &self.status
    }

    pub fn add_lock(&self, lock_flags: LockFlags, owner_id: u64) {
        let mut fh_locks = self.fh_locks.lock().unwrap();
        fh_locks.migrate_legacy_plock_owner();

        match lock_flags {
            LockFlags::Plock => {
                fh_locks.plock_owners.insert(owner_id);
            }

            LockFlags::Flock => {
                fh_locks.flock_owner_id.get_or_insert(owner_id);
            }
        }
    }

    pub fn remove_lock(&self, typ: LockFlags) -> Option<u64> {
        let mut fh_locks = self.fh_locks.lock().unwrap();
        fh_locks.migrate_legacy_plock_owner();

        match typ {
            LockFlags::Plock => {
                let owner = fh_locks.plock_owners.iter().copied().next()?;
                fh_locks.plock_owners.remove(&owner);
                Some(owner)
            }

            LockFlags::Flock => fh_locks.flock_owner_id.take(),
        }
    }

    pub fn take_plock_if_owner(&self, owner_id: u64) -> Option<u64> {
        let mut fh_locks = self.fh_locks.lock().unwrap();
        fh_locks.migrate_legacy_plock_owner();
        if fh_locks.plock_owners.remove(&owner_id) {
            Some(owner_id)
        } else {
            None
        }
    }

    pub fn drain_plock_owners(&self) -> Vec<u64> {
        let mut fh_locks = self.fh_locks.lock().unwrap();
        fh_locks.migrate_legacy_plock_owner();
        fh_locks.plock_owners.drain().collect()
    }

    pub async fn persist(&self, writer: &mut StateWriter) -> FuseResult<()> {
        writer.write_len(self.ino)?;
        writer.write_len(self.fh)?;
        writer.write_struct(&self.flags)?;
        writer.write_struct(&self.opts)?;
        writer.write_struct(&self.status)?;

        let locks = self.fh_locks.lock().unwrap();
        writer.write_struct(&*locks)?;

        Ok(())
    }

    pub async fn restore(reader: &mut StateReader, state: &NodeState) -> FuseResult<Self> {
        let ino = reader.read_len()?;
        let fh = reader.read_len()?;
        let flags: OpenFlags = reader.read_struct()?;
        let opts: CreateFileOpts = reader.read_struct()?;
        let status: FileStatus = reader.read_struct()?;
        let locks: HandleLock = reader.read_struct()?;

        let cm = state.cache_manager();
        let cache = cm
            .get_or_create(ino, flags, opts.clone(), status.len as u64)
            .await?;

        Ok(Self {
            ino,
            fh,
            cache,
            status,
            flags,
            opts,
            reader: AsyncMutex::new(None),
            fh_locks: Mutex::new(locks),
            read_ver: AtomicCounter::new(0),
        })
    }
}
