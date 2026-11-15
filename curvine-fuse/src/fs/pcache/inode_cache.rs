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

use bytes::Bytes;
use curvine_core_error::err_box;
use curvine_io::DataSlice;
use curvine_model::{CreateFileOpts, FileAllocOpts, FileStatus, OpenFlags, SetAttrOpts};
use curvine_runtime::common::TimeSpent;
use curvine_runtime::sync::{AsyncMutex, AtomicBool, AtomicCounter, ErrorMonitor, StdRwLock};
use log::warn;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::task::spawn_blocking;

use crate::fs::pcache::{CacheManager, DirtyMessage, Page};
use crate::fs::FuseWriter;
use crate::{err_fuse, FuseError, FuseMetrics, FuseResult};

pub struct PageState {
    pages: BTreeMap<u64, Page>,
}

impl Default for PageState {
    fn default() -> Self {
        Self::new()
    }
}

impl PageState {
    pub const DEFAULT_PAGE_SIZE: usize = 128 * 1024;

    pub fn new() -> Self {
        Self {
            pages: BTreeMap::new(),
        }
    }

    pub fn write(
        &mut self,
        cache: &InodeCache,
        off: u64,
        data: Bytes,
    ) -> FuseResult<(Vec<(u64, Bytes)>, bool)> {
        if data.is_empty() {
            return Ok((Vec::new(), false));
        }

        let mut src = 0usize;
        let mut unwritten = Vec::new();
        let mut notify_full_page = false;

        while src < data.len() {
            let file_off = off + src as u64;
            let page_id = cache.cm.page_id(file_off);
            let page_off = cache.cm.page_off(file_off);
            let take = (data.len() - src).min((cache.cm.page_size - page_off) as usize);

            let page = match self.pages.entry(page_id) {
                Entry::Occupied(entry) => Some(entry.into_mut()),
                Entry::Vacant(entry) => match cache.cm.allocate_page(cache.ino, page_id) {
                    Ok(page) => Some(entry.insert(page)),
                    Err(e) if e.errno == libc::ENOSPC => None,
                    Err(e) => return Err(e),
                },
            };

            match page {
                Some(page) => {
                    page.write(page_off, &data[src..src + take])?;
                    notify_full_page |= page.is_full(cache.cm.page_size);
                }
                None => unwritten.push((file_off, data.slice(src..src + take))),
            }

            src += take;
        }

        Ok((unwritten, notify_full_page))
    }

    pub fn read(
        &self,
        cache: &InodeCache,
        off: u64,
        len: usize,
    ) -> FuseResult<Option<Vec<DataSlice>>> {
        if len == 0 {
            return Ok(Some(vec![DataSlice::empty()]));
        }
        if off >= cache.len() {
            return Ok(Some(vec![DataSlice::empty()]));
        }

        let want = ((cache.len.get() - off) as usize).min(len);
        let end = off + want as u64;

        let mut out: Vec<DataSlice> = Vec::new();
        let mut cur = off;
        while cur < end {
            let page_id = cache.cm.page_id(cur);
            let page_off = cache.cm.page_off(cur);

            // Missing page: hole in this page; contiguous prefix ends here.
            let Some(page) = self.pages.get(&page_id) else {
                break;
            };

            let Some(avail) = page.readable_len(page_off) else {
                break;
            };
            let take = (avail as usize).min((end - cur) as usize);

            let bytes = page.read(page_off, take)?;
            out.push(DataSlice::bytes(bytes));

            cur += take as u64;
        }

        if out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(out))
        }
    }

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    pub fn page_len(&self) -> u64 {
        self.pages.values().map(|c| c.len()).sum()
    }
}

pub struct InodeCache {
    pub cm: Arc<CacheManager>,
    pub ino: u64,
    pub page_size: u64,
    pub state: StdRwLock<PageState>,
    pub flags: OpenFlags,
    pub create_opts: CreateFileOpts,
    pub writer: AsyncMutex<Option<Arc<FuseWriter>>>,
    pub error: ErrorMonitor<FuseError>,
    writer_ver: AtomicCounter,
    flush_gate: AsyncMutex<()>,
    all_ver: AtomicCounter,
    synced_writer_ver: AtomicCounter,
    len: AtomicCounter,
    deleted: AtomicBool,
}

impl InodeCache {
    pub fn new(
        cm: Arc<CacheManager>,
        ino: u64,
        page_size: u64,
        flags: OpenFlags,
        create_opts: CreateFileOpts,
        file_len: u64,
    ) -> Self {
        Self {
            cm,
            ino,
            page_size,
            flags,
            create_opts,
            state: StdRwLock::new(PageState::new()),
            writer: AsyncMutex::new(None),
            error: ErrorMonitor::new(),
            writer_ver: AtomicCounter::new(0),
            flush_gate: AsyncMutex::new(()),
            all_ver: AtomicCounter::new(0),
            synced_writer_ver: AtomicCounter::new(0),
            len: AtomicCounter::new(file_len),
            deleted: AtomicBool::new(false),
        }
    }

    pub fn mark_deleted(&self) {
        self.deleted.set(true)
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted.get()
    }

    pub fn set_error(&self, e: FuseError) {
        self.error.set_error(e)
    }

    pub fn check_error(self: &Arc<Self>) -> FuseResult<()> {
        if self.is_deleted() {
            return err_fuse!(
                libc::ENOENT,
                "inode {} deleted, stop write-back write",
                self.ino
            );
        }
        if !self.error.has_error() {
            return Ok(());
        }
        let error = self.error.error();
        let lock = error.lock().unwrap();
        match lock.as_ref() {
            Some(e) => Err(FuseError::new(e.errno, e.to_string().into())),
            None => err_box!("no error set"),
        }
    }

    pub async fn get_writer(&self) -> FuseResult<Arc<FuseWriter>> {
        let mut lock = self.writer.lock().await;
        match &*lock {
            Some(writer) => Ok(writer.clone()),
            None => {
                let state = self.cm.state()?;
                let path = state.get_path(self.ino)?;

                // Page-cache flushes and write-through use explicit file
                // offsets. The backend writer must therefore honor seek even
                // when the original FUSE handle was opened with O_APPEND.
                let flags = self.flags.set_append(false);
                let writer = state
                    .new_writer(&path, flags, self.create_opts.clone())
                    .await?;
                let _ = lock.insert(writer.clone());
                Ok(writer)
            }
        }
    }

    pub async fn write_through(self: &Arc<Self>, fragments: Vec<(u64, Bytes)>) -> FuseResult<()> {
        let _gate = self.flush_gate.lock().await;
        let writer = self.get_writer().await?;
        for (off, data) in fragments {
            writer.write(off as i64, data, None).await?;
        }
        Ok(())
    }

    async fn writer_inner(
        self: &Arc<Self>,
        off: u64,
        data: Bytes,
    ) -> FuseResult<(Vec<(u64, Bytes)>, bool)> {
        let cache = self.clone();
        let f = move || {
            let mut lock = cache.state.write();
            lock.write(&cache, off, data)
        };

        let res = if self.cm.store.is_mem() {
            f()
        } else {
            spawn_blocking(f).await?
        };
        if res.is_err() {
            FuseMetrics::with(|m| {
                m.write_back_error_count
                    .with_label_values(&["write_cache"])
                    .inc()
            });
        }
        res
    }

    pub async fn write(self: &Arc<Self>, off: u64, data: Bytes) -> FuseResult<usize> {
        self.check_error()?;

        let data_len = data.len();
        let spent = TimeSpent::new();

        let (unwritten, notify) = self.writer_inner(off, data).await?;
        let label = {
            if unwritten.is_empty() {
                "write_cache"
            } else {
                let miss: usize = unwritten.iter().map(|(_, data)| data.len()).sum();
                self.write_through(unwritten).await?;
                if miss == data_len {
                    "write_enospc"
                } else {
                    "write_partial"
                }
            }
        };
        self.writer_ver.incr();
        self.max_len(off + data_len as u64);

        if notify {
            self.cm.send(DirtyMessage::Write(self.ino)).await?;
        }

        FuseMetrics::with(|m| {
            m.write_back_io_bytes
                .with_label_values(&[label])
                .inc_by(data_len as i64);

            m.write_back_io_us
                .with_label_values(&[label])
                .observe(spent.used_us() as f64)
        });

        Ok(data_len)
    }

    async fn read_inner(
        self: &Arc<Self>,
        off: u64,
        len: usize,
    ) -> FuseResult<Option<Vec<DataSlice>>> {
        let cache = self.clone();
        let f = move || {
            let lock = cache.state.read();
            lock.read(&cache, off, len)
        };

        let res = if self.cm.store.is_mem() {
            f()
        } else {
            spawn_blocking(f).await?
        };

        if res.is_err() {
            FuseMetrics::with(|m| {
                m.write_back_error_count
                    .with_label_values(&["read_cache"])
                    .inc()
            });
        }
        res
    }

    pub async fn read(
        self: &Arc<Self>,
        off: u64,
        len: usize,
    ) -> FuseResult<Option<Vec<DataSlice>>> {
        self.check_error()?;
        let spent = TimeSpent::new();

        let res = self.read_inner(off, len).await;
        if res.is_ok() {
            FuseMetrics::with(|m| {
                m.write_back_io_us
                    .with_label_values(&["read_cache"])
                    .observe(spent.used_us() as f64)
            });
        }
        res
    }

    async fn read_dirty(self: &Arc<Self>, mut page: Page) -> FuseResult<Vec<(u64, Bytes)>> {
        let mut f = move || page.read_dirty();
        let res = if self.cm.store.is_mem() {
            f()
        } else {
            spawn_blocking(f).await?
        };
        if res.is_err() {
            FuseMetrics::with(|m| {
                m.write_back_error_count
                    .with_label_values(&["read_dirty"])
                    .inc()
            });
        }

        res
    }

    async fn flush_inner(
        self: &Arc<Self>,
        pages: Vec<Page>,
        sync: bool,
    ) -> FuseResult<(Arc<FuseWriter>, u64)> {
        let res = async {
            if self.is_deleted() {
                return err_fuse!(libc::ENOENT, "inode {} deleted", self.ino);
            }

            let writer = self.get_writer().await?;

            let mut total = 0;
            for page in pages {
                let dirty = self.read_dirty(page).await?;
                for (off, data) in dirty {
                    total += data.len() as u64;
                    writer.write(off as i64, data, None).await?;
                }
            }

            if sync {
                writer.flush(None).await?;
            }

            Ok((writer, total))
        }
        .await;
        // NOTE: `flushing` stays set here on purpose. It keeps `free_pages(false)`
        // from reclaiming these pages until the caller has published the data
        // (`finish_flush_ids`) or restored the ranges (`restore_flushed_dirty`),
        // which closes the window where eviction could drop a page that publish
        // still needs.
        res
    }

    /// Drain snapshot: which page ids are in flight and which ranges they hold.
    /// The taken pages own the payload; this only records what to put back.
    pub(crate) fn dirty_snapshot(pages: &[Page]) -> Vec<(u64, Vec<(u64, u64)>)> {
        pages
            .iter()
            .map(|page| (page.page_id, page.dirty_ranges().to_vec()))
            .collect()
    }

    /// Clear `flushing` once the data is durably published.
    pub(crate) fn finish_flush_ids(&self, snapshot: &[(u64, Vec<(u64, u64)>)]) {
        if snapshot.is_empty() {
            return;
        }
        let mut state = self.state.write();
        for (page_id, _) in snapshot {
            if let Some(live) = state.pages.get_mut(page_id) {
                live.finish_flush();
            }
        }
    }

    /// Put the drained ranges back after a failed flush/publish.
    ///
    /// `flushing` was never cleared, so `free_pages(false)` keeps these pages and
    /// the live entry is expected to exist. If it somehow does not (a forced
    /// free ran concurrently), the ranges cannot be re-attached: that is
    /// reported instead of silently swallowed.
    pub(crate) fn restore_flushed_dirty(&self, snapshot: &[(u64, Vec<(u64, u64)>)]) {
        if snapshot.is_empty() {
            return;
        }
        let mut state = self.state.write();
        let mut lost = 0usize;
        for (page_id, ranges) in snapshot {
            match state.pages.get_mut(page_id) {
                Some(page) => page.restore_dirty(ranges),
                None => lost += 1,
            }
        }
        drop(state);
        if lost > 0 {
            warn!(
                "inode {} lost {} page(s) during flush recovery",
                self.ino, lost
            );
            FuseMetrics::with(|m| {
                m.write_back_error_count
                    .with_label_values(&["restore_lost"])
                    .inc()
            });
        }
    }

    fn refresh_ver(&self, read_ver: u64) -> Option<u64> {
        let synced = self.synced_writer_ver.get();
        (read_ver < synced).then_some(synced)
    }

    async fn publish_writer(&self, writer: &Arc<FuseWriter>) -> FuseResult<()> {
        if let Some(target_ver) = writer
            .publish_dirty_read_snapshot(self.synced_writer_ver.get())
            .await?
        {
            self.synced_writer_ver.set(target_ver);
        }
        Ok(())
    }

    pub async fn flush_dirty(
        self: &Arc<InodeCache>,
        sync: bool,
    ) -> FuseResult<(Arc<FuseWriter>, u64)> {
        let _gate = self.flush_gate.lock().await;
        let pages = self.take_dirty_pages()?;
        let snapshot = Self::dirty_snapshot(&pages);
        let res = async {
            let (writer, total) = self.flush_inner(pages, sync).await?;
            if sync {
                self.publish_writer(&writer).await?;
            }
            Ok((writer, total))
        }
        .await;
        match res {
            Ok(v) => {
                self.finish_flush_ids(&snapshot);
                Ok(v)
            }
            Err(e) => {
                self.restore_flushed_dirty(&snapshot);
                Err(e)
            }
        }
    }

    pub async fn publish_read_range(
        self: &Arc<InodeCache>,
        off: u64,
        len: usize,
        read_ver: u64,
    ) -> FuseResult<Option<u64>> {
        self.check_error()?;

        let target = self.writer_ver.get();
        if target <= self.all_ver.get() {
            return Ok(self.refresh_ver(read_ver));
        }

        let spent = TimeSpent::new();
        let _gate = self.flush_gate.lock().await;
        if target <= self.all_ver.get() {
            return Ok(self.refresh_ver(read_ver));
        }

        let pages = self.take_dirty_pages_range(off, len)?;
        if pages.is_empty() {
            let writer = self.writer.lock().await.clone();
            if let Some(writer) = writer {
                self.publish_writer(&writer).await?;
            }
            return Ok(self.refresh_ver(read_ver));
        }

        // publish_writer flushes everything enqueued below; a direct
        // writer.flush here would flush the backend twice.
        let snapshot = Self::dirty_snapshot(&pages);
        let res = async {
            let (writer, len) = self.flush_inner(pages, false).await?;
            self.publish_writer(&writer).await?;
            Ok((writer, len))
        }
        .await;
        let len = match res {
            Ok((_writer, len)) => {
                self.finish_flush_ids(&snapshot);
                len
            }
            Err(e) => {
                self.restore_flushed_dirty(&snapshot);
                return Err(e);
            }
        };

        FuseMetrics::with(|m| {
            m.write_back_io_us
                .with_label_values(&["flush_range"])
                .observe(spent.used_us() as f64);

            m.write_back_io_bytes
                .with_label_values(&["flush_range"])
                .inc_by(len as i64);
        });

        Ok(self.refresh_ver(read_ver))
    }

    pub fn len(&self) -> u64 {
        self.len.get()
    }

    pub fn page_len(&self) -> u64 {
        self.state.read().page_len()
    }

    pub fn reached_eof(&self, off: u64, got: usize) -> bool {
        off + got as u64 >= self.len.get()
    }

    pub fn is_empty(&self) -> bool {
        self.len.get() == 0
    }

    fn max_len(&self, new: u64) {
        loop {
            let cur = self.len.get();
            if cur >= new {
                return;
            }
            if self.len.compare_and_set(cur, new) {
                return;
            }
        }
    }

    pub async fn resize(self: &Arc<Self>, opts: FileAllocOpts) -> FuseResult<FileStatus> {
        self.check_error()?;

        let (writer, _) = self.flush_all().await?;
        let status = writer.resize(opts).await?;

        self.writer_ver.incr();
        self.len.set(status.len as u64);

        Ok(status)
    }

    pub fn queue_flush(self: &Arc<Self>) -> FuseResult<()> {
        self.check_error()?;

        self.cm
            .state()?
            .update_dirty_len(self.ino, self.len.get())?;

        Ok(())
    }

    async fn flush_all(self: &Arc<Self>) -> FuseResult<(Arc<FuseWriter>, u64)> {
        let _gate = self.flush_gate.lock().await;
        let target = self.writer_ver.get();

        let writer = if target > self.all_ver.get() {
            let pages = self.take_all_dirty_pages()?;
            let snapshot = Self::dirty_snapshot(&pages);
            // publish_writer flushes everything enqueued below; a direct
            // writer.flush here would flush the backend twice.
            let res = async {
                let (writer, _) = self.flush_inner(pages, false).await?;
                self.publish_writer(&writer).await?;
                Ok(writer)
            }
            .await;
            let writer = match res {
                Ok(writer) => {
                    self.finish_flush_ids(&snapshot);
                    writer
                }
                Err(e) => {
                    self.restore_flushed_dirty(&snapshot);
                    return Err(e);
                }
            };
            self.all_ver.set(target);
            writer
        } else {
            self.get_writer().await?
        };

        Ok((writer, self.all_ver.get()))
    }

    pub async fn flush(self: &Arc<Self>) -> FuseResult<()> {
        self.check_error()?;
        if self.writer_ver.get() <= self.all_ver.get() && self.writer.lock().await.is_none() {
            // Nothing was ever written or published: flush is a no-op; creating
            // a writer would fail on unbound caches (a642 regression).
            return Ok(());
        }
        self.flush_all().await?;
        Ok(())
    }

    pub async fn ensure_flushed(self: &Arc<Self>) -> FuseResult<u64> {
        self.check_error()?;
        if self.writer_ver.get() <= self.all_ver.get() && self.writer.lock().await.is_none() {
            return Ok(self.all_ver.get());
        }
        self.flush_all().await.map(|(_, len)| len)
    }

    pub async fn queue_complete(self: &Arc<Self>) -> FuseResult<()> {
        self.check_error()?;
        self.cm.send(DirtyMessage::new_complete(self.ino)).await?;
        Ok(())
    }

    pub async fn complete(self: &Arc<Self>, opts: Option<SetAttrOpts>) -> FuseResult<()> {
        let fut = async {
            // A deleted inode has no backend to complete against: pages are
            // reclaimed by `free_pages` below regardless, so completing must
            // not try to flush/create a writer (a642df02c regressed this by
            // routing complete through flush_all's get_writer else-branch).
            if self.is_deleted() {
                return Ok(());
            }
            let (writer, _) = self.flush_all().await?;
            writer.complete_with_attr(None, opts).await?;
            Ok(())
        };
        let res = fut.await;

        // Only a successful complete may drop the pages: on failure the dirty
        // data was restored for a retry, and `free_pages(true)` would clear it
        // outright (the caller never asked to delete this inode).
        if res.is_ok() {
            let _ = self.free_pages(true);
        }
        res
    }

    pub fn write_ver(&self) -> u64 {
        self.writer_ver.get()
    }

    pub async fn truncate(self: &Arc<Self>) -> FuseResult<()> {
        let _ = self.free_pages(true);
        self.len.set(0);
        self.writer_ver.incr();

        Ok(())
    }

    pub fn free_pages(self: &Arc<Self>, force: bool) -> u64 {
        let mut state = self.state.write();
        let mut freed = 0;

        state.pages.retain(|_, page| {
            if !force && (page.is_dirty() || page.flushing) {
                return true;
            }

            page.clear();
            self.cm.release_usage(self.page_size);
            freed += self.page_size;

            false
        });

        if freed > 0 {
            self.cm.refresh_full();
        }

        freed
    }

    pub fn take_dirty_pages(self: &Arc<Self>) -> FuseResult<Vec<Page>> {
        self.take_dirty_pages_inner(true)
    }

    pub(crate) fn take_all_dirty_pages(self: &Arc<Self>) -> FuseResult<Vec<Page>> {
        self.take_dirty_pages_inner(false)
    }

    fn take_dirty_pages_inner(self: &Arc<Self>, full_only: bool) -> FuseResult<Vec<Page>> {
        let mut state = self.state.write();
        let mut result = Vec::new();

        for page in state.pages.values_mut() {
            let taken = if full_only {
                page.take_dirty(self.page_size)
            } else {
                page.take_all_dirty(self.page_size)
            };
            match taken {
                Ok(Some(dirty)) => result.push(dirty),
                Ok(None) => {}
                Err(e) => {
                    // Pages taken before this one already lost their ranges and
                    // stay `flushing`; dropping `result` here would silently
                    // discard them. Put them back before propagating.
                    let snapshot = Self::dirty_snapshot(&result);
                    drop(state);
                    self.restore_flushed_dirty(&snapshot);
                    return Err(e);
                }
            }
        }

        Ok(result)
    }

    fn take_dirty_pages_range(self: &Arc<Self>, off: u64, len: usize) -> FuseResult<Vec<Page>> {
        if len == 0 {
            return Ok(Vec::new());
        }

        let mut state = self.state.write();
        let end = off.saturating_add(len as u64).saturating_sub(1);
        let last = self.cm.page_id(end);
        let first = self.cm.page_id(off);
        let mut result = Vec::new();
        for (_, page) in state.pages.range_mut(first..=last) {
            match page.take_all_dirty(self.page_size) {
                Ok(Some(taken)) => result.push(taken),
                Ok(None) => {}
                Err(e) => {
                    let snapshot = Self::dirty_snapshot(&result);
                    drop(state);
                    self.restore_flushed_dirty(&snapshot);
                    return Err(e);
                }
            }
        }

        Ok(result)
    }
}

impl Drop for InodeCache {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.try_write() {
            for page in state.pages.values_mut() {
                page.clear();
                self.cm.release_usage(self.page_size);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::err_fuse;
    use curvine_config::FuseConf;
    use curvine_model::{CreateFileOpts, OpenFlags};
    use curvine_runtime::common::ByteUnit;
    use curvine_runtime::sync::channel::AsyncChannel;

    const TEST_INO: u64 = 100;

    fn test_conf(page_size: u64) -> FuseConf {
        FuseConf {
            write_back_cache_type: "memory".to_string(),
            write_back_page_size: ByteUnit::new(page_size),
            write_back_cache_size: ByteUnit::new(page_size * 16),
            ..Default::default()
        }
    }

    fn test_cache(conf: &FuseConf) -> Arc<InodeCache> {
        let (sender, receiver) = AsyncChannel::new(0).split();
        Box::leak(Box::new(receiver));
        let cm = Arc::new(CacheManager::new(conf, sender).unwrap());
        Arc::new(InodeCache::new(
            cm.clone(),
            TEST_INO,
            cm.page_size,
            OpenFlags::RDWR,
            CreateFileOpts::with_create(false),
            0,
        ))
    }

    fn bytes(data: &[u8]) -> Bytes {
        Bytes::copy_from_slice(data)
    }

    async fn read_all(cache: &Arc<InodeCache>, off: u64, len: usize) -> Vec<u8> {
        let slices = cache.read(off, len).await.unwrap().unwrap();
        let mut out = Vec::new();
        for s in slices {
            out.extend_from_slice(s.as_slice());
        }
        out
    }

    #[tokio::test]
    async fn write_read_roundtrip() {
        let cache = test_cache(&test_conf(4096));
        let data = b"hello world".to_vec();
        assert_eq!(cache.write(0, bytes(&data)).await.unwrap(), data.len());
        assert_eq!(cache.len(), data.len() as u64);
        assert_eq!(read_all(&cache, 0, data.len()).await, data);
    }

    #[tokio::test]
    async fn write_empty_is_noop() {
        let cache = test_cache(&test_conf(4096));
        assert_eq!(cache.write(0, Bytes::new()).await.unwrap(), 0);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.page_len(), 0);
    }

    #[tokio::test]
    async fn write_read_across_pages() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(&vec![1u8; 4096])).await.unwrap();
        cache.write(4096, bytes(&vec![2u8; 4096])).await.unwrap();
        assert_eq!(cache.len(), 8192);
        assert_eq!(cache.page_len(), 8192);

        let got = read_all(&cache, 4000, 200).await;
        assert_eq!(&got[..96], &[1u8; 96]);
        assert_eq!(&got[96..], &[2u8; 104]);
    }

    #[tokio::test]
    async fn write_read_partial_page_offset() {
        let cache = test_cache(&test_conf(4096));
        cache.write(100, bytes(b"abc")).await.unwrap();
        assert_eq!(cache.len(), 103);
        assert_eq!(read_all(&cache, 100, 3).await, b"abc");
        assert_eq!(cache.page_len(), 3);
    }

    #[tokio::test]
    async fn len_tracks_max_offset() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(b"1234567890")).await.unwrap();
        assert_eq!(cache.len(), 10);
        cache.write(100, bytes(b"abc")).await.unwrap();
        assert_eq!(cache.len(), 103);
    }

    #[tokio::test]
    async fn read_beyond_len_returns_empty() {
        let cache = test_cache(&test_conf(4096));
        let slices = cache.read(0, 10).await.unwrap().unwrap();
        assert_eq!(slices.len(), 1);
        assert!(slices[0].is_empty());
    }

    #[tokio::test]
    async fn read_missing_page_returns_none() {
        let cache = test_cache(&test_conf(4096));
        cache.write(5000, bytes(b"abc")).await.unwrap();
        assert!(cache.read(0, 100).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn truncate_clears_all() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(b"hello")).await.unwrap();
        cache.write(5000, bytes(b"tail")).await.unwrap();
        cache.truncate().await.unwrap();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.page_len(), 0);
        assert!(cache.is_empty());
        assert_eq!(cache.cm.used(), 0);
    }

    #[tokio::test]
    async fn free_pages_keeps_dirty_and_partial() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(&vec![1u8; 4096])).await.unwrap();
        cache.write(5000, bytes(b"tail")).await.unwrap();

        assert_eq!(cache.free_pages(false), 0);
        assert_eq!(cache.page_len(), 4100);

        assert_eq!(cache.take_dirty_pages().unwrap().len(), 1);
        cache.finish_flush_ids(&[(0, vec![(0, 4096)])]);
        assert_eq!(cache.free_pages(false), 4096);
        assert_eq!(cache.page_len(), 4);
        assert_eq!(cache.cm.used(), 4096);
    }

    #[tokio::test]
    async fn free_pages_force_clears_all() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(b"hello")).await.unwrap();
        cache.write(5000, bytes(b"tail")).await.unwrap();
        assert_eq!(cache.free_pages(true), 8192);
        assert_eq!(cache.page_len(), 0);
        assert_eq!(cache.cm.used(), 0);
    }

    #[tokio::test]
    async fn take_dirty_pages_copies_data() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(&vec![1u8; 4096])).await.unwrap();
        let pages = cache.take_dirty_pages().unwrap();
        assert_eq!(pages.len(), 1);
        let page = pages.into_iter().next().unwrap();
        assert_eq!(page.page_id, 0);
        assert_eq!(page.start(), 0);
        assert_eq!(&page.read(0, 4).unwrap()[..], &[1u8; 4]);
    }

    #[tokio::test]
    async fn take_dirty_pages_range_handles_sparse_saturated_range() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(b"head")).await.unwrap();

        let pages = cache
            .take_dirty_pages_range(0, usize::MAX)
            .expect("saturated range should not overflow");
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].page_id, 0);
    }

    #[test]
    fn refresh_ver_only_refreshes_older_readers() {
        let cache = test_cache(&test_conf(4096));
        cache.synced_writer_ver.set(7);

        assert_eq!(cache.refresh_ver(6), Some(7));
        assert_eq!(cache.refresh_ver(7), None);
        assert_eq!(cache.refresh_ver(8), None);
    }

    #[tokio::test]
    async fn publish_read_range_fast_path_when_all_flushed() {
        let cache = test_cache(&test_conf(4096));

        cache.writer_ver.set(5);
        cache.all_ver.set(5);
        cache.synced_writer_ver.set(5);

        assert_eq!(cache.publish_read_range(0, 4096, 5).await.unwrap(), None);
        assert_eq!(cache.publish_read_range(0, 4096, 3).await.unwrap(), Some(5));
        assert_eq!(cache.publish_read_range(0, 4096, 9).await.unwrap(), None);
    }

    fn injected_error() -> FuseError {
        (err_fuse!(libc::EIO, "injected") as FuseResult<u32>).unwrap_err()
    }

    #[tokio::test]
    async fn error_blocks_io() {
        let cache = test_cache(&test_conf(4096));
        cache.set_error(injected_error());
        assert!(cache.check_error().is_err());

        let cache = test_cache(&test_conf(4096));
        cache.set_error(injected_error());
        assert!(cache.write(0, bytes(b"x")).await.is_err());

        let cache = test_cache(&test_conf(4096));
        cache.set_error(injected_error());
        assert!(cache.read(0, 1).await.is_err());
    }

    #[tokio::test]
    async fn write_ver_increments() {
        let cache = test_cache(&test_conf(4096));
        assert_eq!(cache.write_ver(), 0);
        cache.write(0, bytes(b"a")).await.unwrap();
        assert_eq!(cache.write_ver(), 1);
    }

    #[tokio::test]
    async fn ensure_flushed_skips_when_nothing_written() {
        let cache = test_cache(&test_conf(4096));
        assert_eq!(cache.ensure_flushed().await.unwrap(), 0);
        cache.flush().await.unwrap();
        assert_eq!(cache.ensure_flushed().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn usage_reserves_full_page() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(b"hello")).await.unwrap();
        assert_eq!(cache.cm.used(), 4096);
        assert_eq!(cache.page_len(), 5);
    }

    #[tokio::test]
    async fn allocate_enospc_when_capacity_below_page_size() {
        let conf = FuseConf {
            write_back_cache_type: "memory".to_string(),
            write_back_page_size: ByteUnit::new(4096),
            write_back_cache_size: ByteUnit::new(16),
            ..Default::default()
        };
        let cache = test_cache(&conf);
        let (unwritten, notify) = cache
            .state
            .write()
            .write(&cache, 0, bytes(b"hello"))
            .unwrap();
        assert_eq!(unwritten.len(), 1);
        assert_eq!(unwritten[0].0, 0);
        assert_eq!(&unwritten[0].1[..], b"hello");
        assert!(!notify);
        assert_eq!(cache.cm.used(), 0);
    }

    #[tokio::test]
    async fn read_random_offset_across_pages() {
        let cache = test_cache(&test_conf(4096));
        let data: Vec<u8> = (0..12288u64).map(|i| (i % 251) as u8).collect();
        cache.write(0, bytes(&data)).await.unwrap();

        let got = read_all(&cache, 3000, 5000).await;
        assert_eq!(got.len(), 5000);
        assert_eq!(got, data[3000..8000]);
    }

    #[tokio::test]
    async fn read_exact_page_boundary() {
        let cache = test_cache(&test_conf(4096));
        let data = vec![0x5Au8; 8192];
        cache.write(0, bytes(&data)).await.unwrap();
        let got = read_all(&cache, 4096, 4096).await;
        assert_eq!(got, data[4096..]);
    }

    #[tokio::test]
    async fn write_crosses_three_pages() {
        let cache = test_cache(&test_conf(4096));
        let data: Vec<u8> = (0..5000u64).map(|i| (i % 251) as u8).collect();
        assert_eq!(cache.write(3000, bytes(&data)).await.unwrap(), 5000);
        assert_eq!(cache.len(), 8000);
        assert_eq!(cache.page_len(), 5000);
        let got = read_all(&cache, 3000, 5000).await;
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn read_zero_len_returns_empty() {
        let cache = test_cache(&test_conf(4096));
        let slices = cache.read(0, 0).await.unwrap().unwrap();
        assert_eq!(slices.len(), 1);
        assert!(slices[0].is_empty());
    }

    #[tokio::test]
    async fn read_at_file_len_returns_empty() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(b"hello")).await.unwrap();
        let slices = cache.read(5, 10).await.unwrap().unwrap();
        assert_eq!(slices.len(), 1);
        assert!(slices[0].is_empty());
    }

    #[tokio::test]
    async fn sparse_write_read_prefix_then_stops_at_hole() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(b"hello")).await.unwrap();
        cache.write(5000, bytes(b"tail")).await.unwrap();
        let got = read_all(&cache, 0, 4096).await;
        assert_eq!(got, b"hello");
    }

    #[tokio::test]
    async fn read_in_page_prefix_hole_returns_none() {
        let cache = test_cache(&test_conf(4096));
        cache.write(5000, bytes(b"tail")).await.unwrap();
        assert!(cache.read(4096, 100).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn mark_deleted_toggles_flag() {
        let cache = test_cache(&test_conf(4096));
        assert!(!cache.is_deleted());
        cache.mark_deleted();
        assert!(cache.is_deleted());
    }

    #[tokio::test]
    async fn write_after_delete_returns_enoent() {
        let cache = test_cache(&test_conf(4096));
        cache.mark_deleted();
        let err = cache.write(0, bytes(b"x")).await.unwrap_err();
        assert_eq!(err.errno, libc::ENOENT);
    }

    #[tokio::test]
    async fn complete_after_delete_frees_pages_without_backend() {
        let cache = test_cache(&test_conf(4096));
        cache.write(0, bytes(&vec![1u8; 4096])).await.unwrap();
        assert_eq!(cache.page_len(), 4096);

        cache.mark_deleted();
        // No bound state: complete must short-circuit before get_writer/flush,
        // otherwise it would error trying to reach the master.
        cache.complete(None).await.unwrap();
        assert_eq!(cache.page_len(), 0);
        assert_eq!(cache.cm.used(), 0);
    }
}
