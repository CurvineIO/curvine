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

use crate::fs::pcache::{
    DirtyMessage, DiskPageStore, EvictInode, InodeCache, MemoryPageStore, PageStore,
    PageStoreVariant,
};
use crate::fs::state::NodeState;
use crate::{FuseError, FuseResult};
use curvine_config::FuseConf;
use curvine_core_error::err_box;
use curvine_model::{CreateFileOpts, OpenFlags};
use curvine_runtime::sync::channel::{AsyncSender, CallChannel};
use curvine_runtime::sync::AsyncSharedMap;
use std::sync::{Arc, OnceLock, Weak};

pub struct CacheManager {
    caches: AsyncSharedMap<u64, InodeCache>,
    pub(crate) store: Arc<PageStoreVariant>,
    pub(crate) page_size: u64,
    pub(crate) state: OnceLock<Weak<NodeState>>,
    pub(crate) sender: AsyncSender<DirtyMessage>,
    pub(crate) free_ratio: f64,
}

impl CacheManager {
    pub fn new(conf: &FuseConf, sender: AsyncSender<DirtyMessage>) -> FuseResult<Self> {
        Self::check_conf(conf)?;

        let store = match conf.write_back_cache_type.as_str() {
            "disk" => PageStoreVariant::Disk(DiskPageStore::from_conf(conf)?),
            _ => PageStoreVariant::Memory(MemoryPageStore::from_conf(conf)),
        };

        let page_size = store.page_size();
        let cm = CacheManager {
            caches: AsyncSharedMap::default(),
            store: Arc::new(store),
            page_size,
            state: OnceLock::new(),
            sender,
            free_ratio: conf.write_back_free_ratio,
        };
        Ok(cm)
    }

    fn check_conf(conf: &FuseConf) -> FuseResult<()> {
        if !conf.enable_write_back {
            return Ok(());
        }
        let flush_delay = conf.write_back_flush_delay.as_millis() as u128;
        let meta_cache_ttl = conf.meta_cache_ttl.as_millis();
        if flush_delay > meta_cache_ttl / 2 {
            return err_box!(
                "write_back_flush_delay ({}ms) must be less than half of meta_cache_ttl ({}ms)",
                flush_delay,
                meta_cache_ttl
            );
        }
        let node_cache_ttl = conf.node_cache_ttl.as_millis();
        if flush_delay > node_cache_ttl / 2 {
            return err_box!(
                "write_back_flush_delay ({}ms) must be less than half of node_cache_ttl ({}ms)",
                flush_delay,
                node_cache_ttl
            );
        }
        if conf.write_back_free_ratio >= 1.0 {
            return err_box!(
                "write_back_free_ratio ({}) must be less than 1.0",
                conf.write_back_free_ratio
            );
        }
        if !conf.enable_meta_cache {
            return err_box!("write_back cache requires enable_meta_cache to be enabled");
        }
        Ok(())
    }

    #[inline]
    pub fn page_id(&self, off: u64) -> u64 {
        (off / self.page_size as u64) * self.page_size as u64
    }

    #[inline]
    pub fn page_off(&self, off: u64) -> u64 {
        off % self.page_size as u64
    }

    pub fn bind_state(&self, state: &Arc<NodeState>) {
        let _ = self.state.set(Arc::downgrade(state));
    }

    pub(crate) fn state(&self) -> FuseResult<Arc<NodeState>> {
        let Some(weak) = self.state.get() else {
            return err_box!("cache manager state not bound");
        };
        let Some(state) = weak.upgrade() else {
            return err_box!("cache manager state not bound");
        };
        Ok(state)
    }

    pub fn len(&self) -> usize {
        self.caches.len()
    }

    pub fn is_empty(&self) -> bool {
        self.caches.is_empty()
    }

    pub fn capacity(&self) -> u64 {
        self.store.capacity()
    }

    pub fn used(&self) -> u64 {
        self.store.used()
    }

    pub fn free_ratio(&self) -> f64 {
        self.store.free_ratio()
    }

    pub fn need_evict(&self) -> bool {
        self.store.free_ratio() < self.free_ratio
    }

    pub fn free_target(&self) -> u64 {
        (self.store.capacity() as f64 * self.free_ratio) as u64
    }

    pub fn add_usage(&self, len: u64) {
        self.store.add_usage(len);
    }

    pub fn release_usage(&self, len: u64) {
        self.store.release_usage(len);
    }

    pub fn is_full(&self) -> bool {
        self.store.free_ratio() < self.free_ratio / 2f64
    }

    pub async fn get_or_create(
        self: &Arc<Self>,
        ino: u64,
        flags: OpenFlags,
        create_opts: CreateFileOpts,
        file_len: u64,
    ) -> FuseResult<Arc<InodeCache>> {
        let cache = self
            .caches
            .get_or_create(ino, async {
                let cache = Arc::new(InodeCache::new(
                    self.clone(),
                    ino,
                    self.page_size,
                    flags,
                    create_opts.clone(),
                    file_len,
                ));
                Ok::<_, FuseError>(cache)
            })
            .await?;

        if flags.truncate() {
            cache.truncate().await?;
        }

        Ok(cache)
    }

    pub async fn remove_cache(&self, ino: u64, force: bool) -> FuseResult<()> {
        let link = if force {
            0
        } else {
            let state = self.state()?;
            let link = state
                .dir_read()
                .get_inode(ino, None)
                .map(|x| x.nlink)
                .unwrap_or(0);
            link
        };

        if link == 0 {
            if let Some(cache) = self.caches.get(&ino).await {
                cache.free_pages(true).await;
            }
            self.caches.remove(ino);
        }
        Ok(())
    }

    pub async fn ensure_open(&self, ino: u64) -> FuseResult<()> {
        self.caches
            .with_resource(&ino, |cache| async move {
                let _ = cache.get_writer().await?;
                Ok::<_, FuseError>(())
            })
            .await?;
        Ok(())
    }

    pub async fn ensure_flush(&self, ino: u64) -> FuseResult<()> {
        self.caches
            .with_resource(&ino, |cache| async move { cache.flush().await })
            .await?;
        Ok(())
    }

    pub async fn get_cache(&self, ino: u64) -> Option<Arc<InodeCache>> {
        self.caches.get(&ino).await
    }

    pub async fn get_cache_opt_check(&self, ino: u64) -> FuseResult<Option<Arc<InodeCache>>> {
        let Some(cache) = self.get_cache(ino).await else {
            return Ok(None);
        };
        cache.check_error()?;
        Ok(Some(cache))
    }

    pub async fn get_cache_check(&self, ino: u64) -> FuseResult<Arc<InodeCache>> {
        match self.get_cache_opt_check(ino).await? {
            Some(cache) => Ok(cache),
            None => {
                let path = self.state()?.get_path(ino);
                err_box!("inode {} {:?} cache not found", ino, path)
            }
        }
    }

    pub async fn release_cache(&self, cache: &Arc<InodeCache>) -> FuseResult<()> {
        let ino = cache.ino;
        let state = self.state()?;

        let opts = {
            state
                .dir_read()
                .get_inode(ino, None)
                .map(|x| x.to_set_opts())
        };

        let (released, res) = self
            .caches
            .release_with_cleanup(ino, move |last| async move {
                if last {
                    cache.complete(opts).await
                } else {
                    cache.flush().await
                }
            })
            .await;

        if released {
            res?;
            state.mark_dirty_commit(ino);
        }

        Ok(())
    }

    pub async fn send(&self, msg: DirtyMessage) -> FuseResult<()> {
        self.sender.send(msg).await?;
        Ok(())
    }

    pub async fn commit_all(&self, stop: bool) -> FuseResult<()> {
        let (tx, rx) = CallChannel::channel();
        self.sender.send(DirtyMessage::CommitAll(stop, tx)).await?;
        rx.receive().await?
    }

    pub fn all_inos(&self) -> Vec<u64> {
        self.caches.keys()
    }

    pub async fn evict_candidates(&self) -> Vec<EvictInode> {
        let inos = self.all_inos();
        let mut res = Vec::with_capacity(inos.len());

        for ino in inos {
            let Some(cache) = self.get_cache(ino).await else {
                continue;
            };

            let file_len = cache.len();
            let page_len = cache.page_len().await;
            res.push(EvictInode::new(ino, page_len, file_len));
        }

        res.sort_by_key(|b| std::cmp::Reverse(b.page_len));
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use curvine_config::FuseConf;
    use curvine_model::{CreateFileOpts, OpenFlags};
    use curvine_runtime::common::ByteUnit;
    use curvine_runtime::sync::channel::AsyncChannel;

    const PAGE_SIZE: u64 = 4096;

    fn test_conf(page_size: u64, capacity: u64, free_ratio: f64) -> FuseConf {
        FuseConf {
            write_back_cache_type: "memory".to_string(),
            write_back_page_size: ByteUnit::new(page_size),
            write_back_cache_size: ByteUnit::new(capacity),
            write_back_free_ratio: free_ratio,
            ..Default::default()
        }
    }

    fn test_cm(conf: &FuseConf) -> Arc<CacheManager> {
        let (sender, mut receiver) = AsyncChannel::new(0).split();
        std::mem::drop(tokio::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                if let DirtyMessage::CommitAll(stop, tx) = msg {
                    let _ = tx.send(Ok(()));
                    if stop {
                        break;
                    }
                }
            }
        }));
        Arc::new(CacheManager::new(conf, sender).unwrap())
    }

    async fn create_cache(cm: &Arc<CacheManager>, ino: u64) -> Arc<InodeCache> {
        cm.get_or_create(ino, OpenFlags::RDWR, CreateFileOpts::with_create(false), 0)
            .await
            .unwrap()
    }

    async fn write_bytes(cache: &Arc<InodeCache>, data: &[u8]) {
        cache.write(0, Bytes::copy_from_slice(data)).await.unwrap();
    }

    #[tokio::test]
    async fn page_id_aligns_to_page_size() {
        let cm = test_cm(&test_conf(PAGE_SIZE, PAGE_SIZE * 16, 0.1));
        assert_eq!(cm.page_size, PAGE_SIZE);
        assert_eq!(cm.page_id(0), 0);
        assert_eq!(cm.page_id(1), 0);
        assert_eq!(cm.page_id(PAGE_SIZE - 1), 0);
        assert_eq!(cm.page_id(PAGE_SIZE), PAGE_SIZE);
        assert_eq!(cm.page_id(PAGE_SIZE + 904), PAGE_SIZE);
        assert_eq!(cm.page_id(PAGE_SIZE * 2), PAGE_SIZE * 2);
    }

    #[tokio::test]
    async fn page_off_returns_in_page_offset() {
        let cm = test_cm(&test_conf(PAGE_SIZE, PAGE_SIZE * 16, 0.1));
        assert_eq!(cm.page_off(0), 0);
        assert_eq!(cm.page_off(PAGE_SIZE - 1), PAGE_SIZE - 1);
        assert_eq!(cm.page_off(PAGE_SIZE), 0);
        assert_eq!(cm.page_off(PAGE_SIZE + 904), 904);
    }

    #[tokio::test]
    async fn capacity_and_used_delegate_to_store() {
        let cm = test_cm(&test_conf(PAGE_SIZE, PAGE_SIZE * 16, 0.1));
        assert_eq!(cm.capacity(), PAGE_SIZE * 16);
        assert_eq!(cm.used(), 0);
        cm.store.add_usage(100);
        cm.store.add_usage(50);
        assert_eq!(cm.used(), 150);
    }

    #[tokio::test]
    async fn free_ratio_reflects_usage() {
        let cm = test_cm(&test_conf(PAGE_SIZE, 1000, 0.1));
        assert_eq!(cm.free_ratio(), 1.0);
        cm.store.add_usage(250);
        assert!((cm.free_ratio() - 0.75).abs() < f64::EPSILON);
        cm.store.add_usage(750);
        assert_eq!(cm.free_ratio(), 0.0);
    }

    #[tokio::test]
    async fn need_evict_triggers_below_free_ratio() {
        let cm = test_cm(&test_conf(PAGE_SIZE, 1000, 0.1));
        assert!(!cm.need_evict());
        cm.store.add_usage(899);
        assert!(!cm.need_evict());
        cm.store.add_usage(2);
        assert!(cm.need_evict());
    }

    #[tokio::test]
    async fn is_full_when_free_ratio_below_half_threshold() {
        let cm = test_cm(&test_conf(PAGE_SIZE, 1000, 0.1));
        assert!(!cm.is_full());
        cm.store.add_usage(940);
        assert!(!cm.is_full());
        cm.store.add_usage(20);
        assert!(cm.is_full());
    }

    #[tokio::test]
    async fn free_target_is_free_ratio_of_capacity() {
        let cm = test_cm(&test_conf(PAGE_SIZE, 1000, 0.1));
        assert_eq!(cm.free_target(), 100);
    }

    #[tokio::test]
    async fn get_or_create_returns_same_cache_for_same_ino() {
        let cm = test_cm(&test_conf(PAGE_SIZE, PAGE_SIZE * 16, 0.1));
        let a = create_cache(&cm, 1).await;
        let b = create_cache(&cm, 1).await;
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(cm.all_inos(), vec![1]);
    }

    #[tokio::test]
    async fn get_cache_missing_returns_none() {
        let cm = test_cm(&test_conf(PAGE_SIZE, PAGE_SIZE * 16, 0.1));
        assert!(cm.get_cache(1).await.is_none());
        create_cache(&cm, 1).await;
        assert!(cm.get_cache(1).await.is_some());
    }

    #[tokio::test]
    async fn get_cache_opt_check_surfaces_cached_error() {
        let cm = test_cm(&test_conf(PAGE_SIZE, PAGE_SIZE * 16, 0.1));
        let cache = create_cache(&cm, 1).await;
        assert!(cm.get_cache_opt_check(1).await.unwrap().is_some());
        cache.set_error(FuseError::from("injected"));
        assert!(cm.get_cache_opt_check(1).await.is_err());
    }

    #[tokio::test]
    async fn get_cache_check_missing_errors_without_state() {
        let cm = test_cm(&test_conf(PAGE_SIZE, PAGE_SIZE * 16, 0.1));
        assert!(cm.get_cache_check(1).await.is_err());
    }

    #[tokio::test]
    async fn get_or_create_with_truncate_flag_clears_len() {
        let cm = test_cm(&test_conf(PAGE_SIZE, PAGE_SIZE * 16, 0.1));
        let cache = create_cache(&cm, 1).await;
        write_bytes(&cache, b"hello").await;
        assert_eq!(cache.len(), 5);

        cm.get_or_create(
            1,
            OpenFlags::RDWR | OpenFlags::TRUNC,
            CreateFileOpts::with_create(false),
            0,
        )
        .await
        .unwrap();
        assert_eq!(cache.len(), 0);
    }

    #[tokio::test]
    async fn send_forwards_message_to_receiver() {
        let (sender, mut receiver) = AsyncChannel::new(0).split();
        let cm = Arc::new(
            CacheManager::new(&test_conf(PAGE_SIZE, PAGE_SIZE * 16, 0.1), sender).unwrap(),
        );
        cm.send(DirtyMessage::new_write(42)).await.unwrap();
        let msg = receiver.recv().await.unwrap();
        assert!(matches!(msg, DirtyMessage::Write(42)));
    }

    #[tokio::test]
    async fn commit_all_roundtrips_through_channel() {
        let cm = test_cm(&test_conf(PAGE_SIZE, PAGE_SIZE * 16, 0.1));
        cm.commit_all(false).await.unwrap();
    }

    #[tokio::test]
    async fn evict_candidates_sorted_by_len_desc() {
        let cm = test_cm(&test_conf(PAGE_SIZE, PAGE_SIZE * 64, 0.1));
        let c1 = create_cache(&cm, 1).await;
        write_bytes(&c1, &[0u8; 100]).await;
        let c2 = create_cache(&cm, 2).await;
        write_bytes(&c2, &vec![0u8; 300]).await;
        let c3 = create_cache(&cm, 3).await;
        write_bytes(&c3, &[0u8; 200]).await;

        let candidates = cm.evict_candidates().await;
        let lens: Vec<u64> = candidates.iter().map(|e| e.page_len).collect();
        assert_eq!(lens, vec![300, 200, 100]);
    }

    #[tokio::test]
    async fn ensure_open_errors_without_bound_state() {
        let cm = test_cm(&test_conf(PAGE_SIZE, PAGE_SIZE * 16, 0.1));
        create_cache(&cm, 1).await;
        assert!(cm.ensure_open(1).await.is_err());
    }

    #[tokio::test]
    async fn ensure_flush_errors_without_bound_state() {
        let cm = test_cm(&test_conf(PAGE_SIZE, PAGE_SIZE * 16, 0.1));
        create_cache(&cm, 1).await;
        assert!(cm.ensure_flush(1).await.is_err());
    }

    #[tokio::test]
    async fn remove_cache_errors_without_bound_state() {
        let cm = test_cm(&test_conf(PAGE_SIZE, PAGE_SIZE * 16, 0.1));
        create_cache(&cm, 1).await;
        assert!(cm.remove_cache(1, false).await.is_err());
    }
}
