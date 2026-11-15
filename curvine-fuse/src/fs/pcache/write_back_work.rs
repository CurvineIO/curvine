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

use crate::fs::pcache::{CacheManager, DirtyEntry, DirtyList, DirtyMessage, Operator};
use crate::{FuseMetrics, FuseResult};
use curvine_config::FuseConf;
use curvine_runtime::common::{ByteUnit, TimeSpent};
use curvine_runtime::sync::channel::AsyncReceiver;
use futures::future::join_all;
use log::{debug, info, warn};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};

pub struct WriteBackWork {
    cm: Arc<CacheManager>,
    receiver: AsyncReceiver<DirtyMessage>,
    flush_delay: Duration,
    tick_interval: Duration,
    max_concurrent: usize,
}

impl WriteBackWork {
    pub fn new(
        cm: Arc<CacheManager>,
        receiver: AsyncReceiver<DirtyMessage>,
        conf: &FuseConf,
    ) -> Self {
        Self {
            cm,
            receiver,
            flush_delay: conf.write_back_flush_delay.as_duration(),
            tick_interval: conf.write_back_tick_interval.as_duration(),
            max_concurrent: conf.write_back_max_concurrent,
        }
    }

    pub async fn execute(cm: &CacheManager, ino: u64, complete_num: usize) -> u64 {
        let Some(cache) = cm.get_cache(ino).await else {
            return 0;
        };

        let flushed = match cache.flush_dirty(false).await {
            Ok((_, len)) => len,
            Err(e) => {
                FuseMetrics::with(|m| {
                    m.write_back_error_count.with_label_values(&["flush"]).inc();
                });

                if e.is_enoent() {
                    debug!("write_back flush ino={} error: {}", ino, e);
                } else {
                    warn!("write_back flush ino={} error: {}", ino, e);
                }
                cache.set_error(e);

                0
            }
        };

        for _ in 0..complete_num {
            if let Err(e) = cm.release_cache(&cache).await {
                FuseMetrics::with(|m| {
                    m.write_back_error_count
                        .with_label_values(&["release"])
                        .inc();
                });

                if e.is_enoent() {
                    debug!("write_back release_cache ino={} error: {}", ino, e);
                } else {
                    warn!("write_back release_cache ino={} error: {}", ino, e);
                }

                cache.set_error(e);
            }
        }

        cache.free_pages(false);
        flushed
    }

    pub async fn flush_inodes(cm: &Arc<CacheManager>, list: VecDeque<DirtyEntry>) {
        let len = list.len();
        if len == 0 {
            return;
        }

        let spend = TimeSpent::new();
        let handles = list.iter().map(|entry| {
            let cm = cm.clone();
            let (ino, complete_num) = (entry.ino, entry.complete_num);
            tokio::spawn(async move { Self::execute(&cm, ino, complete_num).await })
        });

        let flushed: u64 = join_all(handles)
            .await
            .into_iter()
            .map(|res| {
                res.unwrap_or_else(|e| {
                    warn!("write_back flush task panicked: {}", e);
                    FuseMetrics::with(|m| {
                        m.write_back_error_count.with_label_values(&["spawn"]).inc();
                    });
                    0
                })
            })
            .sum();

        FuseMetrics::with(|m| {
            m.write_back_io_us
                .with_label_values(&["flush"])
                .observe(spend.used_us() as f64);
            m.write_back_io_bytes
                .with_label_values(&["flush"])
                .inc_by(flushed as i64);
        });

        info!(
            "write_back flush_inodes={}, flushed={}, elapsed={}ms",
            len,
            ByteUnit::byte_to_string(flushed),
            spend.used_ms()
        );
    }

    pub async fn evict_inodes(cm: &Arc<CacheManager>) {
        let before = cm.used();
        let target = cm.free_target();
        let spend = TimeSpent::new();

        let evict_candidates = cm.evict_candidates().await;
        let mut freed_bytes = 0;
        let mut freed_inodes = 0;
        for inode in &evict_candidates {
            let Some(cache) = cm.get_cache(inode.ino).await else {
                continue;
            };

            // a642df02c dropped the flush step: evict must push dirty pages to
            // the backend before freeing them, otherwise cached writes are
            // silently dropped while `free_pages(false)` still keeps them.
            // `flush_dirty(false)` publishes only full dirty pages; partial
            // pages stay dirty and must therefore survive eviction.
            let force = match cache.flush_dirty(false).await {
                Ok(_) => false,
                Err(e) => {
                    warn!("write_back flush_pages ino={} error: {}", cache.ino, e);
                    cache.set_error(e);
                    FuseMetrics::with(|m| {
                        m.write_back_error_count.with_label_values(&["evict"]).inc();
                    });
                    true
                }
            };
            let bytes = cache.free_pages(force);
            debug!(
                "write_back evict ino={}, file_len={}, freed={}",
                inode.ino, inode.file_len, bytes
            );

            freed_bytes += bytes;
            freed_inodes += 1;

            if freed_bytes >= target {
                break;
            }
        }

        FuseMetrics::with(|m| {
            m.write_back_io_us
                .with_label_values(&["evict"])
                .observe(spend.used_us() as f64);
            m.write_back_io_bytes
                .with_label_values(&["evict"])
                .inc_by(freed_bytes as i64);
        });

        info!(
            "write_back evict_inodes={}, freed={}[{} -> {}], elapsed={}ms",
            freed_inodes,
            ByteUnit::byte_to_string(freed_bytes),
            ByteUnit::byte_to_string(before),
            ByteUnit::byte_to_string(cm.used()),
            spend.used_ms(),
        );
    }

    pub fn set_metrics(cm: &CacheManager, dirty_len: usize) {
        FuseMetrics::with(|m| {
            let free_ratio = (cm.free_ratio() * 100.0).round() as i64;
            m.write_back_cache_free_ratio.set(free_ratio);
            m.write_back_dirty_inode_num.set(dirty_len as i64);
            m.write_back_cache_inode_num.set(cm.len() as i64);
            m.write_back_cache_usage.set(cm.used() as i64);
            m.write_back_cache_limit.set(cm.capacity() as i64);
        });
    }

    pub async fn run(self) -> FuseResult<()> {
        let mut ticker = interval(self.tick_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticker.tick().await;

        let mut receiver = self.receiver;
        let cm = self.cm;
        let max_concurrent = self.max_concurrent.max(1);

        let mut dirty_list = DirtyList::new(self.flush_delay);
        loop {
            tokio::select! {
                biased;

                _ = ticker.tick() => {
                    Self::set_metrics(&cm, dirty_list.len());

                    Self::flush_inodes(&cm, dirty_list.pop_ready(max_concurrent)).await;

                    if cm.need_evict() {
                        Self::evict_inodes(&cm).await;
                    }
                }

                result = receiver.recv() => {
                    match result {
                        None => {
                            Self::flush_inodes(&cm, dirty_list.pop_all()).await;
                            break;
                        }

                        Some(DirtyMessage::Write(ino)) => {
                            dirty_list.insert(ino, Operator::Write);
                        }

                        Some(DirtyMessage::Complete(ino)) => {
                            dirty_list.insert(ino, Operator::Complete);
                            if dirty_list.len() >= max_concurrent {
                                Self::flush_inodes(&cm, dirty_list.pop_ready(max_concurrent)).await;
                            }
                        }

                        Some(DirtyMessage::CommitAll(stop, reply)) => {
                            let all = dirty_list.pop_all();
                            Self::flush_inodes(&cm, all).await;
                            let _ = reply.send(Ok(()));
                            if stop {
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::pcache::{InodeCache, PageStore};
    use bytes::Bytes;
    use curvine_config::FuseConf;
    use curvine_model::{CreateFileOpts, OpenFlags};
    use curvine_runtime::common::{ByteUnit, DurationUnit};
    use curvine_runtime::sync::channel::{AsyncChannel, CallChannel};

    fn test_conf(page_size: u64) -> FuseConf {
        FuseConf {
            write_back_cache_type: "memory".to_string(),
            write_back_page_size: ByteUnit::new(page_size),
            write_back_cache_size: ByteUnit::new(page_size * 16),
            ..Default::default()
        }
    }

    fn test_cm(conf: &FuseConf) -> (Arc<CacheManager>, AsyncReceiver<DirtyMessage>) {
        let (sender, receiver) = AsyncChannel::new(0).split();
        (Arc::new(CacheManager::new(conf, sender).unwrap()), receiver)
    }

    async fn write_cache(cm: &Arc<CacheManager>, ino: u64, data: &[u8]) -> Arc<InodeCache> {
        let cache = cm
            .get_or_create(ino, OpenFlags::RDWR, CreateFileOpts::with_create(false), 0)
            .await
            .unwrap();
        cache.write(0, Bytes::copy_from_slice(data)).await.unwrap();
        cache
    }

    #[tokio::test]
    async fn execute_missing_cache_is_noop() {
        let conf = test_conf(4096);
        let (cm, _receiver) = test_cm(&conf);
        assert_eq!(WriteBackWork::execute(&cm, 999, 0).await, 0);
    }

    #[tokio::test]
    async fn execute_with_dirty_pages_without_state_flushes_nothing() {
        let conf = test_conf(4096);
        let (cm, _receiver) = test_cm(&conf);
        write_cache(&cm, 100, &[0u8; 4096]).await;
        assert_eq!(WriteBackWork::execute(&cm, 100, 0).await, 0);
    }

    #[tokio::test]
    async fn execute_with_partial_page_without_state_flushes_nothing() {
        let conf = test_conf(4096);
        let (cm, _receiver) = test_cm(&conf);
        write_cache(&cm, 100, b"partial").await;
        assert_eq!(WriteBackWork::execute(&cm, 100, 0).await, 0);
    }

    #[tokio::test]
    async fn flush_inodes_ignores_missing_cache() {
        let conf = test_conf(4096);
        let (cm, _receiver) = test_cm(&conf);
        let mut list = VecDeque::new();
        list.push_back(DirtyEntry {
            ino: 1,
            created_at: 0,
            complete_num: 0,
        });
        WriteBackWork::flush_inodes(&cm, list).await;
    }

    #[tokio::test]
    async fn evict_if_needed_no_pressure_is_noop() {
        let conf = test_conf(4096);
        let (cm, _receiver) = test_cm(&conf);
        WriteBackWork::evict_inodes(&cm).await;
        assert!(!cm.is_full());
    }

    #[tokio::test]
    async fn evict_if_needed_with_pressure_handles_empty() {
        let mut conf = test_conf(4096);
        conf.write_back_free_ratio = 0.5;
        let (cm, _receiver) = test_cm(&conf);
        cm.store.add_usage(cm.store.capacity()).unwrap();
        assert!(cm.need_evict());
        WriteBackWork::evict_inodes(&cm).await;
        assert!(cm.is_full());
    }

    #[tokio::test]
    async fn run_stops_on_commit_all() {
        let conf = test_conf(4096);
        let (cm, receiver) = test_cm(&conf);
        let work = WriteBackWork::new(cm.clone(), receiver, &conf);
        let handle = tokio::spawn(async move { work.run().await });

        let (tx, rx) = CallChannel::channel();
        cm.send(DirtyMessage::CommitAll(true, tx)).await.unwrap();
        rx.receive().await.unwrap().unwrap();

        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn run_processes_messages_then_commit_all() {
        let conf = test_conf(4096);
        let (cm, receiver) = test_cm(&conf);
        let work = WriteBackWork::new(cm.clone(), receiver, &conf);
        let handle = tokio::spawn(async move { work.run().await });

        cm.send(DirtyMessage::Write(100)).await.unwrap();
        cm.send(DirtyMessage::Complete(200)).await.unwrap();

        let (tx, rx) = CallChannel::channel();
        cm.send(DirtyMessage::CommitAll(false, tx)).await.unwrap();
        rx.receive().await.unwrap().unwrap();

        let (tx, rx) = CallChannel::channel();
        cm.send(DirtyMessage::CommitAll(true, tx)).await.unwrap();
        rx.receive().await.unwrap().unwrap();

        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn run_tick_flushes_without_panic() {
        let mut conf = test_conf(4096);
        conf.write_back_tick_interval = DurationUnit::new(10);
        let (cm, receiver) = test_cm(&conf);
        let work = WriteBackWork::new(cm.clone(), receiver, &conf);
        let handle = tokio::spawn(async move { work.run().await });

        write_cache(&cm, 100, b"tick data").await;
        cm.send(DirtyMessage::Write(100)).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(40)).await;

        let (tx, rx) = CallChannel::channel();
        cm.send(DirtyMessage::CommitAll(true, tx)).await.unwrap();
        rx.receive().await.unwrap().unwrap();

        handle.await.unwrap().unwrap();
    }
}
