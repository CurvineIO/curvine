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

use curvine_client::file::CurvineFileSystem;
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::state::{FileBlocks, FileStatus};
use curvine_common::utils::ProtoUtils;
use curvine_common::FsResult;
use futures::future::BoxFuture;
use log::{info, warn};
use orpc::common::LocalTime;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::transfer::{MetadataReplicaRefreshObservation, TransferMetrics};

pub trait CvMetadataReader: Send + Sync + 'static {
    fn current_epoch(&self) -> FsResult<Option<u64>>;

    fn current_refresh_time_ms(&self) -> FsResult<Option<i64>> {
        Ok(None)
    }

    fn covers_time_ms(&self, _time_ms: i64) -> FsResult<bool> {
        Ok(true)
    }

    fn get_status<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<FileStatus>>;

    fn get_status_at_epoch<'a>(
        &'a self,
        path: &'a Path,
        _epoch: Option<u64>,
    ) -> BoxFuture<'a, FsResult<FileStatus>> {
        self.get_status(path)
    }

    fn list_status<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<Vec<FileStatus>>>;

    fn list_status_at_epoch<'a>(
        &'a self,
        path: &'a Path,
        _epoch: Option<u64>,
    ) -> BoxFuture<'a, FsResult<Vec<FileStatus>>> {
        self.list_status(path)
    }

    fn get_block_locations<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<FileBlocks>>;

    fn get_block_locations_at_epoch<'a>(
        &'a self,
        path: &'a Path,
        _epoch: Option<u64>,
    ) -> BoxFuture<'a, FsResult<FileBlocks>> {
        self.get_block_locations(path)
    }
}

#[derive(Clone, Default)]
pub struct DisabledCvMetadataReader;

impl DisabledCvMetadataReader {
    fn disabled_error(path: &Path) -> FsError {
        FsError::common(format!(
            "CV metadata reader is disabled for {}; configure a production metadata replica, or explicitly enable transfer.cv_metadata_reader=master for development only",
            path.full_path()
        ))
    }
}

impl CvMetadataReader for DisabledCvMetadataReader {
    fn current_epoch(&self) -> FsResult<Option<u64>> {
        Ok(None)
    }

    fn current_refresh_time_ms(&self) -> FsResult<Option<i64>> {
        Ok(None)
    }

    fn covers_time_ms(&self, _time_ms: i64) -> FsResult<bool> {
        Ok(false)
    }

    fn get_status<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<FileStatus>> {
        Box::pin(async move { Err(Self::disabled_error(path)) })
    }

    fn list_status<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<Vec<FileStatus>>> {
        Box::pin(async move { Err(Self::disabled_error(path)) })
    }

    fn get_block_locations<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<FileBlocks>> {
        Box::pin(async move { Err(Self::disabled_error(path)) })
    }
}

#[derive(Clone)]
pub struct MasterCvMetadataReader {
    fs: CurvineFileSystem,
}

impl MasterCvMetadataReader {
    pub fn new(fs: CurvineFileSystem) -> Self {
        Self { fs }
    }
}

impl CvMetadataReader for MasterCvMetadataReader {
    fn current_epoch(&self) -> FsResult<Option<u64>> {
        Ok(None)
    }

    fn current_refresh_time_ms(&self) -> FsResult<Option<i64>> {
        Ok(None)
    }

    fn get_status<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<FileStatus>> {
        Box::pin(async move { self.fs.get_status(path).await })
    }

    fn list_status<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<Vec<FileStatus>>> {
        Box::pin(async move { self.fs.list_status(path).await })
    }

    fn get_block_locations<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<FileBlocks>> {
        Box::pin(async move { self.fs.get_block_locations(path).await })
    }
}

#[derive(Clone)]
pub struct MetadataReplicaReader {
    inner: Arc<MetadataReplicaInner>,
}

struct MetadataReplicaInner {
    fs: CurvineFileSystem,
    state: RwLock<MetadataReplicaState>,
    max_entries: usize,
    page_size: usize,
    history_size: usize,
    max_staleness: Duration,
}

#[derive(Clone, Default)]
struct MetadataReplicaState {
    current: MetadataReplicaSnapshot,
    history: VecDeque<MetadataReplicaSnapshot>,
}

#[derive(Clone, Default)]
struct MetadataReplicaSnapshot {
    version: u64,
    refresh_time_ms: i64,
    statuses: HashMap<String, FileStatus>,
    children: HashMap<String, Vec<FileStatus>>,
    blocks: HashMap<String, FileBlocks>,
}

impl MetadataReplicaReader {
    pub fn new(
        fs: CurvineFileSystem,
        max_entries: usize,
        page_size: usize,
        history_size: usize,
        max_staleness: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(MetadataReplicaInner {
                fs,
                state: RwLock::new(MetadataReplicaState::default()),
                max_entries,
                page_size,
                history_size,
                max_staleness,
            }),
        }
    }

    pub fn start_refresh_loop(&self, interval: Duration, stop: Arc<AtomicBool>) {
        let reader = self.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                if wait_or_stopped(interval, &stop).await {
                    break;
                }
                if let Err(err) = reader.refresh().await {
                    warn!("metadata replica refresh failed: {}", err);
                }
            }
        });
    }

    pub async fn refresh(&self) -> FsResult<u64> {
        let start = Instant::now();
        match self.refresh_delta(start).await {
            Ok(Some(version)) => return Ok(version),
            Ok(None) => {}
            Err(err) => {
                warn!(
                    "metadata replica delta refresh failed, falling back to full snapshot: {}",
                    err
                );
            }
        }
        self.refresh_full(start).await
    }

    async fn refresh_delta(&self, start: Instant) -> FsResult<Option<u64>> {
        let mut snapshot = {
            let state = self.inner.state.read();
            if state.current.refresh_time_ms <= 0 {
                return Ok(None);
            }
            state.current.clone()
        };
        let from_epoch = snapshot.version;
        let mut target_epoch = None;
        let mut page_token = None;
        let mut page_count = 0usize;
        loop {
            let page = self
                .inner
                .fs
                .get_cv_metadata_delta_page(
                    from_epoch,
                    target_epoch,
                    page_token.clone(),
                    Some(self.inner.page_size as u32),
                )
                .await?;
            page_count = page_count.saturating_add(1);
            if page.full_snapshot_required {
                return Ok(None);
            }
            if page.from_epoch != from_epoch {
                return Err(FsError::common(format!(
                    "Metadata replica delta from_epoch mismatch: expected={}, actual={}",
                    from_epoch, page.from_epoch
                )));
            }
            if let Some(epoch) = target_epoch {
                if epoch != page.to_epoch {
                    return Err(FsError::common(format!(
                        "Metadata replica delta target epoch changed during refresh: expected={}, actual={}",
                        epoch, page.to_epoch
                    )));
                }
            } else {
                target_epoch = Some(page.to_epoch);
            }

            for entry in page.entries {
                apply_delta_entry(&mut snapshot, entry)?;
            }

            match page.next_page_token {
                Some(next) if Some(next.clone()) != page_token => page_token = Some(next),
                Some(next) => {
                    return Err(FsError::common(format!(
                        "Metadata replica delta page token did not advance: {}",
                        next
                    )));
                }
                None => break,
            }
        }

        rebuild_snapshot_children(&mut snapshot);
        self.check_entry_limit(snapshot.statuses.len())?;
        let version = target_epoch.unwrap_or(from_epoch);
        snapshot.version = version;
        let entries = snapshot.statuses.len();
        let refresh_time_ms = LocalTime::mills() as i64;
        snapshot.refresh_time_ms = refresh_time_ms;
        self.publish_snapshot(snapshot);
        record_replica_refresh(
            "success",
            start,
            Some(version),
            Some(entries),
            Some(self.inner.page_size),
            Some(page_count),
            Some(refresh_time_ms),
        );
        info!(
            "metadata replica delta refreshed: from_epoch={}, version={}, entries={}",
            from_epoch, version, entries
        );
        Ok(Some(version))
    }

    async fn refresh_full(&self, start: Instant) -> FsResult<u64> {
        let mut snapshot = MetadataReplicaSnapshot::default();

        let mut page_token = None;
        let mut expected_epoch = None;
        let mut page_count = 0usize;
        loop {
            let page = match self
                .inner
                .fs
                .get_cv_metadata_snapshot_page(
                    page_token.clone(),
                    Some(self.inner.page_size as u32),
                )
                .await
            {
                Ok(page) => page,
                Err(err) => {
                    record_replica_refresh(
                        "failure",
                        start,
                        expected_epoch,
                        Some(snapshot.statuses.len()),
                        Some(self.inner.page_size),
                        Some(page_count),
                        None,
                    );
                    return Err(err);
                }
            };
            page_count = page_count.saturating_add(1);

            if let Some(epoch) = expected_epoch {
                if epoch != page.epoch {
                    record_replica_refresh(
                        "failure",
                        start,
                        Some(epoch),
                        Some(snapshot.statuses.len()),
                        Some(self.inner.page_size),
                        Some(page_count),
                        None,
                    );
                    return Err(FsError::common(format!(
                        "Metadata replica snapshot epoch changed during refresh: expected={}, actual={}",
                        epoch, page.epoch
                    )));
                }
            } else {
                expected_epoch = Some(page.epoch);
            }

            for entry in page.entries {
                let status = ProtoUtils::file_status_from_pb(entry.status);
                let path = match Path::from_str(&status.path) {
                    Ok(path) => path,
                    Err(err) => {
                        record_replica_refresh(
                            "failure",
                            start,
                            expected_epoch,
                            Some(snapshot.statuses.len()),
                            Some(self.inner.page_size),
                            Some(page_count),
                            None,
                        );
                        return Err(replica_error(err));
                    }
                };
                let key = normalized_cv_key(&path);
                if status.is_dir {
                    snapshot.children.entry(key.clone()).or_default();
                } else if let Some(blocks) = entry.blocks {
                    snapshot
                        .blocks
                        .insert(key.clone(), ProtoUtils::file_blocks_from_pb(blocks));
                }

                if key != "/" {
                    let parent_key = parent_cv_key(&key);
                    snapshot
                        .children
                        .entry(parent_key)
                        .or_default()
                        .push(status.clone());
                }
                snapshot.statuses.insert(key, status);
                if let Err(err) = self.check_entry_limit(snapshot.statuses.len()) {
                    record_replica_refresh(
                        "failure",
                        start,
                        expected_epoch,
                        Some(snapshot.statuses.len()),
                        Some(self.inner.page_size),
                        Some(page_count),
                        None,
                    );
                    return Err(err);
                }
            }

            match page.next_page_token {
                Some(next) if Some(next.clone()) != page_token => page_token = Some(next),
                Some(next) => {
                    record_replica_refresh(
                        "failure",
                        start,
                        expected_epoch,
                        Some(snapshot.statuses.len()),
                        Some(self.inner.page_size),
                        Some(page_count),
                        None,
                    );
                    return Err(FsError::common(format!(
                        "Metadata replica snapshot page token did not advance: {}",
                        next
                    )));
                }
                None => break,
            }
        }

        if !snapshot.statuses.contains_key("/") {
            record_replica_refresh(
                "failure",
                start,
                expected_epoch,
                Some(0),
                Some(self.inner.page_size),
                Some(page_count),
                None,
            );
            return Err(FsError::common(
                "Metadata replica snapshot did not include root entry",
            ));
        }
        for children in snapshot.children.values_mut() {
            children.sort_by(|left, right| left.name.cmp(&right.name));
        }

        let current_version = self.inner.state.read().current.version;
        snapshot.version = expected_epoch.unwrap_or_else(|| current_version.saturating_add(1));
        let version = snapshot.version;
        let entries = snapshot.statuses.len();
        let refresh_time_ms = LocalTime::mills() as i64;
        snapshot.refresh_time_ms = refresh_time_ms;
        self.publish_snapshot(snapshot);
        record_replica_refresh(
            "success",
            start,
            Some(version),
            Some(entries),
            Some(self.inner.page_size),
            Some(page_count),
            Some(refresh_time_ms),
        );
        info!(
            "metadata replica refreshed: version={}, entries={}",
            version, entries
        );
        Ok(version)
    }

    fn publish_snapshot(&self, snapshot: MetadataReplicaSnapshot) {
        let mut guard = self.inner.state.write();
        let previous = std::mem::replace(&mut guard.current, snapshot);
        if previous.refresh_time_ms > 0 && previous.version != guard.current.version {
            guard.history.push_front(previous);
            let max_previous = self.inner.history_size.saturating_sub(1);
            while guard.history.len() > max_previous {
                guard.history.pop_back();
            }
        }
    }

    fn check_entry_limit(&self, entries: usize) -> FsResult<()> {
        if entries > self.inner.max_entries {
            return Err(FsError::common(format!(
                "Metadata replica entry limit exceeded: entries={}, limit={}",
                entries, self.inner.max_entries
            )));
        }
        Ok(())
    }

    fn check_snapshot_fresh(&self, snapshot: &MetadataReplicaSnapshot) -> FsResult<()> {
        if snapshot.refresh_time_ms <= 0 {
            return Err(FsError::common(
                "Metadata replica is not ready; wait for initial refresh",
            ));
        }
        let now_ms = LocalTime::mills() as i64;
        let max_staleness_ms = self.inner.max_staleness.as_millis().min(i64::MAX as u128) as i64;
        let staleness_ms = now_ms.saturating_sub(snapshot.refresh_time_ms);
        if staleness_ms > max_staleness_ms {
            return Err(FsError::common(format!(
                "Metadata replica is stale: staleness_ms={}, max_staleness_ms={}, version={}",
                staleness_ms, max_staleness_ms, snapshot.version
            )));
        }
        Ok(())
    }

    fn with_snapshot_at_epoch<T>(
        &self,
        epoch: Option<u64>,
        path: &Path,
        f: impl FnOnce(&MetadataReplicaSnapshot, &str) -> FsResult<T>,
    ) -> FsResult<T> {
        let key = normalized_cv_key(path);
        let state = self.inner.state.read();
        let snapshot = match epoch {
            Some(epoch) if state.current.version != epoch => state
                .history
                .iter()
                .find(|snapshot| snapshot.version == epoch)
                .ok_or_else(|| {
                    FsError::common(format!(
                        "CV metadata replica epoch {} is no longer available for {}",
                        epoch,
                        path.full_path()
                    ))
                })?,
            _ => &state.current,
        };
        self.check_snapshot_fresh(snapshot)?;
        f(snapshot, &key)
    }
}

impl CvMetadataReader for MetadataReplicaReader {
    fn current_epoch(&self) -> FsResult<Option<u64>> {
        let state = self.inner.state.read();
        self.check_snapshot_fresh(&state.current)?;
        Ok(Some(state.current.version))
    }

    fn current_refresh_time_ms(&self) -> FsResult<Option<i64>> {
        let state = self.inner.state.read();
        self.check_snapshot_fresh(&state.current)?;
        Ok(Some(state.current.refresh_time_ms))
    }

    fn covers_time_ms(&self, time_ms: i64) -> FsResult<bool> {
        let state = self.inner.state.read();
        self.check_snapshot_fresh(&state.current)?;
        Ok(state.current.refresh_time_ms >= time_ms)
    }

    fn get_status<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<FileStatus>> {
        self.get_status_at_epoch(path, None)
    }

    fn get_status_at_epoch<'a>(
        &'a self,
        path: &'a Path,
        epoch: Option<u64>,
    ) -> BoxFuture<'a, FsResult<FileStatus>> {
        Box::pin(async move {
            self.with_snapshot_at_epoch(epoch, path, |snapshot, key| {
                snapshot
                    .statuses
                    .get(key)
                    .cloned()
                    .ok_or_else(|| FsError::file_not_found(path.full_path()))
            })
        })
    }

    fn list_status<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<Vec<FileStatus>>> {
        self.list_status_at_epoch(path, None)
    }

    fn list_status_at_epoch<'a>(
        &'a self,
        path: &'a Path,
        epoch: Option<u64>,
    ) -> BoxFuture<'a, FsResult<Vec<FileStatus>>> {
        Box::pin(async move {
            self.with_snapshot_at_epoch(epoch, path, |snapshot, key| {
                let status = snapshot
                    .statuses
                    .get(key)
                    .ok_or_else(|| FsError::file_not_found(path.full_path()))?;
                if !status.is_dir {
                    return Err(FsError::common(format!(
                        "Path {} is not a directory",
                        path.full_path()
                    )));
                }
                Ok(snapshot.children.get(key).cloned().unwrap_or_default())
            })
        })
    }

    fn get_block_locations<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<FileBlocks>> {
        self.get_block_locations_at_epoch(path, None)
    }

    fn get_block_locations_at_epoch<'a>(
        &'a self,
        path: &'a Path,
        epoch: Option<u64>,
    ) -> BoxFuture<'a, FsResult<FileBlocks>> {
        Box::pin(async move {
            self.with_snapshot_at_epoch(epoch, path, |snapshot, key| {
                snapshot
                    .blocks
                    .get(key)
                    .cloned()
                    .ok_or_else(|| FsError::file_not_found(path.full_path()))
            })
        })
    }
}

async fn wait_or_stopped(interval: Duration, stop: &AtomicBool) -> bool {
    let deadline = Instant::now() + interval;
    while !stop.load(Ordering::Relaxed) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        tokio::time::sleep(remaining.min(Duration::from_millis(100))).await;
    }
    true
}

fn normalized_cv_key(path: &Path) -> String {
    let text = path.path().trim_end_matches('/');
    if text.is_empty() {
        "/".to_string()
    } else {
        text.to_string()
    }
}

fn parent_cv_key(key: &str) -> String {
    match key.rsplit_once('/') {
        Some(("", _)) | None => "/".to_string(),
        Some((parent, _)) => parent.to_string(),
    }
}

fn apply_delta_entry(
    snapshot: &mut MetadataReplicaSnapshot,
    entry: curvine_common::proto::CvMetadataDeltaEntryProto,
) -> FsResult<()> {
    let path = Path::from_str(&entry.path).map_err(replica_error)?;
    let key = normalized_cv_key(&path);
    match entry.entry {
        Some(entry) => {
            let status = ProtoUtils::file_status_from_pb(entry.status);
            if status.is_dir {
                snapshot.children.entry(key.clone()).or_default();
                snapshot.blocks.remove(&key);
            } else if let Some(blocks) = entry.blocks {
                snapshot
                    .blocks
                    .insert(key.clone(), ProtoUtils::file_blocks_from_pb(blocks));
                snapshot.children.remove(&key);
            } else {
                snapshot.blocks.remove(&key);
                snapshot.children.remove(&key);
            }
            snapshot.statuses.insert(key, status);
        }
        None => remove_snapshot_subtree(snapshot, &key),
    }
    Ok(())
}

fn remove_snapshot_subtree(snapshot: &mut MetadataReplicaSnapshot, key: &str) {
    let prefix = if key == "/" {
        "/".to_string()
    } else {
        format!("{key}/")
    };
    let removed: Vec<String> = snapshot
        .statuses
        .keys()
        .filter(|path| path.as_str() == key || path.starts_with(&prefix))
        .cloned()
        .collect();
    for path in removed {
        snapshot.statuses.remove(&path);
        snapshot.children.remove(&path);
        snapshot.blocks.remove(&path);
    }
}

fn rebuild_snapshot_children(snapshot: &mut MetadataReplicaSnapshot) {
    snapshot.children.clear();
    let mut statuses: Vec<FileStatus> = snapshot.statuses.values().cloned().collect();
    statuses.sort_by(|left, right| left.path.cmp(&right.path));
    for status in statuses {
        let Ok(path) = Path::from_str(&status.path) else {
            continue;
        };
        let key = normalized_cv_key(&path);
        if status.is_dir {
            snapshot.children.entry(key.clone()).or_default();
        }
        if key != "/" {
            let parent_key = parent_cv_key(&key);
            snapshot
                .children
                .entry(parent_key)
                .or_default()
                .push(status);
        }
    }
    for children in snapshot.children.values_mut() {
        children.sort_by(|left, right| left.name.cmp(&right.name));
    }
}

fn record_replica_refresh(
    result: &str,
    start: Instant,
    version: Option<u64>,
    entries: Option<usize>,
    page_size: Option<usize>,
    pages: Option<usize>,
    refresh_time_ms: Option<i64>,
) {
    if let Ok(metrics) = TransferMetrics::get() {
        metrics.observe_metadata_replica_refresh(MetadataReplicaRefreshObservation {
            result,
            elapsed_us: start.elapsed().as_micros(),
            version,
            entries,
            page_size,
            pages,
            refresh_time_ms,
        });
    }
}

fn replica_error(err: impl std::fmt::Display) -> FsError {
    FsError::common(format!("Metadata replica refresh failed: {}", err))
}
