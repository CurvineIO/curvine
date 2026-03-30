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

use crate::common::heap_trace::{
    clear_latest_summary, record_capture_attempt, record_run_failure, record_run_success,
    store_latest_summary, write_flamegraph, write_summary, HeapProfileSummary, HeapTraceArtifact,
    HeapTraceArtifactKind, HeapTraceConfig, HeapTraceFlamegraph, HeapTraceHttpResponse,
    HeapTraceProfile, HeapTraceSummary,
};
use crate::{err_box, CommonResult};
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::{dump_profile, read_profile};

const PROFILE_FILE_NAME: &str = "heap.pb.gz";
const FLAMEGRAPH_FILE_NAME: &str = "heap.svg";
const SUMMARY_FILE_NAME: &str = "heap-profile-summary.json";
const RAW_PROFILE_FILE_NAME: &str = "heap.raw.pb.gz";
const DEFAULT_FLAMEGRAPH_STACKS: &str = "heap_trace;capture 1\n";

#[derive(Debug, Clone)]
pub struct CapturedProfile {
    pub profile: HeapTraceProfile,
    pub collapsed_stacks: String,
}

impl CapturedProfile {
    pub fn new(profile: HeapTraceProfile, collapsed_stacks: impl Into<String>) -> Self {
        Self {
            profile,
            collapsed_stacks: collapsed_stacks.into(),
        }
    }
}

#[async_trait]
pub trait HeapProfiler: Send + Sync {
    async fn capture(&self, conf: &HeapTraceConfig) -> CommonResult<CapturedProfile>;
}

#[derive(Debug)]
struct JemallocHeapProfiler {
    raw_profile_path: PathBuf,
}

impl JemallocHeapProfiler {
    fn new(raw_profile_path: PathBuf) -> Self {
        Self { raw_profile_path }
    }

    fn raw_profile_path(&self) -> &PathBuf {
        &self.raw_profile_path
    }
}

#[async_trait]
impl HeapProfiler for JemallocHeapProfiler {
    async fn capture(&self, _conf: &HeapTraceConfig) -> CommonResult<CapturedProfile> {
        let raw_profile_path = self.raw_profile_path();
        if let Some(parent) = raw_profile_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        dump_profile(raw_profile_path)?;
        let profile = read_profile(raw_profile_path)?;
        Ok(CapturedProfile::new(profile, DEFAULT_FLAMEGRAPH_STACKS))
    }
}

#[derive(Debug)]
struct RuntimeState {
    latest_profile: Option<HeapTraceProfile>,
    latest_flamegraph: Option<HeapTraceFlamegraph>,
    latest_profile_path: Option<PathBuf>,
    latest_flamegraph_path: Option<PathBuf>,
    latest_summary_path: Option<PathBuf>,
    capture_count: u64,
    last_capture_epoch_ms: Option<u64>,
}

impl RuntimeState {
    fn new() -> Self {
        Self {
            latest_profile: None,
            latest_flamegraph: None,
            latest_profile_path: None,
            latest_flamegraph_path: None,
            latest_summary_path: None,
            capture_count: 0,
            last_capture_epoch_ms: None,
        }
    }
}

#[derive(Clone)]
pub struct HeapTraceRuntime {
    conf: Arc<HeapTraceConfig>,
    profiler: Arc<dyn HeapProfiler>,
    artifact_dir: Arc<PathBuf>,
    state: Arc<Mutex<RuntimeState>>,
    periodic_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    run_lock: Arc<Mutex<()>>,
    running: Arc<AtomicBool>,
}

impl HeapTraceRuntime {
    pub fn new(conf: HeapTraceConfig) -> Self {
        let runtime_id = Uuid::new_v4().simple().to_string();
        let artifact_dir = std::env::temp_dir().join(format!("curvine-heap-trace-{runtime_id}"));
        let raw_profile_path = artifact_dir.join(RAW_PROFILE_FILE_NAME);
        let profiler = Arc::new(JemallocHeapProfiler::new(raw_profile_path));
        Self::with_profiler(conf, artifact_dir, profiler)
    }

    pub fn with_profiler(
        conf: HeapTraceConfig,
        artifact_dir: impl Into<PathBuf>,
        profiler: Arc<dyn HeapProfiler>,
    ) -> Self {
        let conf = Arc::new(conf);
        clear_latest_summary();
        Self {
            conf,
            profiler,
            artifact_dir: Arc::new(artifact_dir.into()),
            state: Arc::new(Mutex::new(RuntimeState::new())),
            periodic_task: Arc::new(Mutex::new(None)),
            run_lock: Arc::new(Mutex::new(())),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn conf(&self) -> &HeapTraceConfig {
        self.conf.as_ref()
    }

    pub fn summary(&self) -> HeapTraceSummary {
        HeapTraceSummary::from_config(self.conf())
    }

    pub fn is_enabled(&self) -> bool {
        self.conf.runtime_enabled
    }

    pub async fn capture_profile(&self) -> CommonResult<HeapTraceProfile> {
        self.trigger_now().await.map(|captured| captured.profile)
    }

    pub async fn capture_flamegraph(&self) -> CommonResult<HeapTraceFlamegraph> {
        self.trigger_now().await.map(|captured| captured.flamegraph)
    }

    pub async fn profile_artifact(&self) -> CommonResult<HeapTraceArtifact> {
        let artifact = self.trigger_now().await?;
        Ok(HeapTraceArtifact::new(
            HeapTraceArtifactKind::Profile,
            "application/octet-stream",
            PROFILE_FILE_NAME,
            artifact.profile.payload,
        ))
    }

    pub async fn flamegraph_artifact(&self) -> CommonResult<HeapTraceArtifact> {
        let artifact = self.trigger_now().await?;
        Ok(HeapTraceArtifact::new(
            HeapTraceArtifactKind::Flamegraph,
            "image/svg+xml",
            FLAMEGRAPH_FILE_NAME,
            artifact.flamegraph.svg.into_bytes(),
        ))
    }

    pub async fn flamegraph_http_response(&self) -> CommonResult<HeapTraceHttpResponse> {
        let artifact = self.flamegraph_artifact().await?;
        Ok(HeapTraceHttpResponse {
            content_type: artifact.media_type,
            body: artifact.payload,
        })
    }

    pub async fn trigger_now(&self) -> CommonResult<RuntimeCapture> {
        if !self.is_enabled() {
            return err_box!("heap trace runtime is disabled");
        }

        if self
            .running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return err_box!("heap trace capture already running");
        }

        let _guard = RunningGuard::new(self.running.clone());
        let _run_guard = self.run_lock.lock().await;
        record_capture_attempt();
        let started_at = Instant::now();

        let captured = match self.profiler.capture(self.conf()).await {
            Ok(captured) => captured,
            Err(err) => {
                record_run_failure();
                return Err(err);
            }
        };

        let profile_path = self.artifact_dir.join(PROFILE_FILE_NAME);
        let flamegraph_path = self.artifact_dir.join(FLAMEGRAPH_FILE_NAME);
        let summary_path = self.artifact_dir.join(SUMMARY_FILE_NAME);

        if let Some(parent) = profile_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&profile_path, &captured.profile.payload)?;

        let flamegraph = write_flamegraph(&flamegraph_path, captured.collapsed_stacks.as_str())?;
        let last_capture_epoch_ms = Some(now_epoch_ms());

        let summary = {
            let mut state = self.state.lock().await;
            state.capture_count += 1;
            state.last_capture_epoch_ms = last_capture_epoch_ms;
            state.latest_profile = Some(captured.profile.clone());
            state.latest_flamegraph = Some(flamegraph.clone());
            state.latest_profile_path = Some(profile_path.clone());
            state.latest_flamegraph_path = Some(flamegraph_path.clone());
            state.latest_summary_path = Some(summary_path.clone());

            HeapProfileSummary {
                runtime_enabled: self.conf.runtime_enabled,
                sample_interval_bytes: self.conf.sample_interval_bytes,
                capture_count: state.capture_count,
                last_capture_epoch_ms: state.last_capture_epoch_ms,
            }
        };

        write_summary(&summary_path, &summary)?;
        store_latest_summary(summary);
        record_run_success(started_at.elapsed());

        Ok(RuntimeCapture {
            profile: captured.profile,
            flamegraph,
            profile_path,
            flamegraph_path,
            summary_path,
        })
    }

    pub async fn start_periodic(&self, interval: Duration) -> CommonResult<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        if interval.is_zero() {
            return err_box!("heap trace periodic interval must be greater than zero");
        }

        let mut periodic_task = self.periodic_task.lock().await;
        if periodic_task.is_some() {
            return Ok(());
        }

        let weak_state = Arc::downgrade(&self.state);
        let running = self.running.clone();
        let run_lock = self.run_lock.clone();
        let conf = self.conf.clone();
        let profiler = self.profiler.clone();
        let artifact_dir = self.artifact_dir.clone();
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let Some(state) = Weak::upgrade(&weak_state) else {
                    break;
                };
                if running.load(Ordering::SeqCst) {
                    continue;
                }
                let runtime = HeapTraceRuntime {
                    conf: conf.clone(),
                    profiler: profiler.clone(),
                    artifact_dir: artifact_dir.clone(),
                    state,
                    periodic_task: Arc::new(Mutex::new(None)),
                    run_lock: run_lock.clone(),
                    running: running.clone(),
                };
                let _ = runtime.trigger_now().await;
            }
        });
        *periodic_task = Some(handle);
        Ok(())
    }

    pub async fn stop_periodic(&self) {
        let handle = self.periodic_task.lock().await.take();
        if let Some(handle) = handle {
            handle.abort();
        }
    }

    pub async fn latest_svg_path(&self) -> Option<PathBuf> {
        self.state.lock().await.latest_flamegraph_path.clone()
    }

    pub async fn latest_profile_artifact(&self) -> Option<HeapTraceArtifact> {
        let state = self.state.lock().await;
        state.latest_profile.clone().map(|profile| {
            HeapTraceArtifact::new(
                HeapTraceArtifactKind::Profile,
                "application/octet-stream",
                PROFILE_FILE_NAME,
                profile.payload,
            )
        })
    }

    pub async fn latest_flamegraph_artifact(&self) -> Option<HeapTraceArtifact> {
        let state = self.state.lock().await;
        state.latest_flamegraph.clone().map(|flamegraph| {
            HeapTraceArtifact::new(
                HeapTraceArtifactKind::Flamegraph,
                "image/svg+xml",
                FLAMEGRAPH_FILE_NAME,
                flamegraph.svg.into_bytes(),
            )
        })
    }
}

impl Drop for HeapTraceRuntime {
    fn drop(&mut self) {
        if Arc::strong_count(&self.periodic_task) != 1 {
            return;
        }
        if let Ok(mut periodic_task) = self.periodic_task.try_lock() {
            if let Some(handle) = periodic_task.take() {
                handle.abort();
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeCapture {
    pub profile: HeapTraceProfile,
    pub flamegraph: HeapTraceFlamegraph,
    pub profile_path: PathBuf,
    pub flamegraph_path: PathBuf,
    pub summary_path: PathBuf,
}

struct RunningGuard {
    running: Arc<AtomicBool>,
}

impl RunningGuard {
    fn new(running: Arc<AtomicBool>) -> Self {
        Self { running }
    }
}

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::heap_trace::latest_summary;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;
    use tokio::sync::Notify;

    #[derive(Debug)]
    struct FakeProfiler {
        calls: AtomicUsize,
        block_first: bool,
        notify: Notify,
    }

    impl FakeProfiler {
        fn new(block_first: bool) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                block_first,
                notify: Notify::new(),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn release(&self) {
            self.notify.notify_waiters();
        }
    }

    #[async_trait]
    impl HeapProfiler for FakeProfiler {
        async fn capture(&self, _conf: &HeapTraceConfig) -> CommonResult<CapturedProfile> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.block_first && call == 0 {
                self.notify.notified().await;
            }
            Ok(CapturedProfile::new(
                HeapTraceProfile::new(vec![1, 2, 3, call as u8]),
                format!("root;worker;run {count}\n", count = call + 1),
            ))
        }
    }

    fn test_runtime(profiler: Arc<FakeProfiler>, dir: &TempDir) -> HeapTraceRuntime {
        HeapTraceRuntime::with_profiler(
            HeapTraceConfig::new(true, 4096),
            dir.path().to_path_buf(),
            profiler,
        )
    }

    #[tokio::test]
    async fn trigger_now_returns_error_when_runtime_disabled() {
        let dir = TempDir::new().unwrap();
        let profiler = Arc::new(FakeProfiler::new(false));
        let runtime = HeapTraceRuntime::with_profiler(
            HeapTraceConfig::disabled(),
            dir.path().to_path_buf(),
            profiler.clone(),
        );

        let err = runtime.trigger_now().await.unwrap_err();

        assert!(err.to_string().contains("heap trace runtime is disabled"));
        assert_eq!(profiler.calls(), 0);
    }

    #[tokio::test]
    async fn trigger_now_writes_profile_flamegraph_and_summary() {
        let dir = TempDir::new().unwrap();
        let profiler = Arc::new(FakeProfiler::new(false));
        let runtime = test_runtime(profiler, &dir);

        let capture = runtime.trigger_now().await.unwrap();

        assert_eq!(capture.profile.format, "pprof");
        assert_eq!(capture.flamegraph.format, "svg");
        assert!(capture.profile_path.exists());
        assert!(capture.flamegraph_path.exists());
        assert!(capture.summary_path.exists());
        assert_eq!(
            runtime.latest_svg_path().await.unwrap(),
            capture.flamegraph_path
        );

        let summary = latest_summary().unwrap();
        assert_eq!(summary.capture_count, 1);
        assert!(summary.last_capture_epoch_ms.is_some());
    }

    #[tokio::test]
    async fn trigger_now_is_single_flight() {
        let dir = TempDir::new().unwrap();
        let profiler = Arc::new(FakeProfiler::new(true));
        let runtime = Arc::new(test_runtime(profiler.clone(), &dir));

        let first_runtime = runtime.clone();
        let first = tokio::spawn(async move { first_runtime.trigger_now().await });
        tokio::task::yield_now().await;

        let second = runtime.trigger_now().await;
        assert!(second.is_err());
        assert_eq!(profiler.calls(), 1);

        profiler.release();
        let first = first.await.unwrap().unwrap();
        assert!(first.flamegraph_path.exists());
    }

    #[tokio::test]
    async fn start_periodic_skips_overlapping_ticks() {
        let dir = TempDir::new().unwrap();
        let profiler = Arc::new(FakeProfiler::new(true));
        let runtime = test_runtime(profiler.clone(), &dir);

        runtime
            .start_periodic(Duration::from_millis(10))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(35)).await;
        assert_eq!(profiler.calls(), 1);

        profiler.release();
        tokio::time::sleep(Duration::from_millis(35)).await;
        runtime.stop_periodic().await;

        assert!(profiler.calls() >= 2);
        assert!(runtime.latest_svg_path().await.is_some());
    }

    #[tokio::test]
    async fn start_periodic_stops_after_runtime_drop_without_explicit_stop() {
        let dir = TempDir::new().unwrap();
        let profiler = Arc::new(FakeProfiler::new(false));
        let runtime = Arc::new(test_runtime(profiler.clone(), &dir));

        runtime
            .start_periodic(Duration::from_millis(10))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(35)).await;
        let before_drop = profiler.calls();
        assert!(before_drop > 0);

        drop(runtime);
        tokio::time::sleep(Duration::from_millis(35)).await;

        assert_eq!(profiler.calls(), before_drop);
    }

    #[tokio::test]
    async fn default_runtime_uses_unique_artifact_directory() {
        let left = HeapTraceRuntime::new(HeapTraceConfig::new(true, 4096));
        let right = HeapTraceRuntime::new(HeapTraceConfig::new(true, 4096));

        assert_ne!(left.artifact_dir.as_ref(), right.artifact_dir.as_ref());
        assert!(left
            .artifact_dir
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("curvine-heap-trace-")));
        assert!(right
            .artifact_dir
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("curvine-heap-trace-")));
    }
}
