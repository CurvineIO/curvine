// Copyright 2026 OPPO.
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

use axum::extract::Query;
use axum::http::{Method, StatusCode};
use axum::Router;
use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_fs_api::{FileSystem, Path};
use curvine_model::{
    AccessMode, FileStatus, JobTaskState, ListOptions, MountOptions, SetAttrOpts, TtlAction,
    WriteType,
};
use curvine_proto::{ListOptionsResponse, ListStatusResponse};
use curvine_runtime::common::{LocalTime, Utils};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_server::test::MiniCluster;
use curvine_unified_fs::UnifiedFileSystem;
use prost::Message;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

// Count real OpenDAL HTTP requests. Switching to failures proves warm paths
// keep working without UFS; cold, expired and verify paths must still reach it.
struct S3Probe {
    endpoint: String,
    requests: Arc<AtomicUsize>,
    failing: Arc<AtomicBool>,
    server: tokio::task::JoinHandle<()>,
}

impl S3Probe {
    async fn start(rt: &Runtime) -> CommonResult<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let requests = Arc::new(AtomicUsize::new(0));
        let failing = Arc::new(AtomicBool::new(false));
        let count = requests.clone();
        let fail = failing.clone();
        let app = Router::new().fallback(
            move |method: Method, Query(query): Query<HashMap<String, String>>| {
                count.fetch_add(1, Ordering::SeqCst);
                let failing = fail.load(Ordering::SeqCst);
                async move {
                    let (status, body) = if failing {
                        (
                            StatusCode::TOO_MANY_REQUESTS,
                            "<Error><Code>SlowDown</Code></Error>".to_string(),
                        )
                    } else if method == Method::HEAD {
                        (
                            StatusCode::NOT_FOUND,
                            "<Error><Code>NoSuchKey</Code></Error>".to_string(),
                        )
                    } else {
                        let prefix = query.get("prefix").cloned().unwrap_or_default();
                        let key = format!("{prefix}remote.bin");
                        let contents = if query.get("start-after").is_some_and(|v| v >= &key) {
                            String::new()
                        } else {
                            format!(
                                "<Contents><Key>{key}</Key><Size>4</Size>\
                                 <LastModified>2026-10-01T00:00:00Z</LastModified>\
                                 <ETag>test-etag</ETag></Contents>"
                            )
                        };
                        (
                            StatusCode::OK,
                            format!(
                                "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                                 <Name>test-bucket</Name><Prefix>{prefix}</Prefix>\
                                 <IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"
                            ),
                        )
                    };
                    (status, [("content-type", "application/xml")], body)
                }
            },
        );
        let server = rt.spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Ok(Self {
            endpoint,
            requests,
            failing,
            server,
        })
    }

    fn count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    fn options(&self) -> MountOptions {
        MountOptions::builder()
            .auto_cache(false)
            .ttl_ms(0)
            .add_property("s3.endpoint_url", &self.endpoint)
            .add_property("s3.region_name", "us-east-1")
            .add_property("s3.credentials.access", "test-access")
            .add_property("s3.credentials.secret", "test-secret")
            .add_property("opendal.retry_times", "0")
            .build()
    }
}

impl Drop for S3Probe {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn assert_names(statuses: impl IntoIterator<Item = FileStatus>, expected: &[&str]) {
    let mut names: Vec<_> = statuses.into_iter().map(|status| status.name).collect();
    names.sort();
    assert_eq!(names, expected);
}

async fn assert_listings(
    fs: &UnifiedFileSystem,
    path: &Path,
    expected: &[&str],
    supports_options: bool,
) -> CommonResult<()> {
    assert_names(fs.list_status(path).await?, expected);
    let bytes = fs.list_status_bytes(path).await?;
    let reply = ListStatusResponse::decode(bytes.freeze())?;
    let mut names: Vec<_> = reply
        .statuses
        .into_iter()
        .map(|status| status.name)
        .collect();
    names.sort();
    assert_eq!(names, expected);
    if supports_options {
        assert_names(
            fs.list_options(path, ListOptions::default()).await?,
            expected,
        );
        let bytes = fs.list_options_bytes(path, ListOptions::default()).await?;
        let reply = ListOptionsResponse::decode(bytes.freeze())?;
        let mut names: Vec<_> = reply
            .statuses
            .into_iter()
            .map(|status| status.name)
            .collect();
        names.sort();
        assert_eq!(names, expected);
    } else {
        // OpenDAL's non-streaming list_options is unsupported today. Keep
        // that existing fallback behavior; do not add a backend API here.
        assert!(fs.list_options(path, ListOptions::default()).await.is_err());
        assert!(fs
            .list_options_bytes(path, ListOptions::default())
            .await
            .is_err());
    }
    let mut stream = fs.list_stream(path, ListOptions::default()).await?;
    assert_names(stream.collect_vec().await?, expected);
    Ok(())
}

async fn assert_listing_errors(fs: &UnifiedFileSystem, path: &Path) -> CommonResult<()> {
    assert!(fs.list_status(path).await.is_err());
    assert!(fs.list_status_bytes(path).await.is_err());
    assert!(fs.list_options(path, ListOptions::default()).await.is_err());
    assert!(fs
        .list_options_bytes(path, ListOptions::default())
        .await
        .is_err());
    if let Ok(mut stream) = fs.list_stream(path, ListOptions::default()).await {
        assert!(stream.collect_vec().await.is_err());
    }
    Ok(())
}

#[test]
fn metadata_trust_routes_real_stat_and_all_five_list_apis() -> CommonResult<()> {
    let base = Utils::test_sub_dir(format!("metadata-trust-{}", Utils::rand_str(8)));
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.master.meta_dir = format!("{base}/meta");
    conf.journal.journal_dir = format!("{base}/journal");
    conf.worker.data_dir = vec![format!("[MEM:64MB]{base}/worker")];
    conf.journal.raft_tick_interval_ms = 100;
    conf.client.short_circuit = false;
    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();
    rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(60), async {
            let probe = S3Probe::start(&rt).await?;
            let fs = UnifiedFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone())?;
            let cv = fs.cv();
            let source = Path::from_str("s3://test-bucket/models")?;
            let mount = Path::from_str("/cache")?;
            fs.mount(&source, &mount, probe.options()).await?;
            let warm = Path::from_str("/cache/model")?;
            cv.mkdir(&warm, true).await?;
            for name in ["a", "b", "c"] {
                cv.mkdir(&Path::from_str(format!("/cache/model/{name}"))?, true)
                    .await?;
            }
            let empty = Path::from_str("/cache/empty")?;
            cv.mkdir(&empty, true).await?;
            let cached = cv.get_status(&warm).await?;
            assert_eq!(cached.storage_policy.ufs_mtime, 0);
            assert!(
                !cached.is_complete,
                "directory completion is not a listing marker"
            );

            // Warm stat preserves the CV inode identity and timestamps. All
            // listing APIs use the CV view even when UFS is unavailable.
            probe.failing.store(true, Ordering::SeqCst);
            let status = fs.get_status(&warm).await?;
            assert_eq!(status.id, cached.id);
            assert_eq!(status.mtime, cached.mtime);
            assert_eq!(status.children_num, 3);
            assert_listings(&fs, &warm, &["a", "b", "c"], true).await?;
            assert_listings(&fs, &empty, &[], true).await?;
            let options = ListOptions {
                limit: Some(1),
                start_after: Some("b".into()),
            };
            assert_names(fs.list_options(&warm, options.clone()).await?, &["c"]);
            let bytes = fs.list_options_bytes(&warm, options).await?;
            let reply = ListOptionsResponse::decode(bytes.freeze())?;
            assert_eq!(reply.statuses.len(), 1);
            assert_eq!(reply.statuses[0].name, "c");
            let mut stream = fs
                .list_stream(
                    &warm,
                    ListOptions {
                        limit: Some(1),
                        start_after: None,
                    },
                )
                .await?;
            assert_names(stream.collect_vec().await?, &["a", "b", "c"]);
            let second = UnifiedFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone())?;
            assert_listings(&second, &warm, &["a", "b", "c"], true).await?;
            assert_eq!(
                probe.count(),
                0,
                "warm metadata must issue no UFS HTTP requests"
            );

            // Expiry can be visible in a successful CV get_status before the
            // background TTL executor deletes it. D2 must check it explicitly.
            let expired = Path::from_str("/cache/expired")?;
            cv.mkdir(&expired, true).await?;
            cv.set_attr(
                &expired,
                SetAttrOpts {
                    mtime: Some(LocalTime::mills() as i64 - 60_000),
                    ttl_ms: Some(1),
                    ttl_action: Some(TtlAction::Delete),
                    ..Default::default()
                },
            )
            .await?;
            assert!(cv.get_status(&expired).await?.is_expired());
            probe.failing.store(false, Ordering::SeqCst);
            for path in [&expired, &Path::from_str("/cache/cold")?] {
                let before = probe.count();
                assert!(fs.get_status(path).await?.is_dir);
                assert_listings(&fs, path, &["remote.bin"], false).await?;
                assert!(
                    probe.count() > before,
                    "expired/missing entries must reach UFS"
                );
            }

            for (root, access, verify, mode) in [
                ("/verify", AccessMode::ReadOnly, true, WriteType::CacheMode),
                (
                    "/writable",
                    AccessMode::ReadWrite,
                    false,
                    WriteType::CacheMode,
                ),
                ("/filesystem", AccessMode::ReadOnly, true, WriteType::FsMode),
            ] {
                let options = MountOptions::builder()
                    .set_properties(probe.options().add_properties)
                    .auto_cache(false)
                    .ttl_ms(0)
                    .access_mode(access)
                    .read_verify_ufs(verify)
                    .write_type(mode)
                    .build();
                let ufs = Path::from_str(format!(
                    "s3://{}-bucket/models",
                    root.trim_start_matches('/')
                ))?;
                fs.mount(&ufs, &Path::from_str(root)?, options).await?;
                let path = Path::from_str(format!("{root}/model"))?;
                cv.mkdir(&path, true).await?;
                cv.mkdir(&Path::from_str(format!("{root}/model/cached"))?, true)
                    .await?;
                let before = probe.count();
                if mode == WriteType::FsMode {
                    assert!(fs.get_status(&path).await?.id > 0);
                    assert_listings(&fs, &path, &["cached"], true).await?;
                    assert!(fs
                        .get_status(&Path::from_str(format!("{root}/missing"))?)
                        .await
                        .is_err());
                    assert_eq!(
                        probe.count(),
                        before,
                        "FM metadata stays CV-only, including misses"
                    );
                } else {
                    assert_eq!(fs.get_status(&path).await?.id, curvine_model::UFS_INODE_ID);
                    assert_listings(&fs, &path, &["remote.bin"], false).await?;
                    assert!(probe.count() > before, "verify/RW must keep UFS listings");
                }
            }

            // Do not turn rate limits or other UFS failures into an empty list.
            probe.failing.store(true, Ordering::SeqCst);
            let before = probe.count();
            assert!(fs.get_status(&expired).await.is_err());
            assert_listing_errors(&fs, &expired).await?;
            assert!(probe.count() > before);

            // A cold directory listing reuses the existing advisory load path.
            // Only this local mount enables auto-cache, so no background S3
            // requests can contaminate the zero-request assertion above.
            let local = format!("{base}/ufs");
            std::fs::create_dir_all(format!("{local}/cold"))?;
            std::fs::write(format!("{local}/cold/data.bin"), b"cached payload")?;
            let target = Path::from_str("/auto")?;
            fs.mount(
                &Path::from_str(format!("file://{local}"))?,
                &target,
                MountOptions::builder()
                    .replicas(1)
                    .block_size(1024 * 1024)
                    .auto_cache(true)
                    .build(),
            )
            .await?;
            let path = Path::from_str("/auto/cold")?;
            assert_names(fs.list_status(&path).await?, &["data.bin"]);
            let child = Path::from_str("/auto/cold/data.bin")?;
            let directory_source = Path::from_str(format!("file://{local}/cold"))?;
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    if let Ok(status) = cv.get_status(&child).await {
                        if status.is_complete && status.cv_valid(None) {
                            break;
                        }
                    }
                    if let Ok(job) = fs.get_job_status(&directory_source).await {
                        assert!(
                            job.state != JobTaskState::Failed,
                            "directory load failed: {}",
                            job.progress.message
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await?;
            std::fs::remove_dir_all(&local)?;
            assert_listings(&fs, &path, &["data.bin"], true).await?;
            assert!(fs.get_status(&path).await?.id > 0);
            Ok::<_, curvine_core_error::CommonError>(())
        })
        .await?
    })
}
