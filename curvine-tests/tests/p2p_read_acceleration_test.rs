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

use bytes::BytesMut;
use curvine_common::error::FsError;
use curvine_common::fs::{Path, Reader, Writer};
use curvine_common::FsResult;
use curvine_tests::Testing;
use once_cell::sync::Lazy;
use orpc::common::Utils;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use std::sync::Arc;
use std::sync::{Mutex, MutexGuard};
use tokio::time::{sleep, Duration};

static P2P_TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

fn p2p_test_guard() -> MutexGuard<'static, ()> {
    P2P_TEST_LOCK.lock().unwrap()
}

async fn wait_bootstrap_addr(fs: &curvine_client::file::CurvineFileSystem) -> Option<String> {
    for _ in 0..400 {
        if let Some(addr) = fs.p2p_bootstrap_peer_addr() {
            return Some(addr);
        }
        sleep(Duration::from_millis(25)).await;
    }
    None
}

async fn wait_policy_version(
    fs: &curvine_client::file::CurvineFileSystem,
    min_version: u64,
) -> bool {
    for _ in 0..200 {
        if fs.p2p_runtime_policy_version().unwrap_or_default() >= min_version {
            return true;
        }
        sleep(Duration::from_millis(25)).await;
    }
    false
}

async fn wait_policy_version_with_deadline(
    fs: &curvine_client::file::CurvineFileSystem,
    min_version: u64,
    deadline: Duration,
) -> bool {
    let started = tokio::time::Instant::now();
    while started.elapsed() < deadline {
        if fs.p2p_runtime_policy_version().unwrap_or_default() >= min_version {
            return true;
        }
        sleep(Duration::from_millis(25)).await;
    }
    false
}

async fn wait_policy_rejects(
    fs: &curvine_client::file::CurvineFileSystem,
    min_rejects: u64,
) -> bool {
    for _ in 0..200 {
        if fs
            .p2p_stats_snapshot()
            .is_some_and(|snapshot| snapshot.policy_rejects >= min_rejects)
        {
            return true;
        }
        sleep(Duration::from_millis(25)).await;
    }
    false
}

async fn read_all(fs: &curvine_client::file::CurvineFileSystem, path: &Path) -> FsResult<Vec<u8>> {
    let status = fs.get_status(path).await?;
    let mut reader = fs.open(path).await?;
    let mut buf = BytesMut::zeroed(status.len as usize);
    let size = reader.read_full(&mut buf).await?;
    reader.complete().await?;
    buf.truncate(size);
    Ok(buf.to_vec())
}

#[test]
fn test_minicluster_p2p_read_acceleration() -> FsResult<()> {
    let _guard = p2p_test_guard();
    let rt = Arc::new(AsyncRuntime::single());
    let testing = Testing::builder()
        .workers(2)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .build()?;
    testing.start_cluster()?;
    let base_conf = testing.get_active_cluster_conf()?;

    let mut provider_conf = base_conf.clone();
    provider_conf.client.p2p.enable = true;
    provider_conf.client.p2p.enable_mdns = false;
    provider_conf.client.p2p.enable_dht = false;
    provider_conf.client.p2p.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
    provider_conf.client.p2p.bootstrap_peers = vec![];
    provider_conf.client.p2p.cache_dir = format!(
        "../testing/curvine-tests/p2p-provider-cache-{}",
        Utils::uuid()
    );
    let provider_fs = testing.get_fs(Some(rt.clone()), Some(provider_conf))?;

    let bootstrap = rt
        .block_on(wait_bootstrap_addr(&provider_fs))
        .expect("provider bootstrap address should be ready");

    let mut consumer_conf = base_conf;
    consumer_conf.client.p2p.enable = true;
    consumer_conf.client.p2p.enable_mdns = false;
    consumer_conf.client.p2p.enable_dht = false;
    consumer_conf.client.p2p.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
    consumer_conf.client.p2p.bootstrap_peers = vec![bootstrap];
    consumer_conf.client.p2p.cache_dir = format!(
        "../testing/curvine-tests/p2p-consumer-cache-{}",
        Utils::uuid()
    );
    let consumer_fs = testing.get_fs(Some(rt.clone()), Some(consumer_conf))?;

    let path = Path::from_str("/p2p/minicluster-e2e.log")?;
    rt.block_on(async move {
        let parent = Path::from_str("/p2p")?;
        let _ = provider_fs.delete(&parent, true).await;
        provider_fs.mkdir(&parent, true).await?;

        let payload = Utils::rand_str(256 * 1024);
        provider_fs.write_string(&path, payload.as_str()).await?;

        let provider_data = read_all(&provider_fs, &path).await?;
        assert_eq!(provider_data, payload.as_bytes());

        let provider_before = provider_fs
            .p2p_stats_snapshot()
            .expect("p2p should be enabled in this test");
        let consumer_data = read_all(&consumer_fs, &path).await?;
        let provider_after = provider_fs
            .p2p_stats_snapshot()
            .expect("p2p should be enabled in this test");
        let consumer_after = consumer_fs
            .p2p_stats_snapshot()
            .expect("p2p should be enabled in this test");

        if consumer_data != payload.as_bytes() {
            let expected = payload.as_bytes();
            let first_diff = consumer_data
                .iter()
                .zip(expected.iter())
                .position(|(left, right)| left != right);
            let left_head_len = consumer_data.len().min(64);
            let right_head_len = expected.len().min(64);
            panic!(
                "consumer payload mismatch: left_len={}, right_len={}, first_diff={:?}, left_head={:?}, right_head={:?}",
                consumer_data.len(),
                expected.len(),
                first_diff,
                &consumer_data[..left_head_len],
                &expected[..right_head_len]
            );
        }
        assert!(provider_after.bytes_sent > provider_before.bytes_sent);
        assert!(consumer_after.bytes_recv > 0);
        assert!(consumer_after.cached_chunks_count > 0);

        Ok::<(), FsError>(())
    })?;

    Ok(())
}

#[test]
fn test_minicluster_p2p_runtime_policy_sync_from_master() -> FsResult<()> {
    let _guard = p2p_test_guard();
    let rt = Arc::new(AsyncRuntime::single());
    let testing = Testing::builder()
        .workers(2)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.master.p2p_policy_version = 1;
            conf.master.p2p_policy_signing_key = "policy-secret".to_string();
            conf.master.p2p_tenant_whitelist = vec!["tenant-a".to_string()];
            conf.master.p2p_peer_whitelist = vec![];
        })
        .build()?;
    testing.start_cluster()?;
    let base_conf = testing.get_active_cluster_conf()?;

    let mut provider_conf = base_conf.clone();
    provider_conf.client.p2p.enable = true;
    provider_conf.client.p2p.enable_mdns = false;
    provider_conf.client.p2p.enable_dht = false;
    provider_conf.client.p2p.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
    provider_conf.client.p2p.bootstrap_peers = vec![];
    provider_conf.client.p2p.tenant_whitelist = vec!["tenant-b".to_string()];
    provider_conf.client.p2p.policy_hmac_key = "policy-secret".to_string();
    provider_conf.client.clean_task_interval = Duration::from_millis(100);
    provider_conf.client.clean_task_interval_str = "100ms".to_string();
    provider_conf.client.p2p.cache_dir = format!(
        "../testing/curvine-tests/p2p-provider-cache-{}",
        Utils::uuid()
    );
    let provider_fs = testing.get_fs(Some(rt.clone()), Some(provider_conf))?;

    let bootstrap = rt
        .block_on(wait_bootstrap_addr(&provider_fs))
        .expect("provider bootstrap address should be ready");

    let mut consumer_conf = base_conf;
    consumer_conf.client.p2p.enable = true;
    consumer_conf.client.p2p.enable_mdns = false;
    consumer_conf.client.p2p.enable_dht = false;
    consumer_conf.client.p2p.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
    consumer_conf.client.p2p.bootstrap_peers = vec![bootstrap];
    consumer_conf.client.p2p.policy_hmac_key = "policy-secret".to_string();
    consumer_conf.client.clean_task_interval = Duration::from_millis(100);
    consumer_conf.client.clean_task_interval_str = "100ms".to_string();
    consumer_conf.client.p2p.cache_dir = format!(
        "../testing/curvine-tests/p2p-consumer-cache-{}",
        Utils::uuid()
    );
    let consumer_fs = testing.get_fs(Some(rt.clone()), Some(consumer_conf))?;

    rt.block_on(async move {
        assert!(wait_policy_version(&provider_fs, 1).await);

        let parent = Path::from_str("/p2p-policy-sync")?;
        let _ = provider_fs.delete(&parent, true).await;
        provider_fs.mkdir(&parent, true).await?;

        let path_allow = Path::from_str("/p2p-policy-sync/allow.log")?;
        let payload_allow = Utils::rand_str(128 * 1024);
        let opts_allow = provider_fs
            .create_opts_builder()
            .create_parent(true)
            .x_attr("tenant_id".to_string(), "tenant-a".as_bytes().to_vec())
            .build();
        let mut writer_allow = provider_fs
            .create_with_opts(&path_allow, opts_allow, true)
            .await?;
        writer_allow.write(payload_allow.as_bytes()).await?;
        writer_allow.complete().await?;
        let provider_allow = read_all(&provider_fs, &path_allow).await?;
        assert_eq!(provider_allow, payload_allow.as_bytes());

        let allow_before = provider_fs
            .p2p_stats_snapshot()
            .expect("p2p should be enabled in this test")
            .bytes_sent;
        let allow_data = read_all(&consumer_fs, &path_allow).await?;
        let allow_after = provider_fs
            .p2p_stats_snapshot()
            .expect("p2p should be enabled in this test")
            .bytes_sent;
        assert_eq!(allow_data, payload_allow.as_bytes());
        assert!(allow_after > allow_before);

        let path_deny = Path::from_str("/p2p-policy-sync/deny.log")?;
        let payload_deny = Utils::rand_str(128 * 1024);
        let opts_deny = provider_fs
            .create_opts_builder()
            .create_parent(true)
            .x_attr("tenant_id".to_string(), "tenant-b".as_bytes().to_vec())
            .build();
        let mut writer_deny = provider_fs
            .create_with_opts(&path_deny, opts_deny, true)
            .await?;
        writer_deny.write(payload_deny.as_bytes()).await?;
        writer_deny.complete().await?;
        let provider_deny = read_all(&provider_fs, &path_deny).await?;
        assert_eq!(provider_deny, payload_deny.as_bytes());

        let deny_before = provider_fs
            .p2p_stats_snapshot()
            .expect("p2p should be enabled in this test")
            .bytes_sent;
        let deny_data = read_all(&consumer_fs, &path_deny).await?;
        let deny_after = provider_fs
            .p2p_stats_snapshot()
            .expect("p2p should be enabled in this test")
            .bytes_sent;
        assert_eq!(deny_data, payload_deny.as_bytes());
        assert_eq!(deny_after, deny_before);

        Ok::<(), FsError>(())
    })?;

    Ok(())
}

#[test]
fn test_minicluster_p2p_runtime_policy_sync_applies_on_startup() -> FsResult<()> {
    let _guard = p2p_test_guard();
    let rt = Arc::new(AsyncRuntime::single());
    let testing = Testing::builder()
        .workers(2)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.master.p2p_policy_version = 1;
            conf.master.p2p_policy_signing_key = "policy-secret".to_string();
            conf.master.p2p_tenant_whitelist = vec!["tenant-a".to_string()];
            conf.master.p2p_peer_whitelist = vec![];
        })
        .build()?;
    testing.start_cluster()?;
    let base_conf = testing.get_active_cluster_conf()?;

    let mut provider_conf = base_conf;
    provider_conf.client.p2p.enable = true;
    provider_conf.client.p2p.enable_mdns = false;
    provider_conf.client.p2p.enable_dht = false;
    provider_conf.client.p2p.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
    provider_conf.client.p2p.bootstrap_peers = vec![];
    provider_conf.client.p2p.tenant_whitelist = vec!["tenant-b".to_string()];
    provider_conf.client.p2p.policy_hmac_key = "policy-secret".to_string();
    provider_conf.client.clean_task_interval = Duration::from_secs(5);
    provider_conf.client.clean_task_interval_str = "5s".to_string();
    provider_conf.client.p2p.cache_dir = format!(
        "../testing/curvine-tests/p2p-provider-startup-policy-cache-{}",
        Utils::uuid()
    );
    let provider_fs = testing.get_fs(Some(rt.clone()), Some(provider_conf))?;

    rt.block_on(async move {
        assert!(
            wait_policy_version_with_deadline(&provider_fs, 1, Duration::from_millis(800)).await
        );
        Ok::<(), FsError>(())
    })?;

    Ok(())
}

#[test]
fn test_minicluster_p2p_runtime_policy_dual_key_rotation_window() -> FsResult<()> {
    let _guard = p2p_test_guard();
    let rt = Arc::new(AsyncRuntime::single());
    let testing = Testing::builder()
        .workers(2)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.master.p2p_policy_version = 1;
            conf.master.p2p_policy_signing_key = "new-secret".to_string();
            conf.master.p2p_policy_transition_signing_key = "old-secret".to_string();
            conf.master.p2p_tenant_whitelist = vec!["tenant-a".to_string()];
            conf.master.p2p_peer_whitelist = vec![];
        })
        .build()?;
    testing.start_cluster()?;
    let base_conf = testing.get_active_cluster_conf()?;

    let mut provider_conf = base_conf.clone();
    provider_conf.client.p2p.enable = true;
    provider_conf.client.p2p.enable_mdns = false;
    provider_conf.client.p2p.enable_dht = false;
    provider_conf.client.p2p.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
    provider_conf.client.p2p.bootstrap_peers = vec![];
    provider_conf.client.p2p.tenant_whitelist = vec!["tenant-b".to_string()];
    provider_conf.client.p2p.policy_hmac_key = "old-secret".to_string();
    provider_conf.client.clean_task_interval = Duration::from_millis(100);
    provider_conf.client.clean_task_interval_str = "100ms".to_string();
    provider_conf.client.p2p.cache_dir = format!(
        "../testing/curvine-tests/p2p-provider-rotation-cache-{}",
        Utils::uuid()
    );
    let provider_fs = testing.get_fs(Some(rt.clone()), Some(provider_conf))?;

    let bootstrap = rt
        .block_on(wait_bootstrap_addr(&provider_fs))
        .expect("provider bootstrap address should be ready");

    let mut consumer_conf = base_conf;
    consumer_conf.client.p2p.enable = true;
    consumer_conf.client.p2p.enable_mdns = false;
    consumer_conf.client.p2p.enable_dht = false;
    consumer_conf.client.p2p.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
    consumer_conf.client.p2p.bootstrap_peers = vec![bootstrap];
    consumer_conf.client.p2p.policy_hmac_key = "old-secret".to_string();
    consumer_conf.client.clean_task_interval = Duration::from_millis(100);
    consumer_conf.client.clean_task_interval_str = "100ms".to_string();
    consumer_conf.client.p2p.cache_dir = format!(
        "../testing/curvine-tests/p2p-consumer-rotation-cache-{}",
        Utils::uuid()
    );
    let consumer_fs = testing.get_fs(Some(rt.clone()), Some(consumer_conf))?;

    rt.block_on(async move {
        assert!(wait_policy_version(&provider_fs, 1).await);

        let parent = Path::from_str("/p2p-policy-rotation-window")?;
        let _ = provider_fs.delete(&parent, true).await;
        provider_fs.mkdir(&parent, true).await?;

        let path = Path::from_str("/p2p-policy-rotation-window/allow.log")?;
        let payload = Utils::rand_str(128 * 1024);
        let opts = provider_fs
            .create_opts_builder()
            .create_parent(true)
            .x_attr("tenant_id".to_string(), "tenant-a".as_bytes().to_vec())
            .build();
        let mut writer = provider_fs.create_with_opts(&path, opts, true).await?;
        writer.write(payload.as_bytes()).await?;
        writer.complete().await?;
        let provider_data = read_all(&provider_fs, &path).await?;
        assert_eq!(provider_data, payload.as_bytes());

        let before = provider_fs
            .p2p_stats_snapshot()
            .expect("p2p should be enabled in this test")
            .bytes_sent;
        let data = read_all(&consumer_fs, &path).await?;
        let after = provider_fs
            .p2p_stats_snapshot()
            .expect("p2p should be enabled in this test")
            .bytes_sent;
        assert_eq!(data, payload.as_bytes());
        assert!(after > before);

        Ok::<(), FsError>(())
    })?;

    Ok(())
}

#[test]
fn test_minicluster_p2p_runtime_policy_signature_mismatch_rejected() -> FsResult<()> {
    let _guard = p2p_test_guard();
    let rt = Arc::new(AsyncRuntime::single());
    let testing = Testing::builder()
        .workers(2)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.master.p2p_policy_version = 1;
            conf.master.p2p_policy_signing_key = "policy-secret".to_string();
            conf.master.p2p_tenant_whitelist = vec!["tenant-a".to_string()];
            conf.master.p2p_peer_whitelist = vec![];
        })
        .build()?;
    testing.start_cluster()?;
    let base_conf = testing.get_active_cluster_conf()?;

    let mut provider_conf = base_conf.clone();
    provider_conf.client.p2p.enable = true;
    provider_conf.client.p2p.enable_mdns = false;
    provider_conf.client.p2p.enable_dht = false;
    provider_conf.client.p2p.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
    provider_conf.client.p2p.bootstrap_peers = vec![];
    provider_conf.client.p2p.tenant_whitelist = vec!["tenant-b".to_string()];
    provider_conf.client.p2p.policy_hmac_key = "wrong-secret".to_string();
    provider_conf.client.clean_task_interval = Duration::from_millis(100);
    provider_conf.client.clean_task_interval_str = "100ms".to_string();
    provider_conf.client.p2p.cache_dir = format!(
        "../testing/curvine-tests/p2p-provider-cache-{}",
        Utils::uuid()
    );
    let provider_fs = testing.get_fs(Some(rt.clone()), Some(provider_conf))?;

    let bootstrap = rt
        .block_on(wait_bootstrap_addr(&provider_fs))
        .expect("provider bootstrap address should be ready");

    let mut consumer_conf = base_conf;
    consumer_conf.client.p2p.enable = true;
    consumer_conf.client.p2p.enable_mdns = false;
    consumer_conf.client.p2p.enable_dht = false;
    consumer_conf.client.p2p.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
    consumer_conf.client.p2p.bootstrap_peers = vec![bootstrap];
    consumer_conf.client.p2p.policy_hmac_key = "wrong-secret".to_string();
    consumer_conf.client.clean_task_interval = Duration::from_millis(100);
    consumer_conf.client.clean_task_interval_str = "100ms".to_string();
    consumer_conf.client.p2p.cache_dir = format!(
        "../testing/curvine-tests/p2p-consumer-cache-{}",
        Utils::uuid()
    );
    let consumer_fs = testing.get_fs(Some(rt.clone()), Some(consumer_conf))?;

    rt.block_on(async move {
        assert!(!wait_policy_version(&provider_fs, 1).await);
        assert!(wait_policy_rejects(&provider_fs, 1).await);

        let parent = Path::from_str("/p2p-policy-signature-reject")?;
        let _ = provider_fs.delete(&parent, true).await;
        provider_fs.mkdir(&parent, true).await?;

        let path = Path::from_str("/p2p-policy-signature-reject/deny.log")?;
        let payload = Utils::rand_str(128 * 1024);
        let opts = provider_fs
            .create_opts_builder()
            .create_parent(true)
            .x_attr("tenant_id".to_string(), "tenant-a".as_bytes().to_vec())
            .build();
        let mut writer = provider_fs.create_with_opts(&path, opts, true).await?;
        writer.write(payload.as_bytes()).await?;
        writer.complete().await?;

        let baseline = provider_fs
            .p2p_stats_snapshot()
            .expect("p2p should be enabled in this test")
            .bytes_sent;
        let data = read_all(&consumer_fs, &path).await?;
        let after = provider_fs
            .p2p_stats_snapshot()
            .expect("p2p should be enabled in this test")
            .bytes_sent;
        assert_eq!(data, payload.as_bytes());
        assert_eq!(after, baseline);

        Ok::<(), FsError>(())
    })?;

    Ok(())
}

#[test]
fn test_minicluster_p2p_miss_fallbacks_to_worker() -> FsResult<()> {
    let _guard = p2p_test_guard();
    let rt = Arc::new(AsyncRuntime::single());
    let testing = Testing::builder()
        .workers(2)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .build()?;
    testing.start_cluster()?;
    let base_conf = testing.get_active_cluster_conf()?;

    let mut provider_conf = base_conf.clone();
    provider_conf.client.p2p.enable = true;
    provider_conf.client.p2p.enable_mdns = false;
    provider_conf.client.p2p.enable_dht = false;
    provider_conf.client.p2p.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
    provider_conf.client.p2p.bootstrap_peers = vec![];
    provider_conf.client.p2p.cache_dir = format!(
        "../testing/curvine-tests/p2p-provider-cache-{}",
        Utils::uuid()
    );
    let provider_fs = testing.get_fs(Some(rt.clone()), Some(provider_conf))?;

    let bootstrap = rt
        .block_on(wait_bootstrap_addr(&provider_fs))
        .expect("provider bootstrap address should be ready");

    let mut consumer_conf = base_conf;
    consumer_conf.client.p2p.enable = true;
    consumer_conf.client.p2p.enable_mdns = false;
    consumer_conf.client.p2p.enable_dht = false;
    consumer_conf.client.p2p.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
    consumer_conf.client.p2p.bootstrap_peers = vec![bootstrap];
    consumer_conf.client.p2p.cache_dir = format!(
        "../testing/curvine-tests/p2p-consumer-cache-{}",
        Utils::uuid()
    );
    let consumer_fs = testing.get_fs(Some(rt.clone()), Some(consumer_conf))?;

    rt.block_on(async move {
        let path = Path::from_str("/p2p-fallback/miss-then-worker.log")?;
        let payload = Utils::rand_str(128 * 1024);
        provider_fs.write_string(&path, payload.as_str()).await?;

        let provider_before = provider_fs
            .p2p_stats_snapshot()
            .expect("p2p should be enabled in this test");
        let consumer_before = consumer_fs
            .p2p_stats_snapshot()
            .expect("p2p should be enabled in this test");
        let out = read_all(&consumer_fs, &path).await?;
        let provider_after = provider_fs
            .p2p_stats_snapshot()
            .expect("p2p should be enabled in this test");
        let consumer_after = consumer_fs
            .p2p_stats_snapshot()
            .expect("p2p should be enabled in this test");

        assert_eq!(out, payload.as_bytes());
        assert_eq!(
            provider_after.bytes_sent, provider_before.bytes_sent,
            "p2p miss should fallback to worker without provider p2p transfer"
        );
        assert_eq!(
            consumer_after.bytes_recv, consumer_before.bytes_recv,
            "consumer should not receive p2p bytes on fallback-worker path"
        );
        Ok::<(), FsError>(())
    })?;

    Ok(())
}

#[test]
fn test_minicluster_p2p_miss_fails_when_fallback_disabled() -> FsResult<()> {
    let _guard = p2p_test_guard();
    let rt = Arc::new(AsyncRuntime::single());
    let testing = Testing::builder()
        .workers(2)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .build()?;
    testing.start_cluster()?;
    let base_conf = testing.get_active_cluster_conf()?;

    let mut provider_conf = base_conf.clone();
    provider_conf.client.p2p.enable = true;
    provider_conf.client.p2p.enable_mdns = false;
    provider_conf.client.p2p.enable_dht = false;
    provider_conf.client.p2p.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
    provider_conf.client.p2p.bootstrap_peers = vec![];
    provider_conf.client.p2p.cache_dir = format!(
        "../testing/curvine-tests/p2p-provider-cache-{}",
        Utils::uuid()
    );
    let provider_fs = testing.get_fs(Some(rt.clone()), Some(provider_conf))?;

    let bootstrap = rt
        .block_on(wait_bootstrap_addr(&provider_fs))
        .expect("provider bootstrap address should be ready");

    let mut consumer_conf = base_conf;
    consumer_conf.client.p2p.enable = true;
    consumer_conf.client.p2p.enable_mdns = false;
    consumer_conf.client.p2p.enable_dht = false;
    consumer_conf.client.p2p.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
    consumer_conf.client.p2p.bootstrap_peers = vec![bootstrap];
    consumer_conf.client.p2p.fallback_worker_on_fail = false;
    consumer_conf.client.p2p.cache_dir = format!(
        "../testing/curvine-tests/p2p-consumer-cache-{}",
        Utils::uuid()
    );
    let consumer_fs = testing.get_fs(Some(rt.clone()), Some(consumer_conf))?;

    rt.block_on(async move {
        let path = Path::from_str("/p2p-fallback/miss-no-worker.log")?;
        let payload = Utils::rand_str(128 * 1024);
        provider_fs.write_string(&path, payload.as_str()).await?;

        let err = read_all(&consumer_fs, &path)
            .await
            .expect_err("p2p miss should fail when worker fallback is disabled");
        let err_msg = err.to_string();
        let err_msg_lower = err_msg.to_lowercase();
        assert!(
            err_msg_lower.contains("fallback")
                || err_msg_lower.contains("p2p read miss")
                || err_msg_lower.contains("buffer read"),
            "unexpected error message: {}",
            err_msg
        );
        Ok::<(), FsError>(())
    })?;

    Ok(())
}
