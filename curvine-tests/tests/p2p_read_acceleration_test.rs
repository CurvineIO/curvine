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
use orpc::common::Utils;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

async fn wait_bootstrap_addr(service: Arc<curvine_client::p2p::P2pService>) -> Option<String> {
    for _ in 0..400 {
        if let Some(addr) = service.bootstrap_peer_addr() {
            return Some(addr);
        }
        sleep(Duration::from_millis(25)).await;
    }
    None
}

async fn wait_policy_version(
    service: Arc<curvine_client::p2p::P2pService>,
    min_version: u64,
) -> bool {
    for _ in 0..200 {
        if service.runtime_policy_version() >= min_version {
            return true;
        }
        sleep(Duration::from_millis(25)).await;
    }
    false
}

async fn wait_policy_rejects(
    service: Arc<curvine_client::p2p::P2pService>,
    min_rejects: u64,
) -> bool {
    for _ in 0..200 {
        if service.snapshot().policy_rejects >= min_rejects {
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
    let provider_service = provider_fs
        .fs_context()
        .p2p_service()
        .expect("p2p should be enabled in this test");

    let bootstrap = rt
        .block_on(wait_bootstrap_addr(provider_service.clone()))
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
    let consumer_service = consumer_fs
        .fs_context()
        .p2p_service()
        .expect("p2p should be enabled in this test");

    let path = Path::from_str("/p2p/minicluster-e2e.log")?;
    rt.block_on(async move {
        let parent = Path::from_str("/p2p")?;
        let _ = provider_fs.delete(&parent, true).await;
        provider_fs.mkdir(&parent, true).await?;

        let payload = Utils::rand_str(256 * 1024);
        provider_fs.write_string(&path, payload.as_str()).await?;

        let provider_data = read_all(&provider_fs, &path).await?;
        assert_eq!(provider_data, payload.as_bytes());

        let provider_before = provider_service.snapshot();
        let consumer_data = read_all(&consumer_fs, &path).await?;
        let provider_after = provider_service.snapshot();
        let consumer_after = consumer_service.snapshot();

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
fn test_minicluster_get_p2p_runtime_policy_rpc() -> FsResult<()> {
    let rt = Arc::new(AsyncRuntime::single());
    let testing = Testing::builder()
        .workers(1)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.master.p2p_policy_version = 9;
            conf.master.p2p_policy_signing_key = "policy-secret".to_string();
            conf.master.p2p_policy_transition_signing_key = "policy-secret-old".to_string();
            conf.master.p2p_peer_whitelist =
                vec!["12D3KooWJ8v1tDU6yE2y9JAzobYjGg9VG7z6gK5yR2rKfZ9W8f2A".to_string()];
            conf.master.p2p_tenant_whitelist = vec!["tenant-a".to_string()];
        })
        .build()?;
    testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        let (version, peers, tenants, signature) = fs.get_p2p_runtime_policy().await?;
        assert_eq!(version, 9);
        assert_eq!(peers.len(), 1);
        assert_eq!(tenants, vec!["tenant-a".to_string()]);
        let signatures: Vec<&str> = signature.split(',').filter(|v| !v.is_empty()).collect();
        assert_eq!(signatures.len(), 2);
        Ok::<(), FsError>(())
    })?;

    Ok(())
}

#[test]
fn test_minicluster_p2p_runtime_policy_sync_from_master() -> FsResult<()> {
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
    let provider_service = provider_fs
        .fs_context()
        .p2p_service()
        .expect("p2p should be enabled in this test");

    let bootstrap = rt
        .block_on(wait_bootstrap_addr(provider_service.clone()))
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
        assert!(wait_policy_version(provider_service.clone(), 1).await);

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

        let allow_before = provider_service.snapshot().bytes_sent;
        let allow_data = read_all(&consumer_fs, &path_allow).await?;
        let allow_after = provider_service.snapshot().bytes_sent;
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

        let deny_before = provider_service.snapshot().bytes_sent;
        let deny_data = read_all(&consumer_fs, &path_deny).await?;
        let deny_after = provider_service.snapshot().bytes_sent;
        assert_eq!(deny_data, payload_deny.as_bytes());
        assert_eq!(deny_after, deny_before);

        Ok::<(), FsError>(())
    })?;

    Ok(())
}

#[test]
fn test_minicluster_p2p_runtime_policy_dual_key_rotation_window() -> FsResult<()> {
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
    let provider_service = provider_fs
        .fs_context()
        .p2p_service()
        .expect("p2p should be enabled in this test");

    let bootstrap = rt
        .block_on(wait_bootstrap_addr(provider_service.clone()))
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
        assert!(wait_policy_version(provider_service.clone(), 1).await);

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

        let before = provider_service.snapshot().bytes_sent;
        let data = read_all(&consumer_fs, &path).await?;
        let after = provider_service.snapshot().bytes_sent;
        assert_eq!(data, payload.as_bytes());
        assert!(after > before);

        Ok::<(), FsError>(())
    })?;

    Ok(())
}

#[test]
fn test_minicluster_p2p_runtime_policy_signature_mismatch_rejected() -> FsResult<()> {
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
    let provider_service = provider_fs
        .fs_context()
        .p2p_service()
        .expect("p2p should be enabled in this test");

    let bootstrap = rt
        .block_on(wait_bootstrap_addr(provider_service.clone()))
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
        assert!(!wait_policy_version(provider_service.clone(), 1).await);
        assert!(wait_policy_rejects(provider_service.clone(), 1).await);

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

        let baseline = provider_service.snapshot().bytes_sent;
        let data = read_all(&consumer_fs, &path).await?;
        let after = provider_service.snapshot().bytes_sent;
        assert_eq!(data, payload.as_bytes());
        assert_eq!(after, baseline);

        Ok::<(), FsError>(())
    })?;

    Ok(())
}

#[test]
fn test_minicluster_p2p_tenant_whitelist_enforced() -> FsResult<()> {
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
    provider_conf.client.p2p.tenant_whitelist = vec!["tenant-a".to_string()];
    provider_conf.client.p2p.cache_dir = format!(
        "../testing/curvine-tests/p2p-provider-cache-{}",
        Utils::uuid()
    );
    let provider_fs = testing.get_fs(Some(rt.clone()), Some(provider_conf))?;
    let provider_service = provider_fs
        .fs_context()
        .p2p_service()
        .expect("p2p should be enabled in this test");

    let bootstrap = rt
        .block_on(wait_bootstrap_addr(provider_service.clone()))
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
        let parent = Path::from_str("/p2p-tenant")?;
        let _ = provider_fs.delete(&parent, true).await;
        provider_fs.mkdir(&parent, true).await?;

        let path_allow = Path::from_str("/p2p-tenant/allow.log")?;
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

        let allow_before = provider_service.snapshot().bytes_sent;
        let allow_data = read_all(&consumer_fs, &path_allow).await?;
        let allow_after = provider_service.snapshot().bytes_sent;
        assert_eq!(allow_data, payload_allow.as_bytes());
        assert!(allow_after > allow_before);

        let path_deny = Path::from_str("/p2p-tenant/deny.log")?;
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

        let deny_before = provider_service.snapshot().bytes_sent;
        let deny_data = read_all(&consumer_fs, &path_deny).await?;
        let deny_after = provider_service.snapshot().bytes_sent;
        assert_eq!(deny_data, payload_deny.as_bytes());
        assert_eq!(deny_after, deny_before);

        Ok::<(), FsError>(())
    })?;

    Ok(())
}

#[test]
fn test_minicluster_p2p_tenant_whitelist_runtime_update() -> FsResult<()> {
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
    let provider_service = provider_fs
        .fs_context()
        .p2p_service()
        .expect("p2p should be enabled in this test");

    let bootstrap = rt
        .block_on(wait_bootstrap_addr(provider_service.clone()))
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
        let parent = Path::from_str("/p2p-tenant-runtime")?;
        let _ = provider_fs.delete(&parent, true).await;
        provider_fs.mkdir(&parent, true).await?;

        let path_before = Path::from_str("/p2p-tenant-runtime/before.log")?;
        let payload_before = Utils::rand_str(128 * 1024);
        let opts_before = provider_fs
            .create_opts_builder()
            .create_parent(true)
            .x_attr("tenant_id".to_string(), "tenant-a".as_bytes().to_vec())
            .build();
        let mut writer_before = provider_fs
            .create_with_opts(&path_before, opts_before, true)
            .await?;
        writer_before.write(payload_before.as_bytes()).await?;
        writer_before.complete().await?;
        let provider_before_data = read_all(&provider_fs, &path_before).await?;
        assert_eq!(provider_before_data, payload_before.as_bytes());

        let before_sent = provider_service.snapshot().bytes_sent;
        let before_read = read_all(&consumer_fs, &path_before).await?;
        let after_sent = provider_service.snapshot().bytes_sent;
        assert_eq!(before_read, payload_before.as_bytes());
        assert!(after_sent > before_sent);

        assert!(
            provider_service
                .update_runtime_policy(None, Some(vec!["tenant-b".to_string()]))
                .await
        );

        let path_denied = Path::from_str("/p2p-tenant-runtime/denied.log")?;
        let payload_denied = Utils::rand_str(128 * 1024);
        let opts_denied = provider_fs
            .create_opts_builder()
            .create_parent(true)
            .x_attr("tenant_id".to_string(), "tenant-a".as_bytes().to_vec())
            .build();
        let mut writer_denied = provider_fs
            .create_with_opts(&path_denied, opts_denied, true)
            .await?;
        writer_denied.write(payload_denied.as_bytes()).await?;
        writer_denied.complete().await?;
        let provider_denied_data = read_all(&provider_fs, &path_denied).await?;
        assert_eq!(provider_denied_data, payload_denied.as_bytes());

        let denied_before = provider_service.snapshot().bytes_sent;
        let denied_read = read_all(&consumer_fs, &path_denied).await?;
        let denied_after = provider_service.snapshot().bytes_sent;
        assert_eq!(denied_read, payload_denied.as_bytes());
        assert_eq!(denied_after, denied_before);

        let path_after = Path::from_str("/p2p-tenant-runtime/after.log")?;
        let payload_after = Utils::rand_str(128 * 1024);
        let opts_after = provider_fs
            .create_opts_builder()
            .create_parent(true)
            .x_attr("tenant_id".to_string(), "tenant-b".as_bytes().to_vec())
            .build();
        let mut writer_after = provider_fs
            .create_with_opts(&path_after, opts_after, true)
            .await?;
        writer_after.write(payload_after.as_bytes()).await?;
        writer_after.complete().await?;
        let provider_after_data = read_all(&provider_fs, &path_after).await?;
        assert_eq!(provider_after_data, payload_after.as_bytes());

        let allow_after_before = provider_service.snapshot().bytes_sent;
        let allow_after_read = read_all(&consumer_fs, &path_after).await?;
        let allow_after_after = provider_service.snapshot().bytes_sent;
        assert_eq!(allow_after_read, payload_after.as_bytes());
        assert!(allow_after_after > allow_after_before);

        Ok::<(), FsError>(())
    })?;

    Ok(())
}

#[test]
fn test_minicluster_p2p_miss_fallbacks_to_worker() -> FsResult<()> {
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
    let provider_service = provider_fs
        .fs_context()
        .p2p_service()
        .expect("p2p should be enabled in this test");

    let bootstrap = rt
        .block_on(wait_bootstrap_addr(provider_service.clone()))
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
    let consumer_service = consumer_fs
        .fs_context()
        .p2p_service()
        .expect("p2p should be enabled in this test");

    rt.block_on(async move {
        let path = Path::from_str("/p2p-fallback/miss-then-worker.log")?;
        let payload = Utils::rand_str(128 * 1024);
        provider_fs.write_string(&path, payload.as_str()).await?;

        let provider_before = provider_service.snapshot();
        let consumer_before = consumer_service.snapshot();
        let out = read_all(&consumer_fs, &path).await?;
        let provider_after = provider_service.snapshot();
        let consumer_after = consumer_service.snapshot();

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
    let provider_service = provider_fs
        .fs_context()
        .p2p_service()
        .expect("p2p should be enabled in this test");

    let bootstrap = rt
        .block_on(wait_bootstrap_addr(provider_service.clone()))
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

#[test]
fn test_minicluster_p2p_hit_succeeds_when_fallback_disabled() -> FsResult<()> {
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
    let provider_service = provider_fs
        .fs_context()
        .p2p_service()
        .expect("p2p should be enabled in this test");

    let bootstrap = rt
        .block_on(wait_bootstrap_addr(provider_service.clone()))
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
    let consumer_service = consumer_fs
        .fs_context()
        .p2p_service()
        .expect("p2p should be enabled in this test");

    rt.block_on(async move {
        let path = Path::from_str("/p2p-fallback/p2p-hit-only.log")?;
        let payload = Utils::rand_str(128 * 1024);
        provider_fs.write_string(&path, payload.as_str()).await?;

        let provider_warm = read_all(&provider_fs, &path).await?;
        assert_eq!(provider_warm, payload.as_bytes());

        let provider_before = provider_service.snapshot().bytes_sent;
        let consumer_before = consumer_service.snapshot();
        let out = read_all(&consumer_fs, &path).await?;
        let provider_after = provider_service.snapshot().bytes_sent;
        let consumer_after = consumer_service.snapshot();

        assert_eq!(out, payload.as_bytes());
        assert!(
            provider_after > provider_before,
            "provider should serve bytes through p2p when fallback is disabled"
        );
        assert!(
            consumer_after.bytes_recv > consumer_before.bytes_recv,
            "consumer should receive p2p bytes when fallback is disabled"
        );

        Ok::<(), FsError>(())
    })?;

    Ok(())
}

#[test]
fn test_minicluster_p2p_provider_disconnect_during_read_fallbacks_to_worker() -> FsResult<()> {
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
    let provider_service = provider_fs
        .fs_context()
        .p2p_service()
        .expect("p2p should be enabled in this test");

    let bootstrap = rt
        .block_on(wait_bootstrap_addr(provider_service.clone()))
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
        let path = Path::from_str("/p2p-fallback/disconnect-during-read.log")?;
        let payload = Utils::rand_str(8 * 1024 * 1024);
        provider_fs.write_string(&path, payload.as_str()).await?;

        let provider_warm = read_all(&provider_fs, &path).await?;
        assert_eq!(provider_warm, payload.as_bytes());

        let provider_for_stop = provider_service.clone();
        let read_task = tokio::spawn(async move { read_all(&consumer_fs, &path).await });
        sleep(Duration::from_millis(10)).await;
        provider_for_stop.stop();

        let out = read_task
            .await
            .expect("consumer read task should finish after provider stop")?;
        assert_eq!(out, payload.as_bytes());
        Ok::<(), FsError>(())
    })?;

    Ok(())
}
