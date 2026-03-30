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

use bytes::Bytes;
use curvine_client::p2p::{ChunkId, P2pService};
use curvine_common::conf::ClientP2pConf;
use once_cell::sync::Lazy;
use orpc::runtime::Runtime;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

fn test_runtime() -> Arc<Runtime> {
    static TEST_RT: Lazy<Arc<Runtime>> = Lazy::new(|| Arc::new(Runtime::new("p2p-e2e-test", 2, 2)));
    TEST_RT.clone()
}

fn test_cache_dir(case: &str) -> String {
    std::env::temp_dir()
        .join(format!(
            "curvine-p2p-e2e-{}-{}",
            case,
            orpc::common::Utils::uuid()
        ))
        .to_string_lossy()
        .to_string()
}

fn build_conf(cache_case: &str) -> ClientP2pConf {
    ClientP2pConf {
        enable: true,
        enable_mdns: false,
        enable_dht: false,
        request_timeout_ms: 300,
        discovery_timeout_ms: 300,
        connect_timeout_ms: 300,
        transfer_timeout_ms: 300,
        max_inflight_requests: 16,
        listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
        cache_dir: test_cache_dir(cache_case),
        ..ClientP2pConf::default()
    }
}

async fn wait_bootstrap_addr(service: &P2pService) -> Option<String> {
    for _ in 0..200 {
        if let Some(addr) = service.bootstrap_peer_addr() {
            return Some(addr);
        }
        sleep(Duration::from_millis(25)).await;
    }
    None
}

async fn wait_fetch(
    service: &P2pService,
    chunk: ChunkId,
    max_len: usize,
    expected_mtime: Option<i64>,
) -> Option<Bytes> {
    for _ in 0..200 {
        if let Some(data) = service.fetch_chunk(chunk, max_len, expected_mtime).await {
            return Some(data);
        }
        sleep(Duration::from_millis(25)).await;
    }
    None
}

async fn wait_publish(service: &P2pService, chunk: ChunkId, data: Bytes, mtime: i64) -> bool {
    for _ in 0..400 {
        if service.publish_chunk(chunk, data.clone(), mtime) {
            return true;
        }
        sleep(Duration::from_millis(2)).await;
    }
    false
}

fn profile_chunk_count() -> usize {
    std::env::var("P2P_PROFILE_CHUNKS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(128)
}

fn profile_chunk_size() -> usize {
    std::env::var("P2P_PROFILE_CHUNK_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(262_144)
}

async fn wait_cached_chunks(service: &P2pService, expected: usize) -> bool {
    for _ in 0..2000 {
        if service.snapshot().cached_chunks_count >= expected {
            return true;
        }
        sleep(Duration::from_millis(10)).await;
    }
    false
}

#[tokio::test]
async fn e2e_network_fetch_then_local_cache_reuse() {
    let provider = P2pService::new_with_runtime(build_conf("provider-reuse"), Some(test_runtime()));
    provider.start();

    let bootstrap = wait_bootstrap_addr(&provider)
        .await
        .expect("provider bootstrap address should be ready");

    let mut consumer_conf = build_conf("consumer-reuse");
    consumer_conf.bootstrap_peers = vec![bootstrap];
    let consumer = P2pService::new_with_runtime(consumer_conf, Some(test_runtime()));
    consumer.start();

    let chunk = ChunkId::new(101, 0);
    assert!(provider.publish_chunk(chunk, Bytes::from_static(b"e2e-data"), 5000));
    let first = wait_fetch(&consumer, chunk, 1024, Some(5000)).await;
    assert_eq!(first, Some(Bytes::from_static(b"e2e-data")));

    provider.stop();
    let second = consumer.fetch_chunk(chunk, 1024, Some(5000)).await;
    let snapshot = consumer.snapshot();

    assert_eq!(second, Some(Bytes::from_static(b"e2e-data")));
    assert!(snapshot.cached_chunks_count >= 1);
    assert!(snapshot.cache_usage_bytes >= 8);
}

#[tokio::test]
async fn e2e_mtime_mismatch_is_detected() {
    let provider = P2pService::new_with_runtime(build_conf("provider-mtime"), Some(test_runtime()));
    provider.start();

    let bootstrap = wait_bootstrap_addr(&provider)
        .await
        .expect("provider bootstrap address should be ready");

    let mut consumer_conf = build_conf("consumer-mtime");
    consumer_conf.bootstrap_peers = vec![bootstrap];
    let consumer = P2pService::new_with_runtime(consumer_conf, Some(test_runtime()));
    consumer.start();

    let chunk = ChunkId::new(202, 0);
    assert!(provider.publish_chunk(chunk, Bytes::from_static(b"mtime-data"), 7000));
    let ok = wait_fetch(&consumer, chunk, 1024, Some(7000)).await;
    assert_eq!(ok, Some(Bytes::from_static(b"mtime-data")));

    let mismatch = consumer.fetch_chunk(chunk, 1024, Some(7001)).await;
    let snapshot = consumer.snapshot();

    assert!(mismatch.is_none());
    assert!(snapshot.mtime_mismatches >= 1);
}

#[tokio::test]
async fn e2e_negative_provider_cache_still_uses_connected_peer() {
    let mut provider_conf = build_conf("provider-negative-cache");
    provider_conf.enable_dht = true;
    let provider = P2pService::new_with_runtime(provider_conf, Some(test_runtime()));
    provider.start();

    let bootstrap = wait_bootstrap_addr(&provider)
        .await
        .expect("provider bootstrap address should be ready");

    let mut consumer_conf = build_conf("consumer-negative-cache");
    consumer_conf.enable_dht = true;
    consumer_conf.bootstrap_peers = vec![bootstrap];
    let consumer = P2pService::new_with_runtime(consumer_conf, Some(test_runtime()));
    consumer.start();

    let chunk = ChunkId::new(303, 0);
    let first = consumer.fetch_chunk(chunk, 1024, Some(9000)).await;
    assert!(first.is_none());

    assert!(provider.publish_chunk(chunk, Bytes::from_static(b"late-publish"), 9000));
    let second = wait_fetch(&consumer, chunk, 1024, Some(9000)).await;

    assert_eq!(second, Some(Bytes::from_static(b"late-publish")));
}

#[tokio::test]
async fn e2e_republish_cached_chunk_makes_new_provider_discoverable() {
    let mut seed_conf = build_conf("seed-republish");
    seed_conf.enable_dht = true;
    let seed = P2pService::new_with_runtime(seed_conf, Some(test_runtime()));
    seed.start();

    let bootstrap = wait_bootstrap_addr(&seed)
        .await
        .expect("seed bootstrap address should be ready");

    let mut provider_conf = build_conf("provider-republish");
    provider_conf.enable_dht = true;
    provider_conf.bootstrap_peers = vec![bootstrap.clone()];
    let provider = P2pService::new_with_runtime(provider_conf, Some(test_runtime()));
    provider.start();

    let mut republisher_conf = build_conf("republisher-republish");
    republisher_conf.enable_dht = true;
    republisher_conf.bootstrap_peers = vec![bootstrap.clone()];
    let republisher = P2pService::new_with_runtime(republisher_conf, Some(test_runtime()));
    republisher.start();

    let mut consumer_conf = build_conf("consumer-republish");
    consumer_conf.enable_dht = true;
    consumer_conf.bootstrap_peers = vec![bootstrap];
    let consumer = P2pService::new_with_runtime(consumer_conf, Some(test_runtime()));
    consumer.start();

    let chunk = ChunkId::new(404, 0);
    let data = Bytes::from_static(b"republish-data");
    assert!(wait_publish(&provider, chunk, data.clone(), 9_100).await);

    let fetched = wait_fetch(&republisher, chunk, 1024, Some(9_100)).await;
    assert_eq!(fetched, Some(data.clone()));

    provider.stop();
    sleep(Duration::from_millis(200)).await;

    assert!(wait_publish(&republisher, chunk, data.clone(), 9_100).await);
    let rediscovered = wait_fetch(&consumer, chunk, 1024, Some(9_100)).await;

    assert_eq!(rediscovered, Some(data));
}

#[tokio::test]
#[ignore]
async fn profile_bulk_network_fetch() {
    let mut provider_conf = build_conf("provider-profile");
    provider_conf.enable_dht = true;
    let provider = P2pService::new_with_runtime(provider_conf, Some(test_runtime()));
    provider.start();

    let bootstrap = wait_bootstrap_addr(&provider)
        .await
        .expect("provider bootstrap address should be ready");

    let mut consumer_conf = build_conf("consumer-profile");
    consumer_conf.enable_dht = true;
    consumer_conf.bootstrap_peers = vec![bootstrap];
    let consumer = P2pService::new_with_runtime(consumer_conf, Some(test_runtime()));
    consumer.start();

    let chunk_count = profile_chunk_count();
    let chunk_size = profile_chunk_size();
    let payload = Bytes::from(vec![7u8; chunk_size]);

    for idx in 0..chunk_count {
        let chunk = ChunkId::new(800_000 + idx as i64, 0);
        assert!(wait_publish(&provider, chunk, payload.clone(), 9_000).await);
    }
    assert!(wait_cached_chunks(&provider, chunk_count).await);

    let mut total = 0usize;
    for idx in 0..chunk_count {
        let chunk = ChunkId::new(800_000 + idx as i64, 0);
        let data = wait_fetch(&consumer, chunk, chunk_size, Some(9_000))
            .await
            .expect("profile chunk should be fetched");
        total = total.saturating_add(data.len());
    }

    assert_eq!(total, chunk_count.saturating_mul(chunk_size));
}
