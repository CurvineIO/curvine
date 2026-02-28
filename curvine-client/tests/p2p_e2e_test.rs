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
