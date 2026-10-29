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

//! Regression for issue #1815: a blackholed object-store path must not leave a
//! foreground UFS operation without a terminal state.
//!
//! The endpoint below accepts TCP connections and then never answers, which is
//! what the reported `iptables DROP` rule looks like to the client: the socket
//! is established, so the failure is a hang rather than a refused connection.
//! Before `opendal.operation_timeout`, every call here waited for the inner
//! `TimeoutLayer` (60 s non-IO, 150 s IO) times the number of retries.
//!
//! Run with: `cargo test -p curvine-ufs-opendal --features opendal-s3`

use curvine_error::FsError;
use curvine_fs_api::{FileSystem, Path};
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_ufs_api::OpendalConf;
use curvine_ufs_opendal::OpendalFileSystem;
use std::collections::HashMap;
use std::net::TcpListener;
use std::time::{Duration, Instant};

/// The per-operation budget used by these tests.
const BUDGET: Duration = Duration::from_millis(300);

/// A ceiling for the whole assertion: far above `BUDGET`, far below the
/// smallest timeout the inner layers would produce on their own (60 s).
const PATIENCE: Duration = Duration::from_secs(5);

/// Bind a listener that accepts connections and never writes a response.
///
/// The accept loop lives on its own thread and keeps the accepted sockets open
/// for the duration of the test process, so the client stays blocked in a read.
fn stalled_endpoint() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a local stalled endpoint");
    let port = listener.local_addr().unwrap().port();

    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => held.push(stream),
                Err(_) => break,
            }
        }
    });

    port
}

fn conf(port: u16) -> HashMap<String, String> {
    HashMap::from([
        (
            "s3.endpoint_url".to_string(),
            format!("http://127.0.0.1:{}", port),
        ),
        ("s3.region_name".to_string(), "us-east-1".to_string()),
        ("s3.credentials.access".to_string(), "test".to_string()),
        ("s3.credentials.secret".to_string(), "test".to_string()),
        (
            OpendalConf::OPERATION_TIMEOUT.to_string(),
            format!("{}ms", BUDGET.as_millis()),
        ),
    ])
}

fn blackholed_fs() -> OpendalFileSystem {
    let port = stalled_endpoint();
    let path = Path::new("s3://bucket/object.bin").unwrap();
    OpendalFileSystem::new(&path, conf(port)).unwrap()
}

fn assert_bounded_timeout(start: Instant, err: FsError) {
    let elapsed = start.elapsed();
    assert!(
        matches!(err, FsError::Timeout(_)),
        "a blackholed endpoint must fail with Timeout (ETIMEDOUT), got: {}",
        err
    );
    assert!(
        elapsed >= BUDGET,
        "failed before the configured budget: {:?}",
        elapsed
    );
    assert!(
        elapsed < PATIENCE,
        "operation stayed started past the deadline: {:?}",
        elapsed
    );
}

/// The hot-read gate: `read_verify_ufs=on` issues a HEAD before serving a cached
/// block, so a hot read depends on this path.
#[test]
fn stat_on_blackholed_endpoint_times_out() {
    let fs = blackholed_fs();
    let path = Path::new("s3://bucket/object.bin").unwrap();
    let rt = AsyncRuntime::single();

    rt.block_on(async move {
        let start = Instant::now();
        let err = fs
            .get_status(&path)
            .await
            .expect_err("a blackholed stat must not succeed");
        assert_bounded_timeout(start, err);
    });
}

/// The cold-read entry point: `open` stats the object before any GET.
#[test]
fn open_on_blackholed_endpoint_times_out() {
    let fs = blackholed_fs();
    let path = Path::new("s3://bucket/object.bin").unwrap();
    let rt = AsyncRuntime::single();

    rt.block_on(async move {
        let start = Instant::now();
        let err = match fs.open(&path).await {
            Ok(_) => panic!("a blackholed open must not succeed"),
            Err(err) => err,
        };
        assert_bounded_timeout(start, err);
    });
}

/// The write path with `write_cache=off`: `create` must reach a terminal state.
#[test]
fn create_on_blackholed_endpoint_times_out() {
    let fs = blackholed_fs();
    let path = Path::new("s3://bucket/object.bin").unwrap();
    let rt = AsyncRuntime::single();

    rt.block_on(async move {
        let start = Instant::now();
        let err = fs
            .create(&path, true)
            .await
            .err()
            .expect("a blackholed create must not succeed");
        assert_bounded_timeout(start, err);
    });
}
