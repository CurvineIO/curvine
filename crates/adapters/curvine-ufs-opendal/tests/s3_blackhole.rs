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
use curvine_fs_api::{FileSystem, Path, Reader, Writer};
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_ufs_api::OpendalConf;
use curvine_ufs_opendal::OpendalFileSystem;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
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

/// The HTTP client honours `HTTP_PROXY`, which would send these loopback
/// requests to a proxy instead of the local endpoint, so the tests would
/// measure the proxy rather than the endpoint they scripted.
fn bypass_proxy() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        std::env::set_var("NO_PROXY", "127.0.0.1,localhost");
        std::env::set_var("no_proxy", "127.0.0.1,localhost");
    });
}

fn conf(port: u16) -> HashMap<String, String> {
    bypass_proxy();
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

/// One HTTP request seen by [`scripted_endpoint`].
struct Request {
    method: String,
    target: String,
    body_len: usize,
}

impl Request {
    fn describe(&self) -> String {
        format!("{} {} ({} bytes)", self.method, self.target, self.body_len)
    }
}

/// Bind an S3-like endpoint that answers requests with `script` and stalls a
/// request, holding its socket open without a response, when `script` returns
/// `None`.
///
/// The blackhole endpoint above stalls the preliminary HEAD of `open` and
/// `create`, so it cannot show that the reader and writer calls themselves are
/// bounded. This endpoint answers that setup and stalls only the request under
/// test. It returns every stalled request, so a test can prove the deadline
/// fired at the intended call rather than at an earlier one.
fn scripted_endpoint(
    script: fn(&Request) -> Option<&'static str>,
) -> (u16, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a local scripted endpoint");
    let port = listener.local_addr().unwrap().port();
    let stalled = Arc::new(Mutex::new(Vec::new()));

    let seen = stalled.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            let seen = seen.clone();
            std::thread::spawn(move || serve(stream, script, seen));
        }
    });

    (port, stalled)
}

/// Serve keep-alive requests on one connection until a request is stalled or
/// the client closes it.
fn serve(
    stream: TcpStream,
    script: fn(&Request) -> Option<&'static str>,
    stalled: Arc<Mutex<Vec<String>>>,
) {
    let mut reader = BufReader::new(stream.try_clone().expect("clone the accepted socket"));
    let mut writer = stream;
    loop {
        let Some(req) = read_request(&mut reader) else {
            return;
        };
        match script(&req) {
            Some(response) => {
                if writer.write_all(response.as_bytes()).is_err() {
                    return;
                }
            }
            None => {
                stalled.lock().unwrap().push(req.describe());
                // Keep the socket open and silent, like a blackholed path.
                loop {
                    std::thread::sleep(Duration::from_secs(60));
                }
            }
        }
    }
}

fn read_request(reader: &mut BufReader<TcpStream>) -> Option<Request> {
    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();

    let mut body_len = 0;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).ok()? == 0 {
            return None;
        }
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                body_len = value.trim().parse().ok()?;
            }
        }
    }

    let mut body = vec![0; body_len];
    reader.read_exact(&mut body).ok()?;
    Some(Request {
        method,
        target,
        body_len,
    })
}

const NOT_FOUND: &str = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
const PUT_OK: &str = "HTTP/1.1 200 OK\r\nETag: \"etag\"\r\nContent-Length: 0\r\n\r\n";
/// HEAD of a 16-byte object. A HEAD response carries no body.
const HEAD_16_BYTES: &str = "HTTP/1.1 200 OK\r\nContent-Length: 16\r\nETag: \"etag\"\r\n\
Last-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\n\r\n";

/// Setup for `create` on a new object: the existence HEAD finds nothing and the
/// empty placeholder PUT succeeds. Every other request stalls.
fn answer_create_setup(req: &Request) -> Option<&'static str> {
    match req.method.as_str() {
        "HEAD" => Some(NOT_FOUND),
        "PUT" if req.body_len == 0 => Some(PUT_OK),
        _ => None,
    }
}

/// Setup for `open`: the stat HEAD succeeds. Every GET stalls.
fn answer_open_setup(req: &Request) -> Option<&'static str> {
    match req.method.as_str() {
        "HEAD" => Some(HEAD_16_BYTES),
        _ => None,
    }
}

fn scripted_fs(
    script: fn(&Request) -> Option<&'static str>,
) -> (OpendalFileSystem, Arc<Mutex<Vec<String>>>) {
    let (port, stalled) = scripted_endpoint(script);
    let path = Path::new("s3://bucket/object.bin").unwrap();
    (OpendalFileSystem::new(&path, conf(port)).unwrap(), stalled)
}

fn assert_stalled_at(stalled: &Arc<Mutex<Vec<String>>>, method: &str) {
    let stalled = stalled.lock().unwrap();
    assert!(
        !stalled.is_empty() && stalled.iter().all(|r| r.starts_with(method)),
        "the deadline must fire at the {} request, stalled requests: {:?}",
        method,
        stalled
    );
}

/// `read_chunk0` itself is bounded: `open` succeeds and the GET stalls.
#[test]
fn read_on_stalled_get_times_out() {
    let (fs, stalled) = scripted_fs(answer_open_setup);
    let path = Path::new("s3://bucket/object.bin").unwrap();
    let rt = AsyncRuntime::single();

    rt.block_on(async move {
        let mut reader = fs.open(&path).await.expect("open must pass its HEAD");
        let mut buf = [0u8; 16];
        let start = Instant::now();
        let err = reader
            .read(&mut buf)
            .await
            .expect_err("a stalled GET must not succeed");
        assert_bounded_timeout(start, err);
    });
    assert_stalled_at(&stalled, "GET");
}

/// `write_chunk` itself is bounded. The first 8 MiB chunk only fills opendal's
/// multipart cache; the second starts the multipart upload, which stalls.
#[test]
fn write_on_stalled_upload_times_out_and_poisons_the_writer() {
    let (fs, stalled) = scripted_fs(answer_create_setup);
    let path = Path::new("s3://bucket/object.bin").unwrap();
    let rt = AsyncRuntime::single();

    rt.block_on(async move {
        let mut writer = fs
            .create(&path, true)
            .await
            .expect("create must pass its setup");
        let data = vec![7u8; 8 * 1024 * 1024 + 1];
        writer
            .write(&data)
            .await
            .expect("the first chunk is only cached");
        let start = Instant::now();
        let err = writer
            .write(&data)
            .await
            .expect_err("a stalled upload must not succeed");
        assert_bounded_timeout(start, err);

        // The timed-out chunk is gone from the buffer, so committing now would
        // publish an object with a hole in it. The writer must refuse at once
        // with the original cause instead of trying another upload.
        let err = writer
            .complete()
            .await
            .expect_err("complete after a failed upload must not report success");
        assert!(
            err.to_string().contains("already failed"),
            "complete must report the earlier upload failure: {}",
            err
        );
    });
    assert_stalled_at(&stalled, "POST");
}

/// `complete` itself is bounded, and a retried `complete` after the timeout
/// must not report an upload that was never committed as successful.
#[test]
fn complete_on_stalled_close_times_out_and_stays_failed() {
    let (fs, stalled) = scripted_fs(answer_create_setup);
    let path = Path::new("s3://bucket/object.bin").unwrap();
    let rt = AsyncRuntime::single();

    rt.block_on(async move {
        let mut writer = fs
            .create(&path, true)
            .await
            .expect("create must pass its setup");
        writer
            .write(b"payload")
            .await
            .expect("a small write is buffered");

        let start = Instant::now();
        let err = writer
            .complete()
            .await
            .expect_err("a stalled close must not succeed");
        assert_bounded_timeout(start, err);

        let err = writer
            .complete()
            .await
            .expect_err("a retried complete must not commit an empty object");
        assert!(
            err.to_string().contains("already failed")
                && err
                    .to_string()
                    .contains("exceeded opendal.operation_timeout"),
            "the retry must report the original cause: {}",
            err
        );
    });
    assert_stalled_at(&stalled, "PUT");
}
