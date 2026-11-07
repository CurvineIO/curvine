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

//! An idle `BufferClient` connection that closes itself must release its
//! socket. Before the fix the client only dropped its write half, so the socket
//! stayed open until the server's own idle timeout.

use std::io::Read;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use curvine_net::net::InetAddr;
use curvine_rpc::client::{BufferClient, ClientConf};
use curvine_runtime::runtime::{RpcRuntime, Runtime};

#[test]
fn idle_close_releases_the_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let start = Instant::now();
        let mut buf = [0u8; 64];
        // The client sends nothing; the only possible read result is EOF.
        let res = sock.read(&mut buf);
        (res.map_err(|e| e.kind()), start.elapsed())
    });

    let rt = Arc::new(Runtime::new("idle-test", 2, 2));
    let conf = ClientConf {
        close_idle: true,
        rpc_timeout_ms: 300,
        ..ClientConf::default()
    };
    let addr = InetAddr::new("127.0.0.1", port);
    let client = rt
        .block_on(BufferClient::new(rt.clone(), &addr, &conf))
        .unwrap();

    let (res, waited) = server.join().unwrap();
    assert_eq!(
        res,
        Ok(0),
        "server should read EOF once the client idles out"
    );
    assert!(waited < Duration::from_secs(3), "EOF took {waited:?}");
    drop(client);
}
