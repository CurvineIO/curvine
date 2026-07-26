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

use crate::io::IOResult;
use crate::{try_err, try_option};
use std::collections::HashMap;
use std::net::{TcpListener, ToSocketAddrs};
use std::sync::{Mutex, OnceLock};
use sysinfo::System;

// Process-global map of pre-bound listeners for test port reservation.
// Keeps sockets alive between port selection and server startup, preventing
// TOCTOU races when multiple test processes run in parallel (e.g. cargo nextest).
static HELD_LISTENERS: OnceLock<Mutex<HashMap<u16, TcpListener>>> = OnceLock::new();

pub struct NetUtils;

impl NetUtils {
    // Get a system-available port.
    pub fn get_available_port() -> u16 {
        TcpListener::bind("0.0.0.0:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    // Bind to an available port and hold the socket open in a global map.
    // The port stays reserved until `take_held_listener` is called, eliminating
    // the TOCTOU race between port discovery and actual server bind.
    // Use this in test infrastructure instead of `get_available_port`.
    pub fn hold_available_port() -> u16 {
        let listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let map = HELD_LISTENERS.get_or_init(|| Mutex::new(HashMap::new()));
        map.lock().unwrap().insert(port, listener);
        port
    }

    // Remove and return the held TcpListener for a port.
    // Called by the server runtime when it is ready to accept connections.
    pub fn take_held_listener(port: u16) -> Option<TcpListener> {
        HELD_LISTENERS
            .get()
            .and_then(|map| map.lock().unwrap().remove(&port))
    }

    pub fn local_hostname() -> String {
        Self::get_hostname().unwrap_or("localhost".to_string())
    }

    pub fn local_ip<T: AsRef<str>>(hostname: T) -> String {
        Self::get_ip_v4(hostname.as_ref()).unwrap_or("127.0.0.1".to_owned())
    }

    pub fn get_hostname() -> Option<String> {
        System::host_name()
    }

    pub fn get_ip_v4<T: AsRef<str>>(host: T) -> IOResult<String> {
        let hostname = format!("{}:{}", host.as_ref(), 0);
        // Try to get an ipv4 address.
        let ip = try_err!(hostname.to_socket_addrs())
            .filter(|x| x.is_ipv4())
            .next();
        let ip = try_option!(ip).ip().to_string();
        Ok(ip)
    }
}
