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

//! Regression guard for the RPC receipt timestamp.
//!
//! `RpcFrame::receive` stamps `Message::time` on every decoded request and
//! `MasterHandler` derives its `rpc_queue_duration` sample from that field.
//! This test drives a real server/client round trip over TCP and asserts the
//! handler observes the timestamp; removing `msg.set_time()` would silently
//! zero the metric, so the decode/handler boundary needs this assertion.

use curvine_core_error::{CommonErrorExt, CommonResultExt};
use curvine_io::DataSlice;
use curvine_net::net::{ConnState, NetUtils};
use curvine_rpc::client::{ClientConf, RpcClient};
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::{Builder, Message};
use curvine_rpc::server::{RpcServer, ServerConf};
use curvine_runtime::runtime::RpcRuntime;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
struct ProbeState {
    handler_invoked: AtomicBool,
    receipt_time_present: AtomicBool,
}

struct TimeProbeHandler {
    state: Arc<ProbeState>,
}

impl MessageHandler for TimeProbeHandler {
    type Error = CommonErrorExt;

    fn handle(&self, msg: &Message) -> CommonResultExt<Message> {
        self.state.handler_invoked.store(true, Ordering::SeqCst);
        if msg.time.is_some() {
            self.state
                .receipt_time_present
                .store(true, Ordering::SeqCst);
        }
        Ok(msg.success_with_data(None, DataSlice::Empty))
    }
}

struct TimeProbeService {
    state: Arc<ProbeState>,
}

impl HandlerService for TimeProbeService {
    type Item = TimeProbeHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        TimeProbeHandler {
            state: self.state.clone(),
        }
    }
}

#[test]
fn decoded_request_carries_receipt_time_into_handler() {
    let state = Arc::new(ProbeState::default());
    let service = TimeProbeService {
        state: state.clone(),
    };

    let conf = ServerConf::with_hostname("127.0.0.1", NetUtils::hold_available_port());
    let server = RpcServer::new(conf, service);
    let addr = server.bind_addr().clone();
    let rt = server.clone_rt();

    let mut listener = server.start();
    rt.block_on(listener.wait_startup())
        .expect("probe server must reach the Running state");

    rt.block_on(async {
        let client = RpcClient::new(false, rt.clone(), &addr, &ClientConf::default())
            .await
            .expect("client must connect to the probe server");
        let response = client
            .timeout_rpc(Duration::from_secs(10), Builder::new_rpc(1i8).build())
            .await
            .expect("probe handler must answer the request");
        assert!(
            response.not_empty(),
            "expected a response from the probe handler"
        );
    });

    assert!(
        state.handler_invoked.load(Ordering::SeqCst),
        "the request must reach the handler through RpcFrame decode"
    );
    assert!(
        state.receipt_time_present.load(Ordering::SeqCst),
        "RpcFrame::receive must stamp the receipt time before the handler runs"
    );
}
