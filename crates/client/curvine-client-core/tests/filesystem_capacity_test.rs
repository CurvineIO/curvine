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

use curvine_client_core::file::CurvineFileSystem;
use curvine_config::ClusterConf;
use curvine_core_error::{err_box, ErrorImpl, StringError};
use curvine_error::{ErrorKind, FsError, FsResult};
use curvine_fs_api::RpcCode;
use curvine_io::DataSlice;
use curvine_model::{FilesystemCapacity, FILESYSTEM_CAPACITY_CAPABILITY};
use curvine_net::net::{ConnState, InetAddr};
use curvine_proto::{
    ComponentInfoProto, GetFilesystemCapacityRequest, GetFilesystemCapacityResponse,
    GetFilesystemInfoRequest, GetFilesystemInfoResponse, ServerCompatibilityInfoProto,
};
use curvine_rpc::handler::{HandlerService, MessageHandler, RpcFrame};
use curvine_rpc::message::{Message, MessageBuilder, ResponseStatus};
use curvine_rpc::server::ServerConf;
use curvine_runtime::runtime::{JoinHandle, RpcRuntime, Runtime};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio::time::timeout;

const LIMIT: Duration = Duration::from_secs(5);
const INFO_RPC: i8 = RpcCode::GetFilesystemInfo as i8;
const CAPACITY_RPC: i8 = RpcCode::GetFilesystemCapacity as i8;
const CAPACITY: FilesystemCapacity = FilesystemCapacity {
    capacity: (1 << 42) + 17,
    available: (1 << 40) + 3,
};

#[derive(Clone, Copy)]
enum Reply {
    Capacity(FilesystemCapacity),
    Unsupported,
    LegacyUnsupported,
    Error(ErrorKind, &'static str),
    NotLeader,
}

struct State {
    reply: Reply,
    info: GetFilesystemInfoResponse,
    calls: Vec<i8>,
}

#[derive(Clone)]
struct Service(Arc<Mutex<State>>);

impl HandlerService for Service {
    type Item = Self;
    fn get_message_handler(&self, _: Option<ConnState>) -> Self {
        self.clone()
    }
}

impl MessageHandler for Service {
    type Error = FsError;
    fn handle(&self, msg: &Message) -> FsResult<Message> {
        let mut state = self.0.lock().unwrap();
        state.calls.push(msg.code());
        if matches!(state.reply, Reply::NotLeader) {
            return Err(FsError::not_leader("mock follower"));
        }
        match RpcCode::from(msg.code()) {
            RpcCode::GetFilesystemInfo => {
                let _: GetFilesystemInfoRequest = msg.parse_header()?;
                Ok(MessageBuilder::success_with_header(msg, state.info.clone()).build())
            }
            RpcCode::GetFilesystemCapacity => {
                let _: GetFilesystemCapacityRequest = msg.parse_header()?;
                match state.reply {
                    Reply::Capacity(capacity) => Ok(MessageBuilder::success_with_header(
                        msg,
                        GetFilesystemCapacityResponse {
                            capacity: capacity.capacity,
                            available: capacity.available,
                        },
                    )
                    .build()),
                    Reply::Unsupported => Err(FsError::unsupported("GetFilesystemCapacity")),
                    Reply::LegacyUnsupported => err_box!("Unsupported operation"),
                    Reply::Error(kind, message) => {
                        let data =
                            ErrorImpl::<StringError>::with_source(message.into()).encode(kind);
                        Ok(MessageBuilder::protocol(msg.protocol)
                            .response(ResponseStatus::Error)
                            .data(DataSlice::Buffer(data))
                            .build())
                    }
                    Reply::NotLeader => unreachable!(),
                }
            }
            _ => Err(FsError::common(format!("unexpected opcode {}", msg.code()))),
        }
    }
}

struct Master {
    service: Service,
    addr: InetAddr,
    task: JoinHandle<()>,
    shutdown: Option<oneshot::Sender<()>>,
    rt: Arc<Runtime>,
}

impl Master {
    fn start(reply: Reply, allocatable: Option<(i64, i64)>) -> Self {
        let service = Service(Arc::new(Mutex::new(State {
            reply,
            info: GetFilesystemInfoResponse {
                capacity: 9000,
                available: 6000,
                allocatable_capacity: allocatable.map(|v| v.0),
                allocatable_available: allocatable.map(|v| v.1),
                compatibility: Some(compatibility(true)),
                ..Default::default()
            },
            calls: vec![],
        })));
        let rt = Arc::new(Runtime::new("capacity-test", 1, 2));
        let listener = bounded(&rt, TcpListener::bind("127.0.0.1:0")).expect("mock master bind");
        let addr = InetAddr::new("127.0.0.1", listener.local_addr().unwrap().port());
        let conf = ServerConf::with_hostname(&addr.hostname, addr.port);
        let (shutdown, mut stopped) = oneshot::channel();
        let server_service = service.clone();
        let server_rt = rt.clone();
        let task = rt.spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    incoming = listener.accept() => {
                        let (stream, _) = incoming.expect("mock master accept");
                        stream.set_nodelay(true).expect("mock TCP_NODELAY");
                        let frame = RpcFrame::with_server(stream, &conf);
                        let mut handler = server_service.get_stream_handler(server_rt.clone(), frame, &conf);
                        connections.spawn(async move { handler.run().await });
                    }
                }
            }
            connections.shutdown().await;
        });
        Self {
            service,
            addr,
            task,
            shutdown: Some(shutdown),
            rt,
        }
    }

    fn set_reply(&self, reply: Reply) {
        self.service.0.lock().unwrap().reply = reply;
    }

    fn set_capability(&self, advertised: Option<bool>) {
        self.service.0.lock().unwrap().info.compatibility = advertised.map(compatibility);
    }

    fn calls(&self) -> Vec<i8> {
        self.service.0.lock().unwrap().calls.clone()
    }
}

impl Drop for Master {
    fn drop(&mut self) {
        let _ = self.shutdown.take().unwrap().send(());
        // Join cancelled connections while this thread still owns the runtime.
        bounded(&self.rt, &mut self.task).expect("mock master shutdown");
    }
}

fn compatibility(capacity: bool) -> ServerCompatibilityInfoProto {
    ServerCompatibilityInfoProto {
        server: ComponentInfoProto {
            component: Some("master".into()),
            capabilities: if capacity {
                vec![FILESYSTEM_CAPACITY_CAPABILITY.into()]
            } else {
                vec![]
            },
            ..Default::default()
        },
        ..Default::default()
    }
}

fn bounded<T>(rt: &Runtime, future: impl Future<Output = T>) -> T {
    rt.block_on(async { timeout(LIMIT, future).await.expect("test deadline") })
}

fn filesystem(masters: &[&Master]) -> CurvineFileSystem {
    let mut conf = ClusterConf::default();
    let client = &mut conf.client;
    client.hostname = "127.0.0.1".into();
    client.metric_report_enable = false;
    client.master_addrs = masters.iter().map(|m| m.addr.clone()).collect();
    client.conn_timeout_ms = 500;
    client.rpc_timeout_ms = 1000;
    client.conn_retry_max_duration_ms = 1500;
    client.rpc_retry_max_duration_ms = 1500;
    client.conn_retry_min_sleep_ms = 5;
    client.conn_retry_max_sleep_ms = 10;
    client.rpc_retry_min_sleep_ms = 5;
    client.rpc_retry_max_sleep_ms = 10;
    CurvineFileSystem::with_rt(conf, masters[0].rt.clone()).unwrap()
}

#[test]
fn capacity_rpc_uses_one_lazy_handshake_and_no_full_info_refresh() {
    assert_eq!(RpcCode::GetFilesystemCapacity as i8, 59);
    let master = Master::start(Reply::Capacity(CAPACITY), Some((400, 250)));
    let fs = filesystem(&[&master]);
    let client = fs.fs_client();
    let empty = FilesystemCapacity::default();
    bounded(&master.rt, async {
        assert_eq!(fs.get_filesystem_capacity().await.unwrap(), CAPACITY);
        assert_eq!(client.get_filesystem_capacity().await.unwrap(), CAPACITY);
        master.set_reply(Reply::Capacity(empty));
        assert_eq!(fs.get_filesystem_capacity().await.unwrap(), empty);
    });
    assert_eq!(
        master.calls(),
        [INFO_RPC, CAPACITY_RPC, CAPACITY_RPC, CAPACITY_RPC]
    );
}

#[test]
fn legacy_capacity_skips_probes_and_maps_allocatable_fields() {
    for advertised in [None, Some(false)] {
        for allocatable in [Some((400, 250)), Some((0, 0)), None] {
            let master = Master::start(Reply::LegacyUnsupported, allocatable);
            master.set_capability(advertised);
            let fs = filesystem(&[&master]);
            let (capacity, available) = allocatable.unwrap_or((9000, 6000));
            let expected = FilesystemCapacity {
                capacity,
                available,
            };
            bounded(&master.rt, async {
                let handshake = fs.fs_client().handshake().await.unwrap();
                assert_eq!(handshake.is_legacy(), advertised.is_none());
                assert_eq!(master.calls(), [INFO_RPC]);
                for _ in 0..2 {
                    assert_eq!(fs.get_filesystem_capacity().await.unwrap(), expected);
                }
            });
            assert_eq!(master.calls(), [INFO_RPC, INFO_RPC, INFO_RPC]);
        }
    }
}

#[test]
fn legacy_leader_upgrade_refreshes_capability_before_using_capacity_rpc() {
    for advertised in [None, Some(false)] {
        let master = Master::start(Reply::LegacyUnsupported, Some((400, 250)));
        master.set_capability(advertised);
        let fs = filesystem(&[&master]);
        let expected = FilesystemCapacity {
            capacity: 400,
            available: 250,
        };
        bounded(&master.rt, async {
            assert_eq!(fs.get_filesystem_capacity().await.unwrap(), expected);
            assert_eq!(master.calls(), [INFO_RPC, INFO_RPC]);
            master.set_reply(Reply::Capacity(CAPACITY));
            master.set_capability(Some(true));
            assert_eq!(fs.get_filesystem_capacity().await.unwrap(), expected);
            assert_eq!(master.calls(), [INFO_RPC, INFO_RPC, INFO_RPC]);
            assert_eq!(fs.get_filesystem_capacity().await.unwrap(), CAPACITY);
        });
        assert_eq!(master.calls(), [INFO_RPC, INFO_RPC, INFO_RPC, CAPACITY_RPC]);
    }
}

#[test]
fn new_to_old_leader_refreshes_stale_positive_capability_after_one_probe() {
    for reply in [Reply::Unsupported, Reply::LegacyUnsupported] {
        for advertised in [None, Some(false)] {
            let first = Master::start(Reply::Capacity(CAPACITY), Some((400, 250)));
            let second = Master::start(Reply::NotLeader, Some((800, 500)));
            second.set_capability(advertised);
            let fs = filesystem(&[&first, &second]);
            let expected = FilesystemCapacity {
                capacity: 800,
                available: 500,
            };
            bounded(&first.rt, async {
                assert_eq!(fs.get_filesystem_capacity().await.unwrap(), CAPACITY);
                first.set_reply(Reply::NotLeader);
                second.set_reply(reply);
                assert_eq!(fs.get_filesystem_capacity().await.unwrap(), expected);
                let calls = second.calls();
                // Leader discovery may already have polled this former follower.
                assert!(calls.ends_with(&[INFO_RPC]), "{calls:?}");
                assert_eq!(
                    calls.iter().filter(|&&code| code == CAPACITY_RPC).count(),
                    1
                );
                assert_eq!(
                    fs.fs_client().master_handshake().is_legacy(),
                    advertised.is_none()
                );
                assert_eq!(fs.get_filesystem_capacity().await.unwrap(), expected);
            });
            let calls = second.calls();
            assert!(calls.ends_with(&[INFO_RPC, INFO_RPC]), "{calls:?}");
            assert_eq!(
                calls.iter().filter(|&&code| code == CAPACITY_RPC).count(),
                1
            );
            assert_eq!(
                first
                    .calls()
                    .iter()
                    .filter(|&&code| code == INFO_RPC)
                    .count(),
                1
            );
        }
    }
}

#[test]
fn old_to_new_leader_discovers_capacity_support_through_full_info() {
    for advertised in [None, Some(false)] {
        let first = Master::start(Reply::LegacyUnsupported, Some((400, 250)));
        first.set_capability(advertised);
        let second = Master::start(Reply::NotLeader, Some((800, 500)));
        let fs = filesystem(&[&first, &second]);
        bounded(&first.rt, async {
            fs.fs_client().handshake().await.unwrap();
            assert_eq!(
                fs.get_filesystem_capacity().await.unwrap(),
                FilesystemCapacity {
                    capacity: 400,
                    available: 250,
                }
            );
            assert_eq!(first.calls(), [INFO_RPC, INFO_RPC]);
            first.set_reply(Reply::NotLeader);
            second.set_reply(Reply::Capacity(CAPACITY));
            // The cached negative selects full info; only the next call uses capacity.
            assert_eq!(
                fs.get_filesystem_capacity().await.unwrap(),
                FilesystemCapacity {
                    capacity: 800,
                    available: 500,
                }
            );
            let calls = second.calls();
            assert!(calls.ends_with(&[INFO_RPC]), "{calls:?}");
            assert!(!calls.contains(&CAPACITY_RPC), "{calls:?}");
            assert!(fs
                .fs_client()
                .master_handshake()
                .capabilities()
                .iter()
                .any(|capability| capability == FILESYSTEM_CAPACITY_CAPABILITY));
            assert_eq!(fs.get_filesystem_capacity().await.unwrap(), CAPACITY);
        });
        let calls = second.calls();
        assert!(calls.ends_with(&[INFO_RPC, CAPACITY_RPC]), "{calls:?}");
        assert_eq!(
            calls.iter().filter(|&&code| code == CAPACITY_RPC).count(),
            1
        );
        assert!(!first.calls().contains(&CAPACITY_RPC));
    }
}

#[test]
fn generic_server_errors_do_not_trigger_full_info_fallback() {
    for (kind, message) in [
        (ErrorKind::Common, "capacity backend unavailable"),
        (
            ErrorKind::Common,
            "Unsupported operation in capacity backend",
        ),
        (ErrorKind::IO, "capacity I/O failure"),
        (ErrorKind::Timeout, "capacity deadline elapsed"),
    ] {
        let master = Master::start(Reply::Error(kind, message), None);
        let fs = filesystem(&[&master]);
        let error = bounded(&master.rt, fs.get_filesystem_capacity()).unwrap_err();
        match kind {
            ErrorKind::Common => assert!(matches!(error, FsError::Common(_)), "{error:?}"),
            // The wire decoder represents server Timeout errors as IO errors.
            _ => assert!(matches!(error, FsError::IO(_)), "{error:?}"),
        }
        assert!(error.to_string().contains(message), "{error}");
        assert_eq!(master.calls(), [INFO_RPC, CAPACITY_RPC]);
    }
}

#[test]
fn capacity_rpc_retries_not_leader_against_another_master() {
    let first = Master::start(Reply::Capacity(CAPACITY), Some((400, 250)));
    let second = Master::start(Reply::NotLeader, Some((400, 250)));
    let fs = filesystem(&[&first, &second]);
    bounded(&first.rt, async {
        assert_eq!(fs.get_filesystem_capacity().await.unwrap(), CAPACITY);
        first.set_reply(Reply::NotLeader);
        second.set_reply(Reply::Capacity(CAPACITY));
        assert_eq!(fs.get_filesystem_capacity().await.unwrap(), CAPACITY);
    });
    assert!(
        first
            .calls()
            .iter()
            .filter(|&&code| code == CAPACITY_RPC)
            .count()
            >= 2
    );
    assert!(second.calls().contains(&CAPACITY_RPC));
    assert_eq!(
        first
            .calls()
            .iter()
            .filter(|&&code| code == INFO_RPC)
            .count(),
        1
    );
}
