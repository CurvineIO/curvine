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
use curvine_io::DataSlice;
use curvine_net::net::InetAddr;
use curvine_rpc::client::{ClientConf, RpcClient};
use curvine_rpc::handler::{Frame, RpcFrame};
use curvine_rpc::message::{Builder, PROTOCOL_USIZE};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use socket2::SockRef;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

async fn connected_pair() -> (RpcClient, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: InetAddr = listener.local_addr().unwrap().into();
    let conf = ClientConf::default();
    let (client, peer) = tokio::join!(RpcClient::with_raw(&addr, &conf), listener.accept(),);
    (client.unwrap(), peer.unwrap().0)
}

#[tokio::test]
async fn cancelled_rpc_closes_raw_connection() {
    let (client, peer) = connected_pair().await;
    let mut frame = RpcFrame::with_client(peer, 4096);
    {
        let request = Builder::new_rpc(1).req_id(42).seq_id(0).build();
        let rpc = client.rpc(request);
        tokio::pin!(rpc);
        let received = timeout(Duration::from_secs(2), async {
            tokio::select! {
                received = frame.receive() => received.unwrap(),
                result = &mut rpc => panic!("RPC completed without a reply: {result:?}"),
            }
        })
        .await
        .unwrap();
        assert_eq!(received.req_id(), 42);
        // The peer has read the request but never sends a reply. Dropping rpc
        // cancels an in-flight receive, just as dropping try_join_all does.
    }

    assert!(
        client.is_closed(),
        "cancellation must poison the raw connection"
    );
    assert!(
        !client.is_active(),
        "a cancelled connection must not be pool-eligible"
    );
    let mut byte = [0];
    let read = timeout(Duration::from_secs(2), frame.io_mut().read(&mut byte))
        .await
        .expect("peer must observe shutdown while the client is still alive");
    assert!(matches!(read, Ok(0)) || read.is_err());
}

#[tokio::test]
async fn cancelled_partial_send_closes_raw_connection() {
    let (client, mut peer) = connected_pair().await;
    SockRef::from(&peer)
        .set_recv_buffer_size(64 * 1024)
        .unwrap();
    {
        let request = Builder::new_rpc(1)
            .req_id(43)
            .data(DataSlice::Buffer(BytesMut::zeroed(16 * 1024 * 1024)))
            .build();
        let send = client.send(request);
        tokio::pin!(send);
        let mut prefix = [0; 32];
        timeout(Duration::from_secs(2), async {
            tokio::select! {
                result = peer.read_exact(&mut prefix) => { result.unwrap(); },
                result = &mut send => panic!("whole frame sent to a stalled peer: {result:?}"),
            }
        })
        .await
        .unwrap();
        assert!(futures::poll!(&mut send).is_pending());
        // The peer consumed only the frame prefix. The rest is backpressured.
    }
    assert!(client.is_closed());
    assert!(!client.is_active());
    assert!(client.send(Builder::new_rpc(1).build()).await.is_err());
}

#[tokio::test]
async fn timed_out_receive_closes_raw_connection() {
    let (client, _peer) = connected_pair().await;
    client
        .send(Builder::new_rpc(1).req_id(44).seq_id(0).build())
        .await
        .unwrap();
    assert!(client
        .timeout_receive(Duration::from_millis(20), 44, 0)
        .await
        .is_err());
    assert!(client.is_closed());
    assert!(!client.is_active());
}

#[tokio::test]
async fn closed_raw_client_rejects_all_entry_points() {
    let (client, _peer) = connected_pair().await;
    client.set_closed();
    assert!(client.send(Builder::new_rpc(1).build()).await.is_err());
    assert!(timeout(Duration::from_millis(100), client.receive(45, 0))
        .await
        .unwrap()
        .is_err());
    assert!(client.rpc(Builder::new_rpc(1).build()).await.is_err());
}

#[tokio::test]
async fn mismatched_response_closes_raw_connection() {
    for (req_id, seq_id) in [(47, 0), (46, 1)] {
        let (client, peer) = connected_pair().await;
        let mut frame = RpcFrame::with_client(peer, 4096);
        frame
            .send(Builder::new_rpc(1).req_id(req_id).seq_id(seq_id).build())
            .await
            .unwrap();
        assert!(client.receive(46, 0).await.is_err());
        assert!(client.is_closed());
    }
}

#[tokio::test]
async fn successful_pipelined_io_keeps_raw_connection_reusable() {
    let (client, peer) = connected_pair().await;
    let mut frame = RpcFrame::with_client(peer, 4096);
    for seq_id in 0..2 {
        client
            .send(Builder::new_rpc(1).req_id(48).seq_id(seq_id).build())
            .await
            .unwrap();
        assert!(client.is_active(), "successful send must disarm its guard");
        assert!(!client.is_reusable(), "unread replies must prevent reuse");
    }
    for seq_id in 0..2 {
        let request = frame.receive().await.unwrap();
        frame
            .send(Builder::success(&request).build())
            .await
            .unwrap();
        let response = client.receive(48, seq_id).await.unwrap();
        assert_eq!(response.seq_id(), seq_id);
        assert!(
            client.is_active(),
            "successful receive must disarm its guard"
        );
        assert_eq!(
            client.is_reusable(),
            seq_id == 1,
            "all outstanding replies must be drained before reuse"
        );
    }
    // A normal RPC must also balance its internal send/receive pair.
    let (response, ()) = timeout(Duration::from_secs(2), async {
        tokio::join!(
            client.rpc(Builder::new_rpc(1).req_id(48).seq_id(2).build()),
            async {
                let request = frame.receive().await.unwrap();
                frame
                    .send(Builder::success(&request).build())
                    .await
                    .unwrap();
            },
        )
    })
    .await
    .unwrap();
    assert_eq!(response.unwrap().seq_id(), 2);
    assert!(client.is_reusable());
}

#[tokio::test]
async fn unpolled_receive_does_not_poison_raw_connection() {
    let (client, _peer) = connected_pair().await;
    drop(client.send(Builder::new_rpc(1).req_id(49).build()));
    drop(client.receive(49, 0));
    assert!(client.is_active());
    assert!(
        client.is_reusable(),
        "unpolled I/O must not count as a send"
    );
}

#[test]
fn cancelled_buffered_rpc_keeps_shared_connection_reusable() {
    let conf = ClientConf::default();
    let rt = Arc::new(conf.create_runtime());
    let rt2: Arc<Runtime> = rt.clone();
    rt.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: InetAddr = listener.local_addr().unwrap().into();
        let (client, peer) =
            tokio::join!(RpcClient::with_buffer(rt2, &addr, &conf), listener.accept(),);
        let client = client.unwrap();
        let mut frame = RpcFrame::with_client(peer.unwrap().0, 4096);
        let cancelled_request;
        {
            let rpc = client.rpc(Builder::new_rpc(1).req_id(50).seq_id(0).build());
            tokio::pin!(rpc);
            cancelled_request = timeout(Duration::from_secs(2), async {
                tokio::select! {
                    request = frame.receive() => request.unwrap(),
                    result = &mut rpc => panic!("no reply was sent: {result:?}"),
                }
            })
            .await
            .unwrap();
        }
        assert!(
            client.is_active(),
            "cancellation must not poison a buffered client"
        );
        assert!(
            client.is_reusable(),
            "buffered dispatch owns outstanding I/O"
        );
        frame
            .send(Builder::success(&cancelled_request).build())
            .await
            .unwrap();
        let shared = client.clone();
        let (response, ()) = timeout(Duration::from_secs(2), async {
            tokio::join!(
                shared.rpc(Builder::new_rpc(1).req_id(51).seq_id(0).build()),
                async {
                    let request = frame.receive().await.unwrap();
                    frame
                        .send(Builder::success(&request).build())
                        .await
                        .unwrap();
                },
            )
        })
        .await
        .unwrap();
        assert_eq!(response.unwrap().req_id(), 51);
        assert!(client.is_active());
    });
}

#[tokio::test]
async fn timed_out_partial_response_closes_raw_connection() {
    let (client, peer) = connected_pair().await;
    let mut frame = RpcFrame::with_client(peer, 4096);
    client
        .send(Builder::new_rpc(1).req_id(53).seq_id(0).build())
        .await
        .unwrap();
    let request = frame.receive().await.unwrap();
    let response = Builder::success(&request).build();
    let mut head = [0; PROTOCOL_USIZE];
    response.encode_protocol(&mut head[..]);
    frame.io_mut().write_all(&head[..8]).await.unwrap();
    assert!(client
        .timeout_receive(Duration::from_millis(20), 53, 0)
        .await
        .is_err());
    assert!(client.is_closed());
    assert!(!client.is_active());
    let mut remaining = Vec::new();
    timeout(
        Duration::from_secs(2),
        frame.io_mut().read_to_end(&mut remaining),
    )
    .await
    .unwrap()
    .unwrap();
}

/// Keep the existing receive-only interface usable without wrapping an empty
/// outstanding count and accidentally making the connection permanently busy.
#[tokio::test]
async fn receive_without_tracked_send_keeps_raw_connection_reusable() {
    let (client, peer) = connected_pair().await;
    let mut frame = RpcFrame::with_client(peer, 4096);
    timeout(Duration::from_secs(2), async {
        frame
            .send(Builder::new_rpc(1).req_id(55).seq_id(0).build())
            .await
            .unwrap();
        assert_eq!(client.receive(55, 0).await.unwrap().req_id(), 55);
    })
    .await
    .unwrap();
    assert!(client.is_reusable());
}
