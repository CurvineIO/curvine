use std::sync::Arc;
use bytes::BytesMut;
use log::{error, info};
use tokio::task::spawn_local;
use orpc::common::Logger;
use orpc::runtime::RpcRuntime;
use orpc::ucp::{Context, Endpoint, Listener, Worker, WorkerRuntime};


// cargo run --example ucp_server
fn main() {
    Logger::default();

    let pool = WorkerRuntime::default();
    let addr = "127.0.0.1:8080".into();

    let mut listener = pool.bind(&addr).unwrap();
    let rt = listener.executor().clone_rt();

    rt.block_on(async move {
        loop {
            let conn = listener.accept().await.unwrap();

            let endpoint = pool.accept(conn).unwrap();
            let rt = endpoint.executor().clone_rt();

            rt.spawn(async move {
                let buf = BytesMut::zeroed(1024);
                let recv_buf = endpoint.stream_recv(buf).await.unwrap();

                info!("recv: {:?}", String::from_utf8_lossy(&recv_buf));
            });
        }
    });
}