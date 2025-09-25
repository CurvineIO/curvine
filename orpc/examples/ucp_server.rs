use std::sync::Arc;
use bytes::BytesMut;
use log::{error, info};
use tokio::task::spawn_local;
use orpc::common::Logger;
use orpc::ucp::{Context, Endpoint, Listener, Worker};


// cargo run --example ucp_server
#[tokio::main]
async fn main() {
    Logger::default();

    let context = Arc::new(Context::default());
    let addr = "127.0.0.1:8080".into();

    let local = tokio::task::LocalSet::new();
    local.run_until(async move {
        let worker = context.create_worker().unwrap();

        let w = worker.clone();
        spawn_local(async move {
            w.event_poll().await.unwrap();
        });

        let mut listener = Listener::bind(&worker, &addr).await.unwrap();

        let conn = listener.accept().await.unwrap();

        let ep = Endpoint::accept(&worker, conn).unwrap();

        ep.print();
        let buf = BytesMut::zeroed(1024);
        let recv_buf = ep.stream_recv(buf).await.unwrap();

        info!("recv: {:?}", String::from_utf8_lossy(&recv_buf));
    }).await;
}