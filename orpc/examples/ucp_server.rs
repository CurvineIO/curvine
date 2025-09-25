use std::sync::Arc;
use bytes::BytesMut;
use log::info;
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
        let mut listener = Listener::bind(context.clone(), &addr).await.unwrap();

        let conn = listener.accept().await.unwrap();

        let ep = Endpoint::accept(listener.worker().clone(), conn).unwrap();

        ep.print();
        let buf = BytesMut::zeroed(1024);
        let recv_buf = ep.stream_recv(buf).await.unwrap();

        info!("recv: {:?}", String::from_utf8_lossy(&recv_buf));
    }).await;
}