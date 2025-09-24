use std::sync::Arc;
use bytes::BytesMut;
use log::info;
use orpc::common::Logger;
use orpc::ucp::{Context, Endpoint, Listener, Worker};


// cargo run --example ucp_server
#[tokio::main]
async fn main() {
    Logger::default();

    let local = tokio::task::LocalSet::new();
    local.run_until(async move {
        let context = Arc::new(Context::default());
        let worker = Arc::new(Worker::new(context.clone()).unwrap());

        let w = worker.clone();
        tokio::task::spawn_local(async move {
            info!("xxx");
            w.polling().await
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let addr = "127.0.0.1:8080".into();
        let mut listener = Listener::new(worker.clone(), &addr).unwrap();

        info!("listening on {:?}", listener.local_addr().unwrap());

        let conn = listener.next().await.unwrap();

        let ep = Endpoint::accept(worker.clone(), conn).await.unwrap();
        ep.print();
        let buf = BytesMut::zeroed(1024);
        let recv_buf = ep.stream_recv(buf).await.unwrap();
        info!("recv: {:?}", String::from_utf8_lossy(&recv_buf));
    }).await;
}