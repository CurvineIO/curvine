use bytes::BytesMut;
use log::info;
use orpc::common::Logger;
use orpc::runtime::RpcRuntime;
use orpc::ucp::WorkerRuntime;

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
                while let Some(recv_buf) = endpoint.stream_recv(BytesMut::zeroed(1024)).await.unwrap() {
                    info!("recv: {:?}", String::from_utf8_lossy(&recv_buf));
                }
            });
        }
    });
}
