use std::mem::transmute;
use std::sync::Arc;
use bytes::{Buf, BytesMut};
use log::info;
use tokio::io::AsyncReadExt;
use orpc::common::Logger;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::DataSlice;
use orpc::ucp::core::SockAddr;
use orpc::ucp::reactor::UcpRuntime;
use orpc::ucp::rma::RKey;

// cargo run --example ucp_server
fn main() {
    Logger::default();

    let pool = UcpRuntime::default();
    let addr = "127.0.0.1:8080".into();

    rma2(pool, addr);
}

fn rma2(pool: UcpRuntime, addr: SockAddr) {
    let mut listener = pool.bind(&addr).unwrap();
    let rt = pool.boss_executor().clone_rt();

    rt.block_on(async move {
        loop {
            let conn = listener.accept().await.unwrap();

            let mut endpoint = pool.accept_rma(conn, 4).unwrap();

            rt.spawn(async move {
                endpoint.handshake_response().await.unwrap();
                loop {
                    let mut buf = BytesMut::zeroed(1024);
                    let recv = endpoint.tag_recv(&mut buf).await.unwrap();
                    info!("recv control {}", String::from_utf8_lossy(&buf[..recv]));

                    // loop读取到永远是相同的数据。
                    let data = endpoint.local_mem_slice();
                    info!("读取到的字符串: {:?}", String::from_utf8_lossy(&data[..4]));

                    endpoint.tag_send("xxxx".as_bytes()).await.unwrap();
                }
            });
        }
    });
}