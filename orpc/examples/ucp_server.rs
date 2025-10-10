use std::mem::transmute;
use std::sync::Arc;
use bytes::{Buf, BytesMut};
use log::info;
use tokio::io::AsyncReadExt;
use orpc::common::Logger;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::DataSlice;
use orpc::ucp::core::{RmaEndpoint, SockAddr};
use orpc::ucp::rma::RKey;
use orpc::ucp::UcpRuntime;

// cargo run --example ucp_server
fn main() {
    Logger::default();

    let pool = UcpRuntime::default();
    let addr = "127.0.0.1:8080".into();

    rma2(pool, addr);
}

fn stream(pool: UcpRuntime, addr: SockAddr) {
    let mut listener = pool.bind(&addr).unwrap();
    let rt = listener.executor().clone_rt();

    rt.block_on(async move {
        loop {
            let conn = listener.accept().await.unwrap();

            let endpoint = pool.accept(conn).unwrap();
            let rt = endpoint.executor().clone_rt();

            rt.spawn(async move {
                loop {
                    let mut buf = BytesMut::zeroed(1024);
                    let recv_size = endpoint.stream_recv(&mut buf).await.unwrap();
                    if recv_size == 0 {
                        break;
                    }
                    info!("recv: {:?}", String::from_utf8_lossy(&buf[..recv_size]));
                }
            });
        }
    });
}

fn rma2(pool: UcpRuntime, addr: SockAddr) {
    let mut listener = pool.bind(&addr).unwrap();
    let rt = listener.executor().clone_rt();

    rt.block_on(async move {
        loop {
            let conn = listener.accept().await.unwrap();

            let endpoint = pool.accept(conn).unwrap();
            let rt = endpoint.executor().clone_rt();
            let local_mem = pool.register_memory(4).unwrap();
            let mut endpoint = RmaEndpoint::new(endpoint, local_mem);


            rt.spawn(async move {
                endpoint.accept_memory().await.unwrap();

                loop {
                    let mut buf = BytesMut::zeroed(1024);
                    let recv = endpoint.stream_recv(&mut buf).await.unwrap();
                    info!("recv control {}", String::from_utf8_lossy(&buf[..recv]));

                    // loop读取到永远是相同的数据。
                    let data = endpoint.local_mem_slice();
                    info!("读取到的字符串: {:?}", String::from_utf8_lossy(&data[..4]));

                    endpoint.stream_send("xxxx".as_ref()).await.unwrap();
                }
            });
        }
    });
}