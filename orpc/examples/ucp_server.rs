use std::mem::transmute;
use std::sync::Arc;
use bytes::{Buf, BytesMut};
use log::info;
use tokio::io::AsyncReadExt;
use orpc::common::Logger;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::DataSlice;
use orpc::ucp::core::SockAddr;
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
                while let Some(recv_buf) = endpoint.stream_recv(BytesMut::zeroed(1024)).await.unwrap() {
                    info!("recv: {:?}", String::from_utf8_lossy(&recv_buf));
                }
            });
        }
    });
}

fn rma(pool: UcpRuntime, addr: SockAddr) {
    let mut listener = pool.bind(&addr).unwrap();
    let rt = listener.executor().clone_rt();

    rt.block_on(async move {
        loop {
            let conn = listener.accept().await.unwrap();

            let endpoint = pool.accept(conn).unwrap();
            let rt = endpoint.executor().clone_rt();

            rt.spawn(async move {
                // step 1：读取远程内存
                let mut buf = endpoint.stream_recv(BytesMut::zeroed(100)).await.unwrap().unwrap();
               // assert_eq!(buf.len(), 16);

                info!("recv: len = {}, buff={:?}", buf.len(), buf);

                let rma_addr = buf.get_u64_ne();
                info!("recv: rma_addr={:#x}", rma_addr);

                info!("recv: len = {}, buff={:?}", buf.len(), buf);

                let rkey = RKey::unpack(&endpoint, &buf).unwrap();

                loop {
                    // loop读取到永远是相同的数据。
                    let mut buf = vec![0; 4];
                    endpoint.get(&mut buf, rma_addr, &rkey).await.unwrap();
                    info!("读取到的字符串: {:?}", String::from_utf8_lossy(&buf));
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

            rt.spawn(async move {
                // step 1：读取远程内存
                let buf = endpoint.stream_recv(BytesMut::zeroed(100)).await.unwrap().unwrap();
                info!("recv start {}", String::from_utf8_lossy(&buf));
                let addr_buf= endpoint.get_memory().unwrap();

                endpoint.stream_send(DataSlice::Buffer(addr_buf)).await.unwrap();

                loop {
                    let recv = endpoint.stream_recv(BytesMut::zeroed(1024)).await.unwrap().unwrap();
                    info!("recv control {}", String::from_utf8_lossy(&recv));

                    // loop读取到永远是相同的数据。
                    let data = endpoint.buffer.buffer();
                    info!("读取到的字符串: {:?}", String::from_utf8_lossy(&data[..4]));

                    endpoint.stream_send("xxxx".into()).await.unwrap();
                }
            });
        }
    });
}