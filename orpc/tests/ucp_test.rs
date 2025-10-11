use bytes::{BufMut, BytesMut};
use log::info;
use orpc::common::{Logger, Utils};
use orpc::runtime::RpcRuntime;
use orpc::sync::channel::CallChannel;
use orpc::sys::DataSlice;
use orpc::ucp::core::{Config, Context, Endpoint, RmaEndpoint, Worker};
use orpc::ucp::UcpRuntime;

#[test]
fn config() {
    let conf = Config::default();
    conf.print()
}

#[test]
fn context() {
    let context = Context::default();
    context.print()
}

#[test]
fn worker() {
    let context = Worker::default();
    context.print();

    let fd = context.raw_fd().unwrap();
    println!("fd = {}", fd);
}

#[test]
fn endpoint_stream() {
    Logger::default();

    let wr = UcpRuntime::default();
    let addr = "127.0.0.1:8080".into();

    let executor = wr.worker_executor().clone();
    let rt = executor.rt().clone();

    let (tx, rx) = CallChannel::channel();
    rt.spawn(async move {
        let endpoint = Endpoint::connect(executor, &addr).unwrap();
        for i in 0..100 {
            endpoint
                .stream_send(format!("hello world {}", i).as_bytes())
                .await
                .unwrap();
        }
        let _ = tx.send(1);
    });

    let _ = rt.block_on(rx.receive());
}

#[test]
fn endpoint_rma() {
    Logger::default();

    let wr = UcpRuntime::default();
    let addr = "127.0.0.1:8080".into();

    let executor = wr.worker_executor().clone();
    let rt = executor.rt().clone();

    let (tx, rx) = CallChannel::channel();


    rt.spawn(async move {
        let mut endpoint = RmaEndpoint::connect(executor, &addr, 4).unwrap();
        endpoint.exchange_mem().await.unwrap();

        for i in 0..10 {
            // 写入数据。
            // 1. 注册内存（使用现有缓冲区）
            let data = format!("abc{}", i);

            // 写入数据。
            endpoint.put(data.as_bytes()).await.unwrap();

            // 2. 通知写入完成。
            let mut buf = BytesMut::zeroed(1000);
            endpoint.tag_send("start".as_bytes()).await.unwrap();
            let recv_size = endpoint.tag_recv(&mut buf).await.unwrap();
            info!("rma send: {:?}", String::from_utf8_lossy(&buf[..recv_size]));
        }

        let _ = tx.send(1);
    });

    let _ = rt.block_on(rx.receive());
}
