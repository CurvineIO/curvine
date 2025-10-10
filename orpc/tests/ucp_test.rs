use bytes::{BufMut, BytesMut};
use log::info;
use orpc::common::{Logger, Utils};
use orpc::runtime::RpcRuntime;
use orpc::sync::channel::CallChannel;
use orpc::sys::DataSlice;
use orpc::ucp::{Config, Context, Endpoint, RmaMemory, Worker, UcpRuntime};

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
                .stream_send(format!("hello world {}", i).into())
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
        // 1. 注册内存（使用现有缓冲区）
        let buf: BytesMut = BytesMut::from("abcd");
        info!("rma send: {:?}", String::from_utf8_lossy(&buf));

        let endpoint = Endpoint::connect(executor, &addr).unwrap();
        let mem = wr.register_memory(buf).unwrap();
        let pack = mem.pack().unwrap();

        // 2. 发送内存地址
        let mut addr_buf = BytesMut::new();

        info!("send: rma_addr={:#x}", mem.buffer_addr());
        addr_buf.put_u64_ne(mem.buffer_addr());

        info!("pack: {:?}", pack.as_slice());
        addr_buf.extend_from_slice(pack.as_slice());

        info!("send: len = {}, buff = {:?}", addr_buf.len(), addr_buf);

        endpoint.stream_send(DataSlice::Buffer(addr_buf)).await.unwrap();

        // @todo 必须发起一个这个请求，服务端才能读取数据。
        // 否则服务端会阻塞，原因不清楚
        endpoint.stream_recv(BytesMut::zeroed(1)).await.unwrap();
        Utils::sleep(10000);

        let _ = tx.send(1);
    });

    let _ = rt.block_on(rx.receive());
}


#[test]
fn endpoint_rma1() {
    Logger::default();

    let wr = UcpRuntime::default();
    let addr = "127.0.0.1:8080".into();

    let executor = wr.worker_executor().clone();
    let rt = executor.rt().clone();

    let (tx, rx) = CallChannel::channel();


    rt.spawn(async move {
        let endpoint = Endpoint::connect(executor, &addr).unwrap();
        let (addr, rkey) = endpoint.send_get_memory().await.unwrap();



        for i in 0..10 {
            // 写入数据。
            // 1. 注册内存（使用现有缓冲区）
            let data = format!("abc{}", i);

            // 写入数据。
            endpoint.put(DataSlice::Buffer(BytesMut::from(data.as_bytes())), addr, &rkey).await.unwrap();

            // 2. 通知写入完成。
            endpoint.stream_send("start".into()).await.unwrap();
            let buf = endpoint.stream_recv(BytesMut::zeroed(1000)).await.unwrap().unwrap();
            info!("rma send: {:?}", String::from_utf8_lossy(&buf));
        }

        let _ = tx.send(1);
    });

    let _ = rt.block_on(rx.receive());
}
