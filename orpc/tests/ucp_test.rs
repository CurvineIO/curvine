use log::info;
use orpc::common::Logger;
use orpc::runtime::RpcRuntime;
use orpc::sync::channel::CallChannel;
use orpc::ucp::{Config, Context, Endpoint, Worker, WorkerRuntime};

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
fn endpoint() {
    Logger::default();

    let wr = WorkerRuntime::default();
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
