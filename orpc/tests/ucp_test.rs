use std::sync::Arc;
use log::info;
use tokio::task::spawn_local;
use orpc::common::Logger;
use orpc::ucp::{Config, Context, Endpoint, SockAddr, Worker};

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

#[tokio::test]
async fn endpoint() {
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

        let endpoint = Endpoint::connect(&worker, &addr).unwrap();
        endpoint.print();
        endpoint.stream_send("hello world".into()).await.unwrap();
    }).await;
}
