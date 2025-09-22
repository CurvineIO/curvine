use std::sync::Arc;
use std::thread::spawn;
use tokio::task::spawn_local;
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
    let worker = Arc::new(Worker::default());
    let addr = "127.0.0.1:8080".into();

    let polling_worker = worker.clone();
    tokio::spawn(async move {
        polling_worker.polling().await;
    });

    let endpoint = Endpoint::connect(worker.clone(), &addr).await.unwrap();
    endpoint.print();
    endpoint.stream_send("hello world".into()).await.unwrap();
}
