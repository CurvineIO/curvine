// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use curvine_runtime::runtime::SingleExecutor;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

#[test]
fn shutdown_drains_a_full_queue() {
    let executor = SingleExecutor::new("shutdown-full-queue", 1);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (task_tx, task_rx) = mpsc::channel();
    executor
        .spawn(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    executor.spawn(move || task_tx.send(()).unwrap()).unwrap();
    let dropper = thread::spawn(move || drop(executor));
    release_tx.send(()).unwrap();
    task_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    dropper.join().unwrap();
}

#[test]
fn immediate_shutdown_completes_and_drains_accepted_tasks() {
    let (done_tx, done_rx) = mpsc::channel();
    let runner = thread::spawn(move || {
        let completed = Arc::new(AtomicUsize::new(0));
        for _ in 0..512 {
            let executor = SingleExecutor::new("shutdown-race", 1);
            let completed = completed.clone();
            executor
                .spawn(move || {
                    completed.fetch_add(1, Ordering::Relaxed);
                })
                .unwrap();
            drop(executor);
        }
        done_tx.send(completed.load(Ordering::Relaxed)).unwrap();
    });
    let completed = done_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("executor shutdown must not hang");
    runner.join().unwrap();
    assert_eq!(completed, 512);
}
