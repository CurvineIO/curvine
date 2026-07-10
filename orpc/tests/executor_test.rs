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

use orpc::common::Logger;
use orpc::runtime::{GroupExecutor, SingleExecutor};
use orpc::sync::AtomicCounter;
use orpc::sys::RawPtr;
use orpc::CommonResult;
use std::sync::{mpsc, Arc};
use std::thread;

#[test]
fn test_single_thread_executor_spawn_blocking() -> CommonResult<()> {
    Logger::default();

    let executor = Arc::new(SingleExecutor::new("test", 10));

    let counter = Arc::new(AtomicCounter::new(0));
    let (tx, rx) = mpsc::sync_channel(1);

    let c1 = counter.clone();
    executor.spawn(move || {
        c1.next();
        tx.send(()).unwrap();
    })?;

    rx.recv()?;
    assert_eq!(counter.get(), 1);

    let c2 = counter.clone();
    executor.spawn_blocking(move || c2.next())?;

    assert_eq!(counter.get(), 2);

    drop(executor);

    Ok(())
}

#[test]
fn single_executor_try_spawn_returns_error_when_queue_is_full() -> CommonResult<()> {
    let executor = SingleExecutor::new("try-spawn-full", 1);
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);

    executor.spawn(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    })?;
    started_rx.recv()?;

    executor.try_spawn(|| {})?;
    let err = executor.try_spawn(|| {}).unwrap_err();
    assert!(
        err.to_string()
            .contains("executor try-spawn-full queue is full"),
        "unexpected error: {}",
        err
    );

    release_tx.send(())?;
    drop(executor);

    Ok(())
}

#[test]
fn group_executor_try_spawn_uses_available_worker() -> CommonResult<()> {
    let executor = GroupExecutor::new("group-try-spawn", 2, 128);
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);

    executor.fixed_spawn(0, move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    })?;
    started_rx.recv()?;

    for _ in 0..128 {
        executor.fixed_try_spawn(0, || {})?;
    }

    let (done_tx, done_rx) = mpsc::sync_channel(64);
    for _ in 0..64 {
        let done_tx = done_tx.clone();
        executor.try_spawn(move || {
            done_tx.send(()).unwrap();
        })?;
    }
    drop(done_tx);

    for _ in 0..64 {
        done_rx.recv()?;
    }

    release_tx.send(())?;
    drop(executor);

    Ok(())
}

#[test]
fn test_group_executor_fixed_thread_allocation_for_thread_safety() {
    let executor = Arc::new(GroupExecutor::new("test", 2, 10));

    let data = 0;
    let ptr = RawPtr::from_ref(&data);
    let mut handle = vec![];

    // We pass references between threads by bare pointers, ensuring thread safety by allocating to fixed thread execution.
    for _ in 0..10 {
        let exe_ref = executor.clone();
        let p = ptr.clone();
        let h = thread::spawn(move || {
            for _ in 0..10000 {
                let mut value = p.clone();
                exe_ref
                    .fixed_spawn(0, move || {
                        *value += 1;
                    })
                    .unwrap();
            }
        });

        handle.push(h);
    }

    for h in handle {
        h.join().unwrap()
    }

    drop(executor);

    assert_eq!(10000 * 10, *ptr.as_ref())
}
