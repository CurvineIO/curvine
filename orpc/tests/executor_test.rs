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
fn single() -> CommonResult<()> {
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
fn group() {
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
