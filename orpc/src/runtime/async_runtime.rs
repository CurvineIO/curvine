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

use crate::runtime::{JoinHandle, RpcRuntime};
use futures::Future;
use std::thread;
use tokio::runtime::{Builder, Runtime};
use tokio::time::Duration;

#[derive(Debug)]
pub struct AsyncRuntime {
    inner: Runtime,
    name_prefix: String,
    io_threads: usize,
    worker_threads: usize,
}

impl AsyncRuntime {
    pub fn new<T: AsRef<str>>(name_prefix: T, io_threads: usize, worker_threads: usize) -> Self {
        let mut builder = Builder::new_multi_thread();
        builder.worker_threads(io_threads)
            .thread_keep_alive(Duration::from_secs(6 * 3600))
            .thread_name(name_prefix.as_ref())
            .enable_all();
        if worker_threads > 0 {
            builder.max_blocking_threads(worker_threads);
        }

        let rt = builder.build().unwrap();
        AsyncRuntime {
            inner: rt,
            name_prefix: String::from(name_prefix.as_ref()),
            io_threads,
            worker_threads,
        }
    }

    pub fn current_thread(name: impl AsRef<str>) -> Self {
        let name = name.as_ref();
        let rt = Builder::new_current_thread()
            .enable_all()
            .thread_name(name)
            .build()
            .unwrap();

        AsyncRuntime {
            inner: rt,
            name_prefix: name.to_string(),
            io_threads: 1,
            worker_threads: 0
        }
    }

    pub fn default(name_prefix: &str) -> Self {
        let default_thread = 2 * thread::available_parallelism().unwrap().get();
        Self::new(name_prefix, 32, default_thread.max(4))
    }

    pub fn single() -> Self {
        Self::new("single", 1, 1)
    }
}

impl RpcRuntime for AsyncRuntime {
    fn io_threads(&self) -> usize {
        self.io_threads
    }

    fn worker_threads(&self) -> usize {
        self.worker_threads
    }

    fn thread_name(&self) -> &str {
        &self.name_prefix
    }

    fn spawn<F>(&self, task: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.inner.spawn(task)
    }

    fn block_on<F>(&self, task: F) -> F::Output
    where
        F: Future,
    {
        self.inner.block_on(task)
    }

    fn spawn_blocking<F, R>(&self, task: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.inner.spawn_blocking(task)
    }
}
