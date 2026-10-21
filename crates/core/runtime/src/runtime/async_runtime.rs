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
    // Option so Drop can move the runtime out and shut it down without blocking.
    inner: Option<Runtime>,
    name_prefix: String,
    io_threads: usize,
    worker_threads: usize,
}

impl AsyncRuntime {
    pub fn new<T: AsRef<str>>(name_prefix: T, io_threads: usize, worker_threads: usize) -> Self {
        let mut builder = Builder::new_multi_thread();
        builder
            .worker_threads(io_threads)
            .thread_keep_alive(Duration::from_secs(6 * 3600))
            .thread_name(name_prefix.as_ref())
            .enable_all();
        if worker_threads > 0 {
            builder.max_blocking_threads(worker_threads);
        }

        let rt = builder.build().unwrap();
        AsyncRuntime {
            inner: Some(rt),
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
            inner: Some(rt),
            name_prefix: name.to_string(),
            io_threads: 1,
            worker_threads: 0,
        }
    }

    pub fn default(name_prefix: &str) -> Self {
        let default_thread = 2 * thread::available_parallelism().unwrap().get();
        Self::new(name_prefix, 32, default_thread.max(4))
    }

    pub fn single() -> Self {
        Self::new("single", 1, 1)
    }

    pub fn shutdown_background(mut self) {
        if let Some(rt) = self.inner.take() {
            rt.shutdown_background();
        }
    }

    fn runtime(&self) -> &Runtime {
        self.inner
            .as_ref()
            .expect("AsyncRuntime used after shutdown")
    }
}

/// Never block while dropping a runtime.
///
/// Curvine clones `Arc<Runtime>` into background I/O tasks, so the last
/// reference can be released from a worker thread of that very runtime.
/// `tokio::runtime::Runtime::drop` performs a blocking shutdown and panics in
/// that context ("Cannot drop a runtime in a context where blocking is not
/// allowed"). With `panic = "abort"` that panic killed the host process.
/// `shutdown_background` never blocks, so teardown is safe from any thread.
impl Drop for AsyncRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.inner.take() {
            rt.shutdown_background();
        }
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
        self.runtime().spawn(task)
    }

    fn block_on<F>(&self, task: F) -> F::Output
    where
        F: Future,
    {
        self.runtime().block_on(task)
    }

    fn spawn_blocking<F, R>(&self, task: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.runtime().spawn_blocking(task)
    }
}

#[cfg(test)]
mod tests {
    use super::AsyncRuntime;
    use crate::runtime::RpcRuntime;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// Dropping the last runtime reference from inside one of that runtime's
    /// own tasks must not panic. The tokio assertion panics on a worker
    /// thread; with `panic = "abort"` that aborts the whole host process,
    /// which is how the StarRocks CN died with SIGABRT.
    ///
    /// The panic is observed through the task's `JoinHandle` instead of the
    /// process exit code, so the test also fails on the default test profile
    /// where panics unwind.
    #[test]
    fn drop_last_reference_inside_async_task_is_safe() {
        let rt = Arc::new(AsyncRuntime::new("rpc-client-test", 2, 2));
        let released = Arc::new(AtomicBool::new(false));
        let task_rt = rt.clone();
        let task_released = released.clone();

        let handle = rt.spawn(async move {
            while !task_released.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert_eq!(
                1,
                Arc::strong_count(&task_rt),
                "task must hold the last reference for this test to be meaningful"
            );
            drop(task_rt);
        });
        drop(rt);
        released.store(true, Ordering::Release);

        let outcome = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("observer runtime")
            .block_on(handle);
        assert!(
            outcome.is_ok(),
            "runtime teardown panicked on a worker thread: {:?}",
            outcome.err()
        );
    }
}
