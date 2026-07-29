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

use futures::Future;

mod async_runtime;
pub use self::async_runtime::AsyncRuntime;

mod single_executor;
pub use self::single_executor::SingleExecutor;

mod group_executor;
pub use self::group_executor::GroupExecutor;

mod job_state;
pub use self::job_state::*;

mod task_trait;
pub use self::task_trait::*;

/// Use conditional compilation to use a different runtime.
/// It cannot be fully implemented using generics and type alias.
pub type JoinHandle<T> = tokio::task::JoinHandle<T>;
pub type Runtime = AsyncRuntime;

// rpc framework runtime abstract interface.
pub trait RpcRuntime {
    fn io_threads(&self) -> usize;

    fn worker_threads(&self) -> usize;

    fn thread_name(&self) -> &str;

    fn spawn<F>(&self, task: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    fn block_on<F>(&self, task: F) -> F::Output
    where
        F: Future;

    fn spawn_blocking<F, R>(&self, task: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static;
}
