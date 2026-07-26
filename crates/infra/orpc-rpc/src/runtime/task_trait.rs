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

// A task that needs to be executed in a loop.
pub trait LoopTask {
    type Error: std::error::Error;

    fn run(&self) -> Result<(), Self::Error>;

    // Whether to terminate the current task.
    fn terminate(&self) -> bool;
}

pub trait MutEvent<T, E> {
    fn run(&mut self, event: Option<T>) -> Result<(), E>;
}
