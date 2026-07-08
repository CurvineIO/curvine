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

use crate::common::Utils;
use crate::runtime::SingleExecutor;
use crate::CommonResult;
use std::fmt::{Debug, Display, Formatter};
use std::io::Cursor;

/// Thread group
#[derive(Debug)]
pub struct GroupExecutor {
    name_prefix: String,
    thread_num: usize,
    channel_size: usize,
    workers: Vec<SingleExecutor>,
}

impl GroupExecutor {
    pub fn new<T: AsRef<str>>(name_prefix: T, thread_num: usize, channel_size: usize) -> Self {
        let mut workers: Vec<SingleExecutor> = Vec::with_capacity(thread_num);

        for index in 0..thread_num {
            let name = format!("{}-{}", name_prefix.as_ref(), index);
            workers.push(SingleExecutor::new(name, channel_size));
        }

        GroupExecutor {
            name_prefix: name_prefix.as_ref().to_string(),
            thread_num,
            channel_size,
            workers,
        }
    }

    fn get_fix_thread(&self, id: i64) -> &SingleExecutor {
        let hash = murmur3::murmur3_32(&mut Cursor::new(&id.to_be_bytes()), 104729).unwrap();
        let index = hash as usize % self.thread_num;
        &self.workers[index]
    }

    fn get_robin_thread(&self) -> &SingleExecutor {
        let hash = Utils::rand_id() as usize;
        let index = hash % self.thread_num;
        &self.workers[index]
    }

    pub fn spawn<F>(&self, task: F) -> CommonResult<()>
    where
        F: FnOnce() + Send + 'static,
    {
        self.get_robin_thread().spawn(task)
    }

    pub fn try_spawn<F>(&self, task: F) -> CommonResult<()>
    where
        F: FnOnce() + Send + 'static,
    {
        self.get_robin_thread().try_spawn(task)
    }

    pub fn spawn_blocking<F, R>(&self, task: F) -> CommonResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.get_robin_thread().spawn_blocking(task)
    }

    pub fn fixed_spawn<F>(&self, id: i64, task: F) -> CommonResult<()>
    where
        F: FnOnce() + Send + 'static,
    {
        self.get_fix_thread(id).spawn(task)
    }

    pub fn fixed_try_spawn<F>(&self, id: i64, task: F) -> CommonResult<()>
    where
        F: FnOnce() + Send + 'static,
    {
        self.get_fix_thread(id).try_spawn(task)
    }

    pub fn fixed_spawn_blocking<F, R>(&self, id: i64, task: F) -> CommonResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.get_fix_thread(id).spawn_blocking(task)
    }
}

impl Display for GroupExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GroupThread: name_prefix = {}, thread_num = {}, channel_size = {}",
            self.name_prefix, self.thread_num, self.channel_size
        )
    }
}
