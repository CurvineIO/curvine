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

use crate::{err_box, try_err, try_log, CommonResult};
use log::{error, warn};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender};
use std::sync::{mpsc, Arc};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

type Task = Box<dyn FnOnce() + 'static + Send>;

#[derive(Debug)]
pub struct SingleExecutor {
    name: String,
    channel_size: usize,
    ctl: Arc<AtomicU8>,
    sender: SyncSender<Task>,
    handler: Option<JoinHandle<()>>,
}

impl SingleExecutor {
    pub const ST_INIT: u8 = 0;
    pub const ST_RUNNING: u8 = 1;
    pub const ST_SHUTDOWN: u8 = 2;
    pub const ST_STOP: u8 = 3;

    pub fn new<T: AsRef<str>>(name: T, channel_size: usize) -> Self {
        let (sender, receiver) = mpsc::sync_channel(channel_size);
        let ctl = Arc::new(AtomicU8::new(Self::ST_RUNNING));
        let handler = Self::create_thread(name.as_ref().to_string(), receiver, ctl.clone());
        Self {
            name: name.as_ref().to_string(),
            channel_size,
            ctl,
            sender,
            handler: Some(handler),
        }
    }

    pub fn spawn<F>(&self, task: F) -> CommonResult<()>
    where
        F: FnOnce() + Send + 'static,
    {
        try_err!(self.sender.send(Box::new(task)));
        Ok(())
    }

    pub fn spawn_blocking<F, R>(&self, task: F) -> CommonResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = mpsc::sync_channel(1);
        let task = move || {
            let res = task();
            let res = tx.send(res);
            let _ = try_log!(res);
        };

        try_err!(self.sender.send(Box::new(task)));

        match rx.recv() {
            Ok(v) => Ok(v),
            Err(e) => err_box!("{}", e),
        }
    }

    pub fn thread_name(&self) -> &str {
        &self.name
    }

    pub fn channel_size(&self) -> usize {
        self.channel_size
    }

    pub fn is_stop(&self) -> bool {
        self.ctl.load(Ordering::SeqCst) == Self::ST_STOP
    }

    pub fn is_shutdown(&self) -> bool {
        self.ctl.load(Ordering::SeqCst) >= Self::ST_SHUTDOWN
    }

    fn create_thread(name: String, receiver: Receiver<Task>, ctl: Arc<AtomicU8>) -> JoinHandle<()> {
        let builder = thread::Builder::new().name(name.clone());

        let poll_time = Duration::from_millis(300);
        let res = builder.spawn(move || {
            ctl.store(Self::ST_RUNNING, Ordering::SeqCst);

            while ctl.load(Ordering::SeqCst) <= Self::ST_SHUTDOWN {
                loop {
                    match receiver.recv_timeout(poll_time) {
                        Ok(task) => task(),
                        Err(RecvTimeoutError::Timeout) => break,
                        Err(RecvTimeoutError::Disconnected) => {
                            // The channel has been closed
                            error!("thread {} abnormal shutdown", name);
                            ctl.store(Self::ST_STOP, Ordering::SeqCst);
                            break;
                        }
                    }
                }
            }
        });

        res.unwrap()
    }
}

/// SingleThread is a multi-producer, single-consumer model.
/// In multithreading, it is usually used with Arc smart pointers. If you want to safely stop threads (to ensure that the task execution in the queue is completed),
/// Then execute drop() in advance to ensure that Arc reference is 0 and automatically recycled.
/// There is an ownership problem when calling executor.shutdown to perform recycle like java, not the way rust should choose.
impl Drop for SingleExecutor {
    fn drop(&mut self) {
        if self.is_shutdown() {
            return;
        }

        self.ctl.store(Self::ST_SHUTDOWN, Ordering::SeqCst);
        while self.sender.try_send(Box::new(|| {})).is_ok() {}

        self.ctl.store(Self::ST_STOP, Ordering::SeqCst);
        match self.handler.take() {
            None => {
                error!("Thread {} does not appear to exist", self.name)
            }

            Some(v) => match v.join() {
                Ok(_) => (),
                Err(_) => warn!(
                    "Thread {} waits for shutdown and exception occurs",
                    self.name
                ),
            },
        }
    }
}
