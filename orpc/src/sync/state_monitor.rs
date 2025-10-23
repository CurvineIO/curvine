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

use crate::{err_box, CommonResult};
use log::error;
use std::sync::atomic::{AtomicI8, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

const ORDER: Ordering = Ordering::SeqCst;

#[derive(Clone)]
pub struct StateCtl {
    ctl: Arc<AtomicI8>,
    writable: bool,
}

impl StateCtl {
    pub fn new(init: i8) -> Self {
        Self {
            ctl: Arc::new(AtomicI8::new(init)),
            writable: true,
        }
    }

    pub fn read_only(&self) -> Self {
        Self {
            ctl: self.ctl.clone(),
            writable: false,
        }
    }

    pub fn compare_and_set(&self, cur: i8, target: i8) -> bool {
        let res = self.ctl.compare_exchange(cur, target, ORDER, ORDER);
        res.is_ok()
    }

    pub fn advance_state<T: Into<i8>>(&self, target_state: T) {
        // If the current state is not writable, then the task will not change.
        if !self.writable {
            return;
        }

        let target: i8 = target_state.into();
        loop {
            let cur = self.ctl.load(ORDER);
            if cur == target || self.compare_and_set(cur, target) {
                break;
            }
        }
    }

    pub fn set_state<T: Into<i8>>(&self, target_state: T) {
        self.ctl.store(target_state.into(), ORDER);
    }

    pub fn state<T: From<i8>>(&self) -> T {
        T::from(self.ctl.load(ORDER))
    }

    pub fn value(&self) -> i8 {
        self.ctl.load(ORDER)
    }
}

#[derive(Clone)]
pub struct StateMonitor {
    ctl: StateCtl,
    sender: broadcast::Sender<i8>,
}

impl StateMonitor {
    pub fn new(init: i8) -> Self {
        let (sender, _) = broadcast::channel(1);
        Self {
            ctl: StateCtl::new(init),
            sender,
        }
    }

    pub fn new_listener(&self) -> StateListener {
        StateListener::new(self.sender.subscribe())
    }

    pub fn read_ctl(&self) -> StateCtl {
        self.ctl.read_only()
    }

    // Send a notification.If there is no recipient, then ignore it.
    fn send_check(&self, target: i8) {
        if self.sender.receiver_count() > 0 {
            if let Err(e) = self.sender.send(target) {
                error!("advance stat to {:?}: {}", target, e)
            }
        }
    }

    pub fn advance_state<T: Into<i8>>(&self, target: T, broadcast: bool) {
        let target: i8 = target.into();
        loop {
            let cur = self.ctl.value();
            if cur == target {
                break;
            } else if self.ctl.compare_and_set(cur, target) {
                if broadcast {
                    self.send_check(target);
                }
                break;
            }
        }
    }

    pub fn state<T: From<i8>>(&self) -> T {
        self.ctl.state()
    }

    pub fn value(&self) -> i8 {
        self.ctl.value()
    }
}

pub struct StateListener {
    receiver: broadcast::Receiver<i8>,
}

impl StateListener {
    pub fn new(receiver: broadcast::Receiver<i8>) -> Self {
        Self { receiver }
    }

    // Get the next state.
    pub async fn next_state(&mut self) -> CommonResult<i8> {
        loop {
            match self.receiver.recv().await {
                Ok(v) => return Ok(v),
                Err(RecvError::Lagged(_)) => continue,
                Err(e) => return err_box!("{}", e),
            }
        }
    }

    // Wait for a certain state.
    pub async fn wait_state<T: Into<i8>>(&mut self, target: T) -> CommonResult<()> {
        let target = target.into();
        loop {
            let cur = self.next_state().await?;
            if cur == target {
                return Ok(());
            } else {
                continue;
            }
        }
    }

    // Wait for a state of progressive evolution.
    pub async fn wait_progress<T: Into<i8>>(&mut self, target: T) -> CommonResult<()> {
        let target = target.into();
        loop {
            let cur = self.next_state().await?;
            if cur >= target {
                return Ok(());
            } else {
                continue;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::{AsyncRuntime, RpcRuntime};
    use crate::sync::StateMonitor;

    #[test]
    fn shutdown_test() {
        let monitor = StateMonitor::new(10);
        let mut listener = monitor.new_listener();
        let rt = AsyncRuntime::single();

        let monitor1 = monitor.clone();
        rt.block_on(async move {
            monitor1.advance_state(1, false);
            monitor1.advance_state(2, true);
            assert!(monitor1.value() == 2);
        });

        rt.block_on(async move {
            listener.wait_progress(2).await.unwrap();
            println!("server is shutdown");
        });
    }
}
