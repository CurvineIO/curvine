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

use crate::{try_err, CommonResult};
use std::sync::{Arc, Condvar, Mutex};

#[derive(Clone)]
pub struct Condition {
    lock: Arc<Mutex<bool>>,
    cond: Arc<Condvar>,
}

impl Condition {
    pub fn new() -> Self {
        Self {
            lock: Arc::new(Mutex::new(false)),
            cond: Arc::new(Condvar::new()),
        }
    }

    pub fn notify_one(&self) -> CommonResult<()> {
        let mut old_source = try_err!(self.lock.lock());
        *old_source = true;
        self.cond.notify_one();
        Ok(())
    }

    pub fn notify_all(&self) -> CommonResult<()> {
        let mut old_source = try_err!(self.lock.lock());
        *old_source = true;
        self.cond.notify_all();
        Ok(())
    }

    pub fn wait(&self) -> CommonResult<()> {
        let lock = try_err!(self.lock.lock());
        let mut value = try_err!(self.cond.wait(lock));
        *value = false;
        Ok(())
    }
}

impl Default for Condition {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::sync::Condition;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn cond() {
        let cond = Condition::new();

        let c1 = cond.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(10));
            c1.notify_one().unwrap()
        });

        cond.wait().unwrap();
        println!("new")
    }
}
