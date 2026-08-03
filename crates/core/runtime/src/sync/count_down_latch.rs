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

use std::sync::{Condvar, Mutex};

pub struct CountDownLatch {
    mutex: Mutex<u32>,
    cond_var: Condvar,
}

impl CountDownLatch {
    pub fn new(count: u32) -> Self {
        CountDownLatch {
            mutex: Mutex::new(count),
            cond_var: Condvar::new(),
        }
    }

    pub fn count_down(&self) {
        let mut count = self.mutex.lock().unwrap();
        *count -= 1;
        if *count == 0 {
            self.cond_var.notify_all();
        }
    }

    // Condvar will release the lock when waiting, and when it is notified to wake up, it will regain the lock, thus ensuring concurrency security.
    pub fn wait(&self) {
        let lock = self.mutex.lock().unwrap();
        let _unused = self.cond_var.wait(lock).unwrap();
    }
}

#[cfg(test)]
mod test {
    use crate::sync::count_down_latch::CountDownLatch;
    use std::sync::Arc;

    #[test]
    fn count_down_latch() {
        let latch = Arc::new(CountDownLatch::new(5));

        for _ in 0..5 {
            let c_latch = latch.clone();
            std::thread::spawn(move || {
                c_latch.count_down();
            });
        }

        latch.wait();
    }
}
