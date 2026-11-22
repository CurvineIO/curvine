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

    /// Decrements the count, waking the waiters when it reaches zero. Once
    /// the count is zero, further calls do nothing.
    pub fn count_down(&self) {
        let mut count = self.mutex.lock().unwrap();
        if *count == 0 {
            return;
        }
        *count -= 1;
        if *count == 0 {
            self.cond_var.notify_all();
        }
    }

    /// Blocks until the count is zero; returns at once if it already is.
    ///
    /// The count is checked under the lock before and after every wakeup. A
    /// single `Condvar::wait` blocked forever when the last `count_down` ran
    /// before `wait` took the lock (its notification had no waiter), and
    /// returned early on a spurious wakeup.
    pub fn wait(&self) {
        let count = self.mutex.lock().unwrap();
        let _count = self.cond_var.wait_while(count, |n| *n > 0).unwrap();
    }
}

#[cfg(test)]
mod test {
    use crate::sync::count_down_latch::CountDownLatch;
    use std::sync::mpsc::{self, Receiver};
    use std::sync::Arc;
    use std::time::Duration;

    /// How long a `wait` that should return may take. If a lost or premature
    /// wakeup comes back, the test fails after this instead of holding the
    /// build job until its timeout.
    const RETURNS_WITHIN: Duration = Duration::from_secs(5);

    /// Runs `latch.wait()` on its own thread; the receiver gets `()` once it
    /// returns.
    fn spawn_wait(latch: &Arc<CountDownLatch>) -> Receiver<()> {
        let (tx, rx) = mpsc::channel();
        let latch = latch.clone();
        std::thread::spawn(move || {
            latch.wait();
            let _ = tx.send(());
        });
        rx
    }

    #[test]
    fn count_down_latch() {
        let latch = Arc::new(CountDownLatch::new(5));

        for _ in 0..5 {
            let c_latch = latch.clone();
            std::thread::spawn(move || {
                c_latch.count_down();
            });
        }

        let done = spawn_wait(&latch);
        assert!(
            done.recv_timeout(RETURNS_WITHIN).is_ok(),
            "wait did not return after five count_down calls"
        );
    }

    /// The count reaches zero before `wait` is called: `wait` must not block.
    /// It used to wait for a notification that had already been sent, which
    /// hung the build job until its timeout.
    #[test]
    fn wait_returns_when_the_count_is_already_zero() {
        let latch = Arc::new(CountDownLatch::new(2));
        latch.count_down();
        latch.count_down();
        let done = spawn_wait(&latch);
        assert!(
            done.recv_timeout(RETURNS_WITHIN).is_ok(),
            "wait blocked although the count was already zero"
        );
    }

    #[test]
    fn count_down_past_zero_does_nothing() {
        let latch = Arc::new(CountDownLatch::new(1));
        latch.count_down();
        latch.count_down();
        let done = spawn_wait(&latch);
        assert!(
            done.recv_timeout(RETURNS_WITHIN).is_ok(),
            "wait blocked after counting down past zero"
        );
    }

    /// `wait` returns only once the count is zero, also when the condition
    /// variable is notified while the count is still positive, as a spurious
    /// wakeup would.
    #[test]
    fn wait_blocks_until_the_count_reaches_zero() {
        let latch = Arc::new(CountDownLatch::new(2));
        let done = spawn_wait(&latch);
        // Let the waiter reach the condition variable before notifying it.
        std::thread::sleep(Duration::from_millis(100));

        latch.count_down();
        latch.cond_var.notify_all();
        assert!(
            done.recv_timeout(Duration::from_millis(200)).is_err(),
            "wait returned while the count was still 1"
        );

        latch.count_down();
        assert!(
            done.recv_timeout(RETURNS_WITHIN).is_ok(),
            "wait did not return after the count reached zero"
        );
    }
}
