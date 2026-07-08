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

use orpc::sync::ArcRwLock;

#[test]
#[should_panic(expected = "ArcRwLock reentrant read")]
fn arc_rw_lock_reentrant_read_panics_in_debug_builds() {
    let lock = ArcRwLock::new(1);
    let _read_guard = lock.read();

    let _reentrant = lock.read();
}

#[test]
#[should_panic(expected = "ArcRwLock reentrant read")]
fn arc_rw_lock_read_while_holding_write_panics_in_debug_builds() {
    let lock = ArcRwLock::new(1);
    let _write_guard = lock.write();

    let _reentrant = lock.read();
}

#[test]
fn arc_rw_lock_can_reacquire_after_guard_drop() {
    let lock = ArcRwLock::new(1);
    {
        let read_guard = lock.read();
        assert_eq!(*read_guard, 1);
    }

    let mut write_guard = lock.write();
    *write_guard = 2;
    drop(write_guard);

    let read_guard = lock.read();
    assert_eq!(*read_guard, 2);
}
