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

use curvine_sys::RawPtr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct Value(Arc<AtomicUsize>);

impl Drop for Value {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn take_transfers_ownership_and_drops_value_once() {
    let drops = Arc::new(AtomicUsize::new(0));
    let ptr = RawPtr::from_owned(Value(drops.clone()));
    let value = ptr.take();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(value);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
#[should_panic(expected = "does not own the value")]
fn borrowed_pointer_cannot_transfer_ownership() {
    let value = 1;
    RawPtr::from_ref(&value).take();
}
