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

use futures::task::AtomicWaker;
use std::ops::Deref;
use std::os::raw::c_void;

#[derive(Default)]
pub struct RequestWaker(AtomicWaker);

impl RequestWaker {
    pub unsafe extern "C" fn init(request: *mut c_void) {
        (request as *mut Self).write(RequestWaker::default());
    }

    pub unsafe extern "C" fn cleanup(request: *mut c_void) {
        std::ptr::drop_in_place(request as *mut Self)
    }
}

impl Deref for RequestWaker {
    type Target = AtomicWaker;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
