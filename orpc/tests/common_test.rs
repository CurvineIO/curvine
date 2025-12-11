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

use orpc::message::{RequestStatus, ResponseStatus, Status};
use orpc::sys::RawPtr;

#[test]
fn test_rpc_status_encoding_and_decoding() {
    let status = Status(RequestStatus::Running, ResponseStatus::Error);
    let code = status.encode();

    println!("code = {}", code);
    assert_eq!(code, 19);

    let new_status = Status::from(code);
    println!("new_status = {:?}", new_status);
    assert_eq!(new_status.0, RequestStatus::Running);
    assert_eq!(new_status.1, ResponseStatus::Error);
}

struct Point(i32, i32);

impl Point {
    pub fn show(&self) {
        println!("x = {}, y = {}", self.0, self.1)
    }
}

#[tokio::test]
async fn test_raw_ptr_cross_thread_sharing() {
    let p = Point(1, 1);
    let my_box = RawPtr::from_ref(&p);

    let p = my_box.clone();
    let _ = tokio::spawn(async move { p.as_mut().show() }).await;

    my_box.as_mut().show();

    my_box.as_mut().show();
}
