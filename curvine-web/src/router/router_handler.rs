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

use axum::routing::get;
use axum::Router;
use std::thread;

pub trait RouterHandler {
    fn router(&self) -> Router;
}

pub struct TestHandler;

async fn hello() -> &'static str {
    "Hello, World!"
}

async fn sleep() -> &'static str {
    println!("sleep 5s");
    thread::sleep(std::time::Duration::from_secs(5));
    println!("sleep 5s end");
    let _ = 5 / 0; //panic test
    "sleep 5s endHello, World!"
}

impl RouterHandler for TestHandler {
    fn router(&self) -> Router {
        Router::new()
            .route("/hello", get(hello))
            .route("/sleep", get(sleep))
    }
}
