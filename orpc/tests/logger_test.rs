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

use orpc::common::{LogConf, Logger};
use tracing::dispatcher;

#[test]
fn logger_init_does_not_panic_when_global_subscriber_already_set() {
    let init_result = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::ERROR)
        .with_test_writer()
        .try_init();

    assert!(
        init_result.is_ok() || dispatcher::has_been_set(),
        "expected global tracing subscriber to be installed"
    );
    assert!(dispatcher::has_been_set());

    Logger::init(LogConf::default());
}
