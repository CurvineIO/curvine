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

use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    static CURRENT_CLIENT_CONTEXT: RefCell<HashMap<String, String>> =
        RefCell::new(HashMap::new());
}

/// Process- and thread-scoped business context attached to metadata RPCs.
///
/// The context is read when `FsClient::rpc` / `rpc_bytes` build the outgoing
/// message. For async callers, set it on the thread that polls the future; on
/// a multi-threaded Tokio runtime this usually means setting it inside the
/// task or using `block_on` on the calling thread.
pub struct ClientContext;

impl ClientContext {
    pub fn set_current<I, K, V>(tags: I)
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let tags = tags
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        CURRENT_CLIENT_CONTEXT.with(|ctx| *ctx.borrow_mut() = tags);
    }

    pub fn add<K, V>(key: K, value: V)
    where
        K: Into<String>,
        V: Into<String>,
    {
        CURRENT_CLIENT_CONTEXT.with(|ctx| ctx.borrow_mut().insert(key.into(), value.into()));
    }

    pub fn clear_current() {
        CURRENT_CLIENT_CONTEXT.with(|ctx| ctx.borrow_mut().clear());
    }

    pub fn current() -> HashMap<String, String> {
        CURRENT_CLIENT_CONTEXT.with(|ctx| ctx.borrow().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn set_current_replaces_previous_context() {
        ClientContext::set_current([("app_id", "job-1")]);
        ClientContext::add("scenario", "etl");

        let current = ClientContext::current();
        assert_eq!(current.get("app_id").map(String::as_str), Some("job-1"));
        assert_eq!(current.get("scenario").map(String::as_str), Some("etl"));

        ClientContext::set_current([("app_id", "job-2")]);
        let current = ClientContext::current();
        assert_eq!(current.len(), 1);
        assert_eq!(current.get("app_id").map(String::as_str), Some("job-2"));

        ClientContext::clear_current();
        assert!(ClientContext::current().is_empty());
    }

    #[test]
    fn context_is_isolated_per_thread() {
        ClientContext::set_current([("app_id", "main")]);

        let thread_a = thread::spawn(|| {
            ClientContext::set_current([("app_id", "thread-a")]);
            ClientContext::current()
        });
        let thread_b = thread::spawn(|| {
            ClientContext::set_current([("app_id", "thread-b")]);
            ClientContext::current()
        });

        assert_eq!(
            thread_a.join().unwrap().get("app_id").map(String::as_str),
            Some("thread-a")
        );
        assert_eq!(
            thread_b.join().unwrap().get("app_id").map(String::as_str),
            Some("thread-b")
        );
        assert_eq!(
            ClientContext::current().get("app_id").map(String::as_str),
            Some("main")
        );
    }
}
