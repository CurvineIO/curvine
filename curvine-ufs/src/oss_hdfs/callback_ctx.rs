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

use curvine_common::error::FsError;
use curvine_common::FsResult;
use std::ffi::CStr;
use tokio::sync::Notify;

use super::ffi::JindoStatus;

#[inline]
pub(crate) fn err_from_c(err: *const std::os::raw::c_char) -> Option<String> {
    if err.is_null() {
        None
    } else {
        Some(
            unsafe { CStr::from_ptr(err) }
                .to_string_lossy()
                .into_owned(),
        )
    }
}

/// Generic, allocation-friendly async callback context.
///
/// This allows FFI callbacks to complete an in-flight operation by writing a value and waking
/// the waiter, without needing `oneshot` channels or `Box::from_raw` in the callback.
#[derive(Debug)]
pub(crate) struct CallbackCtx<T> {
    notify: Notify,
    result: std::sync::Mutex<Option<T>>,
}

impl<T> Default for CallbackCtx<T> {
    fn default() -> Self {
        Self {
            notify: Notify::new(),
            result: std::sync::Mutex::new(None),
        }
    }
}

impl<T> CallbackCtx<T> {
    pub(crate) fn reset(&self) {
        if let Ok(mut g) = self.result.lock() {
            *g = None;
        }
    }

    pub(crate) fn complete(&self, v: T) {
        if let Ok(mut g) = self.result.lock() {
            *g = Some(v);
        }
        self.notify.notify_one();
    }

    pub(crate) async fn wait(&self) -> FsResult<T> {
        loop {
            // Register for notification first to avoid missing a wakeup between check and await.
            let notified = self.notify.notified();
            match self.result.lock() {
                Ok(mut g) => {
                    if let Some(v) = g.take() {
                        return Ok(v);
                    }
                }
                Err(_) => return Err(FsError::common("Async callback mutex poisoned")),
            }
            notified.await;
        }
    }
}

/// Convenience wrapper for callbacks returning `(status, i64, err)`.
///
/// Internally this reuses the generic `CallbackCtx<T>` implementation to avoid duplicating
/// the `reset/wait` logic; only the `complete(...)` signature is specialized.
#[derive(Debug, Default)]
pub(crate) struct I64CallbackCtx {
    inner: CallbackCtx<(JindoStatus, i64, Option<String>)>,
}

impl I64CallbackCtx {
    #[inline]
    pub(crate) fn reset(&self) {
        self.inner.reset();
    }

    #[inline]
    pub(crate) fn complete(
        &self,
        status: JindoStatus,
        value: i64,
        err: *const std::os::raw::c_char,
    ) {
        self.inner.complete((status, value, err_from_c(err)));
    }

    #[inline]
    pub(crate) async fn wait(&self) -> FsResult<(JindoStatus, i64, Option<String>)> {
        self.inner.wait().await
    }
}

/// Convenience wrapper for callbacks returning `(status, err)`.
#[derive(Debug, Default)]
pub(crate) struct StatusCallbackCtx {
    inner: CallbackCtx<(JindoStatus, Option<String>)>,
}

impl StatusCallbackCtx {
    #[inline]
    pub(crate) fn reset(&self) {
        self.inner.reset();
    }

    #[inline]
    pub(crate) fn complete(&self, status: JindoStatus, err: *const std::os::raw::c_char) {
        self.inner.complete((status, err_from_c(err)));
    }

    #[inline]
    pub(crate) async fn wait(&self) -> FsResult<(JindoStatus, Option<String>)> {
        self.inner.wait().await
    }
}
