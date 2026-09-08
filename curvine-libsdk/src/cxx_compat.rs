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

use std::ffi::c_int;
use std::ffi::c_void;

// Older glibc releases such as CentOS 7's 2.17 do not export
// `__cxa_thread_atexit_impl`, while newer libstdc++ static archives may
// reference it. Provide a local fallback so JNI can still load on those hosts.
//
// The fallback degrades thread-local destructor timing to process-exit timing
// via `__cxa_atexit`, which is sufficient for this SDK because we do not rely
// on prompt TLS destructor execution for correctness.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn __cxa_thread_atexit_impl(
    dtor: extern "C" fn(*mut c_void),
    obj: *mut c_void,
    dso_symbol: *mut c_void,
) -> c_int {
    unsafe extern "C" {
        fn __cxa_atexit(
            func: extern "C" fn(*mut c_void),
            arg: *mut c_void,
            dso_handle: *mut c_void,
        ) -> c_int;
    }

    // SAFETY: Mirrors the C ABI of `__cxa_atexit`. The callback and payload are
    // provided by libstdc++ for TLS cleanup registration.
    unsafe { __cxa_atexit(dtor, obj, dso_symbol) }
}

// glibc 2.17 does not export `gettid`, but some transitive native dependencies
// may still reference it directly. Provide a small compatibility shim that
// delegates to the raw syscall.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gettid() -> libc::pid_t {
    // SAFETY: `SYS_gettid` takes no additional arguments and returns the
    // calling thread id. Casting the raw syscall result to `pid_t` matches the
    // Linux ABI used by glibc's newer `gettid` wrapper.
    unsafe { libc::syscall(libc::SYS_gettid as libc::c_long) as libc::pid_t }
}
