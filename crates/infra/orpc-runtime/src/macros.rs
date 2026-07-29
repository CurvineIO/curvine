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

#![allow(unused_macros)]

// Create an error message and add the wrong thread, row, and column information.
macro_rules! err_msg {
    ($e:expr) => ({
        let thread = $crate::thread_name();
        format!("[{}] ERROR: {}({}:{})", thread, $e, file!(), line!())
    });

    ($f:tt, $($arg:expr),+) => ({
        let thread = $crate::thread_name();
        format!("[{}] ERROR: {}({}:{})", thread, format!($f, $($arg),+), file!(), line!())
    });
}

// Convert the error type and add the error message the thread id, file name and line number that occurred.
// There are three forms as follows:
// 1. String error: err_box!("{}", "error")
// 2. Error error: err_box!(std::error::Error)
// @todo 1. The function name cannot be obtained.2. The call stack information cannot be obtained.
macro_rules! err_box {
    ($e:expr) => ({
        Err(err_msg!($e).into())
    });

    ($f:tt, $($arg:expr),+) => ({
        err_box!(format!($f, $($arg),+))
    });
}

// Unified conversion error type.
macro_rules! try_err {
    ($expr:expr) => {{
        match $expr {
            Ok(r) => r,
            Err(e) => return err_box!(e),
        }
    }};
}

macro_rules! try_err_opt {
    ($expr:expr) => {{
        match $expr {
            Err(e) => return err_box!(e),
            Ok(None) => return Ok(None),
            Ok(Some(res)) => res,
        }
    }};
}

/// Equivalent to:
/// match x.as_ref() {
///     None => return err_box!("Uninitialized"),
///     Some(v) => v
/// };
macro_rules! try_option {
    ($expr:expr) => {{
        match $expr {
            None => return err_box!("Uninitialized"),
            Some(res) => res,
        }
    }};

   ($expr:expr, $f:tt, $($arg:expr),+) => ({
        match $expr {
            None => return err_box!(format!($f, $($arg),+)),
            Some(res) => res,
        }
    });
}

macro_rules! try_option_ref {
    ($expr:expr) => {{
        try_option!($expr.as_ref())
    }};
}

macro_rules! try_option_mut {
    ($expr:expr) => {{
        try_option!($expr.as_mut())
    }};
}

macro_rules! option_len {
    ($expr:expr) => {{
        match $expr.as_ref() {
            None => 0,
            Some(v) => v.len(),
        }
    }};
}

// Match Result, if Err, log and return the default value.
// If Ok, return the value.
// Used in some cases no error is returned.
macro_rules! try_log {
    ($expr:expr, $d:expr) => {{
        match $expr {
            Err(e) => {
                log::warn!("{}", err_msg!(e));
                $d
            }
            Ok(res) => res,
        }
    }};

    // Just record the error log and do nothing else.
    ($expr:expr) => {{
        if let Err(e) = &$expr {
            log::warn!("{}", e);
        }
        $expr
    }};
}

macro_rules! try_finally {
    ($expr:expr, $d:expr) => {{
        match $expr {
            Err(e) => {
                $d;
                Err(e)
            }
            Ok(res) => Ok(res),
        }
    }};
}

/// Create a tokio timeout future, which is equivalent to the following code:
macro_rules! timeout {
    ($dur:expr, $future:expr) => {{
        let inner = tokio::time::timeout($dur, $future);
        match inner.await {
            Ok(v) => v,
            Err(e) => err_box!(e),
        }
    }};
}

// Give an ErrorExt type to increase the context in which the current error occurs
macro_rules! err_ext {
    ($e:expr) => {{
        use orpc_error::ErrorExt;
        let ctx: String = format!("({}:{})", file!(), line!());
        Err($e.ctx(ctx))
    }};
}

// Give a Result<ErrorExt, T> type to increase the context in which the current error occurs
macro_rules! result_ext {
    ($e:expr) => {{
        use orpc_error::ResultExt;
        let ctx: String = format!("({}:{})", file!(), line!());
        $e.ctx(ctx)
    }};
}

// Avoid double panic, usually used in drop.
macro_rules! safe_panic {
    () => ({
        safe_panic!("explicit panic")
    });
    ($msg:expr) => ({
        if std::thread::panicking() {
            use log::error;
            error!(concat!($msg, ", double panic prevented"))
        } else {
            panic!($msg)
        }
    });
    ($fmt:expr, $($args:tt)+) => ({
        if std::thread::panicking() {
            use log::error;
            error!(concat!($fmt, ", double panic prevented"), $($args)+)
        } else {
            panic!($fmt, $($args)+)
        }
    });
}

macro_rules! ternary {
    ($condition:expr, $true_expr:expr, $false_expr:expr) => {
        if $condition {
            $true_expr
        } else {
            $false_expr
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::common::{Logger, Utils};
    use crate::CommonResult;
    use std::fs::File;
    use std::io::{Error, ErrorKind};
    use std::panic;

    #[test]
    fn err_box() {
        let err_str: CommonResult<()> = err_box!("not found file: {}", "/d1.log");
        println!("err_str = {:?}", err_str);
        assert!(err_str.is_err());

        let err_box: CommonResult<()> =
            err_box!(Error::new(ErrorKind::ConnectionReset, "connection reset"));
        println!("err_box = {:?}", err_box);
        assert!(err_box.is_err());
    }

    fn open() -> CommonResult<File> {
        let f = try_err!(File::open("xxxxx.log"));

        Ok(f)
    }

    #[test]
    fn try1() {
        let x = open();
        println!("{:?}", x)
    }

    #[test]
    fn try_opt() -> CommonResult<()> {
        let mut x = Some("123".to_string());
        let v = try_option!(x.as_mut());
        println!("{}", v);

        Ok(())
    }

    #[test]
    fn safe_panic() {
        Logger::default();
        struct S;
        impl Drop for S {
            fn drop(&mut self) {
                safe_panic!("safe panic on drop");
            }
        }

        let res = Utils::recover_panic(|| {
            let _s = S;
            panic!("first panic");
        });
        res.unwrap_err();
    }
}
