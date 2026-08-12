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

mod common_error_ext;
pub use self::common_error_ext::CommonErrorExt;

mod error_decoder;
pub use self::error_decoder::ErrorDecoder;

mod error_encoder;
pub use self::error_encoder::ErrorEncoder;

mod error_ext;
pub use self::error_ext::ErrorExt;

mod error_impl;
pub use self::error_impl::ErrorImpl;

mod result_ext;
pub use self::result_ext::ResultExt;

mod string_error;
pub use self::string_error::StringError;

pub type CommonError = Box<dyn std::error::Error + Send + Sync>;
pub type CommonResult<T> = Result<T, CommonError>;
pub type CommonResultExt<T> = Result<T, CommonErrorExt>;

#[macro_export]
macro_rules! err_msg {
    ($e:expr) => {{
        let thread = ::std::thread::current()
            .name()
            .unwrap_or("unknown")
            .to_string();
        format!("[{}] ERROR: {}({}:{})", thread, $e, file!(), line!())
    }};

    ($f:tt, $($arg:expr),+) => {{
        let thread = ::std::thread::current()
            .name()
            .unwrap_or("unknown")
            .to_string();
        format!(
            "[{}] ERROR: {}({}:{})",
            thread,
            format!($f, $($arg),+),
            file!(),
            line!()
        )
    }};
}

#[macro_export]
macro_rules! err_box {
    ($e:expr) => {{
        Err($crate::err_msg!($e).into())
    }};

    ($f:tt, $($arg:expr),+) => {{
        $crate::err_box!(format!($f, $($arg),+))
    }};
}

#[macro_export]
macro_rules! try_err {
    ($expr:expr) => {{
        match $expr {
            Ok(result) => result,
            Err(error) => return $crate::err_box!(error),
        }
    }};
}

#[macro_export]
macro_rules! try_err_opt {
    ($expr:expr) => {{
        match $expr {
            Err(error) => return $crate::err_box!(error),
            Ok(None) => return Ok(None),
            Ok(Some(result)) => result,
        }
    }};
}

#[macro_export]
macro_rules! try_option {
    ($expr:expr) => {{
        match $expr {
            None => return $crate::err_box!("Uninitialized"),
            Some(result) => result,
        }
    }};

    ($expr:expr, $f:tt, $($arg:expr),+) => {{
        match $expr {
            None => return $crate::err_box!(format!($f, $($arg),+)),
            Some(result) => result,
        }
    }};
}

#[macro_export]
macro_rules! try_option_ref {
    ($expr:expr) => {{
        $crate::try_option!($expr.as_ref())
    }};
}

#[macro_export]
macro_rules! try_option_mut {
    ($expr:expr) => {{
        $crate::try_option!($expr.as_mut())
    }};
}

#[macro_export]
macro_rules! option_len {
    ($expr:expr) => {{
        match $expr.as_ref() {
            None => 0,
            Some(value) => value.len(),
        }
    }};
}

#[macro_export]
macro_rules! err_ext {
    ($e:expr) => {{
        use $crate::ErrorExt;
        let ctx: String = format!("({}:{})", file!(), line!());
        Err($e.ctx(ctx))
    }};
}

#[macro_export]
macro_rules! result_ext {
    ($e:expr) => {{
        use $crate::ResultExt;
        let ctx: String = format!("({}:{})", file!(), line!());
        $e.ctx(ctx)
    }};
}

#[macro_export]
macro_rules! timeout {
    ($dur:expr, $future:expr) => {{
        let inner = tokio::time::timeout($dur, $future);
        match inner.await {
            Ok(value) => value,
            Err(error) => $crate::err_box!(error),
        }
    }};
}

#[macro_export]
macro_rules! ternary {
    ($condition:expr, $true_expr:expr, $false_expr:expr) => {
        if $condition {
            $true_expr
        } else {
            $false_expr
        }
    };
}
