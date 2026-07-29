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

macro_rules! err_msg {
    ($e:expr) => {{
        let thread = orpc_runtime::thread_name();
        format!("[{}] ERROR: {}({}:{})", thread, $e, file!(), line!())
    }};

    ($f:tt, $($arg:expr),+) => {{
        let thread = orpc_runtime::thread_name();
        format!(
            "[{}] ERROR: {}({}:{})",
            thread,
            format!($f, $($arg),+),
            file!(),
            line!()
        )
    }};
}

macro_rules! err_box {
    ($e:expr) => {{
        Err(err_msg!($e).into())
    }};

    ($f:tt, $($arg:expr),+) => {{
        err_box!(format!($f, $($arg),+))
    }};
}
