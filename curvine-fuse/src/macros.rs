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

// Create fuse error.
#[macro_export]
macro_rules! err_fuse {
    ($errno:expr) => ({
        let msg = orpc::err_msg!("err_fuse {}", $errno);
        let err = $crate::FuseError::new($errno as i32, msg.into());
        Err(err)
    });

    ($errno:expr, $msg:expr) => ({
        let msg = orpc::err_msg!("{}", $msg);
        let err = $crate::FuseError::new($errno as i32, msg.into());
        Err(err)
    });

    ($errno:expr, $f:tt, $($arg:expr),+) => ({
        let msg = orpc::err_msg!($f, $($arg),+);
        let err = $crate::FuseError::new($errno as i32, msg.into());
        Err(err)
    });
}

#[cfg(test)]
mod test {
    use crate::FuseResult;

    #[test]
    pub fn test() {
        let err1: FuseResult<u32> = err_fuse!(1_usize);
        println!("err1: {:?}", err1);

        let err2: FuseResult<u32> = err_fuse!(2, "error {}", 2);
        println!("err12 {:?}", err2)
    }
}
