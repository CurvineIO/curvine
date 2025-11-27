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

use crate::MyError::{FileExist, NotLeader, IO};
use bytes::BytesMut;
use orpc::error::{ErrorDecoder, ErrorExt, ErrorImpl, StringError};
use orpc::{err_box, err_ext, result_ext, CommonResult};
use std::io::ErrorKind;
use thiserror::Error as ThisError;

// step1: Use enumeration to define an error type.
#[derive(ThisError, Debug)]
pub enum MyError {
    #[error("{0}")]
    FileExist(ErrorImpl<StringError>),

    #[error("{0}")]
    IO(ErrorImpl<std::io::Error>),

    #[error("{0}")]
    NotLeader(ErrorImpl<StringError, u64>),
}

impl MyError {
    pub fn file_exits(file: &str) -> Self {
        FileExist(ErrorImpl::with_source(file.into()))
    }

    pub fn io(error: std::io::Error) -> Self {
        IO(ErrorImpl::with_source(error))
    }

    pub fn not_leader(id: u64) -> Self {
        NotLeader(ErrorImpl::with_data("not a leader".into(), id))
    }
}

// step2: Implement the ErrorExt interface.
impl ErrorExt for MyError {
    fn ctx(self, msg: impl Into<String>) -> Self {
        match self {
            FileExist(e) => FileExist(e.ctx(msg)),

            IO(e) => IO(e.ctx(msg)),

            NotLeader(e) => NotLeader(e.ctx(msg)),
        }
    }

    fn encode(&self) -> BytesMut {
        match self {
            FileExist(e) => e.encode(1),
            IO(e) => e.encode(2),
            NotLeader(e) => e.encode(3),
        }
    }

    fn decode(bytes: BytesMut) -> Self {
        let decoder = ErrorDecoder::new(bytes);
        match &decoder.kind {
            1 => FileExist(decoder.into_string()),

            2 => IO(decoder.into_io()),

            3 => NotLeader(decoder.into_string()),
            _ => panic!("Unknown type id  {}", decoder.kind),
        }
    }
}

// step 3: Call the err_ctx macro to automatically add the wrong context information.
fn fun1() -> Result<i32, MyError> {
    result_ext!(fun2())
}

fn fun2() -> Result<i32, MyError> {
    err_ext!(MyError::file_exits("/a.log"))
}

#[test]
fn test_error_context_propagation_through_call_stack() {
    match fun1() {
        Ok(_) => println!("ok"),
        Err(e) => println!("xxx {}", e),
    }
}

#[test]
fn test_error_io_encoding_decoding_and_type_matching() -> CommonResult<()> {
    let error = std::io::Error::new(ErrorKind::AlreadyExists, "already_exists");
    let error = MyError::io(error);
    println!("io error: {}", error);

    let bytes = error.encode();
    let error = MyError::decode(bytes);

    println!("io error: {}", error);
    match error {
        IO(_) => {
            println!("is io error")
        }

        _ => return err_box!("not io error"),
    }

    Ok(())
}

#[test]
fn test_error_with_data_encoding_decoding_and_retrieval() -> CommonResult<()> {
    let error1 = MyError::not_leader(100);
    let bytes = error1.encode();

    let error2 = MyError::decode(bytes);
    match error2 {
        NotLeader(e) => {
            println!("error {}, data {:?}", e, e.data);
            assert_eq!(100, e.data.unwrap())
        }

        _ => return err_box!("not right error"),
    }

    Ok(())
}
