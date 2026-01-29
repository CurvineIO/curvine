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

use orpc::{try_err, CommonResult};

pub struct SerdeUtils;

impl SerdeUtils {
    // The default serialization and deserialization functions. bincode is used here.
    pub fn serialize<T>(value: &T) -> CommonResult<Vec<u8>>
    where
        T: serde::Serialize,
    {
        let bytes = try_err!(bincode::serialize(value));
        Ok(bytes)
    }

    pub fn serialize_into<W, T>(writer: W, value: &T) -> CommonResult<()>
    where
        W: std::io::Write,
        T: serde::Serialize + ?Sized,
    {
        try_err!(bincode::serialize_into(writer, value));
        Ok(())
    }

    pub fn serialize_json<T>(value: &T) -> CommonResult<Vec<u8>>
    where
        T: serde::Serialize,
    {
        let bytes = try_err!(serde_json::to_vec(value));
        Ok(bytes)
    }

    pub fn serialize_json_into<W, T>(writer: W, value: &T) -> CommonResult<()>
    where
        W: std::io::Write,
        T: serde::Serialize + ?Sized,
    {
        try_err!(serde_json::to_writer(writer, value));
        Ok(())
    }

    pub fn deserialize<'a, T>(bytes: &'a [u8]) -> CommonResult<T>
    where
        T: serde::de::Deserialize<'a>,
    {
        let res = try_err!(bincode::deserialize::<T>(bytes));
        Ok(res)
    }

    pub fn deserialize_from<R, T>(reader: R) -> CommonResult<T>
    where
        R: std::io::Read,
        T: serde::de::DeserializeOwned,
    {
        let res = try_err!(bincode::deserialize_from::<R, T>(reader));
        Ok(res)
    }

    pub fn deserialize_json<'a, T>(bytes: &'a [u8]) -> CommonResult<T>
    where
        T: serde::de::Deserialize<'a>,
    {
        let res = try_err!(serde_json::from_slice::<T>(bytes));
        Ok(res)
    }

    pub fn deserialize_json_from<R, T>(reader: R) -> CommonResult<T>
    where
        R: std::io::Read,
        T: serde::de::DeserializeOwned,
    {
        let res = try_err!(serde_json::from_reader::<R, T>(reader));
        Ok(res)
    }
}
