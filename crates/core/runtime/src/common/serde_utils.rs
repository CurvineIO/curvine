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

use crate::CommonResult;

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

#[cfg(test)]
mod tests {
    use super::SerdeUtils;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Sample {
        id: u64,
        name: String,
        tags: Vec<String>,
        size: Option<i32>,
    }

    fn sample() -> Sample {
        Sample {
            id: 42,
            name: "block".to_string(),
            tags: vec!["a".to_string(), "b".to_string()],
            size: Some(-7),
        }
    }

    #[test]
    fn bincode_round_trips() {
        let value = sample();
        let bytes = SerdeUtils::serialize(&value).unwrap();
        let back: Sample = SerdeUtils::deserialize(&bytes).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn bincode_round_trips_through_a_writer_and_a_reader() {
        let value = sample();
        let mut buf: Vec<u8> = Vec::new();
        SerdeUtils::serialize_into(&mut buf, &value).unwrap();
        assert_eq!(buf, SerdeUtils::serialize(&value).unwrap());
        let back: Sample = SerdeUtils::deserialize_from(&buf[..]).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn json_round_trips() {
        let value = sample();
        let bytes = SerdeUtils::serialize_json(&value).unwrap();
        let back: Sample = SerdeUtils::deserialize_json(&bytes).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn json_round_trips_through_a_writer_and_a_reader() {
        let value = sample();
        let mut buf: Vec<u8> = Vec::new();
        SerdeUtils::serialize_json_into(&mut buf, &value).unwrap();
        assert_eq!(buf, SerdeUtils::serialize_json(&value).unwrap());
        let back: Sample = SerdeUtils::deserialize_json_from(&buf[..]).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn malformed_bincode_is_an_error() {
        let bytes = SerdeUtils::serialize(&sample()).unwrap();
        // One byte short of the trailing i32.
        let truncated = &bytes[..bytes.len() - 1];
        // An Option tag other than 0 or 1; the tag precedes the trailing i32.
        let mut bad_tag = bytes.clone();
        let tag_at = bad_tag.len() - 5;
        bad_tag[tag_at] = 2;
        for bad in [truncated, &bad_tag[..]] {
            let parsed = SerdeUtils::deserialize::<Sample>(bad);
            assert!(parsed.is_err(), "{bad:?}");
            let read = SerdeUtils::deserialize_from::<_, Sample>(bad);
            assert!(read.is_err(), "{bad:?}");
        }
    }

    #[test]
    fn malformed_json_is_an_error() {
        let truncated: &[u8] = br#"{"id": 42, "name": "#;
        let wrong_type: &[u8] = br#"{"id": "x"}"#;
        for bad in [truncated, wrong_type] {
            let parsed = SerdeUtils::deserialize_json::<Sample>(bad);
            assert!(parsed.is_err(), "{bad:?}");
            let read = SerdeUtils::deserialize_json_from::<_, Sample>(bad);
            assert!(read.is_err(), "{bad:?}");
        }
    }
}
