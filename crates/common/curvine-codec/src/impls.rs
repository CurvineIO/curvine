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

//! `Encode`/`Decode` implementations for primitives and common containers.
//!
//! Note on `Vec<u8>`: a byte vector is copied in bulk (`u32` length prefix plus
//! one raw copy) through the hidden `Encode::blob_view` / `Decode::decode_vec`
//! hooks, which only `u8` overrides. The bulk path emits exactly the bytes the
//! element-wise path emits, so nothing depends on which one runs; do not override
//! the hooks for another type.
//!
//! Note on `HashMap` / `HashSet`: iteration order is not stable, so the same
//! collection may encode to different bytes across runs. Use `BTreeMap` or a
//! sorted `Vec` when a stable byte representation (golden tests, checksums)
//! is required.

use crate::error::CodecError;
use crate::{CodecResult, MAX_ALLOC};
use crate::{Decode, Encode, Reader, Writer};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

macro_rules! impl_primitive {
    ($($t:ty => $put:ident, $get:ident),* $(,)?) => {
        $(
            impl Encode for $t {
                fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
                    writer.$put(*self)
                }
            }

            impl Decode for $t {
                fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
                    reader.$get()
                }
            }
        )*
    };
}

impl_primitive!(
    i8 => put_i8, get_i8,
    u16 => put_u16, get_u16,
    i16 => put_i16, get_i16,
    u32 => put_u32, get_u32,
    i32 => put_i32, get_i32,
    u64 => put_u64, get_u64,
    i64 => put_i64, get_i64,
    f32 => put_f32, get_f32,
    f64 => put_f64, get_f64,
);

impl Encode for u8 {
    #[inline(always)]
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_u8(*self)
    }

    #[inline(always)]
    fn blob_view(items: &[Self]) -> Option<&[u8]> {
        Some(items)
    }
}

impl Decode for u8 {
    #[inline(always)]
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        reader.get_u8()
    }

    #[inline(always)]
    fn decode_vec(reader: &mut Reader<'_>) -> CodecResult<Option<Vec<Self>>> {
        reader.get_bytes().map(Some)
    }
}

impl Encode for bool {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_bool(*self)
    }
}

impl Decode for bool {
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        reader.get_bool()
    }
}

impl Encode for str {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_str(self)
    }
}

impl Encode for String {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_str(self)
    }
}

impl Decode for String {
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        reader.get_str()
    }
}

impl Encode for [u8] {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_bytes(self)
    }
}

impl<T: Encode, const N: usize> Encode for [T; N] {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        if let Some(bytes) = T::blob_view(self) {
            return writer.put_slice(bytes);
        }
        for item in self {
            item.encode(writer)?;
        }
        Ok(())
    }
}

impl<T: Decode, const N: usize> Decode for [T; N] {
    // Note: `Encode for [T; N]` takes the `blob_view` fast path for `u8`
    // (one `put_slice`), while this decode side stays element-wise. Metadata
    // fixed arrays are small, so the cost is negligible; mirroring the bulk
    // path would add a second hidden `decode_arr::<N>` hook to `Decode` for
    // one use site. Revisit only if profiling shows otherwise.
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        let mut items = Vec::with_capacity(N.min(MAX_ALLOC));
        for _ in 0..N {
            items.push(T::decode(reader)?);
        }
        match items.try_into() {
            Ok(array) => Ok(array),
            Err(_) => Err(CodecError::decode("fixed array length mismatch")),
        }
    }
}

impl Encode for Cow<'_, str> {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_str(self)
    }
}

impl Decode for Cow<'_, str> {
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        Ok(Cow::Owned(reader.get_str()?))
    }
}

impl Encode for Arc<str> {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_str(self)
    }
}

impl Decode for Arc<str> {
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        Ok(Arc::from(reader.get_str()?))
    }
}

impl<T: Encode> Encode for Vec<T> {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        if let Some(bytes) = T::blob_view(self.as_slice()) {
            return writer.put_bytes(bytes);
        }
        writer.put_len(self.len())?;
        for item in self {
            item.encode(writer)?;
        }
        Ok(())
    }
}

impl<T: Decode> Decode for Vec<T> {
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        #[cfg(debug_assertions)]
        let start = reader.position();
        if let Some(vec) = T::decode_vec(reader)? {
            return Ok(vec);
        }

        #[cfg(debug_assertions)]
        debug_assert_eq!(
            reader.position(),
            start,
            "decode_vec returned None but consumed input"
        );
        let len = reader.take_len()?;
        let mut vec = Vec::with_capacity(len.min(reader.remaining()).min(MAX_ALLOC));
        for _ in 0..len {
            vec.push(T::decode(reader)?);
        }
        Ok(vec)
    }
}

impl<T: Encode> Encode for Option<T> {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        match self {
            Some(value) => {
                writer.put_u8(1)?;
                value.encode(writer)
            }
            None => writer.put_u8(0),
        }
    }
}

impl<T: Decode> Decode for Option<T> {
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        match reader.get_bool()? {
            true => Ok(Some(T::decode(reader)?)),
            false => Ok(None),
        }
    }
}

impl<T: Encode + ?Sized> Encode for Box<T> {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        (**self).encode(writer)
    }
}

impl<T: Encode + ?Sized> Encode for &T {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        (**self).encode(writer)
    }
}

impl<T: Decode> Decode for Box<T> {
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        Ok(Box::new(T::decode(reader)?))
    }
}

impl<K: Encode, V: Encode> Encode for HashMap<K, V> {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_len(self.len())?;
        for (key, value) in self {
            key.encode(writer)?;
            value.encode(writer)?;
        }
        Ok(())
    }
}

impl<K: Decode + Eq + std::hash::Hash, V: Decode> Decode for HashMap<K, V> {
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        let len = reader.take_len()?;
        let mut map = HashMap::with_capacity(len.min(reader.remaining()).min(MAX_ALLOC));
        for _ in 0..len {
            let key = K::decode(reader)?;
            let value = V::decode(reader)?;
            map.insert(key, value);
        }
        if map.len() != len {
            return Err(CodecError::decode(format!(
                "duplicate map keys: {} unique entries for declared count {}",
                map.len(),
                len
            )));
        }
        Ok(map)
    }
}

impl<K: Encode, V: Encode> Encode for BTreeMap<K, V> {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_len(self.len())?;
        for (key, value) in self {
            key.encode(writer)?;
            value.encode(writer)?;
        }
        Ok(())
    }
}

impl<K: Decode + Ord, V: Decode> Decode for BTreeMap<K, V> {
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        let len = reader.take_len()?;
        let mut map = BTreeMap::new();
        for _ in 0..len {
            let key = K::decode(reader)?;
            let value = V::decode(reader)?;
            map.insert(key, value);
        }
        if map.len() != len {
            return Err(CodecError::decode(format!(
                "duplicate map keys: {} unique entries for declared count {}",
                map.len(),
                len
            )));
        }
        Ok(map)
    }
}

impl<T: Encode> Encode for HashSet<T> {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_len(self.len())?;
        for item in self {
            item.encode(writer)?;
        }
        Ok(())
    }
}

impl<T: Decode + Eq + std::hash::Hash> Decode for HashSet<T> {
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        let len = reader.take_len()?;
        let mut set = HashSet::with_capacity(len.min(reader.remaining()).min(MAX_ALLOC));
        for _ in 0..len {
            set.insert(T::decode(reader)?);
        }
        if set.len() != len {
            return Err(CodecError::decode(format!(
                "duplicate set entries: {} unique entries for declared count {}",
                set.len(),
                len
            )));
        }
        Ok(set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Codec, CURRENT_VERSION};

    /// Frames a bare payload with a current-version header and decodes it
    /// through the public entry point.
    fn decode_payload<T: Decode>(bytes: &[u8]) -> CodecResult<T> {
        let mut framed = Vec::with_capacity(bytes.len() + 1);
        framed.push(Codec::make_header(CURRENT_VERSION).unwrap());
        framed.extend_from_slice(bytes);
        Codec::decode(&framed)
    }

    #[test]
    fn string_round_trip() {
        let value = String::from("curvine");
        let bytes = Codec::encode_unversioned(value.as_str()).unwrap();
        assert_eq!(
            bytes.as_slice(),
            &[0x07, 0x00, 0x00, 0x00, b'c', b'u', b'r', b'v', b'i', b'n', b'e']
        );
        let decoded: String = decode_payload(&bytes).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn vec_u8_layout_matches_put_bytes() {
        let value: Vec<u8> = vec![0xAA, 0xBB, 0xCC];
        let bytes = Codec::encode_unversioned(&value).unwrap();
        assert_eq!(
            bytes.as_slice(),
            &[0x03, 0x00, 0x00, 0x00, 0xAA, 0xBB, 0xCC]
        );
        let decoded: Vec<u8> = decode_payload(&bytes).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn vec_string_round_trip() {
        let value: Vec<String> = vec![String::from("a"), String::from("bb")];
        let bytes = Codec::encode_unversioned(&value).unwrap();
        let decoded: Vec<String> = decode_payload(&bytes).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn empty_vec_round_trip() {
        let value: Vec<u64> = vec![];
        let bytes = Codec::encode_unversioned(&value).unwrap();
        assert_eq!(bytes.as_slice(), &[0x00, 0x00, 0x00, 0x00]);
        let decoded: Vec<u64> = decode_payload(&bytes).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn option_round_trip() {
        let some: Option<u32> = Some(7);
        let bytes = Codec::encode_unversioned(&some).unwrap();
        assert_eq!(bytes.as_slice(), &[0x01, 0x07, 0x00, 0x00, 0x00]);
        let decoded: Option<u32> = decode_payload(&bytes).unwrap();
        assert_eq!(decoded, some);

        let none: Option<u32> = None;
        let bytes = Codec::encode_unversioned(&none).unwrap();
        assert_eq!(bytes.as_slice(), &[0x00]);
        let decoded: Option<u32> = decode_payload(&bytes).unwrap();
        assert_eq!(decoded, None);
    }

    #[test]
    fn option_rejects_invalid_tag() {
        let bytes = [0x02];
        let result: CodecResult<Option<u8>> = decode_payload(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn box_round_trip() {
        let value: Box<u64> = Box::new(0x0102_0304_0506_0708);
        let bytes = Codec::encode_unversioned(&value).unwrap();
        let decoded: Box<u64> = decode_payload(&bytes).unwrap();
        assert_eq!(*decoded, *value);
    }

    #[test]
    fn hash_map_round_trip() {
        let mut value: HashMap<String, u64> = HashMap::new();
        value.insert(String::from("a"), 1);
        value.insert(String::from("b"), 2);
        let bytes = Codec::encode_unversioned(&value).unwrap();
        let decoded: HashMap<String, u64> = decode_payload(&bytes).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn btree_map_bytes_are_stable() {
        let mut value: BTreeMap<String, u64> = BTreeMap::new();
        value.insert(String::from("a"), 1);
        value.insert(String::from("b"), 2);
        let first = Codec::encode_unversioned(&value).unwrap();
        let second = Codec::encode_unversioned(&value).unwrap();
        assert_eq!(first.as_slice(), second.as_slice());
        // Keys are emitted in sorted order: "a" then "b".
        assert_eq!(
            first.as_slice(),
            &[
                0x02, 0x00, 0x00, 0x00, //
                0x01, 0x00, 0x00, 0x00, b'a', 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, //
                0x01, 0x00, 0x00, 0x00, b'b', 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ]
        );
        let decoded: BTreeMap<String, u64> = decode_payload(&first).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn vec_of_option_round_trip() {
        let value: Vec<Option<i32>> = vec![Some(-1), None, Some(2)];
        let bytes = Codec::encode_unversioned(&value).unwrap();
        let decoded: Vec<Option<i32>> = decode_payload(&bytes).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn byte_slice_matches_put_bytes() {
        let bytes = Codec::encode_unversioned([0xAAu8, 0xBB].as_slice()).unwrap();
        assert_eq!(bytes.as_slice(), &[0x02, 0x00, 0x00, 0x00, 0xAA, 0xBB]);
    }

    #[test]
    fn vec_u8_bulk_copy_matches_element_wise_bytes() {
        let value: Vec<u8> = vec![0xAA, 0x00, 0xFF];
        // The bulk copy must emit what one `put_u8` per element emits: `u32`
        // length prefix, then the raw bytes.
        let mut expect = Writer::new(1).unwrap();
        expect.put_len(value.len()).unwrap();
        for byte in &value {
            expect.put_u8(*byte).unwrap();
        }
        let bytes = Codec::encode_unversioned(&value).unwrap();
        assert_eq!(bytes, expect.into_inner());

        let decoded: Vec<u8> = decode_payload(&bytes).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn vec_u8_rejects_truncated_payload() {
        let mut writer = Writer::new(1).unwrap();
        writer.put_len(4).unwrap();
        writer.put_slice(&[0x01, 0x02]).unwrap();
        let result: CodecResult<Vec<u8>> = decode_payload(&writer.into_inner());
        assert!(result.is_err());
    }

    #[test]
    fn vec_of_wide_elements_stays_element_wise() {
        // Guard: the bulk hook must not hijack a non-`u8` element type.
        let value: Vec<u16> = vec![0x0102, 0x0304];
        let bytes = Codec::encode_unversioned(&value).unwrap();
        assert_eq!(
            bytes.as_slice(),
            &[0x02, 0x00, 0x00, 0x00, 0x02, 0x01, 0x04, 0x03]
        );
        let decoded: Vec<u16> = decode_payload(&bytes).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn map_of_byte_values_keeps_pair_layout() {
        // xattr-shaped field: `u32` pair count, then a `u32`-prefixed key and a
        // `u32`-prefixed byte value.
        let mut map = HashMap::new();
        map.insert(String::from("k"), vec![0xAAu8, 0xBB]);
        let bytes = Codec::encode_unversioned(&map).unwrap();
        assert_eq!(
            bytes.as_slice(),
            &[
                0x01, 0x00, 0x00, 0x00, //
                0x01, 0x00, 0x00, 0x00, b'k', //
                0x02, 0x00, 0x00, 0x00, 0xAA, 0xBB,
            ]
        );
        let decoded: HashMap<String, Vec<u8>> = decode_payload(&bytes).unwrap();
        assert_eq!(decoded, map);
    }

    #[test]
    fn fixed_array_has_no_length_prefix() {
        let value = [0xAAu8, 0xBB, 0xCC];
        let bytes = Codec::encode_unversioned(&value).unwrap();
        assert_eq!(bytes.as_slice(), &[0xAA, 0xBB, 0xCC]);
        let decoded: [u8; 3] = decode_payload(&bytes).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn u8_array_bulk_matches_element_wise_bytes() {
        // The array fast path writes raw bytes with no prefix; it must agree
        // with one `put_u8` per element.
        let value = [0xAAu8, 0x00, 0xFF];
        let mut expect = Writer::new(1).unwrap();
        for byte in &value {
            expect.put_u8(*byte).unwrap();
        }
        let bytes = Codec::encode_unversioned(&value).unwrap();
        assert_eq!(bytes, expect.into_inner());
        let decoded: [u8; 3] = decode_payload(&bytes).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn vec_u8_empty_and_exact_length_round_trip() {
        let empty: Vec<u8> = Vec::new();
        let bytes = Codec::encode_unversioned(&empty).unwrap();
        assert_eq!(bytes.as_slice(), &[0x00, 0x00, 0x00, 0x00]);
        let decoded: Vec<u8> = decode_payload(&bytes).unwrap();
        assert!(decoded.is_empty());

        // A declared length equal to the remaining bytes is legal.
        let mut writer = Writer::new(1).unwrap();
        writer.put_len(3).unwrap();
        writer.put_slice(&[0x01, 0x02, 0x03]).unwrap();
        let decoded: Vec<u8> = decode_payload(&writer.into_inner()).unwrap();
        assert_eq!(decoded, vec![0x01, 0x02, 0x03]);
    }

    #[test]
    fn vec_of_vec_u8_bulks_only_the_inner_vectors() {
        // The hook is keyed on the element type: the outer `Vec<Vec<u8>>` stays
        // element-wise, each inner `Vec<u8>` takes the bulk path.
        let value: Vec<Vec<u8>> = vec![vec![0xAA, 0xBB], Vec::new()];
        let bytes = Codec::encode_unversioned(&value).unwrap();
        assert_eq!(
            bytes.as_slice(),
            &[
                0x02, 0x00, 0x00, 0x00, // outer count
                0x02, 0x00, 0x00, 0x00, 0xAA, 0xBB, // inner: u32 len + raw
                0x00, 0x00, 0x00, 0x00, // empty inner vector
            ]
        );
        let decoded: Vec<Vec<u8>> = decode_payload(&bytes).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn cow_and_arc_str_round_trip() {
        let borrowed: Cow<'_, str> = Cow::Borrowed("cv");
        let bytes = Codec::encode_unversioned(&borrowed).unwrap();
        assert_eq!(bytes.as_slice(), &[0x02, 0x00, 0x00, 0x00, b'c', b'v']);
        let decoded: Cow<'_, str> = decode_payload(&bytes).unwrap();
        assert_eq!(decoded, "cv");

        let shared: Arc<str> = Arc::from("curvine");
        let bytes = Codec::encode_unversioned(&shared).unwrap();
        let decoded: Arc<str> = decode_payload(&bytes).unwrap();
        assert_eq!(&*decoded, "curvine");
    }

    #[test]
    fn vec_decode_fails_on_truncated_element() {
        let mut writer = Writer::new(1).unwrap();
        writer.put_len(2).unwrap();
        writer.put_str("a").unwrap();
        let bytes = writer.into_inner();
        // Declares two strings but only one is present.
        let result: CodecResult<Vec<String>> = decode_payload(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn map_rejects_duplicate_keys() {
        // Declared count 2 but both pairs carry the same key: the bytes are
        // fully consumed yet can never come from a valid encode.
        let bytes = [0x02, 0x00, 0x00, 0x00, 0x01, 0x07, 0x01, 0x08];
        let mut reader = Reader::new(&bytes, 1).unwrap();
        assert!(BTreeMap::<u8, u8>::decode(&mut reader).is_err());
        let mut reader = Reader::new(&bytes, 1).unwrap();
        assert!(HashMap::<u8, u8>::decode(&mut reader).is_err());
    }

    #[test]
    fn ref_vec_encodes_like_owned_vec() {
        let borrowed: Vec<&str> = vec!["a", "bb"];
        let owned: Vec<String> = vec![String::from("a"), String::from("bb")];
        let borrowed_bytes = Codec::encode_unversioned(&borrowed).unwrap();
        assert_eq!(borrowed_bytes, Codec::encode_unversioned(&owned).unwrap());
        // Same layout as a `Vec<String>`: u32 count, u32-prefixed items.
        assert_eq!(
            borrowed_bytes.as_slice(),
            &[
                0x02, 0x00, 0x00, 0x00, //
                0x01, 0x00, 0x00, 0x00, b'a', //
                0x02, 0x00, 0x00, 0x00, b'b', b'b',
            ]
        );
    }

    #[test]
    fn hash_set_decodes_vec_layout() {
        // `HashSet` shares the `Vec` layout (count + length-prefixed items),
        // so it decodes any payload a `Vec` emits.
        let names: Vec<String> = vec![String::from("a"), String::from("bb")];
        let bytes = Codec::encode_unversioned(&names).unwrap();
        let decoded: HashSet<String> = decode_payload(&bytes).unwrap();
        assert_eq!(decoded.len(), 2);
        assert!(decoded.contains("a"));
        assert!(decoded.contains("bb"));
    }

    #[test]
    fn hash_set_rejects_duplicate_entries() {
        // Declared count 2 but both entries are the same element: the bytes
        // are fully consumed yet can never come from a valid encode.
        let bytes = [0x02, 0x00, 0x00, 0x00, 0x07, 0x07];
        let result: CodecResult<HashSet<u8>> = decode_payload(&bytes);
        assert!(result.is_err());
    }
}
