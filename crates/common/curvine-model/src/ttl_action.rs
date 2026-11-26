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

use curvine_codec::{CodecError, CodecResult, Decode, Encode, Reader, Writer};
use curvine_core_error::{err_box, CommonError};
use num_enum::{FromPrimitive, IntoPrimitive};
use serde::{Deserialize, Serialize};

#[repr(i32)]
#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    PartialEq,
    IntoPrimitive,
    FromPrimitive,
    Eq,
    Default,
    Hash,
)]
pub enum TtlAction {
    #[default]
    None = 0,
    Delete = 1,
    Free = 2,
}

impl Encode for TtlAction {
    #[inline(always)]
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        writer.put_i8(*self as i8)
    }
}

impl Decode for TtlAction {
    #[inline(always)]
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        let raw = reader.get_i8()?;
        let value = Self::from(raw as i32);
        if value as i8 != raw {
            return Err(CodecError::invalid_tag("TtlAction", raw as i64));
        }
        Ok(value)
    }
}

impl TryFrom<&str> for TtlAction {
    type Error = CommonError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let action = match value.to_uppercase().as_str() {
            "NONE" => TtlAction::None,
            "DELETE" => TtlAction::Delete,
            "FREE" => TtlAction::Free,
            _ => return err_box!("invalid ttl action: {}", value),
        };

        Ok(action)
    }
}
