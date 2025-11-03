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

use num_enum::{FromPrimitive, IntoPrimitive};
use orpc::{err_box, CommonError};
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
    Persist = 2,
    Evict = 3,
    Flush = 4,
}

impl TryFrom<&str> for TtlAction {
    type Error = CommonError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let action = match value.to_uppercase().as_str() {
            "NONE" => TtlAction::None,
            "DELETE" => TtlAction::Delete,
            "PERSIST" => TtlAction::Persist,
            "EVICT" => TtlAction::Evict,
            "FLUSH" => TtlAction::Flush,
            _ => return err_box!("invalid ttl action: {}", value),
        };

        Ok(action)
    }
}
