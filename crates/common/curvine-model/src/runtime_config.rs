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

//! Master runtime configuration views for RPC clients (#268).

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeConfigEntry {
    pub key: String,
    pub file_value_json: String,
    pub override_value: Option<String>,
    pub effective_value_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeConfigUpdateResult {
    pub key: String,
    pub previous_value_json: String,
    pub value_json: String,
    pub persistent: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeConfigResetResult {
    pub key: String,
    pub effective_value_json: String,
}
