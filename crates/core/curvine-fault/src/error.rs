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

use thiserror::Error;

#[derive(Debug, Error)]
pub enum FaultRuntimeError {
    #[error("unknown fault point: {0}")]
    UnknownPoint(String),

    #[error("invalid fault rule: {0}")]
    InvalidRule(String),
}

#[derive(Debug, Error)]
pub enum FaultHttpError {
    #[error("fault HTTP control requires the `http-server` Cargo feature")]
    FeatureDisabled,

    #[error("fault HTTP bearer token environment variable is not configured")]
    TokenEnvironmentNotConfigured,

    #[error("fault HTTP bearer token environment variable {0} is not set")]
    TokenEnvironmentNotSet(String),

    #[error("fault HTTP bearer token environment variable {0} is empty")]
    EmptyToken(String),
}

#[cfg(feature = "http-client")]
#[derive(Debug, Error)]
pub enum FaultControlError {
    #[error(transparent)]
    Runtime(#[from] FaultRuntimeError),

    #[error("unknown fault target: {0}")]
    UnknownTarget(String),

    #[error("unknown fault rule {rule_id} at target {target}")]
    UnknownRule { target: String, rule_id: String },

    #[error("fault target is not clean: {rules:?}")]
    DirtyTarget { rules: Vec<String> },

    #[error("fault controller transport error: {0}")]
    Transport(String),

    #[error(
        "timed out waiting for rule {rule_id} at target {target}: expected {expected} executions, observed {observed}"
    )]
    WaitTimeout {
        target: String,
        rule_id: String,
        expected: u64,
        observed: u64,
    },
}
