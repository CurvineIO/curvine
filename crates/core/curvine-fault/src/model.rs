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

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Configuration for exposing the process fault Runtime through HTTP.
///
/// The configuration is framework-level and can be embedded directly in a
/// host application's own configuration model.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct FaultHttpConfig {
    /// Whether the host should expose the fault HTTP routes.
    pub enabled: bool,

    /// Environment variable containing the bearer token.
    ///
    /// The secret itself is deliberately not stored in application config.
    pub auth_token_env: String,
}

/// Validates the minimum point-name contract used by `fault_point!`.
#[doc(hidden)]
pub const fn point_name_is_valid(name: &str) -> bool {
    !name.is_empty()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FaultExecution {
    Sync,
    Async,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum FaultActionKind {
    Record,
    ReturnError,
    Delay,
    Crash,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct FaultPointSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub execution: FaultExecution,
    pub matchers: &'static [&'static str],
    pub actions: &'static [FaultActionKind],
}

#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub struct FaultPointRegistration {
    pub point: &'static FaultPointSpec,
    pub file: &'static str,
    pub line: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct FaultPointView {
    pub name: String,
    pub description: String,
    pub execution: FaultExecution,
    pub matchers: Vec<String>,
    pub actions: Vec<FaultActionKind>,
}

impl From<&FaultPointSpec> for FaultPointView {
    fn from(value: &FaultPointSpec) -> Self {
        Self {
            name: value.name.to_string(),
            description: value.description.to_string(),
            execution: value.execution,
            matchers: value
                .matchers
                .iter()
                .map(|matcher| (*matcher).to_string())
                .collect(),
            actions: value.actions.to_vec(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum FaultValue {
    I64(i64),
    String(String),
    Bool(bool),
}

impl From<i64> for FaultValue {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<i32> for FaultValue {
    fn from(value: i32) -> Self {
        Self::I64(i64::from(value))
    }
}

impl From<i8> for FaultValue {
    fn from(value: i8) -> Self {
        Self::I64(i64::from(value))
    }
}

impl From<u32> for FaultValue {
    fn from(value: u32) -> Self {
        Self::I64(i64::from(value))
    }
}

impl From<String> for FaultValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for FaultValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<bool> for FaultValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct FaultMatcher(BTreeMap<String, FaultValue>);

impl FaultMatcher {
    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: impl Into<FaultValue>,
    ) -> Option<FaultValue> {
        self.0.insert(key.into(), value.into())
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    pub(crate) fn matches(&self, context: &FaultContext) -> bool {
        self.0
            .iter()
            .all(|(key, expected)| context.get(key) == Some(expected))
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct FaultContext(BTreeMap<String, FaultValue>);

impl FaultContext {
    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: impl Into<FaultValue>,
    ) -> Option<FaultValue> {
        self.0.insert(key.into(), value.into())
    }

    pub fn get(&self, key: &str) -> Option<&FaultValue> {
        self.0.get(key)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct FaultTrigger {
    pub after: u64,
    pub max_hits: Option<u64>,
    pub probability: f64,
    pub seed: u64,
}

impl Default for FaultTrigger {
    fn default() -> Self {
        Self {
            after: 0,
            max_hits: None,
            probability: 1.0,
            seed: 0,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FaultErrorSpec {
    pub message: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum FaultAction {
    Record,
    ReturnError { error: FaultErrorSpec },
    Delay { duration_ms: u64 },
    Crash,
}

impl FaultAction {
    pub fn kind(&self) -> FaultActionKind {
        match self {
            Self::Record => FaultActionKind::Record,
            Self::ReturnError { .. } => FaultActionKind::ReturnError,
            Self::Delay { .. } => FaultActionKind::Delay,
            Self::Crash => FaultActionKind::Crash,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct FaultDecision {
    pub rule_id: String,
    pub action: FaultAction,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConfigureFaultRule {
    /// Optional human-readable purpose of the rule.
    #[serde(default)]
    pub description: Option<String>,
    /// Required catalog point name.
    pub point: String,
    /// Optional exact-match fields. An empty matcher selects every context.
    #[serde(default)]
    pub matcher: FaultMatcher,
    /// Optional execution controls. Omitted fields use `FaultTrigger::default`.
    #[serde(default)]
    pub trigger: FaultTrigger,
    /// Required behavior to execute.
    pub action: FaultAction,
}

impl Default for ConfigureFaultRule {
    fn default() -> Self {
        Self {
            description: None,
            point: String::new(),
            matcher: FaultMatcher::default(),
            trigger: FaultTrigger::default(),
            action: FaultAction::Record,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct FaultRuleView {
    pub id: String,
    pub description: Option<String>,
    pub point: String,
    pub matcher: FaultMatcher,
    pub trigger: FaultTrigger,
    pub action: FaultAction,
    pub evaluations: u64,
    pub matches: u64,
    pub executions: u64,
    pub last_evaluated_context: Option<FaultContext>,
    pub last_context: Option<FaultContext>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FaultRuntimeStatus {
    pub points: Vec<FaultPointView>,
    pub rules: Vec<FaultRuleView>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matcher_keeps_flat_json_and_uses_exact_and_semantics() {
        let matcher: FaultMatcher =
            serde_json::from_str(r#"{"enabled":true,"rpc_code":9,"target":"worker-2"}"#).unwrap();
        assert_eq!(
            serde_json::to_value(&matcher).unwrap(),
            serde_json::json!({
                "enabled": true,
                "rpc_code": 9,
                "target": "worker-2",
            })
        );

        let matching = crate::fault_context! {
            "enabled" => true,
            "rpc_code" => 9_i32,
            "target" => "worker-2",
            "unmatched_context_field" => 1_i64,
        };
        assert!(matcher.matches(&matching));

        let different = crate::fault_context! {
            "enabled" => true,
            "rpc_code" => 10_i32,
            "target" => "worker-2",
        };
        assert!(!matcher.matches(&different));
    }

    #[test]
    fn rule_json_requires_point_and_action_and_defaults_optional_fields() {
        let minimal: ConfigureFaultRule = serde_json::from_str(
            r#"{
                "point":"worker.rpc.before_dispatch",
                "action":{
                    "type":"return_error",
                    "error":{"message":"expected"}
                }
            }"#,
        )
        .unwrap();
        assert_eq!(minimal.description, None);
        assert!(matches!(
            minimal.action,
            FaultAction::ReturnError {
                error: FaultErrorSpec { ref message }
            } if message == "expected"
        ));
        assert!(minimal
            .matcher
            .matches(&crate::fault_context! {"rpc_code" => 1_i32}));
        assert_eq!(minimal.trigger, FaultTrigger::default());

        let partial_trigger: ConfigureFaultRule = serde_json::from_str(
            r#"{
                "point":"worker.rpc.before_dispatch",
                "trigger":{"max_hits":1},
                "action":{"type":"record"}
            }"#,
        )
        .unwrap();
        assert_eq!(partial_trigger.trigger.after, 0);
        assert_eq!(partial_trigger.trigger.max_hits, Some(1));
        assert_eq!(partial_trigger.trigger.probability, 1.0);
        assert_eq!(partial_trigger.trigger.seed, 0);

        assert!(
            serde_json::from_str::<ConfigureFaultRule>(r#"{"action":{"type":"record"}}"#).is_err()
        );
        assert!(serde_json::from_str::<ConfigureFaultRule>(
            r#"{"point":"worker.rpc.before_dispatch"}"#
        )
        .is_err());
        assert!(serde_json::from_str::<FaultAction>(
            r#"{"type":"return_error","error":{"kind":"io","message":"legacy"}}"#
        )
        .is_err());
    }
}
