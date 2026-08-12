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

use crate::{
    ConfigureFaultRule, FaultAction, FaultErrorSpec, FaultMatcher, FaultPointSpec,
    FaultRuntimeError, FaultValue, FAULT_POINT_REGISTRATIONS,
};

#[derive(Debug, Clone)]
pub struct FaultRuleBuilder {
    point: Option<&'static FaultPointSpec>,
    rule: ConfigureFaultRule,
}

impl FaultRuleBuilder {
    pub fn named(point_name: impl Into<String>) -> Self {
        let point_name = point_name.into();
        Self {
            point: FAULT_POINT_REGISTRATIONS
                .iter()
                .find(|registration| registration.point.name == point_name)
                .map(|registration| registration.point),
            rule: ConfigureFaultRule {
                point: point_name,
                ..Default::default()
            },
        }
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.rule.description = Some(description.into());
        self
    }

    pub fn matcher(mut self, matcher: FaultMatcher) -> Result<Self, FaultRuntimeError> {
        for key in matcher.keys() {
            self.ensure_matcher(key)?;
        }
        self.rule.matcher = matcher;
        Ok(self)
    }

    pub fn matches(
        mut self,
        key: impl Into<String>,
        value: impl Into<FaultValue>,
    ) -> Result<Self, FaultRuntimeError> {
        let key = key.into();
        self.ensure_matcher(&key)?;
        self.rule.matcher.insert(key, value);
        Ok(self)
    }

    pub fn times(mut self, max_hits: u64) -> Result<Self, FaultRuntimeError> {
        if max_hits == 0 {
            return Err(FaultRuntimeError::InvalidRule(
                "trigger max_hits must be greater than 0".to_string(),
            ));
        }
        self.rule.trigger.max_hits = Some(max_hits);
        Ok(self)
    }

    pub fn after(mut self, matches: u64) -> Self {
        self.rule.trigger.after = matches;
        self
    }

    pub fn probability(mut self, probability: f64, seed: u64) -> Result<Self, FaultRuntimeError> {
        if !(0.0..=1.0).contains(&probability) {
            return Err(FaultRuntimeError::InvalidRule(
                "trigger probability must be between 0 and 1".to_string(),
            ));
        }
        self.rule.trigger.probability = probability;
        self.rule.trigger.seed = seed;
        Ok(self)
    }

    pub fn record(self) -> Result<ConfigureFaultRule, FaultRuntimeError> {
        self.action(FaultAction::Record)
    }

    pub fn return_error(
        self,
        message: impl Into<String>,
    ) -> Result<ConfigureFaultRule, FaultRuntimeError> {
        self.action(FaultAction::ReturnError {
            error: FaultErrorSpec {
                message: message.into(),
            },
        })
    }

    pub fn delay(self, duration_ms: u64) -> Result<ConfigureFaultRule, FaultRuntimeError> {
        self.action(FaultAction::Delay { duration_ms })
    }

    pub fn crash(self) -> Result<ConfigureFaultRule, FaultRuntimeError> {
        self.action(FaultAction::Crash)
    }

    pub fn action(mut self, action: FaultAction) -> Result<ConfigureFaultRule, FaultRuntimeError> {
        let kind = action.kind();
        if self
            .point
            .is_some_and(|point| !point.actions.contains(&kind))
        {
            return Err(FaultRuntimeError::InvalidRule(format!(
                "action {kind:?} is not available at {}",
                self.rule.point
            )));
        }
        self.rule.action = action;
        Ok(self.rule)
    }

    fn ensure_matcher(&self, key: &str) -> Result<(), FaultRuntimeError> {
        if self.point.is_none_or(|point| point.matchers.contains(&key)) {
            Ok(())
        } else {
            Err(FaultRuntimeError::InvalidRule(format!(
                "matcher {key:?} is not available at {}",
                self.rule.point
            )))
        }
    }
}

#[cfg(all(test, feature = "fault-injection"))]
mod tests {
    use super::*;
    use crate::{fault_point, FaultRuntime};

    #[allow(dead_code)]
    fn builder_point(runtime: &FaultRuntime) -> Result<(), String> {
        fault_point! {
            sync,
            name: "client.builder.test",
            description: "builder test",
            runtime: runtime,
            context: {"rpc_code" => 7_i32},
            return_error: |fault| Err(fault.message),
        }
        Ok(())
    }

    #[test]
    fn builder_uses_local_registration_and_defers_unknown_points() {
        let rule = FaultRuleBuilder::named("client.builder.test")
            .matches("rpc_code", 7_i32)
            .unwrap()
            .times(3)
            .unwrap()
            .return_error("expected")
            .unwrap();
        assert_eq!(rule.point, "client.builder.test");
        assert_eq!(rule.matcher, fault_matcher("rpc_code", 7_i32));
        assert_eq!(rule.trigger.max_hits, Some(3));

        assert!(FaultRuleBuilder::named("client.builder.test")
            .times(0)
            .is_err());
        assert!(FaultRuleBuilder::named("client.builder.test")
            .matches("block_id", 1_i64)
            .is_err());

        let remote_only = FaultRuleBuilder::named("worker.remote.only")
            .matches("arbitrary", true)
            .unwrap()
            .return_error("validated by the remote runtime")
            .unwrap();
        assert_eq!(remote_only.point, "worker.remote.only");
    }

    fn fault_matcher(key: &str, value: impl Into<FaultValue>) -> FaultMatcher {
        let mut matcher = FaultMatcher::default();
        matcher.insert(key, value);
        matcher
    }
}
