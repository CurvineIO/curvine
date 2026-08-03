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

#[cfg(all(test, feature = "fault-injection"))]
use crate::catalog::validate_point;
use crate::catalog::{build_full_catalog, validate_rule_id};
use crate::{
    ConfigureFaultRule, FaultAction, FaultContext, FaultDecision, FaultPointSpec, FaultRuleView,
    FaultRuntimeError, FaultRuntimeStatus, FAULT_POINT_REGISTRATIONS,
};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

#[derive(Debug)]
struct FaultRule {
    id: String,
    description: Option<String>,
    point: String,
    matcher: crate::FaultMatcher,
    trigger: crate::FaultTrigger,
    action: FaultAction,
    evidence: RwLock<FaultEvidence>,
}

#[derive(Debug, Default)]
struct FaultEvidence {
    evaluations: u64,
    matches: u64,
    executions: u64,
    last_evaluated_context: Option<FaultContext>,
    last_context: Option<FaultContext>,
}

impl FaultRule {
    fn view(&self) -> FaultRuleView {
        let evidence = self.evidence.read();
        FaultRuleView {
            id: self.id.clone(),
            description: self.description.clone(),
            point: self.point.clone(),
            matcher: self.matcher.clone(),
            trigger: self.trigger.clone(),
            action: self.action.clone(),
            evaluations: evidence.evaluations,
            matches: evidence.matches,
            executions: evidence.executions,
            last_evaluated_context: evidence.last_evaluated_context.clone(),
            last_context: evidence.last_context.clone(),
        }
    }

    fn trigger_matches(&self, match_number: u64) -> bool {
        if match_number <= self.trigger.after {
            return false;
        }
        if self.trigger.probability < 1.0 {
            let sample = stable_probability_sample(&self.id, self.trigger.seed, match_number);
            if sample >= self.trigger.probability {
                return false;
            }
        }
        true
    }
}

fn stable_probability_sample(rule_id: &str, seed: u64, match_number: u64) -> f64 {
    (stable_probability_hash(rule_id, seed, match_number) >> 11) as f64 / (1_u64 << 53) as f64
}

fn stable_probability_hash(rule_id: &str, seed: u64, match_number: u64) -> u64 {
    // Keep both the byte encoding and finalizer fixed so sampling is stable across builds.
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in rule_id
        .bytes()
        .chain(seed.to_le_bytes())
        .chain(match_number.to_le_bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}

#[derive(Debug)]
struct FaultRuntimeInner {
    catalog: Vec<&'static FaultPointSpec>,
    rules: RwLock<BTreeMap<String, Arc<FaultRule>>>,
}

#[derive(Debug, Clone)]
pub struct FaultRuntime {
    inner: Arc<FaultRuntimeInner>,
}

static PROCESS_FAULT_RUNTIME: LazyLock<FaultRuntime> = LazyLock::new(|| {
    FaultRuntime::isolated()
        .unwrap_or_else(|error| panic!("failed to initialize the process fault runtime: {error}"))
});

impl FaultRuntime {
    /// Creates an isolated runtime containing every fault point linked into
    /// the current binary.
    ///
    /// Isolated runtimes are intended for unit tests that need independent
    /// rule tables. Production call sites use [`Self::process`] implicitly
    /// through [`crate::fault_point!`].
    pub fn isolated() -> Result<Self, FaultRuntimeError> {
        let catalog = build_full_catalog(&FAULT_POINT_REGISTRATIONS)?;
        Ok(Self {
            inner: Arc::new(FaultRuntimeInner {
                catalog,
                rules: RwLock::new(BTreeMap::new()),
            }),
        })
    }

    /// Returns the lazily initialized full-catalog process Runtime.
    ///
    /// Business code does not initialize or carry this Runtime. The default
    /// [`crate::fault_point!`] form evaluates against it automatically. A
    /// conflicting linked-point catalog is a programming error and fails at
    /// first access with both declaration locations in the error message.
    pub fn process() -> &'static Self {
        &PROCESS_FAULT_RUNTIME
    }

    /// Installs or replaces one rule.
    ///
    /// Rules for the same point are evaluated in rule-ID order. `Record`
    /// rules publish evidence and allow evaluation to continue; the first
    /// other rule that executes supplies the behavior decision.
    pub fn configure(
        &self,
        rule_id: impl AsRef<str>,
        request: ConfigureFaultRule,
    ) -> Result<FaultRuleView, FaultRuntimeError> {
        let rule_id = rule_id.as_ref();
        validate_rule_id(rule_id)?;
        let point = self.point_spec(&request.point)?;
        self.validate_rule(point, &request)?;

        let mut rules = self.inner.rules.write();
        let rule = Arc::new(FaultRule {
            id: rule_id.to_string(),
            description: request.description,
            point: request.point,
            matcher: request.matcher,
            trigger: request.trigger,
            action: request.action,
            evidence: RwLock::new(FaultEvidence::default()),
        });
        let view = rule.view();
        rules.insert(rule_id.to_string(), rule);
        Ok(view)
    }

    pub fn remove(&self, rule_id: impl AsRef<str>) -> Result<bool, FaultRuntimeError> {
        let rule_id = rule_id.as_ref();
        validate_rule_id(rule_id)?;
        Ok(self.inner.rules.write().remove(rule_id).is_some())
    }

    pub fn rule(
        &self,
        rule_id: impl AsRef<str>,
    ) -> Result<Option<FaultRuleView>, FaultRuntimeError> {
        let rule_id = rule_id.as_ref();
        validate_rule_id(rule_id)?;
        Ok(self.inner.rules.read().get(rule_id).map(|rule| rule.view()))
    }

    pub fn clear(&self) -> Result<usize, FaultRuntimeError> {
        let mut rules = self.inner.rules.write();
        let count = rules.len();
        rules.clear();
        Ok(count)
    }

    pub fn status(&self) -> FaultRuntimeStatus {
        FaultRuntimeStatus {
            points: self
                .inner
                .catalog
                .iter()
                .map(|point| crate::FaultPointView::from(*point))
                .collect(),
            rules: self
                .inner
                .rules
                .read()
                .values()
                .map(|rule| rule.view())
                .collect(),
        }
    }

    /// Evaluates rules for one point using stable rule-ID ordering.
    ///
    /// One write guard publishes all evidence produced by a rule evaluation,
    /// so status readers never observe a partially updated counter snapshot.
    /// `Record` continues to the next rule; the first other executed action
    /// ends evaluation and becomes the returned decision.
    pub fn evaluate(
        &self,
        point: &'static FaultPointSpec,
        context: &FaultContext,
    ) -> Option<FaultDecision> {
        let rules = self
            .inner
            .rules
            .read()
            .values()
            .filter(|rule| rule.point == point.name)
            .cloned()
            .collect::<Vec<_>>();

        for rule in rules {
            let matcher_matches = rule.matcher.matches(context);
            let should_execute = {
                let mut evidence = rule.evidence.write();
                evidence.evaluations += 1;
                evidence.last_evaluated_context = Some(context.clone());
                if !matcher_matches {
                    false
                } else {
                    evidence.matches += 1;
                    let match_number = evidence.matches;
                    if !rule.trigger_matches(match_number)
                        || rule
                            .trigger
                            .max_hits
                            .is_some_and(|max_hits| evidence.executions >= max_hits)
                    {
                        false
                    } else {
                        evidence.executions += 1;
                        evidence.last_context = Some(context.clone());
                        true
                    }
                }
            };
            if !should_execute {
                continue;
            }
            if matches!(rule.action, FaultAction::Record) {
                continue;
            }
            return Some(FaultDecision {
                rule_id: rule.id.clone(),
                action: rule.action.clone(),
            });
        }
        None
    }

    fn point_spec(&self, point: &str) -> Result<&'static FaultPointSpec, FaultRuntimeError> {
        self.inner
            .catalog
            .iter()
            .copied()
            .find(|spec| spec.name == point)
            .ok_or_else(|| FaultRuntimeError::UnknownPoint(point.to_string()))
    }

    fn validate_rule(
        &self,
        point: &FaultPointSpec,
        request: &ConfigureFaultRule,
    ) -> Result<(), FaultRuntimeError> {
        for key in request.matcher.keys() {
            if !point.matchers.contains(&key) {
                return Err(FaultRuntimeError::InvalidRule(format!(
                    "matcher {key:?} is not available at {}",
                    point.name
                )));
            }
        }
        if !point.actions.contains(&request.action.kind()) {
            return Err(FaultRuntimeError::InvalidRule(format!(
                "action {:?} is not available at {}",
                request.action.kind(),
                point.name
            )));
        }
        if !(0.0..=1.0).contains(&request.trigger.probability) {
            return Err(FaultRuntimeError::InvalidRule(
                "trigger probability must be between 0 and 1".to_string(),
            ));
        }
        if request.trigger.max_hits == Some(0) {
            return Err(FaultRuntimeError::InvalidRule(
                "trigger max_hits must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "fault-injection"))]
#[path = "runtime_tests.rs"]
mod tests;
