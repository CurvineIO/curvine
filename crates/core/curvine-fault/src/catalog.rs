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

use crate::{FaultPointRegistration, FaultPointSpec, FaultRuntimeError};
use std::collections::{BTreeMap, HashSet};

/// Builds the process catalog from every point linked into the binary.
///
/// This is the default catalog model for process-owned runtimes. It validates
/// and merges registrations, but deliberately does not infer whether a
/// particular workload will reach a linked call site.
pub(crate) fn build_full_catalog(
    registrations: &'static [FaultPointRegistration],
) -> Result<Vec<&'static FaultPointSpec>, FaultRuntimeError> {
    let mut points = BTreeMap::<&str, &FaultPointRegistration>::new();
    for registration in registrations {
        let point = registration.point;
        validate_point(point)?;
        if let Some(existing) = points.get(point.name) {
            if !same_contract(existing.point, point) {
                return Err(FaultRuntimeError::InvalidRule(format!(
                    "conflicting fault point {} at {}:{} and {}:{}",
                    point.name, existing.file, existing.line, registration.file, registration.line,
                )));
            }
            continue;
        }
        points.insert(point.name, registration);
    }
    Ok(points
        .into_values()
        .map(|registration| registration.point)
        .collect())
}

pub(crate) fn validate_point(point: &FaultPointSpec) -> Result<(), FaultRuntimeError> {
    if point.name.is_empty() {
        return Err(FaultRuntimeError::InvalidRule(
            "fault point name must not be empty".to_string(),
        ));
    }
    if point.actions.is_empty() {
        return Err(FaultRuntimeError::InvalidRule(format!(
            "fault point {} must declare at least one action",
            point.name
        )));
    }
    let mut matchers = HashSet::new();
    for matcher in point.matchers {
        if matcher.is_empty() {
            return Err(FaultRuntimeError::InvalidRule(format!(
                "fault point {} declares an empty matcher key",
                point.name
            )));
        }
        if !matchers.insert(*matcher) {
            return Err(FaultRuntimeError::InvalidRule(format!(
                "fault point {} declares duplicate matcher {matcher:?}",
                point.name
            )));
        }
    }
    let mut actions = HashSet::new();
    for action in point.actions {
        if !actions.insert(*action) {
            return Err(FaultRuntimeError::InvalidRule(format!(
                "fault point {} declares duplicate action {action:?}",
                point.name
            )));
        }
    }
    Ok(())
}

fn same_contract(left: &FaultPointSpec, right: &FaultPointSpec) -> bool {
    left.name == right.name
        && left.description == right.description
        && left.execution == right.execution
        && same_set(left.matchers, right.matchers)
        && same_set(left.actions, right.actions)
}

fn same_set<T: Eq + std::hash::Hash>(left: &[T], right: &[T]) -> bool {
    left.len() == right.len()
        && left.iter().collect::<HashSet<_>>() == right.iter().collect::<HashSet<_>>()
}

pub(crate) fn validate_rule_id(rule_id: &str) -> Result<(), FaultRuntimeError> {
    if rule_id.is_empty() || rule_id.len() > 128 {
        return Err(FaultRuntimeError::InvalidRule(
            "rule id must contain 1 to 128 characters".to_string(),
        ));
    }
    if matches!(rule_id, "." | "..") {
        return Err(FaultRuntimeError::InvalidRule(
            "rule id must not be a URL dot-segment".to_string(),
        ));
    }
    if rule_id
        .bytes()
        .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(FaultRuntimeError::InvalidRule(
            "rule id may contain only ASCII letters, digits, '-', '_' and '.'".to_string(),
        ));
    }
    Ok(())
}
