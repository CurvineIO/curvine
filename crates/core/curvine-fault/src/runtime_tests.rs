use super::*;
use crate::{fault_point, FaultActionKind, FaultExecution, FaultMatcher, FaultTrigger};

const TEST_POINT_NAME: &str = "client.runtime.before";

#[allow(dead_code)]
fn declare_test_point(runtime: &FaultRuntime) -> Result<(), String> {
    fault_point! {
        sync,
        name: "client.runtime.before",
        description: "runtime test point",
        runtime: runtime,
        context: {"block_index" => 0_u32},
        return_error: |fault| Err(fault.message),
    }
    Ok(())
}

fn test_point() -> &'static FaultPointSpec {
    FAULT_POINT_REGISTRATIONS
        .iter()
        .find(|registration| registration.point.name == TEST_POINT_NAME)
        .unwrap()
        .point
}

fn runtime() -> FaultRuntime {
    FaultRuntime::isolated().unwrap()
}

fn declare_arbitrary_point(runtime: &FaultRuntime) {
    fault_point! {
        sync,
        name: "gateway.runtime.before",
        description: "point using an arbitrary application-defined name",
        runtime: runtime,
        context: {},
    }
}

#[test]
fn arbitrary_point_names_register_and_execute() {
    let runtime = FaultRuntime::isolated().unwrap();
    let status = runtime.status();
    assert!(status
        .points
        .iter()
        .any(|point| point.name == "gateway.runtime.before"));

    runtime
        .configure(
            "gateway-record",
            ConfigureFaultRule {
                point: "gateway.runtime.before".to_string(),
                action: FaultAction::Record,
                ..Default::default()
            },
        )
        .unwrap();
    declare_arbitrary_point(&runtime);
    assert_eq!(
        runtime.rule("gateway-record").unwrap().unwrap().executions,
        1
    );
}

fn matcher(key: &str, value: impl Into<crate::FaultValue>) -> FaultMatcher {
    let mut matcher = FaultMatcher::default();
    matcher.insert(key, value);
    matcher
}

#[test]
fn structured_trigger_and_evidence() {
    let runtime = runtime();
    runtime
        .configure(
            "fail_second",
            ConfigureFaultRule {
                point: TEST_POINT_NAME.to_string(),
                matcher: matcher("block_index", 1_u32),
                trigger: FaultTrigger {
                    max_hits: Some(1),
                    ..Default::default()
                },
                action: FaultAction::ReturnError {
                    error: crate::FaultErrorSpec {
                        message: "expected".to_string(),
                    },
                },
                ..Default::default()
            },
        )
        .unwrap();

    assert!(runtime
        .evaluate(
            test_point(),
            &crate::fault_context! { "block_index" => 0_u32 }
        )
        .is_none());
    assert!(matches!(
        runtime.evaluate(
            test_point(),
            &crate::fault_context! { "block_index" => 1_u32 }
        ),
        Some(FaultDecision {
            action: FaultAction::ReturnError { .. },
            ..
        })
    ));
    assert!(runtime
        .evaluate(
            test_point(),
            &crate::fault_context! { "block_index" => 1_u32 }
        )
        .is_none());
    assert!(runtime
        .evaluate(
            test_point(),
            &crate::fault_context! { "block_index" => 0_u32 }
        )
        .is_none());

    let rule = runtime.rule("fail_second").unwrap().unwrap();
    assert_eq!(rule.evaluations, 4);
    assert_eq!(rule.matches, 2);
    assert_eq!(rule.executions, 1);
    assert_eq!(
        rule.last_evaluated_context
            .as_ref()
            .unwrap()
            .get("block_index"),
        Some(&crate::FaultValue::I64(0))
    );
    assert_eq!(
        rule.last_context.as_ref().unwrap().get("block_index"),
        Some(&crate::FaultValue::I64(1))
    );
}

#[test]
fn rules_use_id_order_and_record_continues_to_first_behavior() {
    let runtime = runtime();
    for (id, action) in [
        (
            "z-behavior",
            FaultAction::ReturnError {
                error: crate::FaultErrorSpec {
                    message: "z".to_string(),
                },
            },
        ),
        ("a-record", FaultAction::Record),
        (
            "b-behavior",
            FaultAction::ReturnError {
                error: crate::FaultErrorSpec {
                    message: "b".to_string(),
                },
            },
        ),
    ] {
        runtime
            .configure(
                id,
                ConfigureFaultRule {
                    point: TEST_POINT_NAME.to_string(),
                    action,
                    ..Default::default()
                },
            )
            .unwrap();
    }

    let decision = runtime
        .evaluate(test_point(), &FaultContext::default())
        .unwrap();
    assert_eq!(decision.rule_id, "b-behavior");
    assert_eq!(runtime.rule("a-record").unwrap().unwrap().executions, 1);
    assert_eq!(runtime.rule("b-behavior").unwrap().unwrap().executions, 1);
    assert_eq!(runtime.rule("z-behavior").unwrap().unwrap().evaluations, 0);
}

#[test]
fn raw_rules_reject_invalid_ids_points_triggers_and_capabilities() {
    let runtime = runtime();
    for invalid_id in ["", ".", "bad/id"] {
        assert!(matches!(
            runtime.configure(
                invalid_id,
                ConfigureFaultRule {
                    point: TEST_POINT_NAME.to_string(),
                    ..Default::default()
                }
            ),
            Err(FaultRuntimeError::InvalidRule(_))
        ));
    }

    assert!(matches!(
        runtime.configure(
            "unknown-point",
            ConfigureFaultRule {
                point: "missing.point".to_string(),
                ..Default::default()
            },
        ),
        Err(FaultRuntimeError::UnknownPoint(_))
    ));

    for (id, trigger) in [
        (
            "negative-probability",
            FaultTrigger {
                probability: -0.1,
                ..Default::default()
            },
        ),
        (
            "excessive-probability",
            FaultTrigger {
                probability: 1.1,
                ..Default::default()
            },
        ),
        (
            "zero-max-hits",
            FaultTrigger {
                max_hits: Some(0),
                ..Default::default()
            },
        ),
    ] {
        assert!(matches!(
            runtime.configure(
                id,
                ConfigureFaultRule {
                    point: TEST_POINT_NAME.to_string(),
                    trigger,
                    ..Default::default()
                },
            ),
            Err(FaultRuntimeError::InvalidRule(_))
        ));
    }

    assert!(matches!(
        runtime.configure(
            "bad-matcher",
            ConfigureFaultRule {
                point: TEST_POINT_NAME.to_string(),
                matcher: matcher("undeclared", true),
                ..Default::default()
            },
        ),
        Err(FaultRuntimeError::InvalidRule(_))
    ));
    assert!(matches!(
        runtime.configure(
            "bad",
            ConfigureFaultRule {
                point: "gateway.runtime.before".to_string(),
                action: FaultAction::ReturnError {
                    error: crate::FaultErrorSpec {
                        message: "unsupported".to_string(),
                    },
                },
                ..Default::default()
            },
        ),
        Err(FaultRuntimeError::InvalidRule(_))
    ));
}

#[test]
fn configuring_the_same_rule_id_replaces_rule_and_resets_evidence() {
    let runtime = runtime();
    runtime
        .configure(
            "replace-me",
            ConfigureFaultRule {
                point: TEST_POINT_NAME.to_string(),
                action: FaultAction::ReturnError {
                    error: crate::FaultErrorSpec {
                        message: "first".to_string(),
                    },
                },
                ..Default::default()
            },
        )
        .unwrap();
    assert!(runtime
        .evaluate(test_point(), &FaultContext::default())
        .is_some());
    assert_eq!(runtime.rule("replace-me").unwrap().unwrap().executions, 1);

    let replacement = runtime
        .configure(
            "replace-me",
            ConfigureFaultRule {
                description: Some("replacement".to_string()),
                point: TEST_POINT_NAME.to_string(),
                trigger: FaultTrigger {
                    max_hits: Some(2),
                    ..Default::default()
                },
                action: FaultAction::ReturnError {
                    error: crate::FaultErrorSpec {
                        message: "second".to_string(),
                    },
                },
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(replacement.description.as_deref(), Some("replacement"));
    assert_eq!(replacement.evaluations, 0);
    assert_eq!(replacement.matches, 0);
    assert_eq!(replacement.executions, 0);
    assert_eq!(runtime.status().rules.len(), 1);

    let decision = runtime
        .evaluate(test_point(), &FaultContext::default())
        .unwrap();
    assert!(matches!(
        decision.action,
        FaultAction::ReturnError {
            error: crate::FaultErrorSpec { ref message }
        } if message == "second"
    ));
}

#[test]
fn accepts_unbounded_delay_and_process_crash_rules() {
    let runtime = runtime();
    for (id, action) in [
        (
            "long-delay",
            FaultAction::Delay {
                duration_ms: u64::MAX,
            },
        ),
        ("process-crash", FaultAction::Crash),
    ] {
        runtime
            .configure(
                id,
                ConfigureFaultRule {
                    point: TEST_POINT_NAME.to_string(),
                    action,
                    ..Default::default()
                },
            )
            .unwrap();
    }
    assert_eq!(runtime.status().rules.len(), 2);
}

#[test]
fn max_hits_is_atomic_across_concurrent_evaluations() {
    let runtime = runtime();
    runtime
        .configure(
            "once",
            ConfigureFaultRule {
                point: TEST_POINT_NAME.to_string(),
                trigger: FaultTrigger {
                    max_hits: Some(1),
                    ..Default::default()
                },
                action: FaultAction::ReturnError {
                    error: crate::FaultErrorSpec {
                        message: "expected".to_string(),
                    },
                },
                ..Default::default()
            },
        )
        .unwrap();

    let barrier = Arc::new(std::sync::Barrier::new(32));
    let executions = (0..32)
        .map(|_| {
            let runtime = runtime.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                runtime
                    .evaluate(test_point(), &FaultContext::default())
                    .is_some()
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .filter(|executed| *executed)
        .count();

    assert_eq!(executions, 1);
    assert_eq!(runtime.rule("once").unwrap().unwrap().executions, 1);
}

#[test]
fn trigger_after_skips_matching_calls_before_execution() {
    let runtime = runtime();
    runtime
        .configure(
            "after-two",
            ConfigureFaultRule {
                point: TEST_POINT_NAME.to_string(),
                trigger: FaultTrigger {
                    after: 2,
                    ..Default::default()
                },
                action: FaultAction::ReturnError {
                    error: crate::FaultErrorSpec {
                        message: "expected".to_string(),
                    },
                },
                ..Default::default()
            },
        )
        .unwrap();

    let context = FaultContext::default();
    assert!(runtime.evaluate(test_point(), &context).is_none());
    assert!(runtime.evaluate(test_point(), &context).is_none());
    assert!(runtime.evaluate(test_point(), &context).is_some());

    let rule = &runtime.status().rules[0];
    assert_eq!(rule.matches, 3);
    assert_eq!(rule.executions, 1);
}

#[test]
fn trigger_probability_boundaries_and_seed_are_deterministic() {
    assert_eq!(
        stable_probability_hash("probability", 7, 1),
        0x2bdd_198d_953e_3dcf
    );

    fn configured_runtime(probability: f64, seed: u64) -> FaultRuntime {
        let runtime = runtime();
        runtime
            .configure(
                "probability",
                ConfigureFaultRule {
                    point: TEST_POINT_NAME.to_string(),
                    trigger: FaultTrigger {
                        probability,
                        seed,
                        ..Default::default()
                    },
                    action: FaultAction::ReturnError {
                        error: crate::FaultErrorSpec {
                            message: "expected".to_string(),
                        },
                    },
                    ..Default::default()
                },
            )
            .unwrap();
        runtime
    }

    let context = FaultContext::default();
    assert!(configured_runtime(0.0, 7)
        .evaluate(test_point(), &context)
        .is_none());
    assert!(configured_runtime(1.0, 7)
        .evaluate(test_point(), &context)
        .is_some());

    let first = configured_runtime(0.5, 7);
    let second = configured_runtime(0.5, 7);
    let first_results = (0..32)
        .map(|_| first.evaluate(test_point(), &context).is_some())
        .collect::<Vec<_>>();
    let second_results = (0..32)
        .map(|_| second.evaluate(test_point(), &context).is_some())
        .collect::<Vec<_>>();
    assert_eq!(first_results, second_results);
    assert!(first_results.iter().any(|executed| *executed));
    assert!(first_results.iter().any(|executed| !*executed));
}

#[test]
fn rejects_invalid_points() {
    static EMPTY_NAME: crate::FaultPointSpec = crate::FaultPointSpec {
        name: "",
        description: "point without a name",
        execution: FaultExecution::Sync,
        matchers: &[],
        actions: &[FaultActionKind::Record],
    };
    assert!(validate_point(&EMPTY_NAME).is_err());

    static DUPLICATE_MATCHERS: crate::FaultPointSpec = crate::FaultPointSpec {
        name: "client.test.duplicate_matchers",
        description: "duplicate matcher point",
        execution: FaultExecution::Sync,
        matchers: &["index", "index"],
        actions: &[FaultActionKind::Record],
    };
    assert!(validate_point(&DUPLICATE_MATCHERS).is_err());
}
