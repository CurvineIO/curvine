// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use curvine_config::ObservabilityMode;

use super::model::{
    AlertSeverity, DiagnosticCheck, DiagnosticStatus, DiagnosticsPayload, MissingTarget,
    OperationsAlert, OperationsPayload, TargetHealth,
};
use super::provider::TargetRegistry;
use super::service::ObservabilityService;

pub async fn build_diagnostics(service: &ObservabilityService) -> DiagnosticsPayload {
    let generated_ms = now_ms();
    let health = service.health();
    let configured_mode = service.configured_mode();
    let active_provider = health.active_provider.clone();
    let prometheus_issue_status = prometheus_issue_status(configured_mode, &active_provider);
    let targets = service.builtin().targets();
    let master_count = targets
        .iter()
        .filter(|target| target.component == "master")
        .count();
    let live_worker_count = targets
        .iter()
        .filter(|target| is_live_worker(target))
        .count();
    let transfer_enabled = service.builtin().conf().transfer.enabled;
    let transfer_count = targets
        .iter()
        .filter(|target| target.component == "transfer")
        .count();
    let mut checks = Vec::new();

    checks.push(check(
        "observability-enabled",
        "Observability enabled",
        if health.enabled {
            DiagnosticStatus::Pass
        } else {
            DiagnosticStatus::Fail
        },
        if health.enabled {
            "Observability APIs and collection are enabled."
        } else {
            "Observability is disabled by configuration."
        },
        "",
        if health.enabled {
            ""
        } else {
            "Set web.observability.enabled=true and restart curvine-web."
        },
    ));

    checks.push(check(
        "master-targets",
        "Curvine master targets discovered",
        if master_count > 0 {
            DiagnosticStatus::Pass
        } else {
            DiagnosticStatus::Fail
        },
        &format!("Discovered {master_count} master metrics target(s)."),
        "Master discovery uses the active master and configured metrics web port.",
        if master_count > 0 {
            ""
        } else {
            "Verify master RPC connectivity and master.web_port, then refresh diagnostics."
        },
    ));

    checks.push(check(
        "worker-targets",
        "Curvine worker targets discovered",
        if live_worker_count > 0 {
            DiagnosticStatus::Pass
        } else {
            DiagnosticStatus::Warning
        },
        &format!("Discovered {live_worker_count} live worker metrics target(s)."),
        "Only live workers are required for Prometheus coverage.",
        if live_worker_count > 0 {
            ""
        } else {
            "Check worker registration and reported worker web ports."
        },
    ));

    let fuse_enabled = service.observability_conf().fuse.enabled;
    let fuse_count = targets
        .iter()
        .filter(|target| target.component == "fuse")
        .count();
    let fuse_status = if !fuse_enabled {
        DiagnosticStatus::Skipped
    } else if fuse_count > 0 {
        DiagnosticStatus::Pass
    } else {
        DiagnosticStatus::Warning
    };
    let fuse_summary = if !fuse_enabled {
        "FUSE metrics scraping is disabled in configuration.".to_string()
    } else {
        format!("Discovered {fuse_count} FUSE metrics target(s).")
    };
    let fuse_detail = if fuse_enabled {
        "FUSE targets come from web.observability.fuse or the local fuse.web_port default."
    } else {
        "FUSE observability is opt-in via web.observability.fuse.enabled."
    };
    let fuse_fix = if !fuse_enabled {
        "Set web.observability.fuse.enabled=true and configure targets when FUSE observability is required."
    } else if fuse_count > 0 {
        ""
    } else {
        "Configure web.observability.fuse.targets or ensure curvine-fuse is running and exposing /metrics."
    };
    checks.push(check(
        "fuse-targets",
        "Curvine FUSE targets discovered",
        fuse_status,
        &fuse_summary,
        fuse_detail,
        fuse_fix,
    ));

    let transfer_status = if !transfer_enabled {
        DiagnosticStatus::Skipped
    } else if transfer_count > 0 {
        DiagnosticStatus::Pass
    } else {
        DiagnosticStatus::Warning
    };
    let transfer_summary = if !transfer_enabled {
        "Transfer metrics scraping is disabled in configuration.".to_string()
    } else {
        format!("Discovered {transfer_count} Transfer metrics target(s).")
    };
    let transfer_detail = if transfer_enabled {
        "Transfer targets come from transfer.hostname and transfer.web_port."
    } else {
        "Transfer observability follows the transfer.enabled flag."
    };
    let transfer_fix = if !transfer_enabled {
        "Set transfer.enabled=true when Transfer observability is required."
    } else if transfer_count > 0 {
        ""
    } else {
        "Verify transfer.enabled, transfer.hostname and transfer.web_port, and that curvine-transfer exposes /metrics."
    };
    checks.push(check(
        "transfer-targets",
        "Curvine Transfer targets discovered",
        transfer_status,
        &transfer_summary,
        transfer_detail,
        transfer_fix,
    ));

    let expected_targets = master_count + live_worker_count + fuse_count + transfer_count;
    let builtin_covered_targets = targets
        .iter()
        .filter(|target| {
            (target.component == "master"
                || is_live_worker(target)
                || target.component == "fuse"
                || target.component == "transfer")
                && target.up
        })
        .count();
    let mut covered_targets = 0;
    let mut missing_targets = Vec::new();

    if active_provider == "builtin" {
        let complete = expected_targets > 0 && builtin_covered_targets == expected_targets;
        let master_down = targets
            .iter()
            .any(|target| target.component == "master" && !target.up);
        checks.push(check(
            "builtin-coverage",
            "Builtin scrape coverage",
            if expected_targets == 0 {
                DiagnosticStatus::Skipped
            } else if complete {
                DiagnosticStatus::Pass
            } else if master_down {
                DiagnosticStatus::Fail
            } else {
                DiagnosticStatus::Warning
            },
            if expected_targets == 0 {
                "Coverage is pending until target discovery completes.".to_string()
            } else {
                format!(
                    "Builtin collector covers {builtin_covered_targets} of {expected_targets} required target(s)."
                )
            }
            .as_str(),
            "Coverage is based on the latest builtin scrape result for every discovered master, live worker, FUSE and Transfer target.",
            if expected_targets == 0 || complete {
                ""
            } else {
                "Verify each endpoint and metrics web port, then inspect Observability / Data Sources for the scrape error."
            },
        ));
        missing_targets.extend(
            targets
                .iter()
                .filter(|target| {
                    (target.component == "master"
                        || is_live_worker(target)
                        || target.component == "fuse"
                        || target.component == "transfer")
                        && !target.up
                })
                .map(|target| missing_target(target, "builtin-target")),
        );
    } else {
        checks.push(check(
            "builtin-coverage",
            "Builtin scrape coverage",
            DiagnosticStatus::Skipped,
            "Builtin coverage is not authoritative because another provider is active.",
            "",
            "",
        ));
    }

    match service.prometheus_provider() {
        Some(provider) => {
            let reachable = match provider.check_reachable().await {
                Ok(()) => {
                    checks.push(check(
                        "prometheus-reachable",
                        "Prometheus reachable",
                        DiagnosticStatus::Pass,
                        "Prometheus accepted the health request.",
                        provider.base_url(),
                        "",
                    ));
                    true
                }
                Err(err) => {
                    checks.push(check(
                        "prometheus-reachable",
                        "Prometheus reachable",
                        prometheus_issue_status,
                        "Prometheus cannot be reached.",
                        &err,
                        "Verify web.observability.prometheus.url, DNS, TLS and network policy.",
                    ));
                    false
                }
            };

            if reachable {
                if active_provider == "builtin" {
                    checks.push(check(
                        "prometheus-coverage",
                        "Prometheus target coverage",
                        DiagnosticStatus::Skipped,
                        "Prometheus coverage is not authoritative because builtin is the active provider.",
                        "",
                        "Switch provider mode to prometheus or auto when Prometheus-backed dashboards are required.",
                    ));
                } else if expected_targets == 0 {
                    checks.push(check(
                        "prometheus-coverage",
                        "Prometheus target coverage",
                        DiagnosticStatus::Skipped,
                        "Coverage is pending until target discovery completes.",
                        "No required master, live worker, FUSE or Transfer targets have been discovered yet.",
                        "Verify discovery connectivity, then refresh diagnostics.",
                    ));
                    let overall_status = overall_status(&checks);
                    return DiagnosticsPayload {
                        generated_ms,
                        active_provider,
                        overall_status,
                        checks,
                        discovered_master_targets: master_count,
                        discovered_live_worker_targets: live_worker_count,
                        discovered_transfer_targets: transfer_count,
                        prometheus_covered_targets: covered_targets,
                        active_covered_targets: 0,
                        expected_targets,
                        missing_targets,
                    };
                } else {
                    let mut prometheus_targets: Vec<TargetHealth> = targets
                        .iter()
                        .filter(|target| {
                            target.component == "master"
                                || is_live_worker(target)
                                || target.component == "fuse"
                                || target.component == "transfer"
                        })
                        .cloned()
                        .collect();
                    let ttl = curvine_config::parse_duration_ms(
                        &service.observability_conf().prometheus.target_cache_ttl,
                    )
                    .unwrap_or(30_000);
                    match provider
                    .enrich_target_health(&mut prometheus_targets, ttl)
                    .await
                {
                    Ok(()) => {
                        covered_targets =
                            prometheus_targets.iter().filter(|target| target.up).count();
                        missing_targets.extend(
                            prometheus_targets
                                .iter()
                                .filter(|target| !target.up)
                                .map(|target| missing_target(target, "prometheus-target")),
                        );
                        let complete = covered_targets == expected_targets;
                        let master_down = prometheus_targets
                            .iter()
                            .any(|target| target.component == "master" && !target.up);
                        checks.push(check(
                            "prometheus-coverage",
                            "Prometheus target coverage",
                            if complete {
                                DiagnosticStatus::Pass
                            } else if master_down {
                                prometheus_issue_status
                            } else {
                                DiagnosticStatus::Warning
                            },
                            &format!(
                                "Prometheus covers {covered_targets} of {expected_targets} required target(s)."
                            ),
                            "Coverage is matched by scrape endpoint, instance and component labels.",
                            if complete {
                                ""
                            } else {
                                "Verify every missing target in Prometheus /targets and ensure component/instance labels match discovery."
                            },
                        ));
                    }
                    Err(err) => checks.push(check(
                        "prometheus-coverage",
                        "Prometheus target coverage",
                        prometheus_issue_status,
                        "Prometheus target coverage could not be evaluated.",
                        &err,
                        "Grant access to Prometheus /api/v1/targets and verify the configured URL.",
                    )),
                }
                }
            } else {
                checks.push(check(
                    "prometheus-coverage",
                    "Prometheus target coverage",
                    DiagnosticStatus::Skipped,
                    "Coverage was skipped because Prometheus is unreachable.",
                    "",
                    "Restore Prometheus reachability, then refresh diagnostics.",
                ));
            }
        }
        None => {
            let required = matches!(
                configured_mode,
                ObservabilityMode::Prometheus | ObservabilityMode::Auto
            );
            checks.push(check(
                "prometheus-reachable",
                "Prometheus reachable",
                if required {
                    DiagnosticStatus::Fail
                } else {
                    DiagnosticStatus::Skipped
                },
                "Prometheus URL is not configured.",
                "",
                if required {
                    "Set web.observability.prometheus.url or switch provider mode to builtin."
                } else {
                    "Configure Prometheus when moving from builtin to production provider mode."
                },
            ));
            checks.push(check(
                "prometheus-coverage",
                "Prometheus target coverage",
                DiagnosticStatus::Skipped,
                "Coverage is unavailable without a Prometheus URL.",
                "",
                "Use etc/prometheus-curvine.yml or the Kubernetes monitor examples.",
            ));
        }
    }

    let overall_status = overall_status(&checks);
    DiagnosticsPayload {
        generated_ms,
        active_covered_targets: if active_provider == "builtin" {
            builtin_covered_targets
        } else {
            covered_targets
        },
        active_provider,
        overall_status,
        checks,
        discovered_master_targets: master_count,
        discovered_live_worker_targets: live_worker_count,
        discovered_transfer_targets: transfer_count,
        prometheus_covered_targets: covered_targets,
        expected_targets,
        missing_targets,
    }
}

pub async fn build_operations(service: &ObservabilityService) -> OperationsPayload {
    let diagnostics = build_diagnostics(service).await;
    let alerts = alerts_from_diagnostics(&diagnostics);
    OperationsPayload {
        generated_ms: diagnostics.generated_ms,
        diagnostics,
        alerts,
    }
}

fn alerts_from_diagnostics(diagnostics: &DiagnosticsPayload) -> Vec<OperationsAlert> {
    let active_provider = diagnostics.active_provider.clone();
    let mut alerts = Vec::new();
    for item in &diagnostics.checks {
        let severity = match item.status {
            DiagnosticStatus::Fail => AlertSeverity::Critical,
            DiagnosticStatus::Warning => AlertSeverity::Warning,
            DiagnosticStatus::Pass | DiagnosticStatus::Skipped => continue,
        };
        if coverage_check_has_target_alert(item, &diagnostics.missing_targets) {
            continue;
        }
        alerts.push(OperationsAlert {
            id: format!("diagnostic:{}", item.id),
            severity,
            title: item.title.clone(),
            summary: item.summary.clone(),
            source: "diagnostics".to_string(),
            target_id: None,
            suggested_fix: item.suggested_fix.clone(),
        });
    }
    for target in &diagnostics.missing_targets {
        let is_active_source = target.source.starts_with(&active_provider);
        alerts.push(OperationsAlert {
            id: format!("{}:{}", target.source, target.id),
            severity: if target.component == "master" && is_active_source {
                AlertSeverity::Critical
            } else {
                AlertSeverity::Warning
            },
            title: format!("{} metrics target unavailable", target.component),
            summary: format!("{}: {}", target.instance, target.reason),
            source: target.source.clone(),
            target_id: Some(target.id.clone()),
            suggested_fix: if is_active_source && active_provider == "builtin" {
                "Verify the target endpoint and metrics web port, then inspect Observability / Data Sources."
                    .to_string()
            } else {
                "Verify the target endpoint, component label and Prometheus monitor selector."
                    .to_string()
            },
        });
    }
    alerts
}

fn coverage_check_has_target_alert(
    check: &DiagnosticCheck,
    missing_targets: &[MissingTarget],
) -> bool {
    let source = match check.id.as_str() {
        "builtin-coverage" => "builtin-target",
        "prometheus-coverage" => "prometheus-target",
        _ => return false,
    };
    missing_targets.iter().any(|target| target.source == source)
}

fn check(
    id: &str,
    title: &str,
    status: DiagnosticStatus,
    summary: &str,
    detail: &str,
    suggested_fix: &str,
) -> DiagnosticCheck {
    DiagnosticCheck {
        id: id.to_string(),
        title: title.to_string(),
        status,
        summary: summary.to_string(),
        detail: detail.to_string(),
        suggested_fix: suggested_fix.to_string(),
    }
}

fn is_live_worker(target: &TargetHealth) -> bool {
    target.component == "worker" && target.worker_state.as_deref() == Some("live")
}

fn missing_target(target: &TargetHealth, source: &str) -> MissingTarget {
    let reason = if !target.last_error.is_empty() {
        target.last_error.clone()
    } else if !target.last_warning.is_empty() {
        target.last_warning.clone()
    } else {
        format!(
            "{} has no healthy matching scrape target",
            source.trim_end_matches("-target")
        )
    };
    MissingTarget {
        id: target.id.clone(),
        source: source.to_string(),
        component: target.component.clone(),
        instance: target.instance.clone(),
        url: target.url.clone(),
        reason,
    }
}

fn overall_status(checks: &[DiagnosticCheck]) -> DiagnosticStatus {
    if checks
        .iter()
        .any(|item| item.status == DiagnosticStatus::Fail)
    {
        DiagnosticStatus::Fail
    } else if checks
        .iter()
        .any(|item| item.status == DiagnosticStatus::Warning)
    {
        DiagnosticStatus::Warning
    } else {
        DiagnosticStatus::Pass
    }
}

fn prometheus_issue_status(mode: ObservabilityMode, active_provider: &str) -> DiagnosticStatus {
    match mode {
        ObservabilityMode::Prometheus => DiagnosticStatus::Fail,
        ObservabilityMode::Auto if active_provider == "builtin" => DiagnosticStatus::Warning,
        ObservabilityMode::Auto => DiagnosticStatus::Fail,
        ObservabilityMode::Builtin => DiagnosticStatus::Warning,
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn diagnostics_fixture(
        active_provider: &str,
        checks: Vec<DiagnosticCheck>,
        missing_targets: Vec<MissingTarget>,
    ) -> DiagnosticsPayload {
        DiagnosticsPayload {
            generated_ms: 1,
            active_provider: active_provider.to_string(),
            overall_status: overall_status(&checks),
            checks,
            discovered_master_targets: 1,
            discovered_live_worker_targets: 1,
            discovered_transfer_targets: 0,
            prometheus_covered_targets: 0,
            active_covered_targets: 0,
            expected_targets: 2,
            missing_targets,
        }
    }

    fn missing_fixture(source: &str, component: &str) -> MissingTarget {
        MissingTarget {
            id: format!("{component}:test"),
            source: source.to_string(),
            component: component.to_string(),
            instance: format!("{component}-test"),
            url: format!("http://{component}-test/metrics"),
            reason: "scrape failed".to_string(),
        }
    }

    fn default_service() -> ObservabilityService {
        use std::sync::Arc;

        use curvine_config::ClusterConf;
        use curvine_runtime::runtime::Runtime;
        use curvine_unified_fs::UnifiedFileSystem;

        let conf = ClusterConf::default();
        let unified_fs =
            UnifiedFileSystem::with_rt(conf.clone(), Arc::new(Runtime::single())).expect("ufs");
        ObservabilityService::new(conf, unified_fs)
    }

    #[test]
    fn overall_status_prefers_fail_then_warning() {
        let pass = check("pass", "pass", DiagnosticStatus::Pass, "", "", "");
        let warning = check("warning", "warning", DiagnosticStatus::Warning, "", "", "");
        let fail = check("fail", "fail", DiagnosticStatus::Fail, "", "", "");
        assert_eq!(
            overall_status(std::slice::from_ref(&pass)),
            DiagnosticStatus::Pass
        );
        assert_eq!(
            overall_status(&[pass.clone(), warning]),
            DiagnosticStatus::Warning
        );
        assert_eq!(overall_status(&[pass, fail]), DiagnosticStatus::Fail);
    }

    #[test]
    fn inactive_prometheus_is_warning_but_required_prometheus_is_failure() {
        assert_eq!(
            prometheus_issue_status(ObservabilityMode::Builtin, "builtin"),
            DiagnosticStatus::Warning
        );
        assert_eq!(
            prometheus_issue_status(ObservabilityMode::Prometheus, "prometheus"),
            DiagnosticStatus::Fail
        );
        assert_eq!(
            prometheus_issue_status(ObservabilityMode::Auto, "prometheus"),
            DiagnosticStatus::Fail
        );
        assert_eq!(
            prometheus_issue_status(ObservabilityMode::Auto, "builtin"),
            DiagnosticStatus::Warning
        );
    }

    #[test]
    fn diagnostic_failures_and_warnings_map_to_expected_alert_severity() {
        let diagnostics = diagnostics_fixture(
            "builtin",
            vec![
                check(
                    "observability-enabled",
                    "Observability enabled",
                    DiagnosticStatus::Fail,
                    "disabled",
                    "",
                    "enable it",
                ),
                check(
                    "worker-targets",
                    "Worker discovery",
                    DiagnosticStatus::Warning,
                    "no workers",
                    "",
                    "check registration",
                ),
                check(
                    "master-targets",
                    "Master discovery",
                    DiagnosticStatus::Pass,
                    "healthy",
                    "",
                    "",
                ),
                check(
                    "prometheus-coverage",
                    "Prometheus coverage",
                    DiagnosticStatus::Skipped,
                    "not configured",
                    "",
                    "",
                ),
            ],
            Vec::new(),
        );

        let alerts = alerts_from_diagnostics(&diagnostics);
        assert_eq!(alerts.len(), 2);
        assert!(alerts.iter().any(|alert| {
            alert.id == "diagnostic:observability-enabled"
                && alert.severity == AlertSeverity::Critical
        }));
        assert!(alerts.iter().any(|alert| {
            alert.id == "diagnostic:worker-targets" && alert.severity == AlertSeverity::Warning
        }));
    }

    #[test]
    fn coverage_alert_is_kept_when_no_target_level_detail_exists() {
        let diagnostics = diagnostics_fixture(
            "prometheus",
            vec![check(
                "prometheus-coverage",
                "Prometheus coverage",
                DiagnosticStatus::Fail,
                "target API failed",
                "",
                "check API access",
            )],
            Vec::new(),
        );

        let alerts = alerts_from_diagnostics(&diagnostics);
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].id, "diagnostic:prometheus-coverage");
        assert_eq!(alerts[0].severity, AlertSeverity::Critical);
    }

    #[test]
    fn target_alerts_replace_coverage_summary_and_use_active_source_severity() {
        let builtin_diagnostics = diagnostics_fixture(
            "builtin",
            vec![check(
                "builtin-coverage",
                "Builtin coverage",
                DiagnosticStatus::Fail,
                "one target down",
                "",
                "check target",
            )],
            vec![
                missing_fixture("builtin-target", "master"),
                missing_fixture("builtin-target", "worker"),
            ],
        );
        let alerts = alerts_from_diagnostics(&builtin_diagnostics);
        assert_eq!(alerts.len(), 2);
        assert!(!alerts
            .iter()
            .any(|alert| alert.id == "diagnostic:builtin-coverage"));
        assert!(alerts.iter().any(|alert| {
            alert.target_id.as_deref() == Some("master:test")
                && alert.severity == AlertSeverity::Critical
        }));
        assert!(alerts.iter().any(|alert| {
            alert.target_id.as_deref() == Some("worker:test")
                && alert.severity == AlertSeverity::Warning
        }));

        let inactive_diagnostics = diagnostics_fixture(
            "builtin",
            vec![check(
                "prometheus-coverage",
                "Prometheus coverage",
                DiagnosticStatus::Warning,
                "one target down",
                "",
                "check target",
            )],
            vec![missing_fixture("prometheus-target", "master")],
        );
        let alerts = alerts_from_diagnostics(&inactive_diagnostics);
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].severity, AlertSeverity::Warning);
    }

    #[test]
    fn healthy_and_skipped_checks_do_not_create_alerts() {
        let diagnostics = diagnostics_fixture(
            "builtin",
            vec![
                check("healthy", "Healthy", DiagnosticStatus::Pass, "", "", ""),
                check("pending", "Pending", DiagnosticStatus::Skipped, "", "", ""),
            ],
            Vec::new(),
        );
        assert!(alerts_from_diagnostics(&diagnostics).is_empty());
    }

    #[tokio::test]
    async fn fuse_targets_skipped_when_fuse_observability_disabled_by_default() {
        let service = default_service();
        assert!(!service.observability_conf().fuse.enabled);
        service.builtin().update_target(TargetHealth {
            id: "master:active-master".to_string(),
            component: "master".to_string(),
            instance: "active-master".to_string(),
            url: "http://active-master:9010/metrics".to_string(),
            up: true,
            ..TargetHealth::default()
        });
        service.builtin().update_target(TargetHealth {
            id: "worker:1".to_string(),
            component: "worker".to_string(),
            instance: "worker-1".to_string(),
            url: "http://worker-1:9000/metrics".to_string(),
            worker_state: Some("live".to_string()),
            up: true,
            ..TargetHealth::default()
        });

        let diagnostics = build_diagnostics(&service).await;
        let fuse_check = diagnostics
            .checks
            .iter()
            .find(|item| item.id == "fuse-targets")
            .expect("fuse-targets check");
        assert_eq!(fuse_check.status, DiagnosticStatus::Skipped);
        assert_eq!(diagnostics.overall_status, DiagnosticStatus::Pass);
        assert!(alerts_from_diagnostics(&diagnostics).is_empty());
    }

    #[tokio::test]
    async fn fuse_targets_warns_when_enabled_but_not_discovered() {
        use std::sync::Arc;

        use curvine_config::ClusterConf;
        use curvine_runtime::runtime::Runtime;
        use curvine_unified_fs::UnifiedFileSystem;

        let mut conf = ClusterConf::default();
        conf.web.observability.fuse.enabled = true;
        let unified_fs =
            UnifiedFileSystem::with_rt(conf.clone(), Arc::new(Runtime::single())).expect("ufs");
        let service = ObservabilityService::new(conf, unified_fs);
        service.builtin().update_target(TargetHealth {
            id: "master:active-master".to_string(),
            component: "master".to_string(),
            instance: "active-master".to_string(),
            url: "http://active-master:9010/metrics".to_string(),
            up: true,
            ..TargetHealth::default()
        });
        service.builtin().update_target(TargetHealth {
            id: "worker:1".to_string(),
            component: "worker".to_string(),
            instance: "worker-1".to_string(),
            url: "http://worker-1:9000/metrics".to_string(),
            worker_state: Some("live".to_string()),
            up: true,
            ..TargetHealth::default()
        });

        let diagnostics = build_diagnostics(&service).await;
        let fuse_check = diagnostics
            .checks
            .iter()
            .find(|item| item.id == "fuse-targets")
            .expect("fuse-targets check");
        assert_eq!(fuse_check.status, DiagnosticStatus::Warning);
        assert_eq!(diagnostics.overall_status, DiagnosticStatus::Warning);
    }

    #[tokio::test]
    async fn transfer_targets_skipped_when_transfer_disabled_by_default() {
        let service = default_service();
        assert!(!service.builtin().conf().transfer.enabled);
        service.builtin().update_target(TargetHealth {
            id: "master:active-master".to_string(),
            component: "master".to_string(),
            instance: "active-master".to_string(),
            url: "http://active-master:9010/metrics".to_string(),
            up: true,
            ..TargetHealth::default()
        });
        service.builtin().update_target(TargetHealth {
            id: "worker:1".to_string(),
            component: "worker".to_string(),
            instance: "worker-1".to_string(),
            url: "http://worker-1:9000/metrics".to_string(),
            worker_state: Some("live".to_string()),
            up: true,
            ..TargetHealth::default()
        });

        let diagnostics = build_diagnostics(&service).await;
        let transfer_check = diagnostics
            .checks
            .iter()
            .find(|item| item.id == "transfer-targets")
            .expect("transfer-targets check");
        assert_eq!(transfer_check.status, DiagnosticStatus::Skipped);
        assert_eq!(diagnostics.discovered_transfer_targets, 0);
        assert_eq!(diagnostics.overall_status, DiagnosticStatus::Pass);
        assert!(alerts_from_diagnostics(&diagnostics).is_empty());
    }

    #[tokio::test]
    async fn transfer_targets_warn_and_count_when_enabled_but_not_discovered() {
        use std::sync::Arc;

        use curvine_config::ClusterConf;
        use curvine_runtime::runtime::Runtime;
        use curvine_unified_fs::UnifiedFileSystem;

        let mut conf = ClusterConf::default();
        conf.transfer.enabled = true;
        conf.transfer.hostname = "cv-transfer-0".to_string();
        let unified_fs =
            UnifiedFileSystem::with_rt(conf.clone(), Arc::new(Runtime::single())).expect("ufs");
        let service = ObservabilityService::new(conf, unified_fs);
        service.builtin().update_target(TargetHealth {
            id: "master:active-master".to_string(),
            component: "master".to_string(),
            instance: "active-master".to_string(),
            url: "http://active-master:9010/metrics".to_string(),
            up: true,
            ..TargetHealth::default()
        });
        service.builtin().update_target(TargetHealth {
            id: "worker:1".to_string(),
            component: "worker".to_string(),
            instance: "worker-1".to_string(),
            url: "http://worker-1:9000/metrics".to_string(),
            worker_state: Some("live".to_string()),
            up: true,
            ..TargetHealth::default()
        });

        let diagnostics = build_diagnostics(&service).await;
        let transfer_check = diagnostics
            .checks
            .iter()
            .find(|item| item.id == "transfer-targets")
            .expect("transfer-targets check");
        assert_eq!(transfer_check.status, DiagnosticStatus::Warning);
        assert_eq!(diagnostics.overall_status, DiagnosticStatus::Warning);
        assert_eq!(diagnostics.expected_targets, 2);
        assert_eq!(diagnostics.discovered_transfer_targets, 0);
    }

    #[tokio::test]
    async fn builtin_coverage_includes_up_transfer_target() {
        use std::sync::Arc;

        use curvine_config::ClusterConf;
        use curvine_runtime::runtime::Runtime;
        use curvine_unified_fs::UnifiedFileSystem;

        let mut conf = ClusterConf::default();
        conf.transfer.enabled = true;
        conf.transfer.hostname = "cv-transfer-0".to_string();
        let unified_fs =
            UnifiedFileSystem::with_rt(conf.clone(), Arc::new(Runtime::single())).expect("ufs");
        let service = ObservabilityService::new(conf, unified_fs);
        for target in [
            TargetHealth {
                id: "master:active-master".to_string(),
                component: "master".to_string(),
                instance: "active-master".to_string(),
                url: "http://active-master:9010/metrics".to_string(),
                up: true,
                ..TargetHealth::default()
            },
            TargetHealth {
                id: "worker:1".to_string(),
                component: "worker".to_string(),
                instance: "worker-1".to_string(),
                url: "http://worker-1:9000/metrics".to_string(),
                worker_state: Some("live".to_string()),
                up: true,
                ..TargetHealth::default()
            },
            TargetHealth {
                id: "transfer:cv-transfer-0:9011".to_string(),
                component: "transfer".to_string(),
                instance: "cv-transfer-0:9011".to_string(),
                url: "http://cv-transfer-0:9011/metrics".to_string(),
                up: true,
                ..TargetHealth::default()
            },
        ] {
            service.builtin().update_target(target);
        }

        let diagnostics = build_diagnostics(&service).await;
        let transfer_check = diagnostics
            .checks
            .iter()
            .find(|item| item.id == "transfer-targets")
            .expect("transfer-targets check");
        let coverage = diagnostics
            .checks
            .iter()
            .find(|item| item.id == "builtin-coverage")
            .expect("builtin-coverage check");
        assert_eq!(transfer_check.status, DiagnosticStatus::Pass);
        assert_eq!(coverage.status, DiagnosticStatus::Pass);
        assert_eq!(diagnostics.expected_targets, 3);
        assert_eq!(diagnostics.discovered_transfer_targets, 1);
        assert_eq!(diagnostics.active_covered_targets, 3);
    }

    #[tokio::test]
    async fn builtin_coverage_is_skipped_while_discovery_is_empty() {
        let payload = build_diagnostics(&default_service()).await;

        assert_eq!(payload.active_provider, "builtin");
        assert_eq!(payload.expected_targets, 0);
        assert!(payload.checks.iter().any(|item| {
            item.id == "builtin-coverage" && item.status == DiagnosticStatus::Skipped
        }));
        assert!(payload.missing_targets.is_empty());
    }

    #[tokio::test]
    async fn builtin_down_target_is_reported_by_diagnostics_and_alerts() {
        let service = default_service();
        service.builtin().update_target(TargetHealth {
            id: "master:active-master".to_string(),
            component: "master".to_string(),
            instance: "active-master".to_string(),
            url: "http://active-master:9010/metrics".to_string(),
            last_error: "connection refused".to_string(),
            ..TargetHealth::default()
        });

        let payload = build_diagnostics(&service).await;
        assert!(payload.checks.iter().any(|item| {
            item.id == "builtin-coverage" && item.status == DiagnosticStatus::Fail
        }));
        assert_eq!(payload.missing_targets.len(), 1);
        assert_eq!(payload.missing_targets[0].source, "builtin-target");

        let payload = build_operations(&service).await;
        assert!(payload.alerts.iter().any(|alert| {
            alert.source == "builtin-target" && alert.severity == AlertSeverity::Critical
        }));
        assert!(!payload
            .alerts
            .iter()
            .any(|alert| alert.id == "diagnostic:builtin-coverage"));
        assert_eq!(payload.diagnostics.generated_ms, payload.generated_ms);
    }

    #[tokio::test]
    async fn builtin_worker_only_scrape_failure_is_warning() {
        let service = default_service();
        service.builtin().update_target(TargetHealth {
            id: "master:active-master".to_string(),
            component: "master".to_string(),
            instance: "active-master".to_string(),
            url: "http://active-master:9010/metrics".to_string(),
            up: true,
            ..TargetHealth::default()
        });
        service.builtin().update_target(TargetHealth {
            id: "worker:1".to_string(),
            component: "worker".to_string(),
            instance: "worker-1".to_string(),
            url: "http://worker-1:9000/metrics".to_string(),
            worker_state: Some("live".to_string()),
            last_error: "request timed out".to_string(),
            ..TargetHealth::default()
        });
        for target in service.builtin().targets() {
            if target.component == "fuse" {
                service
                    .builtin()
                    .update_target(TargetHealth { up: true, ..target });
            }
        }

        let diagnostics = build_diagnostics(&service).await;
        assert_eq!(diagnostics.overall_status, DiagnosticStatus::Warning);
        assert!(diagnostics.checks.iter().any(|item| {
            item.id == "builtin-coverage" && item.status == DiagnosticStatus::Warning
        }));
        let alerts = alerts_from_diagnostics(&diagnostics);
        let worker_alerts: Vec<_> = alerts
            .iter()
            .filter(|alert| {
                alert.source == "builtin-target" && alert.target_id == Some("worker:1".to_string())
            })
            .collect();
        assert_eq!(worker_alerts.len(), 1);
        assert_eq!(worker_alerts[0].severity, AlertSeverity::Warning);
        assert!(!alerts
            .iter()
            .any(|alert| alert.id == "diagnostic:prometheus-coverage"));
    }

    #[tokio::test]
    async fn prometheus_coverage_is_skipped_when_no_targets_are_expected() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        use curvine_config::ClusterConf;
        use curvine_runtime::runtime::Runtime;
        use curvine_unified_fs::UnifiedFileSystem;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let addr = listener.local_addr().expect("address");
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = requests.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                observed.fetch_add(1, Ordering::SeqCst);
                let mut request = [0_u8; 1024];
                let _ = stream.read(&mut request).await;
                let _ = stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await;
            }
        });

        let mut conf = ClusterConf::default();
        conf.web.observability.mode = ObservabilityMode::Prometheus;
        conf.web.observability.prometheus.url = format!("http://{addr}");
        let unified_fs =
            UnifiedFileSystem::with_rt(conf.clone(), Arc::new(Runtime::single())).expect("ufs");
        let service = ObservabilityService::new(conf, unified_fs);

        let payload = build_diagnostics(&service).await;
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert!(payload.checks.iter().any(|item| {
            item.id == "prometheus-coverage"
                && item.status == DiagnosticStatus::Skipped
                && item.summary.contains("discovery completes")
        }));
    }

    #[tokio::test]
    async fn unreachable_prometheus_skips_target_coverage_request() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        use curvine_config::ClusterConf;
        use curvine_runtime::runtime::Runtime;
        use curvine_unified_fs::UnifiedFileSystem;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let addr = listener.local_addr().expect("address");
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = requests.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                observed.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        });

        let mut conf = ClusterConf::default();
        conf.web.observability.mode = ObservabilityMode::Prometheus;
        conf.web.observability.prometheus.url = format!("http://{addr}");
        let unified_fs =
            UnifiedFileSystem::with_rt(conf.clone(), Arc::new(Runtime::single())).expect("ufs");
        let service = ObservabilityService::new(conf, unified_fs);

        let payload = build_diagnostics(&service).await;
        tokio::task::yield_now().await;

        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert!(payload.checks.iter().any(|item| {
            item.id == "prometheus-coverage" && item.status == DiagnosticStatus::Skipped
        }));
    }
}
