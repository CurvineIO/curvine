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

use crate::catalog::validate_rule_id;
use crate::{
    ConfigureFaultRule, FaultControlError, FaultRuleView, FaultRuntime, FaultRuntimeStatus,
};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

#[async_trait]
pub trait FaultController: Send + Sync {
    async fn status(&self) -> Result<FaultRuntimeStatus, FaultControlError>;

    async fn configure(
        &self,
        rule_id: &str,
        rule: ConfigureFaultRule,
    ) -> Result<FaultRuleView, FaultControlError>;

    async fn rule(&self, rule_id: &str) -> Result<Option<FaultRuleView>, FaultControlError>;

    async fn remove(&self, rule_id: &str) -> Result<bool, FaultControlError>;

    async fn clear(&self) -> Result<usize, FaultControlError>;

    async fn preflight_clean(&self) -> Result<FaultRuntimeStatus, FaultControlError> {
        let status = self.status().await?;
        if status.rules.is_empty() {
            Ok(status)
        } else {
            Err(FaultControlError::DirtyTarget {
                rules: status.rules.into_iter().map(|rule| rule.id).collect(),
            })
        }
    }
}

#[derive(Clone)]
pub struct FaultLocalController {
    runtime: FaultRuntime,
}

impl FaultLocalController {
    pub fn new(runtime: FaultRuntime) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl FaultController for FaultLocalController {
    async fn status(&self) -> Result<FaultRuntimeStatus, FaultControlError> {
        Ok(self.runtime.status())
    }

    async fn configure(
        &self,
        rule_id: &str,
        rule: ConfigureFaultRule,
    ) -> Result<FaultRuleView, FaultControlError> {
        self.runtime.configure(rule_id, rule).map_err(Into::into)
    }

    async fn remove(&self, rule_id: &str) -> Result<bool, FaultControlError> {
        self.runtime.remove(rule_id).map_err(Into::into)
    }

    async fn rule(&self, rule_id: &str) -> Result<Option<FaultRuleView>, FaultControlError> {
        self.runtime.rule(rule_id).map_err(Into::into)
    }

    async fn clear(&self) -> Result<usize, FaultControlError> {
        self.runtime.clear().map_err(Into::into)
    }
}

#[derive(Clone)]
pub struct FaultHttpController {
    client: reqwest::Client,
    base_url: reqwest::Url,
    token: String,
}

impl FaultHttpController {
    /// Creates a controller from the target site's root URL.
    pub fn new(
        base_url: impl AsRef<str>,
        token: impl Into<String>,
    ) -> Result<Self, FaultControlError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| FaultControlError::Transport(error.to_string()))?;
        let base_url = fault_api_url(base_url.as_ref())?;
        Ok(Self {
            client,
            base_url,
            token: token.into(),
        })
    }

    fn rules_url(&self) -> reqwest::Url {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .expect("FaultHttpController accepts only hierarchical base URLs")
            .push("rules");
        url
    }

    fn rule_url(&self, rule_id: &str) -> Result<reqwest::Url, FaultControlError> {
        validate_rule_id(rule_id)?;
        let mut url = self.rules_url();
        url.path_segments_mut()
            .expect("FaultHttpController accepts only hierarchical base URLs")
            .push(rule_id);
        Ok(url)
    }

    async fn response_error(response: reqwest::Response) -> FaultControlError {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        FaultControlError::Transport(format!("HTTP {status}: {body}"))
    }
}

fn fault_api_url(base_url: &str) -> Result<reqwest::Url, FaultControlError> {
    let mut base_url = reqwest::Url::parse(base_url)
        .map_err(|error| FaultControlError::Transport(error.to_string()))?;
    if base_url.query().is_some() || base_url.fragment().is_some() {
        return Err(FaultControlError::Transport(
            "fault controller base URL must not contain a query or fragment".to_string(),
        ));
    }
    if base_url.path() != "/" {
        return Err(FaultControlError::Transport(
            "fault controller base URL must be a site root".to_string(),
        ));
    }
    base_url.set_path("/api/v1/debug/faults");
    Ok(base_url)
}

#[async_trait]
impl FaultController for FaultHttpController {
    async fn status(&self) -> Result<FaultRuntimeStatus, FaultControlError> {
        let response = self
            .client
            .get(self.base_url.clone())
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|error| FaultControlError::Transport(error.to_string()))?;
        if !response.status().is_success() {
            return Err(Self::response_error(response).await);
        }
        response
            .json()
            .await
            .map_err(|error| FaultControlError::Transport(error.to_string()))
    }

    async fn configure(
        &self,
        rule_id: &str,
        rule: ConfigureFaultRule,
    ) -> Result<FaultRuleView, FaultControlError> {
        let url = self.rule_url(rule_id)?;
        let response = self
            .client
            .put(url)
            .bearer_auth(&self.token)
            .json(&rule)
            .send()
            .await
            .map_err(|error| FaultControlError::Transport(error.to_string()))?;
        if !response.status().is_success() {
            return Err(Self::response_error(response).await);
        }
        response
            .json()
            .await
            .map_err(|error| FaultControlError::Transport(error.to_string()))
    }

    async fn rule(&self, rule_id: &str) -> Result<Option<FaultRuleView>, FaultControlError> {
        let url = self.rule_url(rule_id)?;
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|error| FaultControlError::Transport(error.to_string()))?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(Self::response_error(response).await);
        }
        response
            .json()
            .await
            .map(Some)
            .map_err(|error| FaultControlError::Transport(error.to_string()))
    }

    async fn remove(&self, rule_id: &str) -> Result<bool, FaultControlError> {
        let url = self.rule_url(rule_id)?;
        let response = self
            .client
            .delete(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|error| FaultControlError::Transport(error.to_string()))?;
        if !response.status().is_success() {
            return Err(Self::response_error(response).await);
        }
        #[derive(serde::Deserialize)]
        struct Removed {
            removed: bool,
        }
        response
            .json::<Removed>()
            .await
            .map(|result| result.removed)
            .map_err(|error| FaultControlError::Transport(error.to_string()))
    }

    async fn clear(&self) -> Result<usize, FaultControlError> {
        let response = self
            .client
            .delete(self.rules_url())
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|error| FaultControlError::Transport(error.to_string()))?;
        if !response.status().is_success() {
            return Err(Self::response_error(response).await);
        }
        #[derive(serde::Deserialize)]
        struct Cleared {
            cleared: usize,
        }
        response
            .json::<Cleared>()
            .await
            .map(|result| result.cleared)
            .map_err(|error| FaultControlError::Transport(error.to_string()))
    }
}

pub struct FaultTestSession {
    targets: BTreeMap<String, Arc<dyn FaultController>>,
}

impl FaultTestSession {
    pub fn new() -> Self {
        Self {
            targets: BTreeMap::new(),
        }
    }

    pub fn add_target(&mut self, name: impl Into<String>, controller: Arc<dyn FaultController>) {
        self.targets.insert(name.into(), controller);
    }

    pub async fn preflight(&self) -> Result<(), FaultControlError> {
        for controller in self.targets.values() {
            controller.preflight_clean().await?;
        }
        Ok(())
    }

    pub async fn configure(
        &self,
        target: &str,
        rule_id: &str,
        rule: ConfigureFaultRule,
    ) -> Result<FaultRuleView, FaultControlError> {
        self.target(target)?.configure(rule_id, rule).await
    }

    pub async fn observe(&self, target: &str) -> Result<FaultRuntimeStatus, FaultControlError> {
        self.target(target)?.status().await
    }

    pub async fn rule(
        &self,
        target: &str,
        rule_id: &str,
    ) -> Result<Option<FaultRuleView>, FaultControlError> {
        self.target(target)?.rule(rule_id).await
    }

    pub async fn wait_for_executions(
        &self,
        target: &str,
        rule_id: &str,
        expected: u64,
        timeout: Duration,
    ) -> Result<FaultRuleView, FaultControlError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut observed = 0;
        loop {
            let now = tokio::time::Instant::now();
            let remaining = deadline.saturating_duration_since(now);
            let rule = match tokio::time::timeout(remaining, self.rule(target, rule_id)).await {
                Ok(result) => result?.ok_or_else(|| FaultControlError::UnknownRule {
                    target: target.to_string(),
                    rule_id: rule_id.to_string(),
                })?,
                Err(_) => {
                    return Err(FaultControlError::WaitTimeout {
                        target: target.to_string(),
                        rule_id: rule_id.to_string(),
                        expected,
                        observed,
                    });
                }
            };
            observed = rule.executions;
            if rule.executions >= expected {
                return Ok(rule);
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(FaultControlError::WaitTimeout {
                    target: target.to_string(),
                    rule_id: rule_id.to_string(),
                    expected,
                    observed,
                });
            }
            tokio::time::sleep(Duration::from_millis(100).min(deadline - now)).await;
        }
    }

    pub async fn cleanup(&self) -> Result<(), FaultControlError> {
        let mut failures = Vec::new();
        for (target, controller) in &self.targets {
            if let Err(error) = controller.clear().await {
                failures.push(format!("{target} clear: {error}"));
            }
            if let Err(error) = controller.preflight_clean().await {
                failures.push(format!("{target} verify: {error}"));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(FaultControlError::Transport(format!(
                "fault cleanup failed: {}",
                failures.join("; ")
            )))
        }
    }

    fn target(&self, name: &str) -> Result<Arc<dyn FaultController>, FaultControlError> {
        self.targets
            .get(name)
            .cloned()
            .ok_or_else(|| FaultControlError::UnknownTarget(name.to_string()))
    }
}

impl Default for FaultTestSession {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, feature = "fault-injection"))]
mod tests {
    use super::*;
    use crate::{fault_point, FaultAction, FAULT_POINT_REGISTRATIONS};

    const CONTROLLER_POINT_NAME: &str = "client.controller.test";

    #[allow(dead_code)]
    fn declare_controller_point(runtime: &FaultRuntime) {
        fault_point! {
            sync,
            name: "client.controller.test",
            description: "controller test point",
            runtime: runtime,
            context: {},
        }
    }

    fn controller_point() -> &'static crate::FaultPointSpec {
        FAULT_POINT_REGISTRATIONS
            .iter()
            .find(|registration| registration.point.name == CONTROLLER_POINT_NAME)
            .unwrap()
            .point
    }

    struct HangingController;

    struct CleanupFailureController;

    #[async_trait]
    impl FaultController for HangingController {
        async fn status(&self) -> Result<FaultRuntimeStatus, FaultControlError> {
            std::future::pending().await
        }

        async fn configure(
            &self,
            _rule_id: &str,
            _rule: ConfigureFaultRule,
        ) -> Result<FaultRuleView, FaultControlError> {
            std::future::pending().await
        }

        async fn rule(&self, _rule_id: &str) -> Result<Option<FaultRuleView>, FaultControlError> {
            std::future::pending().await
        }

        async fn remove(&self, _rule_id: &str) -> Result<bool, FaultControlError> {
            std::future::pending().await
        }

        async fn clear(&self) -> Result<usize, FaultControlError> {
            std::future::pending().await
        }
    }

    #[async_trait]
    impl FaultController for CleanupFailureController {
        async fn status(&self) -> Result<FaultRuntimeStatus, FaultControlError> {
            Ok(FaultRuntimeStatus {
                points: Vec::new(),
                rules: Vec::new(),
            })
        }

        async fn configure(
            &self,
            _rule_id: &str,
            _rule: ConfigureFaultRule,
        ) -> Result<FaultRuleView, FaultControlError> {
            Err(FaultControlError::Transport(
                "configure is unavailable".to_string(),
            ))
        }

        async fn rule(&self, _rule_id: &str) -> Result<Option<FaultRuleView>, FaultControlError> {
            Ok(None)
        }

        async fn remove(&self, _rule_id: &str) -> Result<bool, FaultControlError> {
            Ok(false)
        }

        async fn clear(&self) -> Result<usize, FaultControlError> {
            Err(FaultControlError::Transport(
                "expected clear failure".to_string(),
            ))
        }
    }

    #[tokio::test]
    async fn session_preflight_observe_and_cleanup() {
        let runtime = FaultRuntime::isolated().unwrap();
        let mut session = FaultTestSession::new();
        session.add_target(
            "local",
            Arc::new(FaultLocalController::new(runtime.clone())),
        );
        session.preflight().await.unwrap();
        session
            .configure(
                "local",
                "record",
                ConfigureFaultRule {
                    point: CONTROLLER_POINT_NAME.to_string(),
                    action: FaultAction::Record,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        runtime
            .configure(
                "untracked",
                ConfigureFaultRule {
                    point: CONTROLLER_POINT_NAME.to_string(),
                    action: FaultAction::Record,
                    ..Default::default()
                },
            )
            .unwrap();
        runtime.evaluate(controller_point(), &crate::FaultContext::default());
        assert_eq!(
            session
                .rule("local", "record")
                .await
                .unwrap()
                .unwrap()
                .executions,
            1
        );
        assert_eq!(
            session
                .wait_for_executions("local", "record", 1, Duration::from_secs(1))
                .await
                .unwrap()
                .executions,
            1
        );
        session.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn cleanup_attempts_every_target_when_one_target_fails() {
        fn configure_record(runtime: &FaultRuntime, id: &str) {
            runtime
                .configure(
                    id,
                    ConfigureFaultRule {
                        point: CONTROLLER_POINT_NAME.to_string(),
                        action: FaultAction::Record,
                        ..Default::default()
                    },
                )
                .unwrap();
        }

        let first = FaultRuntime::isolated().unwrap();
        let second = FaultRuntime::isolated().unwrap();
        configure_record(&first, "first-rule");
        configure_record(&second, "second-rule");

        let mut session = FaultTestSession::new();
        session.add_target(
            "a-first",
            Arc::new(FaultLocalController::new(first.clone())),
        );
        session.add_target("b-failing", Arc::new(CleanupFailureController));
        session.add_target(
            "c-second",
            Arc::new(FaultLocalController::new(second.clone())),
        );

        let error = session.cleanup().await.unwrap_err().to_string();
        assert!(error.contains("b-failing clear"));
        assert!(first.status().rules.is_empty());
        assert!(second.status().rules.is_empty());
    }

    #[tokio::test]
    async fn session_reports_unknown_and_dirty_targets() {
        let runtime = FaultRuntime::isolated().unwrap();
        runtime
            .configure(
                "existing",
                ConfigureFaultRule {
                    point: CONTROLLER_POINT_NAME.to_string(),
                    action: FaultAction::Record,
                    ..Default::default()
                },
            )
            .unwrap();

        let mut session = FaultTestSession::new();
        session.add_target("local", Arc::new(FaultLocalController::new(runtime)));
        assert!(matches!(
            session.observe("missing").await,
            Err(FaultControlError::UnknownTarget(target)) if target == "missing"
        ));
        assert!(matches!(
            session.preflight().await,
            Err(FaultControlError::DirtyTarget { rules }) if rules == ["existing"]
        ));
        assert!(matches!(
            session
                .wait_for_executions("local", "existing", 1, Duration::ZERO)
                .await,
            Err(FaultControlError::WaitTimeout { observed: 0, .. })
        ));
    }

    #[tokio::test]
    async fn wait_deadline_bounds_a_hanging_controller_request() {
        let mut session = FaultTestSession::new();
        session.add_target("hanging", Arc::new(HangingController));
        let started = tokio::time::Instant::now();
        assert!(matches!(
            session
                .wait_for_executions("hanging", "rule", 1, Duration::from_millis(20),)
                .await,
            Err(FaultControlError::WaitTimeout { observed: 0, .. })
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
