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

use crate::{FaultHttpConfig, FaultHttpError};

#[cfg(feature = "http-server")]
use crate::http_server::process_fault_router;
#[cfg(feature = "http-server")]
use crate::FaultHttpAuth;

/// Optional HTTP exposure for the crate-owned process fault Runtime.
///
/// Hosts construct this once from their configuration and pass their Axum
/// router through [`Self::mount`]. Business fault points do not depend on
/// this type or on an HTTP server.
#[derive(Clone, Default)]
pub struct FaultHttpControl {
    #[cfg(feature = "http-server")]
    auth: Option<FaultHttpAuth>,
}

impl FaultHttpControl {
    /// Creates an HTTP control-plane mount from an environment-backed token.
    ///
    /// A disabled configuration creates a no-op mount and never reads the
    /// environment. Enabling the configuration without compiling the
    /// `http-server` feature fails instead of silently omitting the routes.
    pub fn from_env(config: &FaultHttpConfig) -> Result<Self, FaultHttpError> {
        if !config.enabled {
            return Ok(Self::default());
        }
        Self::enabled_from_env(&config.auth_token_env)
    }

    #[cfg(feature = "http-server")]
    fn enabled_from_env(token_env: &str) -> Result<Self, FaultHttpError> {
        if token_env.is_empty() {
            return Err(FaultHttpError::TokenEnvironmentNotConfigured);
        }
        let token = std::env::var(token_env)
            .map_err(|_| FaultHttpError::TokenEnvironmentNotSet(token_env.to_string()))?;
        if token.is_empty() {
            return Err(FaultHttpError::EmptyToken(token_env.to_string()));
        }
        Ok(Self {
            auth: Some(FaultHttpAuth::new(token)),
        })
    }

    #[cfg(not(feature = "http-server"))]
    fn enabled_from_env(_token_env: &str) -> Result<Self, FaultHttpError> {
        Err(FaultHttpError::FeatureDisabled)
    }

    /// Merges the authenticated fault routes when HTTP control is enabled.
    #[cfg(feature = "http-server")]
    pub fn mount(&self, router: axum::Router) -> axum::Router {
        match &self.auth {
            Some(auth) => router.merge(process_fault_router(auth.clone())),
            None => router,
        }
    }

    /// Returns the host value unchanged when HTTP support is not compiled.
    #[cfg(not(feature = "http-server"))]
    pub fn mount<T>(&self, host: T) -> T {
        host
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_control_does_not_require_token_configuration() {
        let control = FaultHttpControl::from_env(&FaultHttpConfig::default()).unwrap();

        #[cfg(feature = "http-server")]
        assert!(control.auth.is_none());
        #[cfg(not(feature = "http-server"))]
        assert_eq!(control.mount(7), 7);
    }

    #[cfg(not(feature = "http-server"))]
    #[test]
    fn enabled_control_requires_http_server_feature() {
        let Err(error) = FaultHttpControl::from_env(&FaultHttpConfig {
            enabled: true,
            auth_token_env: "FAULT_HTTP_TOKEN".to_string(),
        }) else {
            panic!("enabled HTTP control must require the http-server feature");
        };
        assert!(matches!(error, FaultHttpError::FeatureDisabled));
    }

    #[cfg(feature = "http-server")]
    #[test]
    fn enabled_control_requires_token_environment_name() {
        let Err(error) = FaultHttpControl::from_env(&FaultHttpConfig {
            enabled: true,
            auth_token_env: String::new(),
        }) else {
            panic!("enabled HTTP control must require a token environment name");
        };
        assert!(matches!(
            error,
            FaultHttpError::TokenEnvironmentNotConfigured
        ));
    }
}
