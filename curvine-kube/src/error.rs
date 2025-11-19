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
pub type Result<T> = std::result::Result<T, KubeError>;

#[derive(Error, Debug)]
pub enum KubeError {
    #[error("Kubernetes API error: {0}")]
    KubeError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Resource not found: {resource_type} '{name}' in namespace '{namespace}'")]
    NotFound {
        resource_type: String,
        name: String,
        namespace: String,
    },

    #[error("Resource already exists: {resource_type} '{name}' in namespace '{namespace}'")]
    AlreadyExists {
        resource_type: String,
        name: String,
        namespace: String,
    },

    #[error("Timeout error: {0}")]
    Timeout(String),

    #[error("Invalid resource: {0}")]
    InvalidResource(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("YAML parse error: {0}")]
    YamlParse(#[from] serde_yaml::Error),

    #[error("TOML parse error: {0}")]
    TomlParse(#[from] toml::de::Error),

    #[error("JSON parse error: {0}")]
    JsonParse(#[from] serde_json::Error),
}

impl From<kube::Error> for KubeError {
    fn from(err: kube::Error) -> Self {
        KubeError::KubeError(err.to_string())
    }
}

impl KubeError {
    pub fn config_error(context: impl Into<String>) -> Self {
        Self::ConfigError(context.into())
    }

    pub fn not_found(
        resource_type: impl Into<String>,
        name: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        Self::NotFound {
            resource_type: resource_type.into(),
            name: name.into(),
            namespace: namespace.into(),
        }
    }

    pub fn already_exists(
        resource_type: impl Into<String>,
        name: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        Self::AlreadyExists {
            resource_type: resource_type.into(),
            name: name.into(),
            namespace: namespace.into(),
        }
    }
}
