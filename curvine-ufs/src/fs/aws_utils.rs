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

use aws_config::timeout::TimeoutConfig;
use aws_config::{self, Region};
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_s3::{client as s3_client, config as s3_config};
use aws_types::SdkConfig;
use core::time::Duration;
use log;
use std::collections::HashMap;
use std::sync::Arc;

const AWS_CUSTOM_CONFIG_KEY: [&str; 3] = ["retry_times", "conn_timeout", "read_timeout"];

/// S3 connection parameter verification result
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3ConfigValidationResult {
    /// Valid configuration
    Valid,
    /// Necessary parameters are missing
    MissingRequiredParam(String),
    /// Parameter format error
    InvalidParamFormat { param_name: String, reason: String },
    /// Incomplete authentication information
    IncompleteCredentials(String),
}

impl S3ConfigValidationResult {
    /// Check whether the verification results are valid
    pub fn is_valid(&self) -> bool {
        matches!(self, S3ConfigValidationResult::Valid)
    }

    /// Get error message
    pub fn error_message(&self) -> Option<String> {
        match self {
            S3ConfigValidationResult::Valid => None,
            S3ConfigValidationResult::MissingRequiredParam(param) => {
                Some(format!("Missing the necessary s3 parameters: {}", param))
            }
            S3ConfigValidationResult::InvalidParamFormat { param_name, reason } => Some(format!(
                "S3 parameter format error - {}: {}",
                param_name, reason
            )),
            S3ConfigValidationResult::IncompleteCredentials(msg) => Some(format!(
                "S3 certification information is incomplete: {}",
                msg
            )),
        }
    }
}

pub fn default_conn_config() -> HashMap<String, u64> {
    let mut default_conn_config = HashMap::new();
    default_conn_config.insert("retry_times".to_owned(), 3_u64);
    default_conn_config.insert("conn_timeout".to_owned(), 3_u64);
    default_conn_config.insert("read_timeout".to_owned(), 5_u64);
    default_conn_config
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AwsCustomConfig {
    pub read_timeout: Duration,
    pub conn_timeout: Duration,
    pub retry_times: u32,
}

impl Default for AwsCustomConfig {
    fn default() -> Self {
        let map = default_conn_config();
        AwsCustomConfig::from(map)
    }
}

impl From<HashMap<String, u64>> for AwsCustomConfig {
    fn from(input_config: HashMap<String, u64>) -> Self {
        let mut config = AwsCustomConfig {
            read_timeout: Duration::from_secs(3),
            conn_timeout: Duration::from_secs(3),
            retry_times: 0,
        };
        for key in AWS_CUSTOM_CONFIG_KEY {
            let value = input_config.get(key);
            if let Some(config_value) = value {
                match key {
                    "retry_times" => {
                        config.retry_times = *config_value as u32;
                    }
                    "conn_timeout" => {
                        config.conn_timeout = Duration::from_secs(*config_value);
                    }
                    "read_timeout" => {
                        config.read_timeout = Duration::from_secs(*config_value);
                    }
                    _ => {
                        unreachable!()
                    }
                }
            } else {
                continue;
            }
        }
        config
    }
}

/// Verify S3 connection parameters
///
/// This function is used to verify the validity of S3 connection parameters, including:
/// -Check whether the necessary parameters (region_name, bucket_name) exist
/// -Verify that the endpoint_url format is correct (if provided)
/// -Check whether the authentication information is complete (both access and secret are either provided or neither)
///
/// # Parameters
/// -`config`: HashMap containing S3 configuration parameters
///
/// # return
/// Returns the verification result, if all parameters are valid, returns Valid
pub fn validate_s3_config(config: &HashMap<String, String>) -> S3ConfigValidationResult {
    // Check the endpoint url format (if present)
    if let Some(endpoint) = config.get("s3.endpoint_url") {
        if endpoint.is_empty() {
            return S3ConfigValidationResult::InvalidParamFormat {
                param_name: "s3.endpoint_url".to_string(),
                reason: "Can't be empty".to_string(),
            };
        }

        // Simple verification of url format
        if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            return S3ConfigValidationResult::InvalidParamFormat {
                param_name: "s3.endpoint_url".to_string(),
                reason: "Must start with http://or https://".to_string(),
            };
        }
    }

    // Check whether the certification information is complete
    let has_access = config.contains_key("s3.credentials.access");
    let has_secret = config.contains_key("s3.credentials.secret");

    if has_access != has_secret {
        let missing = if has_access {
            "s3.credentials.secret"
        } else {
            "s3.credentials.access"
        };

        return S3ConfigValidationResult::IncompleteCredentials(format!(
            "Access is provided but {} is missing",
            missing
        ));
    }

    // If authentication information is provided, check whether it is empty
    if has_access && has_secret {
        //Secure access and secret, theoretically, this will not fail because the existence of the key has been checked before
        //But still use a safe way to avoid panic
        let access = match config.get("s3.credentials.access") {
            Some(value) => value,
            None => {
                return S3ConfigValidationResult::IncompleteCredentials(
                    "Unable to get s3.credentials.access value".to_string(),
                )
            }
        };

        let secret = match config.get("s3.credentials.secret") {
            Some(value) => value,
            None => {
                return S3ConfigValidationResult::IncompleteCredentials(
                    "Unable to get s3.credentials.secret value".to_string(),
                )
            }
        };

        if access.is_empty() {
            return S3ConfigValidationResult::InvalidParamFormat {
                param_name: "s3.credentials.access".to_string(),
                reason: "Can't be empty".to_string(),
            };
        }

        if secret.is_empty() {
            return S3ConfigValidationResult::InvalidParamFormat {
                param_name: "s3.credentials.secret".to_string(),
                reason: "Can't be empty".to_string(),
            };
        }
    }

    S3ConfigValidationResult::Valid
}

/// Create AWS SDK configuration
///
/// # Parameters
/// -`region`: AWS Region Name
/// -`endpoint_url`: Optional custom endpoint URL
/// -`access_key`: Optional access key
/// -`secret_key`: Optional secret key
///
/// # return
/// Return to AWS SDK configuration
pub fn create_aws_config(
    region: String,
    endpoint_url: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
) -> SdkConfig {
    let mut config_builder = SdkConfig::builder().region(Region::new(region));

    // Setting up custom endpoints (if any)
    if let Some(endpoint) = endpoint_url {
        config_builder = config_builder.endpoint_url(endpoint);
    }

    // Set up authentication information (if any)
    if let (Some(access), Some(secret)) = (access_key, secret_key) {
        let credentials_provider = Credentials::new(access, secret, None, None, "Static");
        let shared_credentials_provider = SharedCredentialsProvider::new(credentials_provider);
        config_builder = config_builder.credentials_provider(shared_credentials_provider);
    }

    config_builder.build()
}

/// Create an AWS SDK configuration from a configuration map
///
/// # Parameters
/// -`config`: Mapping containing AWS configuration
///
/// # return
/// Return to AWS SDK configuration
pub fn create_aws_config_from_map(config: &HashMap<String, String>) -> SdkConfig {
    let region = config
        .get("s3.region_name")
        .cloned()
        .unwrap_or_else(|| "us-east-1".to_string());
    let endpoint = config.get("s3.endpoint_url").cloned();
    let access_key = config.get("s3.credentials.access").cloned();
    let secret_key = config.get("s3.credentials.secret").cloned();

    create_aws_config(region, endpoint, access_key, secret_key)
}

/// Create S3 client
///
/// # Parameters
/// -`sdk_config`: AWS SDK configuration
/// -`config_pairs`: Optional custom configuration parameters
///
/// # return
/// Return to S3 client
pub fn s3_client(
    sdk_config: &SdkConfig,
    config_pairs: Option<HashMap<String, u64>>,
) -> aws_sdk_s3::Client {
    let s3_config_obj = if let Some(config) = config_pairs {
        let s3_config = AwsCustomConfig::from(config);
        let retry_conf =
            aws_config::retry::RetryConfig::standard().with_max_attempts(s3_config.retry_times);
        let timeout_conf = TimeoutConfig::builder()
            .connect_timeout(s3_config.conn_timeout)
            .read_timeout(s3_config.read_timeout)
            .build();

        s3_config::Builder::from(&sdk_config.clone())
            .retry_config(retry_conf)
            .timeout_config(timeout_conf)
            .force_path_style(true)
            .build()
    } else {
        s3_config::Config::new(sdk_config)
    };
    s3_client::Client::from_conf(s3_config_obj)
}

/// Create S3 client and wrap it in Arc
///
/// # Parameters
/// -`config`: Mapping containing AWS configuration
/// -`custom_config`: Optional custom connection configuration
///
/// # return
/// Returns the S3 client wrapped in Arc, or returns an error when the configuration is invalid
pub fn create_s3_client(
    config: &HashMap<String, String>,
) -> Result<Arc<aws_sdk_s3::Client>, String> {
    // Verify the configuration before creating the client
    let validation_result = validate_s3_config(config);
    if !validation_result.is_valid() {
        // Get the error message and return the error
        if let Some(error_msg) = validation_result.error_message() {
            log::error!("S3 configuration verification failed: {}", error_msg);
            return Err(error_msg);
        }
    }

    let aws_config = create_aws_config_from_map(config);
    Ok(Arc::new(s3_client(
        &aws_config,
        Some(default_conn_config()),
    )))
}
