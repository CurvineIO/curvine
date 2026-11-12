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

use curvine_core_error::{err_box, CommonResult};
use curvine_runtime::common::DurationUnit;
use std::collections::HashMap;
use std::ops::Deref;
use std::time::Duration;
use url::Url;

pub struct ConfMap(HashMap<String, String>);

impl ConfMap {
    pub fn new(map: HashMap<String, String>) -> Self {
        Self(map)
    }

    pub fn get_string(&self, key: &str) -> CommonResult<String> {
        if let Some(v) = self.0.get(key) {
            Ok(v.clone())
        } else {
            err_box!("{} cannot be empty", key)
        }
    }

    pub fn get_bool(&self, key: &str) -> CommonResult<bool> {
        let value = self.get_string(key)?;
        match value.trim().to_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => err_box!("{} must be true or false", key),
        }
    }

    pub fn get_bool_or(&self, key: &str, default: bool) -> CommonResult<bool> {
        if self.0.contains_key(key) {
            self.get_bool(key)
        } else {
            Ok(default)
        }
    }

    pub fn get_u32(&self, key: &str) -> CommonResult<u32> {
        let value = self.get_string(key)?;
        value
            .trim()
            .parse::<u32>()
            .map_err(|_| format!("Invalid u32 value for key '{}': {}", key, value).into())
    }

    pub fn get_u32_or(&self, key: &str, default: u32) -> CommonResult<u32> {
        if self.0.contains_key(key) {
            self.get_u32(key)
        } else {
            Ok(default)
        }
    }

    pub fn get_u64(&self, key: &str) -> CommonResult<u64> {
        let value = self.get_string(key)?;
        value
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("Invalid u64 value for key '{}': {}", key, value).into())
    }

    pub fn get_u64_or(&self, key: &str, default: u64) -> CommonResult<u64> {
        if self.0.contains_key(key) {
            self.get_u64(key)
        } else {
            Ok(default)
        }
    }
}

/// Where the S3 credential comes from.
///
/// `S3Conf` used to require a static `access`/`secret` pair, which forced every deployment to mint
/// a long-lived key and hand it to Curvine. A mount can now name a credential *source* instead, so
/// an operator can hand Curvine an identity (a role, a node/instance role, a workload identity)
/// rather than a secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum S3CredentialKind {
    /// Long-lived access key + secret key. This is the default, so mounts created before the
    /// selector existed keep working unchanged.
    #[default]
    Static,
    /// Temporary credentials: access key + secret key + session token.
    Sts,
    /// Assume `role_arn`. The source credential used for the AssumeRole call is resolved from the
    /// process environment (env vars, `~/.aws/*`, IMDSv2, or a web identity token file).
    Role,
    /// No credential configuration at all: leave the decision to the SDK default chain.
    Auto,
}

impl S3CredentialKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Static => "static",
            Self::Sts => "sts",
            Self::Role => "role",
            Self::Auto => "auto",
        }
    }

    pub fn parse(value: &str) -> CommonResult<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "static" => Ok(Self::Static),
            "sts" => Ok(Self::Sts),
            "role" => Ok(Self::Role),
            "auto" => Ok(Self::Auto),
            other => err_box!(
                "{} must be one of static, sts, role, auto; got '{}'",
                S3Conf::CRED_TYPE,
                other
            ),
        }
    }
}

/// Resolved credential configuration for an S3 mount.
///
/// This is the single parser shared by the CLI, the master and the UFS adapters: previously each
/// of them read the `s3.*` keys on its own and they disagreed about what was required.
#[derive(Debug, Clone, Default)]
pub struct S3CredentialPlan {
    pub kind: S3CredentialKind,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub session_token: Option<String>,
    pub role_arn: Option<String>,
    pub role_session_name: Option<String>,
    pub external_id: Option<String>,
    /// Do not let the SDK load configuration from the environment (`~/.aws/*`, env vars).
    pub disable_config_load: bool,
    /// Do not let the SDK load credentials from the EC2 instance metadata service.
    pub disable_ec2_metadata: bool,
}

impl S3CredentialPlan {
    /// True when the plan carries no long-lived secret, i.e. the credential is resolved per
    /// process from the runtime environment. A `role` mount that carries its own source key pair
    /// is *not* environment-resolved: the AssumeRole source is the caller's key, so the mount
    /// needs no `master.s3_allow_env_credentials` opt-in.
    pub fn is_environment_resolved(&self) -> bool {
        match self.kind {
            S3CredentialKind::Auto => true,
            S3CredentialKind::Role => self.access_key.is_none() && self.secret_key.is_none(),
            S3CredentialKind::Static | S3CredentialKind::Sts => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct S3Conf {
    pub endpoint_url: String,
    pub credential: S3CredentialPlan,
    pub region_name: String,
    pub force_path_style: bool,

    pub retry_times: u32,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,

    pub properties: HashMap<String, String>,
}

/// Unified OpenDAL configuration for timeout and retry settings
/// Uses the same default values as S3Conf for consistency
#[derive(Debug, Clone)]
pub struct OpendalConf {
    pub retry_times: u32,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub retry_interval_ms: u64,
    pub retry_max_delay_ms: u64,
    /// Per-request read buffer size (bytes) for random reads and each concurrent
    /// sequential Range. Configured by [`Self::READ_CHUNK_SIZE`].
    pub read_chunk_size: usize,
    /// Number of concurrent Range requests issued for sequential reads.
    /// Configured by [`Self::READ_CONCURRENT`].
    pub read_concurrent: usize,
    /// Hard upper bound for a single UFS operation as seen by Curvine.
    ///
    /// The per-attempt limits (`connect_timeout`, `read_timeout`) and
    /// `retry_times` are enforced *inside* the retry loop, so the worst case of
    /// one call is `(retry_times + 1)` attempts plus backoff. This bound wraps
    /// the whole call instead, so a blackholed object-store path fails with a
    /// `Timeout` (ETIMEDOUT) instead of staying started indefinitely.
    ///
    /// Covers the operations whose cost does not grow with the object size:
    /// stat, create_dir, create, open, the native rename, delete, list (each
    /// page pull), one read chunk, one write chunk, and the best-effort abort
    /// of a failed upload. Whole-object operations use
    /// [`Self::commit_timeout`] instead.
    /// Configured by [`Self::OPERATION_TIMEOUT`].
    pub operation_timeout: Duration,
    /// Hard upper bound for a UFS operation whose duration grows with the
    /// object size: the server-side `copy` of the rename fallback on backends
    /// without a native rename (e.g. S3), and the writer `close`, which uploads
    /// the last part and commits a multipart upload.
    /// Configured by [`Self::COMMIT_TIMEOUT`].
    pub commit_timeout: Duration,
}

impl S3Conf {
    pub const ENDPOINT: &'static str = "s3.endpoint_url";
    pub const ACCESS_KEY: &'static str = "s3.credentials.access";
    pub const SECRET_KEY: &'static str = "s3.credentials.secret";
    pub const REGION_NAME: &'static str = "s3.region_name";
    pub const CRED_TYPE: &'static str = "s3.credentials.type";
    pub const CRED_TOKEN: &'static str = "s3.credentials.token";
    pub const CRED_ROLE_ARN: &'static str = "s3.credentials.role_arn";
    pub const CRED_ROLE_SESSION_NAME: &'static str = "s3.credentials.role_session_name";
    pub const CRED_EXTERNAL_ID: &'static str = "s3.credentials.external_id";
    pub const CRED_DISABLE_CONFIG_LOAD: &'static str = "s3.credentials.disable_config_load";
    pub const CRED_DISABLE_EC2_METADATA: &'static str = "s3.credentials.disable_ec2_metadata";
    pub const LEGACY_ACCESS_KEY: &'static str = "s3.access_key";
    pub const LEGACY_SECRET_KEY: &'static str = "s3.secret_key";
    pub const LEGACY_REGION_NAME: &'static str = "s3.region";
    pub const LEGACY_SESSION_TOKEN: &'static str = "s3.session_token";
    pub const LEGACY_SECURITY_TOKEN: &'static str = "s3.security_token";
    pub const LEGACY_ROLE_ARN: &'static str = "s3.role_arn";
    pub const LEGACY_ROLE_SESSION_NAME: &'static str = "s3.role_session_name";
    pub const LEGACY_EXTERNAL_ID: &'static str = "s3.external_id";
    pub const FORCE_PATH_STYLE: &'static str = "s3.force.path.style";
    pub const LIST_OBJECTS_VERSION: &'static str = "s3.list_objects_version";
    pub const RETRY_TIMES: &'static str = "s3.retry_times";
    pub const CONNECT_TIMEOUT: &'static str = "s3.connect_timeout";
    pub const READ_TIMEOUT: &'static str = "s3.read_timeout";

    /// Canonical credential keys. Everything else under `s3.credentials.` is rejected: a typo, or
    /// a field the caller expected to be used, must not be dropped on the floor (a silently ignored
    /// `..._token` looks exactly like "STS is configured").
    pub const SUPPORTED_CREDENTIAL_KEYS: [&'static str; 9] = [
        Self::CRED_TYPE,
        Self::ACCESS_KEY,
        Self::SECRET_KEY,
        Self::CRED_TOKEN,
        Self::CRED_ROLE_ARN,
        Self::CRED_ROLE_SESSION_NAME,
        Self::CRED_EXTERNAL_ID,
        Self::CRED_DISABLE_CONFIG_LOAD,
        Self::CRED_DISABLE_EC2_METADATA,
    ];

    pub const DEFAULT_RETRY_TIMES: u32 = 3;
    pub const DEFAULT_CONNECT_TIMEOUT: &'static str = "30s";
    pub const DEFAULT_READ_TIMEOUT: &'static str = "120s";

    /// Legacy property spellings and their canonical names.
    const LEGACY_ALIASES: [(&'static str, &'static str); 8] = [
        (Self::LEGACY_ACCESS_KEY, Self::ACCESS_KEY),
        (Self::LEGACY_SECRET_KEY, Self::SECRET_KEY),
        (Self::LEGACY_REGION_NAME, Self::REGION_NAME),
        (Self::LEGACY_SESSION_TOKEN, Self::CRED_TOKEN),
        (Self::LEGACY_SECURITY_TOKEN, Self::CRED_TOKEN),
        (Self::LEGACY_ROLE_ARN, Self::CRED_ROLE_ARN),
        (Self::LEGACY_ROLE_SESSION_NAME, Self::CRED_ROLE_SESSION_NAME),
        (Self::LEGACY_EXTERNAL_ID, Self::CRED_EXTERNAL_ID),
    ];

    /// Map a legacy property spelling to its canonical name; unknown keys are returned unchanged.
    ///
    /// Callers that merge properties into an existing mount (or remove one of them) need this: the
    /// stored mount holds canonical keys, so an update that names a key by an alias has to be
    /// translated *before* the merge, otherwise both spellings coexist and look like a conflict.
    pub fn canonical_key(key: &str) -> &str {
        for (legacy, canonical) in Self::LEGACY_ALIASES {
            if key == legacy {
                return canonical;
            }
        }
        key
    }

    pub fn validate_region(properties: &HashMap<String, String>) -> CommonResult<()> {
        Self::required_property(properties, Self::REGION_NAME)
    }

    /// Reject any `s3.credentials.*` key the schema does not understand.
    fn reject_unknown_credential_keys(properties: &HashMap<String, String>) -> CommonResult<()> {
        const PREFIX: &str = "s3.credentials.";

        for key in properties.keys() {
            if !key.starts_with(PREFIX) {
                continue;
            }
            if Self::SUPPORTED_CREDENTIAL_KEYS.contains(&key.as_str()) {
                continue;
            }

            let mut supported = Self::SUPPORTED_CREDENTIAL_KEYS.to_vec();
            supported.sort_unstable();
            return err_box!(
                "{} is not a supported S3 credential key; supported keys: {}",
                key,
                supported.join(", ")
            );
        }
        Ok(())
    }

    pub fn validate(properties: &HashMap<String, String>) -> CommonResult<()> {
        let conf = Self::with_map(properties.clone())?;
        Self::uses_list_objects_v1(&conf.properties)?;
        OpendalConf::from_map(&conf.properties)?;
        Ok(())
    }

    /// Convert the S3 property names used by older clients into the mount schema.
    pub fn canonicalize_properties(
        mut properties: HashMap<String, String>,
    ) -> CommonResult<HashMap<String, String>> {
        for (legacy, canonical) in Self::LEGACY_ALIASES {
            let Some(value) = properties.remove(legacy) else {
                continue;
            };

            if let Some(canonical_value) = properties.get(canonical) {
                if canonical_value != &value {
                    return err_box!(
                        "conflicting S3 configuration: both {} and {} are set",
                        legacy,
                        canonical
                    );
                }
            } else {
                properties.insert(canonical.to_string(), value);
            }
        }
        Ok(properties)
    }

    pub fn uses_list_objects_v1(properties: &HashMap<String, String>) -> CommonResult<bool> {
        match properties
            .get(Self::LIST_OBJECTS_VERSION)
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("") | Some("v2") => Ok(false),
            Some("v1") => Ok(true),
            Some(value) => err_box!(
                "s3.list_objects_version must be 'v1' or 'v2', got '{}'",
                value
            ),
        }
    }

    fn required_property(properties: &HashMap<String, String>, key: &str) -> CommonResult<()> {
        if properties
            .get(key)
            .is_some_and(|value| !value.trim().is_empty())
        {
            Ok(())
        } else {
            err_box!("{} is required", key)
        }
    }

    /// A property is "set" when it exists and has non-whitespace content. Values are preserved
    /// verbatim (secrets are never trimmed) so a padded value can never be silently accepted as a
    /// different one.
    fn optional_property(properties: &HashMap<String, String>, key: &str) -> Option<String> {
        properties
            .get(key)
            .filter(|value| !value.trim().is_empty())
            .cloned()
    }

    /// Parse and validate the credential section of an S3 mount.
    ///
    /// Accepts both canonical and legacy key spellings, so it is safe to call on raw mount
    /// properties. Returns a plan only when the configuration is unambiguous; unknown selectors,
    /// half-configured key pairs and fields that do not apply to the selected kind are rejected
    /// instead of being silently ignored (a silently ignored field looks like "STS is configured"
    /// while a static key is still in use).
    ///
    /// See issue #1972 for the key reference, the per-participant identity contract and the known
    /// limits of the pinned SDK version (AWS-only STS endpoint, env-only web identity token,
    /// fixed IMDS address).
    pub fn credential_plan(properties: &HashMap<String, String>) -> CommonResult<S3CredentialPlan> {
        let properties = Self::canonicalize_properties(properties.clone())?;
        Self::reject_unknown_credential_keys(&properties)?;

        let kind = match Self::optional_property(&properties, Self::CRED_TYPE) {
            Some(raw) => S3CredentialKind::parse(&raw)?,
            None => S3CredentialKind::Static,
        };

        let access_key = Self::optional_property(&properties, Self::ACCESS_KEY);
        let secret_key = Self::optional_property(&properties, Self::SECRET_KEY);
        let session_token = Self::optional_property(&properties, Self::CRED_TOKEN);
        let role_arn = Self::optional_property(&properties, Self::CRED_ROLE_ARN);
        let role_session_name = Self::optional_property(&properties, Self::CRED_ROLE_SESSION_NAME);
        let external_id = Self::optional_property(&properties, Self::CRED_EXTERNAL_ID);

        // A half-configured key pair is a mistake for every kind: it is either a typo or a field
        // the caller expected to be used.
        match (&access_key, &secret_key) {
            (Some(_), None) => {
                return err_box!(
                    "{} is required when {} is set",
                    Self::SECRET_KEY,
                    Self::ACCESS_KEY
                )
            }
            (None, Some(_)) => {
                return err_box!(
                    "{} is required when {} is set",
                    Self::ACCESS_KEY,
                    Self::SECRET_KEY
                )
            }
            _ => {}
        }

        // AssumeRole-only fields must not be accepted under another kind.
        if kind != S3CredentialKind::Role {
            for key in [
                Self::CRED_ROLE_ARN,
                Self::CRED_ROLE_SESSION_NAME,
                Self::CRED_EXTERNAL_ID,
            ] {
                if properties.contains_key(key) {
                    return err_box!(
                        "{} is only supported when {}={}; got {}",
                        key,
                        Self::CRED_TYPE,
                        S3CredentialKind::Role.as_str(),
                        kind.as_str()
                    );
                }
            }
        }

        match kind {
            S3CredentialKind::Static => {
                if access_key.is_none() {
                    return err_box!(
                        "{} is required when {} is {} (or unset); set {} to {} or {} to use an identity instead of a key",
                        Self::ACCESS_KEY,
                        Self::CRED_TYPE,
                        S3CredentialKind::Static.as_str(),
                        Self::CRED_TYPE,
                        S3CredentialKind::Role.as_str(),
                        S3CredentialKind::Auto.as_str()
                    );
                }
                if secret_key.is_none() {
                    return err_box!(
                        "{} is required when {} is {} (or unset)",
                        Self::SECRET_KEY,
                        Self::CRED_TYPE,
                        S3CredentialKind::Static.as_str()
                    );
                }
                if session_token.is_some() {
                    return err_box!(
                        "{} is only supported when {}={}",
                        Self::CRED_TOKEN,
                        Self::CRED_TYPE,
                        S3CredentialKind::Sts.as_str()
                    );
                }
            }

            S3CredentialKind::Sts => {
                if access_key.is_none() || secret_key.is_none() {
                    return err_box!(
                        "{} and {} are required when {}={}",
                        Self::ACCESS_KEY,
                        Self::SECRET_KEY,
                        Self::CRED_TYPE,
                        S3CredentialKind::Sts.as_str()
                    );
                }
                if session_token.is_none() {
                    return err_box!(
                        "{} is required when {}={}",
                        Self::CRED_TOKEN,
                        Self::CRED_TYPE,
                        S3CredentialKind::Sts.as_str()
                    );
                }
            }

            S3CredentialKind::Role => {
                if role_arn.is_none() {
                    return err_box!(
                        "{} is required when {}={}",
                        Self::CRED_ROLE_ARN,
                        Self::CRED_TYPE,
                        S3CredentialKind::Role.as_str()
                    );
                }
                if session_token.is_some() {
                    return err_box!(
                        "{} is only supported when {}={}",
                        Self::CRED_TOKEN,
                        Self::CRED_TYPE,
                        S3CredentialKind::Sts.as_str()
                    );
                }
            }

            S3CredentialKind::Auto => {
                for key in [Self::ACCESS_KEY, Self::SECRET_KEY, Self::CRED_TOKEN] {
                    if properties.contains_key(key) {
                        return err_box!(
                            "{} must not be set when {}={}; the credential is resolved from the environment",
                            key,
                            Self::CRED_TYPE,
                            S3CredentialKind::Auto.as_str()
                        );
                    }
                }
            }
        }

        let map = ConfMap::new(properties);
        Ok(S3CredentialPlan {
            kind,
            access_key,
            secret_key,
            session_token,
            role_arn,
            role_session_name,
            external_id,
            disable_config_load: map.get_bool_or(Self::CRED_DISABLE_CONFIG_LOAD, false)?,
            disable_ec2_metadata: map.get_bool_or(Self::CRED_DISABLE_EC2_METADATA, false)?,
        })
    }

    pub fn with_map(properties: HashMap<String, String>) -> CommonResult<Self> {
        let map = ConfMap::new(Self::canonicalize_properties(properties)?);

        Self::required_property(&map.0, Self::ENDPOINT)?;
        let endpoint_url = map.get_string(Self::ENDPOINT)?;
        let endpoint = Url::parse(&endpoint_url)
            .map_err(|_| "s3.endpoint_url must be an absolute http(s) URL with a host")?;
        if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
            return err_box!("s3.endpoint_url must be an absolute http(s) URL with a host");
        }

        // The credential section is validated (and its selector applied) by the shared parser, so
        // a mount can carry an identity instead of a long-lived key.
        let credential = Self::credential_plan(&map.0)?;

        Self::validate_region(&map.0)?;
        let region_name = map.get_string(Self::REGION_NAME)?;

        let force_path_style = map.get_bool_or(Self::FORCE_PATH_STYLE, false)?;

        let retry_times = map.get_u32_or(Self::RETRY_TIMES, Self::DEFAULT_RETRY_TIMES)?;

        let connect_timeout = map
            .get_string(Self::CONNECT_TIMEOUT)
            .unwrap_or(Self::DEFAULT_CONNECT_TIMEOUT.to_string());
        let connect_timeout = DurationUnit::from_str(&connect_timeout)?.as_duration();

        let read_timeout = map
            .get_string(Self::READ_TIMEOUT)
            .unwrap_or(Self::DEFAULT_READ_TIMEOUT.to_string());
        let read_timeout = DurationUnit::from_str(&read_timeout)?.as_duration();

        Ok(Self {
            endpoint_url,
            credential,
            region_name,
            force_path_style,
            retry_times,
            connect_timeout,
            read_timeout,
            properties: map.0,
        })
    }
}

impl OpendalConf {
    // Configuration keys
    pub const RETRY_TIMES: &'static str = "opendal.retry_times";
    pub const CONNECT_TIMEOUT: &'static str = "opendal.connect_timeout";
    pub const READ_TIMEOUT: &'static str = "opendal.read_timeout";
    pub const RETRY_INTERVAL_MS: &'static str = "opendal.retry_interval_ms";
    pub const RETRY_MAX_DELAY_MS: &'static str = "opendal.retry_max_delay_ms";
    pub const READ_CHUNK_SIZE: &'static str = "opendal.read_chunk_size";
    pub const READ_CONCURRENT: &'static str = "opendal.read_concurrent";
    pub const OPERATION_TIMEOUT: &'static str = "opendal.operation_timeout";
    pub const COMMIT_TIMEOUT: &'static str = "opendal.commit_timeout";

    // Default values - using S3Conf defaults for consistency
    pub const DEFAULT_RETRY_TIMES: u32 = S3Conf::DEFAULT_RETRY_TIMES; // 3
    pub const DEFAULT_CONNECT_TIMEOUT: &'static str = S3Conf::DEFAULT_CONNECT_TIMEOUT; // "30s"
    pub const DEFAULT_READ_TIMEOUT: &'static str = S3Conf::DEFAULT_READ_TIMEOUT; // "120s"
    pub const DEFAULT_RETRY_INTERVAL_MS: u64 = 1000; // 1 second
    pub const DEFAULT_RETRY_MAX_DELAY_MS: u64 = 10000; // 10 seconds
    pub const DEFAULT_READ_CHUNK_SIZE: usize = 1024 * 1024; // 1 MiB
    pub const DEFAULT_READ_CONCURRENT: usize = 4; // 4 parallel range requests per object
                                                  // 30s keeps a foreground metadata call (HEAD/stat) inside a 60s response
                                                  // budget even after the retry loop, and still allows ~2.2 Mbps for one 8 MiB
                                                  // write chunk. Mounts on very slow links can raise it per mount.
    pub const DEFAULT_OPERATION_TIMEOUT: &'static str = "30s";
    // A server-side CopyObject or CompleteMultipartUpload of a multi-GiB object
    // can take minutes; 600s still ends a blackholed commit in bounded time.
    pub const DEFAULT_COMMIT_TIMEOUT: &'static str = "600s";

    /// Create OpendalConf from configuration map
    pub fn from_map(properties: &HashMap<String, String>) -> CommonResult<Self> {
        let map = ConfMap::new(properties.clone());

        let retry_times = map.get_u32_or(Self::RETRY_TIMES, Self::DEFAULT_RETRY_TIMES)?;

        let connect_timeout_str = map
            .get_string(Self::CONNECT_TIMEOUT)
            .unwrap_or(Self::DEFAULT_CONNECT_TIMEOUT.to_string());
        let connect_timeout = DurationUnit::from_str(&connect_timeout_str)?.as_duration();

        let read_timeout_str = map
            .get_string(Self::READ_TIMEOUT)
            .unwrap_or(Self::DEFAULT_READ_TIMEOUT.to_string());
        let read_timeout = DurationUnit::from_str(&read_timeout_str)?.as_duration();

        let retry_interval_ms =
            map.get_u64_or(Self::RETRY_INTERVAL_MS, Self::DEFAULT_RETRY_INTERVAL_MS)?;

        let retry_max_delay_ms =
            map.get_u64_or(Self::RETRY_MAX_DELAY_MS, Self::DEFAULT_RETRY_MAX_DELAY_MS)?;

        let read_chunk_size = usize::try_from(
            map.get_u64_or(Self::READ_CHUNK_SIZE, Self::DEFAULT_READ_CHUNK_SIZE as u64)?,
        )
        .map_err(|_| {
            curvine_core_error::CommonError::from(format!("{} is too large", Self::READ_CHUNK_SIZE))
        })?;
        if read_chunk_size == 0 {
            return err_box!("{} must be greater than 0", Self::READ_CHUNK_SIZE);
        }
        let read_concurrent = usize::try_from(
            map.get_u64_or(Self::READ_CONCURRENT, Self::DEFAULT_READ_CONCURRENT as u64)?,
        )
        .map_err(|_| {
            curvine_core_error::CommonError::from(format!("{} is too large", Self::READ_CONCURRENT))
        })?;
        if read_concurrent == 0 {
            return err_box!("{} must be greater than 0", Self::READ_CONCURRENT);
        }

        let operation_timeout_str = map
            .get_string(Self::OPERATION_TIMEOUT)
            .unwrap_or(Self::DEFAULT_OPERATION_TIMEOUT.to_string());
        let operation_timeout = DurationUnit::from_str(&operation_timeout_str)?.as_duration();
        if operation_timeout.is_zero() {
            return err_box!("{} must be greater than 0", Self::OPERATION_TIMEOUT);
        }

        let commit_timeout_str = map
            .get_string(Self::COMMIT_TIMEOUT)
            .unwrap_or(Self::DEFAULT_COMMIT_TIMEOUT.to_string());
        let commit_timeout = DurationUnit::from_str(&commit_timeout_str)?.as_duration();
        if commit_timeout.is_zero() {
            return err_box!("{} must be greater than 0", Self::COMMIT_TIMEOUT);
        }

        Ok(Self {
            retry_times,
            connect_timeout,
            read_timeout,
            retry_interval_ms,
            retry_max_delay_ms,
            read_chunk_size,
            read_concurrent,
            operation_timeout,
            commit_timeout,
        })
    }

    pub fn total_timeout_ms(&self) -> u64 {
        self.connect_timeout.as_millis() as u64 + self.read_timeout.as_millis() as u64
    }
}

impl Deref for S3Conf {
    type Target = HashMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.properties
    }
}

/// OSS-HDFS configuration (JindoSDK).
///
/// Notes:
/// - User-facing configuration keys use `oss.*` format (without `fs.` prefix).
/// - JindoSDK internal configuration keys use `fs.oss.*` format (with `fs.` prefix).
/// - This config struct is used by the `oss://` UFS implementation (`oss_hdfs` module).
#[derive(Debug, Clone)]
pub struct OssHdfsConf {
    pub endpoint_url: String,
    pub access_key: String,
    pub secret_key: String,
    pub region_name: Option<String>,
    pub data_endpoint: Option<String>,
    pub second_level_domain_enable: bool,
    pub data_lake_storage_enable: bool,
    pub properties: HashMap<String, String>,
}

impl OssHdfsConf {
    // User-facing configuration keys (without fs. prefix)
    pub const USER_ENDPOINT: &'static str = "oss.endpoint";
    pub const USER_ACCESS_KEY_ID: &'static str = "oss.accessKeyId";
    pub const USER_ACCESS_KEY_SECRET: &'static str = "oss.accessKeySecret";
    pub const USER_REGION: &'static str = "oss.region";
    pub const USER_DATA_ENDPOINT: &'static str = "oss.data.endpoint";
    pub const USER_SECOND_LEVEL_DOMAIN_ENABLE: &'static str = "oss.second.level.domain.enable";
    pub const USER_DATA_LAKE_STORAGE_ENABLE: &'static str = "oss.data.lake.storage.enable";

    // JindoSDK internal configuration keys (with fs. prefix)
    pub const ENDPOINT: &'static str = "fs.oss.endpoint";
    pub const ACCESS_KEY_ID: &'static str = "fs.oss.accessKeyId";
    pub const ACCESS_KEY_SECRET: &'static str = "fs.oss.accessKeySecret";
    pub const REGION: &'static str = "fs.oss.region";
    pub const DATA_ENDPOINT: &'static str = "fs.oss.data.endpoint";
    pub const SECOND_LEVEL_DOMAIN_ENABLE: &'static str = "fs.oss.second.level.domain.enable";
    pub const DATA_LAKE_STORAGE_ENABLE: &'static str = "fs.oss.data.lake.storage.enable";

    // Default values
    pub const DEFAULT_SECOND_LEVEL_DOMAIN_ENABLE: bool = true;
    pub const DEFAULT_DATA_LAKE_STORAGE_ENABLE: bool = true;

    /// Create OssHdfsConf from configuration map
    /// Uses user-facing configuration keys (oss.*) for reading from config map
    pub fn with_map(properties: HashMap<String, String>) -> CommonResult<Self> {
        let map = ConfMap::new(properties);

        let endpoint_url = map.get_string(Self::USER_ENDPOINT)?;
        let access_key = map.get_string(Self::USER_ACCESS_KEY_ID)?;
        let secret_key = map.get_string(Self::USER_ACCESS_KEY_SECRET)?;
        let region_name = map.get_string(Self::USER_REGION).ok();
        let data_endpoint = map.get_string(Self::USER_DATA_ENDPOINT).ok();
        let second_level_domain_enable = map
            .get_bool(Self::USER_SECOND_LEVEL_DOMAIN_ENABLE)
            .unwrap_or(Self::DEFAULT_SECOND_LEVEL_DOMAIN_ENABLE);
        let data_lake_storage_enable = map
            .get_bool(Self::USER_DATA_LAKE_STORAGE_ENABLE)
            .unwrap_or(Self::DEFAULT_DATA_LAKE_STORAGE_ENABLE);

        Ok(Self {
            endpoint_url,
            access_key,
            secret_key,
            region_name,
            data_endpoint,
            second_level_domain_enable,
            data_lake_storage_enable,
            properties: map.0,
        })
    }
}

impl Deref for OssHdfsConf {
    type Target = HashMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.properties
    }
}

#[cfg(test)]
mod opendal_conf_tests {
    use super::OpendalConf;
    use curvine_runtime::common::DurationUnit;
    use std::collections::HashMap;
    use std::time::Duration;

    #[test]
    fn defaults_when_unset() {
        let conf = OpendalConf::from_map(&HashMap::new()).unwrap();
        assert_eq!(conf.read_chunk_size, OpendalConf::DEFAULT_READ_CHUNK_SIZE);
        assert_eq!(conf.read_concurrent, OpendalConf::DEFAULT_READ_CONCURRENT);
        assert_eq!(conf.read_chunk_size, 1024 * 1024);
        assert_eq!(conf.read_concurrent, 4);
        assert_eq!(
            conf.operation_timeout,
            DurationUnit::from_str(OpendalConf::DEFAULT_OPERATION_TIMEOUT)
                .unwrap()
                .as_duration()
        );
        assert_eq!(conf.operation_timeout, Duration::from_secs(30));
        assert_eq!(conf.commit_timeout, Duration::from_secs(600));
    }

    #[test]
    fn commit_timeout_is_user_overridable_and_rejects_zero() {
        let props = HashMap::from([(OpendalConf::COMMIT_TIMEOUT.to_string(), "90s".to_string())]);
        let conf = OpendalConf::from_map(&props).unwrap();
        assert_eq!(conf.commit_timeout, Duration::from_secs(90));
        assert_eq!(conf.operation_timeout, Duration::from_secs(30));

        for value in ["0", "0s", "not-a-duration"] {
            let props =
                HashMap::from([(OpendalConf::COMMIT_TIMEOUT.to_string(), value.to_string())]);
            assert!(
                OpendalConf::from_map(&props).is_err(),
                "{value} should be rejected"
            );
        }
    }

    #[test]
    fn operation_timeout_is_user_overridable() {
        let props = HashMap::from([(OpendalConf::OPERATION_TIMEOUT.to_string(), "5s".to_string())]);
        let conf = OpendalConf::from_map(&props).unwrap();
        assert_eq!(conf.operation_timeout, Duration::from_secs(5));
        assert_ne!(
            conf.operation_timeout,
            conf.connect_timeout + conf.read_timeout
        );
    }

    #[test]
    fn operation_timeout_rejects_zero_and_invalid_values() {
        for value in ["0", "0s", "not-a-duration"] {
            let props = HashMap::from([(
                OpendalConf::OPERATION_TIMEOUT.to_string(),
                value.to_string(),
            )]);
            assert!(
                OpendalConf::from_map(&props).is_err(),
                "{value} should be rejected"
            );
        }
    }

    #[test]
    fn read_tuning_is_user_overridable() {
        let mut props = HashMap::new();
        props.insert("opendal.read_chunk_size".to_string(), "1024".to_string());
        props.insert("opendal.read_concurrent".to_string(), "32".to_string());
        let conf = OpendalConf::from_map(&props).unwrap();
        assert_eq!(conf.read_chunk_size, 1024);
        assert_eq!(conf.read_concurrent, 32);
    }

    #[test]
    fn read_tuning_rejects_zero_and_invalid_values() {
        for (key, value) in [
            (OpendalConf::READ_CHUNK_SIZE, "0"),
            (OpendalConf::READ_CONCURRENT, "0"),
            (OpendalConf::READ_CHUNK_SIZE, "invalid"),
            (OpendalConf::READ_CONCURRENT, "invalid"),
        ] {
            let props = HashMap::from([(key.to_string(), value.to_string())]);
            assert!(
                OpendalConf::from_map(&props).is_err(),
                "{key}={value} should be rejected"
            );
        }
    }
}

#[cfg(test)]
mod s3_conf_tests {
    use super::{S3Conf, S3CredentialKind};
    use std::collections::HashMap;

    fn with(entries: &[(&str, &str)]) -> HashMap<String, String> {
        let mut properties = HashMap::from([
            (
                S3Conf::ENDPOINT.to_string(),
                "http://s3.example.com".to_string(),
            ),
            (S3Conf::REGION_NAME.to_string(), "cn-test-1".to_string()),
        ]);
        for (key, value) in entries {
            properties.insert((*key).to_string(), (*value).to_string());
        }
        properties
    }

    #[test]
    fn requires_non_empty_region() {
        let mut properties = HashMap::new();

        assert!(S3Conf::validate_region(&properties).is_err());

        properties.insert(S3Conf::REGION_NAME.to_string(), "  ".to_string());
        assert!(S3Conf::validate_region(&properties).is_err());

        properties.insert(S3Conf::REGION_NAME.to_string(), "cn-test-1".to_string());
        assert!(S3Conf::validate_region(&properties).is_ok());
    }

    #[test]
    fn rejects_empty_credentials() {
        let mut properties = HashMap::from([
            (
                S3Conf::ENDPOINT.to_string(),
                "http://s3.example.com".to_string(),
            ),
            (S3Conf::ACCESS_KEY.to_string(), " ".to_string()),
            (S3Conf::SECRET_KEY.to_string(), "secret-key".to_string()),
            (S3Conf::REGION_NAME.to_string(), "cn-test-1".to_string()),
        ]);

        let err = S3Conf::with_map(properties.clone()).expect_err("empty access key must fail");
        assert!(err.to_string().contains(S3Conf::ACCESS_KEY));

        properties.insert(S3Conf::ACCESS_KEY.to_string(), "access-key".to_string());
        properties.insert(S3Conf::SECRET_KEY.to_string(), " ".to_string());
        let err = S3Conf::with_map(properties).expect_err("empty secret key must fail");
        assert!(err.to_string().contains(S3Conf::SECRET_KEY));
    }

    #[test]
    fn canonicalizes_legacy_s3_properties() {
        let properties = HashMap::from([
            (
                S3Conf::ENDPOINT.to_string(),
                "http://s3.example.com".to_string(),
            ),
            (
                S3Conf::LEGACY_ACCESS_KEY.to_string(),
                "access-key".to_string(),
            ),
            (
                S3Conf::LEGACY_SECRET_KEY.to_string(),
                "secret-key".to_string(),
            ),
            (
                S3Conf::LEGACY_REGION_NAME.to_string(),
                "cn-test-1".to_string(),
            ),
        ]);

        let conf = S3Conf::with_map(properties).expect("legacy S3 configuration must work");
        assert_eq!(conf.credential.kind, S3CredentialKind::Static);
        assert_eq!(conf.credential.access_key.as_deref(), Some("access-key"));
        assert_eq!(conf.credential.secret_key.as_deref(), Some("secret-key"));
        assert_eq!(conf.region_name, "cn-test-1");
        assert!(!conf.properties.contains_key(S3Conf::LEGACY_ACCESS_KEY));
        assert!(!conf.properties.contains_key(S3Conf::LEGACY_SECRET_KEY));
        assert!(!conf.properties.contains_key(S3Conf::LEGACY_REGION_NAME));
    }

    #[test]
    fn rejects_conflicting_legacy_and_canonical_property() {
        let properties = HashMap::from([
            (
                S3Conf::LEGACY_REGION_NAME.to_string(),
                "cn-test-1".to_string(),
            ),
            (S3Conf::REGION_NAME.to_string(), "cn-test-2".to_string()),
        ]);

        let err = S3Conf::canonicalize_properties(properties)
            .expect_err("conflicting S3 property names must fail");
        assert!(err.to_string().contains(S3Conf::LEGACY_REGION_NAME));
        assert!(err.to_string().contains(S3Conf::REGION_NAME));
    }

    #[test]
    fn credential_kind_defaults_to_static() {
        let plan = S3Conf::credential_plan(&with(&[
            (S3Conf::ACCESS_KEY, "ak"),
            (S3Conf::SECRET_KEY, "sk"),
        ]))
        .expect("static credentials must validate");

        assert_eq!(plan.kind, S3CredentialKind::Static);
        assert_eq!(plan.access_key.as_deref(), Some("ak"));
        assert_eq!(plan.session_token, None);
        assert!(!plan.is_environment_resolved());
    }

    #[test]
    fn sts_requires_session_token() {
        let err = S3Conf::credential_plan(&with(&[
            (S3Conf::CRED_TYPE, "sts"),
            (S3Conf::ACCESS_KEY, "ak"),
            (S3Conf::SECRET_KEY, "sk"),
        ]))
        .expect_err("sts without a token must be rejected");
        assert!(err.to_string().contains(S3Conf::CRED_TOKEN), "{err}");

        let plan = S3Conf::credential_plan(&with(&[
            (S3Conf::CRED_TYPE, "sts"),
            (S3Conf::ACCESS_KEY, "ak"),
            (S3Conf::SECRET_KEY, "sk"),
            (S3Conf::CRED_TOKEN, "token"),
        ]))
        .expect("sts with a token must validate");
        assert_eq!(plan.kind, S3CredentialKind::Sts);
        assert_eq!(plan.session_token.as_deref(), Some("token"));
    }

    #[test]
    fn static_rejects_session_token() {
        let err = S3Conf::credential_plan(&with(&[
            (S3Conf::ACCESS_KEY, "ak"),
            (S3Conf::SECRET_KEY, "sk"),
            (S3Conf::CRED_TOKEN, "token"),
        ]))
        .expect_err("a static mount must not silently ignore a token");
        assert!(err.to_string().contains(S3Conf::CRED_TOKEN), "{err}");
        assert!(
            err.to_string().contains(S3CredentialKind::Sts.as_str()),
            "{err}"
        );
    }

    #[test]
    fn role_accepts_a_role_arn_without_any_key() {
        let plan = S3Conf::credential_plan(&with(&[
            (S3Conf::CRED_TYPE, "role"),
            (
                S3Conf::CRED_ROLE_ARN,
                "arn:aws:iam::123456789012:role/curvine-ufs",
            ),
            (S3Conf::CRED_ROLE_SESSION_NAME, "curvine"),
            (S3Conf::CRED_EXTERNAL_ID, "ext"),
            (S3Conf::CRED_DISABLE_EC2_METADATA, "true"),
        ]))
        .expect("a role mount needs no long-lived key");

        assert_eq!(plan.kind, S3CredentialKind::Role);
        assert_eq!(plan.access_key, None);
        assert_eq!(plan.secret_key, None);
        assert_eq!(plan.role_session_name.as_deref(), Some("curvine"));
        assert_eq!(plan.external_id.as_deref(), Some("ext"));
        assert!(plan.disable_ec2_metadata);
        assert!(!plan.disable_config_load);
        assert!(plan.is_environment_resolved());
    }

    #[test]
    fn role_accepts_a_static_source_credential() {
        // Documented transition mode: a long-lived key is used only as the AssumeRole source.
        let plan = S3Conf::credential_plan(&with(&[
            (S3Conf::CRED_TYPE, "role"),
            (S3Conf::ACCESS_KEY, "ak"),
            (S3Conf::SECRET_KEY, "sk"),
            (S3Conf::CRED_ROLE_ARN, "arn:role"),
        ]))
        .expect("a source credential is allowed for role mounts");

        assert_eq!(plan.kind, S3CredentialKind::Role);
        assert_eq!(plan.access_key.as_deref(), Some("ak"));
        assert!(!plan.is_environment_resolved());
    }

    #[test]
    fn role_with_a_source_key_is_not_environment_resolved() {
        // The AssumeRole source is the caller's own key, so the mount needs no
        // master.s3_allow_env_credentials opt-in; a role without any key stays environment-resolved.
        let keyed = S3Conf::credential_plan(&with(&[
            (S3Conf::CRED_TYPE, "role"),
            (S3Conf::ACCESS_KEY, "ak"),
            (S3Conf::SECRET_KEY, "sk"),
            (S3Conf::CRED_ROLE_ARN, "arn:role"),
        ]))
        .expect("a role mount with a source key pair must validate");
        assert!(!keyed.is_environment_resolved());

        let bare = S3Conf::credential_plan(&with(&[
            (S3Conf::CRED_TYPE, "role"),
            (S3Conf::CRED_ROLE_ARN, "arn:role"),
        ]))
        .expect("a role mount without any key must validate");
        assert!(bare.is_environment_resolved());

        let half = S3Conf::credential_plan(&with(&[
            (S3Conf::CRED_TYPE, "role"),
            (S3Conf::ACCESS_KEY, "ak"),
            (S3Conf::CRED_ROLE_ARN, "arn:role"),
        ]))
        .expect_err("a half-configured source key pair must be rejected");
        assert!(half.to_string().contains(S3Conf::SECRET_KEY), "{half}");
    }

    #[test]
    fn role_requires_role_arn() {
        let err = S3Conf::credential_plan(&with(&[(S3Conf::CRED_TYPE, "role")]))
            .expect_err("role without a role arn must be rejected");
        assert!(err.to_string().contains(S3Conf::CRED_ROLE_ARN), "{err}");
    }

    #[test]
    fn role_fields_require_the_role_kind() {
        let err = S3Conf::credential_plan(&with(&[
            (S3Conf::ACCESS_KEY, "ak"),
            (S3Conf::SECRET_KEY, "sk"),
            (S3Conf::CRED_ROLE_ARN, "arn:role"),
        ]))
        .expect_err("role_arn under a static mount must be rejected");
        assert!(err.to_string().contains(S3Conf::CRED_ROLE_ARN), "{err}");
    }

    #[test]
    fn auto_rejects_credential_fields() {
        for key in [S3Conf::ACCESS_KEY, S3Conf::SECRET_KEY, S3Conf::CRED_TOKEN] {
            let err =
                S3Conf::credential_plan(&with(&[(S3Conf::CRED_TYPE, "auto"), (key, "value")]))
                    .expect_err("auto must not carry credential fields");
            assert!(err.to_string().contains(key), "{err}");
        }

        let plan = S3Conf::credential_plan(&with(&[(S3Conf::CRED_TYPE, "auto")]))
            .expect("auto needs no credential field");
        assert_eq!(plan.kind, S3CredentialKind::Auto);
        assert!(plan.is_environment_resolved());
    }

    #[test]
    fn half_configured_key_pair_is_rejected() {
        let err = S3Conf::credential_plan(&with(&[(S3Conf::ACCESS_KEY, "ak")]))
            .expect_err("access key without secret must be rejected");
        assert!(err.to_string().contains(S3Conf::SECRET_KEY), "{err}");

        let err = S3Conf::credential_plan(&with(&[(S3Conf::SECRET_KEY, "sk")]))
            .expect_err("secret key without access must be rejected");
        assert!(err.to_string().contains(S3Conf::ACCESS_KEY), "{err}");
    }

    #[test]
    fn unknown_credential_kind_is_rejected() {
        let err = S3Conf::credential_plan(&with(&[(S3Conf::CRED_TYPE, "ram")]))
            .expect_err("unknown credential kind must be rejected");
        assert!(err.to_string().contains(S3Conf::CRED_TYPE), "{err}");
        assert!(err.to_string().contains("static, sts, role, auto"), "{err}");
    }

    #[test]
    fn credential_kind_is_case_insensitive() {
        let plan = S3Conf::credential_plan(&with(&[
            (S3Conf::CRED_TYPE, " ROLE "),
            (S3Conf::CRED_ROLE_ARN, "arn:role"),
        ]))
        .expect("selector must tolerate case and whitespace");
        assert_eq!(plan.kind, S3CredentialKind::Role);
    }

    #[test]
    fn canonicalizes_legacy_credential_aliases() {
        let plan = S3Conf::credential_plan(&with(&[
            (S3Conf::CRED_TYPE, "role"),
            (S3Conf::LEGACY_ROLE_ARN, "arn:role"),
            (S3Conf::LEGACY_ROLE_SESSION_NAME, "session"),
            (S3Conf::LEGACY_EXTERNAL_ID, "ext"),
        ]))
        .expect("legacy role aliases must canonicalize");
        assert_eq!(plan.role_arn.as_deref(), Some("arn:role"));
        assert_eq!(plan.role_session_name.as_deref(), Some("session"));
        assert_eq!(plan.external_id.as_deref(), Some("ext"));

        for legacy in [S3Conf::LEGACY_SESSION_TOKEN, S3Conf::LEGACY_SECURITY_TOKEN] {
            let plan = S3Conf::credential_plan(&with(&[
                (S3Conf::CRED_TYPE, "sts"),
                (S3Conf::ACCESS_KEY, "ak"),
                (S3Conf::SECRET_KEY, "sk"),
                (legacy, "token"),
            ]))
            .unwrap_or_else(|err| panic!("{legacy} must canonicalize: {err}"));
            assert_eq!(plan.session_token.as_deref(), Some("token"));
        }
    }

    #[test]
    fn conflicting_legacy_and_canonical_token_is_rejected() {
        let err = S3Conf::credential_plan(&with(&[
            (S3Conf::CRED_TYPE, "sts"),
            (S3Conf::ACCESS_KEY, "ak"),
            (S3Conf::SECRET_KEY, "sk"),
            (S3Conf::CRED_TOKEN, "token-a"),
            (S3Conf::LEGACY_SESSION_TOKEN, "token-b"),
        ]))
        .expect_err("conflicting token spellings must be rejected");
        assert!(
            err.to_string().contains(S3Conf::LEGACY_SESSION_TOKEN),
            "{err}"
        );
    }

    #[test]
    fn with_map_exposes_the_credential_plan() {
        let conf =
            S3Conf::with_map(with(&[(S3Conf::CRED_TYPE, "auto")])).expect("auto mount must parse");
        assert_eq!(conf.credential.kind, S3CredentialKind::Auto);
        assert!(S3Conf::validate(&conf.properties).is_ok());
    }

    #[test]
    fn static_without_a_key_pair_is_rejected() {
        // Pins the backward-compat anchor: `static` must never silently fall back to the
        // environment when the key pair is missing.
        let err = S3Conf::credential_plan(&with(&[])).expect_err("no credential field must fail");
        assert!(err.to_string().contains(S3Conf::ACCESS_KEY), "{err}");

        let err = S3Conf::credential_plan(&with(&[(S3Conf::CRED_TYPE, "static")]))
            .expect_err("explicit static without keys must fail");
        assert!(err.to_string().contains(S3Conf::ACCESS_KEY), "{err}");
    }

    #[test]
    fn rejects_unknown_credential_keys() {
        // A token spelled outside the schema must not be accepted as `static` and dropped.
        let err = S3Conf::credential_plan(&with(&[
            (S3Conf::ACCESS_KEY, "ak"),
            (S3Conf::SECRET_KEY, "sk"),
            ("s3.credentials.session_token", "token"),
        ]))
        .expect_err("an unknown credential key must be rejected");
        assert!(
            err.to_string().contains("s3.credentials.session_token"),
            "{err}"
        );

        // `auto` must not accept fields it does not understand either.
        let err = S3Conf::credential_plan(&with(&[
            (S3Conf::CRED_TYPE, "auto"),
            ("s3.credentials.some_future_field", "x"),
        ]))
        .expect_err("auto with an unknown field must be rejected");
        assert!(err.to_string().contains("some_future_field"), "{err}");

        // A typo in a supported key is rejected too, instead of silently keeping the default.
        let err = S3Conf::credential_plan(&with(&[
            (S3Conf::CRED_TYPE, "auto"),
            ("s3.credentials.disable_ec2_metadataa", "true"),
        ]))
        .expect_err("a typo in a supported key must be rejected");
        assert!(err.to_string().contains("disable_ec2_metadataa"), "{err}");
    }

    #[test]
    fn canonical_key_maps_legacy_spellings() {
        assert_eq!(
            S3Conf::canonical_key(S3Conf::LEGACY_SESSION_TOKEN),
            S3Conf::CRED_TOKEN
        );
        assert_eq!(
            S3Conf::canonical_key(S3Conf::LEGACY_SECURITY_TOKEN),
            S3Conf::CRED_TOKEN
        );
        assert_eq!(
            S3Conf::canonical_key(S3Conf::LEGACY_ROLE_ARN),
            S3Conf::CRED_ROLE_ARN
        );
        assert_eq!(S3Conf::canonical_key(S3Conf::CRED_TYPE), S3Conf::CRED_TYPE);
        assert_eq!(S3Conf::canonical_key("s3.other"), "s3.other");
    }

    #[test]
    fn list_objects_v2_is_the_default() {
        let mut properties = HashMap::new();
        assert!(!S3Conf::uses_list_objects_v1(&properties).unwrap());

        properties.insert(S3Conf::LIST_OBJECTS_VERSION.to_string(), "  ".to_string());
        assert!(!S3Conf::uses_list_objects_v1(&properties).unwrap());

        properties.insert(S3Conf::LIST_OBJECTS_VERSION.to_string(), "V2".to_string());
        assert!(!S3Conf::uses_list_objects_v1(&properties).unwrap());
    }

    #[test]
    fn list_objects_v1_is_supported() {
        let properties =
            HashMap::from([(S3Conf::LIST_OBJECTS_VERSION.to_string(), " v1 ".to_string())]);

        assert!(S3Conf::uses_list_objects_v1(&properties).unwrap());
    }

    #[test]
    fn rejects_invalid_list_objects_version() {
        let properties =
            HashMap::from([(S3Conf::LIST_OBJECTS_VERSION.to_string(), "v3".to_string())]);

        let err = S3Conf::uses_list_objects_v1(&properties)
            .expect_err("invalid ListObjects version must fail");
        assert!(err.to_string().contains(S3Conf::LIST_OBJECTS_VERSION));
    }

    #[test]
    fn rejects_invalid_optional_values() {
        let mut properties = HashMap::from([
            (
                S3Conf::ENDPOINT.to_string(),
                "http://s3.example.com".to_string(),
            ),
            (S3Conf::ACCESS_KEY.to_string(), "access-key".to_string()),
            (S3Conf::SECRET_KEY.to_string(), "secret-key".to_string()),
            (S3Conf::REGION_NAME.to_string(), "cn-test-1".to_string()),
            (S3Conf::FORCE_PATH_STYLE.to_string(), "maybe".to_string()),
        ]);

        let err = S3Conf::validate(&properties).expect_err("invalid bool must fail");
        assert!(err.to_string().contains(S3Conf::FORCE_PATH_STYLE));

        properties.insert(S3Conf::FORCE_PATH_STYLE.to_string(), "true".to_string());
        properties.insert("opendal.retry_times".to_string(), "many".to_string());
        let err = S3Conf::validate(&properties).expect_err("invalid retry count must fail");
        assert!(err.to_string().contains("opendal.retry_times"));
    }

    #[test]
    fn rejects_endpoint_without_host() {
        let properties = HashMap::from([
            (S3Conf::ENDPOINT.to_string(), "http://".to_string()),
            (S3Conf::ACCESS_KEY.to_string(), "access-key".to_string()),
            (S3Conf::SECRET_KEY.to_string(), "secret-key".to_string()),
            (S3Conf::REGION_NAME.to_string(), "cn-test-1".to_string()),
        ]);

        let err = S3Conf::validate(&properties).expect_err("endpoint without host must fail");
        assert!(err.to_string().contains(S3Conf::ENDPOINT));
    }
}
