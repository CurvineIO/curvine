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

use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("Authentication error: {0}")]
    Auth(#[from] AuthError),

    #[error("S3 API error: {0}")]
    S3Api(#[from] S3Error),

    #[error("HTTP error: {0}")]
    Http(#[from] HttpError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Configuration error: {0}")]
    Config(#[from] ConfigError),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("Invalid credentials")]
    InvalidCredentials,

    #[error("Account not found")]
    NotSignedUp,

    #[error("Credential store unavailable")]
    StoreUnavailable,

    #[error("Invalid credential format: {0}")]
    InvalidFormat(String),

    #[error("Credential expired")]
    Expired,

    #[error("Access denied")]
    AccessDenied,

    #[error("Missing authorization header")]
    MissingAuthHeader,

    #[error("Invalid signature: {0}")]
    InvalidSignature(String),

    #[error("Internal auth error: {0}")]
    Internal(String),
}

#[derive(Debug, Error)]
pub enum S3Error {
    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Bucket not found: {0}")]
    BucketNotFound(String),

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Body parse error: {0}")]
    BodyParse(#[from] BodyParseError),

    #[error("Unsupported operation: {0}")]
    Unsupported(String),

    #[error("Invalid parameters: {0}")]
    InvalidParams(String),

    #[error("Internal S3 error: {0}")]
    Internal(String),
}

#[derive(Debug, Error)]
pub enum BodyParseError {
    #[error("Hash validation failed")]
    HashMismatch,

    #[error("Content length incorrect")]
    ContentLengthMismatch,

    #[error("I/O error: {0}")]
    Io(String),

    #[error("Invalid content encoding: {0}")]
    InvalidEncoding(String),
}

#[derive(Debug, Error)]
pub enum HttpError {
    #[error("Invalid HTTP method: {0}")]
    InvalidMethod(String),

    #[error("Invalid headers: {0}")]
    InvalidHeaders(String),

    #[error("Request parsing error: {0}")]
    RequestParse(String),

    #[error("Response generation error: {0}")]
    ResponseGeneration(String),
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("File not found: {0}")]
    FileNotFound(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Storage quota exceeded")]
    QuotaExceeded,

    #[error("Cache operation failed: {0}")]
    CacheError(String),

    #[error("Filesystem error: {0}")]
    FilesystemError(String),
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Invalid configuration: {0}")]
    InvalidValue(String),

    #[error("Missing required configuration: {0}")]
    MissingValue(String),

    #[error("Configuration file error: {0}")]
    FileError(String),
}

pub type GatewayResult<T> = Result<T, GatewayError>;

pub type AuthResult<T> = Result<T, AuthError>;

pub type S3Result<T> = Result<T, S3Error>;

pub type StorageResult<T> = Result<T, StorageError>;

impl From<String> for GatewayError {
    fn from(s: String) -> Self {
        GatewayError::Internal(s)
    }
}

impl From<&str> for GatewayError {
    fn from(s: &str) -> Self {
        GatewayError::Internal(s.to_string())
    }
}

impl From<String> for AuthError {
    fn from(s: String) -> Self {
        AuthError::Internal(s)
    }
}

impl From<&str> for AuthError {
    fn from(s: &str) -> Self {
        AuthError::Internal(s.to_string())
    }
}

impl From<String> for S3Error {
    fn from(s: String) -> Self {
        S3Error::Internal(s)
    }
}

impl From<&str> for S3Error {
    fn from(s: &str) -> Self {
        S3Error::Internal(s.to_string())
    }
}
