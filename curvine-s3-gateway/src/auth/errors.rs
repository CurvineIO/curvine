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

//! # Authentication Error Types
//!
//! Provides structured error handling for authentication operations,
//! inspired by s3s's elegant error system design.

use std::fmt;
use thiserror::Error;

pub type AuthResult<T = (), E = AuthError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub struct AuthError {
    pub code: AuthErrorCode,
    pub message: Option<String>,
    #[source]
    pub source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthErrorCode {
    InvalidCredentials,
    NotSignedUp,
    StoreUnavailable,
    InternalError,
    InvalidFormat,
    Expired,
    AccessDenied,
}

impl AuthError {
    pub fn new(code: AuthErrorCode) -> Self {
        Self {
            code,
            message: None,
            source: None,
        }
    }

    pub fn with_message(code: AuthErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: Some(message.into()),
            source: None,
        }
    }

    pub fn with_source(
        code: AuthErrorCode,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            code,
            message: None,
            source: Some(Box::new(source)),
        }
    }

    pub fn with_message_and_source(
        code: AuthErrorCode,
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            code,
            message: Some(message.into()),
            source: Some(Box::new(source)),
        }
    }

    pub fn invalid_credentials(message: impl Into<String>) -> Self {
        Self::with_message(AuthErrorCode::InvalidCredentials, message)
    }

    pub fn not_signed_up(message: impl Into<String>) -> Self {
        Self::with_message(AuthErrorCode::NotSignedUp, message)
    }

    pub fn store_unavailable(message: impl Into<String>) -> Self {
        Self::with_message(AuthErrorCode::StoreUnavailable, message)
    }

    pub fn internal_error(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::with_source(AuthErrorCode::InternalError, source)
    }
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.message {
            Some(msg) => write!(f, "{}: {}", self.code, msg),
            None => write!(f, "{}", self.code),
        }
    }
}

impl fmt::Display for AuthErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthErrorCode::InvalidCredentials => write!(f, "InvalidCredentials"),
            AuthErrorCode::NotSignedUp => write!(f, "NotSignedUp"),
            AuthErrorCode::StoreUnavailable => write!(f, "StoreUnavailable"),
            AuthErrorCode::InternalError => write!(f, "InternalError"),
            AuthErrorCode::InvalidFormat => write!(f, "InvalidFormat"),
            AuthErrorCode::Expired => write!(f, "Expired"),
            AuthErrorCode::AccessDenied => write!(f, "AccessDenied"),
        }
    }
}

impl From<AuthErrorCode> for AuthError {
    fn from(code: AuthErrorCode) -> Self {
        Self::new(code)
    }
}
#[macro_export]
macro_rules! auth_error {
    ($code:ident) => {
        $crate::auth::errors::AuthError::new($crate::auth::errors::AuthErrorCode::$code)
    };
    ($code:ident, $msg:expr) => {
        $crate::auth::errors::AuthError::with_message(
            $crate::auth::errors::AuthErrorCode::$code,
            $msg,
        )
    };
    ($source:expr, $code:ident) => {{
        $crate::auth::errors::AuthError::with_source(
            $crate::auth::errors::AuthErrorCode::$code,
            $source,
        )
    }};
    ($source:expr, $code:ident, $msg:expr) => {{
        $crate::auth::errors::AuthError::with_message_and_source(
            $crate::auth::errors::AuthErrorCode::$code,
            $msg,
            $source,
        )
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_error_creation() {
        let err = AuthError::new(AuthErrorCode::InvalidCredentials);
        assert_eq!(err.code, AuthErrorCode::InvalidCredentials);
        assert!(err.message.is_none());
        assert!(err.source.is_none());
    }

    #[test]
    fn test_auth_error_with_message() {
        let err = AuthError::with_message(AuthErrorCode::NotSignedUp, "Account not found");
        assert_eq!(err.code, AuthErrorCode::NotSignedUp);
        assert_eq!(err.message.as_deref(), Some("Account not found"));
    }

    #[test]
    fn test_auth_error_display() {
        let err = AuthError::with_message(AuthErrorCode::InvalidCredentials, "Bad password");
        assert_eq!(format!("{}", err), "InvalidCredentials: Bad password");

        let err = AuthError::new(AuthErrorCode::InternalError);
        assert_eq!(format!("{}", err), "InternalError");
    }

    #[test]
    fn test_auth_error_macro() {
        let err = auth_error!(InvalidCredentials);
        assert_eq!(err.code, AuthErrorCode::InvalidCredentials);

        let err = auth_error!(NotSignedUp, "User not found");
        assert_eq!(err.code, AuthErrorCode::NotSignedUp);
        assert_eq!(err.message.as_deref(), Some("User not found"));
    }
}
