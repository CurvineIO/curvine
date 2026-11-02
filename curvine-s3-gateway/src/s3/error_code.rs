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

use axum::http::StatusCode;
use std::borrow::Cow;
use std::fmt;

/// Generic error type for internal operations
#[derive(Debug, Clone)]
pub enum Error {
    Illegal,
    UnSupport,
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Illegal => write!(f, "Illegal"),
            Self::UnSupport => write!(f, "UnSupport"),
            Self::Other(arg0) => f.debug_tuple("Other").field(arg0).finish(),
        }
    }
}

impl std::error::Error for Error {}

/// Complete S3 error code enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3ErrorCode {
    // Access errors
    AccessDenied,
    AccountProblem,

    // Bucket errors
    BucketAlreadyExists,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    InvalidBucketName,
    NoSuchBucket,

    // Object errors
    NoSuchKey,
    NoSuchUpload,
    InvalidObjectState,

    // Request errors
    InvalidRequest,
    InvalidArgument,
    InvalidRange,
    InvalidURI,
    MalformedXML,
    MissingContentLength,
    MissingRequestBodyError,

    // Auth errors
    InvalidAccessKeyId,
    SignatureDoesNotMatch,
    ExpiredToken,

    // Size errors
    EntityTooLarge,
    EntityTooSmall,
    KeyTooLongError,

    // Multipart errors
    InvalidPart,
    InvalidPartOrder,

    // Server errors
    InternalError,
    ServiceUnavailable,
    SlowDown,

    // Method errors
    MethodNotAllowed,
    NotImplemented,
}

impl S3ErrorCode {
    /// Get HTTP status code for this error
    pub fn status_code(&self) -> StatusCode {
        match self {
            // 403 Forbidden
            Self::AccessDenied | Self::AccountProblem => StatusCode::FORBIDDEN,

            // 404 Not Found
            Self::NoSuchBucket | Self::NoSuchKey | Self::NoSuchUpload => StatusCode::NOT_FOUND,

            // 409 Conflict
            Self::BucketAlreadyExists
            | Self::BucketAlreadyOwnedByYou
            | Self::BucketNotEmpty
            | Self::InvalidObjectState => StatusCode::CONFLICT,

            // 400 Bad Request
            Self::InvalidBucketName
            | Self::InvalidRequest
            | Self::InvalidArgument
            | Self::InvalidURI
            | Self::MalformedXML
            | Self::MissingContentLength
            | Self::MissingRequestBodyError
            | Self::InvalidPart
            | Self::InvalidPartOrder
            | Self::KeyTooLongError => StatusCode::BAD_REQUEST,

            // 416 Range Not Satisfiable
            Self::InvalidRange => StatusCode::RANGE_NOT_SATISFIABLE,

            // 401 Unauthorized
            Self::InvalidAccessKeyId | Self::SignatureDoesNotMatch | Self::ExpiredToken => {
                StatusCode::UNAUTHORIZED
            }

            // 413 Payload Too Large
            Self::EntityTooLarge => StatusCode::PAYLOAD_TOO_LARGE,

            // 405 Method Not Allowed
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,

            // 501 Not Implemented
            Self::NotImplemented => StatusCode::NOT_IMPLEMENTED,

            // 503 Service Unavailable
            Self::ServiceUnavailable | Self::SlowDown => StatusCode::SERVICE_UNAVAILABLE,

            // 500 Internal Server Error (default)
            Self::InternalError | Self::EntityTooSmall => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Get error code string
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AccessDenied => "AccessDenied",
            Self::AccountProblem => "AccountProblem",
            Self::BucketAlreadyExists => "BucketAlreadyExists",
            Self::BucketAlreadyOwnedByYou => "BucketAlreadyOwnedByYou",
            Self::BucketNotEmpty => "BucketNotEmpty",
            Self::InvalidBucketName => "InvalidBucketName",
            Self::NoSuchBucket => "NoSuchBucket",
            Self::NoSuchKey => "NoSuchKey",
            Self::NoSuchUpload => "NoSuchUpload",
            Self::InvalidObjectState => "InvalidObjectState",
            Self::InvalidRequest => "InvalidRequest",
            Self::InvalidArgument => "InvalidArgument",
            Self::InvalidRange => "InvalidRange",
            Self::InvalidURI => "InvalidURI",
            Self::MalformedXML => "MalformedXML",
            Self::MissingContentLength => "MissingContentLength",
            Self::MissingRequestBodyError => "MissingRequestBodyError",
            Self::InvalidAccessKeyId => "InvalidAccessKeyId",
            Self::SignatureDoesNotMatch => "SignatureDoesNotMatch",
            Self::ExpiredToken => "ExpiredToken",
            Self::EntityTooLarge => "EntityTooLarge",
            Self::EntityTooSmall => "EntityTooSmall",
            Self::KeyTooLongError => "KeyTooLongError",
            Self::InvalidPart => "InvalidPart",
            Self::InvalidPartOrder => "InvalidPartOrder",
            Self::InternalError => "InternalError",
            Self::ServiceUnavailable => "ServiceUnavailable",
            Self::SlowDown => "SlowDown",
            Self::MethodNotAllowed => "MethodNotAllowed",
            Self::NotImplemented => "NotImplemented",
        }
    }
}

impl fmt::Display for S3ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// S3 Error with code, message, and metadata
#[derive(Debug)]
pub struct S3Error {
    code: S3ErrorCode,
    message: Option<Cow<'static, str>>,
    request_id: Option<String>,
}

impl S3Error {
    /// Create new error with code
    pub fn new(code: S3ErrorCode) -> Self {
        Self {
            code,
            message: None,
            request_id: None,
        }
    }

    /// Create error with message
    pub fn with_message(code: S3ErrorCode, msg: impl Into<Cow<'static, str>>) -> Self {
        Self {
            code,
            message: Some(msg.into()),
            request_id: None,
        }
    }

    /// Set request ID
    pub fn set_request_id(&mut self, id: impl Into<String>) {
        self.request_id = Some(id.into());
    }

    /// Get error code
    pub fn code(&self) -> S3ErrorCode {
        self.code
    }

    /// Get HTTP status code
    pub fn status_code(&self) -> StatusCode {
        self.code.status_code()
    }

    /// Get message
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Serialize to XML error response
    pub fn to_xml(&self) -> String {
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>");
        xml.push_str(&format!("<Code>{}</Code>", self.code.as_str()));
        if let Some(msg) = &self.message {
            xml.push_str(&format!("<Message>{}</Message>", msg));
        }
        if let Some(rid) = &self.request_id {
            xml.push_str(&format!("<RequestId>{}</RequestId>", rid));
        }
        xml.push_str("</Error>");
        xml
    }
}

impl fmt::Display for S3Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.code)?;
        if let Some(msg) = &self.message {
            write!(f, ": {}", msg)?;
        }
        Ok(())
    }
}

impl std::error::Error for S3Error {}

/// Convenience macro for creating S3 errors
#[macro_export]
macro_rules! s3_error {
    ($code:ident) => {
        $crate::s3::error_code::S3Error::new($crate::s3::error_code::S3ErrorCode::$code)
    };
    ($code:ident, $msg:expr) => {
        $crate::s3::error_code::S3Error::with_message(
            $crate::s3::error_code::S3ErrorCode::$code,
            $msg,
        )
    };
}

pub use s3_error;
