//! S3 Service Layer
//!
//! This module provides S3-compatible service implementations including:
//! - S3 protocol traits and types
//! - S3 request/response handling
//! - S3 operation implementations

pub mod error;
pub mod s3_api;
pub mod s3_handlers;
pub mod types;

// Export APIs to avoid conflicts with duplicated names in types
pub use error::*;
pub use s3_api::*;
// pub use types::*; // avoid ambiguous names by not glob-reexporting types
