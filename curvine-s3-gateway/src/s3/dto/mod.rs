// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0

//! S3 Data Transfer Objects (DTOs)
//!
//! This module contains all S3 API request/response structures organized by category.
//! Each submodule focuses on a specific S3 operation category.

mod bucket;
mod common;
mod multipart;
mod object;

pub use bucket::*;
pub use common::*;
pub use multipart::*;
pub use object::*;
