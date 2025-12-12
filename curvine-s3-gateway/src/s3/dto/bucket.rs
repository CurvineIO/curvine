// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0

//! Bucket-related S3 DTOs

use serde::Serialize;

use super::common::Owner;

/// Single bucket info
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Bucket {
    pub name: String,
    pub creation_date: String,
    pub bucket_region: String,
}

/// Bucket list wrapper
#[derive(Debug, Serialize)]
pub struct Buckets {
    #[serde(rename = "Bucket")]
    pub bucket: Vec<Bucket>,
}

/// List All Buckets result
#[derive(Debug, Serialize)]
#[serde(rename = "ListAllMyBucketsResult")]
#[serde(rename_all = "PascalCase")]
pub struct ListAllMyBucketsResult {
    #[serde(rename = "xmlns", skip_serializing_if = "String::is_empty")]
    pub xmlns: String,
    pub owner: Owner,
    pub buckets: Buckets,
}

/// List Buckets options
#[derive(Debug, Default)]
pub struct ListBucketsOption {
    pub bucket_region: Option<String>,
    pub continuation_token: Option<String>,
    pub max_buckets: Option<i32>,
    pub prefix: Option<String>,
}

/// Create Bucket options
#[derive(Debug, Default, Clone)]
pub struct CreateBucketOption {
    pub acl: Option<String>,
    pub grant_full_control: Option<String>,
    pub grant_read: Option<String>,
    pub grant_read_acp: Option<String>,
    pub grant_write: Option<String>,
    pub grant_write_acp: Option<String>,
    pub object_lock_enabled_for_bucket: Option<bool>,
    pub object_ownership: Option<String>,
}

/// Create Bucket result
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CreateBucketResult {
    pub location: Option<String>,
}

/// Get Bucket Location result
#[derive(Debug, Serialize)]
#[serde(rename = "LocationConstraint")]
pub struct GetBucketLocationResult {
    #[serde(rename = "$value", skip_serializing_if = "Option::is_none")]
    pub location_constraint: Option<String>,
}

/// Head Bucket result
#[derive(Debug, Default)]
pub struct HeadBucketResult {
    pub bucket_region: Option<String>,
    pub access_point_alias: Option<bool>,
}
