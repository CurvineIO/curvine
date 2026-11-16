// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0

//! Common S3 DTO types shared across multiple operations

use serde::Serialize;

/// S3 namespace constant
pub const S3_NAMESPACE: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// Default owner ID
pub const DEFAULT_OWNER_ID: &str = "curvine-owner";

/// S3 Object Owner information
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Owner {
    pub id: String,
    pub display_name: String,
}

impl Default for Owner {
    fn default() -> Self {
        Self {
            id: DEFAULT_OWNER_ID.to_string(),
            display_name: "curvine".to_string(),
        }
    }
}

/// Archive status for S3 objects
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ArchiveStatus {
    #[serde(rename = "ARCHIVE_ACCESS")]
    ArchiveAccess,
    #[serde(rename = "DEEP_ARCHIVE_ACCESS")]
    DeepArchiveAccess,
}

impl std::str::FromStr for ArchiveStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ARCHIVE_ACCESS" => Ok(ArchiveStatus::ArchiveAccess),
            "DEEP_ARCHIVE_ACCESS" => Ok(ArchiveStatus::DeepArchiveAccess),
            _ => Err(format!("Invalid ArchiveStatus: {}", s)),
        }
    }
}

impl std::fmt::Display for ArchiveStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArchiveStatus::ArchiveAccess => write!(f, "ARCHIVE_ACCESS"),
            ArchiveStatus::DeepArchiveAccess => write!(f, "DEEP_ARCHIVE_ACCESS"),
        }
    }
}

/// Checksum algorithm types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChecksumAlgorithm {
    Crc32,
    Crc32c,
    Sha1,
    Sha256,
    Crc64nvme,
}

impl std::str::FromStr for ChecksumAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "CRC32" => Ok(Self::Crc32),
            "CRC32C" => Ok(Self::Crc32c),
            "SHA1" => Ok(Self::Sha1),
            "SHA256" => Ok(Self::Sha256),
            "CRC64NVME" => Ok(Self::Crc64nvme),
            _ => Err(format!("Invalid checksum algorithm: {}", s)),
        }
    }
}

/// Request payer type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestPayer {
    Requester,
}

impl std::str::FromStr for RequestPayer {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "requester" => Ok(Self::Requester),
            _ => Err(format!("Invalid RequestPayer: {}", s)),
        }
    }
}

/// Object lock mode
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectLockMode {
    Governance,
    Compliance,
}

impl std::str::FromStr for ObjectLockMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "GOVERNANCE" => Ok(Self::Governance),
            "COMPLIANCE" => Ok(Self::Compliance),
            _ => Err(format!("Invalid ObjectLockMode: {}", s)),
        }
    }
}

/// Object lock legal hold status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectLockLegalHoldStatus {
    On,
    Off,
}

impl std::str::FromStr for ObjectLockLegalHoldStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ON" => Ok(Self::On),
            "OFF" => Ok(Self::Off),
            _ => Err(format!("Invalid ObjectLockLegalHoldStatus: {}", s)),
        }
    }
}

/// Common prefix for hierarchical listing
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CommonPrefix {
    pub prefix: String,
}

/// DateTime type alias
pub type DateTime = chrono::DateTime<chrono::Utc>;
