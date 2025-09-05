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

//! Constants used throughout the utils module
//! This module centralizes string constants for better performance and maintainability

pub const EMPTY_PAYLOAD_HASH: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

// === S3 Serde Rename Constants ===
pub const SERDE_ACCEPT_RANGES: &str = "AcceptRanges";
pub const SERDE_ARCHIVE_STATUS: &str = "ArchiveStatus";
pub const SERDE_BUCKET_KEY_ENABLED: &str = "BucketKeyEnabled";
pub const SERDE_CACHE_CONTROL: &str = "CacheControl";
pub const SERDE_CHECKSUM_CRC32: &str = "ChecksumCRC32";
pub const SERDE_CHECKSUM_CRC32C: &str = "ChecksumCRC32C";
pub const SERDE_CHECKSUM_CRC64: &str = "ChecksumCRC64";
pub const SERDE_CHECKSUM_SHA1: &str = "ChecksumSHA1";
pub const SERDE_CHECKSUM_SHA256: &str = "ChecksumSHA256";
pub const SERDE_CONTENT_DISPOSITION: &str = "ContentDisposition";
pub const SERDE_CONTENT_ENCODING: &str = "ContentEncoding";
pub const SERDE_CONTENT_LANGUAGE: &str = "ContentLanguage";
pub const SERDE_CONTENT_LENGTH: &str = "ContentLength";
pub const SERDE_CONTENT_RANGE: &str = "ContentRange";
pub const SERDE_CONTENT_TYPE: &str = "ContentType";
pub const SERDE_DELETE_MARKER: &str = "DeleteMarker";
pub const SERDE_ETAG: &str = "ETag";
pub const SERDE_EXPIRATION: &str = "Expiration";
pub const SERDE_EXPIRES: &str = "Expires";
pub const SERDE_LAST_MODIFIED: &str = "LastModified";
pub const SERDE_METADATA: &str = "Metadata";
pub const SERDE_MISSING_META: &str = "MissingMeta";
pub const SERDE_OBJECT_LOCK_LEGAL_HOLD_STATUS: &str = "ObjectLockLegalHoldStatus";
pub const SERDE_OBJECT_LOCK_MODE: &str = "ObjectLockMode";
pub const SERDE_OBJECT_LOCK_RETAIN_UNTIL_DATE: &str = "ObjectLockRetainUntilDate";
pub const SERDE_PARTS_COUNT: &str = "PartsCount";
pub const SERDE_REPLICATION_STATUS: &str = "ReplicationStatus";
pub const SERDE_REQUEST_CHARGED: &str = "RequestCharged";
pub const SERDE_RESTORE: &str = "Restore";
pub const SERDE_SERVER_SIDE_ENCRYPTION: &str = "ServerSideEncryption";
pub const SERDE_SSE_CUSTOMER_ALGORITHM: &str = "SSECustomerAlgorithm";
pub const SERDE_SSE_CUSTOMER_KEY_MD5: &str = "SSECustomerKeyMD5";
pub const SERDE_SSE_KMS_KEY_ID: &str = "SSEKMSKeyId";
pub const SERDE_STORAGE_CLASS: &str = "StorageClass";
pub const SERDE_TAG_COUNT: &str = "TagCount";
pub const SERDE_VERSION_ID: &str = "VersionId";
pub const SERDE_WEBSITE_REDIRECT_LOCATION: &str = "WebsiteRedirectLocation";

// === Default Values ===
pub const DEFAULT_OWNER_ID: &str = "ffffffffffffffff";
