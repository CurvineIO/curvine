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

/// Identifies the kind of filesystem / storage for a path (e.g. cv, s3, oss).
#[repr(i8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FsKind {
    Cv,
    S3,
    Oss,
    OssHdfs,
    Hdfs,
    Gcs,
    Azblob,
    Cos,
    Unknown,
}

impl FsKind {
    // Scheme string constants for each filesystem kind.
    pub const SCHEME_CV: &'static str = "cv";
    pub const SCHEME_S3: &'static str = "s3";
    pub const SCHEME_S3A: &'static str = "s3a";
    pub const SCHEME_OSS: &'static str = "oss";
    pub const SCHEME_HDFS: &'static str = "hdfs";
    pub const SCHEME_GCS: &'static str = "gcs";
    pub const SCHEME_GS: &'static str = "gs";
    pub const SCHEME_AZBLOB: &'static str = "azblob";
    pub const SCHEME_COS: &'static str = "cos";
    pub const SCHEME_UNKNOWN: &'static str = "unknown";

    /// Returns the filesystem kind for the given scheme string (e.g. from `Path::scheme()`).
    pub fn from_scheme(scheme: &str) -> Self {
        let lower = scheme.to_lowercase();
        match lower.as_str() {
            Self::SCHEME_CV => Self::Cv,
            Self::SCHEME_S3 | Self::SCHEME_S3A => Self::S3,
            Self::SCHEME_OSS => Self::Oss,
            Self::SCHEME_HDFS => Self::Hdfs,
            Self::SCHEME_GCS | Self::SCHEME_GS => Self::Gcs,
            Self::SCHEME_AZBLOB => Self::Azblob,
            Self::SCHEME_COS => Self::Cos,
            _ => Self::Unknown,
        }
    }

    /// Returns the kind when the path may have no scheme (treated as Cv).
    pub fn from_scheme_opt(scheme: Option<&str>) -> Self {
        match scheme {
            None => Self::Cv,
            Some(s) => Self::from_scheme(s),
        }
    }

    /// Returns true if this is Curvine native / local.
    pub fn is_cv(self) -> bool {
        self == Self::Cv
    }

    /// Returns the canonical scheme string (e.g. for display).
    pub fn as_scheme_str(self) -> &'static str {
        match self {
            Self::Cv => Self::SCHEME_CV,
            Self::S3 => Self::SCHEME_S3,
            Self::Oss => Self::SCHEME_OSS,
            Self::OssHdfs => Self::SCHEME_OSS,
            Self::Hdfs => Self::SCHEME_HDFS,
            Self::Gcs => Self::SCHEME_GCS,
            Self::Azblob => Self::SCHEME_AZBLOB,
            Self::Cos => Self::SCHEME_COS,
            Self::Unknown => Self::SCHEME_UNKNOWN,
        }
    }

    pub fn support_rename(&self) -> bool {
        matches!(self, Self::Cv | Self::Hdfs | Self::OssHdfs)
    }
}
