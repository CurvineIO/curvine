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

// Include the version constants generated at build time
include!(concat!(env!("OUT_DIR"), "/version.rs"));

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

/// Initial Curvine component protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

/// Lowest Curvine component protocol version accepted by this build.
pub const MIN_PROTOCOL_VERSION: u32 = 1;

/// Structured version metadata emitted by every Curvine component.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentVersion {
    pub component: String,
    pub release_version: String,
    pub git_commit: String,
    pub git_tag: String,
    pub git_branch: String,
    pub protocol_version: u32,
    pub min_protocol_version: u32,
    pub capabilities: Vec<String>,
}

impl ComponentVersion {
    pub fn new(component: impl Into<String>) -> Self {
        Self {
            component: component.into(),
            release_version: PKG_VERSION.to_string(),
            git_commit: GIT_VERSION.to_string(),
            git_tag: GIT_TAG.to_string(),
            git_branch: GIT_BRANCH.to_string(),
            protocol_version: PROTOCOL_VERSION,
            min_protocol_version: MIN_PROTOCOL_VERSION,
            capabilities: Vec::new(),
        }
    }

    /// Override the capabilities advertised by this component. Capabilities
    /// are feature-level negotiation tokens (e.g. `"short-circuit"`,
    /// `"batch-write"`); a feature is enabled only when both peers declare it.
    pub fn with_capabilities(mut self, capabilities: Vec<String>) -> Self {
        self.capabilities = capabilities;
        self
    }

    pub fn to_json_pretty(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }

    pub fn display_version(&self) -> String {
        let mut source = String::new();
        if !self.git_tag.is_empty() && self.git_tag != "unknown" {
            source = format!(", tag: {}", self.git_tag);
        } else if !self.git_branch.is_empty()
            && self.git_branch != "unknown"
            && self.git_branch != "HEAD"
        {
            source = format!(", branch: {}", self.git_branch);
        }

        format!(
            "{} (commit: {}{})",
            self.release_version, self.git_commit, source
        )
    }
}

pub fn component_version(component: impl Into<String>) -> ComponentVersion {
    ComponentVersion::new(component)
}

pub fn component_version_json(component: impl Into<String>) -> serde_json::Result<String> {
    component_version(component).to_json_pretty()
}

/// Error returned when a version string cannot be parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionParseError {
    input: String,
    reason: String,
}

impl VersionParseError {
    fn new(input: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            input: input.into(),
            reason: reason.into(),
        }
    }
}

impl fmt::Display for VersionParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid version {:?}: {}", self.input, self.reason)
    }
}

impl std::error::Error for VersionParseError {}

/// One pre-release identifier. Numeric identifiers compare by value and sort
/// before alphanumeric identifiers, following SemVer precedence rules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreRelease {
    Numeric(u64),
    Alpha(String),
}

impl PreRelease {
    fn as_str(&self) -> String {
        match self {
            PreRelease::Numeric(n) => n.to_string(),
            PreRelease::Alpha(s) => s.clone(),
        }
    }
}

impl Ord for PreRelease {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (PreRelease::Numeric(a), PreRelease::Numeric(b)) => a.cmp(b),
            (PreRelease::Numeric(_), PreRelease::Alpha(_)) => Ordering::Less,
            (PreRelease::Alpha(_), PreRelease::Numeric(_)) => Ordering::Greater,
            (PreRelease::Alpha(a), PreRelease::Alpha(b)) => a.cmp(b),
        }
    }
}

impl PartialOrd for PreRelease {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A SemVer-style `X.Y.Z[-pre][+build]` release version.
///
/// - `release_version` is the Cargo/package version of a component.
/// - Git tags may carry a leading `v` (`v0.4.0-alpha`), which is accepted.
/// - Build metadata is preserved for display but ignored for ordering and
///   equality, matching SemVer precedence.
///
/// This is the comparison unit used by the compatibility checker: a release
/// version with a pre-release suffix sorts before the same version without
/// one, and numeric pre-release identifiers compare by value.
#[derive(Debug, Clone, Eq, Serialize, Deserialize)]
pub struct ReleaseVersion {
    major: u64,
    minor: u64,
    patch: u64,
    pre: Vec<PreRelease>,
    build: Option<String>,
}

impl PartialEq for ReleaseVersion {
    /// Equality ignores build metadata (`1.0.0+1 == 1.0.0+2`), matching how
    /// version comparisons are used for compatibility checks.
    fn eq(&self, other: &Self) -> bool {
        self.major == other.major
            && self.minor == other.minor
            && self.patch == other.patch
            && self.pre == other.pre
    }
}

impl ReleaseVersion {
    pub fn parse(input: &str) -> Result<Self, VersionParseError> {
        parse_release_version(input)
    }

    /// Build a `ReleaseVersion` directly from its components.
    pub fn new(
        major: u64,
        minor: u64,
        patch: u64,
        pre: Vec<PreRelease>,
        build: Option<String>,
    ) -> Self {
        Self {
            major,
            minor,
            patch,
            pre,
            build,
        }
    }

    /// Parse the raw output of `git describe --tags --always --dirty` into a
    /// SemVer-style version.
    ///
    /// Supported forms:
    /// - `v0.4.0` -> `0.4.0`
    /// - `v0.4.0-alpha` -> `0.4.0-alpha`
    /// - `v0.4.0-alpha-121-g26b5dc6b` -> `0.4.0-alpha.121+g26b5dc6b`
    /// - `v0.4.1-19-g0ea9fdd6` -> `0.4.1+19.g0ea9fdd6` (compares equal to `0.4.1`)
    /// - `v0.4.0-alpha-121-g26b5dc6b-dirty` -> `0.4.0-alpha.121+g26b5dc6b.dirty`
    /// - `dev-g26b5dc6b` -> `0.0.0-dev+g26b5dc6b`
    pub fn from_git_describe(input: &str) -> Result<Self, VersionParseError> {
        parse_git_describe(input)
    }

    pub fn major(&self) -> u64 {
        self.major
    }

    pub fn minor(&self) -> u64 {
        self.minor
    }

    pub fn patch(&self) -> u64 {
        self.patch
    }

    pub fn pre(&self) -> &[PreRelease] {
        &self.pre
    }

    pub fn build(&self) -> Option<&str> {
        self.build.as_deref()
    }
}

impl fmt::Display for ReleaseVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
        if !self.pre.is_empty() {
            write!(f, "-")?;
            for (i, id) in self.pre.iter().enumerate() {
                if i > 0 {
                    write!(f, ".")?;
                }
                write!(f, "{}", id.as_str())?;
            }
        }
        if let Some(build) = &self.build {
            write!(f, "+{}", build)?;
        }
        Ok(())
    }
}

impl FromStr for ReleaseVersion {
    type Err = VersionParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Ord for ReleaseVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        self.major
            .cmp(&other.major)
            .then_with(|| self.minor.cmp(&other.minor))
            .then_with(|| self.patch.cmp(&other.patch))
            .then_with(|| compare_pre(&self.pre, &other.pre))
    }
}

impl PartialOrd for ReleaseVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Compare two pre-release identifier lists following SemVer precedence:
/// an empty list (a release) sorts after any pre-release, numeric
/// identifiers compare by value, and a shorter list sorts before a longer
/// one when all shared identifiers are equal. Build metadata is ignored.
fn compare_pre(a: &[PreRelease], b: &[PreRelease]) -> Ordering {
    match (a.is_empty(), b.is_empty()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => {
            for (x, y) in a.iter().zip(b.iter()) {
                match x.cmp(y) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
            a.len().cmp(&b.len())
        }
    }
}

fn parse_release_version(input: &str) -> Result<ReleaseVersion, VersionParseError> {
    let raw = input.trim();
    if raw.is_empty() {
        return Err(VersionParseError::new(input, "empty version"));
    }
    // Accept an optional leading `v`/`V` used by git tags (e.g. `v0.4.0-alpha`).
    let s = raw
        .strip_prefix('v')
        .or_else(|| raw.strip_prefix('V'))
        .unwrap_or(raw);

    let (core, rest) = split_core_version(s).ok_or_else(|| {
        VersionParseError::new(input, "expected <major>.<minor>.<patch> at the start")
    })?;
    let (major, minor, patch) = core;

    let (pre, build) = if rest.is_empty() {
        (Vec::new(), None)
    } else if let Some(rest) = rest.strip_prefix('-') {
        // Split the pre-release from the build metadata at the first '+'.
        let (pre_str, build) = match rest.split_once('+') {
            Some((pre, build)) => (pre, Some(build.to_string())),
            None => (rest, None),
        };
        if pre_str.is_empty() {
            return Err(VersionParseError::new(
                input,
                "missing pre-release after '-'",
            ));
        }
        let pre = parse_pre_identifiers(pre_str, input)?;
        (pre, build)
    } else if let Some(build) = rest.strip_prefix('+') {
        if build.is_empty() {
            return Err(VersionParseError::new(
                input,
                "missing build metadata after '+'",
            ));
        }
        (Vec::new(), Some(build.to_string()))
    } else {
        return Err(VersionParseError::new(
            input,
            "expected '-' or '+' after <major>.<minor>.<patch>",
        ));
    };

    Ok(ReleaseVersion {
        major,
        minor,
        patch,
        pre,
        build,
    })
}

/// Split `<major>.<minor>.<patch>` off the start of `s`. The patch component
/// ends at the first non-digit (which may be `-`, `+`, or end of string);
/// the remainder is returned for further parsing.
fn split_core_version(s: &str) -> Option<((u64, u64, u64), &str)> {
    let end_of_number = |start: usize| -> Option<usize> {
        let mut idx = start;
        while idx < s.len() && s.as_bytes()[idx].is_ascii_digit() {
            idx += 1;
        }
        (idx > start).then_some(idx)
    };

    let e0 = end_of_number(0)?;
    if s.as_bytes().get(e0) != Some(&b'.') {
        return None;
    }
    let e1 = end_of_number(e0 + 1)?;
    if s.as_bytes().get(e1) != Some(&b'.') {
        return None;
    }
    let e2 = end_of_number(e1 + 1)?;

    let major = s[..e0].parse().ok()?;
    let minor = s[e0 + 1..e1].parse().ok()?;
    let patch = s[e1 + 1..e2].parse().ok()?;
    Some(((major, minor, patch), &s[e2..]))
}

fn parse_pre_identifiers(pre_str: &str, input: &str) -> Result<Vec<PreRelease>, VersionParseError> {
    if pre_str.is_empty() {
        return Ok(Vec::new());
    }
    let mut ids = Vec::new();
    for id in pre_str.split('.') {
        if id.is_empty() {
            return Err(VersionParseError::new(
                input,
                "empty pre-release identifier",
            ));
        }
        if id.chars().all(|c| c.is_ascii_digit()) {
            ids.push(PreRelease::Numeric(id.parse().map_err(|_| {
                VersionParseError::new(input, "pre-release identifier too large")
            })?));
        } else if id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            ids.push(PreRelease::Alpha(id.to_string()));
        } else {
            return Err(VersionParseError::new(
                input,
                format!("invalid pre-release identifier {:?}", id),
            ));
        }
    }
    Ok(ids)
}

fn parse_git_describe(input: &str) -> Result<ReleaseVersion, VersionParseError> {
    let raw = input.trim();
    if raw.is_empty() {
        return Err(VersionParseError::new(input, "empty git describe output"));
    }
    let s = raw
        .strip_prefix('v')
        .or_else(|| raw.strip_prefix('V'))
        .unwrap_or(raw);

    // `git describe --dirty` appends `-dirty` when the working tree has
    // uncommitted changes. The suffix can follow any form: a tag, a
    // tag+commit-count string, or a bare short commit when no tags are
    // available (`26b5dc6b-dirty`), so strip it before any shape detection.
    let (s, dirty) = match s.strip_suffix("-dirty") {
        Some(stripped) => (stripped, true),
        None => (s, false),
    };

    // No tags: `dev-g<commit>` or a bare short commit (git describe --always).
    if let Some(commit) = s.strip_prefix("dev-") {
        if commit.is_empty() {
            return Err(VersionParseError::new(input, "missing commit after `dev-`"));
        }
        return Ok(ReleaseVersion::new(
            0,
            0,
            0,
            vec![PreRelease::Alpha("dev".into())],
            dirty_build(Some(commit.to_string()), dirty),
        ));
    }
    if is_short_commit(s) {
        return Ok(ReleaseVersion::new(
            0,
            0,
            0,
            vec![PreRelease::Alpha("dev".into())],
            dirty_build(Some(format!("g{}", s)), dirty),
        ));
    }

    let (core, rest) = split_core_version(s).ok_or_else(|| {
        VersionParseError::new(input, "expected <major>.<minor>.<patch> at the start")
    })?;
    let (major, minor, patch) = core;

    if rest.is_empty() {
        return Ok(ReleaseVersion::new(
            major,
            minor,
            patch,
            Vec::new(),
            dirty_build(None, dirty),
        ));
    }

    // Strip the leading '-' when the tag carried a pre-release
    // (e.g. `-alpha-121-g26b5dc6b`).
    let body = rest.strip_prefix('-').unwrap_or(rest);

    // The trailing `-g<commit>` (git describe commit suffix) becomes build
    // metadata. Only treat it as a commit suffix when the remainder is a
    // short hash AND a numeric commit count precedes it: `git describe` only
    // emits `-g<hash>` in the form `<tag>-<n>-g<hash>`. Without the count the
    // `-g...` suffix is part of the tag itself (e.g. `v1.0.0-rc-gabc1234`),
    // so fall through to exact-tag parsing.
    if let Some(idx) = body.rfind("-g") {
        if idx + 2 < body.len() && is_short_commit(&body[idx + 2..]) {
            let commit = &body[idx + 2..];
            let pre_str = &body[..idx];

            // pre_str = "<tag-pre>-<count>" for commits after a pre-release
            // tag, or just "<count>" for commits after a release tag.
            let (tag_pre, count) = match pre_str.rsplit_once('-') {
                Some((pre, count))
                    if !count.is_empty() && count.chars().all(|c| c.is_ascii_digit()) =>
                {
                    (pre.to_string(), count.parse::<u64>().ok())
                }
                _ if !pre_str.is_empty() && pre_str.chars().all(|c| c.is_ascii_digit()) => {
                    (String::new(), pre_str.parse::<u64>().ok())
                }
                _ => (pre_str.to_string(), None),
            };

            // Without a commit count this is not a git-describe suffix.
            if let Some(count) = count {
                if tag_pre.is_empty() {
                    // Commits after a release tag (`v0.4.0-19-g0ea9fdd6`):
                    // keep the distance in build metadata
                    // (`0.4.0+19.g0ea9fdd6`) so the build compares equal to
                    // the tag. Putting it in the pre-release would sort the
                    // build *before* the release and break `min_*_version`
                    // checks.
                    let build = format!("{}.g{}", count, commit);
                    return Ok(ReleaseVersion::new(
                        major,
                        minor,
                        patch,
                        Vec::new(),
                        dirty_build(Some(build), dirty),
                    ));
                }

                // Commits after a pre-release tag (`v0.4.0-alpha-121-g…`):
                // append the count as the last numeric pre-release
                // identifier, which sorts after the tag's own pre-release
                // and still before the release (`0.4.0-alpha.121`).
                let mut pre = parse_pre_identifiers(&tag_pre, input)?;
                pre.push(PreRelease::Numeric(count));

                let build = format!("g{}", commit);
                return Ok(ReleaseVersion::new(
                    major,
                    minor,
                    patch,
                    pre,
                    dirty_build(Some(build), dirty),
                ));
            }
        }
    }

    // Exact tag (possibly with a pre-release), no commit suffix.
    let pre = if body.is_empty() {
        Vec::new()
    } else {
        parse_pre_identifiers(body, input)?
    };
    Ok(ReleaseVersion::new(
        major,
        minor,
        patch,
        pre,
        dirty_build(None, dirty),
    ))
}

/// Append `dirty` to the build metadata when the working tree had uncommitted
/// changes at build time.
fn dirty_build(build: Option<String>, dirty: bool) -> Option<String> {
    match (build, dirty) {
        (Some(b), true) => Some(format!("{}.dirty", b)),
        (Some(b), false) => Some(b),
        (None, true) => Some("dirty".to_string()),
        (None, false) => None,
    }
}

/// `git describe --always` short commit: 7-40 hex chars.
fn is_short_commit(s: &str) -> bool {
    let len = s.len();
    (7..=40).contains(&len) && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// Raw `git describe --tags --always --dirty` output captured at build time.
pub fn git_describe() -> String {
    GIT_DESCRIBE.to_string()
}

/// The build-time `git describe` output converted to a SemVer-style version.
///
/// Falls back to `0.0.0-dev` when the captured value cannot be parsed (e.g.
/// when built outside a git checkout).
pub fn git_describe_version() -> ReleaseVersion {
    parse_git_describe(GIT_DESCRIBE).unwrap_or_else(|_| {
        ReleaseVersion::new(0, 0, 0, vec![PreRelease::Alpha("dev".into())], None)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_constants_are_available() {
        assert!(!PKG_VERSION.is_empty());
        assert!(!GIT_VERSION.is_empty());
        assert!(!VERSION.is_empty());
    }

    #[test]
    fn build_version_override_is_honored_when_set() {
        if let Ok(build_version) = std::env::var("BUILD_VERSION") {
            if build_version.is_empty() {
                return;
            }
            assert_eq!(PKG_VERSION, build_version);
            assert_eq!(VERSION, build_version);
        }
    }

    #[test]
    fn component_version_json_uses_stable_schema() {
        let json = component_version_json("cli").unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(value["component"], "cli");
        assert_eq!(value["release_version"], PKG_VERSION);
        assert_eq!(value["git_commit"], GIT_VERSION);
        assert_eq!(value["git_tag"], GIT_TAG);
        assert_eq!(value["git_branch"], GIT_BRANCH);
        assert_eq!(value["protocol_version"], PROTOCOL_VERSION);
        assert_eq!(value["min_protocol_version"], MIN_PROTOCOL_VERSION);
        assert!(value["capabilities"].as_array().unwrap().is_empty());
    }

    #[test]
    fn component_version_json_round_trips() {
        let version = component_version("master")
            .with_capabilities(vec!["short-circuit".to_string(), "batch-write".to_string()]);
        let json = version.to_json_pretty().unwrap();
        let decoded: ComponentVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, version);
        assert_eq!(decoded.capabilities.len(), 2);
    }

    #[test]
    fn component_version_display_prefers_tag_over_branch() {
        let version = ComponentVersion {
            component: "master".to_string(),
            release_version: "0.2.0".to_string(),
            git_commit: "abcdef1".to_string(),
            git_tag: "v0.2.0".to_string(),
            git_branch: "main".to_string(),
            protocol_version: PROTOCOL_VERSION,
            min_protocol_version: MIN_PROTOCOL_VERSION,
            capabilities: Vec::new(),
        };

        assert_eq!(
            version.display_version(),
            "0.2.0 (commit: abcdef1, tag: v0.2.0)"
        );
    }

    #[test]
    fn release_version_parses_standard_forms() {
        let cases = [
            ("0.2.0", "0.2.0", None),
            ("1.2.3", "1.2.3", None),
            ("1.2.3-alpha", "1.2.3-alpha", None),
            ("1.2.3-alpha.1", "1.2.3-alpha.1", None),
            ("1.2.3-rc.1", "1.2.3-rc.1", None),
            ("1.2.3+build.5", "1.2.3+build.5", Some("build.5")),
            ("1.2.3-rc.1+build.5", "1.2.3-rc.1+build.5", Some("build.5")),
            ("v0.4.0-alpha", "0.4.0-alpha", None),
            ("V1.2.3", "1.2.3", None),
        ];
        for (input, expected, build) in cases {
            let parsed = ReleaseVersion::parse(input)
                .unwrap_or_else(|e| panic!("failed to parse {:?}: {}", input, e));
            assert_eq!(parsed.to_string(), expected, "input: {}", input);
            assert_eq!(parsed.build(), build, "input: {}", input);
        }
    }

    #[test]
    fn release_version_rejects_invalid_forms() {
        for input in [
            "", "1.2", "1.2.3.4", "abc", "1..2.3", "1.2.a", "1.2.3-", "1.2.3+",
        ] {
            assert!(
                ReleaseVersion::parse(input).is_err(),
                "expected {:?} to be rejected",
                input
            );
        }
    }

    #[test]
    fn release_version_precedence_follows_semver() {
        let ordered = [
            "1.0.0-1",
            "1.0.0-alpha",
            "1.0.0-alpha.1",
            "1.0.0-alpha.2",
            "1.0.0-alpha.10",
            "1.0.0-beta",
            "1.0.0-rc.1",
            "1.0.0",
        ];
        for pair in ordered.windows(2) {
            let a = ReleaseVersion::parse(pair[0]).unwrap();
            let b = ReleaseVersion::parse(pair[1]).unwrap();
            assert!(a < b, "{} should be < {}", pair[0], pair[1]);
            assert!(b > a, "{} should be > {}", pair[1], pair[0]);
        }
        // Release beats any pre-release of the same core.
        assert!(
            ReleaseVersion::parse("1.0.0").unwrap() > ReleaseVersion::parse("1.0.0-rc.1").unwrap()
        );
        // Build metadata does not affect precedence or equality.
        assert_eq!(
            ReleaseVersion::parse("1.0.0+1").unwrap(),
            ReleaseVersion::parse("1.0.0+2").unwrap()
        );
        // Numeric pre-release identifiers compare by value.
        assert!(
            ReleaseVersion::parse("1.0.0-alpha.2").unwrap()
                < ReleaseVersion::parse("1.0.0-alpha.10").unwrap()
        );
        // Numeric sorts before alphanumeric.
        assert!(
            ReleaseVersion::parse("1.0.0-1").unwrap()
                < ReleaseVersion::parse("1.0.0-alpha").unwrap()
        );
    }

    #[test]
    fn release_version_parses_via_from_str() {
        let version: ReleaseVersion = "0.2.0".parse().unwrap();
        assert_eq!(version.major(), 0);
        assert_eq!(version.minor(), 2);
        assert_eq!(version.patch(), 0);
    }

    #[test]
    fn git_describe_exact_tag() {
        let v = ReleaseVersion::from_git_describe("v0.4.0").unwrap();
        assert_eq!(v.to_string(), "0.4.0");
        assert_eq!(v.build(), None);
    }

    #[test]
    fn git_describe_exact_tag_with_pre_release() {
        let v = ReleaseVersion::from_git_describe("v0.4.0-alpha").unwrap();
        assert_eq!(v.to_string(), "0.4.0-alpha");
    }

    #[test]
    fn git_describe_tag_after_commit() {
        let v = ReleaseVersion::from_git_describe("v0.4.0-alpha-121-g26b5dc6b").unwrap();
        assert_eq!(v.to_string(), "0.4.0-alpha.121+g26b5dc6b");
        assert_eq!(v.build(), Some("g26b5dc6b"));
    }

    #[test]
    fn git_describe_commit_after_release_tag() {
        let v = ReleaseVersion::from_git_describe("v0.4.1-19-g0ea9fdd6").unwrap();
        assert_eq!(v.to_string(), "0.4.1+19.g0ea9fdd6");
    }

    #[test]
    fn git_describe_commit_after_release_tag_compares_equal_to_tag() {
        // A build 19 commits after v0.4.0 must not sort before the release;
        // the distance lives in build metadata, which is ignored for
        // precedence, so min_*_version = 0.4.0 still accepts it.
        let after = ReleaseVersion::from_git_describe("v0.4.0-19-g0ea9fdd6").unwrap();
        let release = ReleaseVersion::from_git_describe("v0.4.0").unwrap();
        assert!((after >= release), "{} should not be < {}", after, release);
        assert!(after >= release, "{} should be >= {}", after, release);
        assert_eq!(after, release);
        assert_eq!(after, ReleaseVersion::parse("0.4.0+19.g0ea9fdd6").unwrap());
        // A build after the pre-release tag still sorts before the release.
        let alpha = ReleaseVersion::from_git_describe("v0.4.0-alpha-121-g26b5dc6b").unwrap();
        assert!(alpha < release, "{} should be < {}", alpha, release);
    }

    #[test]
    fn git_describe_dirty_suffixes() {
        let v = ReleaseVersion::from_git_describe("v0.4.0-alpha-121-g26b5dc6b-dirty").unwrap();
        assert_eq!(v.to_string(), "0.4.0-alpha.121+g26b5dc6b.dirty");
        let exact = ReleaseVersion::from_git_describe("v0.2.0-dirty").unwrap();
        assert_eq!(exact.to_string(), "0.2.0+dirty");
        let release_after = ReleaseVersion::from_git_describe("v0.4.1-19-g0ea9fdd6-dirty").unwrap();
        assert_eq!(release_after.to_string(), "0.4.1+19.g0ea9fdd6.dirty");
    }

    #[test]
    fn git_describe_pre_release_containing_g_suffix_is_not_a_commit() {
        // An exact tag whose pre-release contains `-g` must not be treated as
        // a commit suffix (`v1.0.0-rc-ga` -> `1.0.0-rc-ga`, not `1.0.0-rc+ga`).
        let v = ReleaseVersion::from_git_describe("v1.0.0-rc-ga").unwrap();
        assert_eq!(v.to_string(), "1.0.0-rc-ga");
        assert_eq!(v.build(), None);
        // Even a 7-hex-char suffix is not a commit unless a numeric count
        // precedes it: `git describe` only emits `-g<hash>` as
        // `<tag>-<n>-g<hash>`.
        let hex = ReleaseVersion::from_git_describe("v1.0.0-rc-gabc1234").unwrap();
        assert_eq!(hex.to_string(), "1.0.0-rc-gabc1234");
        assert_eq!(hex.build(), None);
        // A real commit suffix is still recognized after such a pre-release.
        let with_commit = ReleaseVersion::from_git_describe("v1.0.0-rc-ga-5-gabc1234").unwrap();
        assert_eq!(with_commit.to_string(), "1.0.0-rc-ga.5+gabc1234");
    }

    #[test]
    fn git_describe_without_tags() {
        let v = ReleaseVersion::from_git_describe("dev-g26b5dc6b").unwrap();
        assert_eq!(v.to_string(), "0.0.0-dev+g26b5dc6b");
        let bare = ReleaseVersion::from_git_describe("26b5dc6b").unwrap();
        assert_eq!(bare.to_string(), "0.0.0-dev+g26b5dc6b");
    }

    #[test]
    fn git_describe_bare_commit_with_dirty_suffix() {
        // `git describe --always --dirty` without tags returns a bare short
        // commit with a `-dirty` suffix; it must still resolve to a version
        // instead of silently falling back to 0.0.0-dev.
        let v = ReleaseVersion::from_git_describe("26b5dc6b-dirty").unwrap();
        assert_eq!(v.to_string(), "0.0.0-dev+g26b5dc6b.dirty");
        let dev = ReleaseVersion::from_git_describe("dev-g26b5dc6b-dirty").unwrap();
        assert_eq!(dev.to_string(), "0.0.0-dev+g26b5dc6b.dirty");
    }

    #[test]
    fn git_describe_versions_compare_like_semver() {
        let on_tag = ReleaseVersion::from_git_describe("v0.4.0-alpha").unwrap();
        let after = ReleaseVersion::from_git_describe("v0.4.0-alpha-121-g26b5dc6b").unwrap();
        let later = ReleaseVersion::from_git_describe("v0.4.0-alpha-130-gdeadbeef").unwrap();
        assert!(on_tag < after);
        assert!(after < later);
        // A dirty build on top of a release still sorts as that release.
        assert_eq!(
            ReleaseVersion::from_git_describe("v0.4.0-dirty").unwrap(),
            ReleaseVersion::parse("0.4.0+dirty").unwrap()
        );
    }

    #[test]
    fn git_describe_captured_at_build_time_is_parseable() {
        let raw = git_describe();
        // Outside a git checkout (or when git is unavailable) build.rs falls
        // back to the literal "unknown"; the runtime helper must then fall
        // back to 0.0.0-dev instead of failing.
        if raw.is_empty() || raw == "unknown" {
            assert_eq!(git_describe_version().to_string(), "0.0.0-dev");
            return;
        }
        // Built inside a git checkout: the value must be parseable (a tagged
        // form, a bare commit, or any of those with a `-dirty` suffix).
        assert!(parse_git_describe(&raw).is_ok(), "unparseable: {}", raw);
    }

    #[test]
    fn release_version_json_round_trips() {
        let v = ReleaseVersion::parse("1.2.3-rc.1+build.5").unwrap();
        let json = serde_json::to_string(&v).unwrap();
        let decoded: ReleaseVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, v);
        assert_eq!(decoded.to_string(), "1.2.3-rc.1+build.5");
    }
}
