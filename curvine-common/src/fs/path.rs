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

#![allow(clippy::should_implement_trait)]

use hyper::Uri;
use once_cell::sync::Lazy;
use orpc::CommonResult;
use regex::Regex;
use std::fmt::{Display, Formatter};
use std::path::MAIN_SEPARATOR;

static SLASHES: Lazy<Regex> = Lazy::new(|| Regex::new(r"/+").unwrap());

#[derive(Debug, Clone)]
pub struct Path {
    full_path: String,
    scheme: Option<String>,
    authority: Option<String>,
    path: String,
}

impl Path {
    pub const SEPARATOR: &'static str = "/";
    const SCHEME_DELIMITER: &'static str = "://";
    const SCHEME_DELIMITER_LEN: usize = 3; // Length of "://"

    pub fn new<T: AsRef<str>>(s: T) -> CommonResult<Self> {
        Self::from_str(s)
    }

    pub fn from_str<T: AsRef<str>>(s: T) -> CommonResult<Self> {
        let input = s.as_ref();

        if let Some(scheme_end) = input.find(Self::SCHEME_DELIMITER) {
            // Parse scheme: "s3://bucket/path///" -> scheme="s3"
            let scheme_str = input[..scheme_end].to_string();
            let after_scheme = &input[scheme_end + Self::SCHEME_DELIMITER_LEN..];

            // Parse authority and path
            let (authority_str, path) = if let Some(path_start) = after_scheme.find(Self::SEPARATOR)
            {
                // "bucket/path///" -> authority="bucket", path="/path"
                let authority_str = after_scheme[..path_start].to_string();
                let path = Self::normalize_path(&after_scheme[path_start..]);
                (authority_str, path)
            } else {
                // "bucket" -> authority="bucket", path="/"
                (after_scheme.to_string(), Self::SEPARATOR.to_string())
            };

            // Build full_path: "s3://bucket/path"
            let full_path = format!(
                "{}{}{}{}",
                scheme_str,
                Self::SCHEME_DELIMITER,
                authority_str,
                path
            );

            Ok(Self {
                full_path,
                scheme: Some(scheme_str),
                authority: Some(authority_str),
                path,
            })
        } else {
            // Local path: "/a/b///" -> normalize to "/a/b"
            let path = Self::normalize_path(input);
            Ok(Self {
                full_path: path.clone(),
                scheme: None,
                authority: None,
                path,
            })
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn full_path(&self) -> &str {
        &self.full_path
    }

    pub fn clone_uri(&self) -> String {
        self.full_path.clone()
    }

    pub fn authority_path(&self) -> String {
        // Return "authority/path" or just "path" for local files
        if let Some(authority) = &self.authority {
            if self.path == Self::SEPARATOR {
                authority.clone()
            } else {
                format!("{}{}", authority, &self.path)
            }
        } else {
            self.path.trim_end_matches(Self::SEPARATOR).to_string()
        }
    }

    pub fn name(&self) -> &str {
        match self.path.rfind(Self::SEPARATOR) {
            None => "",
            Some(v) => &self.path[v + 1..],
        }
    }

    pub fn is_root(&self) -> bool {
        self.path == Self::SEPARATOR
    }

    pub fn is_cv(&self) -> bool {
        matches!(self.scheme(), None | Some("cv"))
    }

    pub fn authority(&self) -> Option<&str> {
        self.authority.as_deref()
    }

    pub fn scheme(&self) -> Option<&str> {
        self.scheme.as_deref()
    }

    /// scheme://authority/path
    pub fn normalize_uri(&self) -> Option<String> {
        let scheme = self.scheme.as_ref()?;
        let authority = self.authority.as_ref()?;
        // self.path is already normalized
        if self.path == Self::SEPARATOR {
            Some(format!("{}{}{}", scheme, Self::SCHEME_DELIMITER, authority))
        } else {
            Some(format!(
                "{}{}{}{}",
                scheme,
                Self::SCHEME_DELIMITER,
                authority,
                &self.path
            ))
        }
    }

    pub fn encode(&self) -> String {
        self.path().to_string()
    }

    pub fn encode_uri(&self) -> String {
        self.full_path().to_string()
    }

    /// Convert to hyper::Uri (for S3/HTTP APIs)
    /// May fail if path contains characters invalid in URI
    pub fn as_uri(&self) -> CommonResult<Uri> {
        Uri::try_from(self.full_path.as_str())
            .map_err(|e| format!("Cannot convert path to URI: {}", e).into())
    }

    fn normalize_path(path: &str) -> String {
        let p = SLASHES.replace_all(path, Self::SEPARATOR);
        let min_len = if MAIN_SEPARATOR == '\\' { 4 } else { 1 };
        if p.len() > min_len && p.ends_with(Self::SEPARATOR) {
            p.trim_end_matches(Self::SEPARATOR).to_owned()
        } else {
            p.to_string()
        }
    }

    pub fn parent(&self) -> CommonResult<Option<Path>> {
        if self.is_root() {
            return Ok(None);
        }

        let path = self.full_path();
        match path.rfind(Self::SEPARATOR).map(|v| &path[..v]) {
            None => Ok(None),

            Some("") => Path::from_str(Self::SEPARATOR).map(Some),

            Some(v) => {
                let res = Path::from_str(v)?;
                Ok(Some(res))
            }
        }
    }

    pub fn get_possible_mounts(&self) -> Vec<String> {
        let parts: Vec<&str> = self.path().split(Self::SEPARATOR).collect();
        let mut result = Vec::new();

        if parts.is_empty() {
            return result;
        }

        let mut current_path = match self.scheme() {
            Some(v) => format!(
                "{}{}{}",
                v,
                Self::SCHEME_DELIMITER,
                self.authority().unwrap_or("")
            ),
            None => Self::SEPARATOR.to_string(),
        };

        for part in parts {
            if !current_path.ends_with(Self::SEPARATOR) {
                current_path.push_str(Self::SEPARATOR);
            };
            current_path.push_str(part);
            result.push(current_path.clone());
        }

        result
    }

    pub fn get_components<T: AsRef<str>>(path: T) -> Vec<String> {
        let path = Self::normalize_path(path.as_ref());
        if path.as_str() == Self::SEPARATOR {
            vec![]
        } else {
            path.split(Self::SEPARATOR).map(|x| x.to_string()).collect()
        }
    }

    pub fn has_prefix<T: AsRef<str>>(path: T, prefix: &str) -> bool {
        let path_components = Self::get_components(path);
        let prefix_components = Self::get_components(prefix);

        if path_components.len() < prefix_components.len() {
            return false;
        }

        for (i, component) in prefix_components.iter().enumerate() {
            if component != &path_components[i] {
                return false;
            }
        }

        true
    }

    pub fn display_path(&self) -> &str {
        if self.is_cv() {
            self.path()
        } else {
            self.full_path()
        }
    }

    pub fn clone_display_path(&self) -> String {
        self.display_path().to_string()
    }
}

impl Display for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.full_path())
    }
}

impl PartialEq for Path {
    fn eq(&self, other: &Self) -> bool {
        self.full_path().eq(other.full_path())
    }
}

impl Eq for Path {}

impl From<String> for Path {
    fn from(s: String) -> Self {
        Path::new(s).unwrap()
    }
}

impl From<Path> for String {
    fn from(path: Path) -> Self {
        path.full_path
    }
}

impl From<&str> for Path {
    fn from(s: &str) -> Self {
        Path::new(s).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::fs::Path;
    use orpc::CommonResult;

    #[test]
    fn normalize_path() -> CommonResult<()> {
        let p = "//a";
        let p1 = Path::from_str(p)?;
        println!("p1 = {}", p1);
        assert_eq!(p1.full_path(), "/a");

        let p = "/a/b/c///";
        let p1 = Path::from_str(p)?;
        println!("p1 = {}", p1);
        assert_eq!(p1.full_path(), "/a/b/c");

        let p = "/x/y/1.log";
        let p1 = Path::from_str(p)?;
        println!("p1 = {}", p1);
        assert_eq!(p1.full_path(), "/x/y/1.log");

        let p = "s3://bucket/y/1.log/";
        let p1 = Path::from_str(p)?;
        println!("p1 = {}", p1);
        assert_eq!(p1.full_path(), "s3://bucket/y/1.log");
        assert_eq!(p1.path(), "/y/1.log");
        Ok(())
    }

    #[test]
    fn parent_path() -> CommonResult<()> {
        let p1 = Path::from_str("/a/b/c")?;
        let parent = p1.parent()?;
        let parent_path = parent.as_ref().map(|x| x.full_path());
        assert_eq!(parent_path, Some("/a/b"));
        println!("{:?}", parent);

        let p1 = Path::from_str("s3://bucket/a/b/c")?;
        let parent = p1.parent()?;
        let parent_path = parent.as_ref().map(|x| x.full_path());
        assert_eq!(parent_path, Some("s3://bucket/a/b"));
        println!("{:?}", parent);
        Ok(())
    }

    #[test]
    fn get_possible_mounts() -> CommonResult<()> {
        let p1 = Path::from_str("/a/b/c")?;
        let mnts = p1.get_possible_mounts();
        println!("{:?}", mnts);
        assert_eq!(mnts, Vec::from(["/", "/a", "/a/b", "/a/b/c"]));

        let p1 = Path::from_str("s3://bucket/a/b")?;
        let mnts = p1.get_possible_mounts();
        println!("{:?}", mnts);
        assert_eq!(
            mnts,
            Vec::from(["s3://bucket/", "s3://bucket/a", "s3://bucket/a/b"])
        );
        Ok(())
    }

    #[test]
    fn is_root() -> CommonResult<()> {
        let p1 = Path::from_str("/")?;
        assert!(p1.is_root());

        let p1 = Path::from_str("s3://bucket/")?;
        assert!(p1.is_root());

        Ok(())
    }

    #[test]
    fn test_special_characters_posix() -> CommonResult<()> {
        // Python package path - square brackets
        let p = "/usr/lib/python3.9/site-packages/package[extra].dist-info";
        let p1 = Path::from_str(p)?;
        assert_eq!(p1.path(), p);
        assert_eq!(p1.full_path(), p);
        assert!(p1.scheme().is_none());
        assert_eq!(p1.name(), "package[extra].dist-info");

        // File name with spaces
        let p = "/home/user/My Documents/file with spaces.txt";
        let p1 = Path::from_str(p)?;
        assert_eq!(p1.name(), "file with spaces.txt");
        assert_eq!(p1.path(), p);

        // Curly braces
        let p = "/config/template-{id}.json";
        let p1 = Path::from_str(p)?;
        assert!(p1.full_path().contains("{id}"));
        assert_eq!(p1.name(), "template-{id}.json");

        // Pipe character
        let p = "/data/file|backup.txt";
        let p1 = Path::from_str(p)?;
        assert_eq!(p1.name(), "file|backup.txt");

        Ok(())
    }

    #[test]
    fn test_s3_paths_unchanged() -> CommonResult<()> {
        // Ensure S3 path behavior is unchanged
        let p = "s3://my-bucket/path/to/file.txt";
        let p1 = Path::from_str(p)?;

        assert_eq!(p1.scheme(), Some("s3"));
        assert_eq!(p1.authority(), Some("my-bucket"));
        assert_eq!(p1.path(), "/path/to/file.txt");
        assert_eq!(p1.full_path(), "s3://my-bucket/path/to/file.txt");

        // URI conversion should succeed
        let uri = p1.as_uri()?;
        assert_eq!(uri.scheme_str(), Some("s3"));

        Ok(())
    }

    #[test]
    fn test_special_chars_path_operations() -> CommonResult<()> {
        // Test that paths with special chars work correctly
        let p = "/path/file[1].txt";
        let p1 = Path::from_str(p)?;

        // from_str should succeed (this is the key improvement!)
        assert_eq!(p1.full_path(), "/path/file[1].txt");
        assert_eq!(p1.path(), "/path/file[1].txt");
        assert_eq!(p1.name(), "file[1].txt");

        // Test path with braces
        let p2 = Path::from_str("/config/{template}.json")?;
        assert_eq!(p2.full_path(), "/config/{template}.json");

        // Test parent path works
        let parent = p1.parent()?.unwrap();
        assert_eq!(parent.full_path(), "/path");

        Ok(())
    }

    #[test]
    fn test_parent_with_special_chars() -> CommonResult<()> {
        let p = "/path/dir[1]/file.txt";
        let p1 = Path::from_str(p)?;
        let parent = p1.parent()?.unwrap();

        assert_eq!(parent.full_path(), "/path/dir[1]");
        assert_eq!(parent.name(), "dir[1]");

        // Test nested parent
        let grandparent = parent.parent()?.unwrap();
        assert_eq!(grandparent.full_path(), "/path");

        Ok(())
    }

    #[test]
    fn test_oss_paths() -> CommonResult<()> {
        let p = "oss://bucket-name/path/file.txt";
        let p1 = Path::from_str(p)?;

        assert_eq!(p1.scheme(), Some("oss"));
        assert_eq!(p1.authority(), Some("bucket-name"));
        assert_eq!(p1.path(), "/path/file.txt");

        Ok(())
    }

    #[test]
    fn test_edge_cases_root_paths() -> CommonResult<()> {
        // Single slash
        let p1 = Path::from_str("/")?;
        assert_eq!(p1.path(), "/");
        assert_eq!(p1.full_path(), "/");
        assert!(p1.is_root());
        assert_eq!(p1.name(), "");
        assert!(p1.parent()?.is_none());

        // Multiple slashes should normalize to single
        let p2 = Path::from_str("///")?;
        assert_eq!(p2.path(), "/");
        assert_eq!(p2.full_path(), "/");
        assert!(p2.is_root());

        // Scheme with root path
        let p3 = Path::from_str("s3://bucket")?;
        assert_eq!(p3.scheme(), Some("s3"));
        assert_eq!(p3.authority(), Some("bucket"));
        assert_eq!(p3.path(), "/");
        assert!(p3.is_root());
        assert_eq!(p3.full_path(), "s3://bucket/");

        Ok(())
    }

    #[test]
    fn test_edge_cases_single_level_paths() -> CommonResult<()> {
        // Single level local path
        let p1 = Path::from_str("/a")?;
        assert_eq!(p1.path(), "/a");
        assert_eq!(p1.name(), "a");
        assert!(!p1.is_root());

        let parent = p1.parent()?.unwrap();
        assert_eq!(parent.full_path(), "/");
        assert!(parent.is_root());

        // Single level with scheme
        let p2 = Path::from_str("s3://bucket/file")?;
        assert_eq!(p2.path(), "/file");
        assert_eq!(p2.name(), "file");

        let parent = p2.parent()?.unwrap();
        assert_eq!(parent.full_path(), "s3://bucket/");
        assert!(parent.is_root());

        Ok(())
    }

    #[test]
    fn test_edge_cases_multiple_slashes() -> CommonResult<()> {
        // Multiple slashes in path
        let p1 = Path::from_str("s3://bucket///a///b///c///")?;
        assert_eq!(p1.scheme(), Some("s3"));
        assert_eq!(p1.authority(), Some("bucket"));
        assert_eq!(p1.path(), "/a/b/c");
        assert_eq!(p1.full_path(), "s3://bucket/a/b/c");

        // Multiple slashes at start of local path
        let p2 = Path::from_str("///a/b")?;
        assert_eq!(p2.path(), "/a/b");
        assert_eq!(p2.full_path(), "/a/b");

        Ok(())
    }

    #[test]
    fn test_authority_path_edge_cases() -> CommonResult<()> {
        // Local path
        let p1 = Path::from_str("/a/b/c")?;
        assert_eq!(p1.authority_path(), "/a/b/c");

        // Local root
        let p2 = Path::from_str("/")?;
        assert_eq!(p2.authority_path(), "");

        // Scheme with root path
        let p3 = Path::from_str("s3://bucket/")?;
        assert_eq!(p3.authority_path(), "bucket");

        // Scheme with path
        let p4 = Path::from_str("s3://my-bucket/path/file.txt")?;
        assert_eq!(p4.authority_path(), "my-bucket/path/file.txt");

        Ok(())
    }

    #[test]
    fn test_normalize_uri() -> CommonResult<()> {
        // Normal path
        let p1 = Path::from_str("s3://bucket/a/b/c")?;
        assert_eq!(p1.normalize_uri(), Some("s3://bucket/a/b/c".to_string()));

        // Root path
        let p2 = Path::from_str("s3://bucket/")?;
        assert_eq!(p2.normalize_uri(), Some("s3://bucket".to_string()));

        // Local path returns None
        let p3 = Path::from_str("/a/b/c")?;
        assert_eq!(p3.normalize_uri(), None);

        // Different schemes
        let p4 = Path::from_str("oss://my-oss-bucket/data")?;
        assert_eq!(
            p4.normalize_uri(),
            Some("oss://my-oss-bucket/data".to_string())
        );

        Ok(())
    }

    #[test]
    fn test_name_edge_cases() -> CommonResult<()> {
        // Root has empty name
        let p1 = Path::from_str("/")?;
        assert_eq!(p1.name(), "");

        // Scheme root has empty name
        let p2 = Path::from_str("s3://bucket/")?;
        assert_eq!(p2.name(), "");

        // Normal file
        let p3 = Path::from_str("/path/to/file.txt")?;
        assert_eq!(p3.name(), "file.txt");

        // Directory-like path
        let p4 = Path::from_str("/path/to/dir")?;
        assert_eq!(p4.name(), "dir");

        // Hidden file
        let p5 = Path::from_str("/home/.bashrc")?;
        assert_eq!(p5.name(), ".bashrc");

        Ok(())
    }

    #[test]
    fn test_get_components_edge_cases() -> CommonResult<()> {
        // Root path
        let components = Path::get_components("/");
        assert_eq!(components, Vec::<String>::new());

        // Single component
        let components = Path::get_components("/a");
        assert_eq!(components, vec!["", "a"]);

        // Multiple components
        let components = Path::get_components("/a/b/c");
        assert_eq!(components, vec!["", "a", "b", "c"]);

        // Multiple slashes
        let components = Path::get_components("///a///b///");
        assert_eq!(components, vec!["", "a", "b"]);

        Ok(())
    }

    #[test]
    fn test_has_prefix_edge_cases() -> CommonResult<()> {
        // Exact match
        assert!(Path::has_prefix("/a/b/c", "/a/b/c"));

        // Valid prefix
        assert!(Path::has_prefix("/a/b/c", "/a/b"));
        assert!(Path::has_prefix("/a/b/c", "/a"));
        assert!(Path::has_prefix("/a/b/c", "/"));

        // Invalid prefix
        assert!(!Path::has_prefix("/a/b/c", "/a/b/c/d"));
        assert!(!Path::has_prefix("/a/b/c", "/x"));
        assert!(!Path::has_prefix("/a/b/c", "/a/x"));

        // Empty path
        assert!(Path::has_prefix("/", "/"));

        // Partial name should not match
        assert!(!Path::has_prefix("/abc", "/ab"));

        Ok(())
    }

    #[test]
    fn test_is_cv() -> CommonResult<()> {
        // Local path is cv
        let p1 = Path::from_str("/a/b/c")?;
        assert!(p1.is_cv());

        // cv scheme is cv
        let p2 = Path::from_str("cv://bucket/path")?;
        assert!(p2.is_cv());

        // s3 is not cv
        let p3 = Path::from_str("s3://bucket/path")?;
        assert!(!p3.is_cv());

        // oss is not cv
        let p4 = Path::from_str("oss://bucket/path")?;
        assert!(!p4.is_cv());

        Ok(())
    }

    #[test]
    fn test_display_and_equality() -> CommonResult<()> {
        let p1 = Path::from_str("s3://bucket/path")?;
        let p2 = Path::from_str("s3://bucket/path")?;
        let p3 = Path::from_str("s3://bucket/other")?;

        // Test equality
        assert_eq!(p1, p2);
        assert_ne!(p1, p3);

        // Test Display
        assert_eq!(format!("{}", p1), "s3://bucket/path");

        // Test display_path for cv
        let p4 = Path::from_str("/local/path")?;
        assert_eq!(p4.display_path(), "/local/path");

        let p5 = Path::from_str("cv://bucket/path")?;
        assert_eq!(p5.display_path(), "/path");

        // Test display_path for non-cv
        assert_eq!(p1.display_path(), "s3://bucket/path");

        Ok(())
    }

    #[test]
    fn test_different_schemes() -> CommonResult<()> {
        // http scheme
        let p1 = Path::from_str("http://example.com/api/v1")?;
        assert_eq!(p1.scheme(), Some("http"));
        assert_eq!(p1.authority(), Some("example.com"));
        assert_eq!(p1.path(), "/api/v1");

        // https scheme
        let p2 = Path::from_str("https://example.com/secure")?;
        assert_eq!(p2.scheme(), Some("https"));

        // file scheme
        let p3 = Path::from_str("file:///local/path")?;
        assert_eq!(p3.scheme(), Some("file"));
        assert_eq!(p3.authority(), Some(""));
        assert_eq!(p3.path(), "/local/path");

        Ok(())
    }

    #[test]
    fn test_path_conversions() -> CommonResult<()> {
        // From String
        let s = String::from("s3://bucket/path");
        let p1: Path = s.into();
        assert_eq!(p1.full_path(), "s3://bucket/path");

        // From &str
        let p2: Path = "s3://bucket/path".into();
        assert_eq!(p2.full_path(), "s3://bucket/path");

        // Into String
        let p3 = Path::from_str("s3://bucket/path")?;
        let s: String = p3.into();
        assert_eq!(s, "s3://bucket/path");

        Ok(())
    }

    #[test]
    fn test_clone_methods() -> CommonResult<()> {
        let p = Path::from_str("s3://bucket/path")?;

        // clone_uri
        assert_eq!(p.clone_uri(), "s3://bucket/path");

        // clone_display_path
        assert_eq!(p.clone_display_path(), "s3://bucket/path");

        // For cv path
        let p2 = Path::from_str("/local")?;
        assert_eq!(p2.clone_display_path(), "/local");

        Ok(())
    }
}
