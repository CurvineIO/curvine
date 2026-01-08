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
use orpc::{try_err, CommonResult};
use regex::Regex;
use std::fmt::{Display, Formatter};
use std::path::MAIN_SEPARATOR;

static SLASHES: Lazy<Regex> = Lazy::new(|| Regex::new(r"/+").unwrap());

#[derive(Debug, Clone)]
pub struct Path {
    uri: Uri,
    full_path: String,
}

impl Path {
    pub const SEPARATOR: &'static str = "/";

    pub fn new<T: AsRef<str>>(s: T) -> CommonResult<Self> {
        Self::from_str(s)
    }

    pub fn from_str<T: AsRef<str>>(s: T) -> CommonResult<Self> {
        let pre_uri = try_err!(Uri::try_from(s.as_ref()));
        let path = Self::normalize_path(pre_uri.path());

        let mut builder = Uri::builder();
        if let Some(v) = pre_uri.scheme() {
            builder = builder.scheme(v.as_str());
        };
        if let Some(v) = pre_uri.authority() {
            builder = builder.authority(v.as_str());
        };

        let uri = builder.path_and_query(path).build()?;
        let full_path = uri.to_string();

        Ok(Self { uri, full_path })
    }

    pub fn path(&self) -> &str {
        self.uri.path()
    }

    pub fn full_path(&self) -> &str {
        &self.full_path
    }

    pub fn clone_path(&self) -> String {
        self.path().to_string()
    }

    pub fn clone_uri(&self) -> String {
        self.full_path.clone()
    }

    pub fn authority_path(&self) -> &str {
        let full_path = self.full_path();

        let auth_path = if let Some(scheme_end) = full_path.find("://") {
            let start_index = scheme_end + 2;
            &full_path[start_index..]
        } else {
            full_path
        };
        auth_path.trim_end_matches("/")
    }

    pub fn name(&self) -> &str {
        match self.path().rfind(Self::SEPARATOR) {
            None => "",
            Some(v) => &self.path()[v + 1..],
        }
    }

    pub fn is_root(&self) -> bool {
        self.path() == Self::SEPARATOR
    }

    pub fn is_cv(&self) -> bool {
        matches!(self.scheme(), None | Some("cv"))
    }

    pub fn authority(&self) -> Option<&str> {
        match self.uri.authority() {
            Some(authority) => Some(authority.as_str()),
            None => None,
        }
    }

    pub fn scheme(&self) -> Option<&str> {
        self.uri.scheme_str()
    }

    /// scheme://authority/path
    pub fn normalize_uri(&self) -> Option<String> {
        let scheme = self.scheme()?;
        let authority = self.authority()?;
        let path = Self::normalize_path(self.path());
        if path == "/" {
            Some(format!("{}://{}", scheme, authority))
        } else {
            Some(format!("{}://{}{}", scheme, authority, path))
        }
    }

    pub fn encode(&self) -> String {
        self.path().to_string()
    }

    pub fn encode_uri(&self) -> String {
        self.full_path().to_string()
    }

    pub fn uri(&self) -> &Uri {
        &self.uri
    }

    pub fn to_local_path(&self) -> Option<String> {
        let path = self.path();
        if self.scheme().is_some() {
            let authority = self.authority().unwrap_or("");
            return Some(format!("/{}{}", authority, path));
        }

        if let Some(stripped) = path.strip_prefix('/') {
            return Some(stripped.to_string());
        }

        Some(path.to_string())
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
            Some(v) => format!("{}://{}", v, self.authority().unwrap_or("")),
            None => String::from("/"),
        };

        for part in parts {
            if !current_path.ends_with('/') {
                current_path.push('/');
            };
            current_path.push_str(part);
            result.push(current_path.clone());
        }

        result
    }

    pub fn get_components<T: AsRef<str>>(path: T) -> Vec<String> {
        let path = Self::normalize_path(path.as_ref());
        if path.as_str() == "/" {
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
}
