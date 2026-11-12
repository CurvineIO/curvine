// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use crate::s3::s3_api::{handle_get_list_object, ListObjectHandler};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

#[derive(Default)]
struct Tree {
    directories: BTreeMap<String, BTreeMap<String, bool>>,
    calls: Mutex<Vec<String>>,
}

impl Tree {
    fn new(keys: &[&str]) -> Self {
        let mut tree = Self::default();
        tree.directories.insert(String::new(), BTreeMap::new());
        for key in keys {
            let parts: Vec<_> = key.trim_end_matches('/').split('/').collect();
            let mut directory = String::new();
            for (i, part) in parts.iter().enumerate() {
                let is_dir = i + 1 < parts.len() || key.ends_with('/');
                tree.directories
                    .entry(directory.clone())
                    .or_default()
                    .insert((*part).into(), is_dir);
                if is_dir {
                    directory.push_str(part);
                    directory.push('/');
                    tree.directories.entry(directory.clone()).or_default();
                }
            }
        }
        tree
    }

    fn read(&self, directory: String) -> std::future::Ready<Result<Vec<FileStatus>, String>> {
        self.calls.lock().unwrap().push(directory.clone());
        std::future::ready(
            self.directories
                .get(&directory)
                .ok_or_else(|| "injected directory read error".to_string())
                .map(|entries| {
                    entries
                        .iter()
                        .rev()
                        .map(|(name, is_dir)| FileStatus::with_name(1, name.clone(), *is_dir))
                        .collect()
                }),
        )
    }
}

impl ListObjectHandler for Tree {
    fn handle(
        &self,
        opt: &ListObjectOption,
        _bucket: &str,
    ) -> impl Future<Output = Result<ListObjectPage, String>> + Send {
        list_objects(opt, |directory| self.read(directory))
    }
}

fn options(prefix: &str, delimiter: &str, max_keys: i32) -> ListObjectOption {
    ListObjectOption {
        bucket: "test-bucket".into(),
        prefix: Some(prefix.into()),
        delimiter: Some(delimiter.into()),
        max_keys: Some(max_keys),
        ..Default::default()
    }
}

fn keys(page: &ListObjectPage) -> Vec<String> {
    let mut keys: Vec<_> = page
        .contents
        .iter()
        .map(|c| c.key.clone())
        .chain(page.common_prefixes.iter().map(|p| p.prefix.clone()))
        .collect();
    keys.sort();
    keys
}

#[tokio::test]
async fn recursive_prefixes_and_empty_directories() {
    let tree = Tree::new(&[
        "a-",
        "a/file",
        "a/deep/file",
        "a0",
        "empty/",
        ".visible",
        "中文/空格+%.txt",
    ]);
    assert_eq!(
        keys(&tree.handle(&options("", "", 1000), "").await.unwrap()),
        [
            ".visible",
            "a-",
            "a/deep/file",
            "a/file",
            "a0",
            "中文/空格+%.txt"
        ]
    );
    for prefix in ["a/d", "a/deep/", "a/deep/f"] {
        assert_eq!(
            keys(&tree.handle(&options(prefix, "", 1000), "").await.unwrap()),
            ["a/deep/file"]
        );
    }
    for prefix in ["missing/", "a/missing/", "empty/"] {
        assert!(keys(&tree.handle(&options(prefix, "", 1000), "").await.unwrap()).is_empty());
    }
}

#[tokio::test]
async fn pagination_matches_flat_object_space() {
    let objects = [
        "a-",
        "a/deep/1",
        "a/deep/2",
        "a/file",
        "a0",
        "b/x:y",
        "b/x:z",
        "root",
        "中文/空格+%.txt",
    ];
    let tree = Tree::new(&objects);
    for prefix in ["", "a", "a/", "a/d", "b/", "中文/", "missing/"] {
        for delimiter in ["", "/", ":", "deep"] {
            for start in ["", "a-", "a/", "a/deep/1", "z"] {
                let expected: Vec<_> = objects
                    .iter()
                    .filter(|k| k.starts_with(prefix))
                    .map(|key| {
                        // Independent projection of the flat object set.
                        if delimiter.is_empty() {
                            return key.to_string();
                        }
                        match key[prefix.len()..].find(delimiter) {
                            Some(i) => key[..prefix.len() + i + delimiter.len()].to_string(),
                            None => key.to_string(),
                        }
                    })
                    .filter(|key| key.as_str() > start)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                for size in [1, 2, 3, 1000] {
                    let mut opt = options(prefix, delimiter, size);
                    opt.start_after = Some(start.into());
                    let mut actual = Vec::new();
                    loop {
                        let page = tree.handle(&opt, "").await.unwrap();
                        assert!(keys(&page).len() <= size as usize);
                        actual.extend(keys(&page));
                        if !page.is_truncated {
                            assert!(page.next_continuation_token.is_none());
                            break;
                        }
                        assert!(page.next_continuation_token.is_some());
                        assert_ne!(opt.continuation_token, page.next_continuation_token);
                        opt.continuation_token = page.next_continuation_token;
                        assert!(actual.len() <= expected.len(), "paging must make progress");
                    }
                    assert_eq!(
                        actual, expected,
                        "prefix={prefix:?}, delimiter={delimiter:?}, start={start:?}, size={size}"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn zero_limit_validation_and_cursor_context() {
    let tree = Tree::new(&["a/1", "a/2", "b/1"]);
    let page = tree.handle(&options("", "", 0), "").await.unwrap();
    assert!(keys(&page).is_empty());
    assert!(!page.is_truncated);
    assert!(tree.calls.lock().unwrap().is_empty());
    assert!(tree.handle(&options("", "", -1), "").await.is_err());
    let original = options("", "", 1);
    let first = tree.handle(&original, "").await.unwrap();
    for kind in 0..5 {
        let mut opt = original.clone();
        opt.continuation_token = first.next_continuation_token.clone();
        match kind {
            0 => opt.bucket = "different-bucket".into(),
            1 => opt.prefix = Some("a/".into()),
            2 => opt.delimiter = Some("/".into()),
            3 => opt.continuation_token = Some("not-a-token".into()),
            _ => opt.continuation_token = Some(URL_SAFE_NO_PAD.encode(b"{}")),
        }
        assert!(tree
            .handle(&opt, "")
            .await
            .unwrap_err()
            .starts_with("InvalidArgument:"));
    }
    let mut next = original;
    next.continuation_token = first.next_continuation_token;
    next.start_after = Some("z".into()); // The continuation token takes precedence.
    assert_eq!(keys(&tree.handle(&next, "").await.unwrap()), ["a/2"]);
    next.encoding_type = Some("invalid".into());
    assert!(tree.handle(&next, "").await.is_err());
}

#[tokio::test]
async fn skips_staging_and_stops_before_unneeded_subtrees() {
    let tree = Tree::new(&[
        "a/1",
        "a/2",
        "a/deep/3",
        "z/1",
        ".curvine-s3-root/hidden",
        "a/.curvine-s3-part",
        ".visible",
    ]);
    let page = tree.handle(&options("a/", "", 1), "").await.unwrap();
    assert_eq!(keys(&page), ["a/1"]);
    assert_eq!(*tree.calls.lock().unwrap(), ["", "a/"]);
    tree.calls.lock().unwrap().clear();
    let page = tree.handle(&options("", "/", 1), "").await.unwrap();
    assert_eq!(keys(&page), [".visible"]);
    assert_eq!(*tree.calls.lock().unwrap(), ["", "a/"]);
    tree.calls.lock().unwrap().clear();
    let mut opt = options("", "/", 1);
    opt.start_after = Some("a/".into());
    assert_eq!(keys(&tree.handle(&opt, "").await.unwrap()), ["z/"]);
    assert_eq!(*tree.calls.lock().unwrap(), ["", "z/"]);
    assert_eq!(
        keys(&tree.handle(&options("", "", 1000), "").await.unwrap()),
        [".visible", "a/1", "a/2", "a/deep/3", "z/1"]
    );
}

#[tokio::test]
async fn directory_errors_are_not_empty_listings() {
    let mut tree = Tree::new(&["a/1"]);
    tree.directories.remove("a/");
    assert_eq!(
        tree.handle(&options("a/", "", 1000), "").await.unwrap_err(),
        "injected directory read error"
    );
}

#[tokio::test]
async fn max_keys_is_capped_at_one_thousand() {
    let owned: Vec<_> = (0..1002).map(|i| format!("key-{i:04}")).collect();
    let refs: Vec<_> = owned.iter().map(String::as_str).collect();
    let tree = Tree::new(&refs);
    let mut opt = options("", "", 5000);
    let first = tree.handle(&opt, "").await.unwrap();
    assert_eq!(first.contents.len(), 1000);
    assert!(first.is_truncated);
    opt.continuation_token = first.next_continuation_token;
    let second = tree.handle(&opt, "").await.unwrap();
    assert_eq!(keys(&second), ["key-1000", "key-1001"]);
    assert!(!second.is_truncated);
}

async fn http(tree: &Tree, query: &str) -> (axum::http::StatusCode, String) {
    let request = axum::http::Request::builder()
        .uri(format!("/test-bucket?{query}"))
        .body(axum::body::Body::empty())
        .unwrap();
    let mut response = crate::http::axum::Response::default();
    handle_get_list_object(
        crate::http::axum::Request::from(request),
        &mut response,
        tree,
    )
    .await;
    let response: axum::response::Response = response.into();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test]
async fn http_serializes_groups_encoding_and_rejects_bad_parameters() {
    let tree = Tree::new(&["a/中文 空格+%.txt", "a/deep/file"]);
    let (status, xml) = http(
        &tree,
        "list-type=2&prefix=a%2F&delimiter=%2F&encoding-type=url&max-keys=2",
    )
    .await;
    assert_eq!(status, 200);
    assert!(xml.contains("<KeyCount>2</KeyCount>"), "{xml}");
    assert!(
        xml.contains("<CommonPrefixes><Prefix>a%2Fdeep%2F</Prefix></CommonPrefixes>"),
        "{xml}"
    );
    assert!(
        xml.contains("<Key>a%2F%E4%B8%AD%E6%96%87%20%E7%A9%BA%E6%A0%BC%2B%25.txt</Key>"),
        "{xml}"
    );
    assert!(xml.contains("<EncodingType>url</EncodingType>"), "{xml}");
    assert!(xml.contains("<Prefix>a%2F</Prefix>"), "{xml}");
    assert!(xml.contains("<Delimiter>%2F</Delimiter>"), "{xml}");
    assert!(xml.contains("<IsTruncated>false</IsTruncated>"), "{xml}");
    let (_, xml) = http(&tree, "list-type=2&prefix=a%2F&max-keys=0").await;
    assert!(xml.contains("<KeyCount>0</KeyCount>"), "{xml}");
    assert!(xml.contains("<MaxKeys>0</MaxKeys>"), "{xml}");
    for query in [
        "max-keys=-1",
        "max-keys=bad",
        "encoding-type=invalid",
        "continuation-token=bad",
    ] {
        assert_eq!(http(&tree, query).await.0, 400, "{query}");
    }
}
