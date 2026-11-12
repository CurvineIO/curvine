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

//! Ordered, demand-driven ListObjectsV2 traversal. The filesystem currently
//! returns an entire directory per call, so memory includes the remaining
//! entries of active ancestor directories, not just the response page. We do
//! not materialize the bucket recursively; traversal stops after one lookahead
//! entry, and irrelevant/already-consumed subtrees are skipped. Later requests
//! may relist directories; concurrent mutations do not provide snapshot paging.
//! Continuation tokens are opaque cursors bound to bucket/prefix/delimiter, not
//! authorization credentials. Restart in-flight listings after upgrading from
//! the previous raw-key tokens, which are no longer accepted.

use super::dto::{CommonPrefix, ListObjectContent};
use super::handlers::is_staging_name;
use super::s3_api::ListObjectOption;
use crate::utils::s3_utils::file_status_to_list_object_content;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use curvine_error::{FsError, FsResult};
use curvine_fs_api::ListStream;
use curvine_model::FileStatus;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::future::Future;

/// Directory reads used by the traversal; `dir` is relative to the bucket.
pub(super) trait DirSource: Send + Sync {
    /// Every child of `dir`.
    fn list(&self, dir: &str) -> impl Future<Output = FsResult<Vec<FileStatus>>> + Send;
    /// Children of `dir` as a lazily paged stream, so a reader can stop early.
    fn stream(&self, dir: &str) -> impl Future<Output = FsResult<ListStream>> + Send;
}

/// A directory removed after its parent was read: the listing is not a
/// snapshot, so it is skipped instead of failing the whole request.
fn vanished(e: &FsError) -> bool {
    matches!(e, FsError::FileNotFound(_))
}

/// Whether `dir`'s subtree holds an object. It descends depth-first and stops
/// at the first file, so a CommonPrefix costs a few bounded pages, not a read
/// of every object below it.
async fn has_object<S: DirSource>(src: &S, dir: &str) -> FsResult<bool> {
    let mut stack: Vec<(String, ListStream)> = Vec::new();
    let mut open = Some(dir.to_string());
    loop {
        if let Some(dir) = open.take() {
            match src.stream(&dir).await {
                Ok(stream) => stack.push((dir, stream)),
                Err(e) if vanished(&e) => {}
                Err(e) => return Err(e),
            }
        }
        let Some((dir, stream)) = stack.last_mut() else {
            return Ok(false);
        };
        match stream.next().await {
            Some(Ok(st)) if is_staging_name(&st.name) => {}
            Some(Ok(st)) if !st.is_dir => return Ok(true),
            Some(Ok(st)) => open = Some(format!("{dir}{}/", st.name)),
            Some(Err(e)) if !vanished(&e) => return Err(e),
            Some(Err(_)) | None => {
                stack.pop();
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct ListObjectPage {
    pub contents: Vec<ListObjectContent>,
    pub common_prefixes: Vec<CommonPrefix>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Cursor {
    version: u8,
    bucket: String,
    prefix: String,
    delimiter: String,
    after: String,
}

fn invalid_argument() -> String {
    "InvalidArgument: invalid listing parameters or continuation token".to_string()
}

fn delimiter(opt: &ListObjectOption) -> &str {
    opt.delimiter.as_deref().unwrap_or("")
}

fn marker(opt: &ListObjectOption) -> Result<Option<String>, String> {
    let Some(token) = &opt.continuation_token else {
        return Ok(opt.start_after.clone());
    };
    let decoded = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| invalid_argument())?;
    let cursor: Cursor = serde_json::from_slice(&decoded).map_err(|_| invalid_argument())?;
    if cursor.version != 1
        || cursor.bucket != opt.bucket
        || cursor.prefix != opt.prefix.as_deref().unwrap_or("")
        || cursor.delimiter != delimiter(opt)
        || !cursor.after.starts_with(&cursor.prefix)
    {
        return Err(invalid_argument());
    }
    Ok(Some(cursor.after))
}

fn token(opt: &ListObjectOption, after: String) -> String {
    URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&Cursor {
            version: 1,
            bucket: opt.bucket.clone(),
            prefix: opt.prefix.clone().unwrap_or_default(),
            delimiter: delimiter(opt).to_string(),
            after,
        })
        .expect("serializing a listing cursor cannot fail"),
    )
}

fn common_prefix<'a>(key: &'a str, prefix: &str, delimiter: &str) -> Option<&'a str> {
    if delimiter.is_empty() {
        return None;
    }
    let rest = key.strip_prefix(prefix)?;
    let end = prefix.len() + rest.find(delimiter)? + delimiter.len();
    Some(&key[..end])
}

fn entries(directory: &str, statuses: Vec<FileStatus>) -> std::vec::IntoIter<(String, FileStatus)> {
    let mut entries: Vec<_> = statuses
        .into_iter()
        .filter(|st| !is_staging_name(&st.name))
        .map(|st| {
            let mut key = format!("{directory}{}", st.name);
            if st.is_dir {
                key.push('/');
            }
            (key, st)
        })
        .collect();
    // Directories sort as their subtree's lower bound, not as bare names:
    // a- < a/file < a0, regardless of the underlying directory enumeration.
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries.into_iter()
}

pub(super) async fn list_objects<S: DirSource>(
    opt: &ListObjectOption,
    src: &S,
) -> Result<ListObjectPage, String> {
    if opt.max_keys.is_some_and(|max| max < 0)
        || opt.encoding_type.as_deref().is_some_and(|v| v != "url")
    {
        return Err(invalid_argument());
    }
    let after = marker(opt)?;
    let max_keys = opt.max_keys.unwrap_or(1000).min(1000) as usize;
    let mut page = ListObjectPage::default();
    if max_keys == 0 {
        return Ok(page);
    }
    let prefix = opt.prefix.as_deref().unwrap_or("");
    let delimiter = delimiter(opt);
    let root = src.list("").await.map_err(|e| e.to_string())?;
    let mut stack = vec![entries("", root)];
    let mut last_entry: Option<String> = None;
    let mut last_group: Option<String> = None;
    while let Some(frame) = stack.last_mut() {
        let Some((key, st)) = frame.next() else {
            stack.pop();
            continue;
        };
        let group = common_prefix(&key, prefix, delimiter);
        if st.is_dir {
            if !(prefix.starts_with(&key) || key.starts_with(prefix)) {
                continue;
            }
            // Every key in this subtree sorts before the marker unless the
            // marker itself is inside the subtree.
            if after
                .as_deref()
                .is_some_and(|m| key.as_str() <= m && !m.starts_with(&key))
            {
                continue;
            }
            let Some(g) = group else {
                match src.list(&key).await {
                    Ok(statuses) => stack.push(entries(&key, statuses)),
                    Err(e) if vanished(&e) => {}
                    Err(e) => return Err(e.to_string()),
                }
                continue;
            };
            // Every key below shares this group, so one object proves it.
            if after.as_deref().is_some_and(|m| g <= m)
                || last_group.as_deref() == Some(g)
                || !has_object(src, &key).await.map_err(|e| e.to_string())?
            {
                continue;
            }
        } else if !key.starts_with(prefix) {
            continue;
        }
        let entry = group.unwrap_or(&key);
        if after.as_deref().is_some_and(|m| entry <= m)
            || group.is_some_and(|g| last_group.as_deref() == Some(g))
        {
            continue;
        }
        if page.contents.len() + page.common_prefixes.len() == max_keys {
            page.is_truncated = true;
            page.next_continuation_token = last_entry.map(|last| token(opt, last));
            break;
        }
        last_entry = Some(entry.to_string());
        if let Some(group) = group {
            last_group = Some(group.to_string());
            page.common_prefixes.push(CommonPrefix {
                prefix: group.to_string(),
            });
        } else {
            page.contents
                .push(file_status_to_list_object_content(&st, key));
        }
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    use std::sync::Arc;

    /// Directory tree whose reads count every entry handed to the traversal.
    struct Tree(BTreeMap<String, Vec<FileStatus>>, Arc<AtomicUsize>);

    impl Tree {
        fn children(&self, dir: &str) -> FsResult<Vec<FileStatus>> {
            self.0
                .get(dir)
                .cloned()
                .ok_or_else(|| FsError::file_not_found(dir))
        }
    }

    impl DirSource for Tree {
        async fn list(&self, dir: &str) -> FsResult<Vec<FileStatus>> {
            let children = self.children(dir)?;
            self.1.fetch_add(children.len(), Relaxed);
            Ok(children)
        }

        async fn stream(&self, dir: &str) -> FsResult<ListStream> {
            let fetched = self.1.clone();
            let children = futures::stream::iter(self.children(dir)?);
            Ok(ListStream::new(children.map(move |st| {
                fetched.fetch_add(1, Relaxed);
                Ok(st)
            })))
        }
    }

    #[tokio::test]
    async fn groups_stop_at_one_object_and_vanished_dirs_are_skipped() {
        let status = |name: &str, dir| FileStatus::with_name(1, name.to_string(), dir);
        let mut tree = BTreeMap::new();
        let mut root: Vec<_> = ["a", "b", "gone"].map(|name| status(name, true)).into();
        root.push(status("root", false));
        tree.insert(String::new(), root);
        for dir in ["a/", "b/"] {
            let files = (0..100).map(|i| status(&i.to_string(), false)).collect();
            tree.insert(dir.to_string(), files);
        }
        let fetched = Arc::new(AtomicUsize::new(0));
        let tree = Tree(tree, fetched.clone());
        let mut opt = ListObjectOption {
            bucket: "bucket".into(),
            delimiter: Some("/".into()),
            ..Default::default()
        };
        let page = list_objects(&opt, &tree).await.unwrap();
        let groups: Vec<_> = page.common_prefixes.iter().map(|p| &p.prefix).collect();
        assert_eq!(groups, ["a/", "b/"]);
        assert_eq!(page.contents.len(), 1);
        // The four root entries plus one probed object per group, not 200.
        assert_eq!(fetched.load(Relaxed), 6);

        // `gone/` was listed by its parent but no longer exists.
        opt.delimiter = None;
        let page = list_objects(&opt, &tree).await.unwrap();
        assert_eq!(page.contents.len(), 201);
    }
}
