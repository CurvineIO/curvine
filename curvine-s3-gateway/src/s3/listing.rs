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

use super::dto::{CommonPrefix, ListObjectContent};
use super::handlers::is_staging_name;
use super::s3_api::ListObjectOption;
use crate::utils::s3_utils::file_status_to_list_object_content;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use curvine_model::FileStatus;
use serde::{Deserialize, Serialize};
use std::future::Future;

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

pub(super) async fn list_objects<F, Fut>(
    opt: &ListObjectOption,
    mut list_directory: F,
) -> Result<ListObjectPage, String>
where
    F: FnMut(String) -> Fut + Send,
    Fut: Future<Output = Result<Vec<FileStatus>, String>> + Send,
{
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
    let mut stack = vec![entries("", list_directory(String::new()).await?)];
    let mut last_entry: Option<String> = None;
    let mut last_group: Option<String> = None;
    while let Some(frame) = stack.last_mut() {
        let Some((key, st)) = frame.next() else {
            stack.pop();
            continue;
        };
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
            if let Some(group) = common_prefix(&key, prefix, delimiter) {
                if after.as_deref().is_some_and(|m| group <= m)
                    || last_group.as_deref() == Some(group)
                {
                    continue;
                }
            }
            let statuses = list_directory(key.clone()).await?;
            stack.push(entries(&key, statuses));
            continue;
        }
        if !key.starts_with(prefix) {
            continue;
        }
        let group = common_prefix(&key, prefix, delimiter);
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
#[path = "listing_tests.rs"]
mod tests;
