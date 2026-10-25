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

use crate::master::meta::inode::PATH_SEPARATOR;
use std::collections::HashMap;

pub const TRASH_ROOT_NAME: &str = ".Trash";

pub const TRASH_ROOT: &str = "/.Trash";

const TRASH_ROOT_PREFIX: &str = ".Trash/";

// Internal xattr keys. The NUL prefix keeps them out of client-visible
// xattr listings, matching the convention used for \0curvine.ctime.
pub const TRASH_XATTR_ORIGIN: &str = "\0curvine.trash.origin";

pub const TRASH_XATTR_DELETE_MS: &str = "\0curvine.trash.delete_ms";

// True for the trash root itself and every path below it.
pub fn is_trash_path(path: &str) -> bool {
    match path.strip_prefix(TRASH_ROOT) {
        Some(rest) => rest.is_empty() || rest.starts_with(PATH_SEPARATOR),
        None => false,
    }
}

// The trash namespace is reserved even while the recycle bin is disabled, so
// enabling it later cannot collide with user data.
pub fn is_reserved_path(path: &str) -> bool {
    let path = path.trim_start_matches(PATH_SEPARATOR);
    path == TRASH_ROOT_NAME || path.starts_with(TRASH_ROOT_PREFIX)
}

pub fn trash_entry_name(delete_ms: i64, inode_id: i64) -> String {
    format!("{}-{}", delete_ms, inode_id)
}

pub fn trash_entry_path(entry_name: &str) -> String {
    format!("{}{}{}", TRASH_ROOT, PATH_SEPARATOR, entry_name)
}

// Trash entries are always direct children of the trash root.
pub fn is_trash_entry_path(path: &str) -> bool {
    let path = path.trim_start_matches(PATH_SEPARATOR);
    match path.strip_prefix(TRASH_ROOT_PREFIX) {
        Some(name) => !name.is_empty() && !name.contains(PATH_SEPARATOR),
        None => false,
    }
}

// Entry names are "{delete_ms}-{inode_id}[-{seq}]". Listing sorts by the
// delete-time prefix so pagination can order names without loading inodes.
pub fn parse_entry_delete_ms(name: &str) -> Option<i64> {
    name.split_once('-')?.0.parse().ok()
}

// Resume key for ListTrash: the (delete_ms, path) sort key of the last entry
// returned to the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrashCursor {
    pub delete_ms: i64,
    pub path: String,
}

pub fn decode_origin(x_attr: &HashMap<String, Vec<u8>>) -> Option<String> {
    x_attr
        .get(TRASH_XATTR_ORIGIN)
        .and_then(|value| std::str::from_utf8(value).ok())
        .map(str::to_owned)
}

pub fn decode_delete_ms(x_attr: &HashMap<String, Vec<u8>>) -> Option<i64> {
    let bytes: [u8; 8] = x_attr
        .get(TRASH_XATTR_DELETE_MS)?
        .as_slice()
        .try_into()
        .ok()?;
    Some(i64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifies_trash_paths() {
        assert!(is_trash_path(TRASH_ROOT));
        assert!(is_trash_path("/.Trash/entry"));
        assert!(is_trash_path("/.Trash/a/b"));

        assert!(!is_trash_path("/"));
        assert!(!is_trash_path("/.Trashy"));
        assert!(!is_trash_path("/a/.Trash"));
    }

    #[test]
    fn identifies_reserved_paths() {
        assert!(is_reserved_path(TRASH_ROOT));
        assert!(is_reserved_path("/.Trash/"));
        assert!(is_reserved_path("/.Trash/a/b"));
        assert!(is_reserved_path(".Trash"));

        assert!(!is_reserved_path("/"));
        assert!(!is_reserved_path("/.Trashx"));
        assert!(!is_reserved_path("/a/.Trash"));
        assert!(!is_reserved_path("/foo"));
    }

    #[test]
    fn identifies_trash_entry_paths() {
        assert!(is_trash_entry_path("/.Trash/1700000000000-42"));
        assert!(is_trash_entry_path(".Trash/1700000000000-42"));

        assert!(!is_trash_entry_path("/.Trash"));
        assert!(!is_trash_entry_path("/.Trash/"));
        assert!(!is_trash_entry_path("/.Trash/a/b"));
        assert!(!is_trash_entry_path("/plain.log"));
        assert!(!is_trash_entry_path("/a/.Trash/entry"));
    }

    #[test]
    fn decodes_trash_metadata() {
        let x_attr = HashMap::from([
            (TRASH_XATTR_ORIGIN.to_string(), b"/data/a.log".to_vec()),
            (
                TRASH_XATTR_DELETE_MS.to_string(),
                1_700_000_000_000_i64.to_le_bytes().to_vec(),
            ),
        ]);

        assert_eq!(decode_origin(&x_attr).as_deref(), Some("/data/a.log"));
        assert_eq!(decode_delete_ms(&x_attr), Some(1_700_000_000_000));
        assert_eq!(decode_origin(&HashMap::new()), None);
        assert_eq!(decode_delete_ms(&HashMap::new()), None);
        assert_eq!(
            decode_delete_ms(&HashMap::from([(
                TRASH_XATTR_DELETE_MS.to_string(),
                vec![1, 2, 3],
            )])),
            None
        );
        assert_eq!(
            decode_origin(&HashMap::from([(
                TRASH_XATTR_ORIGIN.to_string(),
                vec![0xff, 0xfe],
            )])),
            None
        );
    }

    #[test]
    fn entry_names_include_delete_time_and_inode_id() {
        assert_eq!(trash_entry_name(1_700_000_000_000, 42), "1700000000000-42");
        assert_ne!(
            trash_entry_name(1_700_000_000_000, 1),
            trash_entry_name(1_700_000_000_000, 2)
        );
        assert_eq!(
            trash_entry_path(&trash_entry_name(1_700_000_000_000, 42)),
            "/.Trash/1700000000000-42"
        );
    }

    #[test]
    fn parses_delete_time_from_entry_names() {
        assert_eq!(
            parse_entry_delete_ms("1700000000000-42"),
            Some(1_700_000_000_000)
        );
        assert_eq!(
            parse_entry_delete_ms("1700000000000-42-1"),
            Some(1_700_000_000_000)
        );
        assert_eq!(parse_entry_delete_ms("not-an-entry"), None);
        assert_eq!(parse_entry_delete_ms("-42"), None);
        assert_eq!(parse_entry_delete_ms(""), None);
    }
}
