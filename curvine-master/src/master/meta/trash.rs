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

// Staging directory for asynchronous deletes. It lives inside the reserved trash
// namespace, so entries under it are never listed, restored or swept by the
// retention pass: `list_trash` and `empty_trash` filter children by
// `parse_entry_delete_ms`, and `scan_expired_trash` requires trash xattrs.
pub const TRASH_PENDING_NAME: &str = ".pending";

pub const TRASH_PENDING_ROOT: &str = "/.Trash/.pending";

const TRASH_PENDING_PREFIX: &str = ".Trash/.pending/";

// Staging directory for large synchronous permanent deletes (#1864). The delete
// renames the subtree here and then purges it in bounded batches before it
// answers, so the namespace lock is never held for the whole subtree. It is kept
// apart from `.pending` so an `EmptyTrash` drain never waits on, or races with,
// a delete the caller is still waiting for. Like `.pending` its names carry no
// trash xattrs, so listing, restore and the retention sweep ignore it.
pub const TRASH_PURGING_NAME: &str = ".purging";

pub const TRASH_PURGING_ROOT: &str = "/.Trash/.purging";

const TRASH_PURGING_PREFIX: &str = ".Trash/.purging/";

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

// Soft-deleted entries live in the checkpoint directory that covers their
// delete time: `/.Trash/<checkpoint>/<delete_ms>-<inode_id>[-seq]`, where the
// checkpoint name is the delete time rounded down to
// `master.trash_checkpoint_interval`. Checkpoint names are zero-padded to the
// width of a millisecond timestamp, so lexical name order matches numeric order
// even when the interval does not divide 10^13 (a large interval, or one that
// changed between releases); ordered scans and pagination can then work without
// loading entry inodes, and one directory never accumulates the whole retention
// window (the HDFS trash checkpoint layout).
//
// Entries written before this layout existed stay directly under `/.Trash`.
// They remain listable, restorable and purgeable, and the retention pass
// reclaims them like any checkpoint entry.
pub fn checkpoint_name(delete_ms: i64, width_ms: i64) -> String {
    let width = width_ms.max(1);
    format!("{:013}", delete_ms - delete_ms.rem_euclid(width))
}

pub fn is_checkpoint_name(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(|b| b.is_ascii_digit())
}

pub fn parse_checkpoint_ms(name: &str) -> Option<i64> {
    if !is_checkpoint_name(name) {
        return None;
    }
    name.parse().ok()
}

pub fn checkpoint_path(checkpoint: &str) -> String {
    format!("{}{}{}", TRASH_ROOT, PATH_SEPARATOR, checkpoint)
}

pub fn checkpoint_entry_path(checkpoint: &str, entry_name: &str) -> String {
    format!(
        "{}{}{}{}{}",
        TRASH_ROOT, PATH_SEPARATOR, checkpoint, PATH_SEPARATOR, entry_name
    )
}

// Joins a trash-relative entry path (`<checkpoint>/<name>`, or `<name>` for a
// pre-checkpoint entry) into an absolute path.
pub fn trash_path(rel_path: &str) -> String {
    format!("{}{}{}", TRASH_ROOT, PATH_SEPARATOR, rel_path)
}

// Splits a trash-relative entry path into its optional checkpoint and its entry
// name. A bare name must be an entry name (`<delete_ms>-<inode>...`), not a
// checkpoint directory, so a checkpoint path never parses as an entry. `None`
// for malformed paths, staging paths and anything deeper than one entry.
pub fn split_entry_rel_path(rel_path: &str) -> Option<(Option<&str>, &str)> {
    let mut parts = rel_path.split(PATH_SEPARATOR);
    match (parts.next(), parts.next(), parts.next()) {
        (Some(name), None, None) if parse_entry_delete_ms(name).is_some() => Some((None, name)),
        (Some(checkpoint), Some(name), None)
            if is_checkpoint_name(checkpoint) && parse_entry_delete_ms(name).is_some() =>
        {
            Some((Some(checkpoint), name))
        }
        _ => None,
    }
}

// True for the pending staging root itself and every path below it.
pub fn is_pending_path(path: &str) -> bool {
    match path.strip_prefix(TRASH_PENDING_ROOT) {
        Some(rest) => rest.is_empty() || rest.starts_with(PATH_SEPARATOR),
        None => false,
    }
}

// Pending entries are always direct children of the pending root.
pub fn is_pending_entry_path(path: &str) -> bool {
    let path = path.trim_start_matches(PATH_SEPARATOR);
    match path.strip_prefix(TRASH_PENDING_PREFIX) {
        Some(name) => !name.is_empty() && !name.contains(PATH_SEPARATOR),
        None => false,
    }
}

pub fn pending_entry_path(entry_name: &str) -> String {
    format!("{}{}{}", TRASH_PENDING_ROOT, PATH_SEPARATOR, entry_name)
}

// True for the purging staging root itself and every path below it.
pub fn is_purging_path(path: &str) -> bool {
    match path.strip_prefix(TRASH_PURGING_ROOT) {
        Some(rest) => rest.is_empty() || rest.starts_with(PATH_SEPARATOR),
        None => false,
    }
}

// Purging entries are always direct children of the purging root.
pub fn is_purging_entry_path(path: &str) -> bool {
    let path = path.trim_start_matches(PATH_SEPARATOR);
    match path.strip_prefix(TRASH_PURGING_PREFIX) {
        Some(name) => !name.is_empty() && !name.contains(PATH_SEPARATOR),
        None => false,
    }
}

pub fn purging_entry_path(entry_name: &str) -> String {
    format!("{}{}{}", TRASH_PURGING_ROOT, PATH_SEPARATOR, entry_name)
}

// Internal staging areas that clients must never see or modify.
pub fn is_staging_path(path: &str) -> bool {
    is_pending_path(path) || is_purging_path(path)
}

// True for a restorable or purgeable trash entry: a legacy direct child of the
// trash root, or a child of a checkpoint directory. Staging areas and deeper
// paths are not entries.
pub fn is_trash_entry_path(path: &str) -> bool {
    let path = path.trim_start_matches(PATH_SEPARATOR);
    let Some(rel) = path.strip_prefix(TRASH_ROOT_PREFIX) else {
        return false;
    };
    if rel.is_empty() {
        return false;
    }
    let first = rel.split(PATH_SEPARATOR).next().unwrap_or_default();
    if first == TRASH_PENDING_NAME || first == TRASH_PURGING_NAME {
        return false;
    }
    split_entry_rel_path(rel).is_some()
}

// Entry names are "{delete_ms}-{inode_id}[-{seq}]". Listing sorts by the
// delete-time prefix so pagination can order names without loading inodes.
pub fn parse_entry_delete_ms(name: &str) -> Option<i64> {
    name.split_once('-')?.0.parse().ok()
}

// Resume key for ListTrash: the trash path of the last child consumed. Names
// under the trash root sort chronologically, so the path alone identifies the
// position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrashCursor {
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
    fn identifies_pending_paths() {
        assert!(is_pending_path(TRASH_PENDING_ROOT));
        assert!(is_pending_path("/.Trash/.pending/entry"));
        assert!(!is_pending_path(TRASH_ROOT));
        assert!(!is_pending_path("/.Trash/.pendingx"));

        assert!(is_pending_entry_path("/.Trash/.pending/1-2"));
        assert!(!is_pending_entry_path(TRASH_PENDING_ROOT));
        assert!(!is_pending_entry_path("/.Trash/.pending/a/b"));
        assert!(!is_pending_entry_path("/.Trash/1-2"));
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
        assert!(is_trash_entry_path(
            "/.Trash/1700000000000/1700000000000-42"
        ));
        assert!(is_trash_entry_path(".Trash/1700000000000/1700000000000-42"));

        assert!(!is_trash_entry_path("/.Trash"));
        assert!(!is_trash_entry_path("/.Trash/"));
        assert!(!is_trash_entry_path("/.Trash/a/b"));
        assert!(!is_trash_entry_path("/.Trash/1700000000000"));
        assert!(!is_trash_entry_path(
            "/.Trash/1700000000000/1700000000000-42/x"
        ));
        assert!(!is_trash_entry_path("/.Trash/.pending/1700000000000-42"));
        assert!(!is_trash_entry_path("/.Trash/.purging/1700000000000-42"));
        assert!(!is_trash_entry_path("/plain.log"));
        assert!(!is_trash_entry_path("/a/.Trash/entry"));
    }

    #[test]
    fn checkpoint_names_round_down_to_the_interval() {
        let hour = 60 * 60 * 1000;
        assert_eq!(checkpoint_name(1_700_000_000_000, hour), "1699999200000");
        assert_eq!(checkpoint_name(1_699_999_200_000, hour), "1699999200000");

        let name = checkpoint_name(1_700_000_000_000, hour);
        assert!(is_checkpoint_name(&name));
        assert_eq!(parse_checkpoint_ms(&name), Some(1_699_999_200_000));
        assert_eq!(checkpoint_path(&name), "/.Trash/1699999200000");
        assert_eq!(
            checkpoint_entry_path(&name, "1700000000000-42"),
            "/.Trash/1699999200000/1700000000000-42"
        );

        // A width of zero must not divide by zero; it degrades to per-ms names.
        assert_eq!(checkpoint_name(1_700_000_000_000, 0), "1700000000000");

        // A wide interval yields a shorter timestamp; padding keeps lexical
        // order numeric so a scan can still stop at the first newer source.
        let wide = checkpoint_name(1_700_000_000_000, 999_999_999_999);
        assert_eq!(wide, "0999999999999");
        assert_eq!(parse_checkpoint_ms(&wide), Some(999_999_999_999));
        assert!(wide < checkpoint_name(1_700_002_800_000, hour));
        assert!(wide.as_str() < "1700002800000");

        assert!(!is_checkpoint_name(""));
        assert!(!is_checkpoint_name(".pending"));
        assert!(!is_checkpoint_name("1699999200000-42"));
        assert_eq!(parse_checkpoint_ms("not-a-checkpoint"), None);
    }

    #[test]
    fn splits_entry_rel_paths() {
        assert_eq!(
            split_entry_rel_path("1700000000000-42"),
            Some((None, "1700000000000-42"))
        );
        assert_eq!(
            split_entry_rel_path("1699999200000/1700000000000-42"),
            Some((Some("1699999200000"), "1700000000000-42"))
        );
        assert_eq!(split_entry_rel_path(""), None);
        assert_eq!(split_entry_rel_path("a/b"), None);
        // A bare checkpoint name is a directory, not an entry.
        assert_eq!(split_entry_rel_path("1699999200000"), None);
        assert_eq!(split_entry_rel_path("1699999200000/"), None);
        assert_eq!(split_entry_rel_path("1699999200000/a/b"), None);
        assert_eq!(
            trash_path("1699999200000/1700000000000-42"),
            "/.Trash/1699999200000/1700000000000-42"
        );
    }

    #[test]
    fn identifies_purging_paths() {
        assert!(is_purging_path("/.Trash/.purging"));
        assert!(is_purging_path("/.Trash/.purging/1700000000000-42/a"));
        assert!(!is_purging_path("/.Trash/.purgingx"));
        assert!(!is_purging_path("/.Trash/.pending/1700000000000-42"));

        assert!(is_purging_entry_path("/.Trash/.purging/1700000000000-42"));
        assert!(!is_purging_entry_path("/.Trash/.purging"));
        assert!(!is_purging_entry_path("/.Trash/.purging/a/b"));
        assert_eq!(
            purging_entry_path("1700000000000-42"),
            "/.Trash/.purging/1700000000000-42"
        );
        // Staging names never parse as listable trash entries.
        assert_eq!(parse_entry_delete_ms(TRASH_PURGING_NAME), None);

        assert!(is_staging_path("/.Trash/.pending/x"));
        assert!(is_staging_path("/.Trash/.purging/x"));
        assert!(!is_staging_path("/.Trash/1700000000000-42"));
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
            checkpoint_entry_path(
                &checkpoint_name(1_700_000_000_000, 60 * 60 * 1000),
                &trash_entry_name(1_700_000_000_000, 42)
            ),
            "/.Trash/1699999200000/1700000000000-42"
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
