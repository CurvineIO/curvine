// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0

use super::dto::{CommonPrefix, ListMultipartUploadsResult, MultipartUpload, Owner, S3_NAMESPACE};
use super::s3_api::VRequest;
use crate::utils::{s3_utils::encode_url_component, temp_storage::ActiveUpload};
use chrono::SecondsFormat;
use std::cmp::Ordering;

pub(crate) struct UploadListOptions {
    prefix: Option<String>,
    delimiter: Option<String>,
    key_marker: Option<String>,
    upload_id_marker: Option<String>,
    encoding_type: Option<String>,
    pub max_uploads: u32,
}

impl UploadListOptions {
    pub(crate) fn from_request(req: &impl VRequest) -> Result<Self, String> {
        let max_uploads = match req.get_query("max-uploads").as_deref() {
            None | Some("") => 1000,
            Some(raw) => raw
                .parse::<i32>()
                .ok()
                .filter(|&n| n >= 0 && raw.bytes().all(|b| b.is_ascii_digit()))
                .map(|n| n.min(1000) as u32)
                .ok_or("InvalidArgument: max-uploads must be a non-negative integer")?,
        };
        let encoding_type = req.get_query("encoding-type");
        if encoding_type.as_deref().is_some_and(|s| s != "url") {
            return Err("InvalidArgument: encoding-type must be url".to_string());
        }
        Ok(Self {
            prefix: req.get_query("prefix"),
            delimiter: req.get_query("delimiter").filter(|s| !s.is_empty()),
            key_marker: req.get_query("key-marker").filter(|s| !s.is_empty()),
            upload_id_marker: req.get_query("upload-id-marker").filter(|s| !s.is_empty()),
            encoding_type,
            max_uploads,
        })
    }
}

pub(crate) fn page_uploads(
    bucket: &str,
    opt: UploadListOptions,
    mut uploads: Vec<ActiveUpload>,
) -> ListMultipartUploadsResult {
    let encode = |s: &str| match opt.encoding_type.as_deref() {
        Some("url") => encode_url_component(s),
        _ => s.to_owned(),
    };
    let prefix = opt.prefix.as_deref().unwrap_or_default();
    let mut page = ListMultipartUploadsResult {
        xmlns: S3_NAMESPACE,
        bucket: bucket.to_owned(),
        key_marker: opt.key_marker.as_deref().map(encode).unwrap_or_default(),
        upload_id_marker: opt.upload_id_marker.clone().unwrap_or_default(),
        next_key_marker: None,
        next_upload_id_marker: None,
        prefix: opt.prefix.as_deref().map(encode),
        delimiter: opt.delimiter.as_deref().map(encode),
        encoding_type: opt.encoding_type.clone(),
        max_uploads: opt.max_uploads,
        is_truncated: false,
        uploads: Vec::new(),
        common_prefixes: Vec::new(),
    };
    if opt.max_uploads == 0 {
        return page;
    }
    uploads.retain(|upload| upload.key.starts_with(prefix));
    uploads.sort_unstable_by(|a, b| (&a.key, &a.upload_id).cmp(&(&b.key, &b.upload_id)));
    let mut last_key = None;
    let mut last_id = None;
    let mut last_group = None;
    for upload in uploads {
        let group = opt.delimiter.as_deref().and_then(|delimiter| {
            let end = prefix.len() + upload.key[prefix.len()..].find(delimiter)? + delimiter.len();
            Some(&upload.key[..end])
        });
        if let Some(group) = group {
            // A rolled-up prefix counts once and never advances an upload ID.
            if last_group.as_deref() == Some(group)
                || opt
                    .key_marker
                    .as_deref()
                    .is_some_and(|marker| group <= marker)
            {
                continue;
            }
        } else if let Some(marker) = opt.key_marker.as_deref() {
            match upload.key.as_str().cmp(marker) {
                Ordering::Less => continue,
                Ordering::Equal
                    if !opt
                        .upload_id_marker
                        .as_deref()
                        .is_some_and(|id| upload.upload_id.as_str() > id) =>
                {
                    continue;
                }
                _ => {}
            }
        }
        if page.uploads.len() + page.common_prefixes.len() == opt.max_uploads as usize {
            page.is_truncated = true;
            page.next_key_marker = last_key;
            page.next_upload_id_marker = last_id;
            break;
        }
        if let Some(group) = group {
            let encoded = encode(group);
            last_group = Some(group.to_owned());
            last_key = Some(encoded.clone());
            last_id = None;
            page.common_prefixes.push(CommonPrefix { prefix: encoded });
        } else {
            let key = encode(&upload.key);
            last_key = Some(key.clone());
            last_id = Some(upload.upload_id.clone());
            page.uploads.push(MultipartUpload {
                key,
                upload_id: upload.upload_id,
                initiator: Some(Owner::default()),
                owner: Some(Owner::default()),
                storage_class: Some("STANDARD".to_string()),
                initiated: upload
                    .initiated
                    .to_rfc3339_opts(SecondsFormat::Millis, true),
            });
        }
    }
    page
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sessions() -> Vec<ActiveUpload> {
        [
            ("p/a/b", "3"),
            ("p/a/z", "4"),
            ("p/a", "2"),
            ("p/b+中", "5"),
            ("p/a", "1"),
            ("other", "6"),
        ]
        .into_iter()
        .map(|(key, id)| ActiveUpload {
            key: key.into(),
            upload_id: id.into(),
            initiated: chrono::DateTime::UNIX_EPOCH,
        })
        .collect()
    }

    #[test]
    fn page_counts_groups_and_duplicate_keys_together() {
        let options = |key: Option<&str>, id: Option<&str>| UploadListOptions {
            prefix: Some("p/".into()),
            delimiter: Some("/".into()),
            key_marker: key.map(str::to_owned),
            upload_id_marker: id.map(str::to_owned),
            encoding_type: None,
            max_uploads: 1,
        };
        let first = page_uploads("b", options(None, Some("9")), sessions());
        assert_eq!(first.uploads[0].upload_id, "1");
        assert_eq!(first.next_key_marker.as_deref(), Some("p/a"));
        assert_eq!(first.next_upload_id_marker.as_deref(), Some("1"));
        let second = page_uploads("b", options(Some("p/a"), Some("1")), sessions());
        assert_eq!(second.uploads[0].upload_id, "2");
        let group = page_uploads("b", options(Some("p/a"), None), sessions());
        assert!(group.uploads.is_empty() && group.is_truncated);
        assert_eq!(group.common_prefixes[0].prefix, "p/a/");
        assert_eq!(group.next_key_marker.as_deref(), Some("p/a/"));
        assert!(group.next_upload_id_marker.is_none());
        let last = page_uploads("b", options(Some("p/a/"), None), sessions());
        assert_eq!(last.uploads[0].key, "p/b+中");
        assert!(!last.is_truncated && last.next_key_marker.is_none());
    }

    #[test]
    fn url_encoding_applies_to_keys_and_not_upload_ids() {
        let page = page_uploads(
            "b",
            UploadListOptions {
                prefix: Some("p/".into()),
                delimiter: Some("+".into()),
                key_marker: Some("p/a?".into()),
                upload_id_marker: Some("raw+id".into()),
                encoding_type: Some("url".into()),
                max_uploads: 1000,
            },
            sessions(),
        );
        let xml = quick_xml::se::to_string(&page).unwrap();
        assert!(xml.contains("<KeyMarker>p/a%3F</KeyMarker>"));
        assert!(xml.contains("<UploadIdMarker>raw+id</UploadIdMarker>"));
        assert!(xml.contains("<Delimiter>%2B</Delimiter>"));
        assert!(xml.contains("<CommonPrefixes><Prefix>p/b%2B</Prefix></CommonPrefixes>"));
    }
}
