// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Phase 4B — live Curvine [`object_store`] semantics (one integration test).
//!
//! Run (requires a reachable Curvine cluster and `CURVINE_CONF_FILE`):
//! `CURVINE_CONF_FILE=/path/to/cluster.toml cargo test -p curvine-lancedb --test object_store_semantics -- --ignored`
//!
//! Covered operations: `put`, `head`, `get_opts(head=true)`, ranged `get_opts`, overwrite `put`,
//! `copy` (source retained, destination overwritten when present), `copy_if_not_exists`,
//! conditional `PutMode::Update` with e-tags, `delete`, recursive `list`, `list_with_delimiter`
//! (directory prefix is not listed as a file object), and multipart staging invisibility /
//! complete / abort / out-of-order completion / same-path race behavior.

use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::Duration;
use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::Path as CurvinePath;
use futures::StreamExt;
use lance_io::object_store::{ObjectStoreParams, StorageOptionsAccessor};
use lancedb::object_store::{CurvineObjectStoreProvider, CURVINE_CONF_FILE_KEY};
use lancedb::ObjectStoreProvider;
use md5::{Digest, Md5};
use object_store::path::Path;
use object_store::{
    Attribute, Attributes, Error as OsError, GetOptions, GetRange, MultipartUpload, PutMode,
    PutMultipartOptions, PutOptions, UpdateVersion,
};
use url::Url;

#[tokio::test]
#[ignore = "live Curvine cluster + CURVINE_CONF_FILE; cargo test -p curvine-lancedb --test object_store_semantics -- --ignored"]
async fn curvine_object_store_semantics_live_cluster() {
    let conf_path = match env::var(ClusterConf::ENV_CONF_FILE) {
        Ok(v) => v,
        Err(_) => {
            eprintln!("Skipping live object-store semantics test: CURVINE_CONF_FILE is not set");
            return;
        }
    };

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let mut opts = HashMap::new();
    opts.insert(CURVINE_CONF_FILE_KEY.to_string(), conf_path.clone());
    let params = ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(opts))),
        ..Default::default()
    };

    let uri = format!("curvine:///tmp/curvine_os_sem_{unique}");
    let url = Url::parse(&uri).unwrap();
    let provider = CurvineObjectStoreProvider::new();
    let store = provider.new_store(url, &params).await.expect("new_store");

    let pfx = format!("pfx_{unique}");
    let rel_root = "root_marker.txt";
    let rel_key = format!("{pfx}/nested/obj.bin");
    let rel_copy = format!("{pfx}/nested/obj_copy.bin");
    let rel_dst = format!("{pfx}/nested/copy_overwrite_dst.bin");
    let rel_src = format!("{pfx}/nested/copy_overwrite_src.bin");
    let rel_workspace_curvine = format!("{pfx}/.curvine/user-visible.bin");

    let root_key = Path::parse(rel_root).unwrap();
    store.put(&root_key, b"root").await.unwrap();

    let key = Path::parse(&rel_key).unwrap();
    let payload: &[u8] = b"hello-range-copy";
    store.put(&key, payload).await.unwrap();

    let missing = Path::parse(format!("{pfx}/missing.bin")).unwrap();
    let missing_head = store.inner.head(&missing).await;
    assert!(
        matches!(missing_head, Err(OsError::NotFound { .. })),
        "missing object head must map to object_store::Error::NotFound, got {missing_head:?}"
    );

    let workspace_curvine_key = Path::parse(&rel_workspace_curvine).unwrap();
    store
        .put(&workspace_curvine_key, b"workspace-local-curvine")
        .await
        .unwrap();

    let meta = store.inner.head(&key).await.unwrap();
    assert_eq!(meta.size, payload.len() as u64);
    assert!(meta.e_tag.is_some());
    let etag = meta.e_tag.clone().unwrap();

    let head_get = store
        .inner
        .get_opts(
            &key,
            GetOptions {
                head: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(head_get.meta.size, meta.size);

    let if_match_ok = store
        .inner
        .get_opts(
            &key,
            GetOptions {
                if_match: Some(etag.clone()),
                ..Default::default()
            },
        )
        .await;
    assert!(
        if_match_ok.is_ok(),
        "if_match with current e_tag should pass"
    );

    let if_match_bad = store
        .inner
        .get_opts(
            &key,
            GetOptions {
                if_match: Some("W/\"cv:invalid:0\"".to_string()),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(if_match_bad, Err(OsError::Precondition { .. })));

    let if_none_match_same = store
        .inner
        .get_opts(
            &key,
            GetOptions {
                if_none_match: Some(etag.clone()),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(
        if_none_match_same,
        Err(OsError::NotModified { .. })
    ));

    let if_none_match_other = store
        .inner
        .get_opts(
            &key,
            GetOptions {
                if_none_match: Some("W/\"cv:other:1\"".to_string()),
                ..Default::default()
            },
        )
        .await;
    assert!(if_none_match_other.is_ok());

    let if_unmodified_same = store
        .inner
        .get_opts(
            &key,
            GetOptions {
                if_unmodified_since: Some(meta.last_modified),
                ..Default::default()
            },
        )
        .await;
    assert!(if_unmodified_same.is_ok());

    let if_unmodified_old = store
        .inner
        .get_opts(
            &key,
            GetOptions {
                if_unmodified_since: Some(meta.last_modified - Duration::hours(1)),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(
        if_unmodified_old,
        Err(OsError::Precondition { .. })
    ));

    let if_modified_same = store
        .inner
        .get_opts(
            &key,
            GetOptions {
                if_modified_since: Some(meta.last_modified),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(if_modified_same, Err(OsError::NotModified { .. })));

    let if_modified_old = store
        .inner
        .get_opts(
            &key,
            GetOptions {
                if_modified_since: Some(meta.last_modified - Duration::hours(1)),
                ..Default::default()
            },
        )
        .await;
    assert!(if_modified_old.is_ok());

    let slice = store.read_one_range(&key, 1..5).await.unwrap();
    assert_eq!(slice.as_ref(), b"ello");

    let bad_range = store
        .inner
        .get_opts(
            &key,
            GetOptions {
                range: Some(GetRange::Bounded(100..200)),
                ..Default::default()
            },
        )
        .await;
    assert!(bad_range.is_err());

    let create_only = Path::parse(format!("{pfx}/create_only.bin")).unwrap();
    store
        .inner
        .put_opts(
            &create_only,
            Vec::from(&b"create-v1"[..]).into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let create_res = store
        .inner
        .put_opts(
            &Path::parse(format!("{pfx}/create_once.bin")).unwrap(),
            Vec::from(&b"create-v1"[..]).into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(
        create_res.e_tag.is_some(),
        "create put_opts should return an e_tag"
    );
    assert_eq!(
        create_res.version, create_res.e_tag,
        "Curvine version token should match the synthetic e_tag until a native generation is exposed"
    );
    let created_meta = store.inner.head(&create_only).await.unwrap();
    assert_eq!(created_meta.version, create_res.version);
    let create_again = store
        .inner
        .put_opts(
            &create_only,
            Vec::from(&b"create-v2"[..]).into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(create_again, Err(OsError::AlreadyExists { .. })));
    let updated = store
        .inner
        .put_opts(
            &create_only,
            Vec::from(&b"create-v3"[..]).into(),
            PutOptions {
                mode: PutMode::Update(create_res.clone().into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store.read_one_all(&create_only).await.unwrap().as_ref(),
        b"create-v3"
    );
    let stale_update = store
        .inner
        .put_opts(
            &create_only,
            Vec::from(&b"create-stale"[..]).into(),
            PutOptions {
                mode: PutMode::Update(create_res.clone().into()),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(stale_update, Err(OsError::Precondition { .. })));
    store
        .inner
        .put_opts(
            &create_only,
            Vec::from(&b"create-v4"[..]).into(),
            PutOptions {
                mode: PutMode::Update(updated.clone().into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store.read_one_all(&create_only).await.unwrap().as_ref(),
        b"create-v4"
    );
    let overwritten = store
        .inner
        .put_opts(
            &create_only,
            Vec::from(&b"create-v5-longer"[..]).into(),
            PutOptions {
                mode: PutMode::Overwrite,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_ne!(
        overwritten.version, create_res.version,
        "overwrite should produce a different Curvine object version token"
    );
    assert_eq!(overwritten.version, overwritten.e_tag);
    let versioned_get = store
        .inner
        .get_opts(
            &create_only,
            GetOptions {
                version: overwritten.version.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        versioned_get.bytes().await.unwrap().as_ref(),
        b"create-v5-longer"
    );
    let stale_version_get = store
        .inner
        .get_opts(
            &create_only,
            GetOptions {
                version: Some("W/\"cv:stale:0:0:true:1\"".to_string()),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(stale_version_get, Err(OsError::NotImplemented)));
    let version_update = store
        .inner
        .put_opts(
            &create_only,
            Vec::from(&b"create-version"[..]).into(),
            PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: None,
                    version: Some("1".to_string()),
                }),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(version_update, Err(OsError::Generic { .. })));
    let missing_update = store
        .inner
        .put_opts(
            &Path::parse(format!("{pfx}/missing.bin")).unwrap(),
            Vec::from(&b"missing"[..]).into(),
            PutOptions {
                mode: PutMode::Update(updated.into()),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(missing_update, Err(OsError::Precondition { .. })));

    let attrs = Attributes::from_iter([
        (Attribute::CacheControl, "max-age=604800"),
        (
            Attribute::ContentDisposition,
            r#"attachment; filename="curvine.bin""#,
        ),
        (Attribute::ContentEncoding, "gzip"),
        (Attribute::ContentLanguage, "en-US"),
        (Attribute::ContentType, "application/octet-stream"),
        (Attribute::Metadata("curvine-key".into()), "curvine-value"),
    ]);
    let attr_key = Path::parse(format!("{pfx}/attrs/direct.bin")).unwrap();
    store
        .inner
        .put_opts(
            &attr_key,
            Vec::from(&b"attr-body"[..]).into(),
            PutOptions {
                attributes: attrs.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let attr_get = store.inner.get(&attr_key).await.unwrap();
    assert_eq!(attr_get.attributes, attrs);
    assert_eq!(attr_get.bytes().await.unwrap().as_ref(), b"attr-body");
    let attr_head = store
        .inner
        .get_opts(
            &attr_key,
            GetOptions {
                head: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(attr_head.attributes, attrs);

    store.put(&key, b"overwrite").await.unwrap();
    let full = store.read_one_all(&key).await.unwrap();
    assert_eq!(full.as_ref(), b"overwrite");

    let copy_key = Path::parse(&rel_copy).unwrap();
    store.copy(&key, &copy_key).await.unwrap();
    assert_eq!(
        store.read_one_all(&copy_key).await.unwrap().as_ref(),
        b"overwrite"
    );
    assert_eq!(
        store.read_one_all(&key).await.unwrap().as_ref(),
        b"overwrite",
        "copy must retain source object"
    );

    let dst_existing = Path::parse(&rel_dst).unwrap();
    let src_for_overwrite = Path::parse(&rel_src).unwrap();
    store
        .put(&dst_existing, b"stale-destination")
        .await
        .unwrap();
    store
        .put(&src_for_overwrite, b"fresh-source-payload")
        .await
        .unwrap();
    store.copy(&src_for_overwrite, &dst_existing).await.unwrap();
    assert_eq!(
        store.read_one_all(&dst_existing).await.unwrap().as_ref(),
        b"fresh-source-payload",
        "copy replaces an existing destination object"
    );
    assert_eq!(
        store
            .read_one_all(&src_for_overwrite)
            .await
            .unwrap()
            .as_ref(),
        b"fresh-source-payload",
        "copy overwrite must keep the source object"
    );

    let prefix_path = Path::parse(format!("{pfx}/")).unwrap();
    let mut listed: Vec<Path> = store
        .list(Some(prefix_path.clone()))
        .map(|r| r.expect("list entry").location)
        .collect()
        .await;
    listed.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    let ends = |p: &Path, s: &str| p.as_ref().ends_with(s);
    assert!(listed.iter().any(|p| ends(p, &rel_key)));
    assert!(listed.iter().any(|p| ends(p, &rel_workspace_curvine)));
    assert!(listed.iter().any(|p| ends(p, &rel_copy)));
    assert!(listed.iter().any(|p| ends(p, &rel_dst)));
    assert!(listed.iter().any(|p| ends(p, &rel_src)));

    let lr_root = store.inner.list_with_delimiter(None).await.unwrap();
    assert!(lr_root
        .objects
        .iter()
        .any(|o| o.location.as_ref().ends_with(rel_root)));
    assert!(lr_root
        .common_prefixes
        .iter()
        .any(|p| p.as_ref().starts_with(&pfx)));

    let lr_pfx = store
        .inner
        .list_with_delimiter(Some(&prefix_path))
        .await
        .unwrap();
    assert!(
        lr_pfx
            .common_prefixes
            .iter()
            .any(|p| p.as_ref().contains(".curvine")),
        "workspace-local .curvine directory is user data outside the root workspace"
    );
    assert!(
        lr_pfx
            .common_prefixes
            .iter()
            .any(|p| p.as_ref().contains("nested")),
        "expected nested/ as common prefix"
    );
    let flat_name = Path::parse(&pfx).unwrap();
    assert!(
        !lr_pfx.objects.iter().any(|o| o.location == flat_name),
        "directory prefix must not appear as a file object"
    );
    let nested_dir = Path::parse(format!("{pfx}/nested")).unwrap();
    assert!(
        store.inner.head(&nested_dir).await.is_err(),
        "directory prefixes must not produce object metadata in head()"
    );

    store.delete(&copy_key).await.unwrap();
    assert!(store.inner.head(&copy_key).await.is_err());

    let listed_after: Vec<Path> = store
        .list(Some(prefix_path))
        .map(|r| r.expect("list entry").location)
        .collect()
        .await;
    assert!(
        !listed_after.iter().any(|p| ends(p, &rel_copy)),
        "delete must remove object from recursive list results"
    );

    let copy_if_missing = Path::parse(format!("{pfx}/nested/copy_if_missing.bin")).unwrap();
    store
        .inner
        .copy_if_not_exists(&key, &copy_if_missing)
        .await
        .unwrap();
    assert_eq!(
        store.read_one_all(&copy_if_missing).await.unwrap().as_ref(),
        b"overwrite",
        "copy_if_not_exists creates destination when absent"
    );
    let copy_if_exists = store.inner.copy_if_not_exists(&key, &copy_if_missing).await;
    assert!(matches!(copy_if_exists, Err(OsError::AlreadyExists { .. })));

    let rename_src = Path::parse(format!("{pfx}/nested/rename_src.bin")).unwrap();
    let rename_dst = Path::parse(format!("{pfx}/nested/rename_dst.bin")).unwrap();
    store.put(&rename_src, b"rename-me").await.unwrap();
    store.inner.rename(&rename_src, &rename_dst).await.unwrap();
    assert_eq!(
        store.read_one_all(&rename_dst).await.unwrap().as_ref(),
        b"rename-me",
        "rename should move contents to destination"
    );
    assert!(
        store.inner.head(&rename_src).await.is_err(),
        "rename should remove source object"
    );

    let rename_if_src = Path::parse(format!("{pfx}/nested/rename_if_src.bin")).unwrap();
    let rename_if_dst = Path::parse(format!("{pfx}/nested/rename_if_dst.bin")).unwrap();
    store.put(&rename_if_src, b"rename-if").await.unwrap();
    store
        .inner
        .rename_if_not_exists(&rename_if_src, &rename_if_dst)
        .await
        .unwrap();
    assert_eq!(
        store.read_one_all(&rename_if_dst).await.unwrap().as_ref(),
        b"rename-if",
        "rename_if_not_exists should create destination when absent"
    );
    assert!(store.inner.head(&rename_if_src).await.is_err());

    let rename_if_exists_src =
        Path::parse(format!("{pfx}/nested/rename_if_exists_src.bin")).unwrap();
    let rename_if_exists_dst =
        Path::parse(format!("{pfx}/nested/rename_if_exists_dst.bin")).unwrap();
    store.put(&rename_if_exists_src, b"source").await.unwrap();
    store.put(&rename_if_exists_dst, b"dest").await.unwrap();
    let rename_if_exists = store
        .inner
        .rename_if_not_exists(&rename_if_exists_src, &rename_if_exists_dst)
        .await;
    assert!(matches!(
        rename_if_exists,
        Err(OsError::AlreadyExists { .. })
    ));
    assert_eq!(
        store
            .read_one_all(&rename_if_exists_src)
            .await
            .unwrap()
            .as_ref(),
        b"source",
        "failed rename_if_not_exists must keep source object"
    );

    let multipart_key = Path::parse(format!("{pfx}/multipart/final.bin")).unwrap();
    let mut upload = store.inner.put_multipart(&multipart_key).await.unwrap();
    upload
        .put_part(Vec::from(&b"hello "[..]).into())
        .await
        .unwrap();
    upload
        .put_part(Vec::from(&b"multipart"[..]).into())
        .await
        .unwrap();
    let listed_during_multipart: Vec<Path> = store
        .list(None)
        .map(|r| r.expect("list entry").location)
        .collect()
        .await;
    assert!(
        listed_during_multipart
            .iter()
            .all(|p| !p.as_ref().contains(".curvine/lancedb/multipart")),
        "internal multipart staging must stay invisible to object listing"
    );
    let lr_root_during_multipart = store.inner.list_with_delimiter(None).await.unwrap();
    assert!(
        lr_root_during_multipart
            .objects
            .iter()
            .all(|o| !o.location.as_ref().contains(".curvine/lancedb/multipart")),
        "root objects must not expose multipart staging files"
    );
    assert!(
        lr_root_during_multipart
            .common_prefixes
            .iter()
            .all(|p| !p.as_ref().contains(".curvine/lancedb/multipart")),
        "root prefixes must not expose multipart staging directories"
    );
    let mp_res = upload.complete().await.unwrap();
    assert!(mp_res.e_tag.is_some());
    assert_eq!(mp_res.version, mp_res.e_tag);
    assert_eq!(
        store.read_one_all(&multipart_key).await.unwrap().as_ref(),
        b"hello multipart",
        "multipart complete must concatenate parts in order"
    );
    let multipart_attrs = Attributes::from_iter([
        (Attribute::ContentType, "application/x-curvine-multipart"),
        (
            Attribute::Metadata("multipart-key".into()),
            "multipart-value",
        ),
    ]);
    let multipart_attr_key = Path::parse(format!("{pfx}/multipart/attrs.bin")).unwrap();
    let mut attr_upload = store
        .inner
        .put_multipart_opts(
            &multipart_attr_key,
            PutMultipartOptions {
                attributes: multipart_attrs.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    attr_upload
        .put_part(Vec::from(&b"multipart "[..]).into())
        .await
        .unwrap();
    attr_upload
        .put_part(Vec::from(&b"attrs"[..]).into())
        .await
        .unwrap();
    attr_upload.complete().await.unwrap();
    let multipart_attr_get = store.inner.get(&multipart_attr_key).await.unwrap();
    assert_eq!(multipart_attr_get.attributes, multipart_attrs);
    assert_eq!(
        multipart_attr_get.bytes().await.unwrap().as_ref(),
        b"multipart attrs"
    );
    let mp_update = store
        .inner
        .put_opts(
            &multipart_key,
            Vec::from(&b"after multipart"[..]).into(),
            PutOptions {
                mode: PutMode::Update(mp_res.into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(mp_update.e_tag.is_some());
    assert_eq!(mp_update.version, mp_update.e_tag);
    assert_eq!(
        store.read_one_all(&multipart_key).await.unwrap().as_ref(),
        b"after multipart",
        "multipart PutResult must be usable as an UpdateVersion"
    );

    let prefix_only = Path::parse(format!("{pfx}/prefix_only")).unwrap();
    let prefix_child = Path::parse(format!("{pfx}/prefix_only/child.bin")).unwrap();
    store.put(&prefix_child, b"child").await.unwrap();
    store.inner.delete(&prefix_only).await.unwrap();
    assert_eq!(
        store.read_one_all(&prefix_child).await.unwrap().as_ref(),
        b"child",
        "deleting a directory prefix must not delete child objects"
    );

    let multipart_order_key = Path::parse(format!("{pfx}/multipart/out_of_order.bin")).unwrap();
    let mut out_of_order = store
        .inner
        .put_multipart(&multipart_order_key)
        .await
        .unwrap();
    let p1 = out_of_order.put_part(vec![b'1'; 1024].into());
    let p2 = out_of_order.put_part(vec![b'2'; 1024].into());
    let p3 = out_of_order.put_part(vec![b'3'; 1024].into());
    p3.await.unwrap();
    p2.await.unwrap();
    p1.await.unwrap();
    out_of_order.complete().await.unwrap();
    let order_bytes = store.read_one_all(&multipart_order_key).await.unwrap();
    assert_eq!(&order_bytes[..4], b"1111");
    assert_eq!(&order_bytes[1024..1028], b"2222");
    assert_eq!(&order_bytes[2048..2052], b"3333");

    let abort_key = Path::parse(format!("{pfx}/multipart/aborted.bin")).unwrap();
    let mut abort_upload = store.inner.put_multipart(&abort_key).await.unwrap();
    abort_upload
        .put_part(Vec::from(&b"temp-data"[..]).into())
        .await
        .unwrap();
    abort_upload.abort().await.unwrap();
    assert!(store.inner.head(&abort_key).await.is_err());

    let failed_complete_key = Path::parse(format!("{pfx}/multipart/fails_as_dir")).unwrap();
    let failed_complete_child =
        Path::parse(format!("{pfx}/multipart/fails_as_dir/child.bin")).unwrap();
    store.put(&failed_complete_child, b"child").await.unwrap();
    let mut failed_upload = store
        .inner
        .put_multipart(&failed_complete_key)
        .await
        .unwrap();
    failed_upload
        .put_part(Vec::from(&b"will-not-commit"[..]).into())
        .await
        .unwrap();
    let failed_complete = failed_upload.complete().await;
    assert!(matches!(
        failed_complete,
        Err(OsError::AlreadyExists { .. })
    ));
    assert_eq!(
        store
            .read_one_all(&failed_complete_child)
            .await
            .unwrap()
            .as_ref(),
        b"child",
        "failed multipart complete must not disturb existing prefix children"
    );
    assert_staging_clean(&conf_path, &failed_complete_key).await;

    let race_key = Path::parse(format!("{pfx}/multipart/race.bin")).unwrap();
    let mut upload1 = store.inner.put_multipart(&race_key).await.unwrap();
    let mut upload2 = store.inner.put_multipart(&race_key).await.unwrap();

    fn make_payload(prefix: u8, part: u8) -> Vec<u8> {
        let mut payload = vec![b'0'; 5_300_002];
        payload[0] = prefix;
        payload[1] = b':';
        payload[2] = part;
        payload
    }

    upload1
        .put_part(make_payload(b'1', 0).into())
        .await
        .unwrap();
    upload2
        .put_part(make_payload(b'2', 0).into())
        .await
        .unwrap();
    upload2
        .put_part(make_payload(b'2', 1).into())
        .await
        .unwrap();
    upload1
        .put_part(make_payload(b'1', 1).into())
        .await
        .unwrap();
    upload1
        .put_part(make_payload(b'1', 2).into())
        .await
        .unwrap();
    upload2
        .put_part(make_payload(b'2', 2).into())
        .await
        .unwrap();
    upload2
        .put_part(make_payload(b'2', 3).into())
        .await
        .unwrap();
    upload1
        .put_part(make_payload(b'1', 3).into())
        .await
        .unwrap();
    upload1
        .put_part(make_payload(b'1', 4).into())
        .await
        .unwrap();
    upload2
        .put_part(make_payload(b'2', 4).into())
        .await
        .unwrap();

    upload1.complete().await.unwrap();
    upload2.complete().await.unwrap();
    let raced = store.read_one_all(&race_key).await.unwrap();
    let mut expected = Vec::new();
    for part in 0..5 {
        expected.extend_from_slice(&make_payload(b'2', part));
    }
    assert!(
        raced.starts_with(&expected),
        "last completed same-path multipart upload should win"
    );
}

#[tokio::test]
#[ignore = "live Curvine cluster + CURVINE_CONF_FILE; cargo test -p curvine-lancedb --test object_store_semantics -- --ignored"]
async fn curvine_object_store_root_workspace_hides_multipart_staging() {
    let conf = match env::var(ClusterConf::ENV_CONF_FILE) {
        Ok(v) => v,
        Err(_) => {
            eprintln!(
                "Skipping root-workspace object-store semantics test: CURVINE_CONF_FILE is not set"
            );
            return;
        }
    };

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let mut opts = HashMap::new();
    opts.insert(CURVINE_CONF_FILE_KEY.to_string(), conf);
    let params = ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(opts))),
        ..Default::default()
    };

    let url = Url::parse("curvine:///").unwrap();
    let provider = CurvineObjectStoreProvider::new();
    let store = provider.new_store(url, &params).await.expect("new_store");

    let reserved_key = Path::parse(".curvine/user.bin").unwrap();
    let reserved_put = store
        .inner
        .put(&reserved_key, (&b"forbidden"[..]).into())
        .await;
    assert!(matches!(reserved_put, Err(OsError::NotSupported { .. })));

    let key = Path::parse(format!("root_ws_{unique}/multipart/final.bin")).unwrap();
    let mut upload = store.inner.put_multipart(&key).await.unwrap();
    upload
        .put_part(Vec::from(&b"root "[..]).into())
        .await
        .unwrap();
    upload
        .put_part(Vec::from(&b"workspace"[..]).into())
        .await
        .unwrap();

    let listed: Vec<Path> = store
        .list(None)
        .map(|r| r.expect("list entry").location)
        .collect()
        .await;
    assert!(
        listed.iter().all(|p| !p.as_ref().starts_with(".curvine")),
        "root workspace must not leak multipart staging paths into list()"
    );

    let lr_root = store.inner.list_with_delimiter(None).await.unwrap();
    assert!(
        lr_root
            .objects
            .iter()
            .all(|o| !o.location.as_ref().starts_with(".curvine")),
        "root workspace objects must not expose multipart staging files"
    );
    assert!(
        lr_root
            .common_prefixes
            .iter()
            .all(|p| !p.as_ref().starts_with(".curvine")),
        "root workspace prefixes must not expose multipart staging directories"
    );
    assert!(
        !lr_root.common_prefixes.is_empty() || !lr_root.objects.is_empty() || listed.is_empty(),
        "root workspace visibility check should only see user data or no entries, never internal staging"
    );

    upload.complete().await.unwrap();
    let full = store.read_one_all(&key).await.unwrap();
    assert_eq!(full.as_ref(), b"root workspace");

    let top_key = Path::parse(format!("root_ws_top_{unique}.bin")).unwrap();
    let mut top_upload = store.inner.put_multipart(&top_key).await.unwrap();
    top_upload
        .put_part(Vec::from(&b"top-level"[..]).into())
        .await
        .unwrap();
    top_upload.complete().await.unwrap();
    let top_full = store.read_one_all(&top_key).await.unwrap();
    assert_eq!(top_full.as_ref(), b"top-level");
}

async fn assert_staging_clean(conf_path: &str, location: &Path) {
    let conf = ClusterConf::from(conf_path).expect("load Curvine cluster configuration");
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = CurvineFileSystem::with_rt(conf, rt).expect("create Curvine filesystem");
    let staging_dir = CurvinePath::from_str(format!(
        "/.curvine/lancedb/multipart/{}",
        multipart_staging_id("/", location)
    ))
    .expect("valid staging path");

    let statuses = fs.list_status(&staging_dir).await.unwrap_or_default();
    assert!(
        statuses.is_empty(),
        "multipart failure cleanup should remove staging directory entries under {}",
        staging_dir.full_path()
    );
}

fn multipart_staging_id(workspace_root: &str, location: &Path) -> String {
    let mut hasher = Md5::new();
    hasher.update(workspace_root.as_bytes());
    hasher.update([0]);
    hasher.update(location.as_ref().as_bytes());
    format!("{:x}", hasher.finalize())
}
