use std::sync::Arc;
use std::time::Duration;

use curvine_vector_gateway::auth_store::{AccessKeyStore, AccessKeyStoreEnum, LocalAccessKeyStore};
use tempfile::tempdir;

#[tokio::test]
async fn local_store_filters_disabled_credentials() {
    let dir = tempdir().unwrap();
    let credentials = dir.path().join("credentials.jsonl");
    tokio::fs::write(
        &credentials,
        "{\"access_key\":\"ak1\",\"secret_key\":\"sk1\",\"enabled\":true}\n\
         {\"access_key\":\"ak2\",\"secret_key\":\"sk2\",\"enabled\":false}\n",
    )
    .await
    .unwrap();
    let store = LocalAccessKeyStore::new(credentials.to_str(), None).unwrap();
    let store = AccessKeyStoreEnum::Local(Arc::new(store));
    store.initialize().await.unwrap();
    assert_eq!(store.get("ak1").await.unwrap().as_deref(), Some("sk1"));
    assert_eq!(store.get("ak2").await.unwrap(), None);
}

#[tokio::test]
async fn local_store_returns_error_on_invalid_json_line() {
    let dir = tempdir().unwrap();
    let credentials = dir.path().join("credentials.jsonl");
    tokio::fs::write(&credentials, "invalid-json-line\n")
        .await
        .unwrap();
    let store = LocalAccessKeyStore::new(credentials.to_str(), None).unwrap();
    let store = AccessKeyStoreEnum::Local(Arc::new(store));
    let err = store.initialize().await.unwrap_err();
    assert!(err.contains("invalid credential json"));
}

#[tokio::test]
async fn local_store_refreshes_after_interval() {
    let dir = tempdir().unwrap();
    let credentials = dir.path().join("credentials.jsonl");
    tokio::fs::write(
        &credentials,
        "{\"access_key\":\"ak1\",\"secret_key\":\"sk-v1\",\"enabled\":true}\n",
    )
    .await
    .unwrap();
    let store =
        LocalAccessKeyStore::new(credentials.to_str(), Some(Duration::from_millis(5))).unwrap();
    let store = AccessKeyStoreEnum::Local(Arc::new(store));
    store.initialize().await.unwrap();
    assert_eq!(store.get("ak1").await.unwrap().as_deref(), Some("sk-v1"));

    tokio::fs::write(
        &credentials,
        "{\"access_key\":\"ak1\",\"secret_key\":\"sk-v2\",\"enabled\":true}\n",
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(store.get("ak1").await.unwrap().as_deref(), Some("sk-v2"));
}
