use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::middleware;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Extension, Router};
use curvine_vector_gateway::auth::{vectors_sig_v4_middleware, AuthHttpCtx};
use curvine_vector_gateway::auth_store::{AccessKeyStore, AccessKeyStoreEnum, LocalAccessKeyStore};
use curvine_vector_gateway::sig_v4::{
    get_v4_signature, EMPTY_PAYLOAD_HASH, SIGV4_SERVICE_S3VECTORS,
};
use tempfile::tempdir;
use tower::util::ServiceExt;

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";
const DATE: &str = "20260101T000000Z";
const DATE_SCOPE: &str = "20260101";

#[tokio::test]
async fn middleware_accepts_valid_sig_v4_request() {
    let store = setup_store().await;
    let app = test_router(store);
    let request = build_request(SIGV4_SERVICE_S3VECTORS);
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn middleware_rejects_wrong_credential_scope_service() {
    let store = setup_store().await;
    let app = test_router(store);
    let request = build_request("s3");
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn middleware_rejects_unknown_access_key() {
    let store = setup_store().await;
    let app = test_router(store);
    let request = build_request_with(SECRET_KEY, "AKIA_UNKNOWN", SIGV4_SERVICE_S3VECTORS, false);
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn middleware_rejects_signature_mismatch() {
    let store = setup_store().await;
    let app = test_router(store);
    let request = build_request_with(SECRET_KEY, ACCESS_KEY, SIGV4_SERVICE_S3VECTORS, true);
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn middleware_rejects_malformed_request_without_auth() {
    let store = setup_store().await;
    let app = test_router(store);
    let request = Request::builder()
        .method("POST")
        .uri("/CreateIndex")
        .header("host", "vectors.example.com")
        .header("x-amz-date", DATE)
        .header("x-amz-content-sha256", EMPTY_PAYLOAD_HASH)
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn middleware_fails_when_store_not_configured() {
    let app = Router::new()
        .route(
            "/CreateIndex",
            post(|| async { (StatusCode::OK, "ok").into_response() }),
        )
        .layer(middleware::from_fn(vectors_sig_v4_middleware));
    let request = build_request(SIGV4_SERVICE_S3VECTORS);
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn middleware_returns_internal_error_on_store_read_error() {
    let dir = tempdir().unwrap();
    let store = LocalAccessKeyStore::new(dir.path().to_str(), None).unwrap();
    let app = test_router(AccessKeyStoreEnum::Local(Arc::new(store)));
    let request = build_request(SIGV4_SERVICE_S3VECTORS);
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

fn test_router(store: AccessKeyStoreEnum) -> Router {
    Router::new()
        .route(
            "/CreateIndex",
            post(|| async { (StatusCode::OK, "ok").into_response() }),
        )
        .layer(middleware::from_fn(vectors_sig_v4_middleware))
        .layer(Extension(store))
}

async fn setup_store() -> AccessKeyStoreEnum {
    let dir = tempdir().unwrap();
    let credentials = dir.path().join("credentials.jsonl");
    let line = format!(
        "{{\"access_key\":\"{ACCESS_KEY}\",\"secret_key\":\"{SECRET_KEY}\",\"enabled\":true}}\n"
    );
    tokio::fs::write(&credentials, line).await.unwrap();
    let store = LocalAccessKeyStore::new(credentials.to_str(), None).unwrap();
    let enum_store = AccessKeyStoreEnum::Local(Arc::new(store));
    enum_store.initialize().await.unwrap();
    assert_eq!(
        enum_store.get(ACCESS_KEY).await.unwrap().as_deref(),
        Some(SECRET_KEY)
    );
    enum_store
}

fn build_request(credential_scope_service: &str) -> Request<Body> {
    build_request_with(SECRET_KEY, ACCESS_KEY, credential_scope_service, false)
}

fn build_request_with(
    sign_secret: &str,
    auth_access_key: &str,
    credential_scope_service: &str,
    tamper_signature: bool,
) -> Request<Body> {
    let mut headers = HeaderMap::new();
    headers.insert("host", "vectors.example.com".parse().unwrap());
    headers.insert("x-amz-date", DATE.parse().unwrap());
    headers.insert("x-amz-content-sha256", EMPTY_PAYLOAD_HASH.parse().unwrap());

    let ctx = AuthHttpCtx::new(&headers, "POST", "/CreateIndex");
    let signed_headers = vec!["host", "x-amz-date"];
    let (signature, _) = get_v4_signature(
        &ctx,
        "POST",
        REGION,
        credential_scope_service,
        "/CreateIndex",
        sign_secret,
        EMPTY_PAYLOAD_HASH,
        &signed_headers,
        vec![],
    )
    .unwrap();

    let mut authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}/{}/{}/aws4_request, SignedHeaders=host;x-amz-date, Signature={}",
        auth_access_key, DATE_SCOPE, REGION, credential_scope_service, signature
    );
    if tamper_signature {
        authorization.push('0');
    }
    headers.insert("authorization", authorization.parse().unwrap());

    let mut request = Request::builder()
        .method("POST")
        .uri("/CreateIndex")
        .body(Body::empty())
        .unwrap();
    *request.headers_mut() = headers;
    request
}
