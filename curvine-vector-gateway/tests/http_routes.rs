use axum::body::Body;
use axum::http::{Request, StatusCode};
use curvine_vector_gateway::http;
use tower::util::ServiceExt;

#[tokio::test]
async fn health_route_returns_ok() {
    let app = http::health_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn vector_action_route_returns_not_implemented() {
    let app = http::vector_action_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/CreateIndex")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn vector_query_placeholder_returns_not_implemented() {
    let app = http::vector_action_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/QueryVectors")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
}
