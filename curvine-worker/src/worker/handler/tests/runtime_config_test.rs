use super::*;
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use curvine_config::{ClusterConf, TransferConf};
use curvine_fault::FaultHttpControl;
use curvine_runtime::runtime::Runtime;
use tower::ServiceExt;
fn test_handler() -> WorkerRouterHandler {
    let conf = ClusterConf::default();
    let task_manager = Arc::new(
        TaskManager::with_rt(Arc::new(Runtime::single()), &conf, "runtime-config-test")
            .expect("task manager"),
    );
    WorkerRouterHandler {
        fault_http: FaultHttpControl::default(),
        server_state: Arc::default(),
        task_manager,
    }
}
fn test_task_manager(handler: &WorkerRouterHandler) -> Arc<TaskManager> {
    handler.task_manager.clone()
}

async fn response_json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    serde_json::from_slice(&bytes).expect("json body")
}

#[test]
fn parses_supported_value_formats() {
    assert_eq!(
        parse_inflight_budget("100GB").unwrap(),
        100 * 1024 * 1024 * 1024
    );
    assert_eq!(
        parse_duration_ms(TASK_TIMEOUT_CONFIG, "30m").unwrap(),
        30 * 60 * 1000
    );
    assert_eq!(
        parse_usize(WORKER_MAX_CONCURRENT_TASKS_CONFIG, "64").unwrap(),
        64
    );
    assert!(parse_usize(WORKER_MAX_CONCURRENT_TASKS_CONFIG, "0").is_err());
    assert!(parse_duration_ms(TASK_TIMEOUT_CONFIG, "0").is_err());
    assert!(parse_inflight_budget("0").is_err());
    assert!(parse_inflight_budget("nope").is_err());
}

#[test]
fn update_worker_max_concurrent_tasks_resizes_the_pool() {
    let conf = ClusterConf::default();
    let manager = TaskManager::with_rt(Arc::new(Runtime::single()), &conf, "resize-test").unwrap();
    let initial = conf.job.worker_max_concurrent_tasks;

    // Grow: new permits are immediately available.
    let previous = manager
        .update_worker_max_concurrent_tasks(initial + 10)
        .unwrap();
    assert_eq!(previous, initial);
    assert_eq!(manager.available_worker_task_permits(), initial + 10);

    // Shrink: idle surplus permits retire immediately.
    manager.update_worker_max_concurrent_tasks(initial).unwrap();
    assert_eq!(manager.available_worker_task_permits(), initial);
    assert_eq!(manager.worker_max_concurrent_tasks(), initial);
}

#[test]
fn runtime_config_endpoint_reads_the_current_values() {
    let handler = test_handler();
    let task_manager = test_task_manager(&handler);
    let app = handler.router();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let current = rt.block_on(async {
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/worker/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        response_json(response).await
    });
    assert_eq!(
        current[TRANSFER_INFLIGHT_BUDGET_CONFIG],
        task_manager.transfer_inflight_budget_bytes()
    );
    assert_eq!(
        current[WORKER_MAX_CONCURRENT_TASKS_CONFIG],
        task_manager.worker_max_concurrent_tasks()
    );
}

#[test]
fn runtime_config_endpoint_updates_each_writable_config() {
    let handler = test_handler();
    let task_manager = test_task_manager(&handler);
    let app = handler.router();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    rt.block_on(async {
        let update = |config: &str, value: &str| {
            let uri = format!("/api/update_config?{}={}", config, value);
            app.clone().oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
        };

        // Byte budget (StarRocks-style size form).
        let response = update(TRANSFER_INFLIGHT_BUDGET_CONFIG, "100GB")
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["value"], 100 * 1024 * 1024 * 1024_i64);
        assert_eq!(
            body["previous_value"],
            TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES
        );

        // Concurrency.
        let response = update(WORKER_MAX_CONCURRENT_TASKS_CONFIG, "7")
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response_json(response).await["value"], 7);
        assert_eq!(task_manager.worker_max_concurrent_tasks(), 7);
        assert_eq!(task_manager.available_worker_task_permits(), 7);

        // Durations accept duration-unit strings and return milliseconds.
        let response = update(TASK_TIMEOUT_CONFIG, "30m").await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response_json(response).await["value"], 30 * 60 * 1000_u64);
        assert_eq!(task_manager.task_timeout_ms(), 30 * 60 * 1000);

        let response = update(TASK_REPORT_INTERVAL_CONFIG, "5s")
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response_json(response).await["value"], 5_000_u64);
        assert_eq!(task_manager.task_report_interval_ms(), 5_000);

        // GET reflects all updates.
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/worker/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        let current = response_json(response).await;
        assert_eq!(current[WORKER_MAX_CONCURRENT_TASKS_CONFIG], 7);
        assert_eq!(current[TASK_TIMEOUT_CONFIG], "1800000ms");
    });
}

#[test]
fn runtime_config_endpoint_rejects_unknown_or_invalid_updates() {
    let handler = test_handler();
    let task_manager = test_task_manager(&handler);
    let app = handler.router();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    rt.block_on(async {
        for uri in [
            "/api/update_config?unrelated=100GB",
            "/api/update_config?transfer_inflight_budget_bytes=100GB&unrelated=value",
            "/api/update_config?transfer_inflight_budget_bytes=100GB&transfer_inflight_budget_bytes=200GB",
            "/api/update_config?transfer_inflight_budget_bytes=0",
            "/api/update_config?worker_max_concurrent_tasks=0",
            "/api/update_config?worker_max_concurrent_tasks=abc",
            "/api/update_config?task_timeout=0",
            "/api/update_config?task_timeout=soon",
            "/api/update_config?task_report_interval=0",
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY, "{}", uri);
        }

        // Nothing changed.
        assert_eq!(
            task_manager.transfer_inflight_budget_bytes(),
            TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES
        );
        assert_eq!(
            task_manager.worker_max_concurrent_tasks(),
            ClusterConf::default().job.worker_max_concurrent_tasks
        );
    });
}
