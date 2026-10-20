use super::*;
use crate::worker::block::MasterRegistration;
use crate::worker::runtime_config::{
    build_worker_runtime_registry, TASK_REPORT_INTERVAL_CONFIG, TASK_TIMEOUT_CONFIG,
    TRANSFER_INFLIGHT_BUDGET_CONFIG, WORKER_MAX_CONCURRENT_TASKS_CONFIG,
};
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use curvine_config::ClusterConf;
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
        task_manager: task_manager.clone(),
        runtime_config: Arc::new(build_worker_runtime_registry(task_manager, &conf)),
        registration: MasterRegistration::new(),
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

    let cases = [
        (TRANSFER_INFLIGHT_BUDGET_CONFIG, "100GB"),
        (WORKER_MAX_CONCURRENT_TASKS_CONFIG, "32"),
        (TASK_TIMEOUT_CONFIG, "30m"),
        (TASK_REPORT_INTERVAL_CONFIG, "5m"),
    ];

    for (config, value) in cases {
        let uri = format!("/api/update_config?{}={}", config, value);
        let body = rt.block_on(async {
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
            assert_eq!(response.status(), StatusCode::OK);
            response_json(response).await
        });
        assert_eq!(body["config"], config);
        assert_eq!(body["persistent"], false);
        assert!(body.get("previous_value").is_some());
        assert!(body.get("value").is_some());
    }

    assert_eq!(
        task_manager.transfer_inflight_budget_bytes(),
        100 * 1024 * 1024 * 1024
    );
    assert_eq!(task_manager.worker_max_concurrent_tasks(), 32);
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

    let bad_uris = [
        "/api/update_config?unrelated=100GB",
        "/api/update_config?transfer_inflight_budget_bytes=100GB&unrelated=value",
        "/api/update_config?transfer_inflight_budget_bytes=100GB&transfer_inflight_budget_bytes=200GB",
        "/api/update_config?transfer_inflight_budget_bytes=0",
        "/api/update_config?worker_max_concurrent_tasks=0",
        "/api/update_config?worker_max_concurrent_tasks=abc",
        "/api/update_config?task_timeout=0",
        "/api/update_config?task_timeout=soon",
        "/api/update_config?task_report_interval=0",
    ];

    for uri in bad_uris {
        let status = rt.block_on(async {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .expect("response")
                .status()
        });
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "uri {}", uri);
    }

    assert_eq!(
        task_manager.transfer_inflight_budget_bytes(),
        curvine_config::TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES
    );
}
