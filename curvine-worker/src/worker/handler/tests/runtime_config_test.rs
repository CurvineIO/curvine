use super::*;
use crate::worker::block::MasterRegistration;
use crate::worker::runtime_config::{
    build_worker_runtime_registry, TASK_REPORT_INTERVAL_CONFIG, TASK_TIMEOUT_CONFIG,
    TRANSFER_INFLIGHT_BUDGET_CONFIG, WORKER_MAX_CONCURRENT_TASKS_CONFIG,
};
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use curvine_config::{ClusterConf, ConfigSourceMap};
use curvine_fault::FaultHttpControl;
use curvine_runtime::runtime::Runtime;
use tower::ServiceExt;

fn test_handler() -> WorkerRouterHandler {
    let conf = ClusterConf::default();
    let task_manager = Arc::new(
        TaskManager::with_rt(Arc::new(Runtime::single()), &conf, "runtime-config-test")
            .expect("task manager"),
    );
    let runtime_config = Arc::new(build_worker_runtime_registry(task_manager.clone(), &conf));
    WorkerRouterHandler {
        fault_http: FaultHttpControl::default(),
        profiling_conf: conf.profiling.clone(),
        server_state: Arc::default(),
        task_manager: task_manager.clone(),
        config_view: WorkerRouterHandler::build_config_view(
            &conf,
            ConfigSourceMap::new(),
            runtime_config.clone(),
        ),
        runtime_config,
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

/// Finds the runtime entry for `key` in a full config view body.
fn runtime_entry<'a>(body: &'a Value, key: &str) -> &'a Value {
    body["runtime"]
        .as_array()
        .expect("runtime array")
        .iter()
        .find(|entry| entry["key"] == key)
        .unwrap_or_else(|| panic!("runtime entry missing for {key}"))
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

    // The body is the full view, tagged with the process that answered.
    assert_eq!(current["role"], "worker");
    assert_eq!(current["persistent"], false);
    assert!(current["node"].is_string());
    assert!(current["values"].is_object());
    assert!(current["sources"].is_object());

    // Every worker knob is writable, and the one with a config path reports its
    // effective value through the runtime entry.
    let writable: Vec<&str> = current["writable"]
        .as_array()
        .expect("writable array")
        .iter()
        .map(|value| value.as_str().expect("writable string"))
        .collect();
    for key in [
        TRANSFER_INFLIGHT_BUDGET_CONFIG,
        WORKER_MAX_CONCURRENT_TASKS_CONFIG,
        TASK_TIMEOUT_CONFIG,
        TASK_REPORT_INTERVAL_CONFIG,
    ] {
        assert!(writable.contains(&key), "writable missing {key}");
    }

    assert_eq!(
        runtime_entry(&current, WORKER_MAX_CONCURRENT_TASKS_CONFIG)["effective_value"],
        json!(task_manager.worker_max_concurrent_tasks())
    );
    // Runtime-only knobs have no config path, so they are listed as writable but
    // deliberately absent from both `values` and the runtime table.
    assert!(current["values"]
        .get(TRANSFER_INFLIGHT_BUDGET_CONFIG)
        .is_none());

    // Nothing in this registry is stale: an empty `unresolved` proves every
    // declared path still resolves in the config tree.
    assert_eq!(current["unresolved"], json!([]));

    // The worker-specific scheduler note survives on the widened body.
    assert_eq!(current["scheduler_propagation"], "next worker heartbeat");
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
