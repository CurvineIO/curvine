use super::*;

#[test]
fn stale_unregister_does_not_remove_new_worker_session() {
    let mut manager = robin_manager();
    let mut worker = worker_with_available(1, 1 << 30);
    worker.worker_session_id = "new-session".to_string();
    let address = worker.address.clone();
    manager.add_test_worker(worker);
    let cluster_id = manager.cluster_id.clone();

    manager
        .heartbeat(
            &cluster_id,
            HeartbeatStatus::End,
            address,
            1,
            "old-session".to_string(),
            TransferWorkerCapabilities::default(),
            0,
            0,
            String::new(),
            0,
            Vec::new(),
            None,
            false,
        )
        .unwrap();

    assert_eq!(
        manager.get_worker(1).unwrap().worker_session_id,
        "new-session"
    );
}

#[test]
fn lost_session_requires_new_start_before_running() {
    let mut manager = robin_manager();
    let mut worker = worker_with_available(1, 1 << 30);
    worker.worker_session_id = "old-session".to_string();
    worker.startup_time_ms = 10;
    let address = worker.address.clone();
    manager.add_test_worker(worker);
    assert!(manager.remove_expired_worker(1).is_some());
    let cluster_id = manager.cluster_id.clone();

    manager
        .heartbeat(
            &cluster_id,
            HeartbeatStatus::Running,
            address.clone(),
            1,
            "old-session".to_string(),
            TransferWorkerCapabilities::default(),
            0,
            0,
            String::new(),
            10,
            Vec::new(),
            None,
            false,
        )
        .unwrap();
    assert!(manager.get_worker(1).is_none());
    assert!(manager.is_lost_worker_session(1, "old-session"));

    let (_, accepted_start) = manager
        .heartbeat(
            &cluster_id,
            HeartbeatStatus::Start,
            address.clone(),
            1,
            "new-session".to_string(),
            TransferWorkerCapabilities::default(),
            0,
            0,
            String::new(),
            20,
            Vec::new(),
            None,
            false,
        )
        .unwrap();
    assert!(accepted_start);
    assert!(!manager.get_worker(1).unwrap().is_live());

    manager
        .heartbeat(
            &cluster_id,
            HeartbeatStatus::Running,
            address,
            1,
            "new-session".to_string(),
            TransferWorkerCapabilities::default(),
            0,
            0,
            String::new(),
            20,
            Vec::new(),
            None,
            false,
        )
        .unwrap();
    assert!(manager.get_worker(1).unwrap().is_live());
}
