use super::{admit_inflight_bytes, TaskManager};
use curvine_config::ClusterConf;
use curvine_model::{LoadJobInfo, LoadTaskInfo, MountInfo, StorageType, TtlAction, WorkerAddress};
use curvine_runtime::runtime::Runtime;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

fn load_task(task_id: &str, estimated_size: i64) -> LoadTaskInfo {
    LoadTaskInfo {
        job: LoadJobInfo {
            job_id: "budget-test-job".to_string(),
            source_path: "ufs://bucket/source".to_string(),
            target_path: "cv:///target".to_string(),
            block_size: 4096,
            replicas: 1,
            storage_type: StorageType::default(),
            ttl_ms: 0,
            ttl_action: TtlAction::default(),
            mount_info: MountInfo::default(),
            create_time: 0,
            overwrite: None,
            source_type: Default::default(),
            transfer_job_id: None,
        },
        task_id: task_id.to_string(),
        worker: WorkerAddress::default(),
        source_path: "ufs://bucket/source".to_string(),
        target_path: "cv:///target".to_string(),
        create_time: 0,
        source_read_plan_json: String::new(),
        estimated_size,
        transfer_report: None,
    }
}

#[test]
fn runtime_budget_change_affects_subsequent_admission() {
    let inflight = AtomicI64::new(8);
    assert!(admit_inflight_bytes(&inflight, 10, 3).is_err());
    assert!(admit_inflight_bytes(&inflight, 100, 3).is_ok());
    assert_eq!(inflight.load(Ordering::Acquire), 11);
}

#[test]
fn budget_reduction_blocks_new_work_without_canceling_admitted_work() {
    let inflight = AtomicI64::new(80);
    assert!(admit_inflight_bytes(&inflight, 50, 1).is_err());
    assert_eq!(inflight.load(Ordering::Acquire), 80);
}

#[test]
fn oversize_task_requires_an_idle_worker() {
    let inflight = AtomicI64::new(0);
    assert!(admit_inflight_bytes(&inflight, 100, 150).is_ok());
    assert_eq!(inflight.load(Ordering::Acquire), 150);

    let busy = AtomicI64::new(1);
    assert!(admit_inflight_bytes(&busy, 100, 150).is_err());
    assert_eq!(busy.load(Ordering::Acquire), 1);
}

#[test]
fn admission_does_not_overflow_or_mutate_the_ledger() {
    let inflight = AtomicI64::new(i64::MAX - 1);
    assert!(admit_inflight_bytes(&inflight, i64::MAX, 2).is_err());
    assert_eq!(inflight.load(Ordering::Acquire), i64::MAX - 1);
}

#[test]
fn concurrent_admissions_cannot_exceed_the_budget() {
    let inflight = Arc::new(AtomicI64::new(0));
    std::thread::scope(|scope| {
        let first = inflight.clone();
        let second = inflight.clone();
        let first = scope.spawn(move || admit_inflight_bytes(&first, 10, 6));
        let second = scope.spawn(move || admit_inflight_bytes(&second, 10, 6));
        let results = [first.join().unwrap(), second.join().unwrap()];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    });
    assert_eq!(inflight.load(Ordering::Acquire), 6);
}

#[test]
fn rejected_replacement_preserves_the_existing_task_context() {
    let runtime = Arc::new(Runtime::single());
    let manager = TaskManager::with_rt(runtime, &ClusterConf::default(), "budget-test")
        .expect("task manager");
    let old = manager.tasks.insert(load_task("task", 0));
    manager.inflight_bytes.store(1, Ordering::Release);
    manager
        .update_transfer_inflight_budget_bytes(1)
        .expect("lower budget");

    let result = manager
        .submit_task(load_task("task", 2))
        .expect("capacity response");

    assert!(!result.accepted);
    assert!(result.reject_reason.starts_with("worker_at_capacity"));
    let current = manager.tasks.get("task").expect("existing task");
    assert!(Arc::ptr_eq(&old, &*current));
    assert!(!old.is_cancel());
}
