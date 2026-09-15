use curvine_model::{
    summarize_transfer_tasks, TransferProgress, TransferTaskRecord, TransferTaskState,
};

fn task(
    task_id: &str,
    state: TransferTaskState,
    updated_at: i64,
    message: &str,
) -> TransferTaskRecord {
    TransferTaskRecord {
        job_id: "job".to_string(),
        run_id: 1,
        task_id: task_id.to_string(),
        attempt_id: 1,
        source_path: "/source".to_string(),
        target_path: format!("/target/{task_id}"),
        worker_id: 1,
        worker_session_id: "session".to_string(),
        source_read_plan_json: "{}".to_string(),
        report_target_json: "{}".to_string(),
        state,
        progress: TransferProgress {
            message: message.to_string(),
            ..Default::default()
        },
        retry_count: 0,
        attempt_started_at: updated_at,
        last_report_at: updated_at,
        stale_deadline_at: updated_at,
        updated_at,
    }
}

#[test]
fn task_summary_uses_timestamp_and_task_id_not_input_order_for_messages() {
    let newer = task("task-2", TransferTaskState::Failed, 20, "newer failure");
    let older = task("task-1", TransferTaskState::Failed, 10, "older failure");
    let summary = summarize_transfer_tasks([&newer, &older], 30);
    assert_eq!(
        summary.progress.message,
        "2 transfer task(s) failed: older failure"
    );

    let same_time_later = task("task-2", TransferTaskState::Running, 40, "later message");
    let same_time_first = task("task-1", TransferTaskState::Running, 40, "first message");
    let summary = summarize_transfer_tasks([&same_time_later, &same_time_first], 50);
    assert_eq!(summary.progress.message, "later message");
}
