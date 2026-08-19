// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::core::Session;
use curvine_client::rpc::{JobMasterClient, TransferClient};
use curvine_client::unified::UnifiedFileSystem;
use curvine_core_error::err_box;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::Path;
use curvine_model::{
    JobStatus, JobTaskProgress, JobTaskState, LoadJobCommand, LoadJobResult, TransferCommand,
    TransferKind, TransferState,
};
use curvine_proto::{GetTransferStatusResponse, SubmitTransferResponse};
use curvine_runtime::common::TimeSpent;
use log::info;
use std::time::Duration;
use tokio::time;

const SUBMITTER: &str = "curvine-sdk";

pub(crate) fn transfer_enabled(session: &Session) -> bool {
    session.unified().conf().transfer.enabled
}

pub(crate) fn map_transfer_state(state: TransferState) -> JobTaskState {
    match state {
        TransferState::Pending => JobTaskState::Pending,
        TransferState::Planning
        | TransferState::Dispatching
        | TransferState::Running
        | TransferState::Canceling => JobTaskState::Loading,
        TransferState::Completed => JobTaskState::Completed,
        TransferState::Failed => JobTaskState::Failed,
        TransferState::PartialSuccess => JobTaskState::PartialSuccess,
        TransferState::Canceled => JobTaskState::Canceled,
    }
}

pub(crate) fn validate_transfer_load_command(command: &LoadJobCommand) -> FsResult<()> {
    if command.replicas.is_some()
        || command.block_size.is_some()
        || command.storage_type.is_some()
        || command.ttl_ms.is_some()
        || command.ttl_action.is_some()
    {
        return err_box!(
            "transfer path does not support LoadJob options replicas/block_size/storage_type/ttl; \
             use source_path, target_path, and overwrite only"
        );
    }
    Ok(())
}

pub(crate) fn load_job_result_from_submit(
    response: &SubmitTransferResponse,
    target_path: impl Into<String>,
) -> LoadJobResult {
    LoadJobResult {
        job_id: response.job_id.clone(),
        target_path: target_path.into(),
        state: map_transfer_state(TransferState::from(response.state)),
    }
}

pub(crate) fn job_status_from_transfer(status: GetTransferStatusResponse) -> JobStatus {
    let state = map_transfer_state(TransferState::from(status.state));
    JobStatus {
        job_id: status.job_id,
        state,
        source_path: status.source_path.unwrap_or_default(),
        target_path: status.target_path.unwrap_or_default(),
        progress: JobTaskProgress {
            state,
            loaded_size: status.progress.loaded_size,
            total_size: status.progress.total_size,
            update_time: status.progress.update_time,
            message: status.progress.message,
        },
    }
}

async fn resolve_load_paths(
    fs: &UnifiedFileSystem,
    command: &LoadJobCommand,
) -> FsResult<(String, String)> {
    let input = Path::from_str(&command.source_path)?;
    let (source, target) = if let Some(target_path) = &command.target_path {
        (input, Path::from_str(target_path)?)
    } else {
        let peer = match fs.toggle_path(&input, true).await? {
            Some(peer) => peer,
            None => return err_box!("{} is not mounted", command.source_path),
        };
        if input.is_cv() {
            (peer, input)
        } else {
            (input, peer)
        }
    };
    if source.is_cv() || !target.is_cv() {
        return err_box!(
            "load requires a UFS source and Curvine target: {} -> {}",
            source.full_path(),
            target.full_path()
        );
    }
    Ok((source.clone_uri(), target.clone_uri()))
}

fn build_transfer_command(
    source_path: String,
    target_path: String,
    overwrite: bool,
) -> TransferCommand {
    let mut command = TransferCommand {
        kind: TransferKind::Load,
        source_path: source_path.clone(),
        target_path: target_path.clone(),
        client_request_id: TransferCommand::default_client_request_id_with_overwrite(
            TransferKind::Load,
            &source_path,
            &target_path,
            overwrite,
        ),
        submitter: SUBMITTER.to_string(),
        tenant: String::new(),
        options: Default::default(),
    };
    command.set_overwrite(overwrite);
    command
}

fn transfer_client(session: &Session) -> FsResult<TransferClient> {
    TransferClient::with_context(session.unified().fs_context())
}

pub(crate) async fn submit_load_job(
    session: &Session,
    command: LoadJobCommand,
) -> FsResult<LoadJobResult> {
    if !transfer_enabled(session) {
        return JobMasterClient::new(session.fs_client())
            .submit_load_job(command)
            .await;
    }

    validate_transfer_load_command(&command)?;
    let overwrite = command.overwrite.unwrap_or(true);
    let (source_path, target_path) = resolve_load_paths(session.unified(), &command).await?;
    let transfer_command = build_transfer_command(source_path, target_path.clone(), overwrite);
    let response = transfer_client(session)?.submit(transfer_command).await?;
    Ok(load_job_result_from_submit(&response, target_path))
}

pub(crate) async fn get_job_status(
    session: &Session,
    job_id: impl AsRef<str>,
) -> FsResult<JobStatus> {
    if !transfer_enabled(session) {
        return JobMasterClient::new(session.fs_client())
            .get_job_status(job_id)
            .await;
    }
    let status = transfer_client(session)?.status(job_id).await?;
    Ok(job_status_from_transfer(status))
}

pub(crate) async fn cancel_job(session: &Session, job_id: impl AsRef<str>) -> FsResult<()> {
    if !transfer_enabled(session) {
        return JobMasterClient::new(session.fs_client())
            .cancel_job(job_id)
            .await;
    }
    let _ = transfer_client(session)?.cancel(job_id, None).await?;
    Ok(())
}

pub(crate) async fn wait_job_complete(
    session: &Session,
    job_id: impl AsRef<str>,
    fail_if_not_found: bool,
) -> FsResult<()> {
    if !transfer_enabled(session) {
        return JobMasterClient::new(session.fs_client())
            .wait_job_complete(job_id, fail_if_not_found)
            .await;
    }

    let client_conf = &session.unified().conf().client;
    time::timeout(
        Duration::from_millis(client_conf.max_sync_wait_timeout_ms),
        wait_transfer_complete0(session, job_id.as_ref(), fail_if_not_found),
    )
    .await?
}

async fn wait_transfer_complete0(
    session: &Session,
    job_id: &str,
    fail_if_not_found: bool,
) -> FsResult<()> {
    let client = transfer_client(session)?;
    let client_conf = &session.unified().conf().client;
    let mut ticks = 0_u64;
    let elapsed = TimeSpent::new();

    loop {
        let status = match client.status(job_id).await {
            Ok(status) => status,
            Err(FsError::JobNotFound(_)) if !fail_if_not_found => {
                time::sleep(Duration::from_millis(
                    client_conf.sync_check_interval_min_ms,
                ))
                .await;
                continue;
            }
            Err(err) => return Err(err),
        };
        let state = TransferState::from(status.state);
        match state {
            TransferState::Completed => return Ok(()),
            TransferState::Failed | TransferState::Canceled | TransferState::PartialSuccess => {
                return err_box!(
                    "transfer {} {:?}: {}",
                    status.job_id,
                    state,
                    status.progress.message
                )
            }
            TransferState::Pending
            | TransferState::Planning
            | TransferState::Dispatching
            | TransferState::Running
            | TransferState::Canceling => {
                ticks += 1;
                let sleep_ms = client_conf
                    .sync_check_interval_max_ms
                    .min(client_conf.sync_check_interval_min_ms.saturating_mul(ticks));
                time::sleep(Duration::from_millis(sleep_ms)).await;

                if ticks.is_multiple_of(u64::from(client_conf.sync_check_log_tick)) {
                    info!(
                        "waiting for transfer {} to complete, elapsed: {} ms, loaded_size={}, total_size={}",
                        status.job_id,
                        elapsed.used_ms(),
                        status.progress.loaded_size,
                        status.progress.total_size
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::{StorageType, TtlAction};
    use curvine_proto::TransferProgressProto;

    #[test]
    fn maps_transfer_states_to_job_task_states() {
        assert_eq!(
            map_transfer_state(TransferState::Pending),
            JobTaskState::Pending
        );
        assert_eq!(
            map_transfer_state(TransferState::Planning),
            JobTaskState::Loading
        );
        assert_eq!(
            map_transfer_state(TransferState::Dispatching),
            JobTaskState::Loading
        );
        assert_eq!(
            map_transfer_state(TransferState::Running),
            JobTaskState::Loading
        );
        assert_eq!(
            map_transfer_state(TransferState::Canceling),
            JobTaskState::Loading
        );
        assert_eq!(
            map_transfer_state(TransferState::Completed),
            JobTaskState::Completed
        );
        assert_eq!(
            map_transfer_state(TransferState::Failed),
            JobTaskState::Failed
        );
        assert_eq!(
            map_transfer_state(TransferState::Canceled),
            JobTaskState::Canceled
        );
        assert_eq!(
            map_transfer_state(TransferState::PartialSuccess),
            JobTaskState::PartialSuccess
        );
    }

    #[test]
    fn rejects_unsupported_transfer_load_options() {
        let command = LoadJobCommand::builder("s3://bucket/a").replicas(2).build();
        let err = validate_transfer_load_command(&command).unwrap_err();
        assert!(err.to_string().contains("does not support"));

        let command = LoadJobCommand::builder("s3://bucket/a")
            .block_size(1024)
            .build();
        assert!(validate_transfer_load_command(&command).is_err());

        let command = LoadJobCommand::builder("s3://bucket/a")
            .storage_type(StorageType::Disk)
            .build();
        assert!(validate_transfer_load_command(&command).is_err());

        let command = LoadJobCommand::builder("s3://bucket/a")
            .ttl_ms(1000)
            .ttl_action(TtlAction::Delete)
            .build();
        assert!(validate_transfer_load_command(&command).is_err());

        let command = LoadJobCommand::builder("s3://bucket/a")
            .target_path("/mnt/a")
            .overwrite(false)
            .build();
        assert!(validate_transfer_load_command(&command).is_ok());
    }

    #[test]
    fn maps_submit_and_status_responses() {
        let submit = SubmitTransferResponse {
            job_id: "job-1".to_string(),
            run_id: 7,
            state: TransferState::Pending as i32,
        };
        let result = load_job_result_from_submit(&submit, "/mnt/a");
        assert_eq!(result.job_id, "job-1");
        assert_eq!(result.target_path, "/mnt/a");
        assert_eq!(result.state, JobTaskState::Pending);

        let status = GetTransferStatusResponse {
            job_id: "job-1".to_string(),
            run_id: 7,
            state: TransferState::Running as i32,
            progress: TransferProgressProto {
                loaded_size: 10,
                total_size: 100,
                update_time: 123,
                message: "loading".to_string(),
            },
            tasks: vec![],
            next_page_token: None,
            kind: None,
            source_path: Some("s3://bucket/a".to_string()),
            target_path: Some("/mnt/a".to_string()),
            submitter: None,
            tenant: None,
            created_at: None,
            updated_at: None,
            owner: None,
            lease_epoch: None,
            lease_expire_at: None,
            cv_metadata_epoch: None,
            task_summary: None,
        };
        let job_status = job_status_from_transfer(status);
        assert_eq!(job_status.state, JobTaskState::Loading);
        assert_eq!(job_status.source_path, "s3://bucket/a");
        assert_eq!(job_status.target_path, "/mnt/a");
        assert_eq!(job_status.progress.loaded_size, 10);
        assert_eq!(job_status.progress.total_size, 100);
        assert_eq!(job_status.progress.message, "loading");
        assert_eq!(job_status.progress.state, JobTaskState::Loading);
    }
}
