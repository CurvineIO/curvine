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
use curvine_client::rpc::TransferClient;
use curvine_client::unified::UnifiedFileSystem;
use curvine_core_error::err_box;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::Path;
use curvine_model::{TransferCommand, TransferKind, TransferState};
use curvine_proto::{GetTransferStatusResponse, SubmitTransferResponse};
use curvine_runtime::common::TimeSpent;
use log::info;
use std::time::Duration;
use tokio::time;

const SUBMITTER: &str = "curvine-sdk";

pub(crate) fn validate_transfer_command(kind: TransferKind, replicas: Option<i32>) -> FsResult<()> {
    if let Some(replicas) = replicas {
        if kind != TransferKind::Load {
            return err_box!("export does not support replicas");
        }
        if replicas <= 0 {
            return err_box!("load replicas must be greater than zero");
        }
    }
    Ok(())
}

pub(crate) fn infer_transfer_paths(input: Path, peer: Path, kind: TransferKind) -> (Path, Path) {
    match kind {
        TransferKind::Load if input.is_cv() => (peer, input),
        TransferKind::Load | TransferKind::Export => (input, peer),
    }
}

pub(crate) async fn resolve_transfer_paths(
    fs: &UnifiedFileSystem,
    source_path: &str,
    target_path: Option<&str>,
    kind: TransferKind,
) -> FsResult<(String, String)> {
    let input = Path::from_str(source_path)?;
    let (source, target) = if let Some(target_path) = target_path {
        (input, Path::from_str(target_path)?)
    } else {
        let peer = match fs.toggle_path(&input, true).await? {
            Some(peer) => peer,
            None => return err_box!("{} is not mounted", source_path),
        };
        infer_transfer_paths(input, peer, kind)
    };
    match kind {
        TransferKind::Load if !source.is_cv() && target.is_cv() => {}
        TransferKind::Export if source.is_cv() && !target.is_cv() => {}
        TransferKind::Load => {
            return err_box!(
                "load requires a UFS source and Curvine target: {} -> {}",
                source.full_path(),
                target.full_path()
            )
        }
        TransferKind::Export => {
            return err_box!(
                "export requires a Curvine source and UFS target: {} -> {}",
                source.full_path(),
                target.full_path()
            )
        }
    }
    Ok((source.clone_uri(), target.clone_uri()))
}

pub(crate) fn build_transfer_command(
    kind: TransferKind,
    source_path: String,
    target_path: String,
    overwrite: bool,
    replicas: Option<i32>,
) -> TransferCommand {
    let mut command = TransferCommand {
        kind,
        source_path: source_path.clone(),
        target_path: target_path.clone(),
        client_request_id: TransferCommand::default_client_request_id_with_overwrite_and_replicas(
            kind,
            &source_path,
            &target_path,
            overwrite,
            replicas,
        ),
        submitter: SUBMITTER.to_string(),
        tenant: String::new(),
        options: Default::default(),
    };
    command.set_overwrite(overwrite);
    if let Some(replicas) = replicas {
        command.set_replicas(replicas);
    }
    command
}

fn transfer_client(session: &Session) -> FsResult<TransferClient> {
    TransferClient::with_context(session.unified().fs_context())
}

pub(crate) async fn submit_transfer(
    session: &Session,
    kind: TransferKind,
    source_path: &str,
    target_path: Option<&str>,
    overwrite: bool,
    replicas: Option<i32>,
) -> FsResult<SubmitTransferResponse> {
    validate_transfer_command(kind, replicas)?;
    let (source_path, target_path) =
        resolve_transfer_paths(session.unified(), source_path, target_path, kind).await?;
    let command = build_transfer_command(kind, source_path, target_path, overwrite, replicas);
    transfer_client(session)?.submit(command).await
}

pub(crate) async fn submit_load(
    session: &Session,
    source_path: &str,
    target_path: Option<&str>,
    overwrite: bool,
    replicas: Option<i32>,
) -> FsResult<SubmitTransferResponse> {
    submit_transfer(
        session,
        TransferKind::Load,
        source_path,
        target_path,
        overwrite,
        replicas,
    )
    .await
}

pub(crate) async fn submit_export(
    session: &Session,
    source_path: &str,
    overwrite: bool,
) -> FsResult<SubmitTransferResponse> {
    submit_transfer(
        session,
        TransferKind::Export,
        source_path,
        None,
        overwrite,
        None,
    )
    .await
}

pub(crate) async fn get_transfer_status(
    session: &Session,
    job_id: impl AsRef<str>,
) -> FsResult<GetTransferStatusResponse> {
    transfer_client(session)?.status(job_id).await
}

pub(crate) async fn cancel_transfer(session: &Session, job_id: impl AsRef<str>) -> FsResult<()> {
    let _ = transfer_client(session)?.cancel(job_id, None).await?;
    Ok(())
}

pub(crate) async fn retry_transfer(session: &Session, job_id: impl AsRef<str>) -> FsResult<String> {
    Ok(transfer_client(session)?.retry(job_id).await?.job_id)
}

pub(crate) async fn wait_transfer_complete(
    session: &Session,
    job_id: impl AsRef<str>,
    fail_if_not_found: bool,
) -> FsResult<()> {
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

    #[test]
    fn allows_replicas_on_load_and_rejects_them_on_export() {
        assert!(validate_transfer_command(TransferKind::Load, None).is_ok());
        assert!(validate_transfer_command(TransferKind::Load, Some(2)).is_ok());

        let err = validate_transfer_command(TransferKind::Load, Some(0)).unwrap_err();
        assert!(err.to_string().contains("greater than zero"));

        let err = validate_transfer_command(TransferKind::Export, Some(2)).unwrap_err();
        assert!(err.to_string().contains("does not support replicas"));
    }

    #[test]
    fn infers_export_paths_without_reversing_the_cv_source() {
        let cv_path = Path::from_str("/models/v1").unwrap();
        let ufs_path = Path::from_str("s3://bucket/models/v1").unwrap();

        let (load_source, load_target) =
            infer_transfer_paths(cv_path.clone(), ufs_path.clone(), TransferKind::Load);
        assert_eq!(load_source.clone_uri(), ufs_path.clone_uri());
        assert_eq!(load_target.clone_uri(), cv_path.clone_uri());

        let (export_source, export_target) =
            infer_transfer_paths(cv_path.clone(), ufs_path.clone(), TransferKind::Export);
        assert!(export_source.is_cv());
        assert!(!export_target.is_cv());
        assert_eq!(export_source.clone_uri(), cv_path.clone_uri());
        assert_eq!(export_target.clone_uri(), ufs_path.clone_uri());
    }

    #[test]
    fn builds_overwrite_and_replicas_into_the_transfer_command() {
        let command = build_transfer_command(
            TransferKind::Load,
            "s3://bucket/a".to_string(),
            "/mnt/a".to_string(),
            false,
            Some(3),
        );
        assert_eq!(command.kind, TransferKind::Load);
        assert_eq!(command.source_path, "s3://bucket/a");
        assert_eq!(command.target_path, "/mnt/a");
        assert_eq!(command.submitter, SUBMITTER);
        assert!(!command.overwrite());
        assert_eq!(command.replicas(), Some(3));
    }
}
