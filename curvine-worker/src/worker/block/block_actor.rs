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

use crate::worker::block::{BlockMeta, BlockStore, HeartbeatTask, MasterClient};
use curvine_client_core::file::FsContext;
use curvine_config::ClusterConf;
use curvine_core_error::{CommonError, CommonResult};
use curvine_error::FsError;
use curvine_model::ProtoUtils;
use curvine_model::{BlockReportInfo, HeartbeatStatus, WorkerAddress};
use curvine_proto::BlockReportListResponse;
use curvine_rpc::server::ServerState;
use curvine_runtime::common::TimeSpent;
use curvine_runtime::runtime::ScheduledExecutor;
use curvine_runtime::runtime::{GroupExecutor, Runtime};
use curvine_runtime::sync::StateCtl;
use dashmap::{DashMap, DashSet};
use log::{info, warn};
use std::mem::size_of_val;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Attempts per full block report batch, including the first one.
///
/// Each attempt is a full connector RPC that retries internally, so with the
/// default client config (120s rpc timeout + 120s retry window) a sustained
/// failure takes about `3 * 240s + 3s` (~12 min) before startup fails.
const FULL_REPORT_MAX_ATTEMPTS: u32 = 3;
/// Backoff before the first retry; doubled on every further retry.
const FULL_REPORT_RETRY_BASE: Duration = Duration::from_secs(1);
/// Upper bound of the backoff between two attempts.
const FULL_REPORT_RETRY_MAX: Duration = Duration::from_secs(10);

/// Id shared by every batch of one full report round, so the master can tell a
/// resent batch from the start of a new round. Never 0, which means "no id".
fn new_report_id() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1);
    nanos.max(1)
}

/// Whether a failed full block report batch may succeed when sent again.
///
/// The cluster connector already retries timeouts and master switches itself.
/// When that retry budget runs out it surfaces as `FsError::IO`, which is the
/// case that reaches here in practice (the `Timeout` and `NotLeaderMaster`
/// variants are kept for completeness). `FsError::IO` can also be an IO error
/// returned by the master itself. That is how the master asks for a resend while
/// it is still processing the completing batch of the same round; other master
/// IO failures are retried needlessly but only a bounded number of times.
/// Anything else is a decision made by the master, so startup should fail.
fn is_retryable_report_error(err: &CommonError) -> bool {
    matches!(
        err.downcast_ref::<FsError>(),
        Some(FsError::Timeout(_) | FsError::IO(_) | FsError::NotLeaderMaster(_))
    )
}

/// Run one full block report RPC and resend it on transient errors.
///
/// The master accumulates a full report by `worker_id` and `report_id`,
/// de-duplicates block ids and remembers the last completed `report_id`, so
/// resending any batch of the same round is safe.
/// Gives up early once `is_stopped` reports that the worker is shutting down.
fn send_with_retry<T>(
    what: &str,
    is_stopped: impl Fn() -> bool,
    mut sleep: impl FnMut(Duration),
    mut send: impl FnMut() -> CommonResult<T>,
) -> CommonResult<T> {
    let mut attempt = 1;
    let mut backoff = FULL_REPORT_RETRY_BASE;
    loop {
        let err = match send() {
            Ok(v) => return Ok(v),
            Err(e) => e,
        };

        if attempt >= FULL_REPORT_MAX_ATTEMPTS || !is_retryable_report_error(&err) || is_stopped() {
            return Err(err);
        }

        warn!(
            "{} failed (attempt {}/{}), retry in {} ms: {}",
            what,
            attempt,
            FULL_REPORT_MAX_ATTEMPTS,
            backoff.as_millis(),
            err
        );
        sleep(backoff);
        // The worker may have started shutting down while we were waiting.
        if is_stopped() {
            return Err(err);
        }
        backoff = (backoff * 2).min(FULL_REPORT_RETRY_MAX);
        attempt += 1;
    }
}

/// Worker block management role.
/// 1. Register worker with master
/// 2. Report block information to the master
/// 3. Accept the master's instructions and delete the block data.
#[derive(Clone)]
pub struct BlockActor {
    pub(crate) client: MasterClient,
    store: BlockStore,
    executor: Arc<GroupExecutor>,
    heartbeat_interval_ms: u64,
    worker_ctl: StateCtl,
    block_report_limit: usize,

    // Block that needs to be reported when the heartbeats.
    // Includes the following situations:
    // 1. Block file deletion report.
    // 2. Add a new block.
    report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
    pending_deletes: Arc<DashSet<i64>>,
}

impl BlockActor {
    pub fn new(
        rt: Arc<Runtime>,
        conf: &ClusterConf,
        worker_addr: WorkerAddress,
        worker_session_id: String,
        store: BlockStore,
        worker_ctl: StateCtl,
        startup_time_ms: u64,
    ) -> CommonResult<BlockActor> {
        let context = FsContext::with_rt(conf.clone(), rt)?;
        let context = Arc::new(context);
        let client = MasterClient::new(
            context.clone(),
            store.cluster_id()?,
            store.worker_id()?,
            worker_addr,
            conf.worker.weight,
            worker_session_id,
            startup_time_ms,
        );
        let executor = GroupExecutor::new(
            "worker-block-executor",
            conf.worker.executor_threads,
            conf.worker.executor_channel_size,
        );
        let heartbeat_interval_ms = conf.master.heartbeat_interval_ms();
        let block_report_limit = conf.master.block_report_limit;
        Ok(Self {
            client,
            store,
            executor: Arc::new(executor),
            heartbeat_interval_ms,
            worker_ctl,
            block_report_limit,
            report_blocks: Arc::new(DashMap::new()),
            pending_deletes: Arc::new(DashSet::new()),
        })
    }

    pub fn start(self) -> CommonResult<()> {
        info!("start block actor");

        self.register()?;
        info!("worker register success");

        let spend = TimeSpent::new();
        let total_len = self.full_block_report()?;
        info!(
            "worker block report success, total blocks {}, used {} ms",
            total_len,
            spend.used_ms()
        );

        Self::start_heartbeat(
            self.executor.clone(),
            self.worker_ctl.clone(),
            self.client.clone(),
            self.store.clone(),
            self.report_blocks.clone(),
            self.pending_deletes.clone(),
            self.heartbeat_interval_ms,
        )?;
        Ok(())
    }

    // Worker registration.
    pub fn register(&self) -> CommonResult<()> {
        let storages_info = self.store.get_and_check_storages()?;
        let result = self.client.heartbeat(HeartbeatStatus::Start, storages_info);
        result?;
        Ok(())
    }

    pub fn full_block_report(&self) -> CommonResult<usize> {
        let blocks = self.store.all_blocks()?;
        let report_id = new_report_id();
        if blocks.is_empty() {
            let response = self.send_full_report(report_id, 0, &[])?;
            let cmds = ProtoUtils::worker_cmd_from_pb(response.cmds);
            HeartbeatTask::delete_block_task(
                self.executor.clone(),
                self.store.clone(),
                cmds,
                self.report_blocks.clone(),
                self.pending_deletes.clone(),
            );
            return Ok(0);
        }

        let mut off = 0;
        while off < blocks.len() {
            let end = (off + self.block_report_limit).min(blocks.len());
            let batch = &blocks[off..end];
            let spend = TimeSpent::new();
            let response = self.send_full_report(report_id, blocks.len(), batch)?;
            info!(
                "full block report, blocks={}, msg_bytes={}, used_ms={}",
                batch.len(),
                size_of_val(batch),
                spend.used_ms()
            );
            let cmds = ProtoUtils::worker_cmd_from_pb(response.cmds);
            HeartbeatTask::delete_block_task(
                self.executor.clone(),
                self.store.clone(),
                cmds,
                self.report_blocks.clone(),
                self.pending_deletes.clone(),
            );
            off = end;
        }

        Ok(blocks.len())
    }

    fn send_full_report(
        &self,
        report_id: u64,
        total_len: usize,
        batch: &[BlockMeta],
    ) -> CommonResult<BlockReportListResponse> {
        send_with_retry(
            "full block report",
            || self.worker_ctl.state::<ServerState>() == ServerState::Stop,
            std::thread::sleep,
            || self.client.full_block_report(report_id, total_len, batch),
        )
    }

    pub fn start_heartbeat(
        executor: Arc<GroupExecutor>,
        worker_ctl: StateCtl,
        client: MasterClient,
        store: BlockStore,
        report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
        pending_deletes: Arc<DashSet<i64>>,
        heartbeat_interval_ms: u64,
    ) -> CommonResult<()> {
        let scheduler = ScheduledExecutor::new("worker-heartbeat", heartbeat_interval_ms);

        let task = HeartbeatTask {
            executor,
            worker_ctl,
            client,
            store,
            report_blocks,
            pending_deletes,
        };

        scheduler.start(task)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};
    use std::io;

    fn io_error() -> CommonError {
        CommonError::from(FsError::from(io::Error::from(io::ErrorKind::TimedOut)))
    }

    fn master_error() -> CommonError {
        CommonError::from(FsError::common("rejected by master"))
    }

    #[test]
    fn retries_transient_error_until_success() {
        let calls = Cell::new(0);
        let waits = RefCell::new(vec![]);

        let res = send_with_retry(
            "test",
            || false,
            |d| waits.borrow_mut().push(d),
            || {
                calls.set(calls.get() + 1);
                if calls.get() < 3 {
                    Err(io_error())
                } else {
                    Ok(calls.get())
                }
            },
        );

        assert_eq!(res.unwrap(), 3);
        assert_eq!(
            *waits.borrow(),
            vec![Duration::from_secs(1), Duration::from_secs(2)]
        );
    }

    #[test]
    fn gives_up_after_max_attempts() {
        let calls = Cell::new(0);
        let waits = RefCell::new(vec![]);

        let res: CommonResult<()> = send_with_retry(
            "test",
            || false,
            |d| waits.borrow_mut().push(d),
            || {
                calls.set(calls.get() + 1);
                Err(io_error())
            },
        );

        assert!(res.is_err());
        assert_eq!(calls.get(), FULL_REPORT_MAX_ATTEMPTS);
        assert_eq!(waits.borrow().len() as u32, FULL_REPORT_MAX_ATTEMPTS - 1);
        assert!(waits.borrow().iter().all(|d| *d <= FULL_REPORT_RETRY_MAX));
    }

    #[test]
    fn does_not_retry_permanent_error() {
        let calls = Cell::new(0);

        let res: CommonResult<()> = send_with_retry(
            "test",
            || false,
            |_| panic!("must not wait for a permanent error"),
            || {
                calls.set(calls.get() + 1);
                Err(master_error())
            },
        );

        assert!(res.is_err());
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn stops_retrying_when_worker_is_shutting_down() {
        let calls = Cell::new(0);

        let res: CommonResult<()> = send_with_retry(
            "test",
            || true,
            |_| panic!("must not wait while shutting down"),
            || {
                calls.set(calls.get() + 1);
                Err(io_error())
            },
        );

        assert!(res.is_err());
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn stops_when_worker_shuts_down_during_backoff() {
        let calls = Cell::new(0);
        let stopped = Cell::new(false);

        let res: CommonResult<()> = send_with_retry(
            "test",
            || stopped.get(),
            |_| stopped.set(true),
            || {
                calls.set(calls.get() + 1);
                Err(io_error())
            },
        );

        assert!(res.is_err());
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn report_ids_are_nonzero_and_change_between_rounds() {
        let first = new_report_id();
        std::thread::sleep(Duration::from_millis(1));
        let second = new_report_id();
        assert_ne!(first, 0);
        assert_ne!(first, second);
    }

    #[test]
    fn classifies_retryable_errors() {
        assert!(is_retryable_report_error(&io_error()));
        assert!(is_retryable_report_error(&CommonError::from(
            FsError::not_leader("master switched")
        )));
        assert!(!is_retryable_report_error(&master_error()));
        assert!(!is_retryable_report_error(&CommonError::from("plain")));
    }
}
