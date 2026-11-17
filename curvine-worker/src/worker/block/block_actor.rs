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

use crate::worker::block::{
    BlockStore, ChecksumScan, HeartbeatTask, MasterClient, MasterClientConfig,
};
use curvine_client_core::file::FsContext;
use curvine_config::ClusterConf;
use curvine_core_error::{CommonError, CommonResult};
use curvine_error::FsError;
use curvine_model::{BlockReportInfo, HeartbeatStatus, WorkerAddress};
use curvine_rpc::server::ServerState;
use curvine_runtime::common::TimeSpent;
use curvine_runtime::runtime::ScheduledExecutor;
use curvine_runtime::runtime::{GroupExecutor, Runtime};
use curvine_runtime::sync::StateCtl;
use dashmap::{DashMap, DashSet};
use log::{info, warn};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) struct BlockActorConfig {
    pub(crate) worker_addr: WorkerAddress,
    pub(crate) worker_session_id: String,
    pub(crate) worker_ctl: StateCtl,
    pub(crate) startup_time_ms: u64,
    pub(crate) report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
}

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

/// Whole Start + full-report attempts when registration is lost during startup.
/// This is separate from transport retries for an individual report batch.
const STARTUP_REGISTRATION_MAX_ATTEMPTS: u32 = 3;
const STARTUP_REGISTRATION_RETRY_BASE: Duration = Duration::from_secs(1);

/// Id shared by every batch of one full report round, so the master can tell a
/// resent batch from the start of a new round. Never 0, which means "no id".
pub(super) fn new_report_id() -> u64 {
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
/// Other errors leave the batch retry loop. Registration loss is handled by
/// the outer startup sequence, which must repeat Start before sending reports.
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
pub(super) fn send_with_retry<T>(
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

/// Registration loss needs a fresh Start and inventory, not another attempt
/// at the same batch. Match the existing wire contract narrowly; other
/// InProgress errors must retain their original retry behavior.
fn is_registration_lost(err: &CommonError) -> bool {
    err.downcast_ref::<FsError>().is_some_and(|error| {
        matches!(error, FsError::InProgress(_)) && error.to_string().starts_with("NotRegistered:")
    })
}

fn register_and_report_with_recovery<T>(
    is_stopped: impl Fn() -> bool,
    mut sleep: impl FnMut(Duration),
    mut register: impl FnMut() -> CommonResult<()>,
    mut full_report: impl FnMut() -> CommonResult<T>,
) -> CommonResult<T> {
    let check_running = || -> CommonResult<()> {
        if is_stopped() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "worker stopped during registration",
            )
            .into());
        }
        Ok(())
    };
    let mut backoff = STARTUP_REGISTRATION_RETRY_BASE;
    let mut attempt = 1;
    loop {
        check_running()?;
        register()?;
        check_running()?;
        let error = match full_report() {
            Ok(value) => {
                check_running()?;
                return Ok(value);
            }
            Err(error) => error,
        };
        if !is_registration_lost(&error)
            || attempt >= STARTUP_REGISTRATION_MAX_ATTEMPTS
            || is_stopped()
        {
            return Err(error);
        }
        warn!(
            "worker registration lost during full report (attempt {}/{}); repeating Start and full report in {} ms: {}",
            attempt, STARTUP_REGISTRATION_MAX_ATTEMPTS, backoff.as_millis(), error
        );
        sleep(backoff);
        if is_stopped() {
            return Err(error);
        }
        backoff *= 2;
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
    /// Startup checksum scan rate in bytes per second (0 is unlimited), or
    /// None when the scan is off.
    checksum_scan_rate: Option<u64>,

    // Block that needs to be reported when the heartbeats.
    // Includes the following situations:
    // 1. Block file deletion report.
    // 2. Add a new block.
    report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
    pending_deletes: Arc<DashSet<i64>>,
}

impl BlockActor {
    pub(crate) fn new(
        rt: Arc<Runtime>,
        conf: &ClusterConf,
        store: BlockStore,
        config: BlockActorConfig,
    ) -> CommonResult<BlockActor> {
        let BlockActorConfig {
            worker_addr,
            worker_session_id,
            worker_ctl,
            startup_time_ms,
            report_blocks,
        } = config;
        let context = FsContext::with_rt(conf.clone(), rt.clone())?;
        let context = Arc::new(context);
        let client = MasterClient::new(
            context.clone(),
            MasterClientConfig {
                cluster_id: store.cluster_id()?,
                worker_id: store.worker_id()?,
                worker_addr,
                worker_weight: conf.worker.weight,
                worker_session_id,
                startup_time_ms,
                worker_conf: conf.worker.clone(),
            },
        )?;
        let executor = GroupExecutor::new(
            "worker-block-executor",
            conf.worker.executor_threads,
            conf.worker.executor_channel_size,
        );
        let heartbeat_interval_ms = conf.master.heartbeat_interval_ms();
        let block_report_limit = conf.master.block_report_limit;
        let checksum_scan_rate = if conf.worker.checksum_scan_enabled() {
            Some(conf.worker.checksum_scan_bytes_per_sec()?)
        } else {
            None
        };
        Ok(Self {
            client,
            store,
            executor: Arc::new(executor),
            heartbeat_interval_ms,
            worker_ctl,
            block_report_limit,
            report_blocks,
            checksum_scan_rate,
            pending_deletes: Arc::new(DashSet::new()),
        })
    }

    pub fn start(self) -> CommonResult<()> {
        info!("start block actor");

        let spend = TimeSpent::new();
        let total_len = register_and_report_with_recovery(
            || self.worker_ctl.state::<ServerState>() == ServerState::Stop,
            std::thread::sleep,
            || self.register(),
            // Each call rebuilds the inventory with a fresh report id. A new
            // leader may have none of the previously accepted report chunks.
            || self.full_block_report(),
        )?;
        info!(
            "worker registration and full report success, total blocks {}, used {} ms",
            total_len,
            spend.used_ms()
        );

        Self::start_heartbeat(self.heartbeat_task(), self.heartbeat_interval_ms)?;

        if let Some(rate) = self.checksum_scan_rate {
            self.start_checksum_scan(rate)?;
        }
        Ok(())
    }

    /// Verify the loaded blocks in the background. It starts after the full
    /// report so the corrupted deletions it causes find master's locations.
    fn start_checksum_scan(&self, rate: u64) -> CommonResult<()> {
        let scan = ChecksumScan::new(self.store.clone(), rate);
        let worker_ctl = self.worker_ctl.clone();
        std::thread::Builder::new()
            .name("worker-checksum-scan".to_string())
            .spawn(move || {
                let stopped = || worker_ctl.state::<ServerState>() == ServerState::Stop;
                if let Err(e) = scan.run(stopped) {
                    warn!("block checksum scan failed: {}", e);
                }
            })?;
        Ok(())
    }

    // Worker registration.
    pub fn register(&self) -> CommonResult<()> {
        let storages_info = self.store.get_and_check_storages()?;
        let result = self.client.heartbeat(HeartbeatStatus::Start, storages_info);
        result?;
        Ok(())
    }

    fn heartbeat_task(&self) -> HeartbeatTask {
        HeartbeatTask {
            capacity_heartbeat: Default::default(),
            executor: self.executor.clone(),
            worker_ctl: self.worker_ctl.clone(),
            client: self.client.clone(),
            store: self.store.clone(),
            report_blocks: self.report_blocks.clone(),
            pending_deletes: self.pending_deletes.clone(),
            block_report_limit: self.block_report_limit,
        }
    }

    pub fn full_block_report(&self) -> CommonResult<usize> {
        self.heartbeat_task().full_block_report()
    }

    pub(crate) fn start_heartbeat(
        task: HeartbeatTask,
        heartbeat_interval_ms: u64,
    ) -> CommonResult<()> {
        let scheduler = ScheduledExecutor::new("worker-heartbeat", heartbeat_interval_ms);
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

    fn registration_lost() -> CommonError {
        // Exercise the same typed error after the wire round trip as the RPC client.
        let error = FsError::in_progress_msg("NotRegistered: worker must register its session");
        use curvine_core_error::ErrorExt;
        FsError::decode(error.encode()).into()
    }

    #[test]
    fn startup_recovers_when_leader_changes_before_or_during_full_report() {
        #[derive(Default)]
        struct MasterState {
            session: Option<&'static str>,
            report_id: Option<u64>,
            blocks: Vec<i64>,
        }
        // Two independent Master registration/inventory views with mirroring
        // disabled. Switch immediately after Start, or after the first chunk.
        for switch_before_chunk in [0, 1] {
            let masters = RefCell::new([MasterState::default(), MasterState::default()]);
            let leader = Cell::new(0);
            let starts = RefCell::new(vec![]);
            let reports = RefCell::new(vec![]);
            let waits = RefCell::new(vec![]);
            let round = Cell::new(0);
            let result = register_and_report_with_recovery(
                || false,
                |delay| waits.borrow_mut().push(delay),
                || {
                    let target = leader.get();
                    starts.borrow_mut().push(target);
                    masters.borrow_mut()[target].session = Some("same-worker-process");
                    Ok(())
                },
                || {
                    // As in full_block_report(), every invocation starts a
                    // new inventory round and sends it from the first chunk.
                    let report_id = round.get() + 1;
                    round.set(report_id);
                    for (chunk, block) in [11, 22, 33].into_iter().enumerate() {
                        if report_id == 1 && chunk == switch_before_chunk {
                            leader.set(1);
                        }
                        let target = leader.get();
                        reports.borrow_mut().push((target, report_id, chunk));
                        let mut states = masters.borrow_mut();
                        let master = &mut states[target];
                        if master.session != Some("same-worker-process") {
                            return Err(registration_lost());
                        }
                        if master.report_id != Some(report_id) {
                            master.report_id = Some(report_id);
                            master.blocks.clear();
                        }
                        master.blocks.push(block);
                    }
                    Ok(3)
                },
            );
            assert_eq!(
                result.unwrap(),
                3,
                "startup must reach heartbeat scheduling"
            );
            assert_eq!(*starts.borrow(), vec![0, 1]);
            assert_eq!(*waits.borrow(), vec![Duration::from_secs(1)]);
            let states = masters.borrow();
            assert_eq!(states[0].session, states[1].session);
            assert_eq!(states[0].blocks.len(), switch_before_chunk);
            assert_eq!(states[1].report_id, Some(2));
            assert_eq!(states[1].blocks, vec![11, 22, 33]);
            assert_eq!(
                reports
                    .borrow()
                    .iter()
                    .copied()
                    .filter(|(_, id, _)| *id == 2)
                    .collect::<Vec<_>>(),
                vec![(1, 2, 0), (1, 2, 1), (1, 2, 2)],
                "recovery must resend the entire inventory, not just the failed chunk"
            );
        }
    }

    #[test]
    fn startup_registration_loss_is_bounded_and_returns_last_error() {
        let starts = Cell::new(0);
        let reports = Cell::new(0);
        let waits = RefCell::new(vec![]);
        let result: CommonResult<()> = register_and_report_with_recovery(
            || false,
            |delay| waits.borrow_mut().push(delay),
            || {
                starts.set(starts.get() + 1);
                Ok(())
            },
            || {
                reports.set(reports.get() + 1);
                Err(registration_lost())
            },
        );
        assert!(is_registration_lost(&result.unwrap_err()));
        assert_eq!(starts.get(), STARTUP_REGISTRATION_MAX_ATTEMPTS);
        assert_eq!(reports.get(), STARTUP_REGISTRATION_MAX_ATTEMPTS);
        assert_eq!(
            *waits.borrow(),
            vec![Duration::from_secs(1), Duration::from_secs(2)]
        );
    }

    #[test]
    fn startup_does_not_reregister_for_unrelated_report_errors() {
        for error in [
            CommonError::from(FsError::in_progress(42)),
            CommonError::from(FsError::common("NotRegistered: unrelated error kind")),
            io_error(), // existing per-batch retries have already been exhausted
        ] {
            let mut error = Some(error);
            let starts = Cell::new(0);
            let result: CommonResult<()> = register_and_report_with_recovery(
                || false,
                |_| panic!("unrelated errors must not restart registration"),
                || {
                    starts.set(starts.get() + 1);
                    Ok(())
                },
                || Err(error.take().expect("must not repeat full report")),
            );
            assert!(result.is_err());
            assert_eq!(starts.get(), 1);
        }
    }

    #[test]
    fn startup_registration_failure_does_not_send_full_report() {
        let result: CommonResult<()> = register_and_report_with_recovery(
            || false,
            |_| panic!("registration rejection must not retry"),
            || Err(master_error()),
            || panic!("report must follow successful registration"),
        );
        assert!(result.is_err());
    }

    #[test]
    fn startup_honors_shutdown_before_registration_and_between_operations() {
        for stop_after_start in [false, true] {
            let stopped = Cell::new(!stop_after_start);
            let starts = Cell::new(0);
            let result: CommonResult<()> = register_and_report_with_recovery(
                || stopped.get(),
                |_| panic!("shutdown must not back off"),
                || {
                    starts.set(starts.get() + 1);
                    stopped.set(true);
                    Ok(())
                },
                || panic!("shutdown must prevent full report"),
            );
            assert!(result.is_err());
            assert_eq!(starts.get(), u32::from(stop_after_start));
        }
    }

    #[test]
    fn startup_honors_shutdown_during_backoff_or_after_report() {
        for successful_report in [false, true] {
            let stopped = Cell::new(false);
            let starts = Cell::new(0);
            let result: CommonResult<()> = register_and_report_with_recovery(
                || stopped.get(),
                |_| stopped.set(true),
                || {
                    starts.set(starts.get() + 1);
                    Ok(())
                },
                || {
                    if successful_report {
                        stopped.set(true);
                        Ok(())
                    } else {
                        Err(registration_lost())
                    }
                },
            );
            assert!(
                result.is_err(),
                "shutdown must prevent heartbeat scheduling"
            );
            assert_eq!(starts.get(), 1);
        }
    }

    #[test]
    fn startup_registration_loss_does_not_retry_the_same_report_batch() {
        let calls = Cell::new(0);
        let result: CommonResult<()> = send_with_retry(
            "full report",
            || false,
            |_| panic!("registration loss belongs to whole-startup recovery"),
            || {
                calls.set(calls.get() + 1);
                Err(registration_lost())
            },
        );
        assert!(is_registration_lost(&result.unwrap_err()));
        assert_eq!(calls.get(), 1);
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
