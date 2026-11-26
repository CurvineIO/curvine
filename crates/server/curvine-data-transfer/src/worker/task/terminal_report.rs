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

use curvine_error::{FsError, FsResult};
use std::future::Future;
use std::time::Duration;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ReportOutcome {
    Accepted,
    Rejected,
    /// A local/protocol error is not an authoritative rejection by the store.
    Unconfirmed,
    Exhausted,
    Interrupted,
    Deferred,
}

pub(super) struct ReportPolicy {
    pub attempts: usize,
    pub attempt_timeout: Duration,
    pub backoff: Duration,
}

impl Default for ReportPolicy {
    fn default() -> Self {
        Self {
            attempts: 3,
            // The receiver may wait five seconds for persistence. Allow transport headroom.
            attempt_timeout: Duration::from_secs(6),
            backoff: Duration::from_millis(200),
        }
    }
}

/// Bound both waiting and active notifications. Capacity exhaustion or the total
/// deadline leaves the committed result available to scheduler probes.
pub(super) async fn deliver_with_capacity<F, Fut, C>(
    pending: &tokio::sync::Semaphore,
    active: &tokio::sync::Semaphore,
    send: F,
    interrupted: C,
    policy: ReportPolicy,
) -> ReportOutcome
where
    F: FnMut() -> Fut,
    Fut: Future<Output = FsResult<bool>>,
    C: Fn() -> bool,
{
    let Ok(_pending) = pending.try_acquire() else {
        return ReportOutcome::Deferred;
    };
    let work = async {
        let Ok(_active) = active.acquire().await else {
            return ReportOutcome::Interrupted;
        };
        deliver_terminal_report(send, &interrupted, policy).await
    };
    let work = tokio::time::timeout(Duration::from_secs(20), work);
    tokio::pin!(work);
    loop {
        tokio::select! {
            result = &mut work => return result.unwrap_or(ReportOutcome::Exhausted),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                if interrupted() { return ReportOutcome::Interrupted; }
            }
        }
    }
}

/// Retry only the notification, with a fixed identity/payload owned by the caller.
/// No execution permit is held here. Exhaustion leaves the result available to probes.
pub(super) async fn deliver_terminal_report<F, Fut, C>(
    mut send: F,
    interrupted: C,
    policy: ReportPolicy,
) -> ReportOutcome
where
    F: FnMut() -> Fut,
    Fut: Future<Output = FsResult<bool>>,
    C: Fn() -> bool,
{
    for attempt in 0..policy.attempts {
        if interrupted() {
            return ReportOutcome::Interrupted;
        }
        let started = std::time::Instant::now();
        let result = async {
            if attempt > 0 {
                tokio::time::sleep(policy.backoff.saturating_mul(attempt as u32)).await;
            }
            tokio::time::timeout(policy.attempt_timeout, send()).await
        };
        tokio::pin!(result);
        let result = loop {
            tokio::select! {
                result = &mut result => break result,
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    if interrupted() { return ReportOutcome::Interrupted; }
                }
            }
        };
        log::debug!(
            "terminal report attempt={} elapsed_ms={}",
            attempt + 1,
            started.elapsed().as_millis()
        );
        match result {
            Ok(Ok(true)) => return ReportOutcome::Accepted,
            Ok(Ok(false)) => return ReportOutcome::Rejected,
            Ok(Err(
                FsError::IO(_)
                | FsError::Timeout(_)
                | FsError::NotLeaderMaster(_)
                | FsError::TransferOverloaded(_),
            ))
            | Err(_) => {}
            Ok(Err(err)) => {
                log::warn!("terminal report stopped after non-retryable error: {}", err);
                return ReportOutcome::Unconfirmed;
            }
        }
        log::debug!(
            "terminal report attempt {} failed; bounded retry pending",
            attempt + 1
        );
    }
    ReportOutcome::Exhausted
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn policy() -> ReportPolicy {
        ReportPolicy {
            attempts: 3,
            attempt_timeout: Duration::from_millis(10),
            backoff: Duration::ZERO,
        }
    }

    #[tokio::test]
    async fn lost_ack_retries_same_notification() {
        let calls = AtomicUsize::new(0);
        let result = deliver_terminal_report(
            || async {
                if calls.fetch_add(1, Ordering::Relaxed) == 0 {
                    // Side effect was persisted; response was lost.
                    Err(std::io::Error::from(std::io::ErrorKind::ConnectionReset).into())
                } else {
                    Ok(true)
                }
            },
            || false,
            policy(),
        )
        .await;
        assert_eq!(result, ReportOutcome::Accepted);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn rejection_is_not_retried() {
        let calls = AtomicUsize::new(0);
        let result = deliver_terminal_report(
            || async {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(false)
            },
            || false,
            policy(),
        )
        .await;
        assert_eq!(result, ReportOutcome::Rejected);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn timeouts_and_overload_are_bounded() {
        let calls = AtomicUsize::new(0);
        let result = deliver_terminal_report(
            || async {
                calls.fetch_add(1, Ordering::Relaxed);
                std::future::pending::<FsResult<bool>>().await
            },
            || false,
            policy(),
        )
        .await;
        assert_eq!(result, ReportOutcome::Exhausted);
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        calls.store(0, Ordering::Relaxed);
        let result = deliver_terminal_report(
            || async {
                calls.fetch_add(1, Ordering::Relaxed);
                Err(FsError::transfer_overloaded("test queue full"))
            },
            || false,
            policy(),
        )
        .await;
        assert_eq!(result, ReportOutcome::Exhausted);
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn cancellation_stops_before_another_attempt() {
        let calls = AtomicUsize::new(0);
        let result = deliver_terminal_report(
            || async {
                calls.fetch_add(1, Ordering::Relaxed);
                Err(FsError::transfer_overloaded("retry"))
            },
            || calls.load(Ordering::Relaxed) > 0,
            policy(),
        )
        .await;
        assert_eq!(result, ReportOutcome::Interrupted);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
    #[tokio::test]
    async fn notification_waiters_are_bounded_and_resume_after_capacity_returns() {
        use std::sync::Arc;
        use tokio::sync::Semaphore;
        tokio::time::timeout(Duration::from_secs(2), async {
            let pending = Arc::new(Semaphore::new(2));
            let active = Arc::new(Semaphore::new(1));
            let occupied = active.acquire().await.unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let mut waiters = Vec::new();
            for _ in 0..2 {
                let pending = pending.clone();
                let active = active.clone();
                let calls = calls.clone();
                waiters.push(tokio::spawn(async move {
                    deliver_with_capacity(
                        &pending,
                        &active,
                        || async {
                            calls.fetch_add(1, Ordering::Relaxed);
                            Ok(true)
                        },
                        || false,
                        policy(),
                    )
                    .await
                }));
            }
            while pending.available_permits() > 0 {
                tokio::task::yield_now().await;
            }
            let excess = deliver_with_capacity(
                &pending,
                &active,
                || async { panic!("must defer before sending") },
                || false,
                policy(),
            )
            .await;
            assert_eq!(excess, ReportOutcome::Deferred);
            assert_eq!(calls.load(Ordering::Relaxed), 0);
            drop(occupied);
            for waiter in waiters {
                assert_eq!(waiter.await.unwrap(), ReportOutcome::Accepted);
            }
            assert_eq!(calls.load(Ordering::Relaxed), 2);
            assert_eq!(pending.available_permits(), 2);
            assert_eq!(active.available_permits(), 1);
        })
        .await
        .expect("notification queue did not drain");
    }
}
