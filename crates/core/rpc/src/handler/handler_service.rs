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

use crate::handler::{BufferHandler, Frame, MessageHandler, StreamHandler, TestMessageHandler};
use crate::ServerConf;
use curvine_net::net::ConnState;
use curvine_runtime::runtime::Runtime;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Concurrency budget of one RPC service.
///
/// Both halves are live values rather than a snapshot taken at startup: an
/// operator edit through the runtime config API has to be visible to traffic
/// that is already connected, otherwise the process would report a limit it is
/// not applying.
#[derive(Clone, Debug)]
pub struct LimitConf {
    conn_limit: Arc<AtomicUsize>,
    global_limit: Arc<GlobalLimit>,
}

impl LimitConf {
    pub fn new(conn_limit: usize, global_limit: usize) -> Self {
        assert!(conn_limit > 0, "connection limit must be greater than zero");
        assert!(global_limit > 0, "global limit must be greater than zero");
        Self {
            conn_limit: Arc::new(AtomicUsize::new(conn_limit)),
            global_limit: GlobalLimit::new(global_limit),
        }
    }

    /// In-flight request cap for a single connection.
    pub fn conn_limit(&self) -> usize {
        self.conn_limit.load(Ordering::Acquire)
    }

    /// Shared cell the connection loop reads on every poll, so an established
    /// connection picks up a new cap without being reconnected.
    pub fn conn_limit_cell(&self) -> Arc<AtomicUsize> {
        self.conn_limit.clone()
    }

    /// Replace the per-connection cap and return the previous value.
    pub fn set_conn_limit(&self, limit: usize) -> usize {
        assert!(limit > 0, "connection limit must be greater than zero");
        self.conn_limit.swap(limit, Ordering::AcqRel)
    }

    pub fn global_limit(&self) -> &Arc<GlobalLimit> {
        &self.global_limit
    }

    /// Configured value of the process-wide cap, which may differ from
    /// [`GlobalLimit::capacity`] while a shrink is still being collected.
    pub fn global_limit_size(&self) -> usize {
        self.global_limit.target()
    }

    /// Resize the process-wide cap and return the previous configured value.
    pub fn set_global_limit(&self, limit: usize) -> usize {
        assert!(limit > 0, "global limit must be greater than zero");
        self.global_limit.resize(limit)
    }
}

/// Semaphore-backed process-wide concurrency cap that can be resized in place.
///
/// The semaphore handle is never swapped. Swapping is what forces requests
/// admitted before an edit to keep drawing from the old budget while requests
/// admitted after it draw from the new one, so two limits end up applying at the
/// same time. Instead the capacity itself moves, keeping
/// `capacity == available_permits() + in_flight` true throughout:
///
/// * growing adds permits immediately, since idle capacity is never a problem;
/// * shrinking can only withdraw permits that are idle right now, so a target
///   below the current in-flight count is reached progressively as requests
///   finish and [`LimitPermit::drop`] settles what is still owed.
#[derive(Debug)]
pub struct GlobalLimit {
    semaphore: Arc<Semaphore>,
    /// Permits the semaphore was provisioned with, idle or not.
    capacity: AtomicUsize,
    /// Value the operator asked for; the convergence target.
    target: AtomicUsize,
    /// Set while `capacity > target` and no idle permit was left to withdraw.
    /// Lets a permit release check whether it owes work without taking a lock.
    pending_shrink: AtomicBool,
    /// Serializes capacity mutations so two resizes cannot interleave their
    /// add/forget accounting.
    resizing: Mutex<()>,
}

impl GlobalLimit {
    pub fn new(limit: usize) -> Arc<Self> {
        let limit = clamp_limit(limit);
        Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(limit)),
            capacity: AtomicUsize::new(limit),
            target: AtomicUsize::new(limit),
            pending_shrink: AtomicBool::new(false),
            resizing: Mutex::new(()),
        })
    }

    /// Configured value, i.e. what the service is trying to converge on.
    pub fn target(&self) -> usize {
        self.target.load(Ordering::Acquire)
    }

    /// Permits the semaphore currently holds in total, idle plus in flight.
    pub fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Acquire)
    }

    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    pub fn in_flight(&self) -> usize {
        self.capacity().saturating_sub(self.available_permits())
    }

    /// True while a shrink is still owed permits held by in-flight requests.
    pub fn is_settling(&self) -> bool {
        self.pending_shrink.load(Ordering::Acquire)
    }

    pub(crate) fn semaphore(&self) -> &Arc<Semaphore> {
        &self.semaphore
    }

    pub(crate) fn track_permit(self: &Arc<Self>, permit: OwnedSemaphorePermit) -> LimitPermit {
        LimitPermit {
            limit: self.clone(),
            permit: Some(permit),
        }
    }

    /// Point the cap at `limit` and return the previous target.
    pub fn resize(&self, limit: usize) -> usize {
        let limit = clamp_limit(limit);
        let _guard = self.resizing.lock().unwrap_or_else(|err| err.into_inner());
        let previous = self.target.swap(limit, Ordering::AcqRel);
        self.reconcile();
        previous
    }

    /// Close the underlying semaphore so waiters fail instead of hanging.
    pub fn close(&self) {
        self.semaphore.close();
    }

    /// Settle a shrink that was deferred because every excess permit was in
    /// flight. Called on permit release; the flag check keeps the hot path
    /// lock-free whenever the limit is already converged.
    fn settle(&self) {
        if !self.pending_shrink.load(Ordering::Acquire) {
            return;
        }
        let _guard = self.resizing.lock().unwrap_or_else(|err| err.into_inner());
        self.reconcile();
    }

    /// Move `capacity` towards `target` as far as the idle permits allow.
    /// Callers must hold `resizing`.
    fn reconcile(&self) {
        loop {
            let target = self.target.load(Ordering::Acquire);
            let capacity = self.capacity.load(Ordering::Acquire);
            if capacity == target {
                break;
            }
            if capacity < target {
                // Publish before adding so a concurrent reader never sees more
                // permits than the capacity it accounts for.
                self.capacity.store(target, Ordering::Release);
                self.semaphore.add_permits(target - capacity);
                break;
            }
            let idle = self.semaphore.available_permits();
            if idle == 0 {
                break;
            }
            let forgotten = self.semaphore.forget_permits((capacity - target).min(idle));
            if forgotten == 0 {
                break;
            }
            self.capacity.store(capacity - forgotten, Ordering::Release);
        }
        let converged =
            self.capacity.load(Ordering::Acquire) <= self.target.load(Ordering::Acquire);
        self.pending_shrink.store(!converged, Ordering::Release);
    }
}

fn clamp_limit(limit: usize) -> usize {
    limit.clamp(1, Semaphore::MAX_PERMITS - 1)
}

/// A held global permit.
///
/// Releasing it also settles any shrink that could not be applied while the
/// permit was in flight, which is what keeps a lowered limit from staying
/// over-provisioned for the lifetime of the process.
pub struct LimitPermit {
    limit: Arc<GlobalLimit>,
    permit: Option<OwnedSemaphorePermit>,
}

impl fmt::Debug for LimitPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LimitPermit")
            .field("held", &self.permit.is_some())
            .finish()
    }
}

impl Drop for LimitPermit {
    fn drop(&mut self) {
        // Return the permit first: `forget_permits` can only withdraw permits
        // that are idle, and this one just became idle.
        drop(self.permit.take());
        self.limit.settle();
    }
}

/// The message processor runs and manages the following functions:
/// 1. Manage the creation of MessageHandler.
/// 2. Global variables required to manage business logic.
///
/// Unlike ordinary RPC frameworks, orpc also handles stateful file read/write
/// streams. Handlers are shared through `Arc`, so connection-local mutable
/// state must use explicit interior synchronization.
pub trait HandlerService: Send + Sync + 'static {
    type Item: MessageHandler;
    // Whether to pass connection information.
    fn has_conn_state(&self) -> bool {
        false
    }

    fn get_message_handler(&self, conn_info: Option<ConnState>) -> Self::Item;

    fn get_stream_handler<F: Frame>(
        &self,
        rt: Arc<Runtime>,
        frame: F,
        conf: &ServerConf,
    ) -> StreamHandler<F, Self::Item> {
        let conn_state = if self.has_conn_state() {
            Some(frame.new_conn_state())
        } else {
            None
        };

        let handler = self.get_message_handler(conn_state);
        StreamHandler::new(rt, frame, handler, conf)
    }

    /// `true` (default): requests on a connection are processed one at a time
    /// by [`StreamHandler`]. Required for stateful streams.
    ///
    /// `false`: requests are processed concurrently by [`BufferHandler`] and
    /// responses may be out of order. The service must also implement
    /// [`get_limit`](Self::get_limit).
    fn is_stream(&self) -> bool {
        true
    }

    fn get_limit(&self) -> LimitConf {
        panic!("BufferHandler service must provide a shared LimitConf")
    }

    /// Builds a [`BufferHandler`], which processes requests of one connection
    /// concurrently. Used by `RpcServer` when [`is_stream`](Self::is_stream)
    /// returns false.
    fn get_buffer_handler<F: Frame>(
        &self,
        rt: Arc<Runtime>,
        frame: F,
        conf: &ServerConf,
    ) -> BufferHandler<F, Self::Item> {
        let conn_state = if self.has_conn_state() {
            Some(frame.new_conn_state())
        } else {
            None
        };
        let message_handler = self.get_message_handler(conn_state);
        BufferHandler::new(rt, frame, message_handler, conf, self.get_limit())
    }
}

#[allow(unused)]
#[derive(Default)]
pub struct TestService;

impl HandlerService for TestService {
    type Item = TestMessageHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        TestMessageHandler {}
    }
}

#[cfg(test)]
mod tests {
    use super::{GlobalLimit, HandlerService, LimitConf, TestService};
    use std::sync::Arc;

    fn hold(limit: &Arc<GlobalLimit>) -> super::LimitPermit {
        let permit = limit
            .semaphore()
            .clone()
            .try_acquire_owned()
            .expect("permit available");
        limit.track_permit(permit)
    }

    #[test]
    fn cloned_limit_conf_shares_global_semaphore() {
        let limit = LimitConf::new(8, 4096);
        let cloned = limit.clone();

        assert_eq!(limit.conn_limit(), 8);
        assert_eq!(limit.global_limit_size(), 4096);
        assert_eq!(limit.global_limit().available_permits(), 4096);
        assert!(Arc::ptr_eq(limit.global_limit(), cloned.global_limit()));
    }

    #[test]
    fn a_cloned_limit_conf_observes_resizes() {
        let limit = LimitConf::new(4, 2);
        let service_copy = limit.clone();

        // The copy lives inside every per-connection BufferHandler, so this is
        // the path an already-accepted connection reads through.
        limit.set_conn_limit(9);
        limit.set_global_limit(6);

        assert_eq!(service_copy.conn_limit(), 9);
        assert_eq!(service_copy.global_limit_size(), 6);
        assert_eq!(service_copy.global_limit().available_permits(), 6);
    }

    #[test]
    fn growing_the_global_limit_takes_effect_immediately() {
        let limit = LimitConf::new(8, 4);
        assert_eq!(limit.set_global_limit(16), 4);

        assert_eq!(limit.global_limit().capacity(), 16);
        assert_eq!(limit.global_limit().available_permits(), 16);
        assert!(!limit.global_limit().is_settling());
    }

    #[test]
    fn shrinking_the_global_limit_withdraws_idle_permits() {
        let limit = LimitConf::new(8, 16);
        let held = hold(limit.global_limit());
        assert_eq!(limit.global_limit().in_flight(), 1);

        assert_eq!(limit.set_global_limit(8), 16);

        // The single in-flight permit is not withdrawable, but fifteen idle ones
        // are, so the whole shrink lands in one pass.
        assert_eq!(limit.global_limit().capacity(), 8);
        assert_eq!(limit.global_limit().available_permits(), 7);

        drop(held);
        assert_eq!(limit.global_limit().capacity(), 8);
        assert_eq!(limit.global_limit().available_permits(), 8);
        assert!(!limit.global_limit().is_settling());
    }

    #[test]
    fn a_target_below_the_inflight_count_converges_as_permits_are_released() {
        let global = GlobalLimit::new(4);
        let mut held = Vec::new();
        for _ in 0..3 {
            held.push(hold(&global));
        }
        assert_eq!(global.available_permits(), 1);

        // Two permits have to go, but only one is idle: the cap lands on three
        // and the remaining shrink is owed.
        global.resize(2);
        assert_eq!(global.capacity(), 3);
        assert_eq!(global.available_permits(), 0);
        assert!(global.is_settling());

        // The releasing permit is withdrawn instead of being handed back, which
        // is exactly the part a naive `forget_permits` call would miss.
        drop(held.pop().expect("held permit"));
        assert_eq!(global.capacity(), 2);
        assert_eq!(global.available_permits(), 0);
        assert!(!global.is_settling());

        drop(held.pop().expect("held permit"));
        assert_eq!(global.capacity(), 2);
        assert_eq!(global.available_permits(), 1);

        drop(held.pop().expect("held permit"));
        assert_eq!(global.capacity(), 2);
        assert_eq!(global.available_permits(), 2);
    }

    #[test]
    fn resizing_back_up_while_settling_stops_the_shrink() {
        let global = GlobalLimit::new(4);
        let mut held = Vec::new();
        for _ in 0..3 {
            held.push(hold(&global));
        }

        // Only one permit is idle, so the drop to one leaves two owed.
        global.resize(1);
        assert!(global.is_settling());

        global.resize(8);
        assert_eq!(global.capacity(), 8);
        assert!(!global.is_settling());

        held.clear();
        assert_eq!(global.available_permits(), 8);
    }

    #[test]
    #[should_panic(expected = "connection limit must be greater than zero")]
    fn zero_connection_limit_is_rejected() {
        LimitConf::new(0, 4096);
    }

    #[test]
    #[should_panic(expected = "global limit must be greater than zero")]
    fn zero_global_limit_is_rejected() {
        LimitConf::new(8, 0);
    }

    #[test]
    #[should_panic(expected = "BufferHandler service must provide a shared LimitConf")]
    fn buffer_service_must_provide_shared_limit() {
        TestService.get_limit();
    }
}
