//! Generic, business-agnostic byte-level KV abstraction and its in-memory
//! backend.
//!
//! This module is the stable storage boundary the stateless MDS is built on.
//! It deliberately contains NO MDS domain types (mount tables, meta keys,
//! paths); those live in higher layers that depend only on [`KvBackend`]. The
//! FoundationDB backend (a later step) implements the same traits so business
//! code never touches the FDB SDK directly.

mod backend;
mod error;
mod memory;
pub mod metrics;

pub use backend::{run_txn, KvBackend, KvTransaction, DEFAULT_MAX_RETRIES};
pub use error::{KvError, KvResult};
pub use memory::{FaultInjector, MemoryBackend};

#[cfg(test)]
mod contract_tests;
