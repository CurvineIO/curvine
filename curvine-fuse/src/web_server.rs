use crate::fs::dcache::{DirTree, Inode};
use crate::fs::state::{DirHandle, FileHandle, NodeState};
use crate::fs::FuseWriter;
use crate::fuse_metrics::mono_now;
use crate::FuseMetrics;
use axum::extract::State;
use axum::routing::get;
use axum::Router;
use orpc::common::Metrics;
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct WebServer;

impl WebServer {
    pub async fn start(port: u16, state: Arc<NodeState>) -> orpc::CommonResult<()> {
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/healthz", get(|| async { "ok" }))
            .route("/stats", get(stats_handler))
            .route("/details", get(details_handler))
            .with_state(state);

        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        log::info!("FUSE metrics server listening on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}

// The `NodeState` is still injected into the router (`with_state`) so
// `WebServer::start`'s public signature is unchanged, but the handler no longer
// reads it: Phase 1b-2 made the legacy gauges (inode/handle) event-driven and
// removed the scrape-time `set_metrics()` refresh, so the scrape is now a pure
// `text_output()` with no map traversal under read locks.
async fn metrics_handler(State(_): State<Arc<NodeState>>) -> String {
    let fuse_metrics = FuseMetrics::get();

    // Scrape hygiene (unconditional, self-observation): time the render and
    // record the output size. Last-scrape semantics — the values in *this*
    // response reflect the previous scrape; the current scrape is visible next
    // time. This ordering (text_output THEN record_scrape) is what makes it
    // "last scrape"; do not reorder. Recorded on both success and the
    // error-body fallback.
    //
    // As of 1b-2 the scrape-time `set_metrics()` map traversal is gone, so this
    // timer covers the meaningful scrape work — the `text_output()` render —
    // rather than under-counting by that traversal. It deliberately does NOT
    // include Axum's `State` extraction or the `FuseMetrics::get()` before
    // `start`; those are tiny and out of scope for a render-regression signal.
    let start = mono_now();
    let output = Metrics::text_output().unwrap_or_else(|e| format!("Error: {}", e));
    fuse_metrics.record_scrape(start.elapsed().as_micros() as u64, output.len());
    output
}

struct HandleInfo {
    ino: u64,
    fh: u64,
    path: String,
    len: i64,
}

struct InodeInfo {
    ino: u64,
    path: String,
    is_dir: bool,
}

struct WriterInfo {
    ino: u64,
    path: String,
    len: i64,
}

fn resolve_path(dir: &DirTree, ino: u64) -> String {
    dir.get_path(ino).map(|p| p.to_string()).unwrap_or_default()
}

impl NodeState {
    fn list_handle_infos(&self) -> Vec<HandleInfo> {
        let dir = self.dir_read();
        self.all_handles()
            .into_iter()
            .map(|handle| HandleInfo {
                ino: handle.ino(),
                fh: handle.fh(),
                path: resolve_path(&dir, handle.ino()),
                len: handle.status().len,
            })
            .collect()
    }

    fn list_inode_infos(&self) -> Vec<InodeInfo> {
        let dir = self.dir_read();
        dir.nodes_iter()
            .map(|(_, inode)| InodeInfo {
                ino: inode.ino,
                path: resolve_path(&dir, inode.ino),
                is_dir: inode.is_dir,
            })
            .collect()
    }

    async fn list_writer_infos(&self) -> Vec<WriterInfo> {
        let keys = self.writer_keys();
        let mut infos = Vec::with_capacity(keys.len());
        for ino in keys {
            if let Some(writer) = self.find_writer(ino).await {
                infos.push(WriterInfo {
                    ino,
                    path: writer.path().to_string(),
                    len: writer.len(),
                });
            }
        }
        infos
    }
}

// ---------- JSON response structs ----------

#[derive(Serialize)]
struct ComponentStats {
    count: usize,
    mem_usage: usize,
}

#[derive(Serialize)]
struct StatsResponse {
    inode: ComponentStats,
    writer: ComponentStats,
    file_handle: ComponentStats,
    dir_handle: ComponentStats,
}

#[derive(Serialize)]
struct InodeDetail {
    ino: u64,
    path: String,
    is_dir: bool,
}

#[derive(Serialize)]
struct WriterDetail {
    ino: u64,
    path: String,
    len: i64,
}

#[derive(Serialize)]
struct HandleDetail {
    ino: u64,
    fh: u64,
    path: String,
    len: i64,
}

#[derive(Serialize)]
struct DetailsResponse {
    inodes: Vec<InodeDetail>,
    writers: Vec<WriterDetail>,
    handles: Vec<HandleDetail>,
}

async fn stats_handler(State(state): State<Arc<NodeState>>) -> String {
    let inode_count = state.dir_read().inode_lens();
    let writer_count = state.writers_len();
    let handle_count = state.file_handles_len();
    let dir_handle_count = state.dir_handles_len();

    let response = StatsResponse {
        inode: ComponentStats {
            count: inode_count,
            mem_usage: inode_count * size_of::<Inode>(),
        },
        writer: ComponentStats {
            count: writer_count,
            mem_usage: writer_count * size_of::<FuseWriter>(),
        },
        file_handle: ComponentStats {
            count: handle_count,
            mem_usage: handle_count * size_of::<FileHandle>(),
        },
        dir_handle: ComponentStats {
            count: dir_handle_count,
            mem_usage: dir_handle_count * size_of::<DirHandle>(),
        },
    };

    serde_json::to_string_pretty(&response).unwrap_or_else(|e| format!("Error: {}", e))
}

async fn details_handler(State(state): State<Arc<NodeState>>) -> String {
    let inodes: Vec<InodeDetail> = state
        .list_inode_infos()
        .into_iter()
        .map(|i| InodeDetail {
            ino: i.ino,
            path: i.path,
            is_dir: i.is_dir,
        })
        .collect();

    let writers: Vec<WriterDetail> = state
        .list_writer_infos()
        .await
        .into_iter()
        .map(|w| WriterDetail {
            ino: w.ino,
            path: w.path,
            len: w.len,
        })
        .collect();

    let handles: Vec<HandleDetail> = state
        .list_handle_infos()
        .into_iter()
        .map(|h| HandleDetail {
            ino: h.ino,
            fh: h.fh,
            path: h.path,
            len: h.len,
        })
        .collect();

    let response = DetailsResponse {
        inodes,
        writers,
        handles,
    };

    serde_json::to_string_pretty(&response).unwrap_or_else(|e| format!("Error: {}", e))
}
