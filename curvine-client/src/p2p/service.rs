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

use crate::p2p::sha256_bytes;
use crate::p2p::{CacheManager, CachePutResultTag, ChunkId, FetchedCacheBatchItem};
use bytes::Bytes;
use curvine_common::conf::ClientP2pConf;
use curvine_common::utils::CommonUtils;
use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use libp2p::identify;
use libp2p::identity;
use libp2p::kad;
use libp2p::mdns;
use libp2p::multiaddr::Protocol;
use libp2p::noise;
use libp2p::request_response;
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::{NetworkBehaviour, StreamProtocol, SwarmEvent};
use libp2p::tcp;
use libp2p::yamux;
use libp2p::{Multiaddr, PeerId, SwarmBuilder};
use libp2p_stream as p2p_stream;
use log::{debug, warn};
use once_cell::sync::{Lazy, OnceCell};
use orpc::common::LocalTime;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::FastDashMap;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{Error, ErrorKind};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch, Mutex as AsyncMutex, Notify, Semaphore};
use tokio::time::{sleep, timeout};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

static PEER_STATS: Lazy<FastDashMap<String, Arc<PeerStats>>> = Lazy::new(FastDashMap::default);
const CHUNK_PROTOCOL: &str = "/curvine/p2p/chunk/2";
const CHUNK_STREAM_PROTOCOL: &str = "/curvine/p2p/chunk/stream/1";
const REQUEST_FIXED_BYTES: usize = 50;
const RESPONSE_FIXED_BYTES: usize = 20;
const RESPONSE_ITEM_FIXED_BYTES: usize = 22;
const DATA_PLANE_REQUEST_FIXED_BYTES: usize = 62;
const EMPTY_DATA_PLANE_TICKET: [u8; 16] = [0; 16];

#[derive(Debug, Default)]
struct PeerStats {
    bytes_sent: AtomicU64,
    bytes_recv: AtomicU64,
    latency_us_total: AtomicU64,
    latency_samples: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
struct PeerEwma {
    latency_ms: f64,
    failure_ratio: f64,
    throughput_mb_s: f64,
}

impl Default for PeerEwma {
    fn default() -> Self {
        Self {
            latency_ms: 200.0,
            failure_ratio: 0.0,
            throughput_mb_s: 4.0,
        }
    }
}

impl PeerEwma {
    fn observe_success(&mut self, latency_ms: f64, bytes: usize) {
        self.latency_ms = ewma(self.latency_ms, latency_ms.max(0.0), 0.2);
        self.failure_ratio = ewma(self.failure_ratio, 0.0, 0.3);
        if latency_ms > 0.0 && bytes > 0 {
            let throughput_mb_s = bytes as f64 / latency_ms / 1024.0;
            self.throughput_mb_s = ewma(self.throughput_mb_s, throughput_mb_s.max(0.01), 0.2);
        }
    }

    fn observe_failure(&mut self) {
        self.failure_ratio = ewma(self.failure_ratio, 1.0, 0.3);
    }
}

#[derive(Debug, Default)]
struct NetworkState {
    active_peers: AtomicU64,
    mdns_peers: AtomicU64,
    dht_peers: AtomicU64,
    bootstrap_connected: AtomicU64,
    local_peer_id: OnceCell<String>,
    listen_addr: OnceCell<String>,
}

#[derive(Debug, Clone)]
struct DataPlaneTicket {
    file_id: i64,
    version_epoch: i64,
    block_id: i64,
    tenant_id: String,
    expected_mtime: i64,
    expire_at_ms: i64,
}

#[derive(Clone)]
struct DataPlaneState {
    port: Arc<AtomicU64>,
    started: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
    shutdown_tx: Arc<watch::Sender<bool>>,
    tickets: Arc<FastDashMap<[u8; 16], DataPlaneTicket>>,
    next_ticket: Arc<AtomicU64>,
    ticket_ttl_ms: i64,
    tenant_whitelist: Arc<Mutex<Arc<HashSet<String>>>>,
}

impl DataPlaneState {
    fn new(conf: &ClientP2pConf) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            port: Arc::new(AtomicU64::new(0)),
            started: Arc::new(AtomicBool::new(false)),
            generation: Arc::new(AtomicU64::new(0)),
            shutdown_tx: Arc::new(shutdown_tx),
            tickets: Arc::new(FastDashMap::default()),
            next_ticket: Arc::new(AtomicU64::new(1)),
            ticket_ttl_ms: data_plane_ticket_ttl(conf).as_millis().max(1) as i64,
            tenant_whitelist: Arc::new(Mutex::new(Arc::new(parse_tenant_whitelist(
                &conf.tenant_whitelist,
            )))),
        }
    }

    fn port(&self) -> u16 {
        self.port.load(Ordering::Relaxed).min(u16::MAX as u64) as u16
    }

    fn issue_ticket(&self, request: &ChunkFetchRequest) -> [u8; 16] {
        let counter = self.next_ticket.fetch_add(1, Ordering::Relaxed);
        let now = LocalTime::nanos() as u64;
        let mut ticket = EMPTY_DATA_PLANE_TICKET;
        ticket[0..8].copy_from_slice(&counter.to_le_bytes());
        ticket[8..16].copy_from_slice(&now.to_le_bytes());
        self.tickets.insert(
            ticket,
            DataPlaneTicket {
                file_id: request.file_id,
                version_epoch: request.version_epoch,
                block_id: request.block_id,
                tenant_id: request.tenant_id.clone(),
                expected_mtime: request.expected_mtime,
                expire_at_ms: (LocalTime::mills() as i64).saturating_add(self.ticket_ttl_ms),
            },
        );
        ticket
    }

    fn authorize(&self, request: &DataPlaneFetchRequest) -> Option<ChunkFetchRequest> {
        if request.ticket == EMPTY_DATA_PLANE_TICKET {
            return None;
        }
        let ticket = self.tickets.get(&request.ticket)?;
        let now_ms = LocalTime::mills() as i64;
        if ticket.expire_at_ms <= now_ms {
            self.tickets.remove(&request.ticket);
            return None;
        }
        if ticket.file_id != request.file_id
            || ticket.version_epoch != request.version_epoch
            || ticket.block_id != request.block_id
        {
            return None;
        }
        if ticket.expected_mtime > 0
            && request.expected_mtime > 0
            && ticket.expected_mtime != request.expected_mtime
        {
            return None;
        }
        Some(ChunkFetchRequest {
            file_id: request.file_id,
            version_epoch: request.version_epoch,
            block_id: request.block_id,
            off: request.off,
            len: request.len,
            expected_mtime: request.expected_mtime,
            prefetch_count: request.prefetch_count,
            tenant_id: ticket.tenant_id.clone(),
        })
    }

    #[cfg(test)]
    fn tenant_whitelist(&self) -> Arc<HashSet<String>> {
        match self.tenant_whitelist.lock() {
            Ok(tenant_whitelist) => Arc::clone(&tenant_whitelist),
            Err(poisoned) => {
                let tenant_whitelist = poisoned.into_inner();
                Arc::clone(&tenant_whitelist)
            }
        }
    }

    fn update_tenant_whitelist(&self, tenant_whitelist: &[String]) {
        let updated = Arc::new(parse_tenant_whitelist(tenant_whitelist));
        match self.tenant_whitelist.lock() {
            Ok(mut current) => *current = updated,
            Err(poisoned) => *poisoned.into_inner() = updated,
        }
    }

    fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    fn clear_shutdown(&self) {
        if *self.shutdown_tx.borrow() {
            let _ = self.shutdown_tx.send(false);
        }
    }

    fn request_shutdown(&self) {
        if !*self.shutdown_tx.borrow() {
            let _ = self.shutdown_tx.send(true);
        }
    }

    fn next_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
struct QpsLimiter {
    rate_per_sec: u64,
    state: AsyncMutex<QpsBucketState>,
}

#[derive(Debug)]
struct QpsBucketState {
    tokens: f64,
    last_refill: Instant,
}

impl QpsLimiter {
    fn new(rate_per_sec: u64) -> Self {
        Self {
            rate_per_sec,
            state: AsyncMutex::new(QpsBucketState {
                tokens: rate_per_sec as f64,
                last_refill: Instant::now(),
            }),
        }
    }

    async fn acquire(&self, timeout: Duration) -> bool {
        if self.rate_per_sec == 0 {
            return true;
        }
        let deadline = Instant::now() + timeout;
        loop {
            let wait = {
                let mut state = self.state.lock().await;
                let now = Instant::now();
                let elapsed_secs = now.duration_since(state.last_refill).as_secs_f64();
                state.tokens = (state.tokens + elapsed_secs * self.rate_per_sec as f64)
                    .min(self.rate_per_sec as f64);
                state.last_refill = now;
                if state.tokens >= 1.0 {
                    state.tokens -= 1.0;
                    return true;
                }
                Duration::from_secs_f64((1.0 - state.tokens) / self.rate_per_sec as f64)
                    .max(Duration::from_millis(1))
            };

            if Instant::now() + wait > deadline {
                return false;
            }
            sleep(wait).await;
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct P2pStatsSnapshot {
    pub active_peers: usize,
    pub mdns_peers: usize,
    pub dht_peers: usize,
    pub bootstrap_connected: usize,
    pub bytes_sent: u64,
    pub bytes_recv: u64,
    pub avg_peer_latency_ms: f64,
    pub cache_usage_bytes: u64,
    pub cache_capacity_bytes: u64,
    pub cache_usage_ratio: f64,
    pub cached_chunks_count: usize,
    pub expired_chunks: u64,
    pub invalidations: u64,
    pub mtime_mismatches: u64,
    pub checksum_failures: u64,
    pub corruption_count: u64,
    pub policy_rejects: u64,
    pub policy_rollback_ignored: u64,
}

#[derive(Debug, Clone, Default)]
pub struct P2pReadTraceContext {
    pub trace_id: Option<String>,
    pub tenant_id: Option<String>,
    pub job_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum P2pState {
    Disabled = 0,
    Init = 1,
    Running = 2,
    Stopped = 3,
}

#[derive(Debug)]
enum NetworkCommand {
    Publish {
        chunk_id: ChunkId,
    },
    Fetch {
        fetch_token: u64,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
        trace: TraceLabels,
        response: oneshot::Sender<Option<FetchedChunk>>,
    },
    CancelFetch {
        fetch_token: u64,
    },
    UpdateRuntimePolicy {
        peer_whitelist: Option<Vec<String>>,
        tenant_whitelist: Option<Vec<String>>,
        response: oneshot::Sender<bool>,
    },
    Stop,
}

#[derive(Debug, Clone)]
struct ChunkFetchRequest {
    file_id: i64,
    version_epoch: i64,
    block_id: i64,
    off: i64,
    len: usize,
    expected_mtime: i64,
    prefetch_count: u16,
    tenant_id: String,
}

#[derive(Debug, Clone)]
struct ChunkFetchResponse {
    data_port: u16,
    ticket: [u8; 16],
    chunks: Vec<ChunkFetchResponseChunk>,
}

#[derive(Debug, Clone)]
struct ChunkFetchResponseChunk {
    off: i64,
    mtime: i64,
    checksum: Bytes,
    data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DataPlaneFetchRequest {
    ticket: [u8; 16],
    file_id: i64,
    version_epoch: i64,
    block_id: i64,
    off: i64,
    len: usize,
    expected_mtime: i64,
    prefetch_count: u16,
}

#[derive(Debug, Clone)]
struct ChunkTransferCodec {
    request_size_maximum: usize,
    response_size_maximum: usize,
}

impl Default for ChunkTransferCodec {
    fn default() -> Self {
        Self {
            request_size_maximum: 1024 * 1024,
            response_size_maximum: 64 * 1024 * 1024,
        }
    }
}

impl ChunkTransferCodec {
    fn new(request_size_maximum: usize, response_size_maximum: usize) -> Self {
        Self {
            request_size_maximum: request_size_maximum.max(REQUEST_FIXED_BYTES),
            response_size_maximum: response_size_maximum.max(RESPONSE_FIXED_BYTES),
        }
    }
}

fn encode_response_header(
    chunk_count: u16,
    data_port: u16,
    ticket: [u8; 16],
) -> [u8; RESPONSE_FIXED_BYTES] {
    let mut header = [0u8; RESPONSE_FIXED_BYTES];
    header[0..2].copy_from_slice(&chunk_count.to_le_bytes());
    header[2..4].copy_from_slice(&data_port.to_le_bytes());
    header[4..20].copy_from_slice(&ticket);
    header
}

fn decode_response_header(
    header: [u8; RESPONSE_FIXED_BYTES],
) -> std::io::Result<(usize, u16, [u8; 16])> {
    let chunk_count = u16::from_le_bytes(header[0..2].try_into().map_err(|_| {
        Error::new(
            ErrorKind::InvalidData,
            "invalid response chunk_count header",
        )
    })?) as usize;
    let data_port =
        u16::from_le_bytes(header[2..4].try_into().map_err(|_| {
            Error::new(ErrorKind::InvalidData, "invalid response data_port header")
        })?);
    let mut ticket = EMPTY_DATA_PLANE_TICKET;
    ticket.copy_from_slice(&header[4..20]);
    Ok((chunk_count, data_port, ticket))
}

async fn read_exact_bytes<T>(io: &mut T, len: usize) -> std::io::Result<Bytes>
where
    T: futures::AsyncRead + Unpin + Send,
{
    if len == 0 {
        return Ok(Bytes::new());
    }
    let mut buf = Vec::with_capacity(len);
    let uninit = &mut buf.spare_capacity_mut()[..len];
    let bytes = unsafe { std::slice::from_raw_parts_mut(uninit.as_mut_ptr().cast::<u8>(), len) };
    io.read_exact(bytes).await?;
    unsafe {
        buf.set_len(len);
    }
    Ok(Bytes::from(buf))
}

async fn read_response_header_frame<T>(io: &mut T) -> std::io::Result<(usize, u16, [u8; 16])>
where
    T: futures::AsyncRead + Unpin + Send,
{
    let mut header = [0u8; RESPONSE_FIXED_BYTES];
    io.read_exact(&mut header).await?;
    decode_response_header(header)
}

async fn read_response_item_frame<T>(
    io: &mut T,
    total_len: &mut usize,
    response_size_maximum: usize,
) -> std::io::Result<ChunkFetchResponseChunk>
where
    T: futures::AsyncRead + Unpin + Send,
{
    let mut item_header = [0u8; RESPONSE_ITEM_FIXED_BYTES];
    io.read_exact(&mut item_header).await?;
    let off = i64::from_le_bytes(item_header[0..8].try_into().unwrap());
    let mtime = i64::from_le_bytes(item_header[8..16].try_into().unwrap());
    let checksum_len = u16::from_le_bytes(item_header[16..18].try_into().unwrap()) as usize;
    let data_len = u32::from_le_bytes(item_header[18..22].try_into().unwrap()) as usize;
    *total_len = total_len
        .saturating_add(RESPONSE_ITEM_FIXED_BYTES)
        .saturating_add(checksum_len)
        .saturating_add(data_len);
    if *total_len > response_size_maximum {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "response frame exceeds maximum size",
        ));
    }
    Ok(ChunkFetchResponseChunk {
        off,
        mtime,
        checksum: read_exact_bytes(io, checksum_len).await?,
        data: read_exact_bytes(io, data_len).await?,
    })
}

fn encode_data_plane_request(
    request: &DataPlaneFetchRequest,
) -> [u8; DATA_PLANE_REQUEST_FIXED_BYTES] {
    let mut header = [0u8; DATA_PLANE_REQUEST_FIXED_BYTES];
    let len = u32::try_from(request.len).unwrap_or(u32::MAX);
    header[0..16].copy_from_slice(&request.ticket);
    header[16..24].copy_from_slice(&request.file_id.to_le_bytes());
    header[24..32].copy_from_slice(&request.version_epoch.to_le_bytes());
    header[32..40].copy_from_slice(&request.block_id.to_le_bytes());
    header[40..48].copy_from_slice(&request.off.to_le_bytes());
    header[48..52].copy_from_slice(&len.to_le_bytes());
    header[52..60].copy_from_slice(&request.expected_mtime.to_le_bytes());
    header[60..62].copy_from_slice(&request.prefetch_count.to_le_bytes());
    header
}

fn decode_data_plane_request(
    header: [u8; DATA_PLANE_REQUEST_FIXED_BYTES],
) -> std::io::Result<DataPlaneFetchRequest> {
    let mut ticket = EMPTY_DATA_PLANE_TICKET;
    ticket.copy_from_slice(&header[0..16]);
    Ok(DataPlaneFetchRequest {
        ticket,
        file_id: i64::from_le_bytes(
            header[16..24]
                .try_into()
                .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid data file_id"))?,
        ),
        version_epoch: i64::from_le_bytes(
            header[24..32]
                .try_into()
                .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid data version_epoch"))?,
        ),
        block_id: i64::from_le_bytes(
            header[32..40]
                .try_into()
                .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid data block_id"))?,
        ),
        off: i64::from_le_bytes(
            header[40..48]
                .try_into()
                .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid data off"))?,
        ),
        len: u32::from_le_bytes(
            header[48..52]
                .try_into()
                .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid data len"))?,
        ) as usize,
        expected_mtime: i64::from_le_bytes(
            header[52..60]
                .try_into()
                .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid data expected_mtime"))?,
        ),
        prefetch_count: u16::from_le_bytes(
            header[60..62]
                .try_into()
                .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid data prefetch_count"))?,
        ),
    })
}

#[allow(dead_code)]
fn peer_data_endpoint(addr: &Multiaddr, data_port: u16) -> Option<SocketAddr> {
    let ip = addr.iter().find_map(|protocol| match protocol {
        Protocol::Ip4(value) => Some(IpAddr::V4(value)),
        Protocol::Ip6(value) => Some(IpAddr::V6(value)),
        _ => None,
    })?;
    Some(SocketAddr::new(ip, data_port))
}

#[async_trait::async_trait]
impl request_response::Codec for ChunkTransferCodec {
    type Protocol = StreamProtocol;
    type Request = ChunkFetchRequest;
    type Response = ChunkFetchResponse;

    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Request>
    where
        T: futures::AsyncRead + Unpin + Send,
    {
        let mut header = [0u8; REQUEST_FIXED_BYTES];
        io.read_exact(&mut header).await?;
        let prefetch_count = u16::from_le_bytes(header[44..46].try_into().unwrap());
        let tenant_len = u32::from_le_bytes(header[46..50].try_into().unwrap()) as usize;
        let total_len = REQUEST_FIXED_BYTES.saturating_add(tenant_len);
        if total_len > self.request_size_maximum {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "request frame exceeds maximum size",
            ));
        }
        let mut tenant = vec![0u8; tenant_len];
        io.read_exact(&mut tenant).await?;
        let tenant_id = String::from_utf8(tenant)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid tenant_id utf8"))?;
        Ok(ChunkFetchRequest {
            file_id: i64::from_le_bytes(header[0..8].try_into().unwrap()),
            version_epoch: i64::from_le_bytes(header[8..16].try_into().unwrap()),
            block_id: i64::from_le_bytes(header[16..24].try_into().unwrap()),
            off: i64::from_le_bytes(header[24..32].try_into().unwrap()),
            len: u32::from_le_bytes(header[32..36].try_into().unwrap()) as usize,
            expected_mtime: i64::from_le_bytes(header[36..44].try_into().unwrap()),
            prefetch_count,
            tenant_id,
        })
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: futures::AsyncRead + Unpin + Send,
    {
        let (chunk_count, data_port, ticket) = read_response_header_frame(io).await?;
        let mut chunks = Vec::with_capacity(chunk_count);
        let mut total_len = RESPONSE_FIXED_BYTES;
        for _ in 0..chunk_count {
            chunks.push(
                read_response_item_frame(io, &mut total_len, self.response_size_maximum).await?,
            );
        }
        Ok(ChunkFetchResponse {
            data_port,
            ticket,
            chunks,
        })
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> std::io::Result<()>
    where
        T: futures::AsyncWrite + Unpin + Send,
    {
        let tenant = req.tenant_id.into_bytes();
        let tenant_len = u32::try_from(tenant.len())
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "tenant_id too large"))?;
        let req_len = u32::try_from(req.len)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "request len too large"))?;
        let total_len = REQUEST_FIXED_BYTES.saturating_add(tenant.len());
        if total_len > self.request_size_maximum {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "request frame exceeds maximum size",
            ));
        }
        let mut header = [0u8; REQUEST_FIXED_BYTES];
        header[0..8].copy_from_slice(&req.file_id.to_le_bytes());
        header[8..16].copy_from_slice(&req.version_epoch.to_le_bytes());
        header[16..24].copy_from_slice(&req.block_id.to_le_bytes());
        header[24..32].copy_from_slice(&req.off.to_le_bytes());
        header[32..36].copy_from_slice(&req_len.to_le_bytes());
        header[36..44].copy_from_slice(&req.expected_mtime.to_le_bytes());
        header[44..46].copy_from_slice(&req.prefetch_count.to_le_bytes());
        header[46..50].copy_from_slice(&tenant_len.to_le_bytes());
        io.write_all(&header).await?;
        io.write_all(&tenant).await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        resp: Self::Response,
    ) -> std::io::Result<()>
    where
        T: futures::AsyncWrite + Unpin + Send,
    {
        let chunk_count = u16::try_from(resp.chunks.len())
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "response chunks too many"))?;
        let total_len = resp
            .chunks
            .iter()
            .try_fold(RESPONSE_FIXED_BYTES, |acc, chunk| {
                let _ = u16::try_from(chunk.checksum.len())
                    .map_err(|_| Error::new(ErrorKind::InvalidInput, "checksum too large"))?;
                let _ = u32::try_from(chunk.data.len()).map_err(|_| {
                    Error::new(ErrorKind::InvalidInput, "response payload too large")
                })?;
                Ok::<usize, Error>(
                    acc.saturating_add(RESPONSE_ITEM_FIXED_BYTES)
                        .saturating_add(chunk.checksum.len())
                        .saturating_add(chunk.data.len()),
                )
            })?;
        if total_len > self.response_size_maximum {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "response frame exceeds maximum size",
            ));
        }
        io.write_all(&encode_response_header(
            chunk_count,
            resp.data_port,
            resp.ticket,
        ))
        .await?;
        for chunk in resp.chunks {
            let checksum_len = u16::try_from(chunk.checksum.len())
                .map_err(|_| Error::new(ErrorKind::InvalidInput, "checksum too large"))?;
            let data_len = u32::try_from(chunk.data.len())
                .map_err(|_| Error::new(ErrorKind::InvalidInput, "response payload too large"))?;
            let mut item_header = [0u8; RESPONSE_ITEM_FIXED_BYTES];
            item_header[0..8].copy_from_slice(&chunk.off.to_le_bytes());
            item_header[8..16].copy_from_slice(&chunk.mtime.to_le_bytes());
            item_header[16..18].copy_from_slice(&checksum_len.to_le_bytes());
            item_header[18..22].copy_from_slice(&data_len.to_le_bytes());
            io.write_all(&item_header).await?;
            io.write_all(chunk.checksum.as_ref()).await?;
            io.write_all(chunk.data.as_ref()).await?;
        }
        Ok(())
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(
    to_swarm = "P2pNetworkEvent",
    prelude = "libp2p::swarm::derive_prelude"
)]
struct P2pNetworkBehaviour {
    request_response: request_response::Behaviour<ChunkTransferCodec>,
    stream: p2p_stream::Behaviour,
    identify: identify::Behaviour,
    mdns: Toggle<mdns::tokio::Behaviour>,
    kad: kad::Behaviour<kad::store::MemoryStore>,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum P2pNetworkEvent {
    RequestResponse(request_response::Event<ChunkFetchRequest, ChunkFetchResponse>),
    Identify(identify::Event),
    Mdns(mdns::Event),
    Kad(kad::Event),
    Ignore,
}

impl From<request_response::Event<ChunkFetchRequest, ChunkFetchResponse>> for P2pNetworkEvent {
    fn from(value: request_response::Event<ChunkFetchRequest, ChunkFetchResponse>) -> Self {
        Self::RequestResponse(value)
    }
}

impl From<mdns::Event> for P2pNetworkEvent {
    fn from(value: mdns::Event) -> Self {
        Self::Mdns(value)
    }
}

impl From<identify::Event> for P2pNetworkEvent {
    fn from(value: identify::Event) -> Self {
        Self::Identify(value)
    }
}

impl From<kad::Event> for P2pNetworkEvent {
    fn from(value: kad::Event) -> Self {
        Self::Kad(value)
    }
}

impl From<()> for P2pNetworkEvent {
    fn from(_: ()) -> Self {
        Self::Ignore
    }
}

struct PendingProviderQuery {
    fetch_token: u64,
    chunk_id: ChunkId,
    max_len: usize,
    expected_mtime: Option<i64>,
    trace: TraceLabels,
    response: oneshot::Sender<Option<FetchedChunk>>,
    deadline: Instant,
}

struct PendingFetchRequest {
    fetch_token: u64,
    chunk_id: ChunkId,
    max_len: usize,
    expected_mtime: Option<i64>,
    trace: TraceLabels,
    response: oneshot::Sender<Option<FetchedChunk>>,
    candidates: VecDeque<PeerId>,
    active_peer: Option<PeerId>,
    defer_dht: bool,
    started_at: Instant,
    deadline: Instant,
}

struct StreamFetchOutcome {
    request_id: u64,
    peer: PeerId,
    response: Option<ChunkFetchResponse>,
    unsupported_protocol: bool,
}

struct StreamFetchDispatch {
    request_id: u64,
    peer: PeerId,
    request: ChunkFetchRequest,
}

struct DataPlaneFetchOutcome {
    request_id: u64,
    peer: PeerId,
    chunk: Option<FetchedChunk>,
    recv: usize,
}

struct DataPlaneFetchDispatch {
    request_id: u64,
    peer: PeerId,
    endpoint: SocketAddr,
    request: DataPlaneFetchRequest,
}

struct PendingIncomingResponse {
    channel: request_response::ResponseChannel<ChunkFetchResponse>,
    deadline: Instant,
}

struct PreparedIncomingResponse {
    id: u64,
    response: ChunkFetchResponse,
    sent: u64,
}

#[derive(Debug)]
struct FetchedChunk {
    data: Bytes,
    mtime: i64,
    checksum: Bytes,
    prefetched: Vec<PrefetchedChunk>,
}

#[derive(Debug)]
struct PrefetchedChunk {
    chunk_id: ChunkId,
    data: Bytes,
    mtime: i64,
    checksum: Bytes,
}

#[derive(Clone)]
enum PendingFetchedChunk {
    Ready {
        data: Bytes,
        mtime: i64,
        persist_token: u64,
    },
    Loading(Arc<PendingFetchedLoading>),
}

#[derive(Clone)]
struct PendingFetchedLoading {
    receiver: watch::Receiver<PendingFetchedLoadingState>,
    persist_token: u64,
}

#[derive(Clone)]
enum PendingFetchedLoadingState {
    Pending,
    Ready { data: Bytes, mtime: i64 },
    Failed,
}

struct PendingFetchedPlaceholder {
    chunk_id: ChunkId,
    sender: watch::Sender<PendingFetchedLoadingState>,
    persist_token: u64,
}

#[derive(Clone)]
struct PendingFetchedPersist {
    chunk_id: ChunkId,
    data: Bytes,
    mtime: i64,
    checksum: Bytes,
    persist_token: u64,
}

#[derive(Clone)]
struct HotPublishedChunk {
    data: Bytes,
    mtime: i64,
    checksum: Bytes,
    expire_at_ms: i64,
}

#[derive(Clone)]
struct CachedProviders {
    peers: HashSet<PeerId>,
    expire_at: Instant,
}

impl PendingFetchedChunk {
    fn ready(data: Bytes, mtime: i64, persist_token: u64) -> Self {
        Self::Ready {
            data,
            mtime,
            persist_token,
        }
    }

    fn persist_token(&self) -> u64 {
        match self {
            PendingFetchedChunk::Ready { persist_token, .. } => *persist_token,
            PendingFetchedChunk::Loading(loading) => loading.persist_token,
        }
    }
}

#[derive(Debug, Clone)]
struct TraceLabels {
    trace_id: String,
    tenant_id: String,
    job_id: String,
    sampled: bool,
}

pub struct P2pService {
    conf: ClientP2pConf,
    state: AtomicU8,
    peer_id: String,
    inflight: Arc<Semaphore>,
    publish_inflight: Arc<Semaphore>,
    pending_publish_tasks: Arc<AtomicUsize>,
    runtime: Option<Arc<Runtime>>,
    cache_manager: Arc<CacheManager>,
    pending_fetched: Arc<FastDashMap<ChunkId, PendingFetchedChunk>>,
    hot_published_chunks: Arc<FastDashMap<ChunkId, HotPublishedChunk>>,
    hot_published_order: Mutex<VecDeque<ChunkId>>,
    fetched_persist_txs: Mutex<Vec<mpsc::Sender<PendingFetchedPersist>>>,
    qps_limiter: Option<Arc<QpsLimiter>>,
    fetch_flights: FastDashMap<ChunkId, Arc<AsyncMutex<()>>>,
    network_mtime_mismatches: AtomicU64,
    network_checksum_failures: AtomicU64,
    network_state: Arc<NetworkState>,
    data_plane: DataPlaneState,
    network_ready_notify: Arc<Notify>,
    network_tx: Mutex<Option<mpsc::Sender<NetworkCommand>>>,
    fetch_tokens: AtomicU64,
    persist_tokens: Arc<AtomicU64>,
    network_warmup_done: AtomicBool,
    runtime_policy_version: AtomicU64,
    runtime_policy_applied: AtomicBool,
    runtime_policy_lock: AsyncMutex<()>,
    runtime_policy_rejects: AtomicU64,
    runtime_policy_rollback_ignored: AtomicU64,
    rejected_policy_version: AtomicU64,
    rejected_policy_signature: AsyncMutex<String>,
}

impl P2pService {
    pub fn new(conf: ClientP2pConf) -> Self {
        Self::new_with_runtime(conf, None)
    }

    pub fn new_with_runtime(conf: ClientP2pConf, runtime: Option<Arc<Runtime>>) -> Self {
        let state = if conf.enable && runtime.is_some() {
            P2pState::Init as u8
        } else {
            P2pState::Disabled as u8
        };
        let runtime_policy_version = load_runtime_policy_version(&conf);
        let cache_manager = Arc::new(CacheManager::new(&conf));
        let publish_parallelism = publish_persist_parallelism(&conf);
        let data_plane = DataPlaneState::new(&conf);

        Self {
            inflight: Arc::new(Semaphore::new(conf.max_inflight_requests.max(1))),
            publish_inflight: Arc::new(Semaphore::new(publish_parallelism)),
            pending_publish_tasks: Arc::new(AtomicUsize::new(0)),
            qps_limiter: (conf.p2p_qps_limit > 0)
                .then(|| Arc::new(QpsLimiter::new(conf.p2p_qps_limit))),
            conf,
            state: AtomicU8::new(state),
            peer_id: format!("client-{}", orpc::common::Utils::uuid()),
            runtime,
            cache_manager,
            pending_fetched: Arc::new(FastDashMap::default()),
            hot_published_chunks: Arc::new(FastDashMap::default()),
            hot_published_order: Mutex::new(VecDeque::new()),
            fetched_persist_txs: Mutex::new(Vec::new()),
            fetch_flights: FastDashMap::default(),
            network_mtime_mismatches: AtomicU64::new(0),
            network_checksum_failures: AtomicU64::new(0),
            network_state: Arc::new(NetworkState::default()),
            data_plane,
            network_ready_notify: Arc::new(Notify::new()),
            network_tx: Mutex::new(None),
            fetch_tokens: AtomicU64::new(1),
            persist_tokens: Arc::new(AtomicU64::new(1)),
            network_warmup_done: AtomicBool::new(false),
            runtime_policy_version: AtomicU64::new(runtime_policy_version),
            runtime_policy_applied: AtomicBool::new(runtime_policy_version == 0),
            runtime_policy_lock: AsyncMutex::new(()),
            runtime_policy_rejects: AtomicU64::new(0),
            runtime_policy_rollback_ignored: AtomicU64::new(0),
            rejected_policy_version: AtomicU64::new(0),
            rejected_policy_signature: AsyncMutex::new(String::new()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.conf.enable && self.runtime.is_some()
    }

    pub fn state(&self) -> P2pState {
        match self.state.load(Ordering::Relaxed) {
            0 => P2pState::Disabled,
            1 => P2pState::Init,
            2 => P2pState::Running,
            _ => P2pState::Stopped,
        }
    }

    pub fn start(&self) -> bool {
        if self.is_enabled() {
            self.stats();
            self.data_plane.clear_shutdown();
            self.ensure_data_plane_started();
            self.network_warmup_done.store(false, Ordering::Relaxed);
            self.ensure_network_started();
            self.state.store(P2pState::Running as u8, Ordering::Relaxed);
            return true;
        }
        false
    }

    pub fn stop(&self) {
        if self.is_enabled() {
            self.state.store(P2pState::Stopped as u8, Ordering::Relaxed);
            self.network_warmup_done.store(false, Ordering::Relaxed);
            PEER_STATS.remove(&self.peer_id);
            self.data_plane.request_shutdown();
            force_reset_data_plane_listener_state(&self.data_plane);
            self.data_plane.tickets.clear();
            self.stop_network();
        }
    }

    pub fn conf(&self) -> &ClientP2pConf {
        &self.conf
    }

    pub fn has_remote_peer(&self) -> bool {
        self.has_remote_peers()
    }

    pub async fn update_runtime_policy(
        &self,
        peer_whitelist: Option<Vec<String>>,
        tenant_whitelist: Option<Vec<String>>,
    ) -> bool {
        if !self.is_enabled() {
            return false;
        }
        self.ensure_network_started();
        let tx = self
            .network_tx
            .lock()
            .ok()
            .and_then(|network_tx| network_tx.clone());
        let Some(tx) = tx else {
            return false;
        };
        let tenant_whitelist_to_apply = tenant_whitelist.clone();
        let (response_tx, response_rx) = oneshot::channel();
        if tx
            .send(NetworkCommand::UpdateRuntimePolicy {
                peer_whitelist,
                tenant_whitelist,
                response: response_tx,
            })
            .await
            .is_err()
        {
            return false;
        }
        let accepted = timeout(self.transfer_timeout(), response_rx)
            .await
            .ok()
            .and_then(|v| v.ok())
            .unwrap_or(false);
        if accepted {
            if let Some(tenant_whitelist) = tenant_whitelist_to_apply.as_ref() {
                self.data_plane.update_tenant_whitelist(tenant_whitelist);
            }
        }
        accepted
    }

    pub fn runtime_policy_version(&self) -> u64 {
        self.runtime_policy_version.load(Ordering::Relaxed)
    }

    pub async fn sync_runtime_policy_from_master(
        &self,
        policy_version: u64,
        peer_whitelist: Vec<String>,
        tenant_whitelist: Vec<String>,
        policy_signature: String,
    ) -> bool {
        if !self.is_enabled() {
            return false;
        }
        let _guard = self.runtime_policy_lock.lock().await;
        if policy_version == 0 {
            let reverted = self
                .update_runtime_policy(
                    Some(self.conf.peer_whitelist.clone()),
                    Some(self.conf.tenant_whitelist.clone()),
                )
                .await;
            if !reverted {
                self.runtime_policy_rejects.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            if !persist_runtime_policy_version(&self.conf, 0) {
                self.runtime_policy_rejects.fetch_add(1, Ordering::Relaxed);
                warn!("persist p2p runtime policy version failed, version=0");
                return false;
            }
            self.runtime_policy_version.store(0, Ordering::Relaxed);
            self.runtime_policy_applied.store(true, Ordering::Relaxed);
            self.clear_rejected_policy().await;
            return true;
        }
        let current_version = self.runtime_policy_version.load(Ordering::Relaxed);
        let runtime_policy_applied = self.runtime_policy_applied.load(Ordering::Relaxed);
        if policy_version < current_version && runtime_policy_applied {
            self.runtime_policy_rollback_ignored
                .fetch_add(1, Ordering::Relaxed);
            return true;
        }
        if policy_version == current_version && runtime_policy_applied {
            return true;
        }
        if self
            .is_repeated_rejected_policy(policy_version, &policy_signature)
            .await
        {
            return true;
        }
        if !self.verify_master_policy_signature(
            policy_version,
            &peer_whitelist,
            &tenant_whitelist,
            &policy_signature,
        ) {
            self.runtime_policy_rejects.fetch_add(1, Ordering::Relaxed);
            self.remember_rejected_policy(policy_version, &policy_signature)
                .await;
            warn!(
                "reject p2p policy update due to signature mismatch, version={}",
                policy_version
            );
            return false;
        }
        if !self
            .update_runtime_policy(Some(peer_whitelist), Some(tenant_whitelist))
            .await
        {
            self.runtime_policy_rejects.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        if !persist_runtime_policy_version(&self.conf, policy_version) {
            self.runtime_policy_rejects.fetch_add(1, Ordering::Relaxed);
            warn!(
                "failed to persist runtime policy version={}, retry on next sync",
                policy_version
            );
            return false;
        }
        self.runtime_policy_version
            .store(policy_version, Ordering::Relaxed);
        self.runtime_policy_applied.store(true, Ordering::Relaxed);
        self.clear_rejected_policy().await;
        true
    }

    fn verify_master_policy_signature(
        &self,
        policy_version: u64,
        peer_whitelist: &[String],
        tenant_whitelist: &[String],
        policy_signature: &str,
    ) -> bool {
        CommonUtils::verify_p2p_policy_signatures(
            &self.conf.policy_hmac_key,
            policy_version,
            peer_whitelist,
            tenant_whitelist,
            policy_signature,
        )
    }

    async fn is_repeated_rejected_policy(
        &self,
        policy_version: u64,
        policy_signature: &str,
    ) -> bool {
        if self.rejected_policy_version.load(Ordering::Relaxed) != policy_version {
            return false;
        }
        let rejected_signature = self.rejected_policy_signature.lock().await;
        rejected_signature.as_str() == policy_signature
    }

    async fn remember_rejected_policy(&self, policy_version: u64, policy_signature: &str) {
        self.rejected_policy_version
            .store(policy_version, Ordering::Relaxed);
        let mut rejected_signature = self.rejected_policy_signature.lock().await;
        rejected_signature.clear();
        rejected_signature.push_str(policy_signature);
    }

    async fn clear_rejected_policy(&self) {
        self.rejected_policy_version.store(0, Ordering::Relaxed);
        self.rejected_policy_signature.lock().await.clear();
    }

    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    pub fn bootstrap_peer_addr(&self) -> Option<String> {
        let peer_id = PeerId::from_str(self.network_state.local_peer_id.get()?).ok()?;
        let mut multiaddr = self
            .network_state
            .listen_addr
            .get()
            .and_then(|addr| Multiaddr::from_str(addr).ok())
            .or_else(|| {
                self.conf
                    .listen_addrs
                    .iter()
                    .find_map(|addr| Multiaddr::from_str(addr).ok())
            })?;
        if multiaddr.iter().any(|protocol| match protocol {
            Protocol::Tcp(port) => port == 0,
            Protocol::Udp(port) => port == 0,
            _ => false,
        }) {
            return None;
        }
        if multiaddr
            .iter()
            .any(|protocol| matches!(protocol, Protocol::P2p(_)))
        {
            return Some(multiaddr.to_string());
        }
        multiaddr.push(Protocol::P2p(peer_id));
        Some(multiaddr.to_string())
    }

    pub fn snapshot(&self) -> P2pStatsSnapshot {
        let active_peers = self.network_state.active_peers.load(Ordering::Relaxed) as usize;
        let stats = self.stats();
        let cache_snapshot = self.cache_manager.snapshot();
        let pending_chunks = self.pending_fetched.len();
        let pending_usage_bytes: u64 = self
            .pending_fetched
            .iter()
            .map(|entry| match entry.value() {
                PendingFetchedChunk::Ready { data, .. } => data.len() as u64,
                PendingFetchedChunk::Loading(_) => 0,
            })
            .sum();
        let cache_usage_bytes = cache_snapshot
            .usage_bytes
            .saturating_add(pending_usage_bytes);
        let cache_usage_ratio = if cache_snapshot.capacity_bytes == 0 {
            0.0
        } else {
            cache_usage_bytes as f64 * 100.0 / cache_snapshot.capacity_bytes as f64
        };
        let bytes_sent = stats.bytes_sent.load(Ordering::Relaxed);
        let bytes_recv = stats.bytes_recv.load(Ordering::Relaxed);
        let latency_us_total = stats.latency_us_total.load(Ordering::Relaxed);
        let latency_samples = stats.latency_samples.load(Ordering::Relaxed);
        let avg_peer_latency_ms = if latency_samples == 0 {
            0.0
        } else {
            latency_us_total as f64 / latency_samples as f64 / 1000.0
        };

        let mdns_peers = self.network_state.mdns_peers.load(Ordering::Relaxed) as usize;
        let dht_peers = self.network_state.dht_peers.load(Ordering::Relaxed) as usize;
        let bootstrap_connected = self
            .network_state
            .bootstrap_connected
            .load(Ordering::Relaxed) as usize;

        P2pStatsSnapshot {
            active_peers,
            mdns_peers,
            dht_peers,
            bootstrap_connected,
            bytes_sent,
            bytes_recv,
            avg_peer_latency_ms,
            cache_usage_bytes,
            cache_capacity_bytes: cache_snapshot.capacity_bytes,
            cache_usage_ratio,
            cached_chunks_count: cache_snapshot
                .cached_chunks_count
                .saturating_add(pending_chunks),
            expired_chunks: cache_snapshot.expired_chunks,
            invalidations: cache_snapshot.invalidations,
            mtime_mismatches: cache_snapshot
                .mtime_mismatches
                .saturating_add(self.network_mtime_mismatches.load(Ordering::Relaxed)),
            checksum_failures: cache_snapshot
                .checksum_failures
                .saturating_add(self.network_checksum_failures.load(Ordering::Relaxed)),
            corruption_count: cache_snapshot.corruption_count,
            policy_rejects: self.runtime_policy_rejects.load(Ordering::Relaxed),
            policy_rollback_ignored: self.runtime_policy_rollback_ignored.load(Ordering::Relaxed),
        }
    }

    fn cache_hot_published_chunk(
        &self,
        chunk_id: ChunkId,
        data: Bytes,
        mtime: i64,
        checksum: Bytes,
    ) {
        let now_ms = LocalTime::mills() as i64;
        let ttl_ms = hot_published_ttl(&self.conf).as_millis().max(1) as i64;
        self.hot_published_chunks.insert(
            chunk_id,
            HotPublishedChunk {
                data,
                mtime,
                checksum,
                expire_at_ms: now_ms.saturating_add(ttl_ms),
            },
        );
        let limit = hot_published_entries_limit(&self.conf);
        let mut evicted = Vec::new();
        if let Ok(mut order) = self.hot_published_order.lock() {
            order.push_back(chunk_id);
            while order.len() > limit {
                if let Some(evicted_chunk_id) = order.pop_front() {
                    evicted.push(evicted_chunk_id);
                }
            }
        }
        for chunk_id in evicted {
            self.hot_published_chunks.remove(&chunk_id);
        }
    }

    pub fn publish_chunk(&self, chunk_id: ChunkId, data: Bytes, mtime: i64) -> bool {
        if !self.is_enabled() || self.state() != P2pState::Running || data.is_empty() {
            return false;
        }
        let checksum = if self.conf.enable_checksum {
            sha256_bytes(data.as_ref())
        } else {
            Bytes::new()
        };
        self.cache_hot_published_chunk(chunk_id, data.clone(), mtime, checksum.clone());
        if let Some(runtime) = &self.runtime {
            self.ensure_network_started();
            let tx = self
                .network_tx
                .lock()
                .ok()
                .and_then(|network_tx| network_tx.clone());
            let Some(tx) = tx else {
                return false;
            };
            let queued = self
                .pending_publish_tasks
                .fetch_add(1, Ordering::Relaxed)
                .saturating_add(1);
            if queued > publish_task_limit(&self.conf) {
                self.pending_publish_tasks.fetch_sub(1, Ordering::Relaxed);
                return false;
            }
            let cache_manager = self.cache_manager.clone();
            let pending_publish_tasks = self.pending_publish_tasks.clone();
            let publish_inflight = self.publish_inflight.clone();
            let publish_timeout = self.transfer_timeout();
            let checksum_hint = checksum;
            runtime.spawn(async move {
                let permit = timeout(publish_timeout, publish_inflight.acquire_owned())
                    .await
                    .ok()
                    .and_then(|permit| permit.ok());
                if let Some(permit) = permit {
                    let published = tokio::task::spawn_blocking(move || {
                        cache_manager.put_fetched_with_result_and_checksum(
                            chunk_id,
                            data,
                            mtime,
                            Some(checksum_hint),
                        )
                    })
                    .await
                    .ok()
                    .unwrap_or(CachePutResultTag::Failed);
                    if published != CachePutResultTag::Failed {
                        let _ = timeout(
                            publish_timeout,
                            tx.send(NetworkCommand::Publish { chunk_id }),
                        )
                        .await;
                    }
                    drop(permit);
                }
                pending_publish_tasks.fetch_sub(1, Ordering::Relaxed);
            });
            return true;
        }
        match self.cache_manager.put_fetched_with_result_and_checksum(
            chunk_id,
            data,
            mtime,
            Some(checksum),
        ) {
            CachePutResultTag::Stored | CachePutResultTag::Refreshed => {
                self.send_network_command(NetworkCommand::Publish { chunk_id })
            }
            CachePutResultTag::Failed => false,
        }
    }

    pub async fn fetch_chunk(
        &self,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
    ) -> Option<Bytes> {
        self.fetch_chunk_with_context(chunk_id, max_len, expected_mtime, None)
            .await
    }

    pub async fn fetch_chunk_with_context(
        &self,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
        context: Option<&P2pReadTraceContext>,
    ) -> Option<Bytes> {
        self.fetch_chunk_with_prefetch_context(chunk_id, max_len, expected_mtime, context)
            .await
    }

    pub(crate) async fn fetch_chunk_with_prefetch_context(
        &self,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
        context: Option<&P2pReadTraceContext>,
    ) -> Option<Bytes> {
        if !self.is_enabled() || self.state() != P2pState::Running || max_len == 0 {
            return None;
        }
        self.maybe_wait_network_ready().await;
        let fetch_token = self.next_fetch_token();
        let trace = TraceLabels::from_context(fetch_token, context, &self.conf);
        let lookup_start = LocalTime::nanos();

        if let Some(chunk) = self
            .pending_fetched_chunk(chunk_id, max_len, expected_mtime)
            .await
        {
            emit_p2p_trace(
                "cache_get",
                "pending_hit",
                &trace,
                chunk_id,
                None,
                Some(((LocalTime::nanos() - lookup_start) as f64) / 1_000_000.0),
                Some(chunk.len()),
                None,
            );
            return Some(chunk);
        }

        if let Some(chunk) = self
            .cache_manager
            .get(chunk_id, max_len, expected_mtime)
            .chunk
        {
            emit_p2p_trace(
                "cache_get",
                "cache_hit",
                &trace,
                chunk_id,
                None,
                Some(((LocalTime::nanos() - lookup_start) as f64) / 1_000_000.0),
                Some(chunk.data.len()),
                None,
            );
            return Some(chunk.data);
        }

        let flight_lock = self.fetch_flight_lock(chunk_id);
        let flight_guard =
            match timeout(self.transfer_timeout(), flight_lock.clone().lock_owned()).await {
                Ok(guard) => guard,
                Err(_) => {
                    self.cleanup_fetch_flight(&chunk_id, &flight_lock);
                    return None;
                }
            };

        if let Some(chunk) = self
            .cache_manager
            .get(chunk_id, max_len, expected_mtime)
            .chunk
        {
            emit_p2p_trace(
                "cache_get",
                "cache_hit_after_flight",
                &trace,
                chunk_id,
                None,
                Some(((LocalTime::nanos() - lookup_start) as f64) / 1_000_000.0),
                Some(chunk.data.len()),
                None,
            );
            drop(flight_guard);
            self.cleanup_fetch_flight(&chunk_id, &flight_lock);
            return Some(chunk.data);
        }

        if let Some(chunk) = self
            .pending_fetched_chunk(chunk_id, max_len, expected_mtime)
            .await
        {
            emit_p2p_trace(
                "cache_get",
                "pending_hit_after_flight",
                &trace,
                chunk_id,
                None,
                Some(((LocalTime::nanos() - lookup_start) as f64) / 1_000_000.0),
                Some(chunk.len()),
                None,
            );
            drop(flight_guard);
            self.cleanup_fetch_flight(&chunk_id, &flight_lock);
            return Some(chunk);
        }

        if let Some(limiter) = &self.qps_limiter {
            if !limiter.acquire(self.transfer_timeout()).await {
                drop(flight_guard);
                self.cleanup_fetch_flight(&chunk_id, &flight_lock);
                return None;
            }
        }
        let permit = timeout(
            self.transfer_timeout(),
            self.inflight.clone().acquire_owned(),
        )
        .await
        .ok()
        .and_then(|permit| permit.ok());
        let Some(permit) = permit else {
            drop(flight_guard);
            self.cleanup_fetch_flight(&chunk_id, &flight_lock);
            return None;
        };
        emit_p2p_trace(
            "discover", "begin", &trace, chunk_id, None, None, None, None,
        );
        if let Some(mut chunk) = self
            .fetch_chunk_from_network_with_hedge(
                fetch_token,
                chunk_id,
                max_len,
                expected_mtime,
                trace.clone(),
            )
            .await
        {
            if let Some(mtime) = expected_mtime {
                if mtime > 0 && chunk.mtime > 0 && chunk.mtime != mtime {
                    self.network_mtime_mismatches
                        .fetch_add(1, Ordering::Relaxed);
                    emit_p2p_trace(
                        "validate",
                        "mtime_mismatch",
                        &trace,
                        chunk_id,
                        None,
                        None,
                        None,
                        Some("mtime_mismatch"),
                    );
                    drop(flight_guard);
                    self.cleanup_fetch_flight(&chunk_id, &flight_lock);
                    drop(permit);
                    return None;
                }
            }

            let verify_checksum = should_verify_network_checksum(fetch_token);
            if self.conf.enable_checksum {
                if chunk.checksum.is_empty() {
                    self.network_checksum_failures
                        .fetch_add(1, Ordering::Relaxed);
                    emit_p2p_trace(
                        "validate",
                        "checksum_mismatch",
                        &trace,
                        chunk_id,
                        None,
                        None,
                        Some(chunk.data.len()),
                        Some("checksum_empty"),
                    );
                    drop(flight_guard);
                    self.cleanup_fetch_flight(&chunk_id, &flight_lock);
                    drop(permit);
                    return None;
                }
                if verify_checksum && sha256_bytes(&chunk.data) != chunk.checksum {
                    self.network_checksum_failures
                        .fetch_add(1, Ordering::Relaxed);
                    emit_p2p_trace(
                        "validate",
                        "checksum_mismatch",
                        &trace,
                        chunk_id,
                        None,
                        None,
                        Some(chunk.data.len()),
                        Some("checksum_mismatch"),
                    );
                    drop(flight_guard);
                    self.cleanup_fetch_flight(&chunk_id, &flight_lock);
                    drop(permit);
                    return None;
                }
            }
            emit_p2p_trace(
                "validate",
                "ok",
                &trace,
                chunk_id,
                None,
                None,
                Some(chunk.data.len()),
                None,
            );

            let mtime = if chunk.mtime > 0 {
                chunk.mtime
            } else {
                expected_mtime.unwrap_or(0)
            };
            self.persist_fetched_chunk(chunk_id, chunk.data.clone(), mtime, chunk.checksum.clone());
            let mut prefetched_persist_batch = Vec::with_capacity(chunk.prefetched.len());
            for prefetched in chunk.prefetched.drain(..) {
                if let Some(expected) = expected_mtime {
                    if expected > 0 && prefetched.mtime > 0 && prefetched.mtime != expected {
                        self.network_mtime_mismatches
                            .fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                }
                if self.conf.enable_checksum && prefetched.checksum.is_empty() {
                    self.network_checksum_failures
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let prefetched_mtime = if prefetched.mtime > 0 {
                    prefetched.mtime
                } else {
                    expected_mtime.unwrap_or(0)
                };
                prefetched_persist_batch.push(FetchedCacheBatchItem {
                    chunk_id: prefetched.chunk_id,
                    data: prefetched.data,
                    mtime: prefetched_mtime,
                    checksum: prefetched.checksum,
                });
            }
            self.persist_fetched_batch(chunk_id, &trace, prefetched_persist_batch);
            emit_p2p_trace(
                "cache_put",
                "ok",
                &trace,
                chunk_id,
                None,
                None,
                Some(chunk.data.len()),
                None,
            );
            drop(flight_guard);
            self.cleanup_fetch_flight(&chunk_id, &flight_lock);
            drop(permit);
            return Some(chunk.data);
        }
        emit_p2p_trace(
            "transfer",
            "miss",
            &trace,
            chunk_id,
            None,
            None,
            None,
            Some("network_miss"),
        );
        drop(flight_guard);
        self.cleanup_fetch_flight(&chunk_id, &flight_lock);
        drop(permit);
        None
    }

    async fn fetch_chunk_from_network_with_hedge(
        &self,
        fetch_token: u64,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
        trace: TraceLabels,
    ) -> Option<FetchedChunk> {
        let Some(delay) = hedge_delay(&self.conf) else {
            return self
                .fetch_chunk_from_network(fetch_token, chunk_id, max_len, expected_mtime, trace)
                .await;
        };

        let primary = self.fetch_chunk_from_network(
            fetch_token,
            chunk_id,
            max_len,
            expected_mtime,
            trace.clone(),
        );
        tokio::pin!(primary);
        let timer = sleep(delay);
        tokio::pin!(timer);
        tokio::select! {
            result = &mut primary => result,
            _ = &mut timer => {
                if let Some(limiter) = &self.qps_limiter {
                    if !limiter.acquire(self.transfer_timeout()).await {
                        return primary.await;
                    }
                }
                let secondary = self.fetch_chunk_from_network(
                    fetch_token,
                    chunk_id,
                    max_len,
                    expected_mtime,
                    trace,
                );
                tokio::pin!(secondary);
                tokio::select! {
                    primary_result = &mut primary => {
                        if primary_result.is_some() {
                            self.cancel_network_fetch(fetch_token);
                            return primary_result;
                        }
                        let secondary_result = secondary.await;
                        self.cancel_network_fetch(fetch_token);
                        secondary_result
                    },
                    secondary_result = &mut secondary => {
                        if secondary_result.is_some() {
                            self.cancel_network_fetch(fetch_token);
                            return secondary_result;
                        }
                        let primary_result = primary.await;
                        self.cancel_network_fetch(fetch_token);
                        primary_result
                    },
                }
            }
        }
    }

    async fn fetch_chunk_from_network(
        &self,
        fetch_token: u64,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
        trace: TraceLabels,
    ) -> Option<FetchedChunk> {
        self.ensure_network_started();
        let tx = self
            .network_tx
            .lock()
            .ok()
            .and_then(|network_tx| network_tx.clone())?;
        let timeout_budget = self.transfer_timeout();
        let deadline = Instant::now() + timeout_budget;
        let (response_tx, response_rx) = oneshot::channel();
        timeout(
            timeout_budget,
            tx.send(NetworkCommand::Fetch {
                fetch_token,
                chunk_id,
                max_len,
                expected_mtime,
                trace,
                response: response_tx,
            }),
        )
        .await
        .ok()?
        .ok()?;
        let wait_budget = deadline.saturating_duration_since(Instant::now());
        if wait_budget.is_zero() {
            return None;
        }
        timeout(wait_budget, response_rx).await.ok()?.ok()?
    }

    fn next_fetch_token(&self) -> u64 {
        self.fetch_tokens.fetch_add(1, Ordering::Relaxed)
    }

    fn has_remote_peers(&self) -> bool {
        self.network_state
            .active_peers
            .load(Ordering::Relaxed)
            .saturating_sub(1)
            > 0
            || self
                .network_state
                .bootstrap_connected
                .load(Ordering::Relaxed)
                > 0
    }

    async fn maybe_wait_network_ready(&self) {
        if self.network_warmup_done.load(Ordering::Relaxed)
            || (!self.conf.enable_mdns
                && self.conf.bootstrap_peers.is_empty()
                && !self.conf.enable_dht)
            || self.has_remote_peers()
        {
            return;
        }
        if self
            .network_warmup_done
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        self.ensure_network_started();
        let timeout_budget = network_warmup_timeout(&self.conf);
        if timeout_budget.is_zero() || self.has_remote_peers() {
            return;
        }
        let notified = self.network_ready_notify.notified();
        if self.has_remote_peers() {
            return;
        }
        let _ = timeout(timeout_budget, notified).await;
    }

    fn next_persist_token(&self) -> u64 {
        self.persist_tokens.fetch_add(1, Ordering::Relaxed)
    }

    fn cancel_network_fetch(&self, fetch_token: u64) {
        let _ = self.send_network_command(NetworkCommand::CancelFetch { fetch_token });
    }

    fn ensure_data_plane_started(&self) {
        if self.runtime.is_none() || !self.is_enabled() {
            return;
        }
        if self
            .data_plane
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let generation = self.data_plane.next_generation();
        let data_plane = self.data_plane.clone();
        let cache_manager = self.cache_manager.clone();
        let pending_fetched = self.pending_fetched.clone();
        let hot_published_chunks = self.hot_published_chunks.clone();
        let tenant_whitelist = Arc::new(parse_tenant_whitelist(&self.conf.tenant_whitelist));
        let conf = self.conf.clone();
        let stats = self.stats();
        let future = async move {
            let mut shutdown_rx = data_plane.subscribe_shutdown();
            loop {
                let listener = match TcpListener::bind(("0.0.0.0", 0)).await {
                    Ok(listener) => listener,
                    Err(e) => {
                        clear_data_plane_listener_port(&data_plane, generation);
                        warn!("failed to bind p2p data plane listener: {}", e);
                        tokio::select! {
                            _ = shutdown_rx.changed() => break,
                            _ = tokio::time::sleep(Duration::from_millis(200)) => continue,
                        }
                    }
                };
                if let Ok(addr) = listener.local_addr() {
                    set_data_plane_listener_port(&data_plane, generation, addr.port());
                }
                loop {
                    tokio::select! {
                        _ = shutdown_rx.changed() => {
                            reset_data_plane_listener_state(&data_plane, generation);
                            return;
                        }
                        accepted = listener.accept() => {
                            match accepted {
                                Ok((stream, _)) => handle_data_plane_connection(
                                    stream,
                                    data_plane.clone(),
                                    cache_manager.clone(),
                                    pending_fetched.clone(),
                                    hot_published_chunks.clone(),
                                    tenant_whitelist.clone(),
                                    conf.clone(),
                                    stats.clone(),
                                ),
                                Err(e) => {
                                    clear_data_plane_listener_port(&data_plane, generation);
                                    warn!("p2p data plane accept failed: {}", e);
                                    tokio::select! {
                                        _ = shutdown_rx.changed() => {
                                            reset_data_plane_listener_state(&data_plane, generation);
                                            return;
                                        }
                                        _ = tokio::time::sleep(Duration::from_millis(200)) => break,
                                    }
                                }
                            }
                        }
                    }
                }
            }
            reset_data_plane_listener_state(&data_plane, generation);
        };
        if let Some(runtime) = &self.runtime {
            runtime.spawn(future);
        }
    }

    fn ensure_network_started(&self) {
        if self.runtime.is_none() || !self.is_enabled() {
            return;
        }
        let mut network_tx = match self.network_tx.lock() {
            Ok(v) => v,
            Err(_) => return,
        };
        if network_tx
            .as_ref()
            .is_some_and(|sender| !sender.is_closed())
        {
            return;
        }

        let (tx, rx) = mpsc::channel(self.conf.max_inflight_requests.max(64));
        *network_tx = Some(tx);
        drop(network_tx);
        self.network_warmup_done.store(false, Ordering::Relaxed);
        self.ensure_fetched_persist_started();

        let conf = self.conf.clone();
        let cache_manager = self.cache_manager.clone();
        let pending_fetched = self.pending_fetched.clone();
        let hot_published_chunks = self.hot_published_chunks.clone();
        let fetched_persist_txs = self
            .fetched_persist_txs
            .lock()
            .ok()
            .map(|persist_txs| persist_txs.clone())
            .unwrap_or_default();
        let persist_tokens = self.persist_tokens.clone();
        let data_plane = self.data_plane.clone();
        let network_ready_notify = self.network_ready_notify.clone();
        let network_state = self.network_state.clone();
        let stats = self.stats();
        let future = async move {
            run_network_loop(
                conf,
                cache_manager,
                pending_fetched,
                hot_published_chunks,
                fetched_persist_txs,
                persist_tokens,
                data_plane,
                rx,
                network_state,
                network_ready_notify,
                stats,
            )
            .await;
        };

        if let Some(runtime) = &self.runtime {
            runtime.spawn(future);
        }
    }

    fn send_network_command(&self, cmd: NetworkCommand) -> bool {
        self.ensure_network_started();
        let tx = self
            .network_tx
            .lock()
            .ok()
            .and_then(|network_tx| network_tx.clone());
        let Some(tx) = tx else {
            return false;
        };
        match tx.try_send(cmd) {
            Ok(_) => true,
            Err(TrySendError::Closed(_)) => false,
            Err(TrySendError::Full(_)) => false,
        }
    }

    fn stop_network(&self) {
        let tx = self
            .network_tx
            .lock()
            .ok()
            .and_then(|mut network_tx| network_tx.take());
        if let Some(tx) = tx {
            match tx.try_send(NetworkCommand::Stop) {
                Ok(_) => {}
                Err(TrySendError::Closed(_)) => {}
                Err(TrySendError::Full(_)) => {}
            }
        }
        self.network_state.active_peers.store(0, Ordering::Relaxed);
        self.network_state.mdns_peers.store(0, Ordering::Relaxed);
        self.network_state.dht_peers.store(0, Ordering::Relaxed);
        self.network_state
            .bootstrap_connected
            .store(0, Ordering::Relaxed);
        self.network_warmup_done.store(false, Ordering::Relaxed);
        if let Ok(mut persist_txs) = self.fetched_persist_txs.lock() {
            persist_txs.clear();
        }
        self.hot_published_chunks.clear();
        if let Ok(mut order) = self.hot_published_order.lock() {
            order.clear();
        }
    }

    fn transfer_timeout(&self) -> Duration {
        transfer_timeout(&self.conf)
    }

    fn ready_pending_fetched_chunk(
        &self,
        data: Bytes,
        mtime: i64,
        max_len: usize,
        expected_mtime: Option<i64>,
    ) -> Option<Bytes> {
        if expected_mtime.is_some_and(|expected| expected > 0 && mtime > 0 && mtime != expected) {
            self.network_mtime_mismatches
                .fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let len = data.len().min(max_len.max(1));
        Some(data.slice(0..len))
    }

    async fn pending_fetched_chunk(
        &self,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
    ) -> Option<Bytes> {
        let pending = self.pending_fetched.get(&chunk_id)?.clone();
        match pending {
            PendingFetchedChunk::Ready { data, mtime, .. } => {
                self.ready_pending_fetched_chunk(data, mtime, max_len, expected_mtime)
            }
            PendingFetchedChunk::Loading(loading) => {
                let mut receiver = loading.receiver.clone();
                let wait = timeout(self.transfer_timeout(), async {
                    loop {
                        let state = receiver.borrow().clone();
                        match state {
                            PendingFetchedLoadingState::Pending => {
                                if receiver.changed().await.is_err() {
                                    return None;
                                }
                            }
                            PendingFetchedLoadingState::Ready { data, mtime } => {
                                return self.ready_pending_fetched_chunk(
                                    data,
                                    mtime,
                                    max_len,
                                    expected_mtime,
                                );
                            }
                            PendingFetchedLoadingState::Failed => return None,
                        }
                    }
                })
                .await;
                wait.ok().flatten()
            }
        }
    }

    fn persist_fetched_chunk(&self, chunk_id: ChunkId, data: Bytes, mtime: i64, checksum: Bytes) {
        let persist_token = self.next_persist_token();
        self.pending_fetched.insert(
            chunk_id,
            PendingFetchedChunk::ready(data.clone(), mtime, persist_token),
        );
        let request = PendingFetchedPersist {
            chunk_id,
            data,
            mtime,
            checksum,
            persist_token,
        };
        if self.enqueue_fetched_persist(request.clone()) {
            return;
        }
        self.persist_fetched_chunk_direct(request);
    }

    fn enqueue_fetched_persist(&self, request: PendingFetchedPersist) -> bool {
        if !self.is_enabled() || self.runtime.is_none() {
            return false;
        }
        self.ensure_fetched_persist_started();
        let txs = self
            .fetched_persist_txs
            .lock()
            .ok()
            .map(|persist_txs| persist_txs.clone())
            .unwrap_or_default();
        if txs.is_empty() {
            return false;
        }
        let worker_idx = fetched_persist_worker_index(request.chunk_id, txs.len());
        txs[worker_idx].try_send(request).is_ok()
    }

    fn persist_fetched_chunk_direct(&self, request: PendingFetchedPersist) {
        if let Some(runtime) = &self.runtime {
            let cache_manager = self.cache_manager.clone();
            let pending_fetched = self.pending_fetched.clone();
            let persist_inflight = self.publish_inflight.clone();
            let persist_timeout = self.transfer_timeout();
            runtime.spawn(async move {
                let permit = timeout(persist_timeout, persist_inflight.acquire_owned())
                    .await
                    .ok()
                    .and_then(|permit| permit.ok());
                if let Some(permit) = permit {
                    let _ = tokio::task::spawn_blocking(move || {
                        cache_manager.put_fetched_with_result_and_checksum(
                            request.chunk_id,
                            request.data,
                            request.mtime,
                            Some(request.checksum),
                        )
                    })
                    .await;
                    drop(permit);
                }
                cleanup_pending_fetched_entry(
                    &pending_fetched,
                    request.chunk_id,
                    request.persist_token,
                );
            });
            return;
        }
        let _ = self.cache_manager.put_fetched_with_result_and_checksum(
            request.chunk_id,
            request.data,
            request.mtime,
            Some(request.checksum),
        );
        cleanup_pending_fetched_entry(
            &self.pending_fetched,
            request.chunk_id,
            request.persist_token,
        );
    }

    fn persist_fetched_batch(
        &self,
        chunk_id: ChunkId,
        trace: &TraceLabels,
        batch: Vec<FetchedCacheBatchItem>,
    ) {
        if batch.is_empty() {
            return;
        }
        let batch_bytes = batch.iter().map(|item| item.data.len()).sum::<usize>();
        let sync_start = LocalTime::nanos();
        let (pending_entries, completed) =
            build_pending_fetched_batch(&batch, || self.next_persist_token());
        for (chunk_id, pending) in pending_entries {
            self.pending_fetched.insert(chunk_id, pending);
        }
        emit_p2p_trace(
            "cache_put",
            "prefetched_pending_visible",
            trace,
            chunk_id,
            None,
            Some(((LocalTime::nanos() - sync_start) as f64) / 1_000_000.0),
            Some(batch_bytes),
            None,
        );
        if let Some(runtime) = &self.runtime {
            let cache_manager = self.cache_manager.clone();
            let pending_fetched = self.pending_fetched.clone();
            let persist_inflight = self.publish_inflight.clone();
            let persist_timeout = self.transfer_timeout();
            runtime.spawn(async move {
                let permit = timeout(persist_timeout, persist_inflight.acquire_owned())
                    .await
                    .ok()
                    .and_then(|permit| permit.ok());
                if let Some(permit) = permit {
                    let _ = tokio::task::spawn_blocking(move || {
                        let _ = cache_manager.put_fetched_batch_with_checksum(batch);
                    })
                    .await;
                    drop(permit);
                }
                for (chunk_id, persist_token) in completed {
                    cleanup_pending_fetched_entry(&pending_fetched, chunk_id, persist_token);
                }
            });
            return;
        }
        let _ = self.cache_manager.put_fetched_batch_with_checksum(batch);
        for (chunk_id, persist_token) in completed {
            cleanup_pending_fetched_entry(&self.pending_fetched, chunk_id, persist_token);
        }
    }

    fn ensure_fetched_persist_started(&self) {
        if self.runtime.is_none() || !self.is_enabled() {
            return;
        }
        let mut persist_txs = match self.fetched_persist_txs.lock() {
            Ok(v) => v,
            Err(_) => return,
        };
        if persist_txs.iter().any(mpsc::Sender::is_closed) {
            persist_txs.clear();
        }
        if !persist_txs.is_empty() {
            return;
        }
        let worker_count = fetched_persist_worker_count(&self.conf);
        let batch_size = fetched_persist_batch_size(&self.conf);
        let max_batch_size = fetched_persist_max_batch_size(&self.conf);
        let queue_capacity = fetched_persist_queue_capacity(&self.conf)
            .div_ceil(worker_count.max(1))
            .max(batch_size);
        let mut receivers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let (tx, rx) = mpsc::channel(queue_capacity);
            persist_txs.push(tx);
            receivers.push(rx);
        }
        drop(persist_txs);

        if let Some(runtime) = &self.runtime {
            for rx in receivers {
                let cache_manager = self.cache_manager.clone();
                let pending_fetched = self.pending_fetched.clone();
                let batch_window = fetched_persist_batch_window(&self.conf);
                runtime.spawn(async move {
                    run_fetched_persist_loop(
                        cache_manager,
                        pending_fetched,
                        rx,
                        batch_size,
                        max_batch_size,
                        batch_window,
                    )
                    .await;
                });
            }
        }
    }

    fn stats(&self) -> Arc<PeerStats> {
        PEER_STATS
            .entry(self.peer_id.clone())
            .or_insert_with(|| Arc::new(PeerStats::default()))
            .clone()
    }

    fn fetch_flight_lock(&self, chunk_id: ChunkId) -> Arc<AsyncMutex<()>> {
        self.fetch_flights
            .entry(chunk_id)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    fn cleanup_fetch_flight(&self, chunk_id: &ChunkId, lock: &Arc<AsyncMutex<()>>) {
        if Arc::strong_count(lock) != 2 {
            return;
        }
        if let Some((_, current)) = self.fetch_flights.remove(chunk_id) {
            if !Arc::ptr_eq(&current, lock) {
                self.fetch_flights.insert(*chunk_id, current);
            }
        }
    }
}

impl Drop for P2pService {
    fn drop(&mut self) {
        if !self.conf.enable {
            return;
        }
        PEER_STATS.remove(&self.peer_id);
        self.stop_network();
    }
}

fn normalize_trace_label(value: Option<&str>) -> String {
    match value.map(str::trim) {
        Some(v) if !v.is_empty() => v.to_string(),
        _ => "unknown".to_string(),
    }
}

fn cleanup_pending_fetched_entry(
    pending_fetched: &FastDashMap<ChunkId, PendingFetchedChunk>,
    chunk_id: ChunkId,
    persist_token: u64,
) {
    let _ = pending_fetched.remove_if(&chunk_id, |_, current| {
        current.persist_token() == persist_token
    });
}

fn new_pending_fetched_placeholder(
    chunk_id: ChunkId,
    persist_token: u64,
) -> (PendingFetchedChunk, PendingFetchedPlaceholder) {
    let (sender, receiver) = watch::channel(PendingFetchedLoadingState::Pending);
    (
        PendingFetchedChunk::Loading(Arc::new(PendingFetchedLoading {
            receiver,
            persist_token,
        })),
        PendingFetchedPlaceholder {
            chunk_id,
            sender,
            persist_token,
        },
    )
}

fn fulfill_pending_fetched_placeholder(
    pending_fetched: &FastDashMap<ChunkId, PendingFetchedChunk>,
    placeholder: PendingFetchedPlaceholder,
    data: Bytes,
    mtime: i64,
) {
    let _ = placeholder.sender.send(PendingFetchedLoadingState::Ready {
        data: data.clone(),
        mtime,
    });
    pending_fetched.insert(
        placeholder.chunk_id,
        PendingFetchedChunk::ready(data, mtime, placeholder.persist_token),
    );
}

fn fail_pending_fetched_placeholder(
    pending_fetched: &FastDashMap<ChunkId, PendingFetchedChunk>,
    placeholder: PendingFetchedPlaceholder,
) {
    let _ = placeholder.sender.send(PendingFetchedLoadingState::Failed);
    cleanup_pending_fetched_entry(
        pending_fetched,
        placeholder.chunk_id,
        placeholder.persist_token,
    );
}

fn compact_fetched_persist_batch(
    mut batch: Vec<PendingFetchedPersist>,
) -> Vec<PendingFetchedPersist> {
    if batch.len() <= 1 {
        return batch;
    }
    let mut latest_by_chunk = HashMap::with_capacity(batch.len());
    for item in batch.drain(..) {
        latest_by_chunk.insert(item.chunk_id, item);
    }
    latest_by_chunk.into_values().collect()
}

async fn run_fetched_persist_loop(
    cache_manager: Arc<CacheManager>,
    pending_fetched: Arc<FastDashMap<ChunkId, PendingFetchedChunk>>,
    mut rx: mpsc::Receiver<PendingFetchedPersist>,
    batch_size: usize,
    max_batch_size: usize,
    batch_window: Duration,
) {
    let batch_size = batch_size.max(1);
    let max_batch_size = max_batch_size.max(batch_size);
    let mut batch = Vec::with_capacity(max_batch_size);
    loop {
        batch.clear();
        let received = rx.recv_many(&mut batch, batch_size).await;
        if received == 0 {
            break;
        }
        let mut closed = false;
        if batch.len() < max_batch_size && !batch_window.is_zero() {
            let mut deadline = Instant::now() + batch_window;
            while batch.len() < max_batch_size {
                let remaining_window = deadline.saturating_duration_since(Instant::now());
                if remaining_window.is_zero() {
                    break;
                }
                let remaining_capacity = max_batch_size.saturating_sub(batch.len());
                match timeout(
                    remaining_window,
                    rx.recv_many(&mut batch, remaining_capacity),
                )
                .await
                {
                    Ok(0) => {
                        closed = true;
                        break;
                    }
                    Ok(_) => {
                        deadline = Instant::now() + batch_window;
                    }
                    Err(_) => break,
                }
            }
        }
        let write_batch = compact_fetched_persist_batch(std::mem::take(&mut batch));
        if write_batch.is_empty() {
            if closed {
                break;
            }
            continue;
        }
        let completed: Vec<(ChunkId, u64)> = write_batch
            .iter()
            .map(|item| (item.chunk_id, item.persist_token))
            .collect();
        let write_cache_manager = cache_manager.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let persist_batch: Vec<FetchedCacheBatchItem> = write_batch
                .into_iter()
                .map(|item| FetchedCacheBatchItem {
                    chunk_id: item.chunk_id,
                    data: item.data,
                    mtime: item.mtime,
                    checksum: item.checksum,
                })
                .collect();
            let _ = write_cache_manager.put_fetched_batch_with_checksum(persist_batch);
        })
        .await;
        for (chunk_id, persist_token) in completed {
            cleanup_pending_fetched_entry(&pending_fetched, chunk_id, persist_token);
        }
        if closed {
            break;
        }
    }
}

fn should_sample_trace(fetch_token: u64, conf: &ClientP2pConf) -> bool {
    if !conf.trace_enable {
        return false;
    }
    let rate = conf.trace_sample_rate;
    if rate <= 0.0 {
        return false;
    }
    if rate >= 1.0 {
        return true;
    }
    let hash = fetch_token.wrapping_mul(0x9e3779b97f4a7c15);
    let threshold = (rate * u64::MAX as f64) as u64;
    hash <= threshold
}

impl TraceLabels {
    fn from_context(
        fetch_token: u64,
        context: Option<&P2pReadTraceContext>,
        conf: &ClientP2pConf,
    ) -> Self {
        let sampled = should_sample_trace(fetch_token, conf);
        let tenant_id = normalize_trace_label(context.and_then(|ctx| ctx.tenant_id.as_deref()));
        let job_id = normalize_trace_label(context.and_then(|ctx| ctx.job_id.as_deref()));
        if !sampled {
            return Self {
                trace_id: String::new(),
                tenant_id,
                job_id,
                sampled,
            };
        }
        let trace_id = context
            .and_then(|ctx| ctx.trace_id.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .unwrap_or_else(|| format!("fetch-{}", fetch_token));
        Self {
            trace_id,
            tenant_id,
            job_id,
            sampled,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_p2p_trace(
    stage: &'static str,
    event: &'static str,
    labels: &TraceLabels,
    chunk_id: ChunkId,
    peer: Option<&PeerId>,
    elapsed_ms: Option<f64>,
    bytes: Option<usize>,
    reason: Option<&str>,
) {
    if !labels.sampled {
        return;
    }
    tracing::debug!(
        target: "curvine_client::p2p_trace",
        stage = stage,
        event = event,
        trace_id = %labels.trace_id,
        tenant_id = %labels.tenant_id,
        job_id = %labels.job_id,
        file_id = chunk_id.file_id,
        version_epoch = chunk_id.version_epoch,
        block_id = chunk_id.block_id,
        off = chunk_id.off,
        peer_id = ?peer,
        elapsed_ms = elapsed_ms.unwrap_or(0.0),
        bytes = bytes.unwrap_or(0),
        reason = reason.unwrap_or(""),
    );
}

fn handle_network_command(
    command: NetworkCommand,
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    conf: &ClientP2pConf,
    init: &mut NetworkInitState,
    loop_state: &mut NetworkLoopState,
) -> bool {
    match command {
        NetworkCommand::Publish { chunk_id } => {
            let provider_id = provider_record_id(chunk_id);
            if conf.enable_dht
                && loop_state.published_provider_records.insert(provider_id)
                && swarm
                    .behaviour_mut()
                    .kad
                    .start_providing(provider_record_key(provider_id))
                    .is_err()
            {
                loop_state.published_provider_records.remove(&provider_id);
            }
        }
        NetworkCommand::Fetch {
            fetch_token,
            chunk_id,
            max_len,
            expected_mtime,
            trace,
            response,
        } => {
            let now = Instant::now();
            let deadline = now + loop_state.transfer_timeout;
            let cached = loop_state
                .provider_cache
                .get(&chunk_id)
                .and_then(|providers| (providers.expire_at > now).then_some(providers.clone()));
            if let Some(cached) = cached {
                emit_p2p_trace(
                    "discover",
                    "provider_cache",
                    &trace,
                    chunk_id,
                    None,
                    None,
                    Some(cached.peers.len()),
                    None,
                );
                if cached.peers.is_empty() {
                    emit_p2p_trace(
                        "discover",
                        "provider_empty",
                        &trace,
                        chunk_id,
                        None,
                        None,
                        None,
                        Some("provider_empty"),
                    );
                }
                let candidates = build_candidates(
                    cached.peers,
                    &loop_state.connected_peers,
                    &loop_state.bootstrap_connected,
                    init.local_peer_id,
                    &loop_state.peer_ewma,
                    &loop_state.inflight_per_peer,
                );
                dispatch_fetch_request(
                    swarm,
                    PendingFetchRequest {
                        fetch_token,
                        chunk_id,
                        max_len,
                        expected_mtime,
                        trace,
                        response,
                        candidates,
                        active_peer: None,
                        defer_dht: false,
                        started_at: now,
                        deadline,
                    },
                    init,
                    loop_state,
                    conf,
                );
                return true;
            }
            loop_state.provider_cache.remove(&chunk_id);

            if conf.enable_dht {
                if let Some(candidates) = direct_connected_probe_candidates(
                    conf,
                    &loop_state.connected_peers,
                    &loop_state.bootstrap_connected,
                    init.local_peer_id,
                    &loop_state.peer_ewma,
                    &loop_state.inflight_per_peer,
                ) {
                    emit_p2p_trace(
                        "discover",
                        "direct_connected",
                        &trace,
                        chunk_id,
                        None,
                        None,
                        Some(candidates.len()),
                        None,
                    );
                    dispatch_fetch_request(
                        swarm,
                        PendingFetchRequest {
                            fetch_token,
                            chunk_id,
                            max_len,
                            expected_mtime,
                            trace,
                            response,
                            candidates,
                            active_peer: None,
                            defer_dht: true,
                            started_at: now,
                            deadline,
                        },
                        init,
                        loop_state,
                        conf,
                    );
                    return true;
                }
                enqueue_provider_query(
                    swarm,
                    loop_state,
                    fetch_token,
                    chunk_id,
                    max_len,
                    expected_mtime,
                    trace,
                    response,
                );
            } else {
                emit_p2p_trace(
                    "discover",
                    "skip_dht",
                    &trace,
                    chunk_id,
                    None,
                    None,
                    None,
                    Some("dht_disabled"),
                );
                let candidates = build_candidates(
                    std::iter::empty::<PeerId>(),
                    &loop_state.connected_peers,
                    &loop_state.bootstrap_connected,
                    init.local_peer_id,
                    &loop_state.peer_ewma,
                    &loop_state.inflight_per_peer,
                );
                dispatch_fetch_request(
                    swarm,
                    PendingFetchRequest {
                        fetch_token,
                        chunk_id,
                        max_len,
                        expected_mtime,
                        trace,
                        response,
                        candidates,
                        active_peer: None,
                        defer_dht: false,
                        started_at: now,
                        deadline,
                    },
                    init,
                    loop_state,
                    conf,
                );
            }
        }
        NetworkCommand::CancelFetch { fetch_token } => {
            cancel_pending_by_token(
                fetch_token,
                &mut loop_state.pending_queries,
                &mut loop_state.pending_requests,
                &mut loop_state.pending_stream_requests,
                &mut loop_state.pending_data_requests,
                &mut loop_state.inflight_per_peer,
            );
        }
        NetworkCommand::UpdateRuntimePolicy {
            peer_whitelist: updated_peer_whitelist,
            tenant_whitelist: updated_tenant_whitelist,
            response,
        } => {
            let mut accepted = true;
            if let Some(updated_peer_whitelist) = updated_peer_whitelist {
                if let Some(parsed_peer_whitelist) = parse_peer_whitelist(&updated_peer_whitelist) {
                    init.peer_whitelist = build_effective_peer_whitelist(
                        parsed_peer_whitelist,
                        &init.bootstrap_peer_ids,
                    );

                    let to_disconnect: Vec<PeerId> = loop_state
                        .connected_peers
                        .iter()
                        .copied()
                        .filter(|peer| {
                            !is_peer_allowed(*peer, init.local_peer_id, &init.peer_whitelist)
                        })
                        .collect();
                    for peer in to_disconnect {
                        loop_state.connected_peers.remove(&peer);
                        loop_state.mdns_peers.remove(&peer);
                        loop_state.dht_peers.remove(&peer);
                        loop_state.bootstrap_connected.remove(&peer);
                        loop_state.inflight_per_peer.remove(&peer);
                        loop_state.peer_ewma.remove(&peer);
                        for providers in loop_state.provider_cache.values_mut() {
                            providers.peers.remove(&peer);
                        }
                        let _ = swarm.disconnect_peer_id(peer);
                    }
                } else {
                    accepted = false;
                }
            }
            if accepted {
                if let Some(updated_tenant_whitelist) = updated_tenant_whitelist {
                    init.tenant_whitelist =
                        Arc::new(parse_tenant_whitelist(&updated_tenant_whitelist));
                }
            }
            let _ = response.send(accepted);
        }
        NetworkCommand::Stop => {
            if conf.enable_dht {
                for provider_id in loop_state.published_provider_records.drain() {
                    swarm
                        .behaviour_mut()
                        .kad
                        .stop_providing(&provider_record_key(provider_id));
                }
            }
            return false;
        }
    }
    true
}

struct SwarmEventCtx<'a> {
    swarm: &'a mut libp2p::Swarm<P2pNetworkBehaviour>,
    conf: &'a ClientP2pConf,
    data_plane: &'a DataPlaneState,
    cache_manager: &'a Arc<CacheManager>,
    pending_fetched: &'a Arc<FastDashMap<ChunkId, PendingFetchedChunk>>,
    hot_published_chunks: &'a Arc<FastDashMap<ChunkId, HotPublishedChunk>>,
    init: &'a NetworkInitState,
    loop_state: &'a mut NetworkLoopState,
    incoming_tx: &'a mpsc::Sender<PreparedIncomingResponse>,
    stats: &'a Arc<PeerStats>,
}

fn handle_swarm_event(event: SwarmEvent<P2pNetworkEvent>, ctx: &mut SwarmEventCtx<'_>) -> bool {
    match event {
        SwarmEvent::Behaviour(P2pNetworkEvent::RequestResponse(event)) => {
            let mut rr_ctx = RequestResponseEventCtx {
                swarm: ctx.swarm,
                data_plane: ctx.data_plane,
                cache_manager: ctx.cache_manager,
                pending_fetched: ctx.pending_fetched,
                hot_published_chunks: ctx.hot_published_chunks,
                conf: ctx.conf,
                init: ctx.init,
                loop_state: ctx.loop_state,
                incoming_tx: ctx.incoming_tx,
                stats: ctx.stats,
            };
            handle_request_response_event(event, &mut rr_ctx);
        }
        SwarmEvent::Behaviour(P2pNetworkEvent::Identify(event)) => {
            handle_identify_event(event, ctx.swarm, ctx.init, ctx.loop_state);
        }
        SwarmEvent::Behaviour(P2pNetworkEvent::Mdns(event)) => {
            if ctx.conf.enable_mdns {
                handle_mdns_event(
                    event,
                    ctx.swarm,
                    ctx.conf.enable_dht,
                    ctx.init.local_peer_id,
                    &ctx.init.peer_whitelist,
                    &mut ctx.loop_state.mdns_peers,
                );
            }
        }
        SwarmEvent::Behaviour(P2pNetworkEvent::Kad(event)) => {
            if ctx.conf.enable_dht {
                handle_kad_event(event, ctx.swarm, ctx.conf, ctx.init, ctx.loop_state);
            }
        }
        SwarmEvent::Behaviour(P2pNetworkEvent::Ignore) => {}
        SwarmEvent::ConnectionEstablished {
            peer_id,
            connection_id,
            endpoint,
            ..
        } => {
            if !handle_connection_established(
                ctx.swarm,
                peer_id,
                connection_id,
                endpoint.get_remote_address().clone(),
                ctx.init,
                ctx.loop_state,
                ctx.conf.max_active_peers,
            ) {
                return false;
            }
        }
        SwarmEvent::ConnectionClosed { peer_id, .. } => {
            handle_connection_closed(peer_id, ctx.loop_state);
        }
        SwarmEvent::NewListenAddr { .. } => {}
        _ => {}
    }
    true
}

fn handle_mdns_event(
    event: mdns::Event,
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    enable_dht: bool,
    local_peer_id: PeerId,
    peer_whitelist: &HashSet<PeerId>,
    mdns_peers: &mut HashSet<PeerId>,
) {
    match event {
        mdns::Event::Discovered(entries) => {
            for (peer, addr) in entries {
                if !is_peer_allowed(peer, local_peer_id, peer_whitelist) || peer == local_peer_id {
                    continue;
                }
                mdns_peers.insert(peer);
                if enable_dht {
                    swarm
                        .behaviour_mut()
                        .kad
                        .add_address(&peer, strip_peer_id(&addr));
                }
                let _ = swarm.dial(addr);
            }
        }
        mdns::Event::Expired(entries) => {
            for (peer, _) in entries {
                mdns_peers.remove(&peer);
            }
        }
    }
}

fn handle_identify_event(
    event: identify::Event,
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    init: &NetworkInitState,
    loop_state: &mut NetworkLoopState,
) {
    let identify::Event::Received { peer_id, info, .. } = event else {
        return;
    };
    if !is_peer_allowed(peer_id, init.local_peer_id, &init.peer_whitelist) {
        return;
    }
    let Some(remote_addr) = loop_state.peer_remote_addrs.get(&peer_id) else {
        return;
    };
    for listen_addr in info.listen_addrs {
        if let Some(dial_addr) = identify_dial_addr(&listen_addr, remote_addr) {
            swarm.behaviour_mut().kad.add_address(&peer_id, dial_addr);
        }
    }
}

fn handle_kad_event(
    event: kad::Event,
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    conf: &ClientP2pConf,
    init: &NetworkInitState,
    loop_state: &mut NetworkLoopState,
) {
    match event {
        kad::Event::RoutingUpdated { peer, .. } => {
            loop_state.dht_peers.insert(peer);
        }
        kad::Event::OutboundQueryProgressed {
            id, result, step, ..
        } => {
            if let Some(pending) = loop_state.pending_queries.remove(&id) {
                let (providers, keep_waiting) = parse_provider_query_outcome(
                    result,
                    step.last,
                    init.local_peer_id,
                    &init.peer_whitelist,
                );

                if keep_waiting {
                    loop_state.pending_queries.insert(id, pending);
                } else {
                    finalize_provider_query(swarm, pending, providers, init, loop_state, conf);
                }
            }
        }
        _ => {}
    }
}

fn parse_provider_query_outcome(
    result: kad::QueryResult,
    step_last: bool,
    local_peer_id: PeerId,
    peer_whitelist: &HashSet<PeerId>,
) -> (HashSet<PeerId>, bool) {
    let mut providers = HashSet::new();
    let mut keep_waiting = !step_last;
    match result {
        kad::QueryResult::GetProviders(Ok(ok)) => match ok {
            kad::GetProvidersOk::FoundProviders {
                providers: found, ..
            } => {
                providers.extend(found);
                providers.retain(|peer| is_peer_allowed(*peer, local_peer_id, peer_whitelist));
                keep_waiting = false;
            }
            kad::GetProvidersOk::FinishedWithNoAdditionalRecord { .. } => {}
        },
        kad::QueryResult::GetProviders(Err(_)) => {
            keep_waiting = false;
        }
        _ => {
            keep_waiting = false;
        }
    }
    (providers, keep_waiting)
}

fn finalize_provider_query(
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    pending: PendingProviderQuery,
    providers: HashSet<PeerId>,
    init: &NetworkInitState,
    loop_state: &mut NetworkLoopState,
    conf: &ClientP2pConf,
) {
    emit_p2p_trace(
        "discover",
        "dht_result",
        &pending.trace,
        pending.chunk_id,
        None,
        None,
        Some(providers.len()),
        None,
    );
    loop_state.provider_cache.insert(
        pending.chunk_id,
        CachedProviders {
            peers: providers.clone(),
            expire_at: Instant::now()
                + if providers.is_empty() {
                    negative_provider_cache_ttl_from_discovery(loop_state.discovery_timeout)
                } else {
                    loop_state.provider_cache_ttl
                },
        },
    );
    let candidates = build_candidates(
        providers,
        &loop_state.connected_peers,
        &loop_state.bootstrap_connected,
        init.local_peer_id,
        &loop_state.peer_ewma,
        &loop_state.inflight_per_peer,
    );
    dispatch_fetch_request(
        swarm,
        PendingFetchRequest {
            fetch_token: pending.fetch_token,
            chunk_id: pending.chunk_id,
            max_len: pending.max_len,
            expected_mtime: pending.expected_mtime,
            trace: pending.trace.clone(),
            response: pending.response,
            candidates,
            active_peer: None,
            defer_dht: false,
            started_at: Instant::now(),
            deadline: Instant::now() + loop_state.transfer_timeout,
        },
        init,
        loop_state,
        conf,
    );
}

fn negative_provider_cache_ttl_from_discovery(discovery_timeout: Duration) -> Duration {
    discovery_timeout
}

#[allow(clippy::too_many_arguments)]
fn enqueue_provider_query(
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    loop_state: &mut NetworkLoopState,
    fetch_token: u64,
    chunk_id: ChunkId,
    max_len: usize,
    expected_mtime: Option<i64>,
    trace: TraceLabels,
    response: oneshot::Sender<Option<FetchedChunk>>,
) {
    emit_p2p_trace(
        "discover",
        "dht_query",
        &trace,
        chunk_id,
        None,
        None,
        None,
        None,
    );
    let query_id = swarm
        .behaviour_mut()
        .kad
        .get_providers(provider_record_key(provider_record_id(chunk_id)));
    loop_state.pending_queries.insert(
        query_id,
        PendingProviderQuery {
            fetch_token,
            chunk_id,
            max_len,
            expected_mtime,
            trace,
            response,
            deadline: Instant::now() + loop_state.discovery_timeout,
        },
    );
}

fn handle_connection_established(
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    peer_id: PeerId,
    connection_id: libp2p::swarm::ConnectionId,
    remote_addr: Multiaddr,
    init: &NetworkInitState,
    loop_state: &mut NetworkLoopState,
    max_active_peers: usize,
) -> bool {
    if !is_peer_allowed(peer_id, init.local_peer_id, &init.peer_whitelist)
        || !should_accept_peer(
            peer_id,
            &loop_state.connected_peers,
            &init.bootstrap_peer_ids,
            max_active_peers,
        )
    {
        let _ = swarm.close_connection(connection_id);
        return false;
    }
    loop_state.connected_peers.insert(peer_id);
    loop_state.peer_remote_addrs.insert(peer_id, remote_addr);
    if init.bootstrap_peer_ids.contains(&peer_id) {
        loop_state.bootstrap_connected.insert(peer_id);
    }
    true
}

fn handle_connection_closed(peer_id: PeerId, loop_state: &mut NetworkLoopState) {
    loop_state.connected_peers.remove(&peer_id);
    loop_state.bootstrap_connected.remove(&peer_id);
    loop_state.inflight_per_peer.remove(&peer_id);
    loop_state.peer_ewma.remove(&peer_id);
    loop_state.peer_remote_addrs.remove(&peer_id);
    loop_state.peer_data_ports.remove(&peer_id);
    loop_state
        .peer_block_tickets
        .retain(|(peer, _, _, _), _| *peer != peer_id);
    loop_state.stream_unsupported_peers.remove(&peer_id);
    for providers in loop_state.provider_cache.values_mut() {
        providers.peers.remove(&peer_id);
    }
}

fn sync_network_state_metrics(network_state: &Arc<NetworkState>, loop_state: &NetworkLoopState) {
    network_state.active_peers.store(
        loop_state.connected_peers.len().saturating_add(1) as u64,
        Ordering::Relaxed,
    );
    network_state
        .mdns_peers
        .store(loop_state.mdns_peers.len() as u64, Ordering::Relaxed);
    network_state
        .dht_peers
        .store(loop_state.dht_peers.len() as u64, Ordering::Relaxed);
    network_state.bootstrap_connected.store(
        loop_state.bootstrap_connected.len() as u64,
        Ordering::Relaxed,
    );
}

struct NetworkInitState {
    local_peer_id: PeerId,
    bootstrap_peer_ids: HashSet<PeerId>,
    peer_whitelist: HashSet<PeerId>,
    tenant_whitelist: Arc<HashSet<String>>,
}

struct NetworkLoopState {
    connected_peers: HashSet<PeerId>,
    mdns_peers: HashSet<PeerId>,
    dht_peers: HashSet<PeerId>,
    bootstrap_connected: HashSet<PeerId>,
    provider_cache: HashMap<ChunkId, CachedProviders>,
    provider_cache_ttl: Duration,
    discovery_timeout: Duration,
    transfer_timeout: Duration,
    cache_cleanup_interval: Duration,
    next_cache_cleanup_at: Instant,
    published_provider_records: HashSet<ProviderRecordId>,
    pending_queries: HashMap<kad::QueryId, PendingProviderQuery>,
    pending_requests: HashMap<request_response::OutboundRequestId, PendingFetchRequest>,
    pending_stream_requests: HashMap<u64, PendingFetchRequest>,
    pending_data_requests: HashMap<u64, PendingFetchRequest>,
    pending_incoming: HashMap<u64, PendingIncomingResponse>,
    next_incoming_id: u64,
    next_stream_request_id: u64,
    next_data_request_id: u64,
    inflight_per_peer: HashMap<PeerId, usize>,
    peer_ewma: HashMap<PeerId, PeerEwma>,
    peer_remote_addrs: HashMap<PeerId, Multiaddr>,
    peer_data_ports: HashMap<PeerId, u16>,
    peer_block_tickets: HashMap<(PeerId, ProviderRecordId, String, i64), [u8; 16]>,
    stream_unsupported_peers: HashSet<PeerId>,
    stream_dispatch_txs: Vec<mpsc::Sender<StreamFetchDispatch>>,
    data_dispatch_txs: Vec<mpsc::Sender<DataPlaneFetchDispatch>>,
    stream_dispatch_cursor: usize,
}

impl NetworkLoopState {
    fn new(conf: &ClientP2pConf) -> Self {
        let cache_cleanup_interval = cache_cleanup_interval(conf);
        Self {
            connected_peers: HashSet::new(),
            mdns_peers: HashSet::new(),
            dht_peers: HashSet::new(),
            bootstrap_connected: HashSet::new(),
            provider_cache: HashMap::new(),
            provider_cache_ttl: provider_cache_ttl(conf),
            discovery_timeout: discovery_timeout(conf),
            transfer_timeout: transfer_timeout(conf),
            cache_cleanup_interval,
            next_cache_cleanup_at: Instant::now() + cache_cleanup_interval,
            published_provider_records: HashSet::new(),
            pending_queries: HashMap::new(),
            pending_requests: HashMap::new(),
            pending_stream_requests: HashMap::new(),
            pending_data_requests: HashMap::new(),
            pending_incoming: HashMap::new(),
            next_incoming_id: 1,
            next_stream_request_id: 1,
            next_data_request_id: 1,
            inflight_per_peer: HashMap::new(),
            peer_ewma: HashMap::new(),
            peer_remote_addrs: HashMap::new(),
            peer_data_ports: HashMap::new(),
            peer_block_tickets: HashMap::new(),
            stream_unsupported_peers: HashSet::new(),
            stream_dispatch_txs: Vec::new(),
            data_dispatch_txs: Vec::new(),
            stream_dispatch_cursor: 0,
        }
    }
}

fn init_network_identity_and_policy(
    conf: &ClientP2pConf,
    swarm: &libp2p::Swarm<P2pNetworkBehaviour>,
    network_state: &Arc<NetworkState>,
) -> NetworkInitState {
    let local_peer_id = *swarm.local_peer_id();
    let _ = network_state.local_peer_id.set(local_peer_id.to_string());
    let bootstrap_peers = parse_bootstrap_peers(&conf.bootstrap_peers);
    let bootstrap_peer_ids: HashSet<PeerId> = bootstrap_peers.iter().map(|v| v.0).collect();
    let peer_whitelist = parse_peer_whitelist(&conf.peer_whitelist)
        .map(|parsed_peer_whitelist| {
            build_effective_peer_whitelist(parsed_peer_whitelist, &bootstrap_peer_ids)
        })
        .unwrap_or_else(|| HashSet::from([local_peer_id]));
    let tenant_whitelist = Arc::new(parse_tenant_whitelist(&conf.tenant_whitelist));
    NetworkInitState {
        local_peer_id,
        bootstrap_peer_ids,
        peer_whitelist,
        tenant_whitelist,
    }
}

fn bootstrap_and_listen_swarm(
    conf: &ClientP2pConf,
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
) {
    for entry in &conf.bootstrap_peers {
        let Ok(addr) = Multiaddr::from_str(entry) else {
            continue;
        };
        let Some(peer_id) = addr.iter().find_map(|protocol| match protocol {
            Protocol::P2p(peer_id) => Some(peer_id),
            _ => None,
        }) else {
            continue;
        };
        if conf.enable_dht {
            swarm
                .behaviour_mut()
                .kad
                .add_address(&peer_id, strip_peer_id(&addr));
        }
        if let Err(e) = swarm.dial(addr.clone()) {
            debug!("failed to dial bootstrap {} {}: {}", peer_id, addr, e);
        }
    }

    let mut has_listen = false;
    for addr in &conf.listen_addrs {
        match Multiaddr::from_str(addr) {
            Ok(addr) => {
                if swarm.listen_on(addr).is_ok() {
                    has_listen = true;
                }
            }
            Err(e) => warn!("invalid p2p listen addr {}: {}", addr, e),
        }
    }
    if !has_listen {
        if let Ok(addr) = Multiaddr::from_str("/ip4/0.0.0.0/tcp/0") {
            let _ = swarm.listen_on(addr);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_network_loop(
    conf: ClientP2pConf,
    cache_manager: Arc<CacheManager>,
    pending_fetched: Arc<FastDashMap<ChunkId, PendingFetchedChunk>>,
    hot_published_chunks: Arc<FastDashMap<ChunkId, HotPublishedChunk>>,
    fetched_persist_txs: Vec<mpsc::Sender<PendingFetchedPersist>>,
    persist_tokens: Arc<AtomicU64>,
    data_plane: DataPlaneState,
    mut command_rx: mpsc::Receiver<NetworkCommand>,
    network_state: Arc<NetworkState>,
    network_ready_notify: Arc<Notify>,
    stats: Arc<PeerStats>,
) {
    let (incoming_tx, mut incoming_rx) =
        mpsc::channel::<PreparedIncomingResponse>(conf.max_inflight_requests.max(64));
    let (stream_result_tx, mut stream_result_rx) =
        mpsc::channel::<StreamFetchOutcome>(conf.max_inflight_requests.max(64));
    let (data_result_tx, mut data_result_rx) =
        mpsc::channel::<DataPlaneFetchOutcome>(conf.max_inflight_requests.max(64));
    let mut swarm = match build_swarm(&conf) {
        Ok(v) => v,
        Err(e) => {
            warn!("failed to start libp2p swarm: {}", e);
            return;
        }
    };

    let mut init = init_network_identity_and_policy(&conf, &swarm, &network_state);
    bootstrap_and_listen_swarm(&conf, &mut swarm);
    let mut loop_state = NetworkLoopState::new(&conf);
    let mut stream_control = swarm.behaviour().stream.new_control();
    let mut incoming_streams = stream_control
        .accept(StreamProtocol::new(CHUNK_STREAM_PROTOCOL))
        .ok();
    let stream_worker_count = stream_fetch_worker_count(&conf);
    for _ in 0..stream_worker_count {
        let queue_capacity = conf
            .max_inflight_requests
            .max(64)
            .div_ceil(stream_worker_count.max(1))
            .max(16);
        let (dispatch_tx, dispatch_rx) = mpsc::channel(queue_capacity);
        loop_state.stream_dispatch_txs.push(dispatch_tx);
        tokio::spawn(run_stream_fetch_worker(
            stream_control.clone(),
            conf.clone(),
            dispatch_rx,
            stream_result_tx.clone(),
        ));
    }
    let data_worker_count = stream_fetch_worker_count(&conf);
    for _ in 0..data_worker_count {
        let queue_capacity = conf
            .max_inflight_requests
            .max(64)
            .div_ceil(data_worker_count.max(1))
            .max(16);
        let (dispatch_tx, dispatch_rx) = mpsc::channel(queue_capacity);
        loop_state.data_dispatch_txs.push(dispatch_tx);
        tokio::spawn(run_data_plane_fetch_worker(
            conf.clone(),
            cache_manager.clone(),
            pending_fetched.clone(),
            fetched_persist_txs.clone(),
            persist_tokens.clone(),
            dispatch_rx,
            data_result_tx.clone(),
        ));
    }
    let mut ticker = tokio::time::interval(maintenance_tick_interval(&conf));

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                maybe_cleanup_expired_cache(cache_manager.as_ref(), &mut loop_state, Instant::now());
                expire_pending_queries(
                    &mut swarm,
                    &conf,
                    &init,
                    &mut loop_state,
                );
                expire_pending_requests(
                    &mut loop_state.pending_requests,
                    &mut loop_state.pending_stream_requests,
                    &mut loop_state.pending_data_requests,
                    &mut loop_state.inflight_per_peer,
                );
                expire_pending_incoming(&mut loop_state.pending_incoming);
                expire_provider_cache(&mut loop_state.provider_cache, Instant::now());
            }
            incoming_stream = next_incoming_stream(&mut incoming_streams) => {
                if let Some((peer, stream)) = incoming_stream {
                    handle_incoming_stream_fetch_request(
                        peer,
                        stream,
                        data_plane.clone(),
                        cache_manager.clone(),
                        pending_fetched.clone(),
                        hot_published_chunks.clone(),
                        Arc::clone(&init.tenant_whitelist),
                        conf.clone(),
                        stats.clone(),
                    );
                }
            }
            stream_result = stream_result_rx.recv() => {
                let Some(stream_result) = stream_result else { continue; };
                handle_stream_fetch_outcome(
                    stream_result,
                    &mut swarm,
                    &conf,
                    &init,
                    &mut loop_state,
                    &stats,
                );
            }
            data_result = data_result_rx.recv() => {
                let Some(data_result) = data_result else { continue; };
                handle_data_plane_fetch_outcome(
                    data_result,
                    &mut swarm,
                    &conf,
                    &init,
                    &mut loop_state,
                    &stats,
                );
            }
            incoming = incoming_rx.recv() => {
                let Some(incoming) = incoming else { continue; };
                let Some(pending) = loop_state.pending_incoming.remove(&incoming.id) else {
                    continue;
                };
                if swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(pending.channel, incoming.response)
                    .is_ok()
                    && incoming.sent > 0
                {
                    stats.bytes_sent.fetch_add(incoming.sent, Ordering::Relaxed);
                }
            }
            command = command_rx.recv() => {
                let Some(command) = command else { break; };
                if !handle_network_command(
                    command,
                    &mut swarm,
                    &conf,
                    &mut init,
                    &mut loop_state,
                ) {
                    break;
                }
            }
            event = swarm.select_next_some() => {
                if let SwarmEvent::NewListenAddr { address, .. } = &event {
                    let _ = network_state.listen_addr.set(address.to_string());
                }
                let mut ctx = SwarmEventCtx {
                    swarm: &mut swarm,
                    conf: &conf,
                    data_plane: &data_plane,
                    cache_manager: &cache_manager,
                    pending_fetched: &pending_fetched,
                    hot_published_chunks: &hot_published_chunks,
                    init: &init,
                    loop_state: &mut loop_state,
                    incoming_tx: &incoming_tx,
                    stats: &stats,
                };
                if !handle_swarm_event(event, &mut ctx) {
                    continue;
                }
                sync_network_state_metrics(&network_state, &loop_state);
                if !loop_state.connected_peers.is_empty() {
                    network_ready_notify.notify_waiters();
                }
            }
        }
    }
}

async fn next_incoming_stream(
    incoming_streams: &mut Option<p2p_stream::IncomingStreams>,
) -> Option<(PeerId, libp2p::swarm::Stream)> {
    if let Some(streams) = incoming_streams {
        streams.next().await
    } else {
        futures::future::pending::<Option<(PeerId, libp2p::swarm::Stream)>>().await
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_incoming_stream_fetch_request(
    _peer: PeerId,
    mut stream: libp2p::swarm::Stream,
    data_plane: DataPlaneState,
    cache_manager: Arc<CacheManager>,
    pending_fetched: Arc<FastDashMap<ChunkId, PendingFetchedChunk>>,
    hot_published_chunks: Arc<FastDashMap<ChunkId, HotPublishedChunk>>,
    tenant_whitelist: Arc<HashSet<String>>,
    conf: ClientP2pConf,
    stats: Arc<PeerStats>,
) {
    tokio::spawn(async move {
        let mut codec = ChunkTransferCodec::new(1024 * 1024, response_frame_limit_bytes(&conf));
        let protocol = StreamProtocol::new(CHUNK_STREAM_PROTOCOL);
        loop {
            let Ok(request) =
                request_response::Codec::read_request(&mut codec, &protocol, &mut stream).await
            else {
                break;
            };
            let prepared = if let Some(hot) = prepare_hot_incoming_fetch_response(
                &data_plane,
                hot_published_chunks.as_ref(),
                tenant_whitelist.as_ref(),
                &request,
                true,
            ) {
                hot
            } else {
                prepare_incoming_fetch_response(
                    &data_plane,
                    pending_fetched.as_ref(),
                    &cache_manager,
                    hot_published_chunks.as_ref(),
                    tenant_whitelist.as_ref(),
                    request,
                    0,
                    true,
                )
            };
            let sent = prepared.sent;
            if request_response::Codec::write_response(
                &mut codec,
                &protocol,
                &mut stream,
                prepared.response,
            )
            .await
            .is_ok()
            {
                if sent > 0 {
                    stats.bytes_sent.fetch_add(sent, Ordering::Relaxed);
                }
                continue;
            }
            break;
        }
        let _ = stream.close().await;
    });
}

#[allow(clippy::too_many_arguments)]
fn handle_data_plane_connection(
    stream: TcpStream,
    data_plane: DataPlaneState,
    cache_manager: Arc<CacheManager>,
    pending_fetched: Arc<FastDashMap<ChunkId, PendingFetchedChunk>>,
    hot_published_chunks: Arc<FastDashMap<ChunkId, HotPublishedChunk>>,
    tenant_whitelist: Arc<HashSet<String>>,
    conf: ClientP2pConf,
    stats: Arc<PeerStats>,
) {
    tokio::spawn(async move {
        let mut stream = stream.compat();
        let mut codec = ChunkTransferCodec::new(1024 * 1024, response_frame_limit_bytes(&conf));
        let protocol = StreamProtocol::new(CHUNK_STREAM_PROTOCOL);
        loop {
            let mut header = [0u8; DATA_PLANE_REQUEST_FIXED_BYTES];
            if stream.read_exact(&mut header).await.is_err() {
                break;
            }
            let Ok(request) = decode_data_plane_request(header) else {
                break;
            };
            let Some(request) = data_plane.authorize(&request) else {
                break;
            };
            let prepared = if let Some(hot) = prepare_hot_incoming_fetch_response(
                &data_plane,
                hot_published_chunks.as_ref(),
                tenant_whitelist.as_ref(),
                &request,
                false,
            ) {
                PreparedIncomingResponse {
                    id: 0,
                    response: ChunkFetchResponse {
                        data_port: 0,
                        ticket: EMPTY_DATA_PLANE_TICKET,
                        chunks: hot.response.chunks,
                    },
                    sent: hot.sent,
                }
            } else {
                let mut prepared = prepare_incoming_fetch_response(
                    &data_plane,
                    pending_fetched.as_ref(),
                    &cache_manager,
                    hot_published_chunks.as_ref(),
                    tenant_whitelist.as_ref(),
                    request,
                    0,
                    false,
                );
                prepared.response.data_port = 0;
                prepared.response.ticket = EMPTY_DATA_PLANE_TICKET;
                prepared
            };
            let sent = prepared.sent;
            if request_response::Codec::write_response(
                &mut codec,
                &protocol,
                &mut stream,
                prepared.response,
            )
            .await
            .is_err()
            {
                break;
            }
            if sent > 0 {
                stats.bytes_sent.fetch_add(sent, Ordering::Relaxed);
            }
        }
    });
}

fn build_swarm(
    conf: &ClientP2pConf,
) -> Result<libp2p::Swarm<P2pNetworkBehaviour>, Box<dyn std::error::Error + Send + Sync>> {
    let req_config = request_response::Config::default()
        .with_request_timeout(request_timeout(conf))
        .with_max_concurrent_streams(conf.max_inflight_requests.max(1));
    let identity = load_or_generate_identity(conf)?;
    if enable_quic_transport(conf) {
        let swarm = SwarmBuilder::with_existing_identity(identity)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_quic()
            .with_behaviour(|key| build_network_behaviour(key, conf, &req_config))?
            .with_swarm_config(std::convert::identity)
            .with_connection_timeout(connect_timeout(conf))
            .build();
        return Ok(swarm);
    }
    let swarm = SwarmBuilder::with_existing_identity(identity)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| build_network_behaviour(key, conf, &req_config))?
        .with_swarm_config(std::convert::identity)
        .with_connection_timeout(connect_timeout(conf))
        .build();
    Ok(swarm)
}

fn build_network_behaviour(
    key: &identity::Keypair,
    conf: &ClientP2pConf,
    req_config: &request_response::Config,
) -> Result<P2pNetworkBehaviour, Box<dyn std::error::Error + Send + Sync>> {
    let local_peer_id = key.public().to_peer_id();
    let mut kad_config = kad::Config::new(kad::PROTOCOL_NAME);
    kad_config
        .set_query_timeout(discovery_timeout(conf))
        .set_provider_record_ttl(Some(conf.provider_ttl))
        .set_provider_publication_interval(Some(conf.provider_publish_interval));
    let mut kad_behaviour = kad::Behaviour::with_config(
        local_peer_id,
        kad::store::MemoryStore::new(local_peer_id),
        kad_config,
    );
    kad_behaviour.set_mode(Some(kad::Mode::Server));

    let request_response_behaviour = request_response::Behaviour::with_codec(
        ChunkTransferCodec::new(1024 * 1024, response_frame_limit_bytes(conf)),
        [(
            StreamProtocol::new(CHUNK_PROTOCOL),
            request_response::ProtocolSupport::Full,
        )],
        req_config.clone(),
    );
    let identify_behaviour = identify::Behaviour::new(
        identify::Config::new("/curvine/p2p/identify/1".to_string(), key.public())
            .with_push_listen_addr_updates(true),
    );
    let mdns_behaviour = if conf.enable_mdns {
        Toggle::from(Some(mdns::tokio::Behaviour::new(
            mdns::Config::default(),
            local_peer_id,
        )?))
    } else {
        Toggle::from(None)
    };

    Ok(P2pNetworkBehaviour {
        request_response: request_response_behaviour,
        stream: p2p_stream::Behaviour::new(),
        identify: identify_behaviour,
        mdns: mdns_behaviour,
        kad: kad_behaviour,
    })
}

fn identity_key_path(conf: &ClientP2pConf) -> PathBuf {
    if conf.identity_key_path.is_empty() {
        Path::new(&conf.cache_dir).join("peer_identity.key")
    } else {
        PathBuf::from(&conf.identity_key_path)
    }
}

fn load_or_generate_identity(
    conf: &ClientP2pConf,
) -> Result<identity::Keypair, Box<dyn std::error::Error + Send + Sync>> {
    let key_path = identity_key_path(conf);
    if let Ok(encoded) = fs::read(&key_path) {
        if let Ok(identity) = identity::Keypair::from_protobuf_encoding(&encoded) {
            return Ok(identity);
        }
    }
    if let Some(parent) = key_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let identity = identity::Keypair::generate_ed25519();
    let encoded = identity.to_protobuf_encoding()?;
    fs::write(key_path, encoded)?;
    Ok(identity)
}

fn runtime_policy_version_path(conf: &ClientP2pConf) -> PathBuf {
    Path::new(&conf.cache_dir).join("runtime_policy.version")
}

fn load_runtime_policy_version(conf: &ClientP2pConf) -> u64 {
    fs::read_to_string(runtime_policy_version_path(conf))
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or_default()
}

fn persist_runtime_policy_version(conf: &ClientP2pConf, policy_version: u64) -> bool {
    let path = runtime_policy_version_path(conf);
    path.parent()
        .is_none_or(|parent| fs::create_dir_all(parent).is_ok())
        && fs::write(path, policy_version.to_string()).is_ok()
}

fn set_data_plane_listener_port(data_plane: &DataPlaneState, generation: u64, port: u16) {
    if data_plane.generation() == generation {
        data_plane.port.store(port as u64, Ordering::Relaxed);
    }
}

fn clear_data_plane_listener_port(data_plane: &DataPlaneState, generation: u64) {
    if data_plane.generation() == generation {
        data_plane.port.store(0, Ordering::Relaxed);
    }
}

fn force_reset_data_plane_listener_state(data_plane: &DataPlaneState) {
    data_plane.port.store(0, Ordering::Relaxed);
    data_plane.started.store(false, Ordering::Relaxed);
}

fn reset_data_plane_listener_state(data_plane: &DataPlaneState, generation: u64) {
    if data_plane.generation() == generation {
        force_reset_data_plane_listener_state(data_plane);
    }
}
fn parse_bootstrap_peers(peers: &[String]) -> Vec<(PeerId, Multiaddr)> {
    peers
        .iter()
        .filter_map(|entry| {
            let addr = Multiaddr::from_str(entry).ok()?;
            let peer_id = addr.iter().find_map(|protocol| match protocol {
                Protocol::P2p(peer_id) => Some(peer_id),
                _ => None,
            })?;
            Some((peer_id, addr))
        })
        .collect()
}

type ProviderRecordId = (i64, i64, i64);

fn provider_record_id(chunk_id: ChunkId) -> ProviderRecordId {
    (chunk_id.file_id, chunk_id.version_epoch, chunk_id.block_id)
}

fn provider_record_key(provider: ProviderRecordId) -> kad::RecordKey {
    let mut key = [0u8; 32];
    key[..8].copy_from_slice(&provider.0.to_be_bytes());
    key[8..16].copy_from_slice(&provider.1.to_be_bytes());
    key[16..24].copy_from_slice(&provider.2.to_be_bytes());
    kad::RecordKey::new(&key)
}

fn strip_peer_id(addr: &Multiaddr) -> Multiaddr {
    let mut result = Multiaddr::empty();
    for protocol in addr.iter() {
        if matches!(protocol, Protocol::P2p(_)) {
            continue;
        }
        result.push(protocol);
    }
    result
}

fn identify_dial_addr(listen_addr: &Multiaddr, remote_addr: &Multiaddr) -> Option<Multiaddr> {
    let remote_ip = remote_addr.iter().find_map(|protocol| match protocol {
        Protocol::Ip4(value) => Some(IpAddr::V4(value)),
        Protocol::Ip6(value) => Some(IpAddr::V6(value)),
        _ => None,
    })?;
    let mut result = Multiaddr::empty();
    let mut has_ip = false;
    for protocol in strip_peer_id(listen_addr).iter() {
        match protocol {
            Protocol::Ip4(value) => {
                has_ip = true;
                result.push(Protocol::Ip4(if value.is_unspecified() {
                    match remote_ip {
                        IpAddr::V4(remote) => remote,
                        IpAddr::V6(_) => return None,
                    }
                } else {
                    value
                }));
            }
            Protocol::Ip6(value) => {
                has_ip = true;
                result.push(Protocol::Ip6(if value.is_unspecified() {
                    match remote_ip {
                        IpAddr::V6(remote) => remote,
                        IpAddr::V4(_) => return None,
                    }
                } else {
                    value
                }));
            }
            protocol => result.push(protocol),
        }
    }
    has_ip.then_some(result)
}

fn enable_quic_transport(conf: &ClientP2pConf) -> bool {
    conf.listen_addrs
        .iter()
        .chain(conf.bootstrap_peers.iter())
        .any(|addr| addr.contains("/quic"))
}

fn response_frame_limit_bytes(conf: &ClientP2pConf) -> usize {
    let max_cap = (256 * 1024 * 1024) as u64;
    conf.cache_capacity.clamp(4 * 1024 * 1024, max_cap) as usize
}

type PendingFetchedEntries = Vec<(ChunkId, PendingFetchedChunk)>;
type CompletedPendingFetchedEntries = Vec<(ChunkId, u64)>;

fn build_pending_fetched_batch<F>(
    batch: &[FetchedCacheBatchItem],
    mut next_persist_token: F,
) -> (PendingFetchedEntries, CompletedPendingFetchedEntries)
where
    F: FnMut() -> u64,
{
    let mut pending_entries = Vec::with_capacity(batch.len());
    let mut completed = Vec::with_capacity(batch.len());
    for item in batch {
        let persist_token = next_persist_token();
        pending_entries.push((
            item.chunk_id,
            PendingFetchedChunk::ready(item.data.clone(), item.mtime, persist_token),
        ));
        completed.push((item.chunk_id, persist_token));
    }
    (pending_entries, completed)
}

fn should_accept_peer(
    peer_id: PeerId,
    connected_peers: &HashSet<PeerId>,
    bootstrap_peer_ids: &HashSet<PeerId>,
    max_active_peers: usize,
) -> bool {
    if connected_peers.contains(&peer_id) || bootstrap_peer_ids.contains(&peer_id) {
        return true;
    }
    max_active_peers == 0 || connected_peers.len() < max_active_peers
}

fn parse_peer_whitelist(peers: &[String]) -> Option<HashSet<PeerId>> {
    let mut parsed = HashSet::new();
    for entry in peers {
        match PeerId::from_str(entry) {
            Ok(peer_id) => {
                parsed.insert(peer_id);
            }
            Err(e) => {
                warn!("invalid p2p peer whitelist entry '{}': {}", entry, e);
                return None;
            }
        }
    }
    Some(parsed)
}

fn build_effective_peer_whitelist(
    mut peer_whitelist: HashSet<PeerId>,
    bootstrap_peer_ids: &HashSet<PeerId>,
) -> HashSet<PeerId> {
    if !peer_whitelist.is_empty() {
        peer_whitelist.extend(bootstrap_peer_ids.iter().copied());
    }
    peer_whitelist
}

fn parse_tenant_whitelist(tenants: &[String]) -> HashSet<String> {
    tenants
        .iter()
        .map(|tenant| tenant.trim())
        .filter(|tenant| !tenant.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn is_peer_allowed(
    peer_id: PeerId,
    local_peer_id: PeerId,
    peer_whitelist: &HashSet<PeerId>,
) -> bool {
    peer_id == local_peer_id || peer_whitelist.is_empty() || peer_whitelist.contains(&peer_id)
}

fn is_tenant_allowed(tenant_id: &str, tenant_whitelist: &HashSet<String>) -> bool {
    if tenant_whitelist.is_empty() {
        return true;
    }
    let tenant = tenant_id.trim();
    !tenant.is_empty() && tenant_whitelist.contains(tenant)
}

fn peer_dispatch_window(
    peer_id: PeerId,
    max_inflight_per_peer: usize,
    peer_ewma: &HashMap<PeerId, PeerEwma>,
) -> usize {
    if max_inflight_per_peer == 0 {
        return 0;
    }
    let Some(stats) = peer_ewma.get(&peer_id).copied() else {
        return max_inflight_per_peer;
    };
    let factor = if stats.failure_ratio >= 0.5 {
        0.2
    } else if stats.failure_ratio >= 0.2 {
        0.5
    } else if stats.latency_ms >= 500.0 {
        0.3
    } else if stats.latency_ms >= 250.0 {
        0.6
    } else if stats.throughput_mb_s < 1.0 {
        0.7
    } else {
        1.0
    };
    (max_inflight_per_peer as f64 * factor)
        .ceil()
        .max(1.0)
        .min(max_inflight_per_peer as f64) as usize
}

fn can_dispatch_peer(
    peer_id: PeerId,
    inflight_per_peer: &HashMap<PeerId, usize>,
    max_inflight_per_peer: usize,
    adaptive_window: bool,
    min_inflight_per_peer: usize,
    peer_ewma: &HashMap<PeerId, PeerEwma>,
) -> bool {
    if max_inflight_per_peer == 0 {
        return true;
    }
    let min_window = min_inflight_per_peer.max(1).min(max_inflight_per_peer);
    let adaptive = if adaptive_window {
        peer_dispatch_window(peer_id, max_inflight_per_peer, peer_ewma)
    } else {
        max_inflight_per_peer
    };
    let limit = adaptive.max(min_window);
    inflight_per_peer.get(&peer_id).copied().unwrap_or(0) < limit
}

fn release_peer_inflight(
    pending: &PendingFetchRequest,
    inflight_per_peer: &mut HashMap<PeerId, usize>,
) {
    let Some(peer_id) = pending.active_peer else {
        return;
    };
    let Some(count) = inflight_per_peer.get_mut(&peer_id) else {
        return;
    };
    if *count <= 1 {
        inflight_per_peer.remove(&peer_id);
    } else {
        *count -= 1;
    }
}

fn build_candidates<I>(
    providers: I,
    connected_peers: &HashSet<PeerId>,
    bootstrap_connected: &HashSet<PeerId>,
    local_peer_id: PeerId,
    peer_ewma: &HashMap<PeerId, PeerEwma>,
    inflight_per_peer: &HashMap<PeerId, usize>,
) -> VecDeque<PeerId>
where
    I: IntoIterator<Item = PeerId>,
{
    let mut seen = HashSet::new();
    let mut ordered = VecDeque::new();

    for peer in providers {
        if peer != local_peer_id && seen.insert(peer) {
            ordered.push_back(peer);
        }
    }

    for peer in connected_peers {
        if *peer != local_peer_id && seen.insert(*peer) {
            ordered.push_back(*peer);
        }
    }

    for peer in bootstrap_connected {
        if *peer != local_peer_id && seen.insert(*peer) {
            ordered.push_back(*peer);
        }
    }
    let mut ranked: Vec<(PeerId, f64)> = ordered
        .into_iter()
        .map(|peer_id| {
            (
                peer_id,
                candidate_score(peer_id, peer_ewma, inflight_per_peer),
            )
        })
        .collect();
    ranked.sort_unstable_by(|left, right| left.1.total_cmp(&right.1));
    ranked.into_iter().map(|(peer_id, _)| peer_id).collect()
}

fn direct_connected_probe_candidates(
    conf: &ClientP2pConf,
    connected_peers: &HashSet<PeerId>,
    bootstrap_connected: &HashSet<PeerId>,
    local_peer_id: PeerId,
    peer_ewma: &HashMap<PeerId, PeerEwma>,
    inflight_per_peer: &HashMap<PeerId, usize>,
) -> Option<VecDeque<PeerId>> {
    if !conf.enable_dht {
        return None;
    }
    let candidates = build_candidates(
        std::iter::empty::<PeerId>(),
        connected_peers,
        bootstrap_connected,
        local_peer_id,
        peer_ewma,
        inflight_per_peer,
    );
    if candidates.is_empty() || candidates.len() > direct_connected_probe_max_peers() {
        return None;
    }
    Some(candidates)
}

fn direct_connected_probe_max_peers() -> usize {
    2
}

fn candidate_score(
    peer_id: PeerId,
    peer_ewma: &HashMap<PeerId, PeerEwma>,
    inflight_per_peer: &HashMap<PeerId, usize>,
) -> f64 {
    let stats = peer_ewma.get(&peer_id).copied().unwrap_or_default();
    let inflight = inflight_per_peer.get(&peer_id).copied().unwrap_or(0) as f64;
    let throughput_penalty = 200.0 / stats.throughput_mb_s.max(0.1);
    stats.latency_ms + stats.failure_ratio * 1000.0 + throughput_penalty + inflight * 10.0
}

fn dispatch_fetch_request(
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    mut pending: PendingFetchRequest,
    init: &NetworkInitState,
    loop_state: &mut NetworkLoopState,
    conf: &ClientP2pConf,
) {
    while let Some(peer_id) = pending.candidates.pop_front() {
        if !is_peer_allowed(peer_id, init.local_peer_id, &init.peer_whitelist)
            || peer_id == init.local_peer_id
        {
            continue;
        }
        if !can_dispatch_peer(
            peer_id,
            &loop_state.inflight_per_peer,
            conf.max_inflight_per_peer,
            conf.adaptive_window,
            conf.min_inflight_per_peer,
            &loop_state.peer_ewma,
        ) {
            continue;
        }
        emit_p2p_trace(
            "connect",
            "dispatch",
            &pending.trace,
            pending.chunk_id,
            Some(&peer_id),
            None,
            None,
            None,
        );
        let control_prefetch_count =
            dynamic_prefetch_count(pending.max_len, loop_state.peer_ewma.get(&peer_id).copied());
        let data_plane_ready = loop_state.peer_remote_addrs.contains_key(&peer_id)
            && loop_state.peer_data_ports.contains_key(&peer_id)
            && loop_state
                .peer_block_tickets
                .contains_key(&peer_block_ticket_key(
                    peer_id,
                    pending.chunk_id,
                    pending.trace.tenant_id.as_str(),
                    pending.expected_mtime,
                ));
        if let Some(dispatch) = data_plane_ready
            .then(|| {
                build_data_plane_fetch_dispatch(
                    loop_state,
                    peer_id,
                    &pending,
                    data_plane_prefetch_count(pending.max_len, conf),
                )
            })
            .flatten()
        {
            let request_id = dispatch.request_id;
            if enqueue_data_plane_fetch(loop_state, dispatch) {
                pending.active_peer = Some(peer_id);
                *loop_state.inflight_per_peer.entry(peer_id).or_insert(0) += 1;
                loop_state.pending_data_requests.insert(request_id, pending);
                loop_state.next_data_request_id = loop_state.next_data_request_id.wrapping_add(1);
                return;
            }
        }
        if !loop_state.stream_unsupported_peers.contains(&peer_id) {
            let request_id = loop_state.next_stream_request_id;
            loop_state.next_stream_request_id = loop_state.next_stream_request_id.wrapping_add(1);
            let request = chunk_fetch_request_from_pending(&pending, control_prefetch_count);
            if enqueue_stream_fetch(
                loop_state,
                StreamFetchDispatch {
                    request_id,
                    peer: peer_id,
                    request,
                },
            ) {
                pending.active_peer = Some(peer_id);
                *loop_state.inflight_per_peer.entry(peer_id).or_insert(0) += 1;
                loop_state
                    .pending_stream_requests
                    .insert(request_id, pending);
                return;
            }
        }
        let request_id = swarm.behaviour_mut().request_response.send_request(
            &peer_id,
            chunk_fetch_request_from_pending(&pending, control_prefetch_count),
        );
        pending.active_peer = Some(peer_id);
        *loop_state.inflight_per_peer.entry(peer_id).or_insert(0) += 1;
        loop_state.pending_requests.insert(request_id, pending);
        return;
    }
    if pending.defer_dht && conf.enable_dht {
        enqueue_provider_query(
            swarm,
            loop_state,
            pending.fetch_token,
            pending.chunk_id,
            pending.max_len,
            pending.expected_mtime,
            pending.trace,
            pending.response,
        );
        return;
    }
    emit_p2p_trace(
        "connect",
        "no_candidate",
        &pending.trace,
        pending.chunk_id,
        None,
        None,
        None,
        Some("no_candidate"),
    );
    let _ = pending.response.send(None);
}

fn build_data_plane_fetch_dispatch(
    loop_state: &NetworkLoopState,
    peer_id: PeerId,
    pending: &PendingFetchRequest,
    prefetch_count: u16,
) -> Option<DataPlaneFetchDispatch> {
    let endpoint = peer_data_endpoint(
        loop_state.peer_remote_addrs.get(&peer_id)?,
        *loop_state.peer_data_ports.get(&peer_id)?,
    )?;
    let ticket = *loop_state.peer_block_tickets.get(&peer_block_ticket_key(
        peer_id,
        pending.chunk_id,
        pending.trace.tenant_id.as_str(),
        pending.expected_mtime,
    ))?;
    Some(DataPlaneFetchDispatch {
        request_id: loop_state.next_data_request_id,
        peer: peer_id,
        endpoint,
        request: data_plane_request_from_pending(pending, prefetch_count, ticket),
    })
}

fn chunk_fetch_request_from_pending(
    pending: &PendingFetchRequest,
    prefetch_count: u16,
) -> ChunkFetchRequest {
    ChunkFetchRequest {
        file_id: pending.chunk_id.file_id,
        version_epoch: pending.chunk_id.version_epoch,
        block_id: pending.chunk_id.block_id,
        off: pending.chunk_id.off,
        len: pending.max_len,
        expected_mtime: pending.expected_mtime.unwrap_or(0),
        prefetch_count,
        tenant_id: pending.trace.tenant_id.clone(),
    }
}

fn data_plane_request_from_pending(
    pending: &PendingFetchRequest,
    prefetch_count: u16,
    ticket: [u8; 16],
) -> DataPlaneFetchRequest {
    DataPlaneFetchRequest {
        ticket,
        file_id: pending.chunk_id.file_id,
        version_epoch: pending.chunk_id.version_epoch,
        block_id: pending.chunk_id.block_id,
        off: pending.chunk_id.off,
        len: pending.max_len,
        expected_mtime: pending.expected_mtime.unwrap_or(0),
        prefetch_count,
    }
}

fn enqueue_stream_fetch(loop_state: &mut NetworkLoopState, dispatch: StreamFetchDispatch) -> bool {
    let len = loop_state.stream_dispatch_txs.len();
    if len == 0 {
        return false;
    }
    let preferred = stream_worker_index_for_peer(dispatch.peer, len);
    let mut dispatch = Some(dispatch);
    let mut offset = 0usize;
    while offset < len {
        let idx = (preferred + offset) % len;
        let result = loop_state.stream_dispatch_txs[idx].try_send(dispatch.take().unwrap());
        match result {
            Ok(_) => {
                loop_state.stream_dispatch_cursor = idx.wrapping_add(1) % len;
                return true;
            }
            Err(TrySendError::Full(value)) | Err(TrySendError::Closed(value)) => {
                dispatch = Some(value);
                offset += 1;
            }
        }
    }
    false
}

fn enqueue_data_plane_fetch(
    loop_state: &mut NetworkLoopState,
    dispatch: DataPlaneFetchDispatch,
) -> bool {
    let len = loop_state.data_dispatch_txs.len();
    if len == 0 {
        return false;
    }
    let preferred = stream_worker_index_for_peer(dispatch.peer, len);
    let mut dispatch = Some(dispatch);
    let mut offset = 0usize;
    while offset < len {
        let idx = (preferred + offset) % len;
        let result = loop_state.data_dispatch_txs[idx].try_send(dispatch.take().unwrap());
        match result {
            Ok(_) => return true,
            Err(TrySendError::Full(value)) | Err(TrySendError::Closed(value)) => {
                dispatch = Some(value);
                offset += 1;
            }
        }
    }
    false
}

fn stream_worker_index_for_peer(peer_id: PeerId, worker_count: usize) -> usize {
    if worker_count <= 1 {
        return 0;
    }
    let bytes = peer_id.to_bytes();
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x00000100000001b3);
    }
    (hash as usize) % worker_count
}

async fn run_stream_fetch_worker(
    control: p2p_stream::Control,
    conf: ClientP2pConf,
    mut dispatch_rx: mpsc::Receiver<StreamFetchDispatch>,
    stream_result_tx: mpsc::Sender<StreamFetchOutcome>,
) {
    let timeout_budget = transfer_timeout(&conf);
    let response_limit = response_frame_limit_bytes(&conf);
    let protocol = StreamProtocol::new(CHUNK_STREAM_PROTOCOL);
    let mut control = control;
    let mut active_peer: Option<PeerId> = None;
    let mut active_stream: Option<libp2p::swarm::Stream> = None;
    while let Some(dispatch) = dispatch_rx.recv().await {
        let mut codec = ChunkTransferCodec::new(1024 * 1024, response_limit);
        let mut unsupported_protocol = false;
        let mut response = None;
        let mut attempts = 0u8;
        while attempts < 2 {
            let need_open = active_stream.is_none() || active_peer != Some(dispatch.peer);
            if need_open {
                active_peer = None;
                active_stream = None;
                match timeout(
                    timeout_budget,
                    control.open_stream(dispatch.peer, protocol.clone()),
                )
                .await
                {
                    Ok(Ok(stream)) => {
                        active_peer = Some(dispatch.peer);
                        active_stream = Some(stream);
                    }
                    Ok(Err(p2p_stream::OpenStreamError::UnsupportedProtocol(_))) => {
                        unsupported_protocol = true;
                        break;
                    }
                    _ => break,
                }
            }
            let Some(stream) = active_stream.as_mut() else {
                break;
            };
            let write_ok = timeout(
                timeout_budget,
                request_response::Codec::write_request(
                    &mut codec,
                    &protocol,
                    stream,
                    dispatch.request.clone(),
                ),
            )
            .await
            .ok()
            .and_then(Result::ok)
            .is_some();
            if !write_ok {
                active_peer = None;
                active_stream = None;
                attempts += 1;
                continue;
            }
            response = timeout(
                timeout_budget,
                request_response::Codec::read_response(&mut codec, &protocol, stream),
            )
            .await
            .ok()
            .and_then(Result::ok);
            if response.is_some() {
                break;
            }
            active_peer = None;
            active_stream = None;
            attempts += 1;
        }
        if stream_result_tx
            .send(StreamFetchOutcome {
                request_id: dispatch.request_id,
                peer: dispatch.peer,
                response,
                unsupported_protocol,
            })
            .await
            .is_err()
        {
            break;
        }
    }
}

fn data_plane_request_chunk_id(request: &DataPlaneFetchRequest, index: usize) -> Option<ChunkId> {
    let step = i64::try_from(request.len.max(1)).ok()?;
    let idx = i64::try_from(index).ok()?;
    let off = request.off.checked_add(step.checked_mul(idx)?)?;
    Some(ChunkId::with_version(
        request.file_id,
        request.version_epoch,
        request.block_id,
        off,
    ))
}

fn enqueue_pending_fetched_persist_request(
    persist_txs: &[mpsc::Sender<PendingFetchedPersist>],
    request: PendingFetchedPersist,
) -> Result<(), PendingFetchedPersist> {
    if persist_txs.is_empty() {
        return Err(request);
    }
    let worker_idx = fetched_persist_worker_index(request.chunk_id, persist_txs.len());
    match persist_txs[worker_idx].try_send(request) {
        Ok(_) => Ok(()),
        Err(TrySendError::Full(request)) | Err(TrySendError::Closed(request)) => Err(request),
    }
}

fn spawn_direct_fetched_persist(
    cache_manager: Arc<CacheManager>,
    pending_fetched: Arc<FastDashMap<ChunkId, PendingFetchedChunk>>,
    request: PendingFetchedPersist,
) {
    tokio::spawn(async move {
        let chunk_id = request.chunk_id;
        let persist_token = request.persist_token;
        let _ = tokio::task::spawn_blocking(move || {
            let _ = cache_manager.put_fetched_with_result_and_checksum(
                request.chunk_id,
                request.data,
                request.mtime,
                Some(request.checksum),
            );
        })
        .await;
        cleanup_pending_fetched_entry(&pending_fetched, chunk_id, persist_token);
    });
}

fn fail_pending_fetched_placeholders(
    pending_fetched: &FastDashMap<ChunkId, PendingFetchedChunk>,
    placeholders: VecDeque<PendingFetchedPlaceholder>,
) {
    for placeholder in placeholders {
        fail_pending_fetched_placeholder(pending_fetched, placeholder);
    }
}

#[allow(clippy::too_many_arguments)]
async fn stream_data_plane_response<T>(
    io: &mut T,
    dispatch: &DataPlaneFetchDispatch,
    response_limit: usize,
    pending_fetched: Arc<FastDashMap<ChunkId, PendingFetchedChunk>>,
    persist_tokens: &AtomicU64,
    data_result_tx: &mpsc::Sender<DataPlaneFetchOutcome>,
    enable_checksum: bool,
    persist_txs: &[mpsc::Sender<PendingFetchedPersist>],
    cache_manager: Arc<CacheManager>,
    first_chunk_sent: &mut bool,
) -> std::io::Result<usize>
where
    T: futures::AsyncRead + Unpin + Send,
{
    let (chunk_count, _, _) = read_response_header_frame(io).await?;
    let mut placeholders = VecDeque::with_capacity(chunk_count.saturating_sub(1));
    for idx in 1..chunk_count {
        let Some(chunk_id) = data_plane_request_chunk_id(&dispatch.request, idx) else {
            fail_pending_fetched_placeholders(pending_fetched.as_ref(), placeholders);
            return Err(Error::new(
                ErrorKind::InvalidData,
                "invalid data plane chunk index",
            ));
        };
        let persist_token = persist_tokens.fetch_add(1, Ordering::Relaxed);
        let (pending, placeholder) = new_pending_fetched_placeholder(chunk_id, persist_token);
        pending_fetched.insert(chunk_id, pending);
        placeholders.push_back(placeholder);
    }

    let mut total_len = RESPONSE_FIXED_BYTES;
    let mut prefetched = 0usize;
    for idx in 0..chunk_count {
        let item = match read_response_item_frame(io, &mut total_len, response_limit).await {
            Ok(item) => item,
            Err(e) => {
                fail_pending_fetched_placeholders(pending_fetched.as_ref(), placeholders);
                return Err(e);
            }
        };
        let Some(expected_chunk_id) = data_plane_request_chunk_id(&dispatch.request, idx) else {
            fail_pending_fetched_placeholders(pending_fetched.as_ref(), placeholders);
            return Err(Error::new(
                ErrorKind::InvalidData,
                "invalid data plane response offset",
            ));
        };
        if item.off != expected_chunk_id.off {
            fail_pending_fetched_placeholders(pending_fetched.as_ref(), placeholders);
            return Err(Error::new(
                ErrorKind::InvalidData,
                "unexpected data plane chunk order",
            ));
        }
        if idx == 0 {
            if item.data.is_empty() {
                fail_pending_fetched_placeholders(pending_fetched.as_ref(), placeholders);
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "missing requested data plane chunk",
                ));
            }
            let len = item.data.len().min(dispatch.request.len.max(1));
            let data = item.data.slice(0..len);
            let checksum = if item.checksum.is_empty() || len == item.data.len() {
                item.checksum
            } else {
                sha256_bytes(data.as_ref())
            };
            data_result_tx
                .send(DataPlaneFetchOutcome {
                    request_id: dispatch.request_id,
                    peer: dispatch.peer,
                    chunk: Some(FetchedChunk {
                        data,
                        mtime: item.mtime,
                        checksum,
                        prefetched: Vec::new(),
                    }),
                    recv: total_len,
                })
                .await
                .map_err(|_| {
                    Error::new(ErrorKind::BrokenPipe, "data plane result channel closed")
                })?;
            *first_chunk_sent = true;
            continue;
        }
        let Some(placeholder) = placeholders.pop_front() else {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "missing data plane placeholder",
            ));
        };
        let resolved_mtime = if item.mtime > 0 {
            item.mtime
        } else {
            dispatch.request.expected_mtime.max(0)
        };
        let valid_mtime = dispatch.request.expected_mtime <= 0
            || resolved_mtime <= 0
            || resolved_mtime == dispatch.request.expected_mtime;
        if item.data.is_empty() || !valid_mtime || (enable_checksum && item.checksum.is_empty()) {
            fail_pending_fetched_placeholder(pending_fetched.as_ref(), placeholder);
            continue;
        }
        let persist_request = PendingFetchedPersist {
            chunk_id: placeholder.chunk_id,
            data: item.data.clone(),
            mtime: resolved_mtime,
            checksum: item.checksum.clone(),
            persist_token: placeholder.persist_token,
        };
        fulfill_pending_fetched_placeholder(
            pending_fetched.as_ref(),
            placeholder,
            item.data,
            resolved_mtime,
        );
        prefetched += 1;
        if let Err(request) = enqueue_pending_fetched_persist_request(persist_txs, persist_request)
        {
            spawn_direct_fetched_persist(cache_manager.clone(), pending_fetched.clone(), request);
        }
    }
    if !*first_chunk_sent {
        fail_pending_fetched_placeholders(pending_fetched.as_ref(), placeholders);
        return Err(Error::new(
            ErrorKind::InvalidData,
            "missing selected data plane chunk",
        ));
    }
    Ok(prefetched)
}

async fn run_data_plane_fetch_worker(
    conf: ClientP2pConf,
    cache_manager: Arc<CacheManager>,
    pending_fetched: Arc<FastDashMap<ChunkId, PendingFetchedChunk>>,
    fetched_persist_txs: Vec<mpsc::Sender<PendingFetchedPersist>>,
    persist_tokens: Arc<AtomicU64>,
    mut dispatch_rx: mpsc::Receiver<DataPlaneFetchDispatch>,
    data_result_tx: mpsc::Sender<DataPlaneFetchOutcome>,
) {
    let timeout_budget = transfer_timeout(&conf);
    let response_limit = response_frame_limit_bytes(&conf);
    let mut active_peer: Option<PeerId> = None;
    let mut active_endpoint: Option<SocketAddr> = None;
    let mut active_stream: Option<Compat<TcpStream>> = None;
    while let Some(dispatch) = dispatch_rx.recv().await {
        let mut first_chunk_sent = false;
        let mut completed = false;
        let mut attempts = 0u8;
        while attempts < 2 {
            let need_open = active_stream.is_none()
                || active_peer != Some(dispatch.peer)
                || active_endpoint != Some(dispatch.endpoint);
            if need_open {
                active_peer = None;
                active_endpoint = None;
                active_stream = None;
                match timeout(timeout_budget, TcpStream::connect(dispatch.endpoint)).await {
                    Ok(Ok(stream)) => {
                        active_peer = Some(dispatch.peer);
                        active_endpoint = Some(dispatch.endpoint);
                        active_stream = Some(stream.compat());
                    }
                    Ok(Err(_)) | Err(_) => break,
                }
            }
            let Some(stream) = active_stream.as_mut() else {
                break;
            };
            let header = encode_data_plane_request(&dispatch.request);
            let write_ok = timeout(timeout_budget, stream.write_all(&header))
                .await
                .ok()
                .and_then(Result::ok)
                .is_some();
            if !write_ok {
                active_peer = None;
                active_endpoint = None;
                active_stream = None;
                attempts += 1;
                continue;
            }
            let streamed = timeout(
                timeout_budget,
                stream_data_plane_response(
                    stream,
                    &dispatch,
                    response_limit,
                    pending_fetched.clone(),
                    persist_tokens.as_ref(),
                    &data_result_tx,
                    conf.enable_checksum,
                    &fetched_persist_txs,
                    cache_manager.clone(),
                    &mut first_chunk_sent,
                ),
            )
            .await;
            if matches!(streamed, Ok(Ok(_))) {
                completed = true;
                break;
            }
            if first_chunk_sent {
                completed = true;
                active_peer = None;
                active_endpoint = None;
                active_stream = None;
                break;
            }
            active_peer = None;
            active_endpoint = None;
            active_stream = None;
            attempts += 1;
        }
        if !completed
            && data_result_tx
                .send(DataPlaneFetchOutcome {
                    request_id: dispatch.request_id,
                    peer: dispatch.peer,
                    chunk: None,
                    recv: 0,
                })
                .await
                .is_err()
        {
            break;
        }
    }
}

struct RequestResponseEventCtx<'a> {
    swarm: &'a mut libp2p::Swarm<P2pNetworkBehaviour>,
    data_plane: &'a DataPlaneState,
    cache_manager: &'a Arc<CacheManager>,
    pending_fetched: &'a Arc<FastDashMap<ChunkId, PendingFetchedChunk>>,
    hot_published_chunks: &'a Arc<FastDashMap<ChunkId, HotPublishedChunk>>,
    conf: &'a ClientP2pConf,
    init: &'a NetworkInitState,
    loop_state: &'a mut NetworkLoopState,
    incoming_tx: &'a mpsc::Sender<PreparedIncomingResponse>,
    stats: &'a Arc<PeerStats>,
}

fn handle_request_response_event(
    event: request_response::Event<ChunkFetchRequest, ChunkFetchResponse>,
    ctx: &mut RequestResponseEventCtx<'_>,
) {
    match event {
        request_response::Event::Message { peer, message, .. } => match message {
            request_response::Message::Request {
                request, channel, ..
            } => {
                if let Some(prepared) = prepare_hot_incoming_fetch_response(
                    ctx.data_plane,
                    ctx.hot_published_chunks.as_ref(),
                    ctx.init.tenant_whitelist.as_ref(),
                    &request,
                    true,
                ) {
                    if ctx
                        .swarm
                        .behaviour_mut()
                        .request_response
                        .send_response(channel, prepared.response)
                        .is_ok()
                    {
                        ctx.stats
                            .bytes_sent
                            .fetch_add(prepared.sent, Ordering::Relaxed);
                    }
                    return;
                }
                let incoming_id = ctx.loop_state.next_incoming_id;
                ctx.loop_state.next_incoming_id = ctx.loop_state.next_incoming_id.wrapping_add(1);
                ctx.loop_state.pending_incoming.insert(
                    incoming_id,
                    PendingIncomingResponse {
                        channel,
                        deadline: Instant::now() + ctx.loop_state.transfer_timeout,
                    },
                );
                handle_incoming_fetch_request(
                    ctx.data_plane.clone(),
                    ctx.cache_manager.clone(),
                    ctx.pending_fetched.clone(),
                    ctx.hot_published_chunks.clone(),
                    Arc::clone(&ctx.init.tenant_whitelist),
                    request,
                    incoming_id,
                    ctx.incoming_tx.clone(),
                );
            }
            request_response::Message::Response {
                request_id,
                response,
            } => {
                let mut response_ctx = IncomingFetchResponseCtx {
                    swarm: ctx.swarm,
                    conf: ctx.conf,
                    init: ctx.init,
                    loop_state: ctx.loop_state,
                    stats: ctx.stats,
                };
                handle_incoming_fetch_response(&mut response_ctx, peer, request_id, response);
            }
        },
        request_response::Event::OutboundFailure {
            request_id, peer, ..
        } => handle_outbound_fetch_failure(
            ctx.swarm,
            ctx.conf,
            ctx.init,
            ctx.loop_state,
            peer,
            request_id,
        ),
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_incoming_fetch_request(
    data_plane: DataPlaneState,
    cache_manager: Arc<CacheManager>,
    pending_fetched: Arc<FastDashMap<ChunkId, PendingFetchedChunk>>,
    hot_published_chunks: Arc<FastDashMap<ChunkId, HotPublishedChunk>>,
    tenant_whitelist: Arc<HashSet<String>>,
    request: ChunkFetchRequest,
    incoming_id: u64,
    incoming_tx: mpsc::Sender<PreparedIncomingResponse>,
) {
    tokio::spawn(async move {
        let prepared = prepare_incoming_fetch_response(
            &data_plane,
            pending_fetched.as_ref(),
            &cache_manager,
            hot_published_chunks.as_ref(),
            tenant_whitelist.as_ref(),
            request,
            incoming_id,
            true,
        );
        let _ = incoming_tx.send(prepared).await;
    });
}

#[allow(clippy::too_many_arguments)]
fn prepare_incoming_fetch_response(
    data_plane: &DataPlaneState,
    pending_fetched: &FastDashMap<ChunkId, PendingFetchedChunk>,
    cache_manager: &CacheManager,
    hot_published_chunks: &FastDashMap<ChunkId, HotPublishedChunk>,
    tenant_whitelist: &HashSet<String>,
    request: ChunkFetchRequest,
    incoming_id: u64,
    redirect_to_data_plane: bool,
) -> PreparedIncomingResponse {
    if !is_tenant_allowed(&request.tenant_id, tenant_whitelist) {
        return PreparedIncomingResponse {
            id: incoming_id,
            response: ChunkFetchResponse {
                data_port: 0,
                ticket: EMPTY_DATA_PLANE_TICKET,
                chunks: Vec::new(),
            },
            sent: 0,
        };
    }
    let now_ms = LocalTime::mills() as i64;
    let expected_mtime = (request.expected_mtime > 0).then_some(request.expected_mtime);
    let mut chunks = Vec::new();
    let mut sent = 0u64;
    let Some(first_chunk_id) = request_chunk_id(&request, 0) else {
        return PreparedIncomingResponse {
            id: incoming_id,
            response: ChunkFetchResponse {
                data_port: 0,
                ticket: EMPTY_DATA_PLANE_TICKET,
                chunks,
            },
            sent,
        };
    };
    let first_chunk = load_remote_response_chunk(
        pending_fetched,
        cache_manager,
        hot_published_chunks,
        first_chunk_id,
        request.len,
        expected_mtime,
        now_ms,
    );
    if redirect_to_data_plane {
        let Some(_) = first_chunk.as_ref() else {
            return PreparedIncomingResponse {
                id: incoming_id,
                response: ChunkFetchResponse {
                    data_port: 0,
                    ticket: EMPTY_DATA_PLANE_TICKET,
                    chunks,
                },
                sent,
            };
        };
        let data_port = data_plane.port();
        if data_port > 0 {
            return PreparedIncomingResponse {
                id: incoming_id,
                response: ChunkFetchResponse {
                    data_port,
                    ticket: data_plane.issue_ticket(&request),
                    chunks: Vec::new(),
                },
                sent: 0,
            };
        }
    }
    let Some(first_chunk) = first_chunk else {
        return PreparedIncomingResponse {
            id: incoming_id,
            response: ChunkFetchResponse {
                data_port: 0,
                ticket: EMPTY_DATA_PLANE_TICKET,
                chunks,
            },
            sent,
        };
    };
    sent = sent.saturating_add(first_chunk.data.len() as u64);
    chunks.push(first_chunk);
    let prefetch_count = request.prefetch_count.min(max_prefetch_count());
    let limit = usize::from(prefetch_count) + 1;
    for idx in 1..limit {
        let Some(chunk_id) = request_chunk_id(&request, idx) else {
            break;
        };
        let chunk = load_remote_response_chunk(
            pending_fetched,
            cache_manager,
            hot_published_chunks,
            chunk_id,
            request.len,
            expected_mtime,
            now_ms,
        );
        match (idx == 0, chunk) {
            (true, Some(item)) => {
                sent = sent.saturating_add(item.data.len() as u64);
                chunks.push(item);
            }
            (true, None) => break,
            (false, Some(item)) => {
                sent = sent.saturating_add(item.data.len() as u64);
                chunks.push(item);
            }
            (false, None) => break,
        }
    }
    PreparedIncomingResponse {
        id: incoming_id,
        response: ChunkFetchResponse {
            data_port: 0,
            ticket: EMPTY_DATA_PLANE_TICKET,
            chunks,
        },
        sent,
    }
}

fn prepare_hot_incoming_fetch_response(
    data_plane: &DataPlaneState,
    hot_published_chunks: &FastDashMap<ChunkId, HotPublishedChunk>,
    tenant_whitelist: &HashSet<String>,
    request: &ChunkFetchRequest,
    redirect_to_data_plane: bool,
) -> Option<PreparedIncomingResponse> {
    if !is_tenant_allowed(&request.tenant_id, tenant_whitelist) {
        return Some(PreparedIncomingResponse {
            id: 0,
            response: ChunkFetchResponse {
                data_port: 0,
                ticket: EMPTY_DATA_PLANE_TICKET,
                chunks: Vec::new(),
            },
            sent: 0,
        });
    }
    let now_ms = LocalTime::mills() as i64;
    let expected_mtime = (request.expected_mtime > 0).then_some(request.expected_mtime);
    let prefetch_count = request.prefetch_count.min(max_prefetch_count());
    let limit = usize::from(prefetch_count) + 1;
    let first_chunk_id = request_chunk_id(request, 0)?;
    let build_hot_item = |chunk_id| {
        hot_published_chunks.get(&chunk_id).and_then(|chunk| {
            if chunk.expire_at_ms <= now_ms {
                hot_published_chunks.remove(&chunk_id);
                return None;
            }
            if expected_mtime
                .is_some_and(|expected| expected > 0 && chunk.mtime > 0 && chunk.mtime != expected)
            {
                return None;
            }
            let len = chunk.data.len().min(request.len.max(1));
            let data = chunk.data.slice(0..len);
            if data.is_empty() {
                return None;
            }
            let checksum = if chunk.checksum.is_empty() || len == chunk.data.len() {
                chunk.checksum.clone()
            } else {
                sha256_bytes(data.as_ref())
            };
            Some(ChunkFetchResponseChunk {
                off: chunk_id.off,
                mtime: chunk.mtime,
                checksum,
                data,
            })
        })
    };
    let first_item = build_hot_item(first_chunk_id)?;
    if redirect_to_data_plane {
        let data_port = data_plane.port();
        if data_port > 0 {
            return Some(PreparedIncomingResponse {
                id: 0,
                response: ChunkFetchResponse {
                    data_port,
                    ticket: data_plane.issue_ticket(request),
                    chunks: Vec::new(),
                },
                sent: 0,
            });
        }
    }
    let mut sent = first_item.data.len() as u64;
    let mut chunks = Vec::with_capacity(limit);
    chunks.push(first_item);
    for idx in 1..limit {
        let Some(chunk_id) = request_chunk_id(request, idx) else {
            break;
        };
        let Some(item) = build_hot_item(chunk_id) else {
            break;
        };
        sent = sent.saturating_add(item.data.len() as u64);
        chunks.push(item);
    }
    Some(PreparedIncomingResponse {
        id: 0,
        response: ChunkFetchResponse {
            data_port: 0,
            ticket: EMPTY_DATA_PLANE_TICKET,
            chunks,
        },
        sent,
    })
}

fn request_chunk_id(request: &ChunkFetchRequest, index: usize) -> Option<ChunkId> {
    let step = i64::try_from(request.len.max(1)).ok()?;
    let idx = i64::try_from(index).ok()?;
    let off = request.off.checked_add(step.checked_mul(idx)?)?;
    Some(ChunkId::with_version(
        request.file_id,
        request.version_epoch,
        request.block_id,
        off,
    ))
}

fn load_remote_response_chunk(
    pending_fetched: &FastDashMap<ChunkId, PendingFetchedChunk>,
    cache_manager: &CacheManager,
    hot_published_chunks: &FastDashMap<ChunkId, HotPublishedChunk>,
    chunk_id: ChunkId,
    max_len: usize,
    expected_mtime: Option<i64>,
    now_ms: i64,
) -> Option<ChunkFetchResponseChunk> {
    if let Some(chunk) =
        load_remote_pending_chunk(pending_fetched, chunk_id, max_len, expected_mtime)
    {
        return Some(chunk);
    }
    let hot_chunk = hot_published_chunks
        .get(&chunk_id)
        .map(|chunk| chunk.clone());
    if hot_chunk
        .as_ref()
        .is_some_and(|chunk| chunk.expire_at_ms <= now_ms)
    {
        hot_published_chunks.remove(&chunk_id);
    }
    if let Some(chunk) = hot_chunk.filter(|chunk| {
        chunk.expire_at_ms > now_ms
            && expected_mtime
                .is_none_or(|expected| expected <= 0 || chunk.mtime <= 0 || chunk.mtime == expected)
    }) {
        let len = chunk.data.len().min(max_len.max(1));
        let data = chunk.data.slice(0..len);
        let checksum = if chunk.checksum.is_empty() || len == chunk.data.len() {
            chunk.checksum
        } else {
            sha256_bytes(data.as_ref())
        };
        return Some(ChunkFetchResponseChunk {
            off: chunk_id.off,
            mtime: chunk.mtime,
            checksum,
            data,
        });
    }
    let cache = cache_manager.get_for_remote(chunk_id, max_len, expected_mtime);
    cache.chunk.map(|chunk| ChunkFetchResponseChunk {
        off: chunk_id.off,
        mtime: chunk.mtime,
        checksum: chunk.checksum,
        data: chunk.data,
    })
}

fn load_remote_pending_chunk(
    pending_fetched: &FastDashMap<ChunkId, PendingFetchedChunk>,
    chunk_id: ChunkId,
    max_len: usize,
    expected_mtime: Option<i64>,
) -> Option<ChunkFetchResponseChunk> {
    let pending = pending_fetched.get(&chunk_id)?.clone();
    match pending {
        PendingFetchedChunk::Ready { data, mtime, .. } => {
            if expected_mtime.is_some_and(|expected| expected > 0 && mtime > 0 && mtime != expected)
            {
                return None;
            }
            let len = data.len().min(max_len.max(1));
            let data = data.slice(0..len);
            Some(ChunkFetchResponseChunk {
                off: chunk_id.off,
                mtime,
                checksum: sha256_bytes(data.as_ref()),
                data,
            })
        }
        PendingFetchedChunk::Loading(_) => None,
    }
}

struct IncomingFetchResponseCtx<'a> {
    swarm: &'a mut libp2p::Swarm<P2pNetworkBehaviour>,
    conf: &'a ClientP2pConf,
    init: &'a NetworkInitState,
    loop_state: &'a mut NetworkLoopState,
    stats: &'a Arc<PeerStats>,
}

fn handle_incoming_fetch_response(
    ctx: &mut IncomingFetchResponseCtx<'_>,
    peer: PeerId,
    request_id: request_response::OutboundRequestId,
    response: ChunkFetchResponse,
) {
    if let Some(mut pending) = ctx.loop_state.pending_requests.remove(&request_id) {
        release_peer_inflight(&pending, &mut ctx.loop_state.inflight_per_peer);
        pending.active_peer = None;
        if let Some(mut pending) =
            handle_fetch_response_payload(pending, peer, response, ctx.loop_state, ctx.stats)
        {
            pending.started_at = Instant::now();
            dispatch_fetch_request(ctx.swarm, pending, ctx.init, ctx.loop_state, ctx.conf);
        }
    }
}

fn handle_fetch_response_payload(
    mut pending: PendingFetchRequest,
    peer: PeerId,
    response: ChunkFetchResponse,
    loop_state: &mut NetworkLoopState,
    stats: &Arc<PeerStats>,
) -> Option<PendingFetchRequest> {
    let redirected_to_data_plane = response.chunks.is_empty()
        && response.data_port > 0
        && response.ticket != EMPTY_DATA_PLANE_TICKET;
    cache_peer_data_route(
        loop_state,
        peer,
        pending.chunk_id,
        pending.trace.tenant_id.as_str(),
        pending.expected_mtime,
        response.data_port,
        response.ticket,
    );
    if redirected_to_data_plane {
        emit_p2p_trace(
            "transfer",
            "response_redirect",
            &pending.trace,
            pending.chunk_id,
            Some(&peer),
            Some(pending.started_at.elapsed().as_secs_f64() * 1000.0),
            None,
            Some("data_plane_redirect"),
        );
        pending.candidates.push_front(peer);
        return Some(pending);
    }
    let response_len = response
        .chunks
        .iter()
        .map(|item| item.data.len())
        .sum::<usize>();
    let mut selected = None;
    let mut prefetched = Vec::new();
    for item in response.chunks {
        if item.off == pending.chunk_id.off {
            selected = Some(item);
            continue;
        }
        if item.data.is_empty() {
            continue;
        }
        prefetched.push(PrefetchedChunk {
            chunk_id: ChunkId::with_version(
                pending.chunk_id.file_id,
                pending.chunk_id.version_epoch,
                pending.chunk_id.block_id,
                item.off,
            ),
            data: item.data,
            mtime: item.mtime,
            checksum: item.checksum,
        });
    }
    if let Some(item) = selected {
        let len = item.data.len().min(pending.max_len.max(1));
        let bytes = item.data.slice(0..len);
        let checksum = if item.checksum.is_empty() || len == item.data.len() {
            item.checksum
        } else {
            sha256_bytes(bytes.as_ref())
        };
        if !bytes.is_empty() {
            let elapsed = pending.started_at.elapsed();
            let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
            let recv = response_len.max(bytes.len());
            stats.bytes_recv.fetch_add(recv as u64, Ordering::Relaxed);
            stats.latency_us_total.fetch_add(
                elapsed.as_micros().min(u64::MAX as u128) as u64,
                Ordering::Relaxed,
            );
            stats.latency_samples.fetch_add(1, Ordering::Relaxed);
            loop_state
                .peer_ewma
                .entry(peer)
                .or_default()
                .observe_success(elapsed_ms, recv);
            emit_p2p_trace(
                "transfer",
                "response_ok",
                &pending.trace,
                pending.chunk_id,
                Some(&peer),
                Some(elapsed_ms),
                Some(recv),
                None,
            );
            let _ = pending.response.send(Some(FetchedChunk {
                data: bytes,
                mtime: item.mtime,
                checksum,
                prefetched,
            }));
            return None;
        }
    }
    emit_p2p_trace(
        "transfer",
        "response_retry",
        &pending.trace,
        pending.chunk_id,
        Some(&peer),
        Some(pending.started_at.elapsed().as_secs_f64() * 1000.0),
        Some(response_len),
        Some("response_not_found_or_empty"),
    );
    loop_state
        .peer_ewma
        .entry(peer)
        .or_default()
        .observe_failure();
    Some(pending)
}

fn cache_peer_data_route(
    loop_state: &mut NetworkLoopState,
    peer: PeerId,
    chunk_id: ChunkId,
    tenant_id: &str,
    expected_mtime: Option<i64>,
    data_port: u16,
    ticket: [u8; 16],
) {
    if data_port > 0 {
        loop_state.peer_data_ports.insert(peer, data_port);
    }
    if ticket != EMPTY_DATA_PLANE_TICKET {
        loop_state.peer_block_tickets.insert(
            peer_block_ticket_key(peer, chunk_id, tenant_id, expected_mtime),
            ticket,
        );
    }
}

fn peer_block_ticket_key(
    peer: PeerId,
    chunk_id: ChunkId,
    tenant_id: &str,
    expected_mtime: Option<i64>,
) -> (PeerId, ProviderRecordId, String, i64) {
    (
        peer,
        provider_record_id(chunk_id),
        tenant_id.trim().to_string(),
        expected_mtime.unwrap_or(0),
    )
}

fn handle_stream_fetch_outcome(
    outcome: StreamFetchOutcome,
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    conf: &ClientP2pConf,
    init: &NetworkInitState,
    loop_state: &mut NetworkLoopState,
    stats: &Arc<PeerStats>,
) {
    let Some(mut pending) = loop_state
        .pending_stream_requests
        .remove(&outcome.request_id)
    else {
        return;
    };
    release_peer_inflight(&pending, &mut loop_state.inflight_per_peer);
    pending.active_peer = None;
    if let Some(response) = outcome.response {
        if let Some(next_pending) =
            handle_fetch_response_payload(pending, outcome.peer, response, loop_state, stats)
        {
            pending = next_pending;
        } else {
            return;
        }
    } else {
        emit_p2p_trace(
            "transfer",
            "stream_failure",
            &pending.trace,
            pending.chunk_id,
            Some(&outcome.peer),
            Some(pending.started_at.elapsed().as_secs_f64() * 1000.0),
            None,
            Some("stream_failure"),
        );
        loop_state
            .peer_ewma
            .entry(outcome.peer)
            .or_default()
            .observe_failure();
    }
    if outcome.unsupported_protocol {
        loop_state.stream_unsupported_peers.insert(outcome.peer);
        pending.candidates.push_front(outcome.peer);
    }
    pending.started_at = Instant::now();
    dispatch_fetch_request(swarm, pending, init, loop_state, conf);
}

fn handle_data_plane_fetch_outcome(
    outcome: DataPlaneFetchOutcome,
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    conf: &ClientP2pConf,
    init: &NetworkInitState,
    loop_state: &mut NetworkLoopState,
    stats: &Arc<PeerStats>,
) {
    let Some(mut pending) = loop_state.pending_data_requests.remove(&outcome.request_id) else {
        return;
    };
    release_peer_inflight(&pending, &mut loop_state.inflight_per_peer);
    pending.active_peer = None;
    if let Some(chunk) = outcome.chunk {
        let elapsed = pending.started_at.elapsed();
        let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
        let recv = outcome.recv.max(chunk.data.len());
        stats.bytes_recv.fetch_add(recv as u64, Ordering::Relaxed);
        stats.latency_us_total.fetch_add(
            elapsed.as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        stats.latency_samples.fetch_add(1, Ordering::Relaxed);
        loop_state
            .peer_ewma
            .entry(outcome.peer)
            .or_default()
            .observe_success(elapsed_ms, recv);
        emit_p2p_trace(
            "transfer",
            "response_ok",
            &pending.trace,
            pending.chunk_id,
            Some(&outcome.peer),
            Some(elapsed_ms),
            Some(recv),
            None,
        );
        let _ = pending.response.send(Some(chunk));
        return;
    } else {
        emit_p2p_trace(
            "transfer",
            "data_plane_failure",
            &pending.trace,
            pending.chunk_id,
            Some(&outcome.peer),
            Some(pending.started_at.elapsed().as_secs_f64() * 1000.0),
            None,
            Some("data_plane_failure"),
        );
        loop_state
            .peer_ewma
            .entry(outcome.peer)
            .or_default()
            .observe_failure();
        loop_state.peer_block_tickets.remove(&peer_block_ticket_key(
            outcome.peer,
            pending.chunk_id,
            pending.trace.tenant_id.as_str(),
            pending.expected_mtime,
        ));
    }
    pending.started_at = Instant::now();
    dispatch_fetch_request(swarm, pending, init, loop_state, conf);
}

fn handle_outbound_fetch_failure(
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    conf: &ClientP2pConf,
    init: &NetworkInitState,
    loop_state: &mut NetworkLoopState,
    peer: PeerId,
    request_id: request_response::OutboundRequestId,
) {
    if let Some(mut pending) = loop_state.pending_requests.remove(&request_id) {
        release_peer_inflight(&pending, &mut loop_state.inflight_per_peer);
        pending.active_peer = None;
        emit_p2p_trace(
            "transfer",
            "outbound_failure",
            &pending.trace,
            pending.chunk_id,
            Some(&peer),
            Some(pending.started_at.elapsed().as_secs_f64() * 1000.0),
            None,
            Some("outbound_failure"),
        );
        loop_state
            .peer_ewma
            .entry(peer)
            .or_default()
            .observe_failure();
        pending.started_at = Instant::now();
        dispatch_fetch_request(swarm, pending, init, loop_state, conf);
    }
}

fn cancel_pending_by_token(
    fetch_token: u64,
    pending_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>,
    pending_requests: &mut HashMap<request_response::OutboundRequestId, PendingFetchRequest>,
    pending_stream_requests: &mut HashMap<u64, PendingFetchRequest>,
    pending_data_requests: &mut HashMap<u64, PendingFetchRequest>,
    inflight_per_peer: &mut HashMap<PeerId, usize>,
) {
    let query_ids: Vec<kad::QueryId> = pending_queries
        .iter()
        .filter_map(|(id, pending)| (pending.fetch_token == fetch_token).then_some(*id))
        .collect();
    for query_id in query_ids {
        if let Some(pending) = pending_queries.remove(&query_id) {
            emit_p2p_trace(
                "discover",
                "cancelled",
                &pending.trace,
                pending.chunk_id,
                None,
                None,
                None,
                Some("cancelled"),
            );
            let _ = pending.response.send(None);
        }
    }

    let request_ids: Vec<request_response::OutboundRequestId> = pending_requests
        .iter()
        .filter_map(|(id, pending)| (pending.fetch_token == fetch_token).then_some(*id))
        .collect();
    for request_id in request_ids {
        if let Some(pending) = pending_requests.remove(&request_id) {
            release_peer_inflight(&pending, inflight_per_peer);
            emit_p2p_trace(
                "transfer",
                "cancelled",
                &pending.trace,
                pending.chunk_id,
                pending.active_peer.as_ref(),
                None,
                None,
                Some("cancelled"),
            );
            let _ = pending.response.send(None);
        }
    }

    let stream_request_ids: Vec<u64> = pending_stream_requests
        .iter()
        .filter_map(|(id, pending)| (pending.fetch_token == fetch_token).then_some(*id))
        .collect();
    for request_id in stream_request_ids {
        if let Some(pending) = pending_stream_requests.remove(&request_id) {
            release_peer_inflight(&pending, inflight_per_peer);
            emit_p2p_trace(
                "transfer",
                "cancelled",
                &pending.trace,
                pending.chunk_id,
                pending.active_peer.as_ref(),
                None,
                None,
                Some("cancelled"),
            );
            let _ = pending.response.send(None);
        }
    }

    let data_request_ids: Vec<u64> = pending_data_requests
        .iter()
        .filter_map(|(id, pending)| (pending.fetch_token == fetch_token).then_some(*id))
        .collect();
    for request_id in data_request_ids {
        if let Some(pending) = pending_data_requests.remove(&request_id) {
            release_peer_inflight(&pending, inflight_per_peer);
            emit_p2p_trace(
                "transfer",
                "cancelled",
                &pending.trace,
                pending.chunk_id,
                pending.active_peer.as_ref(),
                None,
                None,
                Some("cancelled"),
            );
            let _ = pending.response.send(None);
        }
    }
}

fn expire_pending_queries(
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    conf: &ClientP2pConf,
    init: &NetworkInitState,
    loop_state: &mut NetworkLoopState,
) {
    let now = Instant::now();
    let expired_ids: Vec<kad::QueryId> = loop_state
        .pending_queries
        .iter()
        .filter_map(|(id, pending)| (pending.deadline <= now).then_some(*id))
        .collect();
    for query_id in expired_ids {
        if let Some(pending) = loop_state.pending_queries.remove(&query_id) {
            emit_p2p_trace(
                "discover",
                "timeout",
                &pending.trace,
                pending.chunk_id,
                None,
                None,
                None,
                Some("discover_timeout"),
            );
            let candidates = build_candidates(
                std::iter::empty::<PeerId>(),
                &loop_state.connected_peers,
                &loop_state.bootstrap_connected,
                init.local_peer_id,
                &loop_state.peer_ewma,
                &loop_state.inflight_per_peer,
            );
            dispatch_fetch_request(
                swarm,
                PendingFetchRequest {
                    fetch_token: pending.fetch_token,
                    chunk_id: pending.chunk_id,
                    max_len: pending.max_len,
                    expected_mtime: pending.expected_mtime,
                    trace: pending.trace,
                    response: pending.response,
                    candidates,
                    active_peer: None,
                    defer_dht: false,
                    started_at: now,
                    deadline: now + loop_state.transfer_timeout,
                },
                init,
                loop_state,
                conf,
            );
        }
    }
}

fn expire_pending_requests(
    pending_requests: &mut HashMap<request_response::OutboundRequestId, PendingFetchRequest>,
    pending_stream_requests: &mut HashMap<u64, PendingFetchRequest>,
    pending_data_requests: &mut HashMap<u64, PendingFetchRequest>,
    inflight_per_peer: &mut HashMap<PeerId, usize>,
) {
    let now = Instant::now();
    let expired_ids: Vec<request_response::OutboundRequestId> = pending_requests
        .iter()
        .filter_map(|(id, pending)| (pending.deadline <= now).then_some(*id))
        .collect();
    for request_id in expired_ids {
        if let Some(pending) = pending_requests.remove(&request_id) {
            release_peer_inflight(&pending, inflight_per_peer);
            emit_p2p_trace(
                "transfer",
                "timeout",
                &pending.trace,
                pending.chunk_id,
                pending.active_peer.as_ref(),
                None,
                None,
                Some("transfer_timeout"),
            );
            let _ = pending.response.send(None);
        }
    }

    let expired_stream_ids: Vec<u64> = pending_stream_requests
        .iter()
        .filter_map(|(id, pending)| (pending.deadline <= now).then_some(*id))
        .collect();
    for request_id in expired_stream_ids {
        if let Some(pending) = pending_stream_requests.remove(&request_id) {
            release_peer_inflight(&pending, inflight_per_peer);
            emit_p2p_trace(
                "transfer",
                "timeout",
                &pending.trace,
                pending.chunk_id,
                pending.active_peer.as_ref(),
                None,
                None,
                Some("transfer_timeout"),
            );
            let _ = pending.response.send(None);
        }
    }

    let expired_data_ids: Vec<u64> = pending_data_requests
        .iter()
        .filter_map(|(id, pending)| (pending.deadline <= now).then_some(*id))
        .collect();
    for request_id in expired_data_ids {
        if let Some(pending) = pending_data_requests.remove(&request_id) {
            release_peer_inflight(&pending, inflight_per_peer);
            emit_p2p_trace(
                "transfer",
                "timeout",
                &pending.trace,
                pending.chunk_id,
                pending.active_peer.as_ref(),
                None,
                None,
                Some("transfer_timeout"),
            );
            let _ = pending.response.send(None);
        }
    }
}

fn expire_pending_incoming(pending_incoming: &mut HashMap<u64, PendingIncomingResponse>) {
    let now = Instant::now();
    let expired_ids: Vec<u64> = pending_incoming
        .iter()
        .filter_map(|(id, pending)| (pending.deadline <= now).then_some(*id))
        .collect();
    for pending_id in expired_ids {
        pending_incoming.remove(&pending_id);
    }
}

fn ewma(current: f64, observed: f64, alpha: f64) -> f64 {
    current * (1.0 - alpha) + observed * alpha
}

fn maybe_cleanup_expired_cache(
    cache_manager: &CacheManager,
    loop_state: &mut NetworkLoopState,
    now: Instant,
) {
    if now >= loop_state.next_cache_cleanup_at {
        cache_manager.cleanup_expired();
        loop_state.next_cache_cleanup_at = now + loop_state.cache_cleanup_interval;
    }
}

fn provider_cache_ttl(conf: &ClientP2pConf) -> Duration {
    let min_ttl = discovery_timeout(conf);
    std::cmp::max(
        min_ttl,
        std::cmp::min(conf.provider_publish_interval, conf.provider_ttl),
    )
}

fn discovery_timeout(conf: &ClientP2pConf) -> Duration {
    Duration::from_millis(conf.discovery_timeout_ms.max(1))
}

fn connect_timeout(conf: &ClientP2pConf) -> Duration {
    Duration::from_millis(conf.connect_timeout_ms.max(1))
}

fn transfer_timeout(conf: &ClientP2pConf) -> Duration {
    Duration::from_millis(conf.transfer_timeout_ms.max(1))
}

fn data_plane_ticket_ttl(conf: &ClientP2pConf) -> Duration {
    Duration::from_millis(
        conf.transfer_timeout_ms
            .max(1)
            .saturating_mul(4)
            .clamp(500, 5_000),
    )
}

fn network_warmup_timeout(conf: &ClientP2pConf) -> Duration {
    connect_timeout(conf).min(Duration::from_millis(500))
}

fn publish_persist_parallelism(conf: &ClientP2pConf) -> usize {
    conf.max_inflight_requests.clamp(1, 8)
}

fn publish_task_limit(conf: &ClientP2pConf) -> usize {
    publish_persist_parallelism(conf).saturating_mul(32)
}

fn fetched_persist_batch_size(conf: &ClientP2pConf) -> usize {
    publish_persist_parallelism(conf)
        .saturating_mul(8)
        .clamp(8, 128)
}

fn fetched_persist_max_batch_size(conf: &ClientP2pConf) -> usize {
    fetched_persist_batch_size(conf)
        .saturating_mul(4)
        .clamp(32, 512)
}

fn fetched_persist_worker_count(conf: &ClientP2pConf) -> usize {
    let _ = conf;
    1
}

fn stream_fetch_worker_count(conf: &ClientP2pConf) -> usize {
    conf.max_inflight_requests.div_ceil(64).clamp(1, 4)
}

fn fetched_persist_queue_capacity(conf: &ClientP2pConf) -> usize {
    fetched_persist_batch_size(conf).saturating_mul(64)
}

fn fetched_persist_batch_window(conf: &ClientP2pConf) -> Duration {
    let timeout_ms = (transfer_timeout(conf).as_millis() / 2).clamp(100, 1000);
    Duration::from_millis(timeout_ms as u64)
}

fn hot_published_entries_limit(conf: &ClientP2pConf) -> usize {
    conf.max_inflight_requests.clamp(64, 1024)
}

fn hot_published_ttl(conf: &ClientP2pConf) -> Duration {
    conf.provider_ttl
        .max(Duration::from_secs(60))
        .min(Duration::from_secs(300))
}

fn fetched_persist_worker_index(chunk_id: ChunkId, worker_count: usize) -> usize {
    if worker_count <= 1 {
        return 0;
    }
    let hash = (chunk_id.file_id as u64)
        ^ (chunk_id.version_epoch as u64).rotate_left(7)
        ^ (chunk_id.block_id as u64).rotate_left(13)
        ^ (chunk_id.off as u64).rotate_left(29);
    (hash as usize) % worker_count
}

fn request_timeout(conf: &ClientP2pConf) -> Duration {
    Duration::from_millis(conf.request_timeout_ms.max(1))
}

fn dynamic_prefetch_count(max_len: usize, stats: Option<PeerEwma>) -> u16 {
    let full_window_bytes =
        max_len.saturating_mul(usize::from(max_prefetch_count()).saturating_add(1));
    let target_bytes = match stats {
        None => 16 * 1024 * 1024usize,
        Some(v) if v.latency_ms <= 2.0 => 16 * 1024 * 1024usize,
        Some(v) if v.latency_ms <= 10.0 => 32 * 1024 * 1024usize,
        Some(v) if v.latency_ms <= 40.0 => 64 * 1024 * 1024usize,
        Some(_) => full_window_bytes,
    };
    prefetch_count_for_target(max_len, target_bytes.min(full_window_bytes))
}

fn data_plane_prefetch_count(max_len: usize, conf: &ClientP2pConf) -> u16 {
    prefetch_count_for_target(max_len, response_frame_limit_bytes(conf))
}

fn prefetch_count_for_target(max_len: usize, target_bytes: usize) -> u16 {
    if max_len == 0 {
        return 0;
    }
    let total = target_bytes
        .checked_div(max_len.max(1))
        .unwrap_or(1)
        .clamp(1, usize::from(max_prefetch_count()) + 1);
    (total.saturating_sub(1)) as u16
}

fn max_prefetch_count() -> u16 {
    127
}

fn should_verify_network_checksum(fetch_token: u64) -> bool {
    fetch_token <= 4 || fetch_token.is_multiple_of(64)
}

fn hedge_delay(conf: &ClientP2pConf) -> Option<Duration> {
    (conf.hedge_delay_ms > 0).then(|| Duration::from_millis(conf.hedge_delay_ms))
}

fn maintenance_tick_interval(conf: &ClientP2pConf) -> Duration {
    let min_timeout_ms = discovery_timeout(conf)
        .as_millis()
        .min(transfer_timeout(conf).as_millis());
    Duration::from_millis(min_timeout_ms.clamp(100, 1000) as u64)
}

fn cache_cleanup_interval(conf: &ClientP2pConf) -> Duration {
    let ttl_ms = conf.cache_ttl.as_millis();
    Duration::from_millis(ttl_ms.clamp(1000, 60_000) as u64)
}

fn expire_provider_cache(provider_cache: &mut HashMap<ChunkId, CachedProviders>, now: Instant) {
    let expired_ids: Vec<ChunkId> = provider_cache
        .iter()
        .filter_map(|(chunk_id, providers)| (providers.expire_at <= now).then_some(*chunk_id))
        .collect();
    for chunk_id in expired_ids {
        provider_cache.remove(&chunk_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot::error::TryRecvError;

    fn new_peer_id() -> PeerId {
        identity::Keypair::generate_ed25519().public().to_peer_id()
    }

    fn test_conf(case: &str) -> ClientP2pConf {
        ClientP2pConf {
            enable_mdns: false,
            enable_dht: true,
            request_timeout_ms: 200,
            discovery_timeout_ms: 200,
            connect_timeout_ms: 200,
            transfer_timeout_ms: 200,
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
            cache_dir: std::env::temp_dir()
                .join(format!("curvine-p2p-service-{case}-{}", new_peer_id()))
                .to_string_lossy()
                .to_string(),
            ..ClientP2pConf::default()
        }
    }

    fn test_runtime() -> Arc<Runtime> {
        static TEST_RT: Lazy<Arc<Runtime>> =
            Lazy::new(|| Arc::new(Runtime::new("p2p-service-test", 2, 2)));
        TEST_RT.clone()
    }

    async fn wait_for_data_plane_port(service: &P2pService, deadline: Duration) -> Option<u16> {
        let started = Instant::now();
        while started.elapsed() < deadline {
            let port = service.data_plane.port();
            if port > 0 && service.data_plane.started.load(Ordering::Relaxed) {
                return Some(port);
            }
            sleep(Duration::from_millis(25)).await;
        }
        None
    }

    async fn wait_for_data_plane_stopped(service: &P2pService, deadline: Duration) -> Option<()> {
        let started = Instant::now();
        while started.elapsed() < deadline {
            if service.data_plane.port() == 0 && !service.data_plane.started.load(Ordering::Relaxed)
            {
                return Some(());
            }
            sleep(Duration::from_millis(25)).await;
        }
        None
    }

    #[test]
    fn empty_peer_whitelist_remains_open_after_bootstrap_merge() {
        let bootstrap_peer_ids = HashSet::from([new_peer_id()]);
        let effective = build_effective_peer_whitelist(HashSet::new(), &bootstrap_peer_ids);
        assert!(effective.is_empty());
    }

    #[test]
    fn explicit_peer_whitelist_keeps_bootstrap_peers_allowed() {
        let allowed_peer = new_peer_id();
        let bootstrap_peer = new_peer_id();
        let effective = build_effective_peer_whitelist(
            HashSet::from([allowed_peer]),
            &HashSet::from([bootstrap_peer]),
        );
        assert!(effective.contains(&allowed_peer));
        assert!(effective.contains(&bootstrap_peer));
    }

    #[test]
    fn invalid_peer_whitelist_entry_is_rejected() {
        assert!(parse_peer_whitelist(&["invalid-peer-id".to_string()]).is_none());
    }

    #[test]
    fn invalid_startup_peer_whitelist_fails_closed() {
        let local_peer_id = new_peer_id();
        let whitelist = parse_peer_whitelist(&["invalid-peer-id".to_string()])
            .map(|parsed_peer_whitelist| {
                build_effective_peer_whitelist(parsed_peer_whitelist, &HashSet::new())
            })
            .unwrap_or_else(|| HashSet::from([local_peer_id]));
        assert_eq!(whitelist, HashSet::from([local_peer_id]));
    }

    #[test]
    fn request_timeout_uses_dedicated_config() {
        let conf = ClientP2pConf {
            request_timeout_ms: 1234,
            ..ClientP2pConf::default()
        };
        assert_eq!(request_timeout(&conf), Duration::from_millis(1234));
    }

    #[test]
    fn network_checksum_verify_sampling_is_periodic() {
        assert!(should_verify_network_checksum(1));
        assert!(should_verify_network_checksum(4));
        assert!(!should_verify_network_checksum(5));
        assert!(should_verify_network_checksum(64));
        assert!(!should_verify_network_checksum(65));
    }

    #[test]
    fn provider_record_id_collapses_offsets_in_same_block() {
        let a = ChunkId::with_version(1, 2, 3, 0);
        let b = ChunkId::with_version(1, 2, 3, 1024);
        let c = ChunkId::with_version(1, 2, 4, 0);
        assert_eq!(provider_record_id(a), provider_record_id(b));
        assert_ne!(provider_record_id(a), provider_record_id(c));
    }

    #[test]
    fn cache_cleanup_interval_is_clamped() {
        let short_ttl = ClientP2pConf {
            cache_ttl: Duration::from_millis(10),
            ..ClientP2pConf::default()
        };
        assert_eq!(cache_cleanup_interval(&short_ttl), Duration::from_secs(1));

        let long_ttl = ClientP2pConf {
            cache_ttl: Duration::from_secs(120),
            ..ClientP2pConf::default()
        };
        assert_eq!(cache_cleanup_interval(&long_ttl), Duration::from_secs(60));
    }

    #[test]
    fn network_warmup_timeout_is_capped() {
        let quick = ClientP2pConf {
            connect_timeout_ms: 100,
            ..ClientP2pConf::default()
        };
        assert_eq!(network_warmup_timeout(&quick), Duration::from_millis(100));

        let slow = ClientP2pConf {
            connect_timeout_ms: 5_000,
            ..ClientP2pConf::default()
        };
        assert_eq!(network_warmup_timeout(&slow), Duration::from_millis(500));
    }

    #[test]
    fn fetched_persist_batch_window_is_clamped() {
        let fast = ClientP2pConf {
            transfer_timeout_ms: 0,
            ..ClientP2pConf::default()
        };
        assert_eq!(
            fetched_persist_batch_window(&fast),
            Duration::from_millis(100)
        );

        let slow = ClientP2pConf {
            transfer_timeout_ms: 5_000,
            ..ClientP2pConf::default()
        };
        assert_eq!(
            fetched_persist_batch_window(&slow),
            Duration::from_millis(1000)
        );
    }

    #[test]
    fn fetched_persist_max_batch_size_is_capped() {
        let small = ClientP2pConf {
            max_inflight_requests: 1,
            ..ClientP2pConf::default()
        };
        assert_eq!(fetched_persist_batch_size(&small), 8);
        assert_eq!(fetched_persist_max_batch_size(&small), 32);

        let large = ClientP2pConf {
            max_inflight_requests: 2048,
            ..ClientP2pConf::default()
        };
        assert_eq!(fetched_persist_batch_size(&large), 64);
        assert_eq!(fetched_persist_max_batch_size(&large), 256);
    }

    #[test]
    fn hot_published_cache_params_are_clamped() {
        let tiny = ClientP2pConf {
            max_inflight_requests: 1,
            provider_ttl: Duration::from_millis(1),
            ..ClientP2pConf::default()
        };
        assert_eq!(hot_published_entries_limit(&tiny), 64);
        assert_eq!(hot_published_ttl(&tiny), Duration::from_secs(60));

        let huge = ClientP2pConf {
            max_inflight_requests: 16_384,
            provider_ttl: Duration::from_secs(1000),
            ..ClientP2pConf::default()
        };
        assert_eq!(hot_published_entries_limit(&huge), 1024);
        assert_eq!(hot_published_ttl(&huge), Duration::from_secs(300));
    }

    #[test]
    fn compact_fetched_persist_batch_keeps_latest_for_same_chunk() {
        let chunk_a = ChunkId::with_version(7, 1, 1, 0);
        let chunk_b = ChunkId::with_version(7, 1, 1, 64);
        let compacted = compact_fetched_persist_batch(vec![
            PendingFetchedPersist {
                chunk_id: chunk_a,
                data: Bytes::from_static(b"a-1"),
                mtime: 10,
                checksum: Bytes::from_static(b"sum-a-1"),
                persist_token: 1,
            },
            PendingFetchedPersist {
                chunk_id: chunk_b,
                data: Bytes::from_static(b"b-1"),
                mtime: 11,
                checksum: Bytes::from_static(b"sum-b-1"),
                persist_token: 2,
            },
            PendingFetchedPersist {
                chunk_id: chunk_a,
                data: Bytes::from_static(b"a-2"),
                mtime: 12,
                checksum: Bytes::from_static(b"sum-a-2"),
                persist_token: 3,
            },
        ]);
        let compacted_map: HashMap<ChunkId, PendingFetchedPersist> = compacted
            .into_iter()
            .map(|item| (item.chunk_id, item))
            .collect();
        assert_eq!(compacted_map.len(), 2);
        let chunk_a_item = compacted_map.get(&chunk_a).expect("chunk_a should exist");
        assert_eq!(chunk_a_item.data, Bytes::from_static(b"a-2"));
        assert_eq!(chunk_a_item.mtime, 12);
        assert_eq!(chunk_a_item.persist_token, 3);
    }

    #[test]
    fn fetched_persist_worker_index_stays_in_range() {
        let chunk = ChunkId::with_version(9, 1, 2, 3);
        assert_eq!(fetched_persist_worker_index(chunk, 0), 0);
        assert_eq!(fetched_persist_worker_index(chunk, 1), 0);
        assert!(fetched_persist_worker_index(chunk, 8) < 8);
    }

    #[test]
    fn stream_worker_index_for_peer_is_stable() {
        let peer = new_peer_id();
        assert_eq!(stream_worker_index_for_peer(peer, 0), 0);
        assert_eq!(stream_worker_index_for_peer(peer, 1), 0);
        let idx1 = stream_worker_index_for_peer(peer, 8);
        let idx2 = stream_worker_index_for_peer(peer, 8);
        assert!(idx1 < 8);
        assert_eq!(idx1, idx2);
    }

    #[test]
    fn direct_connected_probe_candidates_is_limited() {
        let conf = test_conf("direct-probe-limit");
        let local_peer_id = new_peer_id();
        let peer_a = new_peer_id();
        let peer_b = new_peer_id();
        let peer_c = new_peer_id();
        let bootstrap_connected = HashSet::new();
        let peer_ewma = HashMap::new();
        let inflight = HashMap::new();

        let two_candidates = direct_connected_probe_candidates(
            &conf,
            &HashSet::from([peer_a, peer_b]),
            &bootstrap_connected,
            local_peer_id,
            &peer_ewma,
            &inflight,
        );
        assert!(two_candidates.is_some());

        let three_candidates = direct_connected_probe_candidates(
            &conf,
            &HashSet::from([peer_a, peer_b, peer_c]),
            &bootstrap_connected,
            local_peer_id,
            &peer_ewma,
            &inflight,
        );
        assert!(three_candidates.is_none());
    }

    #[test]
    fn cold_start_prefetch_caps_buffered_response_window() {
        assert_eq!(dynamic_prefetch_count(1024 * 1024, None), 15);
    }

    #[test]
    fn low_latency_prefetch_stays_medium_sized() {
        let stats = PeerEwma {
            latency_ms: 1.0,
            failure_ratio: 0.0,
            throughput_mb_s: 256.0,
        };
        assert_eq!(dynamic_prefetch_count(1024 * 1024, Some(stats)), 15);
    }

    #[test]
    fn medium_latency_prefetch_expands_window() {
        let stats = PeerEwma {
            latency_ms: 8.0,
            failure_ratio: 0.0,
            throughput_mb_s: 128.0,
        };
        assert_eq!(dynamic_prefetch_count(1024 * 1024, Some(stats)), 31);
    }

    #[test]
    fn high_latency_prefetch_uses_full_window() {
        let stats = PeerEwma {
            latency_ms: 50.0,
            failure_ratio: 0.0,
            throughput_mb_s: 32.0,
        };
        assert_eq!(dynamic_prefetch_count(1024 * 1024, Some(stats)), 127);
    }

    #[test]
    fn data_plane_prefetch_uses_full_response_window() {
        let conf = test_conf("data-plane-prefetch");
        assert_eq!(data_plane_prefetch_count(1024 * 1024, &conf), 127);
    }

    #[test]
    fn pending_fetched_batch_keeps_prefetched_chunks_immediately_available() {
        let chunk_a = ChunkId::with_version(1, 2, 3, 0);
        let chunk_b = ChunkId::with_version(1, 2, 3, 1024);
        let batch = vec![
            FetchedCacheBatchItem {
                chunk_id: chunk_a,
                data: Bytes::from_static(b"a"),
                mtime: 7,
                checksum: Bytes::from_static(b"ca"),
            },
            FetchedCacheBatchItem {
                chunk_id: chunk_b,
                data: Bytes::from_static(b"b"),
                mtime: 8,
                checksum: Bytes::from_static(b"cb"),
            },
        ];
        let (pending_entries, completed) = build_pending_fetched_batch(&batch, {
            let mut next = 40u64;
            move || {
                next += 1;
                next
            }
        });
        assert_eq!(pending_entries.len(), 2);
        assert_eq!(pending_entries[0].0, chunk_a);
        match &pending_entries[0].1 {
            PendingFetchedChunk::Ready {
                data,
                mtime,
                persist_token,
            } => {
                assert_eq!(*data, Bytes::from_static(b"a"));
                assert_eq!(*mtime, 7);
                assert_eq!(*persist_token, 41);
            }
            PendingFetchedChunk::Loading(_) => panic!("batch entries should be ready"),
        }
        assert_eq!(pending_entries[1].0, chunk_b);
        match &pending_entries[1].1 {
            PendingFetchedChunk::Ready {
                data,
                mtime,
                persist_token,
            } => {
                assert_eq!(*data, Bytes::from_static(b"b"));
                assert_eq!(*mtime, 8);
                assert_eq!(*persist_token, 42);
            }
            PendingFetchedChunk::Loading(_) => panic!("batch entries should be ready"),
        }
        assert_eq!(completed, vec![(chunk_a, 41), (chunk_b, 42)]);
    }

    #[test]
    fn response_header_round_trips_data_port() {
        let ticket = [7u8; 16];
        let header = encode_response_header(3, 31_002, ticket);
        let (chunk_count, data_port, parsed_ticket) =
            decode_response_header(header).expect("header should parse");
        assert_eq!(chunk_count, 3);
        assert_eq!(data_port, 31_002);
        assert_eq!(parsed_ticket, ticket);
    }

    #[test]
    fn data_plane_request_round_trips_ticket_and_range() {
        let request = DataPlaneFetchRequest {
            ticket: [9u8; 16],
            file_id: 1,
            version_epoch: 2,
            block_id: 3,
            off: 4,
            len: 1024,
            expected_mtime: 5,
            prefetch_count: 6,
        };
        let encoded = encode_data_plane_request(&request);
        let decoded = decode_data_plane_request(encoded).expect("request should parse");
        assert_eq!(decoded.ticket, request.ticket);
        assert_eq!(decoded.file_id, request.file_id);
        assert_eq!(decoded.version_epoch, request.version_epoch);
        assert_eq!(decoded.block_id, request.block_id);
        assert_eq!(decoded.off, request.off);
        assert_eq!(decoded.len, request.len);
        assert_eq!(decoded.expected_mtime, request.expected_mtime);
        assert_eq!(decoded.prefetch_count, request.prefetch_count);
    }

    #[test]
    fn peer_data_endpoint_uses_remote_ip_for_tcp_peer() {
        let addr = Multiaddr::from_str("/ip4/172.29.0.11/tcp/31001").expect("addr should parse");
        let endpoint = peer_data_endpoint(&addr, 31_002).expect("endpoint should exist");
        assert_eq!(
            endpoint,
            std::net::SocketAddr::from(([172, 29, 0, 11], 31_002))
        );
    }

    #[test]
    fn peer_data_endpoint_uses_remote_ip_for_quic_peer() {
        let addr =
            Multiaddr::from_str("/ip4/172.29.0.11/udp/31001/quic-v1").expect("addr should parse");
        let endpoint = peer_data_endpoint(&addr, 31_002).expect("endpoint should exist");
        assert_eq!(
            endpoint,
            std::net::SocketAddr::from(([172, 29, 0, 11], 31_002))
        );
    }

    #[test]
    fn identify_dial_addr_uses_remote_ip_for_unspecified_listen_addr() {
        let listen_addr =
            Multiaddr::from_str("/ip4/0.0.0.0/tcp/31001").expect("listen addr should parse");
        let remote_addr =
            Multiaddr::from_str("/ip4/172.29.0.11/tcp/49822").expect("remote addr should parse");
        assert_eq!(
            identify_dial_addr(&listen_addr, &remote_addr),
            Some(
                Multiaddr::from_str("/ip4/172.29.0.11/tcp/31001")
                    .expect("normalized addr should parse")
            )
        );
    }

    #[test]
    fn identify_dial_addr_strips_peer_id_suffix() {
        let listen_addr = Multiaddr::from_str(
            "/ip4/172.29.0.11/tcp/31001/p2p/12D3KooWQdB3bVnqJjREd3m7v6ybVNewc19hXtF87mpy4VbQJu1A",
        )
        .expect("listen addr should parse");
        let remote_addr =
            Multiaddr::from_str("/ip4/172.29.0.11/tcp/49822").expect("remote addr should parse");
        assert_eq!(
            identify_dial_addr(&listen_addr, &remote_addr),
            Some(
                Multiaddr::from_str("/ip4/172.29.0.11/tcp/31001")
                    .expect("normalized addr should parse")
            )
        );
    }

    #[test]
    fn control_plane_response_redirects_to_data_plane_when_available() {
        let conf = test_conf("control-plane-redirect");
        let data_plane = DataPlaneState::new(&conf);
        data_plane.port.store(31_002, Ordering::Relaxed);
        let cache_manager = CacheManager::new(&conf);
        let pending_fetched = FastDashMap::default();
        let hot_published_chunks = FastDashMap::default();
        let chunk_id = ChunkId::with_version(1, 2, 3, 0);
        let data = Bytes::from_static(b"hello");
        let checksum = sha256_bytes(data.as_ref());
        let stored =
            cache_manager.put_fetched_with_result_and_checksum(chunk_id, data, 9, Some(checksum));
        assert_eq!(stored, CachePutResultTag::Stored);
        let prepared = prepare_incoming_fetch_response(
            &data_plane,
            &pending_fetched,
            &cache_manager,
            &hot_published_chunks,
            &HashSet::new(),
            ChunkFetchRequest {
                file_id: 1,
                version_epoch: 2,
                block_id: 3,
                off: 0,
                len: 5,
                expected_mtime: 9,
                prefetch_count: 0,
                tenant_id: String::new(),
            },
            7,
            true,
        );
        assert_eq!(prepared.id, 7);
        assert_eq!(prepared.sent, 0);
        assert_eq!(prepared.response.data_port, 31_002);
        assert_ne!(prepared.response.ticket, EMPTY_DATA_PLANE_TICKET);
        assert!(prepared.response.chunks.is_empty());
    }

    #[test]
    fn control_plane_response_does_not_redirect_without_first_chunk() {
        let conf = test_conf("control-plane-no-redirect-without-first-chunk");
        let data_plane = DataPlaneState::new(&conf);
        data_plane.port.store(31_002, Ordering::Relaxed);
        let cache_manager = CacheManager::new(&conf);
        let pending_fetched = FastDashMap::default();
        let hot_published_chunks = FastDashMap::default();
        let prepared = prepare_incoming_fetch_response(
            &data_plane,
            &pending_fetched,
            &cache_manager,
            &hot_published_chunks,
            &HashSet::new(),
            ChunkFetchRequest {
                file_id: 1,
                version_epoch: 2,
                block_id: 3,
                off: 0,
                len: 5,
                expected_mtime: 9,
                prefetch_count: 0,
                tenant_id: String::new(),
            },
            7,
            true,
        );
        assert_eq!(prepared.id, 7);
        assert_eq!(prepared.sent, 0);
        assert_eq!(prepared.response.data_port, 0);
        assert_eq!(prepared.response.ticket, EMPTY_DATA_PLANE_TICKET);
        assert!(prepared.response.chunks.is_empty());
        assert!(data_plane.tickets.is_empty());
    }

    #[test]
    fn hot_control_plane_response_falls_back_when_first_chunk_missing() {
        let conf = test_conf("hot-control-plane-no-redirect-without-first-chunk");
        let data_plane = DataPlaneState::new(&conf);
        data_plane.port.store(31_002, Ordering::Relaxed);
        let hot_published_chunks = FastDashMap::default();
        let prepared = prepare_hot_incoming_fetch_response(
            &data_plane,
            &hot_published_chunks,
            &HashSet::new(),
            &ChunkFetchRequest {
                file_id: 1,
                version_epoch: 2,
                block_id: 3,
                off: 0,
                len: 5,
                expected_mtime: 9,
                prefetch_count: 0,
                tenant_id: String::new(),
            },
            true,
        );
        assert!(prepared.is_none());
        assert!(data_plane.tickets.is_empty());
    }

    #[test]
    fn data_plane_follow_up_response_does_not_issue_ticket() {
        let conf = test_conf("data-plane-follow-up");
        let data_plane = DataPlaneState::new(&conf);
        data_plane.port.store(31_002, Ordering::Relaxed);
        let cache_manager = CacheManager::new(&conf);
        let pending_fetched = FastDashMap::default();
        let hot_published_chunks = FastDashMap::default();
        let chunk_id = ChunkId::with_version(1, 2, 3, 0);
        let data = Bytes::from_static(b"hello");
        let checksum = sha256_bytes(data.as_ref());
        let stored =
            cache_manager.put_fetched_with_result_and_checksum(chunk_id, data, 9, Some(checksum));
        assert_eq!(stored, CachePutResultTag::Stored);
        let prepared = prepare_incoming_fetch_response(
            &data_plane,
            &pending_fetched,
            &cache_manager,
            &hot_published_chunks,
            &HashSet::new(),
            ChunkFetchRequest {
                file_id: 1,
                version_epoch: 2,
                block_id: 3,
                off: 0,
                len: 5,
                expected_mtime: 9,
                prefetch_count: 0,
                tenant_id: String::new(),
            },
            7,
            false,
        );
        assert_eq!(prepared.response.data_port, 0);
        assert_eq!(prepared.response.ticket, EMPTY_DATA_PLANE_TICKET);
        assert_eq!(prepared.sent, 5);
        assert_eq!(prepared.response.chunks.len(), 1);
        assert!(data_plane.tickets.is_empty());
    }

    #[test]
    fn control_plane_response_redirects_with_pending_published_first_chunk() {
        let conf = test_conf("control-plane-pending-redirect");
        let data_plane = DataPlaneState::new(&conf);
        data_plane.port.store(31_002, Ordering::Relaxed);
        let cache_manager = CacheManager::new(&conf);
        let pending_fetched = FastDashMap::default();
        let hot_published_chunks = FastDashMap::default();
        let chunk_id = ChunkId::with_version(1, 2, 3, 0);
        pending_fetched.insert(
            chunk_id,
            PendingFetchedChunk::ready(Bytes::from_static(b"hello"), 9, 41),
        );
        let prepared = prepare_incoming_fetch_response(
            &data_plane,
            &pending_fetched,
            &cache_manager,
            &hot_published_chunks,
            &HashSet::new(),
            ChunkFetchRequest {
                file_id: 1,
                version_epoch: 2,
                block_id: 3,
                off: 0,
                len: 5,
                expected_mtime: 9,
                prefetch_count: 0,
                tenant_id: String::new(),
            },
            7,
            true,
        );
        assert_eq!(prepared.response.data_port, 31_002);
        assert_ne!(prepared.response.ticket, EMPTY_DATA_PLANE_TICKET);
        assert!(prepared.response.chunks.is_empty());
    }

    #[test]
    fn control_plane_response_returns_pending_published_chunk_before_persist() {
        let conf = test_conf("control-plane-pending-data");
        let data_plane = DataPlaneState::new(&conf);
        let cache_manager = CacheManager::new(&conf);
        let pending_fetched = FastDashMap::default();
        let hot_published_chunks = FastDashMap::default();
        let chunk_id = ChunkId::with_version(1, 2, 3, 0);
        pending_fetched.insert(
            chunk_id,
            PendingFetchedChunk::ready(Bytes::from_static(b"hello"), 9, 41),
        );
        let prepared = prepare_incoming_fetch_response(
            &data_plane,
            &pending_fetched,
            &cache_manager,
            &hot_published_chunks,
            &HashSet::new(),
            ChunkFetchRequest {
                file_id: 1,
                version_epoch: 2,
                block_id: 3,
                off: 0,
                len: 5,
                expected_mtime: 9,
                prefetch_count: 0,
                tenant_id: String::new(),
            },
            7,
            false,
        );
        assert_eq!(prepared.response.chunks.len(), 1);
        assert_eq!(
            prepared.response.chunks[0].data,
            Bytes::from_static(b"hello")
        );
        assert_eq!(prepared.response.chunks[0].mtime, 9);
    }
    #[test]
    fn cached_data_plane_ticket_is_not_reused_across_tenants_or_mtime() {
        let conf = test_conf("data-plane-ticket-scope");
        let mut loop_state = NetworkLoopState::new(&conf);
        let peer = new_peer_id();
        let chunk_id = ChunkId::with_version(1, 2, 3, 0);
        let ticket = [9u8; 16];
        let (response, _rx) = oneshot::channel();
        loop_state
            .peer_remote_addrs
            .insert(peer, "/ip4/127.0.0.1/tcp/4001".parse().unwrap());
        loop_state.peer_data_ports.insert(peer, 31_002);
        loop_state.peer_block_tickets.insert(
            peer_block_ticket_key(peer, chunk_id, "tenant-a", Some(11)),
            ticket,
        );
        let pending = PendingFetchRequest {
            fetch_token: 1,
            chunk_id,
            max_len: 128,
            expected_mtime: Some(22),
            trace: TraceLabels {
                trace_id: String::new(),
                tenant_id: "tenant-b".to_string(),
                job_id: String::new(),
                sampled: false,
            },
            response,
            candidates: VecDeque::new(),
            active_peer: None,
            defer_dht: false,
            started_at: Instant::now(),
            deadline: Instant::now() + Duration::from_secs(1),
        };

        assert!(
            build_data_plane_fetch_dispatch(&loop_state, peer, &pending, 1).is_none(),
            "ticket cache must not be reused across tenant or mtime changes"
        );
    }

    #[tokio::test]
    async fn pending_fetched_loading_waits_until_ready() {
        let service = P2pService::new(test_conf("pending-fetched-loading"));
        let chunk_id = ChunkId::with_version(1, 2, 3, 0);
        let (pending, placeholder) = new_pending_fetched_placeholder(chunk_id, 7);
        service.pending_fetched.insert(chunk_id, pending);
        let pending_fetched = service.pending_fetched.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(10)).await;
            fulfill_pending_fetched_placeholder(
                &pending_fetched,
                placeholder,
                Bytes::from_static(b"payload"),
                9,
            );
        });
        let bytes = service
            .pending_fetched_chunk(chunk_id, 7, Some(9))
            .await
            .expect("loading entry should resolve");
        assert_eq!(bytes, Bytes::from_static(b"payload"));
    }

    #[tokio::test]
    async fn stream_data_plane_response_emits_first_chunk_before_tail_finishes() {
        let service = P2pService::new(test_conf("data-plane-streaming"));
        let dispatch = DataPlaneFetchDispatch {
            request_id: 11,
            peer: new_peer_id(),
            endpoint: SocketAddr::from(([127, 0, 0, 1], 31002)),
            request: DataPlaneFetchRequest {
                ticket: EMPTY_DATA_PLANE_TICKET,
                file_id: 1,
                version_epoch: 2,
                block_id: 3,
                off: 0,
                len: 7,
                expected_mtime: 9,
                prefetch_count: 1,
            },
        };
        let (client, server) = tokio::io::duplex(4096);
        let (outcome_tx, mut outcome_rx) = mpsc::channel(1);
        let pending_fetched = service.pending_fetched.clone();
        let cache_manager = service.cache_manager.clone();
        let persist_tokens = AtomicU64::new(100);
        let response_limit = response_frame_limit_bytes(&service.conf);
        let stream_task = tokio::spawn(async move {
            let mut client = client.compat();
            let mut first_chunk_sent = false;
            stream_data_plane_response(
                &mut client,
                &dispatch,
                response_limit,
                pending_fetched,
                &persist_tokens,
                &outcome_tx,
                true,
                &[],
                cache_manager,
                &mut first_chunk_sent,
            )
            .await
        });
        let writer = tokio::spawn(async move {
            let mut server = server.compat();
            server
                .write_all(&encode_response_header(2, 0, EMPTY_DATA_PLANE_TICKET))
                .await
                .expect("header should write");
            let first = ChunkFetchResponseChunk {
                off: 0,
                mtime: 9,
                checksum: Bytes::from_static(b"first-checksum"),
                data: Bytes::from_static(b"chunk-0"),
            };
            let first_checksum_len = u16::try_from(first.checksum.len()).expect("fits u16");
            let first_data_len = u32::try_from(first.data.len()).expect("fits u32");
            let mut first_header = [0u8; RESPONSE_ITEM_FIXED_BYTES];
            first_header[0..8].copy_from_slice(&first.off.to_le_bytes());
            first_header[8..16].copy_from_slice(&first.mtime.to_le_bytes());
            first_header[16..18].copy_from_slice(&first_checksum_len.to_le_bytes());
            first_header[18..22].copy_from_slice(&first_data_len.to_le_bytes());
            server
                .write_all(&first_header)
                .await
                .expect("first header should write");
            server
                .write_all(first.checksum.as_ref())
                .await
                .expect("first checksum should write");
            server
                .write_all(first.data.as_ref())
                .await
                .expect("first data should write");
            sleep(Duration::from_millis(50)).await;
            let second = ChunkFetchResponseChunk {
                off: 7,
                mtime: 9,
                checksum: Bytes::from_static(b"second-checksum"),
                data: Bytes::from_static(b"chunk-1"),
            };
            let second_checksum_len = u16::try_from(second.checksum.len()).expect("fits u16");
            let second_data_len = u32::try_from(second.data.len()).expect("fits u32");
            let mut second_header = [0u8; RESPONSE_ITEM_FIXED_BYTES];
            second_header[0..8].copy_from_slice(&second.off.to_le_bytes());
            second_header[8..16].copy_from_slice(&second.mtime.to_le_bytes());
            second_header[16..18].copy_from_slice(&second_checksum_len.to_le_bytes());
            second_header[18..22].copy_from_slice(&second_data_len.to_le_bytes());
            server
                .write_all(&second_header)
                .await
                .expect("second header should write");
            server
                .write_all(second.checksum.as_ref())
                .await
                .expect("second checksum should write");
            server
                .write_all(second.data.as_ref())
                .await
                .expect("second data should write");
        });

        timeout(Duration::from_millis(20), outcome_rx.recv())
            .await
            .expect("first chunk should arrive before tail is fully streamed")
            .expect("worker should emit an outcome");
        writer.await.expect("writer should finish");
        let prefetched = stream_task
            .await
            .expect("stream task should join")
            .expect("streaming should succeed");
        assert_eq!(prefetched, 1);
        let chunk_id = ChunkId::with_version(1, 2, 3, 7);
        let bytes = service
            .pending_fetched_chunk(chunk_id, 7, Some(9))
            .await
            .expect("tail chunk should become visible");
        assert_eq!(bytes, Bytes::from_static(b"chunk-1"));
    }

    #[tokio::test]
    async fn expire_pending_query_timeout_falls_back_to_connected_peers() {
        let conf = test_conf("query-timeout-fallback");
        let mut swarm = build_swarm(&conf).expect("swarm should build");
        let local_peer_id = *swarm.local_peer_id();
        let connected_peer = new_peer_id();
        let init = NetworkInitState {
            local_peer_id,
            bootstrap_peer_ids: HashSet::new(),
            peer_whitelist: HashSet::new(),
            tenant_whitelist: Arc::new(HashSet::new()),
        };
        let mut loop_state = NetworkLoopState::new(&conf);
        loop_state.connected_peers.insert(connected_peer);
        let chunk_id = ChunkId::new(1, 0);
        let query_id = swarm
            .behaviour_mut()
            .kad
            .get_providers(provider_record_key(provider_record_id(chunk_id)));
        let (response_tx, mut response_rx) = oneshot::channel();
        loop_state.pending_queries.insert(
            query_id,
            PendingProviderQuery {
                fetch_token: 1,
                chunk_id,
                max_len: 1024,
                expected_mtime: None,
                trace: TraceLabels {
                    trace_id: "trace".to_string(),
                    tenant_id: "tenant".to_string(),
                    job_id: "job".to_string(),
                    sampled: false,
                },
                response: response_tx,
                deadline: Instant::now() - Duration::from_millis(1),
            },
        );

        expire_pending_queries(&mut swarm, &conf, &init, &mut loop_state);

        assert!(loop_state.pending_queries.is_empty());
        assert_eq!(loop_state.pending_requests.len(), 1);
        assert!(matches!(response_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn stale_local_policy_version_does_not_block_lower_master_version() {
        let mut conf = test_conf("stale-policy-version");
        conf.enable = true;
        conf.enable_dht = false;
        conf.policy_hmac_key = "policy-secret".to_string();
        let version_path = runtime_policy_version_path(&conf);
        fs::create_dir_all(version_path.parent().unwrap()).expect("policy dir should exist");
        fs::write(&version_path, "10").expect("stale version should be persisted");

        let service = P2pService::new_with_runtime(conf.clone(), Some(test_runtime()));
        service.start();

        let peer_whitelist = vec![new_peer_id().to_string()];
        let tenant_whitelist = vec!["tenant-a".to_string()];
        let signature = CommonUtils::sign_p2p_policy(
            &conf.policy_hmac_key,
            5,
            &peer_whitelist,
            &tenant_whitelist,
        );
        let updated = service
            .sync_runtime_policy_from_master(5, peer_whitelist, tenant_whitelist, signature)
            .await;

        assert!(updated);
        assert_eq!(service.runtime_policy_version(), 5);
        assert!(service.runtime_policy_applied.load(Ordering::Relaxed));
        let persisted = fs::read_to_string(version_path).expect("policy version should exist");
        assert_eq!(persisted.trim(), "5");
        service.stop();
    }

    #[tokio::test]
    async fn runtime_policy_version_zero_rolls_back_to_local_policy() {
        let mut conf = test_conf("runtime-policy-rollback-zero");
        conf.enable = true;
        conf.enable_dht = false;
        conf.policy_hmac_key = "policy-secret".to_string();
        conf.tenant_whitelist = vec!["tenant-local".to_string()];
        let version_path = runtime_policy_version_path(&conf);

        let service = P2pService::new_with_runtime(conf.clone(), Some(test_runtime()));
        service.start();

        let peer_whitelist = vec![new_peer_id().to_string()];
        let tenant_whitelist = vec!["tenant-remote".to_string()];
        let signature = CommonUtils::sign_p2p_policy(
            &conf.policy_hmac_key,
            2,
            &peer_whitelist,
            &tenant_whitelist,
        );
        assert!(
            service
                .sync_runtime_policy_from_master(2, peer_whitelist, tenant_whitelist, signature)
                .await
        );
        assert_eq!(service.runtime_policy_version(), 2);
        assert!(is_tenant_allowed(
            "tenant-remote",
            service.data_plane.tenant_whitelist().as_ref()
        ));

        assert!(
            service
                .sync_runtime_policy_from_master(0, vec![], vec![], String::new())
                .await
        );
        assert_eq!(service.runtime_policy_version(), 0);
        assert!(is_tenant_allowed(
            "tenant-local",
            service.data_plane.tenant_whitelist().as_ref()
        ));
        assert!(!is_tenant_allowed(
            "tenant-remote",
            service.data_plane.tenant_whitelist().as_ref()
        ));
        let persisted = fs::read_to_string(version_path).expect("policy version should exist");
        assert_eq!(persisted.trim(), "0");
        service.stop();
    }

    #[test]
    fn clear_data_plane_listener_port_keeps_task_started() {
        let conf = test_conf("data-plane-reset");
        let data_plane = DataPlaneState::new(&conf);
        let generation = data_plane.next_generation();
        data_plane.started.store(true, Ordering::Relaxed);
        data_plane.port.store(31_002, Ordering::Relaxed);

        clear_data_plane_listener_port(&data_plane, generation);

        assert!(data_plane.started.load(Ordering::Relaxed));
        assert_eq!(data_plane.port(), 0);
    }
    #[test]
    fn stale_generation_cannot_reset_new_listener_state() {
        let conf = test_conf("data-plane-generation");
        let data_plane = DataPlaneState::new(&conf);
        let stale_generation = data_plane.next_generation();
        let current_generation = data_plane.next_generation();
        data_plane.started.store(true, Ordering::Relaxed);
        set_data_plane_listener_port(&data_plane, current_generation, 31_002);

        reset_data_plane_listener_state(&data_plane, stale_generation);

        assert!(data_plane.started.load(Ordering::Relaxed));
        assert_eq!(data_plane.port(), 31_002);
    }

    #[tokio::test]
    async fn data_plane_listener_stops_and_restarts_cleanly() {
        let mut conf = test_conf("data-plane-lifecycle");
        conf.enable = true;
        conf.enable_mdns = false;
        conf.enable_dht = false;

        let service = P2pService::new_with_runtime(conf, Some(test_runtime()));
        service.start();

        let first_port = wait_for_data_plane_port(&service, Duration::from_secs(2))
            .await
            .expect("data plane should start");
        assert!(first_port > 0);

        service.stop();
        wait_for_data_plane_stopped(&service, Duration::from_secs(2))
            .await
            .expect("data plane should stop");

        service.start();
        let restarted_port = wait_for_data_plane_port(&service, Duration::from_secs(2))
            .await
            .expect("data plane should restart");
        assert!(restarted_port > 0);

        service.stop();
        wait_for_data_plane_stopped(&service, Duration::from_secs(2))
            .await
            .expect("data plane should stop after restart");
    }
    #[tokio::test]
    async fn invalid_runtime_peer_whitelist_update_is_rejected() {
        let mut conf = test_conf("invalid-runtime-peer-whitelist");
        conf.enable = true;
        conf.enable_dht = false;
        conf.policy_hmac_key = "policy-secret".to_string();

        let service = P2pService::new_with_runtime(conf.clone(), Some(test_runtime()));
        service.start();

        let peer_whitelist = vec!["invalid-peer-id".to_string()];
        let tenant_whitelist = vec!["tenant-a".to_string()];
        let signature = CommonUtils::sign_p2p_policy(
            &conf.policy_hmac_key,
            1,
            &peer_whitelist,
            &tenant_whitelist,
        );
        let updated = service
            .sync_runtime_policy_from_master(1, peer_whitelist, tenant_whitelist, signature)
            .await;

        assert!(!updated);
        assert_eq!(service.runtime_policy_version(), 0);
        service.stop();
    }
}
