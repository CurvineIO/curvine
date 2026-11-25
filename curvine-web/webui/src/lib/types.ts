/**
 * Response types for the master REST API. Field names mirror the Rust
 * serialization in `curvine-master/src/master/router_handler.rs`.
 */

export interface Overview {
  cluster_id: string
  master_addr: string
  start_time: string
  start_time_ms: number
  live_workers: number
  lost_workers: number
  available: number
  capacity: number
  fs_used: number
  reserved_bytes: number
  files_total: number
  dir_total: number
  master_state: string
  /** Added by newer masters; absent on older builds. */
  version?: string
  git_commit?: string
  git_branch?: string
}

export interface WorkerAddress {
  worker_id: number
  hostname: string
  ip_addr: string
  rpc_port: number
  web_port: number
}

export interface WorkerInfo {
  address: WorkerAddress
  weight: number
  software_version: string
  startup_time_ms: number
  capacity: number
  available: number
  fs_used: number
  non_fs_used: number
  reserved_bytes: number
  last_update: number
  block_num: number
  status: string
  worker_session_id: string
}

export interface WorkersResponse {
  live_workers: WorkerInfo[]
  lost_workers: WorkerInfo[]
}

export interface StoragePolicy {
  storage_type: string
  ttl_ms: number
  ttl_action: string
  ufs_mtime: number
  state: string
}

export interface FileStatus {
  id: number
  path: string
  name: string
  is_dir: boolean
  mtime: number
  atime: number
  children_num: number
  is_complete: boolean
  len: number
  replicas: number
  block_size: number
  file_type: string
  x_attr: Record<string, number[]>
  storage_policy: StoragePolicy
  mode: number
  owner: string
  group: string
  nlink: number
  target: string | null
}

export interface ExtendedBlock {
  id: number
  len: number
  storage_type: string
  file_type: string
  alloc_opts?: unknown
}

export interface LocatedBlock {
  block: ExtendedBlock
  locs: WorkerAddress[]
  has_spdk: boolean
}

export interface FileBlocks {
  status: FileStatus
  block_locs: LocatedBlock[]
}

/** Nested `ClusterConf` tree; keys are only known at run time. */
export type ConfigMap = Record<string, unknown>
