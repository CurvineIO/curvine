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
  /** Whether the Web write APIs are enabled (`master.web_write_enabled`). */
  web_write_enabled?: boolean
  /** Standby masters only: web address (`host:port`) of the active master. */
  active_master_web_addr?: string | null
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

/**
 * Mount enums mirror the Rust serde output: PascalCase variant names (the
 * enums have no `rename_all`). `ResyncState` is the exception (lowercase).
 */
export type WriteType = 'CacheMode' | 'FsMode'
export type AccessMode = 'ReadOnly' | 'ReadWrite'
export type TtlAction = 'None' | 'Delete' | 'Free'
export type StorageType = 'Mem' | 'Ssd' | 'Hdd' | 'Ufs' | 'Disk' | 'SpdkDisk'
export type Provider = 'Auto' | 'OssHdfs' | 'Opendal' | 'Cosn'

/** A mount point as returned by `/api/mounts` and `/api/mount_info`. Secret
 * property values are already masked (`******`) by the master. */
export interface MountInfo {
  cv_path: string
  ufs_path: string
  mount_id: number
  properties: Record<string, string>
  ttl_ms: number
  ttl_action: TtlAction
  read_verify_ufs: boolean
  storage_type: StorageType | null
  block_size: number | null
  replicas: number | null
  write_type: WriteType
  provider: Provider | null
  auto_cache: boolean
  access_mode: AccessMode
  write_cache: boolean
}

/** Body of `POST /api/free`. */
export interface FreeRequest {
  path: string
  recursive?: boolean
}

export interface FreeResponse {
  inodes: number
  bytes: number
}

/** Body of `POST /api/mount`. Absent optional fields keep the CLI defaults for
 * a new mount, or the stored values when `update` is set. */
export interface MountRequest {
  cv_path: string
  ufs_path: string
  update?: boolean
  properties?: Record<string, string>
  check_path_consist?: boolean
  ttl_ms?: number
  ttl_action?: TtlAction
  read_verify_ufs?: boolean
  storage_type?: StorageType
  block_size?: number
  replicas?: number
  write_type?: WriteType
  provider?: Provider
  auto_cache?: boolean
  access_mode?: AccessMode
  write_cache?: boolean
}

export interface MountResponse {
  mount: MountInfo | null
}

/** Body of `POST /api/umount`; a non-empty `cv_path` wins over `mount_id`. */
export interface UmountRequest {
  cv_path?: string
  mount_id?: number
}

export interface UmountResponse {
  unmounted: boolean
}

/** Body of `POST /api/resync`. */
export interface ResyncRequest {
  cv_path: string
  dry_run?: boolean
}

export interface ResyncSubmitResponse {
  started: boolean
  cv_path: string
}

export interface ResyncStats {
  scanned: number
  skip_same_mtime: number
  skip_ufs_time_zero: number
  recreated: number
  failed: number
}

export type ResyncState = 'running' | 'done' | 'failed'

/** One resync task as returned by `GET /api/resync`. */
export interface ResyncTask {
  cv_path: string
  trigger: 'manual' | 'auto'
  state: ResyncState
  stats: ResyncStats
  started_at_ms: number
  finished_at_ms: number | null
  error: string | null
}
