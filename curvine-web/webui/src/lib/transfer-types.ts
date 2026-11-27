/**
 * Response types for the transfer REST API. Field names mirror the DTOs in
 * `crates/server/curvine-data-transfer/src/transfer/rest.rs`.
 */

export type TransferKind = 'load' | 'export'

export type TransferState =
  | 'pending'
  | 'planning'
  | 'dispatching'
  | 'running'
  | 'canceling'
  | 'completed'
  | 'failed'
  | 'canceled'
  | 'partial_success'
  | 'unknown'

export interface TransferProgress {
  loaded_size: number
  total_size: number
  update_time: number
  message: string
}

export interface TransferJob {
  job_id: string
  run_id: number
  kind: TransferKind
  state: TransferState
  source_path: string
  target_path: string
  progress: TransferProgress
  submitter: string
  tenant: string
  created_at: number
  updated_at: number
  owner: string | null
  lease_epoch: number | null
  lease_expire_at: number | null
  cv_metadata_epoch: number | null
}

export interface TransferTask {
  task_id: string
  attempt_id: number
  source_path: string
  target_path: string
  worker_id: number
  worker_session_id: string
  state: string
  progress: TransferProgress
  retry_count: number
  updated_at: number
}

export interface TransferTaskSummary {
  pending: number
  running: number
  completed: number
  failed: number
  canceled: number
  stale: number
  completed_size: number
}

export interface TransferJobsResponse {
  jobs: TransferJob[]
  next_page_token: string | null
}

export interface TransferJobDetailResponse {
  job: TransferJob
  task_summary: TransferTaskSummary
  tasks: TransferTask[]
  next_page_token: string | null
}

export interface TransferTasksResponse {
  tasks: TransferTask[]
  next_page_token: string | null
}

export interface TransferTenant {
  tenant: string
  pending: number
  executing: number
  completed: number
  failed: number
  canceled: number
  partial_success: number
  total: number
}

export interface TransferTenantsResponse {
  tenants: TransferTenant[]
  next_page_token: string | null
}

export interface TransferJobState {
  job_id: string
  run_id: number
  state: TransferState
}

/** Body of `POST /api/transfer/jobs`. The target path is derived server-side
 * from the mount table: load maps UFS -> Curvine, export maps Curvine -> UFS. */
export interface TransferSubmitRequest {
  /** Defaults to `load` when omitted. */
  kind?: TransferKind
  source_path: string
  /** Explicit file list, relative to the source; load jobs only. */
  files?: string[]
  /** Defaults to true when omitted. */
  overwrite?: boolean
  /** Defaults to `http-api` when omitted. */
  submitter?: string
  tenant?: string
  /** Re-run an existing terminal job instead of returning it. */
  force?: boolean
}

export interface TransferSubmitResponse {
  job_id: string
  run_id: number
  state: TransferState
  files: number
  /** False when dedup returned an existing job instead of creating one. */
  created: boolean
}

export interface TransferJobsQuery {
  kind?: TransferKind | ''
  state?: TransferState | ''
  submitter?: string
  tenant?: string
  path?: string
  limit?: number
  page_token?: string | null
}
