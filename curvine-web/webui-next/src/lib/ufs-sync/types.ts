/** A tracked UFS sync job (per-path, or a folder-upload aggregate keyed by path). */
export interface UfsSyncJob {
  path: string
  batch_id?: string
  batch_label?: string
  batch_aggregate?: boolean
  state?: string
  status?: string
  progress?: number
  total_files?: number
  completed_files?: number
  loaded_size?: number
  total_size?: number
  done?: boolean
  attempts?: number
  message?: string
  /** Local write time; refreshed on every tracker update. */
  updated_at?: number
  /** Server-side update time; preferred over the local `updated_at`. */
  update_time_ms?: number
  mount_path?: string
  source_path?: string
  target_path?: string
  ufs_path?: string
  job_id?: string
  id?: string
  display_name?: string
  trigger_event?: string
  /**
   * Backend-resolved "related job" for the deep link: transfer jobs executed by
   * the transfer service, load jobs otherwise. Falls back to the id shape.
   */
  related_job_kind?: string
  related_job_id?: string
  /** Raw transfer job id from the job status payload, when recorded. */
  transfer_job_id?: string
  [key: string]: unknown
}

export interface TrackedUploadBatch {
  id: string
  label: string
  state: string
  totalFiles: number
  uploadedFiles: number
  uploadedSize: number
  totalSize: number
  progress: number
  done: boolean
  message: string
  updated_at: number
}

/** Aggregate of every tracked job sharing a folder-upload batch id. */
export interface UfsSyncBatch {
  id: string
  label: string
  subtitle?: string
  title?: string
  totalFiles: number
  syncedFiles: number
  terminalFiles: number
  uploadFiles: number
  waitingFiles: number
  syncingFiles: number
  failedFiles: number
  syncedSize: number
  totalSize: number
  progressSize: number
  progress: number
  state: string
  message: string
}
