import type { BrowseEntry } from '@/lib/browseColumns'

/**
 * A listing row as `GET /v1/fs/list` returns it: the shared attribute fields
 * plus the identity/size fields the browser needs.
 */
export interface BrowseRow extends BrowseEntry {
  path?: string | null
  name?: string | null
  len?: number | null
}

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
  /** Local write time; always overwritten by `setUfsSyncJob`. */
  updated_at?: number
  mount_path?: string
  source_path?: string
  target_path?: string
  ufs_path?: string
  job_id?: string
  id?: string
  display_name?: string
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

export interface BrowseSnapshot {
  items: unknown[]
  saved_at: number
}
