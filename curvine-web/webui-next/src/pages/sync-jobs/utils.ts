import type { JobTask } from '@/api/types'
import type { UfsSyncJob } from '@/lib/ufs-sync/types'
import { isTerminalSyncState, syncState, syncStatusClass } from '@/pages/browse/utils'

export type SyncJobBucket = 'active' | 'completed' | 'failed' | 'attention'

export const SYNC_JOB_BUCKETS: SyncJobBucket[] = ['active', 'completed', 'failed', 'attention']

/** UUID-shaped ids belong to the transfer service; other ids are load jobs. */
export const TRANSFER_JOB_ID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

/** Chip buckets reuse the panel's status classes: running/waiting → active. */
export function syncJobBucket(job: UfsSyncJob): SyncJobBucket {
  switch (syncStatusClass(syncState(job))) {
    case 'completed':
      return 'completed'
    case 'failed':
      return 'failed'
    case 'warning':
      return 'attention'
    default:
      return 'active'
  }
}

export function isTerminalSyncJob(job: UfsSyncJob | null | undefined): boolean {
  // A missing job is not a finished one; only real records can be terminal.
  if (!job) return false
  return Boolean(job.done) || isTerminalSyncState(syncState(job))
}

export interface RelatedJobRef {
  kind: 'load' | 'transfer'
  jobId: string
}

/**
 * Where the row's job can be inspected. Backend-resolved related_job_* fields
 * (job_status_payload) win; otherwise fall back to the id shape so older
 * backends and cached rows still resolve.
 */
export function relatedJobRef(job: UfsSyncJob): RelatedJobRef | null {
  const kind = String(job.related_job_kind || '')
  const id = String(job.related_job_id || job.job_id || job.id || '').trim()
  if ((kind === 'load' || kind === 'transfer') && id) return { kind, jobId: id }
  if (!id) return null
  return TRANSFER_JOB_ID_RE.test(id) ? { kind: 'transfer', jobId: id } : { kind: 'load', jobId: id }
}

export function relatedJobHref(ref: RelatedJobRef): string {
  const target = ref.kind === 'transfer' ? 'transfers' : 'jobs'
  return `/${target}?job=${encodeURIComponent(ref.jobId)}`
}

/** Lowercased haystack over every field the search box matches. */
export function syncJobSearchText(job: UfsSyncJob): string {
  return [
    job.path,
    job.source_path,
    job.target_path,
    job.ufs_path,
    job.job_id,
    job.id,
    job.display_name,
    job.message,
    job.batch_label
  ]
    .map((value) => String(value || ''))
    .join(' ')
    .toLowerCase()
}

/** Newest first; rows without a timestamp sink to the bottom. */
export function compareSyncJobsByUpdated(a: UfsSyncJob, b: UfsSyncJob): number {
  return (
    Number(b.update_time_ms || b.updated_at || 0) - Number(a.update_time_ms || a.updated_at || 0)
  )
}

export function syncJobUpdatedMs(job: UfsSyncJob): number {
  return Number(job.update_time_ms || job.updated_at || 0)
}

/**
 * Byte-based progress for one task row; null when the task carries no size
 * information yet. Finished tasks count as 100% so the column does not read
 * like stalled work.
 */
export function taskProgressPercent(task: JobTask): number | null {
  const total = Number(task.total_size || 0)
  const loaded = Number(task.loaded_size || 0)
  if (total > 0) return Math.max(0, Math.min(100, Math.round((loaded / total) * 100)))
  return String(task.state || task.status || '') === 'Completed' ? 100 : null
}
