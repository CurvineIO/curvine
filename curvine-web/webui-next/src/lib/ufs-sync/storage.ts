import type { UfsSyncJob } from './types'
import { isTerminalSyncState, normalizePath, syncState } from '@/pages/browse/utils'

export const UFS_SYNC_KEEP_MS = 30 * 60 * 1000
export const UFS_SYNC_RESTORE_KEEP_MS = 24 * 60 * 60 * 1000
export const MAX_POLL_ATTEMPTS = 240

export const UFS_SYNC_STORAGE_KEY = 'curvine.web.fs.ufsSyncJobs.v3'
export const UFS_SYNC_DISMISSED_STORAGE_KEY = 'curvine.web.fs.ufsSyncDismissed.v1'

export function readJson<T>(key: string, fallback: T): T {
  try {
    const raw = window.localStorage.getItem(key)
    return raw ? ((JSON.parse(raw) as T) ?? fallback) : fallback
  } catch {
    return fallback
  }
}

export function writeJson(key: string, value: unknown) {
  try {
    window.localStorage.setItem(key, JSON.stringify(value))
  } catch {
    // Storage may be full or unavailable; tracking is best-effort.
  }
}

export function readTrackedUfsSyncJobs(): Record<string, UfsSyncJob> {
  return readJson<Record<string, UfsSyncJob>>(UFS_SYNC_STORAGE_KEY, {})
}

export function writeTrackedUfsSyncJobs(jobs: Record<string, UfsSyncJob>) {
  writeJson(UFS_SYNC_STORAGE_KEY, jobs)
}

export function removeStoredUfsSyncJob(path: string) {
  try {
    const stored = readTrackedUfsSyncJobs()
    delete stored[path]
    window.localStorage.setItem(UFS_SYNC_STORAGE_KEY, JSON.stringify(stored))
  } catch {
    // Best-effort persistence.
  }
}

export function readUfsSyncDismissed(): Record<string, number> {
  return readJson<Record<string, number>>(UFS_SYNC_DISMISSED_STORAGE_KEY, {})
}

export function writeUfsSyncDismissed(record: Record<string, number>) {
  writeJson(UFS_SYNC_DISMISSED_STORAGE_KEY, record)
}

/**
 * A dismissed stamp hides a finished row until the server reports a strictly
 * newer update, so "clear completed" survives reloads and re-imports.
 */
export function isUfsSyncDismissed(
  dismissed: Record<string, number>,
  path: string,
  job?: UfsSyncJob | null
): boolean {
  const normalized = normalizePath(path)
  const dismissedAt = Number(dismissed[normalized] || 0)
  if (!dismissedAt) return false
  if (!job) return true
  const state = job.state || job.status || ''
  const done = Boolean(job.done) || isTerminalSyncState(state)
  if (!done) return false
  const serverUpdated = Number(job.update_time_ms || job.updated_at || 0)
  return !serverUpdated || serverUpdated <= dismissedAt
}

/**
 * What a tracker flush persists: the live rows, minus anything terminal,
 * stale beyond the keep window or dismissed, merged over what is stored.
 */
export function pruneTrackedUfsSyncJobs(
  current: Record<string, UfsSyncJob>,
  dismissed: Record<string, number>
): Record<string, UfsSyncJob> {
  const now = Date.now()
  const jobs: Record<string, UfsSyncJob> = { ...readTrackedUfsSyncJobs() }
  for (const [path, job] of Object.entries(current)) {
    const state = syncState(job)
    if (job.done || isTerminalSyncState(state)) {
      delete jobs[path]
      continue
    }
    const currentUpdatedAt = Number(job.updated_at || 0)
    const storedUpdatedAt = Number(jobs[path]?.updated_at || 0)
    if (now - Number(job.updated_at || now) <= UFS_SYNC_KEEP_MS && currentUpdatedAt >= storedUpdatedAt) {
      jobs[path] = job
    }
  }
  for (const [path, job] of Object.entries(jobs)) {
    const state = syncState(job)
    if (
      !job ||
      job.done ||
      isTerminalSyncState(state) ||
      now - Number(job.updated_at || now) > UFS_SYNC_KEEP_MS ||
      isUfsSyncDismissed(dismissed, path, job)
    ) {
      delete jobs[path]
    }
  }
  return jobs
}

export function flushTrackedUfsSyncJobs(
  current: Record<string, UfsSyncJob>,
  dismissed: Record<string, number>
) {
  writeJson(UFS_SYNC_STORAGE_KEY, pruneTrackedUfsSyncJobs(current, dismissed))
}

/** Persisted rows worth resuming on mount: recent, not dismissed, still running. */
export function readRestorableUfsSyncJobs(): Record<string, UfsSyncJob> {
  const now = Date.now()
  const dismissed = readUfsSyncDismissed()
  const jobs: Record<string, UfsSyncJob> = {}
  for (const [path, job] of Object.entries(readTrackedUfsSyncJobs())) {
    if (!job || now - Number(job.updated_at || now) > UFS_SYNC_RESTORE_KEEP_MS) continue
    if (isUfsSyncDismissed(dismissed, path, job)) continue
    const state = syncState(job)
    if (job.done || isTerminalSyncState(state)) continue
    jobs[path] = job
  }
  return jobs
}
