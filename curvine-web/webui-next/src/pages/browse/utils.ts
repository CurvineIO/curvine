import type { UfsSyncJob } from './types'

export type SyncStatusClass = 'completed' | 'failed' | 'warning' | 'running' | 'waiting'

const TERMINAL_SYNC_STATES = [
  'Completed',
  'Failed',
  'Canceled',
  'Unsupported',
  'NotMounted',
  'Unknown',
  'Timed out',
  'Upload failed'
]

export function normalizePath(path: unknown): string {
  const value = String(path || '/').trim()
  if (!value || value === '/') return '/'
  return `/${value.replace(/^\/+|\/+$/g, '')}`.replace(/\/+/g, '/')
}

export function parentPath(path: string): string {
  const parts = normalizePath(path).split('/').filter(Boolean)
  parts.pop()
  return parts.length ? `/${parts.join('/')}` : '/'
}

export function joinPath(parent: string, name: string): string {
  return `${String(parent || '/').replace(/\/$/, '')}/${name}`.replace(/^\/\//, '/')
}

export function fileName(path: string): string {
  const parts = String(path || '').split('/').filter(Boolean)
  return parts.pop() || '/'
}

export function syncState(job: UfsSyncJob | null | undefined): string {
  if (!job) return 'Unknown'
  return String(job.state || job.status || 'Unknown')
}

export function isTerminalSyncState(state: unknown): boolean {
  return TERMINAL_SYNC_STATES.includes(String(state || ''))
}

export function syncProgress(job: UfsSyncJob): number {
  const state = syncState(job)
  if (state === 'Completed') return 100
  if (['Queued', 'NotStarted', 'Waiting for UFS sync', 'Checking UFS sync'].includes(state)) return 0
  const value = Number(job.progress || 0)
  if (value > 0 || ['Pending', 'Loading'].includes(state)) {
    return Math.max(0, Math.min(100, Math.round(value)))
  }
  const total = Number(job.total_size || 0)
  if (total <= 0) return 0
  return Math.max(0, Math.min(100, Math.round((Number(job.loaded_size || 0) / total) * 100)))
}

export function syncStatusClass(state: string): SyncStatusClass {
  if (state === 'Completed') return 'completed'
  if (
    ['Failed', 'Canceled', 'Unknown', 'Timed out', 'Needs attention', 'Upload failed', 'Interrupted'].includes(
      state
    )
  ) {
    return 'failed'
  }
  if (['Unsupported', 'NotMounted', 'Finished with skipped files'].includes(state)) return 'warning'
  if (['Uploading to Curvine', 'Pending', 'Loading', 'Syncing to UFS'].includes(state)) return 'running'
  return 'waiting'
}

export function syncDisplayPath(job: UfsSyncJob, browsePath: string): string {
  const path = normalizePath(job.path || job.source_path || '')
  const current = normalizePath(browsePath)
  if (!path) return '-'
  if (path === current) return path
  if (current !== '/' && path.startsWith(`${current}/`)) return path.slice(current.length + 1)
  return path
}

export function writeTypeLabel(value: unknown): string {
  if (value === 1 || value === 'FsMode') return 'fs_mode'
  if (value === 0 || value === 'CacheMode') return 'cache_mode'
  return String(value || 'fs_mode').toLowerCase()
}
