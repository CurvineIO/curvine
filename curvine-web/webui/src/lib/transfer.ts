import type { MountInfo } from '@/lib/types'
import type { TransferJob, TransferKind, TransferProgress, TransferState } from '@/lib/transfer-types'

/** Job states that no longer change without a new run. */
export const TERMINAL_STATES: TransferState[] = ['completed', 'failed', 'canceled', 'partial_success']

export const TRANSFER_STATES: TransferState[] = [
  'pending',
  'planning',
  'dispatching',
  'running',
  'canceling',
  'completed',
  'failed',
  'canceled',
  'partial_success',
]

export type TransferTone = 'active' | 'success' | 'warning' | 'danger' | 'muted'

export function isTerminalState(state: string): boolean {
  return (TERMINAL_STATES as string[]).includes(state)
}

export function canCancel(state: string): boolean {
  return !isTerminalState(state) && state !== 'canceling'
}

export function canRetry(state: string): boolean {
  return isTerminalState(state)
}

export function transferStateTone(state: string): TransferTone {
  if (state === 'completed') return 'success'
  if (state === 'partial_success') return 'warning'
  if (state === 'failed') return 'danger'
  if (state === 'canceled' || state === 'canceling' || state === 'stale') return 'muted'
  return 'active'
}

export function displayState(state: string): string {
  return (state || 'unknown').replace(/_/g, ' ')
}

export function progressPercent(progress: TransferProgress | null | undefined): number {
  if (!progress) {
    return 0
  }
  const total = Number(progress.total_size) || 0
  const loaded = Number(progress.loaded_size) || 0
  if (total <= 0) {
    return 0
  }
  return Math.min(100, Math.max(0, Math.floor((loaded * 100) / total)))
}

export function shortId(jobId: string | null | undefined): string {
  return jobId ? jobId.slice(0, 8) : '-'
}

/** Job duration in ms: final duration once terminal, elapsed since creation otherwise. */
export function jobDurationMs(job: TransferJob): number {
  const end = isTerminalState(job.state) ? job.updated_at : Date.now()
  return Math.max(0, end - job.created_at)
}

/**
 * Client-side preview of the server's `target_path_from_mount`: load maps a
 * UFS source onto its Curvine mount path, export maps a Curvine source back
 * to UFS. The transfer service derives the target authoritatively.
 */
export function deriveTransferTarget(
  kind: TransferKind,
  source: string,
  mounts: MountInfo[],
): string | null {
  const src = source.trim()
  if (!src) {
    return null
  }
  for (const mount of mounts) {
    const from = (kind === 'load' ? mount.ufs_path : mount.cv_path).replace(/\/+$/, '')
    const to = (kind === 'load' ? mount.cv_path : mount.ufs_path).replace(/\/+$/, '')
    const matches = from === '' ? src.startsWith('/') : src === from || src.startsWith(`${from}/`)
    if (!matches) {
      continue
    }
    const suffix = (from === '' ? src : src.slice(from.length)).replace(/^\/+/, '')
    return suffix ? `${to}/${suffix}` : to || '/'
  }
  return null
}
