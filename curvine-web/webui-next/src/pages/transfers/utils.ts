import type { JobTask, TransferInfo } from '@/api/types'
import { formatBytes } from '@/lib/format'
import { buildJobDisplayName, jobListSubtitle, jobListTitle, jobListTooltip, shortJobId } from '@/lib/jobDisplay'
import { isWithinRange, relativeTime, retryCount } from '@/pages/jobs/utils'
import { translateStatus } from '@/lib/i18nLabels'
import type { Translate } from '@/i18n'

/**
 * Task pages are requested from offset 0 with a growing page size instead of
 * following page tokens, so each poll refresh replaces the task list as a whole
 * and never has to merge paged state. Jobs beyond TASK_PAGE_MAX are inspected
 * with `curvine-cli transfer status --all`.
 */
export const TASK_PAGE_DEFAULT = 100
export const TASK_PAGE_STEP = 100
export const TASK_PAGE_MAX = 1000

const ACTIVE_STATES = ['pending', 'planning', 'dispatching', 'running', 'canceling']

export function transferId(transfer: TransferInfo | null | undefined): string {
  return String(transfer?.job_id || transfer?.id || '')
}

export function transferStateValue(transfer: TransferInfo | JobTask | null | undefined): string {
  const raw = transfer?.state || transfer?.status
  return raw ? String(raw).toLowerCase() : ''
}

export function transferStateLabel(
  t: Translate,
  transfer: TransferInfo | JobTask | null | undefined
): string {
  return translateStatus(t, String(transfer?.state || transfer?.status || 'Unknown'))
}

export function isTransferFinal(transfer: TransferInfo | null | undefined): boolean {
  if (transfer?.done === true) return true
  return ['completed', 'failed', 'canceled', 'partialsuccess'].includes(transferStateValue(transfer))
}

/** Align with transfer service retry_transfer: terminal jobs only. */
export function canRetry(transfer: TransferInfo | null | undefined): boolean {
  return isTransferFinal(transfer)
}

export function transferBucket(transfer: TransferInfo): string {
  const value = transferStateValue(transfer)
  if (ACTIVE_STATES.includes(value)) return 'active'
  if (value === 'completed') return 'completed'
  if (value === 'failed' || value === 'partialsuccess') return 'failed'
  if (value === 'canceled') return 'canceled'
  return 'active'
}

export function transferClass(transfer: TransferInfo | JobTask): string {
  const value = transferStateValue(transfer)
  if (value === 'partialsuccess' || value.includes('fail') || value.includes('cancel')) return 'failed'
  if (
    value.includes('pend') ||
    value.includes('wait') ||
    value.includes('plan') ||
    value.includes('dispatch')
  ) {
    return 'waiting'
  }
  if (value.includes('run')) return 'running'
  if (value.includes('complete')) return 'healthy'
  return 'warning'
}

export function kindValue(transfer: TransferInfo | null | undefined): string {
  return String(transfer?.kind || 'load').toLowerCase()
}

export function kindLabel(t: Translate, transfer: TransferInfo | null | undefined): string {
  return kindValue(transfer) === 'export' ? t('transfers.kinds.export') : t('transfers.kinds.load')
}

export function transferProgress(transfer: TransferInfo): number {
  return Math.max(0, Math.min(100, Math.round(Number(transfer.progress || 0))))
}

export function formatFiles(transfer: TransferInfo): string {
  const total = Number(transfer.total_files || 0)
  if (!total) return '-'
  return `${Number(transfer.completed_files || 0)} / ${total}`
}

/** Compact human duration: 45s, 12m 3s, 3h 20m, 2d 4h. */
export function formatDuration(ms: number): string {
  const seconds = Math.max(0, Math.floor(ms / 1000))
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ${minutes % 60}m`
  return `${Math.floor(hours / 24)}d ${hours % 24}h`
}

/**
 * How long the transfer took: finished jobs measure create → last update; active
 * ones measure create → now, so the cell keeps moving as the list polls. `-` when
 * the create time was never reported.
 */
export function transferDuration(transfer: TransferInfo | null | undefined): string {
  const created = Number(transfer?.create_time_ms || 0)
  if (!created) return '-'
  const end = isTransferFinal(transfer) ? Number(transfer?.update_time_ms || 0) : Date.now()
  if (!end || end < created) return '-'
  return formatDuration(end - created)
}

export function sizeLabel(transfer: TransferInfo): string {
  const loaded = Number(transfer.loaded_size || 0)
  const total = Number(transfer.total_size || 0)
  if (!loaded && !total) return '-'
  return `${formatBytes(loaded)} / ${formatBytes(total)}`
}

export function transferredLabel(transfer: TransferInfo): string {
  const value = transfer.completed_size
  if (value === null || value === undefined) return '-'
  return formatBytes(Number(value) || 0)
}

export function taskCountLabel(value: unknown): string | number {
  if (value === null || value === undefined) return '-'
  return Number(value) || 0
}

export function displayTaskTotal(transfer: TransferInfo | null | undefined): string | number {
  if (!transfer) return '-'
  const tasks = transferTasks(transfer).length
  if (transfer.total_files === null || transfer.total_files === undefined) return tasks || '-'
  const total = Number(transfer.total_files || 0)
  return total > 0 ? total : tasks || '-'
}

export function transferTasks(transfer: TransferInfo | null | undefined): JobTask[] {
  return Array.isArray(transfer?.tasks) ? (transfer!.tasks as JobTask[]) : []
}

export function failedTaskCount(transfer: TransferInfo): number {
  const tasks = transferTasks(transfer)
  if (tasks.length) return tasks.filter((task) => isFailedTask(task)).length
  return Number(transfer.failed_files || transfer.failed_tasks || 0) || 0
}

export function runningTaskCount(transfer: TransferInfo): number {
  if (!ACTIVE_STATES.includes(transferStateValue(transfer))) return 0
  const tasks = transferTasks(transfer)
  if (tasks.length) {
    return tasks.filter((task) => ['pending', 'running'].includes(transferStateValue(task))).length
  }
  return Number(transfer.running_files || transfer.loading_files || transfer.pending_files || 0)
}

export function isFailedTask(task: JobTask): boolean {
  return transferStateValue(task) === 'failed'
}

/**
 * Scheduling reasons ("waiting for scheduling", capacity backpressure) surface
 * here too; only render them as errors for failed jobs.
 */
export function isErrorMessage(transfer: TransferInfo): boolean {
  const value = transferStateValue(transfer)
  return value === 'failed' || value === 'partialsuccess' || value === 'canceled'
}

export function taskMessage(task: JobTask, transfer: TransferInfo | null = null): string {
  const message = task.message || task.progress?.message
  return message || transfer?.message || '-'
}

export function shortTaskId(id: string | null | undefined): string {
  if (!id) return '-'
  const value = String(id)
  return value.length > 22 ? `${value.slice(0, 12)}...${value.slice(-7)}` : value
}

export function sourcePath(transfer: TransferInfo | null | undefined): string {
  return String(transfer?.path || transfer?.source_path || '-')
}

export function targetPath(transfer: TransferInfo | null | undefined): string {
  return String(transfer?.target_path || '-')
}

export function mergeTransferList(
  previousTransfers: TransferInfo[],
  nextTransfers: TransferInfo[]
): TransferInfo[] {
  const previousById = new Map(previousTransfers.map((transfer) => [transferId(transfer), transfer]))
  const byId = new Map<string, TransferInfo>()
  for (const transfer of nextTransfers) {
    const id = transferId(transfer)
    if (!id) continue
    const previous = previousById.get(id)
    if (previous) {
      const next: TransferInfo = { ...previous, ...transfer }
      if (!Array.isArray(next.tasks) || next.tasks.length === 0) next.tasks = previous.tasks || []
      byId.set(id, next)
    } else {
      byId.set(id, transfer)
    }
  }
  return Array.from(byId.values()).sort(
    (left, right) => Number(right.update_time_ms || 0) - Number(left.update_time_ms || 0)
  )
}

export function upsertTransfer(
  transfers: TransferInfo[],
  transfer: TransferInfo
): TransferInfo[] {
  const id = transferId(transfer)
  if (!id) return transfers
  const index = transfers.findIndex((item) => transferId(item) === id)
  if (index < 0) return [transfer, ...transfers]
  const previous = transfers[index]
  const next: TransferInfo = { ...previous, ...transfer }
  if (!Array.isArray(next.tasks) || next.tasks.length === 0) next.tasks = previous.tasks || []
  return [...transfers.slice(0, index), next, ...transfers.slice(index + 1)]
}

export { buildJobDisplayName, isWithinRange, jobListSubtitle, jobListTitle, jobListTooltip, relativeTime, retryCount, shortJobId }
