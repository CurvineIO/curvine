import type { Translate } from '@/i18n'
import type { UfsSyncBatch, UfsSyncJob } from './types'
import { isTerminalSyncState, syncState } from '@/pages/browse/utils'

/**
 * Roll the folder-upload batch member jobs into the aggregate row stored under
 * the batch prefix path. State precedence: done > failures > syncing > waiting.
 */
export function aggregateBatchUfsSyncJobs(
  serverJobs: UfsSyncJob[],
  previous: UfsSyncJob
): Partial<UfsSyncJob> {
  let completedFiles = 0
  let failedFiles = 0
  let syncingFiles = 0
  let waitingFiles = 0
  let loadedSize = 0
  let message = ''
  for (const job of serverJobs) {
    const state = syncState(job)
    const totalFiles = Math.max(1, Number(job.total_files || 1))
    const completed = state === 'Completed' ? totalFiles : Number(job.completed_files || 0)
    completedFiles += Math.max(0, Math.min(totalFiles, completed))
    loadedSize += Number(job.loaded_size || 0)
    if (['Failed', 'Canceled', 'Unknown', 'Timed out'].includes(state)) {
      failedFiles += 1
      if (!message && job.message) message = String(job.message)
    } else if (['Pending', 'Loading'].includes(state)) {
      syncingFiles += 1
    } else if (!isTerminalSyncState(state)) {
      waitingFiles += 1
    }
  }
  const totalFiles = Math.max(Number(previous.total_files || 0), serverJobs.length)
  let activeState = 'Waiting for UFS sync'
  if (completedFiles >= totalFiles) activeState = 'Completed'
  else if (failedFiles > 0) activeState = 'Needs attention'
  else if (syncingFiles > 0) activeState = 'Syncing to UFS'
  else if (waitingFiles > 0) activeState = 'Waiting for UFS sync'
  const done = activeState === 'Completed' || (failedFiles > 0 && completedFiles + failedFiles >= totalFiles)
  return {
    state: activeState,
    status: activeState,
    total_files: totalFiles,
    completed_files: completedFiles,
    loaded_size: loadedSize,
    total_size: Number(previous.total_size || 0),
    done,
    message
  }
}

export function batchUfsSyncPollIntervalMs(jobCount: number) {
  if (jobCount > 200) return 8000
  if (jobCount > 50) return 5000
  return 3000
}

export function ufsSyncPollIntervalMs(activePolls: number) {
  if (activePolls > 20) return 5000
  if (activePolls > 5) return 3000
  return 2000
}

export function countActiveUfsSyncJobs(jobs: Record<string, UfsSyncJob>): number {
  return Object.values(jobs).filter(
    (job) => job && !job.done && !isTerminalSyncState(syncState(job))
  ).length
}

/**
 * Folder-upload rows grouped by batch id, with size-weighted progress and the
 * same state precedence the per-path tracker uses.
 */
export function buildUfsSyncBatches(jobs: Record<string, UfsSyncJob>): UfsSyncBatch[] {
  const batches: Record<string, UfsSyncBatch> = {}
  for (const job of Object.values(jobs)) {
    if (!job.batch_id) continue
    const firstPath = String(job.path || job.source_path || '')
    const batch: UfsSyncBatch =
      batches[job.batch_id] ||
      ({
        id: job.batch_id,
        label: job.batch_label || firstPath,
        subtitle: firstPath,
        title: firstPath,
        totalFiles: 0,
        syncedFiles: 0,
        terminalFiles: 0,
        uploadFiles: 0,
        waitingFiles: 0,
        syncingFiles: 0,
        failedFiles: 0,
        syncedSize: 0,
        totalSize: 0,
        progressSize: 0,
        progress: 0,
        state: 'Queued',
        message: ''
      } as UfsSyncBatch)
    const state = syncState(job)
    const progress = Number(job.progress || 0)
    const totalSize = Number(job.total_size || 0)
    const totalFiles = Math.max(1, Number(job.total_files || 0))
    const completedFiles = state === 'Completed' ? totalFiles : Number(job.completed_files || 0)
    batch.totalFiles += totalFiles
    batch.syncedFiles += Math.max(0, Math.min(totalFiles, completedFiles))
    batch.totalSize += totalSize
    batch.progressSize += totalSize > 0 ? (totalSize * progress) / 100 : 0
    if (state === 'Completed') batch.syncedSize += totalSize || Number(job.loaded_size || 0)
    if (['Uploading to Curvine', 'Queued'].includes(state)) batch.uploadFiles += 1
    else if (['Waiting for UFS sync', 'Checking UFS sync', 'NotStarted'].includes(state)) {
      batch.waitingFiles += 1
    } else if (['Pending', 'Loading'].includes(state)) batch.syncingFiles += 1
    if (Boolean(job.done) || isTerminalSyncState(state)) batch.terminalFiles += 1
    if (['Failed', 'Canceled', 'Unknown', 'Timed out'].includes(state)) batch.failedFiles += 1
    if (!batch.message && job.message) batch.message = String(job.message)
    batches[job.batch_id] = batch
  }
  return Object.values(batches).map((batch) => {
    const progress =
      batch.totalSize > 0
        ? Math.max(0, Math.min(100, Math.round((batch.progressSize / batch.totalSize) * 100)))
        : batch.syncedFiles === batch.totalFiles
          ? 100
          : 0
    if (batch.syncedFiles >= batch.totalFiles) return { ...batch, state: 'Completed', progress: 100 }
    if (batch.failedFiles > 0) return { ...batch, state: 'Needs attention', progress }
    if (batch.syncingFiles > 0) return { ...batch, state: 'Syncing to UFS', progress }
    if (batch.waitingFiles > 0) return { ...batch, state: 'Waiting for UFS sync', progress }
    if (batch.uploadFiles > 0) return { ...batch, state: 'Uploading to Curvine', progress }
    if (batch.terminalFiles >= batch.totalFiles) {
      return { ...batch, state: 'Finished with skipped files', progress }
    }
    return { ...batch, progress }
  })
}

/** Panel summary line: per-path rows plus one row per folder-upload batch. */
export function ufsSyncSummaryText(t: Translate, jobs: Record<string, UfsSyncJob>): string {
  const rows = Object.values(jobs).filter((job) => !job.batch_id).length
  const batchIds = new Set(
    Object.values(jobs)
      .filter((job) => job.batch_id)
      .map((job) => String(job.batch_id))
  )
  const total = rows + batchIds.size
  if (!total) return ''
  const active = countActiveUfsSyncJobs(jobs)
  return active > 0
    ? String(t('browse.ufsSync.activeTotal', { active, total }))
    : String(t('browse.ufsSync.tracked', { total }))
}
