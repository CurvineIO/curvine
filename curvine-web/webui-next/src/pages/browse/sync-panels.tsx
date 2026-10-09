import { ChevronDownIcon } from 'lucide-react'

import { Meter } from '@/components/meter'
import { StatusBadge, type StatusTone } from '@/components/resource-table'
import { Button } from '@/components/ui/button'
import { useT } from '@/i18n'
import { translateSyncState } from '@/lib/browseSyncState'
import { formatBytes } from '@/lib/format'
import { cn } from '@/lib/utils'
import type { TrackedUploadBatch, UfsSyncBatch, UfsSyncJob } from './types'
import { syncDisplayPath, syncState, syncStatusClass, type SyncStatusClass } from './utils'

const TONE_BY_CLASS: Record<SyncStatusClass, StatusTone> = {
  completed: 'positive',
  running: 'info',
  waiting: 'warning',
  warning: 'warning',
  failed: 'negative'
}

function SyncStateBadge({ state }: { state: string }) {
  const t = useT()
  return <StatusBadge value={translateSyncState(t, state)} tone={TONE_BY_CLASS[syncStatusClass(state)]} />
}

function SyncProgress({ percent }: { percent: number }) {
  return (
    <div className="flex min-w-0 items-center gap-2">
      <Meter percent={percent} className="min-w-16 flex-1" />
      <strong className="w-9 shrink-0 text-right text-xs tabular-nums">{percent}%</strong>
    </div>
  )
}

export function UploadBatchesPanel({
  batches,
  onClearCompleted,
  onClear
}: {
  batches: TrackedUploadBatch[]
  onClearCompleted(): void
  onClear(): void
}) {
  const t = useT()
  const hasCompleted = batches.some((batch) => batch.done)

  return (
    <section
      className="flex flex-col gap-2 rounded-lg border border-border bg-muted/30 p-3"
      data-testid="upload-batches"
    >
      <div className="flex items-center justify-between gap-3">
        <span className="text-xs font-semibold">{t('browse.upload.progress')}</span>
        <div className="flex items-center gap-2">
          {hasCompleted && (
            <Button variant="ghost" size="xs" className="text-primary" onClick={onClearCompleted}>
              {t('browse.upload.clearCompleted')}
            </Button>
          )}
          <Button variant="ghost" size="xs" className="text-primary" onClick={onClear}>
            {t('browse.upload.clear')}
          </Button>
        </div>
      </div>

      {batches.map((batch) => (
        <div
          key={batch.id}
          className="flex flex-col gap-1 border-t border-border pt-2 first:border-t-0 first:pt-0"
          data-testid={`upload-batch-${batch.id}`}
        >
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs">
            <strong className="min-w-0 truncate font-medium" title={batch.label}>
              {batch.label}
            </strong>
            <SyncStateBadge state={batch.state} />
            <span className="text-muted-foreground">
              {t('browse.upload.uploadedCount', {
                uploaded: batch.uploadedFiles,
                total: batch.totalFiles
              })}
            </span>
            <span className="text-muted-foreground tabular-nums">
              {formatBytes(batch.uploadedSize)}
            </span>
            <div className="min-w-40 flex-1">
              <SyncProgress percent={batch.progress} />
            </div>
          </div>
          {batch.message && (
            <p className="text-xs text-warning" title={batch.message}>
              {batch.message}
            </p>
          )}
        </div>
      ))}
    </section>
  )
}

export function UfsSyncPanel({
  batches,
  jobs,
  browsePath,
  expanded,
  summaryText,
  onToggle,
  onClearCompleted,
  onViewJobs
}: {
  batches: UfsSyncBatch[]
  jobs: UfsSyncJob[]
  browsePath: string
  expanded: boolean
  summaryText: string
  onToggle(): void
  onClearCompleted(): void
  onViewJobs(): void
}) {
  const t = useT()
  const hasCompleted = jobs.some((job) => job.done)

  return (
    <section
      className="flex flex-col rounded-lg border border-border bg-muted/30 p-3"
      data-testid="ufs-sync-panel"
    >
      <div className={cn('flex items-center gap-2', expanded && 'mb-2 border-b border-border pb-2')}>
        <Button
          variant="outline"
          size="icon-sm"
          onClick={onToggle}
          aria-expanded={expanded}
          aria-label={
            expanded ? t('browse.ufsSync.collapseAria') : t('browse.ufsSync.expandAria')
          }
          title={expanded ? t('browse.ufsSync.collapse') : t('browse.ufsSync.expand')}
          data-testid="ufs-sync-toggle"
        >
          <ChevronDownIcon className={cn('transition-transform', !expanded && '-rotate-90')} />
        </Button>
        <button
          type="button"
          className="flex min-w-0 flex-1 flex-wrap items-baseline gap-x-3 gap-y-0.5 rounded-md px-2 py-1 text-left transition-colors hover:bg-card/60"
          onClick={onToggle}
        >
          <span className="text-xs font-semibold">{t('browse.ufsSync.title')}</span>
          <span className="text-xs text-muted-foreground">{summaryText}</span>
        </button>
        <div className="flex shrink-0 items-center gap-2">
          <Button variant="ghost" size="xs" className="text-primary" onClick={onViewJobs}>
            {t('browse.ufsSync.viewJobs')}
          </Button>
          {hasCompleted && (
            <Button variant="ghost" size="xs" className="text-primary" onClick={onClearCompleted}>
              {t('browse.upload.clearCompleted')}
            </Button>
          )}
        </div>
      </div>

      {expanded && (
        <div className="flex max-h-[40vh] flex-col gap-2 overflow-y-auto">
          {batches.map((batch) => (
            <div
              key={batch.id}
              className="flex flex-col gap-1 text-xs font-semibold"
              data-testid={`ufs-batch-${batch.id}`}
            >
              <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                <div className="flex min-w-0 flex-col" title={batch.title || batch.label}>
                  <strong className="truncate">{batch.label}</strong>
                  {batch.subtitle && (
                    <small className="truncate text-xs font-normal text-muted-foreground">
                      {batch.subtitle}
                    </small>
                  )}
                </div>
                <SyncStateBadge state={batch.state} />
                <span className="font-normal text-muted-foreground">
                  {t('browse.ufsSync.ufsSynced', {
                    synced: batch.syncedFiles,
                    total: batch.totalFiles
                  })}
                </span>
                <span className="font-normal text-muted-foreground tabular-nums">
                  {formatBytes(batch.syncedSize)} / {formatBytes(batch.totalSize)}
                </span>
                <div className="min-w-40 flex-1">
                  <SyncProgress percent={batch.progress} />
                </div>
              </div>
              {batch.message && (
                <p className="font-normal text-warning" title={batch.message}>
                  {batch.message}
                </p>
              )}
            </div>
          ))}

          {jobs.map((job) => {
            const state = syncState(job)
            const total = Number(job.total_size || 0)
            const loaded = state === 'Completed' && total > 0 ? total : Number(job.loaded_size || 0)
            const totalFiles = Number(job.total_files || 0)
            const source = String(job.source_path || job.path || '')
            const target = String(job.target_path || job.ufs_path || '')
            const jobId = String(job.job_id || job.id || '')
            const title = [
              source && t('browse.ufsSync.tooltip.cv', { path: source }),
              target && t('browse.ufsSync.tooltip.ufs', { path: target }),
              jobId && t('browse.ufsSync.tooltip.job', { id: jobId })
            ]
              .filter(Boolean)
              .join('\n')

            return (
              <div
                key={job.path}
                className="flex flex-col gap-1 text-xs"
                data-testid={`ufs-job-${job.path}`}
              >
                <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                  <div className="flex min-w-0 flex-col" title={title || undefined}>
                    <strong className="truncate">{syncDisplayPath(job, browsePath)}</strong>
                    <small className="truncate text-muted-foreground">
                      {job.target_path || job.ufs_path || job.display_name || source}
                    </small>
                  </div>
                  <SyncStateBadge state={state} />
                  <span className="text-muted-foreground">
                    {totalFiles
                      ? t('browse.ufsSync.messages.filesCount', {
                          completed: Number(job.completed_files || 0),
                          total: totalFiles
                        })
                      : state === 'NotStarted'
                        ? t('browse.ufsSync.messages.waitingForJob')
                        : t('browse.ufsSync.messages.filesPending')}
                  </span>
                  <span className="text-muted-foreground tabular-nums">
                    {formatBytes(loaded)} / {formatBytes(total)}
                  </span>
                  <div className="min-w-40 flex-1">
                    <SyncProgress percent={Number(job.progress || 0)} />
                  </div>
                </div>
                {job.message && (
                  <p className="text-warning" title={String(job.message)}>
                    {String(job.message)}
                  </p>
                )}
              </div>
            )
          })}
        </div>
      )}
    </section>
  )
}
