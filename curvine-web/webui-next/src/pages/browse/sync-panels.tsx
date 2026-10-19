import { Meter } from '@/components/meter'
import { StatusBadge, type StatusTone } from '@/components/resource-table'
import { Button } from '@/components/ui/button'
import { useT } from '@/i18n'
import { translateSyncState } from '@/lib/browseSyncState'
import { formatBytes } from '@/lib/format'
import type { TrackedUploadBatch } from './types'
import { syncStatusClass, type SyncStatusClass } from './utils'

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
