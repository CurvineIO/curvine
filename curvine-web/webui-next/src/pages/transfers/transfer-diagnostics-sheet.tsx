import { useState } from 'react'

import type { TransferInfo } from '@/api/types'
import { EmptyNotice } from '@/components/data-state'
import { DetailList, DetailRow } from '@/components/detail-list'
import { MonoText, StatusBadge, type StatusTone } from '@/components/resource-table'
import { Button } from '@/components/ui/button'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Spinner } from '@/components/ui/spinner'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { useT } from '@/i18n'
import { formatTime } from '@/lib/format'
import { cn } from '@/lib/utils'
import { PANEL_FALLBACK_CLASS, PANEL_SHARE, useContentWidth } from '@/lib/use-content-width'
import {
  TASK_PAGE_MAX,
  canRetry,
  displayTaskTotal,
  failedTaskCount,
  isErrorMessage,
  isFailedTask,
  kindLabel,
  kindValue,
  relativeTime,
  retryCount,
  runningTaskCount,
  shortTaskId,
  sizeLabel,
  sourcePath,
  targetPath,
  taskCountLabel,
  taskMessage,
  transferClass,
  transferId,
  transferStateLabel,
  transferTasks,
  transferredLabel
} from './utils'

const TONE_BY_CLASS: Record<string, StatusTone> = {
  healthy: 'positive',
  running: 'info',
  waiting: 'warning',
  failed: 'negative',
  warning: 'warning'
}

export function TransferDiagnosticsSheet({
  open,
  transfer,
  loading,
  busy,
  taskNextPageToken,
  taskPageSize,
  taskLoadingMore,
  onClose,
  onCopy,
  onRetry,
  onLoadMore
}: {
  open: boolean
  transfer: TransferInfo
  loading: boolean
  busy: boolean
  taskNextPageToken: string
  taskPageSize: number
  taskLoadingMore: boolean
  onClose(): void
  onCopy(text: string, message: string): void
  onRetry(): void
  onLoadMore(): void
}) {
  const [failedOnly, setFailedOnly] = useState(false)
  const t = useT()
  const contentWidth = useContentWidth()

  const tasks = transferTasks(transfer)
  const selectedTasks = failedOnly ? tasks.filter((task) => isFailedTask(task)) : tasks
  const failureCount = failedTaskCount(transfer)
  const canLoadMore = taskPageSize < TASK_PAGE_MAX && Boolean(taskNextPageToken)
  const truncated = taskPageSize >= TASK_PAGE_MAX && Boolean(taskNextPageToken)

  const emptyMessage = failedOnly
    ? t('transfers.detail.noFailedTasks')
    : t('transfers.detail.noTasks')

  return (
    <Sheet open={open} onOpenChange={(next) => !next && onClose()}>
      <SheetContent
        side="right"
        data-testid="transfer-diagnostics-sheet"
        className={cn('sm:max-w-none', contentWidth === null && PANEL_FALLBACK_CLASS)}
        style={contentWidth === null ? undefined : { width: contentWidth * PANEL_SHARE }}
      >
        <SheetHeader>
          <SheetTitle className="flex flex-wrap items-center gap-2">
            {t('transfers.detail.title')}
            <StatusBadge
              value={transferStateLabel(t, transfer)}
              tone={TONE_BY_CLASS[transferClass(transfer)]}
            />
            <StatusBadge
              value={kindLabel(t, transfer)}
              tone={kindValue(transfer) === 'export' ? 'info' : 'positive'}
            />
          </SheetTitle>
          <SheetDescription asChild>
            <div className="flex min-w-0 flex-col gap-1">
              <MonoText className="break-all">{transferId(transfer)}</MonoText>
              <div className="flex flex-wrap gap-2">
                <Button
                  variant="ghost"
                  size="xs"
                  className="text-primary"
                  onClick={() => onCopy(transferId(transfer), t('transfers.copy.transferId'))}
                >
                  {t('transfers.actions.copyJobId')}
                </Button>
                {canRetry(transfer) && (
                  <Button
                    variant="ghost"
                    size="xs"
                    className="text-primary"
                    disabled={busy}
                    onClick={onRetry}
                    data-testid="transfer-detail-retry"
                  >
                    {busy ? t('transfers.actions.working') : t('transfers.actions.retry')}
                  </Button>
                )}
              </div>
            </div>
          </SheetDescription>
        </SheetHeader>

        {loading && (
          <div className="flex items-center gap-2 px-4 text-xs text-muted-foreground">
            <Spinner className="size-3.5" />
            {t('common.loading')}
          </div>
        )}

        <div className="flex min-h-0 flex-1 flex-col gap-4 overflow-auto px-4 pb-4">
          <section className="flex flex-col gap-2 border-t border-border pt-3">
            <h3 className="text-[13px] font-medium">{t('transfers.detail.overview')}</h3>
            <DetailList>
              <DetailRow label={t('transfers.detail.kind')}>{kindLabel(t, transfer)}</DetailRow>
              <DetailRow label={t('transfers.detail.state')}>
                {transferStateLabel(t, transfer)}
              </DetailRow>
              <DetailRow label={t('transfers.detail.submitter')}>
                {String(transfer.submitter || t('transfers.detail.submitterFallback'))}
              </DetailRow>
              <DetailRow label={t('transfers.detail.tenant')}>{String(transfer.tenant || '-')}</DetailRow>
              <DetailRow label={t('transfers.detail.runId')}>
                <span className="font-mono text-xs">{String(transfer.run_id ?? '-')}</span>
              </DetailRow>
              <DetailRow label={t('transfers.detail.updatedAt')}>
                {formatTime(transfer.update_time_ms)}
              </DetailRow>
              <DetailRow label={t('transfers.detail.sourcePath')}>
                <span className="flex min-w-0 items-center gap-2">
                  <span className="min-w-0 break-all font-mono text-xs" title={sourcePath(transfer)}>
                    {sourcePath(transfer)}
                  </span>
                  <Button
                    variant="ghost"
                    size="xs"
                    className="shrink-0 text-primary"
                    onClick={() => onCopy(sourcePath(transfer), t('transfers.copy.sourcePath'))}
                  >
                    {t('transfers.actions.copy')}
                  </Button>
                </span>
              </DetailRow>
              <DetailRow label={t('transfers.detail.targetPath')}>
                <span className="flex min-w-0 items-center gap-2">
                  <span className="min-w-0 break-all font-mono text-xs" title={targetPath(transfer)}>
                    {targetPath(transfer)}
                  </span>
                  <Button
                    variant="ghost"
                    size="xs"
                    className="shrink-0 text-primary"
                    onClick={() => onCopy(targetPath(transfer), t('transfers.copy.targetPath'))}
                  >
                    {t('transfers.actions.copy')}
                  </Button>
                </span>
              </DetailRow>
            </DetailList>
            {transfer.message && (
              <p
                className={
                  isErrorMessage(transfer)
                    ? 'rounded-lg border border-destructive/25 bg-destructive/10 p-2.5 text-xs break-words text-destructive'
                    : 'rounded-lg border border-border bg-muted/40 p-2.5 text-xs break-words text-muted-foreground'
                }
              >
                {transfer.message}
              </p>
            )}
          </section>

          <section className="flex flex-col gap-2 border-t border-border pt-3">
            <h3 className="text-[13px] font-medium">{t('transfers.detail.taskCounters')}</h3>
            <dl className="flex flex-wrap gap-x-6 gap-y-1.5 text-xs">
              <CounterItem
                label={t('transfers.detail.total')}
                value={String(displayTaskTotal(transfer))}
              />
              <CounterItem
                label={t('transfers.detail.completed')}
                value={String(taskCountLabel(transfer.completed_files))}
              />
              <CounterItem
                label={t('transfers.detail.failed')}
                value={String(failureCount)}
                destructive
              />
              <CounterItem
                label={t('transfers.detail.running')}
                value={String(runningTaskCount(transfer))}
              />
              <CounterItem label={t('transfers.detail.loaded')} value={sizeLabel(transfer)} />
              <CounterItem
                label={t('transfers.detail.transferred')}
                value={transferredLabel(transfer)}
              />
            </dl>
            {failureCount > 0 && (
              <Button
                variant="outline"
                size="sm"
                className="self-start"
                onClick={() => setFailedOnly((current) => !current)}
                data-testid="transfer-toggle-failed-tasks"
              >
                {failedOnly
                  ? t('transfers.actions.showAllTasks')
                  : t('transfers.actions.viewFailedTasks')}
              </Button>
            )}
          </section>

          <section className="flex flex-col gap-2 border-t border-border pt-3">
            <h3 className="text-[13px] font-medium">
              {failedOnly ? t('transfers.detail.failedTasks') : t('transfers.detail.allTasks')}
            </h3>
            {selectedTasks.length === 0 ? (
              <EmptyNotice title={emptyMessage} />
            ) : (
              <div className="overflow-x-auto rounded-lg border border-border">
                <Table className="table-fixed">
                  <TableHeader className="bg-card">
                    <TableRow>
                      <TableHead className="w-[13%]">{t('transfers.detail.taskId')}</TableHead>
                      <TableHead className="w-[9%]">{t('transfers.detail.state')}</TableHead>
                      <TableHead className="w-[10%]">{t('transfers.detail.worker')}</TableHead>
                      <TableHead className="w-[17%]">{t('transfers.detail.sourcePath')}</TableHead>
                      <TableHead className="w-[17%]">{t('transfers.detail.targetPath')}</TableHead>
                      <TableHead className="w-[6%]">{t('transfers.detail.retry')}</TableHead>
                      <TableHead className="w-[18%]">{t('transfers.detail.message')}</TableHead>
                      <TableHead className="w-[10%]">{t('transfers.detail.updatedAt')}</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {selectedTasks.map((task) => (
                      <TableRow
                        key={String(task.task_id || task.id || '')}
                        className={isFailedTask(task) ? 'bg-destructive/5' : undefined}
                      >
                        <TableCell>
                          <MonoText className="block truncate">
                            {shortTaskId(String(task.task_id || task.id || ''))}
                          </MonoText>
                        </TableCell>
                        <TableCell>
                          <StatusBadge
                            value={transferStateLabel(t, task)}
                            tone={isFailedTask(task) ? 'negative' : TONE_BY_CLASS[transferClass(task)]}
                          />
                        </TableCell>
                        <TableCell>
                          <div className="truncate text-xs" title={task.worker || '-'}>
                            {task.worker || '-'}
                          </div>
                        </TableCell>
                        <TableCell>
                          <div
                            className="truncate font-mono text-xs"
                            title={task.source_path || '-'}
                          >
                            {task.source_path || '-'}
                          </div>
                        </TableCell>
                        <TableCell>
                          <div
                            className="truncate font-mono text-xs"
                            title={task.target_path || '-'}
                          >
                            {task.target_path || '-'}
                          </div>
                        </TableCell>
                        <TableCell className="text-xs tabular-nums">{retryCount(task)}</TableCell>
                        <TableCell>
                          <div className="truncate text-xs" title={taskMessage(task, transfer)}>
                            {taskMessage(task, transfer)}
                          </div>
                        </TableCell>
                        <TableCell className="text-xs whitespace-nowrap text-muted-foreground">
                          {relativeTime(task.update_time_ms)}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            )}
            {canLoadMore && (
              <Button
                variant="outline"
                size="sm"
                className="self-start"
                disabled={taskLoadingMore}
                onClick={onLoadMore}
                data-testid="transfer-load-more"
              >
                {taskLoadingMore
                  ? t('common.loading')
                  : t('transfers.detail.loadMore', {
                      count: transfer.tasks ? transfer.tasks.length : 0
                    })}
              </Button>
            )}
            {truncated && (
              <p className="text-xs text-muted-foreground">
                {t('transfers.detail.truncatedPrefix', { count: taskPageSize })}{' '}
                <code className="font-mono">curvine-cli transfer status &lt;job&gt; --all</code>{' '}
                {t('transfers.detail.truncatedSuffix')}
              </p>
            )}
          </section>
        </div>
      </SheetContent>
    </Sheet>
  )
}

function CounterItem({
  label,
  value,
  destructive = false
}: {
  label: string
  value: string
  destructive?: boolean
}) {
  return (
    <div className="flex items-baseline gap-1.5">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className={`font-medium tabular-nums ${destructive ? 'text-destructive' : ''}`}>{value}</dd>
    </div>
  )
}
