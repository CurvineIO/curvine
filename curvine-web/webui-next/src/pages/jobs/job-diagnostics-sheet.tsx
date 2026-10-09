import { useState } from 'react'

import type { JobInfo, MountInfo } from '@/api/types'
import { EmptyNotice } from '@/components/data-state'
import { MonoText, StatusBadge } from '@/components/resource-table'
import { Button } from '@/components/ui/button'
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle
} from '@/components/ui/sheet'
import { Spinner } from '@/components/ui/spinner'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { useT } from '@/i18n'
import type { CopyOutcome } from '@/lib/clipboard'
import { formatTime } from '@/lib/format'
import {
  allTasksForJob,
  cliCommands,
  displayTaskTotal,
  failedTaskCount,
  isFailedTask,
  isJobLevelFailure,
  jobClass,
  jobCreatedBy,
  jobId,
  jobTriggerLabel,
  jobTypeLabel,
  mountPath,
  relativeTime,
  retryCount,
  runningTaskCount,
  shortTaskId,
  sizeLabel,
  sourcePath,
  stateLabel,
  targetPath,
  taskErrorCode,
  taskMessage,
  taskWorker,
  ufsUri
} from './utils'

export function JobDiagnosticsSheet({
  open,
  job,
  mounts,
  loading,
  onClose,
  onCopy
}: {
  open: boolean
  job: JobInfo
  mounts: MountInfo[]
  loading: boolean
  onClose(): void
  onCopy(text: string, message: string): Promise<CopyOutcome>
}) {
  const t = useT()
  const [failedOnly, setFailedOnly] = useState(false)
  const [copiedCliKey, setCopiedCliKey] = useState('')

  const tasks = allTasksForJob(job)
  const selectedTasks = failedOnly ? tasks.filter((task) => isFailedTask(task)) : tasks
  const failureCount = failedTaskCount(job)

  const emptyMessage = isJobLevelFailure(job)
    ? t('jobs.detail.jobLevelFailure')
    : failedOnly
      ? t('jobs.detail.noFailedTasks')
      : t('jobs.detail.noTasks')

  return (
    <Sheet open={open} onOpenChange={(next) => !next && onClose()}>
      <SheetContent
        side="right"
        className="data-[side=right]:sm:max-w-3xl"
        data-testid="job-diagnostics-sheet"
      >
        <SheetHeader>
          <SheetTitle className="flex flex-wrap items-center gap-2">
            {t('jobs.detail.title')}
            <StatusBadge
              value={stateLabel(job)}
              tone={
                jobClass(job) === 'failed'
                  ? 'negative'
                  : jobClass(job) === 'healthy'
                    ? 'positive'
                    : jobClass(job) === 'waiting'
                      ? 'warning'
                      : 'info'
              }
            />
            <StatusBadge value={jobTypeLabel(t, job)} tone="info" />
          </SheetTitle>
          <SheetDescription asChild>
            <div className="flex min-w-0 flex-col gap-0.5">
              <MonoText className="truncate">{jobId(job)}</MonoText>
              <div className="flex flex-wrap gap-2">
                <Button
                  variant="ghost"
                  size="xs"
                  className="text-primary"
                  onClick={() => onCopy(jobId(job), t('jobs.copy.jobId'))}
                >
                  {t('jobs.actions.copyJobId')}
                </Button>
                <Button
                  variant="ghost"
                  size="xs"
                  className="text-primary"
                  onClick={() =>
                    onCopy(`curvine load-status ${jobId(job)} --verbose`, t('jobs.copy.cli'))
                  }
                >
                  {t('jobs.actions.copyCli')}
                </Button>
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
            <h3 className="text-[13px] font-medium">{t('jobs.detail.overview')}</h3>
            <dl className="grid gap-2 md:grid-cols-2">
              <DetailItem label={t('jobs.detail.sourceType')} value={jobTypeLabel(t, job)} />
              <DetailItem label={t('jobs.detail.triggerEvent')} value={jobTriggerLabel(t, job)} />
              <DetailItem label={t('jobs.detail.createdBy')} value={jobCreatedBy(t, job)} />
              <DetailItem label={t('jobs.detail.mountPath')} value={mountPath(job, mounts)} />
              <DetailItem label={t('jobs.detail.ufsUri')} value={ufsUri(job, mounts)} />
              <DetailItem label={t('jobs.detail.updatedAt')} value={formatTime(job.update_time_ms)} />
              <DetailItem
                label={t('jobs.detail.sourcePath')}
                value={sourcePath(job)}
                onCopy={() => onCopy(sourcePath(job), t('jobs.copy.sourcePath'))}
                copyLabel={t('jobs.actions.copy')}
                mono
              />
              <DetailItem
                label={t('jobs.detail.targetPath')}
                value={targetPath(job)}
                onCopy={() => onCopy(targetPath(job), t('jobs.copy.targetPath'))}
                copyLabel={t('jobs.actions.copy')}
                mono
              />
            </dl>
            {job.message && (
              <p className="rounded-lg border border-destructive/25 bg-destructive/10 p-2.5 text-xs break-words text-destructive">
                {job.message}
              </p>
            )}
          </section>

          <section className="flex flex-col gap-2 border-t border-border pt-3">
            <h3 className="text-[13px] font-medium">{t('jobs.detail.taskCounters')}</h3>
            <div className="grid grid-cols-2 gap-2 md:grid-cols-5">
              <CounterItem label={t('jobs.detail.total')} value={String(displayTaskTotal(job))} />
              <CounterItem
                label={t('jobs.detail.completed')}
                value={String(Number(job.completed_files || 0))}
              />
              <CounterItem label={t('jobs.detail.failed')} value={String(failureCount)} destructive />
              <CounterItem
                label={t('jobs.detail.running')}
                value={String(runningTaskCount(job))}
              />
              <CounterItem label={t('jobs.detail.loaded')} value={sizeLabel(job)} />
            </div>
            {failureCount > 0 && (
              <Button
                variant="outline"
                size="sm"
                className="self-start"
                onClick={() => setFailedOnly((current) => !current)}
                data-testid="job-toggle-failed-tasks"
              >
                {failedOnly ? t('jobs.actions.showAllTasks') : t('jobs.actions.viewFailedTasks')}
              </Button>
            )}
          </section>

          <section className="flex flex-col gap-2 border-t border-border pt-3">
            <h3 className="text-[13px] font-medium">
              {failedOnly ? t('jobs.detail.failedTasks') : t('jobs.detail.allTasks')}
            </h3>
            {selectedTasks.length === 0 ? (
              <EmptyNotice title={emptyMessage} />
            ) : (
              <div className="overflow-x-auto rounded-lg border border-border">
                <Table className="table-fixed">
                  <TableHeader className="bg-card">
                    <TableRow>
                      <TableHead className="w-[12%]">{t('jobs.detail.taskId')}</TableHead>
                      <TableHead className="w-[9%]">{t('jobs.detail.state')}</TableHead>
                      <TableHead className="w-[10%]">{t('jobs.detail.worker')}</TableHead>
                      <TableHead className="w-[16%]">{t('jobs.detail.sourcePath')}</TableHead>
                      <TableHead className="w-[16%]">{t('jobs.detail.targetPath')}</TableHead>
                      <TableHead className="w-[8%]">{t('jobs.detail.errorCode')}</TableHead>
                      <TableHead className="w-[17%]">{t('jobs.detail.errorMessage')}</TableHead>
                      <TableHead className="w-[5%]">{t('jobs.detail.retry')}</TableHead>
                      <TableHead className="w-[7%]">{t('jobs.detail.updatedAt')}</TableHead>
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
                            value={stateLabel(task)}
                            tone={isFailedTask(task) ? 'negative' : 'neutral'}
                          />
                        </TableCell>
                        <TableCell>
                          <div className="truncate text-xs" title={taskWorker(task)}>
                            {taskWorker(task)}
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
                        <TableCell className="font-mono text-xs">
                          {taskErrorCode(task, job)}
                        </TableCell>
                        <TableCell>
                          <div className="truncate text-xs" title={taskMessage(task, job)}>
                            {taskMessage(task, job)}
                          </div>
                        </TableCell>
                        <TableCell className="text-xs tabular-nums">{retryCount(task)}</TableCell>
                        <TableCell className="text-xs whitespace-nowrap text-muted-foreground">
                          {relativeTime(task.update_time_ms)}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            )}
          </section>

          <section className="flex flex-col gap-2 border-t border-border pt-3">
            <h3 className="text-[13px] font-medium">{t('jobs.detail.cliCommands')}</h3>
            <div className="flex flex-col gap-2">
              {cliCommands(t, job, mounts).map((item) => (
                <div key={item.key} className="flex flex-col gap-1">
                  <span className="text-xs text-muted-foreground">{item.label}</span>
                  <div className="flex items-start gap-2">
                    <code className="min-w-0 flex-1 rounded-md border border-border bg-muted/40 p-2 font-mono text-xs break-all whitespace-pre-wrap">
                      {item.command}
                    </code>
                    <Button
                      variant="outline"
                      size="xs"
                      onClick={async () => {
                        // Only claim success when the copy actually happened: the fallback
                        // hands the text over for a manual copy instead.
                        const outcome = await onCopy(item.command, t('jobs.copy.command'))
                        if (outcome !== 'copied') return
                        setCopiedCliKey(item.key)
                        window.setTimeout(() => setCopiedCliKey(''), 1500)
                      }}
                    >
                      {copiedCliKey === item.key ? t('common.copied') : t('jobs.actions.copy')}
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          </section>
        </div>
      </SheetContent>
    </Sheet>
  )
}

function DetailItem({
  label,
  value,
  onCopy,
  copyLabel,
  mono = false
}: {
  label: string
  value: string
  onCopy?(): void
  copyLabel?: string
  mono?: boolean
}) {
  return (
    <div className="flex min-h-14 flex-col gap-1 rounded-lg border border-border bg-card/60 p-2">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd
        className={`min-w-0 flex-1 break-words text-xs ${mono ? 'font-mono' : ''}`}
        title={value}
      >
        {value}
      </dd>
      {onCopy && copyLabel && (
        <Button variant="ghost" size="xs" className="self-start text-primary" onClick={onCopy}>
          {copyLabel}
        </Button>
      )}
    </div>
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
    <div className="flex min-h-14 flex-col gap-1 rounded-lg border border-border bg-card/60 p-2">
      <span className="text-xs text-muted-foreground">{label}</span>
      <strong className={`text-xs ${destructive ? 'text-destructive' : ''}`}>{value}</strong>
    </div>
  )
}
