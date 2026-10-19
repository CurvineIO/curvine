import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { ChevronDownIcon } from 'lucide-react'
import { Link } from 'react-router-dom'

import { useApi } from '@/api/api-context'
import type { JobInfo } from '@/api/types'
import { EmptyNotice, ErrorAlert } from '@/components/data-state'
import { Meter } from '@/components/meter'
import { MetricCard } from '@/components/metric-card'
import { PageHeader } from '@/components/page-header'
import { MonoText, MutedLine, StatusBadge, type StatusTone } from '@/components/resource-table'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Checkbox } from '@/components/ui/checkbox'
import { Input } from '@/components/ui/input'
import { Spinner } from '@/components/ui/spinner'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'
import { useT } from '@/i18n'
import { translateSyncState } from '@/lib/browseSyncState'
import { formatBytes, formatTime } from '@/lib/format'
import { buildUfsSyncBatches } from '@/lib/ufs-sync/aggregate'
import type { UfsSyncBatch, UfsSyncJob } from '@/lib/ufs-sync/types'
import { useUfsSyncTracked } from '@/lib/ufs-sync/useUfsSyncTracked'
import { useRefreshSignal } from '@/refresh/refresh-context'
import {
  SYNC_JOB_BUCKETS,
  isTerminalSyncJob,
  relatedJobHref,
  relatedJobRef,
  syncJobBucket,
  syncJobSearchText,
  syncJobUpdatedMs,
  taskProgressPercent,
  type SyncJobBucket
} from './sync-jobs/utils'
import {
  normalizePath,
  syncProgress,
  syncState,
  syncStatusClass,
  isTerminalSyncState,
  syncDisplayPath
} from '@/pages/browse/utils'
import { relativeTime } from '@/pages/jobs/utils'

const LIST_POLL_MS = 3000
const LIST_LIMIT = 500

const TONE_BY_CLASS: Record<string, StatusTone> = {
  completed: 'positive',
  running: 'info',
  waiting: 'warning',
  warning: 'warning',
  failed: 'negative'
}

type BucketFilter = SyncJobBucket | 'all'

interface SyncJobRow {
  key: string
  testId: string
  kind: 'job' | 'batch'
  job?: UfsSyncJob
  batch?: UfsSyncBatch
  state: string
  bucket: SyncJobBucket
  updatedMs: number
}

function SyncStateBadge({ state }: { state: string }) {
  const t = useT()
  return (
    <StatusBadge
      value={translateSyncState(t, state)}
      tone={TONE_BY_CLASS[syncStatusClass(state)]}
    />
  )
}

export function SyncJobsPage() {
  const api = useApi()
  const t = useT()
  const {
    jobs: trackedJobs,
    jobsRef: trackedJobsRef,
    isDismissed: isUfsSyncDismissed
  } = useUfsSyncTracked()

  const [loading, setLoading] = useState(true)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [truncated, setTruncated] = useState(false)
  const [polling, setPolling] = useState(true)
  const [bucketFilter, setBucketFilter] = useState<BucketFilter>('all')
  const [query, setQuery] = useState('')
  const [debouncedQuery, setDebouncedQuery] = useState('')
  const [expandedKeys, setExpandedKeys] = useState<Set<string>>(new Set())
  const [details, setDetails] = useState<Record<string, JobInfo>>({})
  const [detailErrors, setDetailErrors] = useState<Record<string, string>>({})
  const [detailLoading, setDetailLoading] = useState<Record<string, boolean>>({})

  const serverRef = useRef<Record<string, UfsSyncJob>>({})
  const [serverVersion, setServerVersion] = useState(0)

  useEffect(() => {
    const timer = window.setTimeout(() => setDebouncedQuery(query), 300)
    return () => window.clearTimeout(timer)
  }, [query])

  /* ----------------------------------------------------------- data loading */

  const refresh = useCallback(async () => {
    try {
      const data = await api.fetchUfsSyncJobs('/', LIST_LIMIT, true, { includeTasks: false })
      const items = (
        Array.isArray(data) ? data : (data as { items?: UfsSyncJob[] })?.items || []
      ) as UfsSyncJob[]
      const reportedTotal = Number((data as { total?: number })?.total ?? items.length)
      const prev = serverRef.current
      const local = trackedJobsRef.current
      const next: Record<string, UfsSyncJob> = {}
      for (const item of items) {
        if (!item || !item.path) continue
        const path = normalizePath(item.path)
        if (isUfsSyncDismissed(path, item)) continue
        const existing = prev[path] || local[path] || ({} as UfsSyncJob)
        const state = String(item.state || item.status || '')
        const done = Boolean(item.done) || isTerminalSyncState(state)
        if (syncState(existing) === 'Uploading to Curvine' && !done) {
          next[path] = { ...existing, ...item, path, done: Boolean(existing.done) }
          continue
        }
        const serverUpdated = Number(item.update_time_ms || item.updated_at || 0)
        const localUpdated = Number(existing.updated_at || 0)
        if (serverUpdated && localUpdated && serverUpdated < localUpdated && !done) continue
        next[path] = { ...item, path, done }
      }
      serverRef.current = next
      setServerVersion((version) => version + 1)
      setTruncated(reportedTotal > items.length)
      setLoadError(null)
    } catch (error) {
      setLoadError(
        t('syncJobs.alerts.loadFailed', {
          error: error instanceof Error ? error.message : String(error)
        })
      )
    } finally {
      setLoading(false)
    }
  }, [api, t, trackedJobsRef, isUfsSyncDismissed])

  useEffect(() => {
    void refresh()
  }, [refresh])

  useRefreshSignal(refresh)

  useEffect(() => {
    if (!polling) return
    let cancelled = false
    let timer = 0
    const tick = async () => {
      await refresh()
      if (!cancelled) timer = window.setTimeout(() => void tick(), LIST_POLL_MS)
    }
    timer = window.setTimeout(() => void tick(), LIST_POLL_MS)
    return () => {
      cancelled = true
      window.clearTimeout(timer)
    }
  }, [polling, refresh])

  /* ------------------------------------------------------------- view model */

  const [viewJobs, localOnlyPaths] = useMemo(() => {
    const merged: Record<string, UfsSyncJob> = { ...serverRef.current }
    const localOnly = new Set<string>()
    for (const [path, job] of Object.entries(trackedJobs)) {
      const normalized = normalizePath(path)
      if (merged[normalized]) continue
      if (isUfsSyncDismissed(normalized, job)) continue
      // Terminal local-only rows are dropped; the server list (include_finished)
      // is the authority for finished jobs.
      if (isTerminalSyncJob(job)) continue
      merged[normalized] = job
      localOnly.add(normalized)
    }
    return [merged, localOnly] as const
  }, [trackedJobs, isUfsSyncDismissed, serverVersion])

  const batches = useMemo(() => buildUfsSyncBatches(viewJobs), [viewJobs])

  const batchMembers = useMemo(() => {
    const members: Record<string, UfsSyncJob[]> = {}
    for (const job of Object.values(viewJobs)) {
      if (!job.batch_id) continue
      const list = members[job.batch_id] || []
      list.push(job)
      members[job.batch_id] = list
    }
    return members
  }, [viewJobs])

  const rows = useMemo<SyncJobRow[]>(() => {
    const result: SyncJobRow[] = []
    for (const job of Object.values(viewJobs)) {
      if (job.batch_id) continue
      const path = normalizePath(job.path || job.source_path || '')
      if (!path) continue
      const state = syncState(job)
      result.push({
        key: `job:${path}`,
        testId: `sync-job-row-${path}`,
        kind: 'job',
        job,
        state,
        bucket: syncJobBucket(job),
        updatedMs: syncJobUpdatedMs(job)
      })
    }
    for (const batch of batches) {
      const members = batchMembers[batch.id] || []
      const updatedMs = members.reduce((max, job) => Math.max(max, syncJobUpdatedMs(job)), 0)
      result.push({
        key: `batch:${batch.id}`,
        testId: `sync-job-row-batch-${batch.id}`,
        kind: 'batch',
        batch,
        state: batch.state,
        bucket: syncJobBucket({ state: batch.state } as UfsSyncJob),
        updatedMs
      })
    }
    result.sort((a, b) => b.updatedMs - a.updatedMs)
    return result
  }, [viewJobs, batches, batchMembers])

  const bucketCounts = useMemo(() => {
    const counts: Record<SyncJobBucket, number> = {
      active: 0,
      completed: 0,
      failed: 0,
      attention: 0
    }
    for (const row of rows) counts[row.bucket] += 1
    return counts
  }, [rows])

  const filteredRows = useMemo(() => {
    const q = debouncedQuery.trim().toLowerCase()
    return rows.filter((row) => {
      if (bucketFilter !== 'all' && row.bucket !== bucketFilter) return false
      if (!q) return true
      const haystack =
        row.kind === 'job' && row.job
          ? syncJobSearchText(row.job)
          : `${row.batch?.label || ''} ${row.batch?.subtitle || ''}`.toLowerCase()
      return haystack.includes(q)
    })
  }, [rows, bucketFilter, debouncedQuery])

  /* ------------------------------------------------------------- expansion */

  const toggleRow = useCallback((key: string) => {
    setExpandedKeys((current) => {
      const next = new Set(current)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }, [])

  const loadDetail = useCallback(
    async (row: SyncJobRow) => {
      if (row.kind !== 'job' || !row.job) return
      if (details[row.key] || detailLoading[row.key] || detailErrors[row.key]) return
      const jobId = String(row.job.job_id || row.job.id || '')
      if (!jobId) {
        setDetailErrors((current) => ({ ...current, [row.key]: 'waitingForJob' }))
        return
      }
      setDetailLoading((current) => ({ ...current, [row.key]: true }))
      try {
        const info = (await api.fetchJobStatus(jobId)) as JobInfo
        setDetails((current) => ({ ...current, [row.key]: info }))
      } catch (error) {
        const status = (error as { status?: number }).status
        setDetailErrors((current) => ({
          ...current,
          [row.key]: status === 404 ? 'waitingForJob' : String(error)
        }))
      } finally {
        setDetailLoading((current) => ({ ...current, [row.key]: false }))
      }
    },
    [api, details, detailLoading, detailErrors]
  )

  const onToggleRow = useCallback(
    (row: SyncJobRow) => {
      toggleRow(row.key)
      if (!expandedKeys.has(row.key)) void loadDetail(row)
    },
    [toggleRow, expandedKeys, loadDetail]
  )

  /* ---------------------------------------------------------------- render */

  const activeCount = bucketCounts.active
  const attentionCount = bucketCounts.failed + bucketCounts.attention

  const emptyMessage = useMemo(() => {
    if (!debouncedQuery.trim() && bucketFilter === 'all' && rows.length === 0) {
      return t('syncJobs.list.emptyNoJobs')
    }
    const hints: string[] = []
    if (bucketFilter !== 'all') hints.push(t('syncJobs.list.hintBucket', { value: bucketFilter }))
    if (debouncedQuery.trim()) {
      hints.push(t('syncJobs.list.hintSearch', { value: debouncedQuery.trim() }))
    }
    const suffix = hints.length ? t('syncJobs.list.activeFilters', { filters: hints.join(', ') }) : ''
    return t('syncJobs.list.emptyFiltered', { suffix })
  }, [debouncedQuery, bucketFilter, rows.length, t])

  return (
    <>
      <PageHeader
        eyebrow={t('nav.groupJobs')}
        title={t('syncJobs.list.title')}
        description={t('syncJobs.list.subtitle')}
        actions={
          <Button
            variant="outline"
            size="sm"
            disabled={loading}
            onClick={() => void refresh()}
            data-testid="sync-jobs-refresh"
          >
            {loading ? <Spinner data-icon="inline-start" /> : null}
            {t('syncJobs.actions.refresh')}
          </Button>
        }
      />

      {loadError && <ErrorAlert message={loadError} />}
      {truncated && (
        <p className="text-xs text-warning" data-testid="sync-jobs-truncated">
          {t('syncJobs.list.truncated')}
        </p>
      )}

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          label={t('syncJobs.metrics.active')}
          value={activeCount}
          meta={t('syncJobs.metrics.activeMeta')}
          tone={activeCount ? 'good' : 'neutral'}
        />
        <MetricCard
          label={t('syncJobs.metrics.completed')}
          value={bucketCounts.completed}
          meta={t('syncJobs.metrics.completedMeta')}
          tone="neutral"
        />
        <MetricCard
          label={t('syncJobs.metrics.attention')}
          value={attentionCount}
          meta={t('syncJobs.metrics.attentionMeta')}
          tone={attentionCount ? 'warn' : 'good'}
        />
        <MetricCard
          label={t('syncJobs.metrics.local')}
          value={localOnlyPaths.size}
          meta={t('syncJobs.metrics.localMeta')}
          tone="neutral"
        />
      </section>

      <section className="flex flex-col gap-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <ToggleGroup
            type="single"
            value={bucketFilter}
            onValueChange={(value) => {
              if (value) setBucketFilter(value as BucketFilter)
            }}
            aria-label={t('syncJobs.filters.ariaStatus')}
            className="max-w-full flex-wrap"
            data-testid="sync-jobs-status-filter"
          >
            <ToggleGroupItem value="all" size="sm" data-testid="sync-jobs-status-all">
              {t('syncJobs.filters.all')} {rows.length}
            </ToggleGroupItem>
            {SYNC_JOB_BUCKETS.map((bucket) => (
              <ToggleGroupItem
                key={bucket}
                value={bucket}
                size="sm"
                data-testid={`sync-jobs-status-${bucket}`}
              >
                {t(`syncJobs.filters.${bucket}`)} {bucketCounts[bucket]}
              </ToggleGroupItem>
            ))}
          </ToggleGroup>

          <div className="flex flex-wrap items-center gap-2">
            <Input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder={t('syncJobs.list.searchPlaceholder')}
              className="w-80"
              aria-label={t('syncJobs.list.searchAria')}
              data-testid="sync-jobs-search"
            />
            <label className="flex items-center gap-1.5 text-xs">
              <Checkbox
                checked={polling}
                onCheckedChange={(checked) => setPolling(checked === true)}
                data-testid="sync-jobs-poll-toggle"
              />
              {t('syncJobs.list.pollActive')}
            </label>
          </div>
        </div>

        {loading && rows.length === 0 ? (
          <CardTableSkeleton />
        ) : filteredRows.length === 0 ? (
          <EmptyNotice title={emptyMessage} />
        ) : (
          <Card className="gap-0 overflow-hidden py-0">
            <Table>
            <TableHeader className="bg-card">
              <TableRow>
                <TableHead className="w-8" />
                <TableHead>{t('syncJobs.list.headerPath')}</TableHead>
                <TableHead className="hidden md:table-cell">
                  {t('syncJobs.list.headerTarget')}
                </TableHead>
                <TableHead>{t('syncJobs.list.headerStatus')}</TableHead>
                <TableHead className="hidden sm:table-cell">
                  {t('syncJobs.list.headerProgress')}
                </TableHead>
                <TableHead className="hidden lg:table-cell">
                  {t('syncJobs.list.headerFiles')}
                </TableHead>
                <TableHead className="hidden lg:table-cell">
                  {t('syncJobs.list.headerSizes')}
                </TableHead>
                <TableHead className="hidden xl:table-cell">
                  {t('syncJobs.list.headerUpdated')}
                </TableHead>
                <TableHead className="text-right">{t('syncJobs.list.headerActions')}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredRows.map((row) => (
                <SyncJobTableRows
                  key={row.key}
                  row={row}
                  expanded={expandedKeys.has(row.key)}
                  detail={details[row.key]}
                  detailError={detailErrors[row.key]}
                  detailLoading={Boolean(detailLoading[row.key])}
                  members={row.kind === 'batch' ? batchMembers[row.batch?.id || ''] || [] : []}
                  onToggle={() => onToggleRow(row)}
                />
              ))}
            </TableBody>
          </Table>
          </Card>
        )}
      </section>
    </>
  )
}

function CardTableSkeleton() {
  const t = useT()
  return (
    <div className="flex items-center justify-center gap-2 rounded-lg border border-border p-6 text-xs text-muted-foreground">
      <Spinner className="size-3.5" />
      {t('common.loading')}
    </div>
  )
}

function SyncJobTableRows({
  row,
  expanded,
  detail,
  detailError,
  detailLoading,
  members,
  onToggle
}: {
  row: SyncJobRow
  expanded: boolean
  detail?: JobInfo
  detailError?: string
  detailLoading: boolean
  members: UfsSyncJob[]
  onToggle(): void
}) {
  const t = useT()
  const job = row.job
  const batch = row.batch
  const state = row.state
  const progress = job ? syncProgress(job) : batch ? batch.progress : 0
  const totalFiles = job ? Number(job.total_files || 0) : batch ? batch.totalFiles : 0
  const completedFiles = job
    ? state === 'Completed' && totalFiles > 0
      ? totalFiles
      : Number(job.completed_files || 0)
    : batch
      ? batch.syncedFiles
      : 0
  const totalSize = Number(job?.total_size || batch?.totalSize || 0)
  const loadedSize =
    state === 'Completed' && totalSize > 0
      ? totalSize
      : Number(job?.loaded_size || batch?.syncedSize || 0)
  const related = job ? relatedJobRef(job) : null
  const title = [
    job && String(job.source_path || job.path || '')
      ? t('browse.ufsSync.tooltip.cv', { path: String(job.source_path || job.path) })
      : '',
    job && String(job.target_path || job.ufs_path || '')
      ? t('browse.ufsSync.tooltip.ufs', { path: String(job.target_path || job.ufs_path) })
      : '',
    job && String(job.job_id || job.id || '')
      ? t('browse.ufsSync.tooltip.job', { id: String(job.job_id || job.id) })
      : ''
  ]
    .filter(Boolean)
    .join('\n')

  return (
    <>
      <TableRow
        data-testid={row.testId}
        className="cursor-pointer"
        onClick={onToggle}
      >
        <TableCell className="w-8">
          <Button
            variant="outline"
            size="icon-sm"
            onClick={(event) => {
              event.stopPropagation()
              onToggle()
            }}
            aria-expanded={expanded}
            aria-label={expanded ? t('syncJobs.actions.collapse') : t('syncJobs.actions.expand')}
            data-testid={`${row.testId}-toggle`}
          >
            <ChevronDownIcon className={expanded ? '' : '-rotate-90'} />
          </Button>
        </TableCell>
        <TableCell>
          <div className="flex min-w-0 flex-col" title={title || undefined}>
            <strong className="truncate text-sm font-medium">
              {job ? syncDisplayPath(job, '/') : batch?.label}
            </strong>
            {batch?.subtitle && <MutedLine>{batch.subtitle}</MutedLine>}
            {job?.display_name && <MutedLine>{job.display_name}</MutedLine>}
          </div>
        </TableCell>
        <TableCell className="hidden md:table-cell">
          <MonoText className="block max-w-56 truncate">
            {job ? job.target_path || job.ufs_path || '-' : '-'}
          </MonoText>
        </TableCell>
        <TableCell>
          <SyncStateBadge state={state} />
        </TableCell>
        <TableCell className="hidden sm:table-cell">
          <div className="flex items-center gap-2">
            <Meter percent={progress} className="min-w-16 flex-1" />
            <span className="w-9 text-right text-xs tabular-nums">{progress}%</span>
          </div>
        </TableCell>
        <TableCell className="hidden lg:table-cell text-xs tabular-nums">
          {totalFiles ? t('browse.ufsSync.messages.filesCount', { completed: completedFiles, total: totalFiles }) : '-'}
        </TableCell>
        <TableCell className="hidden lg:table-cell text-xs tabular-nums whitespace-nowrap">
          {formatBytes(loadedSize)} / {formatBytes(totalSize)}
        </TableCell>
        <TableCell className="hidden xl:table-cell text-xs whitespace-nowrap text-muted-foreground">
          {row.updatedMs ? relativeTime(row.updatedMs) : '-'}
        </TableCell>
        <TableCell className="text-right">
          {related && (
            <Button
              variant="ghost"
              size="xs"
              className="text-primary"
              onClick={(event) => event.stopPropagation()}
              asChild
            >
              <Link to={relatedJobHref(related)} data-testid={`${row.testId}-related`}>
                {t('syncJobs.actions.relatedJob')}
              </Link>
            </Button>
          )}
        </TableCell>
      </TableRow>
      {expanded && (
        <TableRow data-testid={`${row.testId}-detail`}>
          <TableCell colSpan={9} className="bg-muted/20">
            {row.kind === 'batch' ? (
              <div className="flex flex-col gap-1 p-2 text-xs">
                <span className="font-medium">{t('syncJobs.detail.memberHeader')}</span>
                {members.map((member) => (
                  <MonoText key={normalizePath(member.path)} className="truncate">
                    {syncDisplayPath(member, '/')}
                  </MonoText>
                ))}
              </div>
            ) : detailLoading ? (
              <div className="flex items-center gap-2 p-2 text-xs text-muted-foreground">
                <Spinner className="size-3.5" />
                {t('common.loading')}
              </div>
            ) : detailError ? (
              <p className="p-2 text-xs text-muted-foreground" data-testid={`${row.testId}-detail-error`}>
                {detailError === 'waitingForJob'
                  ? t('syncJobs.detail.waitingForJob')
                  : t('syncJobs.detail.loadFailed', { error: detailError })}
              </p>
            ) : detail ? (
              <JobDetailBody job={detail} />
            ) : null}
          </TableCell>
        </TableRow>
      )}
    </>
  )
}

function JobDetailBody({ job }: { job: JobInfo }) {
  const t = useT()
  const tasks = Array.isArray(job.tasks) ? job.tasks : []
  return (
    <div className="flex flex-col gap-3 p-2">
      <div className="flex flex-wrap gap-x-6 gap-y-1 text-xs">
        <span>
          <MutedLine>{t('syncJobs.detail.jobId')}</MutedLine>
          <MonoText>{job.job_id || job.id || '-'}</MonoText>
        </span>
        {Boolean(job.mount_path) && (
          <span>
            <MutedLine>{t('syncJobs.detail.mount')}</MutedLine>
            <MonoText>{String(job.mount_path)}</MonoText>
          </span>
        )}
        {Boolean(job.trigger_event) && (
          <span>
            <MutedLine>{t('syncJobs.detail.triggerEvent')}</MutedLine>
            {String(job.trigger_event)}
          </span>
        )}
        {Boolean(job.message) && (
          <span className="min-w-0">
            <MutedLine>{t('syncJobs.detail.message')}</MutedLine>
            <span className="text-warning">{String(job.message)}</span>
          </span>
        )}
      </div>
      {tasks.length > 0 && (
        <div className="flex flex-col gap-1">
          <span className="text-xs font-medium">{t('syncJobs.detail.tasksHeader')}</span>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>{t('syncJobs.detail.taskId')}</TableHead>
                <TableHead>{t('syncJobs.detail.source')}</TableHead>
                <TableHead>{t('syncJobs.detail.target')}</TableHead>
                <TableHead>{t('syncJobs.detail.worker')}</TableHead>
                <TableHead>{t('syncJobs.detail.state')}</TableHead>
                <TableHead>{t('syncJobs.detail.taskProgress')}</TableHead>
                <TableHead>{t('syncJobs.detail.taskMessage')}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {tasks.map((task) => {
                const percent = taskProgressPercent(task)
                return (
                  <TableRow key={String(task.task_id || task.id || '')}>
                    <TableCell>
                      <MonoText>{task.task_id || task.id || '-'}</MonoText>
                    </TableCell>
                    <TableCell>
                      <MonoText className="block max-w-48 truncate">{task.source_path || '-'}</MonoText>
                    </TableCell>
                    <TableCell>
                      <MonoText className="block max-w-48 truncate">{task.target_path || '-'}</MonoText>
                    </TableCell>
                    <TableCell className="text-xs">{task.worker || '-'}</TableCell>
                    <TableCell className="text-xs">{translateSyncState(t, String(task.state || task.status || ''))}</TableCell>
                    <TableCell>
                      {percent === null ? (
                        <span className="text-xs text-muted-foreground">-</span>
                      ) : (
                        <div className="flex min-w-24 items-center gap-2">
                          <Meter percent={percent} className="min-w-16 flex-1" />
                          <span className="w-9 text-right text-xs tabular-nums">{percent}%</span>
                        </div>
                      )}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">{task.message || '-'}</TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
        </div>
      )}
      <p className="text-xs text-muted-foreground">
        <MutedLine>{t('syncJobs.list.headerUpdated')}</MutedLine>
        {formatTime(Number(job.update_time_ms || job.updated_at || 0))}
      </p>
    </div>
  )
}
