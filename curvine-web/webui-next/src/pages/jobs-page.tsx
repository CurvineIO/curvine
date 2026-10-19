import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { CircleAlert } from 'lucide-react'
import { useSearchParams } from 'react-router-dom'

import { useApi } from '@/api/api-context'
import type { JobInfo, MountInfo } from '@/api/types'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { EmptyNotice, ErrorAlert } from '@/components/data-state'
import { Meter } from '@/components/meter'
import { MetricCard } from '@/components/metric-card'
import { PageHeader } from '@/components/page-header'
import { MonoText, StatusBadge, type StatusTone } from '@/components/resource-table'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Checkbox } from '@/components/ui/checkbox'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/components/ui/select'
import { Spinner } from '@/components/ui/spinner'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { useToast } from '@/components/ui/toast'
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'
import { useCopy } from '@/components/copy-fallback'
import type { CopyOutcome } from '@/lib/clipboard'
import { useT } from '@/i18n'
import { truncatedListLimit } from '@/lib/list-window'
import { useSlidingSheet } from '@/lib/use-sliding-sheet'
import { useRefreshSignal } from '@/refresh/refresh-context'
import { JobDiagnosticsSheet } from './jobs/job-diagnostics-sheet'
import { SubmitLoadSheet } from './jobs/submit-load-sheet'
import {
  failedTaskCount,
  formatFiles,
  hasFailedWork,
  isAutoExportJob,
  isCompleteJobId,
  isFinal,
  isWithinRange,
  jobBucket,
  jobClass,
  jobId,
  jobListSubtitle,
  jobListTitle,
  jobListTooltip,
  jobProgress,
  jobTypeLabel,
  mergeJobList,
  mountPath,
  normalizeJobId,
  relativeTime,
  runningTaskCount,
  sourcePath,
  stateLabel,
  targetPath,
  taskDetails,
  taskMessage,
  taskWorker,
  upsertJob,
  ufsUri
} from './jobs/utils'

const STATUS_FILTERS = ['all', 'running', 'completed', 'failed', 'canceled'] as const
type StatusFilter = (typeof STATUS_FILTERS)[number]

const TONE_BY_CLASS: Record<string, StatusTone> = {
  healthy: 'positive',
  running: 'info',
  waiting: 'warning',
  failed: 'negative',
  warning: 'warning'
}

export function JobsPage() {
  const t = useT()
  const api = useApi()
  const toast = useToast()
  const { copy } = useCopy()

  const [jobs, setJobs] = useState<JobInfo[]>([])
  const [mounts, setMounts] = useState<MountInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [detailLoading, setDetailLoading] = useState(false)
  const [loadError, setLoadError] = useState<string | null>(null)
  /**
   * Set when the list response filled the server's window (500 rows), meaning
   * older jobs exist but were not sent. Surfaced so a full-looking page is not
   * mistaken for the complete set.
   */
  const [listLimit, setListLimit] = useState<number | null>(null)

  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all')
  const [query, setQuery] = useState('')
  const [typeFilter, setTypeFilter] = useState('all')
  const [mountFilter, setMountFilter] = useState('')
  const [workerFilter, setWorkerFilter] = useState('')
  const [taskFilter, setTaskFilter] = useState('all')
  const [timeRange, setTimeRange] = useState('all')
  const [failedOnly, setFailedOnly] = useState(false)
  const [polling, setPolling] = useState(true)

  const [selectedJobId, setSelectedJobId] = useState('')
  const [searchParams, setSearchParams] = useSearchParams()
  const [loadFormOpen, setLoadFormOpen] = useState(false)
  const loadSheet = useSlidingSheet(loadFormOpen)
  const [cancelTarget, setCancelTarget] = useState<JobInfo | null>(null)
  const [busyJobs, setBusyJobs] = useState<Record<string, string>>({})

  const jobsRef = useRef(jobs)
  jobsRef.current = jobs
  const lastLookupQuery = useRef('')
  const lookupInFlight = useRef('')

  const fetchMounts = useCallback(async () => {
    try {
      const data = await api.fetchMountsData()
      setMounts((Array.isArray(data) ? data : (data as { items?: MountInfo[] })?.items || []) as MountInfo[])
    } catch {
      setMounts([])
    }
  }, [api])

  const refreshJobs = useCallback(async () => {
    setLoading(true)
    setLoadError(null)
    try {
      const response = await api.fetchJobsData({ page_size: 500 })
      const next = (Array.isArray(response)
        ? response
        : (response as { items?: JobInfo[] })?.items || []) as JobInfo[]
      setListLimit(truncatedListLimit(response, next.length))
      setJobs((current) => mergeJobList(current, next))
    } catch (error) {
      setLoadError(
        t('jobs.alerts.refreshFailed', {
          error: error instanceof Error ? error.message : String(error)
        })
      )
    } finally {
      setLoading(false)
    }
  }, [api, t])

  useEffect(() => {
    void refreshJobs()
    void fetchMounts()
  }, [refreshJobs, fetchMounts])

  useRefreshSignal(refreshJobs)

  /* Poll only non-final jobs, one round every 3s while enabled. */
  useEffect(() => {
    if (!polling) return
    let cancelled = false
    let timer = 0
    const tick = async () => {
      const active = jobsRef.current.filter((job) => !isFinal(job))
      for (const job of active) {
        const id = jobId(job)
        if (!id) continue
        try {
          const next = (await api.fetchJobStatus(id)) as JobInfo
          setJobs((current) => upsertJob(current, next))
        } catch {
          // A single failed poll must not stop the loop.
        }
      }
      if (!cancelled) timer = window.setTimeout(() => void tick(), 3000)
    }
    timer = window.setTimeout(() => void tick(), 3000)
    return () => {
      cancelled = true
      window.clearTimeout(timer)
    }
  }, [polling, api])

  const setJobBusy = useCallback((id: string, action: string | null) => {
    if (!id) return
    setBusyJobs((current) => {
      if (!action) {
        const next = { ...current }
        delete next[id]
        return next
      }
      return { ...current, [id]: action }
    })
  }, [])

  const lookupJob = useCallback(
    async (rawId: string): Promise<JobInfo | null> => {
      const id = String(rawId || '').trim()
      if (!id || !isCompleteJobId(id)) return null
      const normalized = normalizeJobId(id)
      if (lookupInFlight.current === normalized) return null
      lookupInFlight.current = normalized
      setLoading(true)
      try {
        const job = (await api.fetchJobStatus(id)) as JobInfo
        setJobs((current) => upsertJob(current, job))
        lastLookupQuery.current = normalized
        toast.success(`Job loaded: ${normalized}`)
        return job
      } catch (error) {
        lastLookupQuery.current = ''
        setLoadError(
          t('jobs.alerts.lookupFailed', {
            error: error instanceof Error ? error.message : String(error)
          })
        )
        return null
      } finally {
        lookupInFlight.current = ''
        setLoading(false)
      }
    },
    [api, t]
  )

  const performJobIdLookup = useCallback(
    async (rawId: string) => {
      const normalized = normalizeJobId(rawId)
      if (!normalized) return
      const existing = jobsRef.current.find((job) => normalizeJobId(jobId(job)) === normalized)
      if (existing) {
        lastLookupQuery.current = normalized
        toast.success(`Job found: ${normalized}`)
        return
      }
      await lookupJob(rawId)
    },
    [lookupJob]
  )

  useEffect(() => {
    const trimmed = query.trim()
    if (!isCompleteJobId(trimmed)) {
      lastLookupQuery.current = ''
      return
    }
    if (normalizeJobId(trimmed) === lastLookupQuery.current) return
    const timer = window.setTimeout(() => void performJobIdLookup(trimmed), 300)
    return () => window.clearTimeout(timer)
  }, [query, performJobIdLookup])

  // Deep link: /jobs?job=<id> opens that job's diagnostics sheet on arrival.
  useEffect(() => {
    const param = String(searchParams.get('job') || '').trim()
    if (!param) return
    const match = jobsRef.current.find(
      (job) => normalizeJobId(jobId(job)) === normalizeJobId(param)
    )
    if (match) {
      setSelectedJobId(jobId(match))
      return
    }
    let cancelled = false
    void lookupJob(param).then((job) => {
      if (cancelled || !job) return
      setSelectedJobId(jobId(job))
      void openDetails(job)
    })
    return () => {
      cancelled = true
    }
    // Only the URL parameter may retrigger this, not list refreshes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams])

  const clearJobParam = useCallback(() => {
    setSearchParams(
      (current) => {
        if (!current.has('job')) return current
        const next = new URLSearchParams(current)
        next.delete('job')
        return next
      },
      { replace: true }
    )
  }, [setSearchParams])

  /* ------------------------------------------------------------- derived */

  const selectedJob = useMemo(
    () => (selectedJobId ? jobs.find((job) => jobId(job) === selectedJobId) || null : null),
    [jobs, selectedJobId]
  )
  const detailSheet = useSlidingSheet(Boolean(selectedJob), selectedJob)

  const counts = useMemo(
    () => ({
      active: jobs.filter((job) => jobBucket(job) === 'running').length,
      completed: jobs.filter((job) => stateLabel(job).toLowerCase() === 'completed').length,
      failed: jobs.filter((job) => stateLabel(job).toLowerCase() === 'failed').length,
      canceled: jobs.filter((job) => stateLabel(job).toLowerCase() === 'canceled').length,
      autoExport: jobs.filter((job) => isAutoExportJob(job)).length,
      failed24h: jobs.filter(
        (job) => stateLabel(job).toLowerCase() === 'failed' && isWithinRange(job.update_time_ms, '24h')
      ).length,
      failedTasks: jobs.reduce((sum, job) => sum + failedTaskCount(job), 0),
      runningTasks: jobs.reduce((sum, job) => sum + runningTaskCount(job), 0)
    }),
    [jobs]
  )

  const mountOptions = useMemo(() => {
    const values = new Set(mounts.map((mount) => mount.cv_path).filter(Boolean))
    for (const job of jobs) {
      const mount = mountPath(job, mounts)
      if (mount && mount !== '-') values.add(mount)
    }
    return Array.from(values).sort()
  }, [mounts, jobs])

  const workerOptions = useMemo(() => {
    const values = new Set<string>()
    for (const job of jobs) {
      for (const task of taskDetails(job)) {
        const worker = taskWorker(task)
        if (worker && worker !== '-') values.add(worker)
      }
    }
    return Array.from(values).sort()
  }, [jobs])

  const filteredJobs = useMemo(() => {
    const q = query.trim()
    const qLower = q.toLowerCase()
    const exactJobId = isCompleteJobId(q)
    return jobs.filter((job) => {
      if (exactJobId && normalizeJobId(jobId(job)) === qLower) return true
      if (statusFilter !== 'all' && jobBucket(job) !== statusFilter) return false
      if (failedOnly && !hasFailedWork(job)) return false
      if (typeFilter === 'manual' && isAutoExportJob(job)) return false
      if (typeFilter === 'auto' && !isAutoExportJob(job)) return false
      if (mountFilter && mountPath(job, mounts) !== mountFilter) return false
      const tasks = taskDetails(job)
      if (workerFilter && tasks.length && !tasks.some((task) => taskWorker(task) === workerFilter)) {
        return false
      }
      if (taskFilter === 'failed' && !hasFailedWork(job)) return false
      if (!isWithinRange(job.update_time_ms || job.updated_at, timeRange)) return false
      if (!qLower) return true
      const taskText = tasks.length
        ? tasks
            .map(
              (task) =>
                `${task.task_id || ''} ${task.source_path || ''} ${task.target_path || ''} ${taskWorker(task)} ${taskMessage(task, job)}`
            )
            .join(' ')
        : ''
      const haystack =
        `${jobListTitle(job)} ${jobListSubtitle(job, jobId(job))} ${jobId(job)} ${job.submitter || ''} ` +
        `${sourcePath(job)} ${targetPath(job)} ${mountPath(job, mounts)} ${ufsUri(job, mounts)} ` +
        `${jobTypeLabel(t, job)} ${job.message || ''} ${taskText}`.toLowerCase()
      return haystack.includes(qLower)
    })
  }, [
    jobs,
    query,
    statusFilter,
    failedOnly,
    typeFilter,
    mountFilter,
    workerFilter,
    taskFilter,
    timeRange,
    mounts,
    t
  ])

  const emptyMessage = useMemo(() => {
    if (!jobs.length) return t('jobs.list.emptyNoJobs')
    const hints: string[] = []
    if (timeRange !== 'all') hints.push(`time range: ${timeRange}`)
    if (statusFilter !== 'all') hints.push(`status: ${statusFilter}`)
    if (typeFilter !== 'all') hints.push(`type: ${typeFilter}`)
    if (failedOnly) hints.push('failed only')
    const suffix = hints.length ? t('jobs.list.activeFilters', { filters: hints.join(', ') }) : ''
    return t('jobs.list.emptyFiltered', { suffix })
  }, [jobs.length, timeRange, statusFilter, typeFilter, failedOnly, t])

  /* ------------------------------------------------------------ actions */

  async function copyText(text: string, message: string): Promise<CopyOutcome> {
    if (!text || text === '-') {
      toast.info(t('jobs.copy.nothing'))
      return 'manual'
    }
    return copy(text, message)
  }

  async function confirmCancel() {
    const job = cancelTarget
    if (!job) return
    const id = jobId(job)
    setCancelTarget(null)
    setJobBusy(id, 'cancel')
    try {
      const next = (await api.cancelLoadJob(id)) as JobInfo
      setJobs((current) => upsertJob(current, next))
      toast.success(
        t('jobs.alerts.cancelRequested', { id, state: stateLabel(next) })
      )
    } catch (error) {
      toast.error(
        t('jobs.alerts.cancelFailed', {
          error: error instanceof Error ? error.message : String(error)
        })
      )
    } finally {
      setJobBusy(id, null)
    }
  }

  async function openDetails(job: JobInfo) {
    const id = jobId(job)
    if (!id) return
    setSelectedJobId(id)
    if (taskDetails(job).length) return
    setDetailLoading(true)
    try {
      const next = (await api.fetchJobStatus(id)) as JobInfo
      setJobs((current) => upsertJob(current, next))
    } catch {
      // The sheet still renders with the summary already available.
    } finally {
      setDetailLoading(false)
    }
  }

  return (
    <>
      <PageHeader
        eyebrow={t('nav.groupJobs')}
        title={t('jobs.list.title')}
        description={t('jobs.list.subtitle')}
        actions={
          <>
            <Button
              variant="outline"
              size="sm"
              disabled={loading}
              onClick={() => void refreshJobs()}
              data-testid="jobs-refresh"
            >
              {loading ? <Spinner data-icon="inline-start" /> : null}
              {t('jobs.list.refresh')}
            </Button>
            <Button size="sm" onClick={() => setLoadFormOpen(true)} data-testid="jobs-open-load-form">
              {t('jobs.list.submitLoad')}
            </Button>
          </>
        }
      />

      {loadError && <ErrorAlert message={loadError} />}
      {listLimit !== null && (
        <Alert data-testid="jobs-list-truncated">
          <AlertDescription>{t('jobs.list.truncated', { limit: listLimit })}</AlertDescription>
        </Alert>
      )}

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
        <MetricCard
          label={t('jobs.metrics.recentJobs')}
          value={jobs.length}
          meta={t('jobs.metrics.recentJobsMeta')}
          delta={t('jobs.metrics.recent')}
          tone="neutral"
        />
        <MetricCard
          label={t('jobs.metrics.autoExportJobs')}
          value={counts.autoExport}
          meta={t('jobs.metrics.autoExportMeta')}
          delta={t('jobs.metrics.fs_mode')}
          tone="neutral"
        />
        <MetricCard
          label={t('jobs.metrics.failedJobs24h')}
          value={counts.failed24h}
          meta={t('jobs.metrics.failedJobs24hMeta')}
          delta={t(`jobs.metrics.${counts.failed24h ? 'review' : 'none'}`)}
          tone={counts.failed24h ? 'bad' : 'good'}
        />
        <MetricCard
          label={t('jobs.metrics.failedTasks')}
          value={counts.failedTasks}
          meta={t('jobs.metrics.failedTasksMeta')}
          delta={t('jobs.metrics.tasks')}
          tone={counts.failedTasks ? 'bad' : 'good'}
        />
        <MetricCard
          label={t('jobs.metrics.runningTasks')}
          value={counts.runningTasks}
          meta={t('jobs.metrics.runningTasksMeta')}
          delta={t('jobs.metrics.active')}
          tone="good"
        />
      </section>

      <section className="flex flex-col gap-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <ToggleGroup
            type="single"
            value={statusFilter}
            onValueChange={(value) => value && setStatusFilter(value as StatusFilter)}
            aria-label={t('jobs.filters.ariaLabel')}
            data-testid="jobs-status-filter"
          >
            {STATUS_FILTERS.map((filter) => (
              <ToggleGroupItem key={filter} value={filter} size="sm" data-testid={`jobs-status-${filter}`}>
                {t(`jobs.filters.${filter}`)}{' '}
                {filter === 'all'
                  ? jobs.length
                  : filter === 'running'
                    ? counts.active
                    : counts[filter]}
              </ToggleGroupItem>
            ))}
          </ToggleGroup>

          <div className="flex flex-wrap items-center gap-2">
            <Input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              onKeyDown={(event) => {
                if (event.key !== 'Enter') return
                event.preventDefault()
                const trimmed = query.trim()
                if (isCompleteJobId(trimmed)) void performJobIdLookup(trimmed)
              }}
              placeholder={t('jobs.list.searchPlaceholder')}
              className="w-80"
              aria-label={t('jobs.list.searchPlaceholder')}
              data-testid="jobs-search"
            />
            <FilterSelect
              value={typeFilter}
              onChange={setTypeFilter}
              testId="jobs-filter-type"
              options={[
                { value: 'all', label: t('jobs.filters.allTypes') },
                { value: 'manual', label: t('jobs.filters.manualLoad') },
                { value: 'auto', label: t('jobs.filters.autoExport') }
              ]}
            />
            <FilterSelect
              value={mountFilter}
              onChange={setMountFilter}
              testId="jobs-filter-mount"
              options={[
                { value: '', label: t('jobs.filters.allMounts') },
                ...mountOptions.map((mount) => ({ value: mount, label: mount }))
              ]}
            />
            <FilterSelect
              value={workerFilter}
              onChange={setWorkerFilter}
              testId="jobs-filter-worker"
              options={[
                { value: '', label: t('jobs.filters.allWorkers') },
                ...workerOptions.map((worker) => ({ value: worker, label: worker }))
              ]}
            />
            <FilterSelect
              value={taskFilter}
              onChange={setTaskFilter}
              testId="jobs-filter-task"
              options={[
                { value: 'all', label: t('jobs.filters.allTasks') },
                { value: 'failed', label: t('jobs.filters.hasFailedTask') }
              ]}
            />
            <FilterSelect
              value={timeRange}
              onChange={setTimeRange}
              testId="jobs-filter-time"
              options={[
                { value: 'all', label: t('jobs.filters.anyTime') },
                { value: '1h', label: t('jobs.filters.last1h') },
                { value: '24h', label: t('jobs.filters.last24h') },
                { value: '7d', label: t('jobs.filters.last7d') }
              ]}
            />
            <label className="flex items-center gap-1.5 text-xs">
              <Checkbox
                checked={polling}
                onCheckedChange={(checked) => setPolling(checked === true)}
                data-testid="jobs-poll-toggle"
              />
              {t('jobs.list.pollActive')}
            </label>
            <label className="flex items-center gap-1.5 text-xs">
              <Checkbox
                checked={failedOnly}
                onCheckedChange={(checked) => setFailedOnly(checked === true)}
                data-testid="jobs-failed-only"
              />
              {t('jobs.list.failedOnly')}
            </label>
          </div>
        </div>

        <Card className="gap-0 overflow-hidden py-0">
          <Table className="table-fixed">
            <TableHeader className="bg-card">
              <TableRow>
                <TableHead className="w-[14%]">{t('jobs.list.headerJob')}</TableHead>
                <TableHead className="w-[9%]">{t('jobs.list.headerType')}</TableHead>
                <TableHead className="w-[28%]">{t('jobs.list.headerPath')}</TableHead>
                <TableHead className="w-[9%]">{t('jobs.list.headerStatus')}</TableHead>
                <TableHead className="w-[12%]">{t('jobs.list.headerProgress')}</TableHead>
                <TableHead className="w-[7%]">{t('jobs.list.headerFiles')}</TableHead>
                <TableHead className="w-[9%]">{t('jobs.list.headerUpdated')}</TableHead>
                <TableHead className="w-[12%] text-right">{t('jobs.list.headerActions')}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredJobs.map((job) => {
                const id = jobId(job)
                const busy = Boolean(busyJobs[id])
                const auto = isAutoExportJob(job)
                return (
                  <TableRow
                    key={id}
                    data-state={selectedJobId === id ? 'selected' : undefined}
                    data-testid={`job-row-${id}`}
                  >
                    <TableCell>
                      <button
                        type="button"
                        className="block w-full min-w-0 truncate text-left"
                        title={jobListTooltip(job, id)}
                        onClick={() => void copyText(id, t('jobs.copy.jobId'))}
                        data-testid={`job-copy-${id}`}
                      >
                        <strong className="truncate font-mono text-xs">{jobListTitle(job)}</strong>
                      </button>
                    </TableCell>
                    <TableCell>
                      <StatusBadge
                        value={jobTypeLabel(t, job)}
                        tone={auto ? 'info' : 'positive'}
                      />
                    </TableCell>
                    <TableCell>
                      {/* One line: the full source/target/message text is in the
                          hover title, and the warning icon keeps a message from
                          being invisible. */}
                      <div
                        className="flex w-full min-w-0 items-center gap-1.5"
                        title={`${sourcePath(job)}\n→ ${targetPath(job)}${job.message ? `\n${job.message}` : ''}`}
                      >
                        <span className="truncate text-xs">{sourcePath(job)}</span>
                        <MonoText className="min-w-0 truncate">→ {targetPath(job)}</MonoText>
                        {job.message && (
                          <span className="shrink-0" title={job.message}>
                            <CircleAlert
                              className="size-3.5 text-warning"
                              data-testid={`job-message-${id}`}
                            />
                          </span>
                        )}
                      </div>
                    </TableCell>
                    <TableCell>
                      <StatusBadge value={stateLabel(job)} tone={TONE_BY_CLASS[jobClass(job)]} />
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center gap-1.5">
                        <Meter percent={jobProgress(job)} className="flex-1" />
                        <span className="shrink-0 text-xs tabular-nums">{jobProgress(job)}%</span>
                      </div>
                    </TableCell>
                    <TableCell className="text-xs tabular-nums">{formatFiles(job)}</TableCell>
                    <TableCell
                      className="text-xs whitespace-nowrap text-muted-foreground"
                      title={relativeTime(job.update_time_ms)}
                    >
                      {relativeTime(job.update_time_ms)}
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center justify-end gap-1">
                        <Button
                          variant="ghost"
                          size="xs"
                          className="text-primary"
                          onClick={() => void openDetails(job)}
                          data-testid={`job-details-${id}`}
                        >
                          {t('jobs.actions.details')}
                        </Button>
                        {!isFinal(job) && (
                          <Button
                            variant="ghost"
                            size="xs"
                            className="text-destructive"
                            disabled={busy}
                            onClick={() => setCancelTarget(job)}
                            data-testid={`job-cancel-${id}`}
                          >
                            {busyJobs[id] === 'cancel' ? t('jobs.actions.working') : t('jobs.actions.cancel')}
                          </Button>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>

          {filteredJobs.length === 0 && !loading && (
            <div className="border-t border-border p-2">
              <EmptyNotice title={emptyMessage} />
            </div>
          )}
          {loading && (
            <div className="flex items-center justify-center gap-2 border-t border-border p-3 text-xs text-muted-foreground">
              <Spinner className="size-3.5" />
              {t('common.loading')}
            </div>
          )}
        </Card>
      </section>

      {detailSheet.mounted && detailSheet.value && (
        <JobDiagnosticsSheet
          key={jobId(detailSheet.value)}
          open={detailSheet.open}
          job={detailSheet.value}
          mounts={mounts}
          loading={detailLoading}
          onClose={() => {
            setSelectedJobId('')
            clearJobParam()
          }}
          onCopy={(text, message) => copyText(text, message)}
        />
      )}

      {loadSheet.mounted && (
        <SubmitLoadSheet
          open={loadSheet.open}
          mounts={mounts}
          onClose={() => setLoadFormOpen(false)}
          onSubmitted={(job) => {
            setJobs((current) => upsertJob(current, job))
            setLoadFormOpen(false)
            toast.success(t('jobs.alerts.submitted', { name: jobListTitle(job) }))
          }}
        />
      )}

      <ConfirmDialog
        open={cancelTarget !== null}
        title={t('jobs.actions.cancel')}
        description={
          cancelTarget
            ? t('jobs.alerts.cancelConfirm', { id: jobId(cancelTarget) })
            : ''
        }
        confirmLabel={t('common.confirm')}
        cancelLabel={t('common.cancel')}
        destructive
        onOpenChange={(open) => {
          if (!open) setCancelTarget(null)
        }}
        onConfirm={() => void confirmCancel()}
        testId="jobs-cancel-confirm"
      />
    </>
  )
}

function FilterSelect({
  value,
  onChange,
  options,
  testId
}: {
  value: string
  onChange(value: string): void
  options: { value: string; label: string }[]
  testId: string
}) {
  return (
    <Select value={value} onValueChange={onChange}>
      <SelectTrigger size="sm" className="w-44" data-testid={testId}>
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          {options.map((option) => (
            <SelectItem key={option.value} value={option.value}>
              {option.label}
            </SelectItem>
          ))}
        </SelectGroup>
      </SelectContent>
    </Select>
  )
}
