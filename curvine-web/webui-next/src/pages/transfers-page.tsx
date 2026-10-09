import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { ChevronLeftIcon, ChevronRightIcon, CircleAlert } from 'lucide-react'

import { ApiError } from '@/api/error'
import { useApi } from '@/api/api-context'
import type { TransferInfo, TransferListCounts, TransferListData } from '@/api/types'
import { ColumnsPicker } from '@/components/columns-picker'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { EmptyNotice, ErrorAlert } from '@/components/data-state'
import { Meter } from '@/components/meter'
import { MetricCard } from '@/components/metric-card'
import { PageHeader } from '@/components/page-header'
import { MonoText, StatusBadge, type StatusTone } from '@/components/resource-table'
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
import { readStoredColumns, writeStoredColumns } from '@/lib/column-visibility'
import { shortJobId } from '@/lib/jobDisplay'
import {
  DEFAULT_VISIBLE_OPTIONAL_TRANSFER_COLUMNS,
  DEFAULT_VISIBLE_TRANSFER_COLUMNS,
  OPTIONAL_TRANSFER_COLUMNS,
  PINNED_TRANSFER_COLUMNS,
  TRANSFER_COLUMNS,
  normalizeTransferColumns,
  orderTransferColumns,
  type TransferColumnDef,
  type TransferColumnKey
} from '@/lib/transferColumns'
import { useT } from '@/i18n'
import { useSlidingSheet } from '@/lib/use-sliding-sheet'
import { useRefreshSignal } from '@/refresh/refresh-context'
import { TransferDiagnosticsSheet } from './transfers/transfer-diagnostics-sheet'
import { SubmitTransferSheet } from './transfers/submit-transfer-sheet'
import {
  TASK_PAGE_DEFAULT,
  TASK_PAGE_MAX,
  TASK_PAGE_STEP,
  buildJobDisplayName,
  canRetry,
  formatFiles,
  isTransferFinal,
  isWithinRange,
  jobListTitle,
  jobListTooltip,
  kindLabel,
  kindValue,
  relativeTime,
  sourcePath,
  targetPath,
  transferBucket,
  transferClass,
  transferDuration,
  transferId,
  transferProgress,
  transferStateLabel,
  upsertTransfer
} from './transfers/utils'

const STATUS_FILTERS = ['all', 'active', 'completed', 'failed', 'canceled'] as const
type StatusFilter = (typeof STATUS_FILTERS)[number]

const LAYOUT_STORAGE_KEY = 'curvine.web.transfers.columns.v1'
const TRANSFER_JOB_ID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

/** Rows per page the list offers; the backend clamps anything above 1000. */
const LIST_PAGE_SIZES = [20, 50, 100]
const LIST_PAGE_DEFAULT = 50
const MOBILE_HIDDEN_COLUMNS = new Set<TransferColumnKey>([
  'owner',
  'kind',
  'path',
  'progress',
  'files',
  'duration',
  'updated'
])

function transferColumnClass(column: TransferColumnDef) {
  const classes = MOBILE_HIDDEN_COLUMNS.has(column.key) ? ['hidden sm:table-cell'] : []
  if (column.key === 'status') classes.push('w-[35%] sm:w-[9%]')
  return classes.join(' ')
}

function transferColumnWidth(column: TransferColumnDef) {
  return column.key === 'status' ? undefined : column.width
}

const TONE_BY_CLASS: Record<string, StatusTone> = {
  healthy: 'positive',
  running: 'info',
  waiting: 'warning',
  failed: 'negative',
  warning: 'warning'
}

export function TransfersPage() {
  const api = useApi()
  const t = useT()
  const toast = useToast()
  const { copy } = useCopy()

  const [transfers, setTransfers] = useState<TransferInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [detailLoading, setDetailLoading] = useState(false)
  const [loadError, setLoadError] = useState<string | null>(null)
  /** Rows the current filter matches, across all pages, as reported by the server. */
  const [total, setTotal] = useState(0)
  /** Per-bucket counts over the whole store, so the tabs and cards stay global. */
  const [counts, setCounts] = useState<TransferListCounts>({})
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(LIST_PAGE_DEFAULT)

  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all')
  const [query, setQuery] = useState('')
  const [jobIdResult, setJobIdResult] = useState<TransferInfo | null>(null)
  const [jobIdLookupLoading, setJobIdLookupLoading] = useState(false)
  const [jobIdLookupError, setJobIdLookupError] = useState<string | null>(null)
  const [pollError, setPollError] = useState<string | null>(null)
  const [kindFilter, setKindFilter] = useState('all')
  const [timeRange, setTimeRange] = useState('all')
  const [failedOnly, setFailedOnly] = useState(false)
  const [polling, setPolling] = useState(true)

  const [selectedTransferId, setSelectedTransferId] = useState('')
  const [submitFormOpen, setSubmitFormOpen] = useState(false)
  const submitSheet = useSlidingSheet(submitFormOpen)
  const [cancelTarget, setCancelTarget] = useState<TransferInfo | null>(null)
  const [retryTarget, setRetryTarget] = useState<TransferInfo | null>(null)
  const [busyTransfers, setBusyTransfers] = useState<Record<string, string>>({})
  const [taskPageSize, setTaskPageSize] = useState(TASK_PAGE_DEFAULT)
  const [taskNextPageToken, setTaskNextPageToken] = useState('')
  const [taskLoadingMore, setTaskLoadingMore] = useState(false)

  const [visibleColumns, setVisibleColumns] = useState<TransferColumnKey[]>(
    DEFAULT_VISIBLE_TRANSFER_COLUMNS
  )
  const visibleColumnList = useMemo(() => orderTransferColumns(visibleColumns), [visibleColumns])
  // The picker only sees the columns it can toggle; State is pinned and always
  // rendered, so it is not offered and not stored as a choice.
  const optionalVisibleColumns = useMemo(
    () => visibleColumns.filter((key) => !PINNED_TRANSFER_COLUMNS.includes(key)),
    [visibleColumns]
  )

  function setColumns(next: TransferColumnKey[]) {
    const normalized = normalizeTransferColumns(next)
    setVisibleColumns(normalized)
    writeStoredColumns(LAYOUT_STORAGE_KEY, normalized)
  }

  useEffect(() => {
    const stored = readStoredColumns(LAYOUT_STORAGE_KEY, TRANSFER_COLUMNS)
    if (stored) setVisibleColumns(normalizeTransferColumns(stored))
  }, [])

  const transfersRef = useRef(transfers)
  transfersRef.current = transfers
  const jobIdResultRef = useRef(jobIdResult)
  jobIdResultRef.current = jobIdResult
  const detailRequestRef = useRef(0)
  const upsertVisibleTransfer = useCallback((next: TransferInfo) => {
    const id = transferId(next)
    const inList = transfersRef.current.some((item) => transferId(item) === id)
    const isSearched = transferId(jobIdResultRef.current) === id
    if (isSearched) {
      setJobIdResult((current) => (current ? upsertTransfer([current], next)[0] : current))
    }
    if (inList || !isSearched) setTransfers((current) => upsertTransfer(current, next))
  }, [])

  const refreshTransfers = useCallback(async () => {
    setLoading(true)
    setLoadError(null)
    try {
      const response = await api.fetchTransfersData({
        page,
        page_size: pageSize,
        kind: kindFilter === 'all' ? undefined : kindFilter,
        bucket: statusFilter === 'all' ? undefined : statusFilter,
        failed_only: failedOnly || undefined
      })
      const payload = response as TransferListData | null
      const items = (Array.isArray(response) ? response : payload?.items || []) as TransferInfo[]
      // A page replaces the previous one: the server already applied the filters
      // and knows the row order, so merging would mix windows.
      setTransfers(items)
      setTotal(Number(payload?.total ?? items.length))
      setCounts(payload?.counts ?? {})
    } catch (error) {
      setLoadError(
        t('transfers.alerts.refreshFailed', {
          error: error instanceof Error ? error.message : String(error)
        })
      )
    } finally {
      setLoading(false)
    }
  }, [api, t, page, pageSize, kindFilter, statusFilter, failedOnly])

  useEffect(() => {
    void refreshTransfers()
  }, [refreshTransfers])

  useRefreshSignal(refreshTransfers)

  useEffect(() => {
    const jobId = query.trim()
    if (!TRANSFER_JOB_ID_RE.test(jobId)) {
      setJobIdResult(null)
      setJobIdLookupLoading(false)
      setJobIdLookupError(null)
      return
    }

    setJobIdResult(null)
    setJobIdLookupLoading(true)
    setJobIdLookupError(null)
    const existing = transfersRef.current.find(
      (transfer) => transferId(transfer).toLowerCase() === jobId.toLowerCase()
    )
    if (existing) {
      setJobIdResult(existing)
      setJobIdLookupLoading(false)
      return
    }

    let cancelled = false
    const timer = window.setTimeout(() => {
      void api
        .fetchTransferStatus(jobId, { page_size: 0 })
        .then((response) => {
          if (cancelled) return
          const transfer = response as TransferInfo
          setJobIdResult(
            transferId(transfer).toLowerCase() === jobId.toLowerCase() ? transfer : null
          )
        })
        .catch((error) => {
          if (cancelled) return
          const notFound =
            error instanceof ApiError &&
            (error.status === 404 || /\bjob\b.*\bnot found\b/i.test(error.message))
          if (!notFound) {
            setJobIdLookupError(
              t('transfers.alerts.lookupFailed', {
                error: error instanceof Error ? error.message : String(error)
              })
            )
          }
        })
        .finally(() => {
          if (!cancelled) setJobIdLookupLoading(false)
        })
    }, 300)

    return () => {
      cancelled = true
      window.clearTimeout(timer)
    }
  }, [api, query, t])

  useEffect(() => {
    if (!polling) {
      setPollError(null)
      return
    }
    let cancelled = false
    let timer = 0
    const tick = async () => {
      let bucketChanged = false
      let pollFailure: unknown
      const active = transfersRef.current.filter((transfer) => !isTransferFinal(transfer))
      const searchedTransfer = jobIdResultRef.current
      if (
        searchedTransfer &&
        !isTransferFinal(searchedTransfer) &&
        !active.some((transfer) => transferId(transfer) === transferId(searchedTransfer))
      ) {
        active.push(searchedTransfer)
      }
      for (const transfer of active) {
        const id = transferId(transfer)
        if (!id) continue
        // Summary only: the list needs counters, not every active job's task list.
        // Loaded task rows are preserved by upsertTransfer's merge.
        try {
          const next = (await api.fetchTransferStatus(id, { page_size: 0 })) as TransferInfo
          bucketChanged ||= transferBucket(next) !== transferBucket(transfer)
          upsertVisibleTransfer(next)
        } catch (error) {
          pollFailure ??= error
        }
      }
      if (cancelled) return
      if (bucketChanged) await refreshTransfers()
      if (cancelled) return
      setPollError(
        pollFailure
          ? t('transfers.alerts.pollFailed', {
              error: pollFailure instanceof Error ? pollFailure.message : String(pollFailure)
            })
          : null
      )
      if (!cancelled) timer = window.setTimeout(() => void tick(), 3000)
    }
    timer = window.setTimeout(() => void tick(), 3000)
    return () => {
      cancelled = true
      window.clearTimeout(timer)
    }
  }, [polling, api, t, refreshTransfers, upsertVisibleTransfer])

  const selectedTransfer = useMemo(() => {
    if (!selectedTransferId) return null
    return (
      transfers.find((item) => transferId(item) === selectedTransferId) ||
      (transferId(jobIdResult) === selectedTransferId ? jobIdResult : null)
    )
  }, [transfers, selectedTransferId, jobIdResult])
  const detailSheet = useSlidingSheet(Boolean(selectedTransfer), selectedTransfer)

  /**
   * The tab badges and the summary cards. The server reports per-bucket counts
   * over the whole store, so they stay global while the table is paged; when a
   * response omits them (older backend, test stub) fall back to the current page.
   */
  const bucketCounts = useMemo(() => {
    if (counts.all !== undefined) {
      return {
        all: Number(counts.all || 0),
        active: Number(counts.active || 0),
        completed: Number(counts.completed || 0),
        failed: Number(counts.failed || 0),
        canceled: Number(counts.canceled || 0)
      }
    }
    return {
      all: transfers.length,
      active: transfers.filter((transfer) => transferBucket(transfer) === 'active').length,
      completed: transfers.filter((transfer) => transferBucket(transfer) === 'completed').length,
      failed: transfers.filter((transfer) => transferBucket(transfer) === 'failed').length,
      canceled: transfers.filter((transfer) => transferBucket(transfer) === 'canceled').length
    }
  }, [counts, transfers])

  /** Status, kind and failed-only are applied by the server; search and time stay local. */
  const filteredTransfers = useMemo(() => {
    const q = query.trim().toLowerCase()
    const isJobIdQuery = TRANSFER_JOB_ID_RE.test(query.trim())
    const jobIdMatchesFilters =
      jobIdResult &&
      transferId(jobIdResult).toLowerCase() === q &&
      (statusFilter === 'all' || transferBucket(jobIdResult) === statusFilter) &&
      (kindFilter === 'all' || kindValue(jobIdResult) === kindFilter) &&
      (!failedOnly || transferBucket(jobIdResult) === 'failed')
    const candidates = isJobIdQuery ? (jobIdMatchesFilters ? [jobIdResult] : []) : transfers

    return candidates.filter((transfer) => {
      if (!isWithinRange(transfer.update_time_ms, timeRange)) return false
      if (!q) return true
      const haystack =
        `${transferId(transfer)} ${buildJobDisplayName(transfer)} ` +
        `${sourcePath(transfer)} ${targetPath(transfer)} ${transfer.submitter || ''} ${transfer.tenant || ''} ` +
        `${transfer.kind || ''} ${transfer.message || ''}`.toLowerCase()
      return haystack.includes(q)
    })
  }, [transfers, query, timeRange, jobIdResult, statusFilter, kindFilter, failedOnly])

  const displayedTotal = TRANSFER_JOB_ID_RE.test(query.trim()) ? filteredTransfers.length : total
  const totalPages = Math.max(1, Math.ceil(displayedTotal / pageSize))

  // A shrinking result set (jobs completing out of a bucket, a narrowed filter)
  // can leave the cursor past the end; pull it back onto a real page.
  useEffect(() => {
    if (page > totalPages) setPage(totalPages)
  }, [page, totalPages])

  const emptyMessage = useMemo(() => {
    if (!query.trim() && Number(counts.all ?? total) === 0) {
      return t('transfers.list.emptyNoTransfers')
    }
    const hints: string[] = []
    if (timeRange !== 'all') {
      hints.push(t('transfers.list.hintTimeRange', { value: timeRange }))
    }
    if (kindFilter !== 'all') hints.push(t('transfers.list.hintKind', { value: kindFilter }))
    if (failedOnly) hints.push(t('transfers.list.hintFailedOnly'))
    const suffix = hints.length
      ? t('transfers.list.activeFilters', { filters: hints.join(', ') })
      : ''
    return t('transfers.list.emptyFiltered', { suffix })
  }, [counts.all, total, query, timeRange, kindFilter, failedOnly, t])

  const setBusy = useCallback((id: string, action: string | null) => {
    if (!id) return
    setBusyTransfers((current) => {
      if (!action) {
        const next = { ...current }
        delete next[id]
        return next
      }
      return { ...current, [id]: action }
    })
  }, [])

  async function copyText(text: string, message: string) {
    if (!text || text === '-') {
      toast.info(t('transfers.copy.nothing'))
      return
    }
    await copy(String(text), message)
  }

  async function refreshTransfer(transfer: TransferInfo, pageSize: number): Promise<TransferInfo | null> {
    const id = transferId(transfer)
    if (!id) return null
    const requestToken = (detailRequestRef.current += 1)
    setDetailLoading(true)
    try {
      const next = (await api.fetchTransferStatus(id, { page_size: pageSize })) as TransferInfo
      if (requestToken !== detailRequestRef.current) return null
      setTaskNextPageToken(next?.next_page_token ? String(next.next_page_token) : '')
      upsertVisibleTransfer(next)
      toast.success(
        t('transfers.alerts.statusRefreshed', {
          id,
          state: transferStateLabel(t, next),
          progress: transferProgress(next)
        })
      )
      return next
    } catch (error) {
      toast.error(t('transfers.alerts.statusFailed', { error: String(error) }))
      return null
    } finally {
      if (requestToken === detailRequestRef.current) setDetailLoading(false)
    }
  }

  async function openDetails(transfer: TransferInfo) {
    const id = transferId(transfer)
    if (!id) return
    setSelectedTransferId(id)
    setTaskPageSize(TASK_PAGE_DEFAULT)
    setTaskNextPageToken('')
    await refreshTransfer(transfer, TASK_PAGE_DEFAULT)
  }

  async function loadMoreTasks() {
    if (!selectedTransfer || taskLoadingMore) return
    if (taskPageSize >= TASK_PAGE_MAX || !taskNextPageToken) return
    setTaskLoadingMore(true)
    const nextSize = Math.min(taskPageSize + TASK_PAGE_STEP, TASK_PAGE_MAX)
    setTaskPageSize(nextSize)
    try {
      await refreshTransfer(selectedTransfer, nextSize)
    } finally {
      setTaskLoadingMore(false)
    }
  }

  function closeDetails() {
    setSelectedTransferId('')
    setTaskNextPageToken('')
    setTaskPageSize(TASK_PAGE_DEFAULT)
  }

  async function confirmCancel() {
    const transfer = cancelTarget
    if (!transfer) return
    const id = transferId(transfer)
    setCancelTarget(null)
    setBusy(id, 'cancel')
    try {
      const next = (await api.cancelTransfer(id)) as TransferInfo
      upsertVisibleTransfer(next)
      toast.success(
        t('transfers.alerts.cancelRequested', { id, state: transferStateLabel(t, next) })
      )
    } catch (error) {
      toast.error(t('transfers.alerts.cancelFailed', { error: String(error) }))
    } finally {
      setBusy(id, null)
    }
  }

  async function confirmRetry() {
    const transfer = retryTarget
    if (!transfer) return
    const id = transferId(transfer)
    setRetryTarget(null)
    setBusy(id, 'retry')
    try {
      const next = (await api.retryTransfer(id)) as TransferInfo
      setTransfers((current) => upsertTransfer(current, next))
      const nextId = transferId(next)
      setSelectedTransferId(nextId)
      toast.success(t('transfers.alerts.retrySubmitted', { id: nextId, original: id }))
      if (nextId) {
        try {
          const refreshed = (await api.fetchTransferStatus(nextId)) as TransferInfo
          setTransfers((current) => upsertTransfer(current, refreshed))
        } catch {
          // The retry already succeeded; a refresh miss is not an error here.
        }
      }
    } catch (error) {
      toast.error(t('transfers.alerts.retryFailed', { error: String(error) }))
    } finally {
      setBusy(id, null)
    }
  }

  /**
   * One cell per optional column. Kept as a switch rather than a map of components
   * because the ordering is driven by the user's column selection, and two of these
   * render badges while one renders a meter, so they cannot be reduced to a string
   * the way the file browser's `columnText` is.
   */
  function renderTransferCell(column: TransferColumnDef, transfer: TransferInfo) {
    switch (column.key) {
      case 'owner':
        return (
          <span className="block truncate text-xs text-muted-foreground" title={jobListTitle(transfer)}>
            {jobListTitle(transfer)}
          </span>
        )
      case 'kind':
        return (
          <StatusBadge
            value={kindLabel(t, transfer)}
            tone={kindValue(transfer) === 'export' ? 'info' : 'positive'}
          />
        )
      case 'path':
        return (
          <div
            className="flex w-full min-w-0 items-center gap-1.5"
            title={`${sourcePath(transfer)}\n→ ${targetPath(transfer)}${transfer.message ? `\n${transfer.message}` : ''}`}
          >
            <span className="truncate text-xs">{sourcePath(transfer)}</span>
            <MonoText className="min-w-0 truncate">→ {targetPath(transfer)}</MonoText>
            {transfer.message && (
              <span className="shrink-0" title={transfer.message}>
                <CircleAlert
                  className="size-3.5 text-warning"
                  data-testid={`transfer-message-${transferId(transfer)}`}
                />
              </span>
            )}
          </div>
        )
      case 'status':
        return (
          <StatusBadge
            value={transferStateLabel(t, transfer)}
            tone={TONE_BY_CLASS[transferClass(transfer)]}
          />
        )
      case 'progress':
        return (
          <div className="flex items-center gap-1.5">
            <Meter percent={transferProgress(transfer)} className="flex-1" />
            <span className="shrink-0 text-xs tabular-nums">{transferProgress(transfer)}%</span>
          </div>
        )
      case 'files':
        return <span className="text-xs tabular-nums">{formatFiles(transfer)}</span>
      case 'duration':
        return (
          <span className="text-xs whitespace-nowrap tabular-nums" title={t('transfers.list.headerDuration')}>
            {transferDuration(transfer)}
          </span>
        )
      case 'updated':
        return (
          <span className="text-xs whitespace-nowrap text-muted-foreground">
            {relativeTime(transfer.update_time_ms)}
          </span>
        )
      default:
        return null
    }
  }

  return (
    <>
      <PageHeader
        eyebrow={t('nav.groupJobs')}
        title={t('transfers.list.title')}
        description={t('transfers.list.subtitle')}
        actions={
          <>
            <Button
              variant="outline"
              size="sm"
              disabled={loading}
              onClick={() => void refreshTransfers()}
              data-testid="transfers-refresh"
            >
              {loading ? <Spinner data-icon="inline-start" /> : null}
              {t('common.refresh')}
            </Button>
            <Button size="sm" onClick={() => setSubmitFormOpen(true)} data-testid="transfers-open-submit">
              {t('transfers.list.submit')}
            </Button>
          </>
        }
      />

      {loadError && <ErrorAlert message={loadError} />}
      {jobIdLookupError && <ErrorAlert message={jobIdLookupError} />}
      {pollError && <ErrorAlert message={pollError} />}

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          label={t('transfers.metrics.transferJobs')}
          value={bucketCounts.all}
          meta={t('transfers.metrics.transferJobsMeta')}
          delta={t('transfers.metrics.total')}
          tone="neutral"
        />
        <MetricCard
          label={t('transfers.metrics.activeJobs')}
          value={bucketCounts.active}
          meta={t('transfers.metrics.activeJobsMeta')}
          delta={t('transfers.metrics.active')}
          tone="good"
        />
        <MetricCard
          label={t('transfers.metrics.failedJobs')}
          value={bucketCounts.failed}
          meta={t('transfers.metrics.failedJobsMeta')}
          delta={t('transfers.metrics.review')}
          tone={bucketCounts.failed ? 'bad' : 'good'}
        />
        <MetricCard
          label={t('transfers.metrics.completedJobs')}
          value={bucketCounts.completed}
          meta={t('transfers.metrics.completedJobsMeta')}
          delta={t('transfers.metrics.done')}
          tone="good"
        />
      </section>

      <section className="flex flex-col gap-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <ToggleGroup
            type="single"
            value={statusFilter}
            onValueChange={(value) => {
              if (!value) return
              setPage(1)
              setStatusFilter(value as StatusFilter)
            }}
            aria-label={t('transfers.filters.ariaStatus')}
            className="max-w-full flex-wrap"
            data-testid="transfers-status-filter"
          >
            {STATUS_FILTERS.map((filter) => (
              <ToggleGroupItem
                key={filter}
                value={filter}
                size="sm"
                data-testid={`transfers-status-${filter}`}
              >
                {t(`transfers.filters.${filter}`)}{' '}
                {filter === 'all' ? bucketCounts.all : bucketCounts[filter]}
              </ToggleGroupItem>
            ))}
          </ToggleGroup>

          <div className="flex flex-wrap items-center gap-2">
            <Input
              value={query}
              onChange={(event) => {
                setPage(1)
                setQuery(event.target.value)
              }}
              placeholder={t('transfers.list.searchPlaceholder')}
              className="w-80"
              aria-label={t('transfers.list.searchAria')}
              data-testid="transfers-search"
            />
            <Select
              value={kindFilter}
              onValueChange={(value) => {
                setPage(1)
                setKindFilter(value)
              }}
            >
              <SelectTrigger size="sm" className="w-36" data-testid="transfers-filter-kind">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  <SelectItem value="all">{t('transfers.filters.allKinds')}</SelectItem>
                  <SelectItem value="load">{t('transfers.kinds.load')}</SelectItem>
                  <SelectItem value="export">{t('transfers.kinds.export')}</SelectItem>
                </SelectGroup>
              </SelectContent>
            </Select>
            <Select
              value={timeRange}
              onValueChange={(value) => {
                setPage(1)
                setTimeRange(value)
              }}
            >
              <SelectTrigger size="sm" className="w-36" data-testid="transfers-filter-time">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  <SelectItem value="all">{t('transfers.filters.anyTime')}</SelectItem>
                  <SelectItem value="1h">{t('transfers.filters.last1h')}</SelectItem>
                  <SelectItem value="24h">{t('transfers.filters.last24h')}</SelectItem>
                  <SelectItem value="7d">{t('transfers.filters.last7d')}</SelectItem>
                </SelectGroup>
              </SelectContent>
            </Select>
            <ColumnsPicker
              columns={OPTIONAL_TRANSFER_COLUMNS}
              visible={optionalVisibleColumns}
              defaultVisible={DEFAULT_VISIBLE_OPTIONAL_TRANSFER_COLUMNS}
              onChange={setColumns}
              testIdPrefix="transfers-columns"
            />
            <label className="flex items-center gap-1.5 text-xs">
              <Checkbox
                checked={polling}
                onCheckedChange={(checked) => setPolling(checked === true)}
                data-testid="transfers-poll-toggle"
              />
              {t('transfers.list.pollActive')}
            </label>
            <label className="flex items-center gap-1.5 text-xs">
              <Checkbox
                checked={failedOnly}
                onCheckedChange={(checked) => {
                  setPage(1)
                  setFailedOnly(checked === true)
                }}
                data-testid="transfers-failed-only"
              />
              {t('transfers.list.failedOnly')}
            </label>
          </div>
        </div>

        <Card className="gap-0 overflow-hidden py-0">
          <Table className="table-fixed">
            <TableHeader className="bg-card">
              <TableRow>
                <TableHead className="w-[35%] sm:w-[14%]">{t('transfers.list.headerJob')}</TableHead>
                {visibleColumnList.map((column) => (
                  <TableHead
                    key={column.key}
                    className={transferColumnClass(column)}
                    style={{ width: transferColumnWidth(column) }}
                  >
                    {t(column.labelKey)}
                  </TableHead>
                ))}
                <TableHead className="w-[30%] text-right sm:w-[10%]">
                  {t('transfers.list.headerActions')}
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredTransfers.map((transfer) => {
                const id = transferId(transfer)
                const busy = busyTransfers[id]
                return (
                  <TableRow
                    key={id}
                    data-state={selectedTransferId === id ? 'selected' : undefined}
                    data-testid={`transfer-row-${id}`}
                  >
                    <TableCell className="w-[35%] sm:w-[14%]">
                      {/* The id heads the row; the display name and submitter stay
                          reachable through the hover title, the copy action and the
                          optional Owner column. */}
                      <button
                        type="button"
                        className="block w-full min-w-0 truncate text-left"
                        title={jobListTooltip(transfer, id)}
                        data-testid={`transfer-copy-${id}`}
                        onClick={() => void copyText(id, t('transfers.copy.transferId'))}
                      >
                        <strong className="hidden truncate font-mono text-xs sm:block">{id || '-'}</strong>
                        <strong className="block truncate font-mono text-xs sm:hidden">
                          {shortJobId(id)}
                        </strong>
                      </button>
                    </TableCell>
                    {visibleColumnList.map((column) => (
                      <TableCell key={column.key} className={transferColumnClass(column)}>
                        {renderTransferCell(column, transfer)}
                      </TableCell>
                    ))}
                    <TableCell className="w-[30%] sm:w-[10%]">
                      <div className="flex flex-col items-stretch gap-1 sm:flex-row sm:items-center sm:justify-end">
                        <Button
                          variant="ghost"
                          size="xs"
                          className="text-primary"
                          onClick={() => void openDetails(transfer)}
                          data-testid={`transfer-details-${id}`}
                        >
                          {t('transfers.actions.details')}
                        </Button>
                        {canRetry(transfer) && (
                          <Button
                            variant="ghost"
                            size="xs"
                            className="text-primary"
                            disabled={Boolean(busy)}
                            onClick={() => setRetryTarget(transfer)}
                            data-testid={`transfer-retry-${id}`}
                          >
                            {busy === 'retry' ? t('transfers.actions.working') : t('transfers.actions.retry')}
                          </Button>
                        )}
                        {!isTransferFinal(transfer) && (
                          <Button
                            variant="ghost"
                            size="xs"
                            className="text-destructive"
                            disabled={Boolean(busy)}
                            onClick={() => setCancelTarget(transfer)}
                            data-testid={`transfer-cancel-${id}`}
                          >
                            {busy === 'cancel' ? t('transfers.actions.working') : t('transfers.actions.cancel')}
                          </Button>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>

          {filteredTransfers.length === 0 &&
            !loading &&
            !jobIdLookupLoading &&
            !jobIdLookupError && (
              <div className="border-t border-border p-2">
                <EmptyNotice title={emptyMessage} />
              </div>
            )}
          {(loading || jobIdLookupLoading) && (
            <div className="flex items-center justify-center gap-2 border-t border-border p-3 text-xs text-muted-foreground">
              <Spinner className="size-3.5" />
              {t('common.loading')}
            </div>
          )}

          <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border p-2">
            <div className="flex items-center gap-2">
              <span className="text-xs text-muted-foreground">
                {t('common.pagination.pageSize')}
              </span>
              <Select
                value={String(pageSize)}
                onValueChange={(value) => {
                  setPage(1)
                  setPageSize(Number(value))
                }}
              >
                <SelectTrigger size="sm" className="w-20" data-testid="transfers-page-size">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    {LIST_PAGE_SIZES.map((size) => (
                      <SelectItem key={size} value={String(size)}>
                        {size}
                      </SelectItem>
                    ))}
                  </SelectGroup>
                </SelectContent>
              </Select>
              <span className="text-xs tabular-nums text-muted-foreground" data-testid="transfers-total">
                {t('common.pagination.total', { count: displayedTotal })}
              </span>
            </div>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                disabled={page <= 1 || loading}
                onClick={() => setPage((current) => Math.max(1, current - 1))}
                data-testid="transfers-prev-page"
              >
                <ChevronLeftIcon data-icon="inline-start" />
                {t('common.pagination.previous')}
              </Button>
              <span className="text-xs tabular-nums text-muted-foreground">
                {t('common.pagination.pageOf', { page, pages: totalPages })}
              </span>
              <Button
                variant="outline"
                size="sm"
                disabled={page >= totalPages || loading}
                onClick={() => setPage((current) => current + 1)}
                data-testid="transfers-next-page"
              >
                {t('common.pagination.next')}
                <ChevronRightIcon data-icon="inline-end" />
              </Button>
            </div>
          </div>
        </Card>
      </section>

      {detailSheet.mounted && detailSheet.value && (
        <TransferDiagnosticsSheet
          key={transferId(detailSheet.value)}
          open={detailSheet.open}
          transfer={detailSheet.value}
          loading={detailLoading}
          busy={Boolean(busyTransfers[transferId(detailSheet.value)])}
          taskNextPageToken={taskNextPageToken}
          taskPageSize={taskPageSize}
          taskLoadingMore={taskLoadingMore}
          onClose={closeDetails}
          onCopy={(text, message) => void copyText(text, message)}
          onRetry={() => setRetryTarget(detailSheet.value ?? null)}
          onLoadMore={() => void loadMoreTasks()}
        />
      )}

      {submitSheet.mounted && (
        <SubmitTransferSheet
          open={submitSheet.open}
          onClose={() => setSubmitFormOpen(false)}
          onSubmitted={(transfer) => {
            setTransfers((current) => upsertTransfer(current, transfer))
            setSubmitFormOpen(false)
            toast.success(t('transfers.alerts.submitted', { name: buildJobDisplayName(transfer) }))
          }}
        />
      )}

      <ConfirmDialog
        open={cancelTarget !== null}
        title={t('transfers.actions.cancel')}
        description={
          cancelTarget
            ? t('transfers.confirm.cancel', { id: transferId(cancelTarget) })
            : ''
        }
        confirmLabel={t('common.confirm')}
        cancelLabel={t('common.back')}
        destructive
        onOpenChange={(open) => {
          if (!open) setCancelTarget(null)
        }}
        onConfirm={() => void confirmCancel()}
        testId="transfers-cancel-confirm"
      />

      <ConfirmDialog
        open={retryTarget !== null}
        title={t('transfers.actions.retry')}
        description={
          retryTarget
            ? t('transfers.confirm.retry', { id: transferId(retryTarget) })
            : ''
        }
        confirmLabel={t('common.confirm')}
        cancelLabel={t('common.back')}
        onOpenChange={(open) => {
          if (!open) setRetryTarget(null)
        }}
        onConfirm={() => void confirmRetry()}
        testId="transfers-retry-confirm"
      />
    </>
  )
}
