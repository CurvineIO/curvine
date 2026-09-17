import { formatBytes, formatBytesPerSec, formatNumber } from '@/utils/format'

/** Shared observability UI constants (time range controls). */

export const TIME_RANGES = [
  { value: '15m', labelKey: 'observability.timeRanges.15m' },
  { value: '1h', labelKey: 'observability.timeRanges.1h' },
  { value: '6h', labelKey: 'observability.timeRanges.6h' },
  { value: '24h', labelKey: 'observability.timeRanges.24h' },
  { value: '3d', labelKey: 'observability.timeRanges.3d' }
] as const

export type TimeRangeValue = (typeof TIME_RANGES)[number]['value']

export const TARGET_FILTERS = [
  { value: 'all', labelKey: 'observability.targetFilters.all' },
  { value: 'master', labelKey: 'observability.targetFilters.master' },
  { value: 'worker', labelKey: 'observability.targetFilters.worker' },
  { value: 'fuse', labelKey: 'observability.targetFilters.fuse' },
  { value: 'transfer', labelKey: 'observability.targetFilters.transfer' },
  { value: 'instance', labelKey: 'observability.targetFilters.instance' }
] as const

export type TargetFilterValue = (typeof TARGET_FILTERS)[number]['value']

/** Step duration per selected time range (matches backend default_step_for_range). */
export function stepForRange(range: string): string {
  switch (range) {
    case '1h':
      return '1m'
    case '6h':
      return '5m'
    case '24h':
      return '15m'
    case '3d':
      return '1h'
    default:
      return '15s'
  }
}

/** Query window length in milliseconds for chart x-axis alignment. */
export function rangeToMs(range: string): number {
  switch (range) {
    case '1h':
      return 60 * 60 * 1000
    case '6h':
      return 6 * 60 * 60 * 1000
    case '24h':
      return 24 * 60 * 60 * 1000
    case '3d':
      return 3 * 24 * 60 * 60 * 1000
    default:
      return 15 * 60 * 1000
  }
}

export function seriesInstanceKey(series: {
  name?: string
  labels?: Record<string, string>
}) {
  const labels = series.labels || {}
  return labels.instance || labels.worker_id || series.name || ''
}

export type SeriesLike = {
  name?: string
  labels?: Record<string, string>
}

/** Strip port suffix for legend/tooltip display (workers/masters). */
export function shortenInstanceLabel(instance: string, workerId?: string): string {
  if (workerId) return `worker-${workerId}`
  if (!instance) return ''
  if (instance === 'active-master') return ''
  return instance.split(':')[0] || instance
}

/** Keep host:port for FUSE instances so multiple mounts on one host stay distinct. */
export function fuseInstanceLabel(instance: string): string {
  if (!instance) return ''
  return instance
}

/** Dropdown label for a FUSE scrape target (always includes port). */
export function fuseFilterOptionLabel(instance: string): string {
  if (!instance) return ''
  const host = shortenInstanceLabel(instance)
  const port = instance.includes(':') ? instance.split(':').pop() : ''
  if (host && port) return `${host}:${port}`
  return instance
}

/** Legend label for FUSE series (preserve port). */
export function fuseSeriesLabel(labels: Record<string, string> = {}): string {
  return fuseInstanceLabel(labels.instance || '')
}

function extractMetricPrefix(name?: string): string {
  if (!name) return ''
  return name.split('(')[0].trim()
}

function seriesMetricName(series: SeriesLike): string {
  const labels = series.labels || {}
  return labels.derived_metric || labels.operation || labels.tag || extractMetricPrefix(series.name) || series.name || ''
}

const WORKER_MULTI_METRIC_PANELS = new Set([
  'worker-storage',
  'worker-blocks',
  'worker-disks',
  'worker-read-block-rate',
  'worker-write-block-rate',
  'worker-transfer-task-water',
  'worker-transfer-concurrency'
])

const OPERATION_LABELED_PANELS = new Set(['operation-rate', 'operation-latency', 'operation-latency-p99'])

/** Ordered FUSE label keys for legend/tooltip disambiguation. */
const FUSE_SERIES_LABEL_KEYS = [
  'opcode',
  'kind',
  'reply_type',
  'request_status',
  'io_type',
  'path_type',
  'cache',
  'status',
  'errno',
  'stage',
  'operation',
  'reason',
  'phase',
  'action',
  'code',
  'metric'
] as const

const WORKER_TRANSFER_SERIES_LABEL_KEYS = [
  'result',
  'reason',
  'state',
  'kind'
] as const

export function workerTransferLabelDetailParts(labels: Record<string, string> = {}): string[] {
  return WORKER_TRANSFER_SERIES_LABEL_KEYS
    .map((key) => labels[key])
    .filter(Boolean)
}

/** Ordered Transfer label keys for legend/tooltip disambiguation. */
const TRANSFER_SERIES_LABEL_KEYS = [
  'backend',
  'operation',
  'source',
  'kind',
  'result',
  'state',
  'reason',
  'lane',
  'type',
  'class',
  'worker_id',
  'status'
] as const

export function transferLabelDetailParts(labels: Record<string, string> = {}): string[] {
  return TRANSFER_SERIES_LABEL_KEYS
    .map((key) => labels[key])
    .filter(Boolean)
}

export function fuseLabelDetailParts(labels: Record<string, string> = {}): string[] {
  return FUSE_SERIES_LABEL_KEYS
    .map((key) => labels[key])
    .filter(Boolean)
}

const FUSE_LABELED_PANELS = new Set([
  'fuse-request-qps',
  'fuse-error-rate',
  'fuse-request-p99',
  'fuse-request-avg',
  'fuse-operation-p99',
  'fuse-io-p99',
  'fuse-io-size',
  'fuse-io-dispatch-p99',
  'fuse-stream-lifecycle-p99',
  'fuse-readdir-p99',
  'fuse-readdir-entries',
  'fuse-setlkw-wait-p99',
  'fuse-stage-p99',
  'fuse-response-write-p99',
  'fuse-active-requests',
  'fuse-io-bytes-rate',
  'fuse-io-requests-rate',
  'fuse-response-bytes-rate',
  'fuse-stream-lifecycle-qps',
  'fuse-user-meta-cache',
  'fuse-meta-cache-invalidations',
  'fuse-node-cache',
  'fuse-negative-entries',
  'fuse-decode-errors',
  'fuse-interrupted-rate',
  'fuse-unsupported-rate',
  'fuse-notify-events',
  'fuse-receive-errors',
  'fuse-reply-enqueue-errors',
  'fuse-response-write-errors',
  'fuse-session-init',
  'fuse-session-shutdown',
  'fuse-stream-io-inflight',
  'fuse-stream-lifecycle-inflight'
])

const TRANSFER_LABELED_PANELS = new Set([
  'transfer-job-flow-rate',
  'transfer-scheduler-rate',
  'transfer-report-rate',
  'transfer-completed-bytes-rate',
  'transfer-anomaly-rate',
  'transfer-job-summary-flush-rate',
  'transfer-store-latency-p99',
  'transfer-store-op-rate',
  'transfer-store-unavailable',
  'transfer-store-unavailable-rate',
  'transfer-store-unavailable-duration-rate',
  'transfer-metadata-latency-p99',
  'transfer-metadata-op-rate',
  'transfer-snapshot-refresh-rate',
  'transfer-snapshot-refresh-p99',
  'transfer-job-summary-flush-p99',
  'transfer-process-cpu',
  'transfer-report-envelope-rate',
  'transfer-requeue-rate',
  'transfer-worker-reject-rate',
  'transfer-worker-inflight-count',
  'transfer-worker-inflight-bytes'
])

const WORKER_TRANSFER_LABELED_PANELS = new Set([
  'worker-transfer-submit-rate',
  'worker-transfer-submit-rejected-rate',
  'worker-transfer-terminal-rate',
  'worker-transfer-failed-rate',
  'worker-transfer-report-rate',
  'worker-transfer-copy-bytes-rate',
  'worker-transfer-execution-p99',
  'worker-transfer-read-p99',
  'worker-transfer-write-p99',
  'worker-transfer-queue-p99'
])

const TRANSFER_PANEL_IDS = new Set([
  ...Array.from(TRANSFER_LABELED_PANELS),
  'transfer-job-water',
  'transfer-task-water',
  'transfer-report-queue',
  'transfer-snapshot-health',
  'transfer-job-summary-water',
  'transfer-process-memory',
  'transfer-process-fd'
])

const WORKER_TRANSFER_PANEL_IDS = new Set([
  ...Array.from(WORKER_TRANSFER_LABELED_PANELS),
  'worker-transfer-task-water',
  'worker-transfer-concurrency'
])

/** Prefer scrape hostname; fall back to opaque worker id. */
export function workerSeriesLabel(labels: Record<string, string> = {}): string {
  const host = shortenInstanceLabel(labels.instance || '')
  if (host) return host
  if (labels.worker_id) return `worker-${labels.worker_id}`
  return ''
}

/** Dropdown label for a worker scrape target. */
export function workerFilterOptionLabel(instance: string, workerId?: number | string): string {
  const host = shortenInstanceLabel(instance)
  if (host && workerId) return `${host} (${workerId})`
  if (host) return host
  if (workerId) return `worker-${workerId}`
  return instance
}

/** When set, only matching worker series are shown (master/cluster/fuse series unchanged). */
export function matchesWorkerFilter(
  workerFilter: string,
  component: string | undefined,
  seriesKey: string,
  labels: Record<string, string> = {}
) {
  if (!workerFilter) return true
  if (component !== 'worker') return true
  if (seriesKey === workerFilter || labels.instance === workerFilter) return true
  if (labels.worker_id && workerFilter === String(labels.worker_id)) return true
  return false
}

/** When set, only matching FUSE series are shown (master/worker/cluster series unchanged). */
export function matchesFuseFilter(
  fuseFilter: string,
  component: string | undefined,
  seriesKey: string,
  labels: Record<string, string> = {}
) {
  if (!fuseFilter) return true
  if (component !== 'fuse') return true
  if (seriesKey === fuseFilter || labels.instance === fuseFilter) return true
  return false
}

/** When set, only matching Transfer series are shown (other components unchanged). */
export function matchesTransferFilter(
  transferFilter: string,
  component: string | undefined,
  seriesKey: string,
  labels: Record<string, string> = {}
) {
  if (!transferFilter) return true
  if (component !== 'transfer') return true
  if (seriesKey === transferFilter || labels.instance === transferFilter) return true
  return false
}

/** Dropdown label for a Transfer scrape target. */
export function transferFilterOptionLabel(instance: string): string {
  return instance || ''
}

/** Legend label for Transfer series (preserve host:port). */
export function transferSeriesLabel(labels: Record<string, string> = {}): string {
  return labels.instance || ''
}

const METRIC_SERIES_PANELS = new Set([
  'eviction-events-rate',
  'eviction-bytes-rate',
  'journal-flush-rate',
  'master-rpc-rate',
  'replication-failure-rate',
  'ttl-skipped-rate',
  'transfer-job-flow-rate',
  'transfer-scheduler-rate',
  'transfer-report-rate',
  'transfer-completed-bytes-rate',
  'transfer-anomaly-rate',
  'transfer-store-op-rate',
  'transfer-store-unavailable-rate',
  'transfer-store-unavailable-duration-rate',
  'transfer-metadata-op-rate',
  'transfer-snapshot-refresh-rate',
  'transfer-job-summary-flush-rate',
  'worker-transfer-submit-rate',
  'worker-transfer-submit-rejected-rate',
  'worker-transfer-terminal-rate',
  'worker-transfer-failed-rate',
  'transfer-report-envelope-rate',
  'transfer-requeue-rate',
  'transfer-worker-reject-rate',
  'worker-transfer-report-rate',
  'worker-transfer-copy-bytes-rate'
])

const METRIC_DISPLAY_OVERRIDES: Record<string, string> = {
  eviction_trigger_count: 'Eviction triggers',
  eviction_files_deleted: 'Files deleted',
  eviction_bytes_freed: 'Bytes freed',
  journal_flush_count: 'Journal flush',
  rpc_request_total_count: 'RPC requests',
  replication_failure_count: 'Replication failures',
  read_count: 'Read ops',
  write_count: 'Write ops',
  read_blocks: 'Block opens',
  write_blocks: 'Block opens',
  transfer_submit_total: 'Submits',
  transfer_terminal_total: 'Terminal',
  transfer_cleanup_purged_total: 'Purged',
  transfer_acquire_total: 'Acquire',
  transfer_planning_total: 'Planning',
  transfer_dispatch_total: 'Dispatch',
  transfer_lease_renew_total: 'Lease renew',
  transfer_stale_retry_total: 'Stale retry',
  transfer_task_report_total: 'Task reports',
  transfer_completed_bytes_total: 'Completed bytes',
  transfer_store_operation_total: 'Store ops',
  transfer_store_unavailable_total: 'Store unavailable',
  transfer_store_unavailable_duration_us_total: 'Unavailable duration',
  transfer_metadata_operation_total: 'Metadata ops',
  transfer_cluster_snapshot_refresh_total: 'Snapshot refresh',
  transfer_job_summary_flush_total: 'Summary flush',
  worker_transfer_task_submit_total: 'Submit',
  worker_transfer_task_submit_rejected_total: 'Submit rejected',
  worker_transfer_task_terminal_total: 'Terminal',
  worker_transfer_task_failed_total: 'Failed',
  worker_transfer_task_report_total: 'Progress reports',
  worker_transfer_task_copy_bytes_total: 'Copy bytes',
  transfer_tasks_pending: 'Tasks pending',
  transfer_tasks_running: 'Tasks running',
  transfer_tasks_completed: 'Tasks completed',
  transfer_tasks_failed: 'Tasks failed',
  transfer_tasks_stale: 'Tasks stale',
  transfer_task_report_rpc_total: 'Report RPCs',
  transfer_task_report_batch_tasks_total: 'Batched reports',
  transfer_requeue_total: 'Task requeues',
  transfer_worker_reject_total: 'Worker rejects',
  transfer_worker_inflight_count: 'Worker in-flight tasks',
  transfer_worker_inflight_bytes: 'Worker in-flight bytes',
  worker_transfer_task_read_duration_us: 'Read duration',
  worker_transfer_task_write_duration_us: 'Write duration',
  worker_transfer_task_queue_duration_us: 'Queue wait',
  process_cpu_seconds_total: 'CPU cores',
  process_resident_memory_bytes: 'Resident memory'
}

function formatMetricLabel(metric: string): string {
  return METRIC_DISPLAY_OVERRIDES[metric] || metric.replace(/_/g, ' ')
}

const CLUSTER_AGG_PANELS = new Set([
  'cluster-blocks-to-delete',
  'cluster-disks',
  'cluster-read-throughput',
  'cluster-write-throughput',
  'cluster-worker-capacity',
  'cluster-iops',
  'cluster-storage-watermark'
])

/** Panels that should render even when the current range has no points yet. */
export const ALWAYS_VISIBLE_PANELS = new Set([
  'cluster-capacity',
  'cluster-worker-capacity',
  'cluster-storage-watermark',
  'cluster-iops',
  'cluster-risks',
  'master-memory',
  'master-process-cpu',
  'master-process-memory',
  'master-active-connections',
  'master-process-fd',
  'metadata-scale',
  'worker-state-counts',
  'master-rpc-qps',
  'operation-latency',
  'operation-latency-p99',
  'cluster-read-throughput',
  'cluster-write-throughput',
  'replication-inflight',
  'replication-staging',
  'journal-queue',
  'journal-lag',
  'journal-term',
  'fs-dir-stalled',
  'fs-dir-probe-acquire',
  'namespace-scale',
  'maintenance-signals',
  'eviction-events-rate',
  'eviction-bytes-rate',
  'ttl-status',
  'ttl-skipped-rate',
  'worker-read-latency',
  'worker-write-latency',
  'worker-read-iops',
  'worker-write-iops',
  'worker-read-block-rate',
  'worker-write-block-rate',
  'worker-process-cpu',
  'worker-process-memory',
  'worker-process-fd',
  'worker-storage-watermark',
  'worker-storage-used-pct',
  'transfer-job-water',
  'transfer-report-queue',
  'transfer-store-unavailable',
  'transfer-snapshot-health',
  'transfer-job-flow-rate',
  'transfer-scheduler-rate',
  'transfer-report-rate',
  'transfer-completed-bytes-rate',
  'transfer-anomaly-rate',
  'transfer-store-latency-p99',
  'transfer-store-op-rate',
  'transfer-store-unavailable-rate',
  'transfer-metadata-latency-p99',
  'transfer-metadata-op-rate',
  'transfer-store-unavailable-duration-rate',
  'transfer-snapshot-refresh-rate',
  'transfer-snapshot-refresh-p99',
  'transfer-job-summary-water',
  'transfer-job-summary-flush-rate',
  'transfer-job-summary-flush-p99',
  'transfer-process-cpu',
  'transfer-process-memory',
  'transfer-process-fd',
  'worker-transfer-task-water',
  'worker-transfer-concurrency',
  'worker-transfer-submit-rate',
  'worker-transfer-submit-rejected-rate',
  'worker-transfer-terminal-rate',
  'worker-transfer-failed-rate',
  'worker-transfer-report-rate',
  'worker-transfer-copy-bytes-rate',
  'worker-transfer-execution-p99',
  'fuse-runtime-handles',
  'fuse-memory',
  'fuse-process-cpu',
  'fuse-process-memory',
  'fuse-process-fd',
  'fuse-kernel-fd-health',
  'fuse-active-requests',
  'fuse-request-qps',
  'fuse-request-p99',
  'fuse-io-bytes-rate',
  'fuse-reply-queue-depth',
  'fuse-stream-write-queue-depth'
])

/** Whether a dashboard panel should render for the current filter state. */
export function shouldShowDashboardPanel(
  panel: { id: string; series?: Array<{ points?: unknown[] }>; error?: string },
  options: { showEmptyPanels?: boolean; dashboardLoading?: boolean } = {}
): boolean {
  const hasPoints = (panel.series || []).some(
    (item) => Array.isArray(item.points) && seriesHasFinitePoints(item.points)
  )
  if (panel.error) return true
  if (hasPoints) return true
  if (options.showEmptyPanels) return true
  if (options.dashboardLoading && ALWAYS_VISIBLE_PANELS.has(panel.id)) return true
  return false
}

export const DASHBOARD_SECTION_DEFS: Record<string, Array<{ id: string; titleKey: string }>> = {
  overview: [
    { id: 'capacity', titleKey: 'observability.sections.capacity' },
    { id: 'performance', titleKey: 'observability.sections.performance' },
    { id: 'risks', titleKey: 'observability.sections.risks' }
  ],
  master: [
    { id: 'trends', titleKey: 'observability.sections.trends' },
    { id: 'process', titleKey: 'observability.sections.process' },
    { id: 'performance', titleKey: 'observability.sections.performance' },
    { id: 'capacity', titleKey: 'observability.sections.capacity' }
  ],
  workers: [
    { id: 'trends', titleKey: 'observability.sections.trends' },
    { id: 'process', titleKey: 'observability.sections.process' },
    { id: 'capacity', titleKey: 'observability.sections.capacity' },
    { id: 'performance', titleKey: 'observability.sections.performance' },
    { id: 'transfer-tasks', titleKey: 'observability.sections.transferTasks' }
  ],
  transfer: [
    { id: 'status', titleKey: 'observability.sections.status' },
    { id: 'flow', titleKey: 'observability.sections.flow' },
    { id: 'store', titleKey: 'observability.sections.store' },
    { id: 'snapshot', titleKey: 'observability.sections.snapshot' },
    { id: 'process', titleKey: 'observability.sections.process' }
  ],
  fuse: [
    { id: 'runtime', titleKey: 'observability.sections.runtime' },
    { id: 'process', titleKey: 'observability.sections.process' },
    { id: 'io', titleKey: 'observability.sections.io' },
    { id: 'latency', titleKey: 'observability.sections.latency' },
    { id: 'queue', titleKey: 'observability.sections.queue' },
    { id: 'cache', titleKey: 'observability.sections.cache' },
    { id: 'health', titleKey: 'observability.sections.health' }
  ]
}

const TAG_LABELED_MASTER_PANELS = new Set([
  'worker-state-counts',
  'metadata-scale',
  'replication-inflight',
  'replication-staging',
  'ttl-inodes'
])

const CLUSTER_RISK_PANELS = new Set(['cluster-risks'])

/** Short label for legend/tooltip; series.name stays the unique backend key. */
export function seriesDisplayName(series: SeriesLike, panelId = ''): string {
  const labels = series.labels || {}
  const component = labels.component || ''

  if (OPERATION_LABELED_PANELS.has(panelId) && labels.operation) {
    const inst = shortenInstanceLabel(labels.instance || '')
    const suffix = panelId === 'operation-latency-p99' ? ' P99' : ''
    return inst ? `${labels.operation}${suffix} · ${inst}` : `${labels.operation}${suffix}`
  }

  if (METRIC_SERIES_PANELS.has(panelId) && labels.metric) {
    const base = formatMetricLabel(labels.metric)
    const inst = component === 'transfer'
      ? transferSeriesLabel(labels)
      : workerSeriesLabel(labels)
    if (component === 'transfer') {
      const detail = transferLabelDetailParts(labels).join(' · ')
      return [base, detail, inst].filter(Boolean).join(' · ')
    }
    if (component === 'worker' && panelId.startsWith('worker-transfer-')) {
      const detail = workerTransferLabelDetailParts(labels).join(' · ')
      return [base, detail, inst].filter(Boolean).join(' · ')
    }
    return inst ? `${base} · ${inst}` : base
  }

  if (CLUSTER_AGG_PANELS.has(panelId)) {
    const metric = labels.metric || seriesMetricName(series)
    return metric ? `${metric} (cluster)` : 'cluster'
  }

  if (CLUSTER_RISK_PANELS.has(panelId) && labels.metric) {
    const base = formatMetricLabel(labels.metric)
    const inst = shortenInstanceLabel(labels.instance || '')
    return inst ? `${base} · ${inst}` : base
  }

  if (TRANSFER_PANEL_IDS.has(panelId) || component === 'transfer') {
    const inst = transferSeriesLabel(labels)
    const detail = transferLabelDetailParts(labels).join(' · ')
    const metric = labels.metric ? formatMetricLabel(labels.metric) : seriesMetricName(series)
    if (detail && inst) return `${detail} · ${inst}`
    if (detail) return detail
    if (metric && inst) return `${metric} · ${inst}`
    if (metric) return metric
    if (inst) return inst
  }

  if (WORKER_TRANSFER_PANEL_IDS.has(panelId) || (component === 'worker' && panelId.startsWith('worker-transfer-'))) {
    const worker = workerSeriesLabel(labels)
    const detail = workerTransferLabelDetailParts(labels).join(' · ')
    const metric = labels.metric ? formatMetricLabel(labels.metric) : seriesMetricName(series)
    if (detail && worker) return `${detail} · ${worker}`
    if (detail) return detail
    if (metric && worker) return `${metric} · ${worker}`
    if (worker) return worker
    if (metric) return metric
  }

  if (TAG_LABELED_MASTER_PANELS.has(panelId) && labels.tag) {
    const inst = shortenInstanceLabel(labels.instance || '')
    const tag = labels.tag
    return inst ? `${tag} · ${inst}` : tag
  }

  if (FUSE_LABELED_PANELS.has(panelId)) {
    const inst = fuseSeriesLabel(labels)
    const detail = fuseLabelDetailParts(labels).join(' · ')
    if (detail && inst) return `${detail} · ${inst}`
    if (detail) return detail
    if (inst) return inst
  }

  if (component === 'fuse' || panelId.startsWith('fuse-')) {
    const inst = fuseSeriesLabel(labels)
    const metric = seriesMetricName(series)
    if (metric && inst) return `${metric} · ${inst}`
    if (inst) return inst
    if (metric) return metric
  }

  if (component === 'worker' || panelId.startsWith('worker-')) {
    const worker = workerSeriesLabel(labels)
    const metric = seriesMetricName(series)
    if ((panelId.includes('block-rate') || labels.type) && labels.type) {
      return worker ? `${labels.type} · ${worker}` : labels.type
    }
    if (WORKER_MULTI_METRIC_PANELS.has(panelId) && metric && worker) {
      return `${metric} · ${worker}`
    }
    if (worker) return worker
  }

  if (panelId === 'cluster-capacity' || panelId === 'cluster-capacity-used-pct' || panelId === 'cluster-blocks-to-delete' || panelId === 'cluster-disks' || component === 'master') {
    const metric = seriesMetricName(series)
    if (metric) return metric
  }

  if (labels.instance) {
    return workerSeriesLabel(labels) || shortenInstanceLabel(labels.instance, labels.worker_id)
  }
  if (labels.worker_id) return `worker-${labels.worker_id}`

  return extractMetricPrefix(series.name) || series.name || ''
}

/** Unique ECharts/summary names when multiple series share the same short label. */
export function uniqueSeriesChartNames(
  seriesList: SeriesLike[],
  panelId = ''
): string[] {
  const baseNames = seriesList.map((series) => seriesDisplayName(series, panelId))
  const counts = new Map<string, number>()
  baseNames.forEach((name) => counts.set(name, (counts.get(name) || 0) + 1))

  const usedByBase = new Map<string, Set<string>>()
  const reserveName = (base: string, candidate: string): string => {
    const used = usedByBase.get(base) || new Set<string>()
    if (!used.has(candidate)) {
      used.add(candidate)
      usedByBase.set(base, used)
      return candidate
    }
    return ''
  }

  return seriesList.map((series, index) => {
    const base = baseNames[index]
    if ((counts.get(base) || 0) <= 1) {
      return base
    }

    const labels = series.labels || {}
    const inst = labels.component === 'fuse'
      ? fuseSeriesLabel(labels)
      : (labels.component === 'transfer'
        ? transferSeriesLabel(labels)
        : (workerSeriesLabel(labels) || shortenInstanceLabel(labels.instance || '')))
    if (inst && !base.includes(inst)) {
      const candidate = reserveName(base, `${base} · ${inst}`)
      if (candidate) return candidate
    }

    const detail = seriesFullDetail(series)
    if (detail && detail !== base) {
      const candidate = reserveName(base, detail)
      if (candidate) return candidate
    }

    if (series.name && series.name !== base) {
      const candidate = reserveName(base, series.name)
      if (candidate) return candidate
    }

    let suffix = 2
    while (suffix < 1000) {
      const candidate = reserveName(base, `${base} (${suffix})`)
      if (candidate) return candidate
      suffix += 1
    }
    return `${base} (${index + 1})`
  })
}

export type SeriesChartLabel = {
  chartName: string
  shortName: string
  fullDetail: string
}

export function resolveSeriesChartLabels(
  seriesList: SeriesLike[],
  panelId = ''
): SeriesChartLabel[] {
  const chartNames = uniqueSeriesChartNames(seriesList, panelId)
  return seriesList.map((series, index) => ({
    chartName: chartNames[index],
    shortName: seriesDisplayName(series, panelId),
    fullDetail: seriesFullDetail(series)
  }))
}

export type SeriesSummaryLike = {
  current?: number | null
  min?: number | null
  max?: number | null
  avg?: number | null
}

export type PointLike = [number, number] | { timestamp_ms?: number; value?: number }

export function seriesPointValue(point: PointLike): number | null {
  if (Array.isArray(point)) {
    const raw = point[1]
    if (raw == null || (typeof raw === 'string' && raw === '')) return null
    const value = Number(raw)
    return Number.isFinite(value) ? value : null
  }
  if (point && typeof point === 'object') {
    const raw = point.value
    if (raw == null || (typeof raw === 'string' && raw === '')) return null
    const value = Number(raw)
    return Number.isFinite(value) ? value : null
  }
  return null
}

export function seriesHasFinitePoints(points: PointLike[] = []): boolean {
  return points.some((point) => seriesPointValue(point) != null)
}

export function computeSeriesSummaryFromPoints(points: PointLike[] = []): SeriesSummaryLike {
  const values: number[] = []
  let lastFinite: number | null = null
  for (const point of points) {
    const value = seriesPointValue(point)
    if (value == null) continue
    values.push(value)
    lastFinite = value
  }
  if (!values.length) {
    return { current: null, min: null, max: null, avg: null }
  }
  const min = Math.min(...values)
  const max = Math.max(...values)
  const sum = values.reduce((total, value) => total + value, 0)
  return {
    current: lastFinite,
    min,
    max,
    avg: sum / values.length
  }
}

export function resolveSeriesSummary(
  summary: SeriesSummaryLike | null | undefined,
  points: PointLike[] = []
): SeriesSummaryLike {
  const hasBackend = summary && ['current', 'min', 'max', 'avg'].some((key) => {
    const value = summary[key as keyof SeriesSummaryLike]
    return value != null && Number.isFinite(Number(value))
  })
  if (hasBackend) return summary
  return computeSeriesSummaryFromPoints(points)
}

/** Full instance or series name for tooltip detail line. */
export function seriesFullDetail(series: SeriesLike): string {
  const labels = series.labels || {}
  const metric = seriesMetricName(series)
  const instance = labels.component === 'fuse'
    ? fuseSeriesLabel(labels)
    : (labels.instance || '')
  if (labels.component === 'transfer') {
    const detail = transferLabelDetailParts(labels).join(' · ')
    if (detail && instance) return `${detail} · ${instance}`
    if (detail) return detail
  }
  if (labels.component === 'fuse') {
    const detail = fuseLabelDetailParts(labels).join(' · ')
    if (detail && instance) return `${detail} · ${instance}`
    if (detail) return detail
  }
  if (labels.operation && instance) {
    return `${labels.operation} · ${instance}`
  }
  if (labels.metric && instance) {
    return `${formatMetricLabel(labels.metric)} · ${instance}`
  }
  if (instance && metric) return `${metric} · ${instance}`
  if (instance) return instance
  return series.name || ''
}

export function truncateLabel(text: string, maxLen = 24): string {
  if (!text || text.length <= maxLen) return text
  return `${text.slice(0, maxLen - 1)}…`
}

export type TargetRow = {
  component?: string
  instance?: string
}

export function resolveInstanceComponent(
  rows: TargetRow[],
  instanceFilter: string
): string | undefined {
  if (!instanceFilter) return undefined
  const row = rows.find((item) => item.instance === instanceFilter)
  return row?.component
}

/** Whether a dashboard group should render for the current target filter. */
export function groupMatchesTargetFilter(
  groupId: string,
  filter: string,
  instanceFilter: string,
  rows: TargetRow[]
): boolean {
  if (filter === 'all') return true
  if (filter === 'master') return groupId === 'master'
  if (filter === 'worker') return groupId === 'workers'
  if (filter === 'fuse') return groupId === 'fuse'
  if (filter === 'transfer') return groupId === 'transfer'
  if (filter === 'instance') {
    const component = resolveInstanceComponent(rows, instanceFilter)
    if (component === 'master') return groupId === 'master'
    if (component === 'worker') return groupId === 'workers'
    if (component === 'fuse') return groupId === 'fuse'
    if (component === 'transfer') return groupId === 'transfer'
    return false
  }
  return true
}

export function matchesTargetFilter(
  filter: string,
  instanceFilter: string,
  component: string | undefined,
  instance: string,
  filteredComponent?: string,
  fuseFilter = ''
) {
  if (component === 'cluster') {
    if (filter === 'all' || filter === 'worker' || filter === 'master' || filter === 'fuse') return true
    // Cluster aggregates sum workers but live under Master; keep them when the
    // selected instance is the active master, hide for per-worker drill-down.
    if (filter === 'instance') {
      return filteredComponent === 'master'
    }
    return false
  }
  if (filter === 'instance') {
    return !!instanceFilter && instance === instanceFilter
  }
  if (filter === 'fuse') {
    if (component !== 'fuse') return false
    if (fuseFilter) return instance === fuseFilter
    return true
  }
  if (filter === 'transfer') {
    if (component !== 'transfer') return false
    return true
  }
  if (filter === 'master' || filter === 'worker') {
    if (component !== filter) return false
    if (instanceFilter) return instance === instanceFilter
    return true
  }
  if (instanceFilter) return instance === instanceFilter
  return true
}

function formatPerSecond(value: number | string) {
  return `${formatNumber(value)}/s`
}

function formatMillis(value: number | string) {
  return `${formatNumber(value)} ms`
}

function formatMicros(value: number | string) {
  return `${formatNumber(value)} us`
}

/** FUSE panels whose values are byte sizes (checked before latency heuristics). */
const FUSE_BYTES_PANELS = new Set([
  'fuse-memory',
  'fuse-io-size',
  'fuse-scrape-bytes'
])

/** Transfer histograms are exported in microseconds (no backend scaling). */
const TRANSFER_LATENCY_PANELS = new Set([
  'transfer-store-latency-p99',
  'transfer-metadata-latency-p99',
  'transfer-snapshot-refresh-p99',
  'transfer-job-summary-flush-p99',
  'worker-transfer-execution-p99',
  'worker-transfer-read-p99',
  'worker-transfer-write-p99',
  'worker-transfer-queue-p99',
  'fs-dir-probe-acquire'
])

/** FUSE panels that display latency in milliseconds. */
const FUSE_LATENCY_PANELS = new Set([
  'fuse-request-p99',
  'fuse-request-avg',
  'fuse-operation-p99',
  'fuse-io-p99',
  'fuse-io-dispatch-p99',
  'fuse-stream-lifecycle-p99',
  'fuse-readdir-p99',
  'fuse-setlkw-wait-p99',
  'fuse-stage-p99',
  'fuse-response-write-p99',
  'fuse-receive-loop-wait',
  'fuse-scrape-duration'
])

/** Panels backed by `counter_rate_panel` whose ids omit rate/qps/iops. */
const COUNTER_RATE_PANELS = new Set([
  'operation-rate',
  'replication-failure-rate',
  'master-rpc-qps',
  'eviction-events-rate',
  'eviction-bytes-rate',
  'journal-flush-rate',
  'master-rpc-rate',
  'ttl-skipped-rate',
  'transfer-job-flow-rate',
  'transfer-scheduler-rate',
  'transfer-report-rate',
  'transfer-completed-bytes-rate',
  'transfer-anomaly-rate',
  'transfer-store-op-rate',
  'transfer-store-unavailable-rate',
  'transfer-store-unavailable-duration-rate',
  'transfer-metadata-op-rate',
  'transfer-snapshot-refresh-rate',
  'transfer-job-summary-flush-rate',
  'transfer-process-cpu',
  'worker-transfer-submit-rate',
  'worker-transfer-submit-rejected-rate',
  'worker-transfer-terminal-rate',
  'worker-transfer-failed-rate',
  'worker-transfer-report-rate',
  'worker-transfer-copy-bytes-rate',
  'master-process-cpu',
  'worker-process-cpu',
  'fuse-process-cpu',
  'worker-read-iops',
  'worker-write-iops',
  'worker-read-block-rate',
  'worker-write-block-rate',
  'fuse-io-bytes-rate',
  'fuse-io-requests-rate',
  'fuse-response-bytes-rate',
  'fuse-stream-lifecycle-qps',
  'fuse-request-qps',
  'fuse-user-meta-cache',
  'fuse-meta-cache-invalidations',
  'fuse-node-cache',
  'fuse-negative-entries',
  'fuse-error-rate',
  'fuse-decode-errors',
  'fuse-interrupted-rate',
  'fuse-unsupported-rate',
  'fuse-notify-events',
  'fuse-receive-errors',
  'fuse-reply-enqueue-errors',
  'fuse-response-write-errors',
  'fuse-session-init',
  'fuse-session-shutdown'
])

export type PanelValueFormatter = (value: number | string) => string

/** Resolve the chart value formatter for a dashboard panel id. */
export function panelValueFormatter(panelId: string): PanelValueFormatter {
  if (panelId.includes('capacity-used-pct')) {
    return (value) => `${formatNumber(value)}%`
  }
  if (
    panelId.includes('watermark')
    || panelId.includes('used-pct')
    || panelId.includes('fd-usage')
    || panelId.includes('process-fd')
  ) {
    return (value) => `${formatNumber(value)}%`
  }
  if (panelId.includes('throughput') || panelId.includes('bytes-rate')) {
    return formatBytesPerSec
  }
  if (TRANSFER_LATENCY_PANELS.has(panelId)) {
    return formatMicros
  }
  if (panelId.includes('worker-read-latency') || panelId.includes('worker-write-latency')) {
    return formatMicros
  }
  if (
    panelId.includes('memory')
    || panelId.includes('capacity')
    || panelId.includes('storage')
    || panelId.includes('rocksdb')
    || panelId.includes('block-size')
    || panelId.includes('process-memory')
    || FUSE_BYTES_PANELS.has(panelId)
  ) {
    return formatBytes
  }
  if (panelId.startsWith('fuse-') && FUSE_LATENCY_PANELS.has(panelId)) {
    return formatMillis
  }
  if (panelId.includes('latency')) {
    return formatMillis
  }
  if (
    COUNTER_RATE_PANELS.has(panelId)
    || panelId.includes('iops')
    || panelId.includes('rate')
    || panelId.includes('qps')
  ) {
    return formatPerSecond
  }
  return formatNumber
}
