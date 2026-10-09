/**
 * ObservabilityPage — React 18 + TypeScript port of
 * webui/src/views/Observability.vue.
 *
 * Behaviour-preserving port:
 * - the toolbar keeps the exact same controls, options, defaults and cascade
 *   (time range, target filter, worker / FUSE / Transfer / instance selects,
 *   "show empty panels" toggle, provider mode + status block, retention /
 *   sparse-data hints, zoom indicator);
 * - `CollectorPanel` is rendered with the same props and in the same place;
 * - dashboard groups come from `dashboard.groups`, are grouped into the
 *   `DASHBOARD_SECTION_DEFS` sections and are filtered with
 *   `shouldShowDashboardPanel` / `groupMatchesTargetFilter`;
 * - the drag-to-zoom range is kept per page (as the Vue `data` did) and fed
 *   back into every chart through `zoomStartMs` / `zoomEndMs`; `null` clears it;
 * - the provider mode switch calls `api.updateObservabilityMode(mode)` and then
 *   reloads names / targets / dashboard / settings;
 * - the dashboard query is `{ range, step: stepForRange(range) }`.
 *
 * React-specific notes:
 * - `CollectorPanel`, `TargetHealthTable` and `DashboardDataSourcesTable` are
 *   the ready-made ports. `Observability.vue` only ever rendered
 *   `CollectorPanel` and `TimeSeriesChart` (its `chartGroups` computed filters
 *   `panel.kind === 'timeseries'` and the template iterates those sections), so
 *   `TargetHealthTable` has no place in this page's layout and is not rendered.
 *   `DashboardDataSourcesTable` *is* rendered for `kind: 'table'` panels, which
 *   the Vue computed dropped on the floor; the backend never emits such panels
 *   today (`data_sources_panel` is `#[allow(dead_code)]`), so this branch cannot
 *   change the rendered output of the Vue page.
 * - `health` and `collectorSettings` are plain state instead of `useAsync`
 *   because `applyMode()` / `onSettingsUpdated()` write them outside of a fetch.
 *   `targets`, `metricNames` and `dashboard` go through `useAsync`.
 * - The refresh signal uses the silent `refresh()` (project convention, see
 *   `refresh-context.tsx`) while the header refresh button uses `reload()`.
 */
import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties } from 'react'

import { ChevronRightIcon } from 'lucide-react'

import { useApi } from '@/api/api-context'
import type {
  DashboardSeries,
  DashboardTableRow,
  ObservabilityDashboard,
  ObservabilityHealth,
  ObservabilitySettings,
  TargetHealth
} from '@/api/types'
import { CollectorPanel } from '@/components/observability/collector-panel'
import { AbsoluteRangePicker } from '@/components/observability/absolute-range-picker'
import { DashboardDataSourcesTable } from '@/components/observability/dashboard-data-sources-table'
import { TimeSeriesChart } from '@/components/observability/time-series-chart'
import { EmptyNotice, ErrorAlert } from '@/components/data-state'
import { PageHeader } from '@/components/page-header'
import { StatusBadge } from '@/components/resource-table'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/components/ui/select'
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'
import { useT, useTe } from '@/i18n'
import {
  formatObservabilityRelative,
  translateGroupTitle,
  translatePanelTitle,
  translateProviderHealth
} from '@/lib/observabilityI18n'
import { useAsync } from '@/lib/use-async'
import { cn } from '@/lib/utils'
import {
  DASHBOARD_SECTION_DEFS,
  TARGET_FILTERS,
  TIME_RANGES,
  fuseFilterOptionLabel,
  groupMatchesTargetFilter,
  matchesFuseFilter,
  matchesTargetFilter,
  matchesTransferFilter,
  matchesWorkerFilter,
  panelValueFormatter,
  rangeToMs,
  resolveInstanceComponent,
  seriesInstanceKey,
  shouldShowDashboardPanel,
  stepForRange,
  transferFilterOptionLabel,
  workerFilterOptionLabel
} from '@/observability/metrics'
import { useRefreshSignal } from '@/refresh/refresh-context'

const SHOW_EMPTY_PANELS_KEY = 'observability-show-empty-panels'

/**
 * Radix `Select` rejects an empty item value, while the Vue template used the
 * empty string for the "no filter" option of the worker / FUSE / Transfer /
 * instance selects. The sentinel is only the rendered encoding — the state
 * stays the empty string the `metrics.ts` matchers expect.
 */
const ALL_OPTION_VALUE = '__all__'

const EMPTY_TARGETS: TargetHealth[] = []
const EMPTY_NAMES: string[] = []

type BadgeTone = 'positive' | 'info' | 'warning' | 'negative' | 'neutral'

interface FilterOption {
  value: string
  label: string
}

interface DetailItem {
  label: string
  value: string
}

/** Vue `timeseriesPanels` entry (see `chartGroups`). */
interface ChartPanel {
  id: string
  title: string
  subtitle: string
  section: string | null
  series: DashboardSeries[]
  error: string
  valueFormatter: (value: number | string) => string
}

/** Vue `buildGroupSections()` result. */
interface ChartSection {
  id: string
  title: string
  panels: ChartPanel[]
}

/** `kind: 'table'` group panel, rendered with `DashboardDataSourcesTable`. */
interface TablePanel {
  id: string
  title: string
  subtitle: string
  rows: DashboardTableRow[]
  error: string
  /** Only used to evaluate `shouldShowDashboardPanel`. */
  series: DashboardSeries[]
}

interface ChartGroup {
  id: string
  title: string
  sections: ChartSection[]
  tablePanels: TablePanel[]
}

function readShowEmptyPanels(): boolean {
  try {
    return localStorage.getItem(SHOW_EMPTY_PANELS_KEY) === '1'
  } catch {
    return false
  }
}

function errorMessage(error: unknown): string {
  return String((error as { message?: string } | null)?.message || error)
}

/** Vue `normalizePoints()`: accepts `[ts, value]` tuples or `{timestamp_ms, value}`. */
function normalizePoints(points: unknown): Array<[number, number]> {
  if (!Array.isArray(points)) return []
  const normalized = (points as unknown[]).map((point): [number, number] | null => {
    if (Array.isArray(point) && point.length >= 2) {
      return [Number(point[0]), Number(point[1])]
    }
    if (point && typeof point === 'object') {
      const record = point as { timestamp_ms?: unknown; value?: unknown }
      return [Number(record.timestamp_ms), Number(record.value)]
    }
    return null
  })
  return normalized.filter(
    (point): point is [number, number] =>
      point !== null && Number.isFinite(point[0]) && Number.isFinite(point[1])
  )
}

function normalizeSummary(summary: DashboardSeries['summary']): DashboardSeries['summary'] {
  if (!summary || typeof summary !== 'object') return null
  return {
    current: summary.current ?? null,
    min: summary.min ?? null,
    max: summary.max ?? null,
    avg: summary.avg ?? null
  }
}

function formatDuration(ms: number): string {
  const value = Number(ms || 0)
  if (!value) return '-'
  if (value < 60_000) return `${Math.round(value / 1000)}s`
  if (value < 3_600_000) return `${Math.round(value / 60_000)}m`
  if (value < 86_400_000) return `${Math.round(value / 3_600_000)}h`
  return `${Math.round(value / 86_400_000)}d`
}

function pointTimestamp(point: unknown): number {
  if (Array.isArray(point)) return Number(point[0])
  if (point && typeof point === 'object') {
    return Number((point as { timestamp_ms?: unknown }).timestamp_ms)
  }
  return NaN
}

function groupChartCount(sections: ChartSection[]): number {
  return sections.reduce((sum, section) => sum + section.panels.length, 0)
}

function providerStatusTone(health: ObservabilityHealth | null): BadgeTone {
  if (!health?.enabled) return 'negative'
  const status = (health.provider_health || '').toLowerCase()
  if (status === 'healthy') return 'positive'
  if (status === 'unhealthy' || status === 'stopped') return 'negative'
  if (status === 'warning') return 'warning'
  return health.fallback_reason ? 'warning' : 'positive'
}

function providerHealthTone(status: string): BadgeTone {
  const normalized = (status || '').toLowerCase()
  if (normalized === 'healthy') return 'positive'
  if (normalized === 'unhealthy' || normalized === 'stopped') return 'negative'
  if (normalized === 'warning') return 'warning'
  return 'positive'
}

export function ObservabilityPage() {
  const t = useT()
  const te = useTe()
  const api = useApi()

  // ---- Toolbar state -------------------------------------------------------
  const [selectedRange, setSelectedRange] = useState('15m')
  const [targetFilter, setTargetFilter] = useState('all')
  const [instanceFilter, setInstanceFilter] = useState('')
  const [workerFilter, setWorkerFilter] = useState('')
  const [fuseFilter, setFuseFilter] = useState('')
  const [transferFilter, setTransferFilter] = useState('')
  const [showEmptyPanels, setShowEmptyPanels] = useState(readShowEmptyPanels)
  const [collapsedGroups, setCollapsedGroups] = useState<Record<string, boolean>>({})
  const [collapsedSections, setCollapsedSections] = useState<Record<string, boolean>>({})

  // ---- Pinned chart controls ----------------------------------------------
  const stickyRef = useRef<HTMLElement | null>(null)
  const [stickyHeight, setStickyHeight] = useState<number | null>(null)

  /**
   * The pinned bar wraps on narrow screens, so its height has to be measured rather than
   * assumed: a zoomed panel starts below it and would otherwise cover the range control.
   * Published as a custom property because the panel is fixed, not a descendant of this
   * element's layout — custom properties still inherit through it.
   */
  useEffect(() => {
    const node = stickyRef.current
    if (!node) return
    const measure = () => setStickyHeight(Math.round(node.getBoundingClientRect().height))
    measure()
    const observer = new ResizeObserver(measure)
    observer.observe(node)
    return () => observer.disconnect()
  }, [])

  // ---- Chart window / drag-to-zoom ----------------------------------------
  const [chartWindowStartMs, setChartWindowStartMs] = useState(0)
  const [chartWindowEndMs, setChartWindowEndMs] = useState(() => Date.now())
  const [chartZoomStartMs, setChartZoomStartMs] = useState(0)
  const [chartZoomEndMs, setChartZoomEndMs] = useState(0)

  // ---- Provider mode -------------------------------------------------------
  const [draftMode, setDraftMode] = useState('builtin')
  const [modeSaving, setModeSaving] = useState(false)
  const [modeError, setModeError] = useState('')
  const [modeNotice, setModeNotice] = useState('')
  const modeSavingRef = useRef(false)

  // ---- Collector health / settings ----------------------------------------
  // Written both by their own fetchers and by `applyMode` /
  // `onSettingsUpdated`, so they stay explicit local state.
  const [health, setHealth] = useState<ObservabilityHealth | null>(null)
  const [healthError, setHealthError] = useState('')
  const [collectorSettings, setCollectorSettings] = useState<ObservabilitySettings | null>(null)

  // Latest-value refs, so the `fetchAll` orchestrator can read the pre-fetch
  // values without depending on (and re-creating itself for) every state change.
  const healthRef = useRef<ObservabilityHealth | null>(health)
  healthRef.current = health
  const dashboardRef = useRef<ObservabilityDashboard | null>(null)

  // ---- Data sources --------------------------------------------------------
  const loadTargets = useCallback(() => api.fetchObservabilityTargets(), [api])
  const targetsState = useAsync(loadTargets, [loadTargets])
  const targets = targetsState.error ? EMPTY_TARGETS : targetsState.data ?? EMPTY_TARGETS
  const targetsLoading = targetsState.loading
  const targetsError = targetsState.error ?? ''

  const loadNames = useCallback(() => api.fetchObservabilityNames(), [api])
  const namesState = useAsync(loadNames, [loadNames])
  const metricNames = namesState.error ? EMPTY_NAMES : namesState.data ?? EMPTY_NAMES
  const namesError = namesState.error ?? ''

  // Re-created when the range changes, which is exactly when the Vue watcher
  // re-ran `fetchDashboard()` (useAsync reloads whenever the loader changes).
  const loadDashboard = useCallback(
    () =>
      api.fetchObservabilityDashboard({
        range: selectedRange,
        step: stepForRange(selectedRange)
      }),
    [api, selectedRange]
  )
  const dashboardState = useAsync(loadDashboard, [loadDashboard])
  const dashboard: ObservabilityDashboard | null = dashboardState.error
    ? null
    : dashboardState.data
  const dashboardLoading = dashboardState.loading
  const dashboardError = dashboardState.error ?? ''
  dashboardRef.current = dashboard

  const loadCollectorSettings = useCallback(async () => {
    try {
      const settings = await api.fetchObservabilitySettings()
      setCollectorSettings(settings)
      return settings
    } catch {
      setCollectorSettings(null)
      return null
    }
  }, [api])

  /** Vue `fetchHealth()`; returns the fetched payload for `fetchAll()`. */
  const fetchHealth = useCallback(async (): Promise<ObservabilityHealth | null> => {
    try {
      const next = await api.fetchObservabilityHealth()
      setHealth(next)
      setHealthError('')
      if (!modeSavingRef.current && next?.mode) setDraftMode(next.mode)
      return next
    } catch (err) {
      setHealth(null)
      setHealthError(errorMessage(err))
      return null
    }
  }, [api])

  // Vue `created()` fetched settings + health imperatively; the other sources
  // load themselves through `useAsync`.
  useEffect(() => {
    void fetchHealth()
    void loadCollectorSettings()
  }, [fetchHealth, loadCollectorSettings])

  // ---- Derived toolbar options --------------------------------------------
  const timeRanges = useMemo(
    () => TIME_RANGES.map((item) => ({ ...item, label: t(item.labelKey) })),
    [t]
  )
  const targetFilters = useMemo(
    () => TARGET_FILTERS.map((item) => ({ ...item, label: t(item.labelKey) })),
    [t]
  )

  const filteredInstanceComponent = useMemo(
    () => resolveInstanceComponent(targets, instanceFilter),
    [targets, instanceFilter]
  )

  /** Vue `matchesRowFilter()`. */
  const matchesRowFilter = useCallback(
    (target: TargetHealth) => {
      const instance = target.instance || ''
      const workerLabels: Record<string, string> = { instance: target.instance }
      if (target.worker_id != null) workerLabels.worker_id = String(target.worker_id)
      if (
        !matchesTargetFilter(
          targetFilter,
          instanceFilter,
          target.component,
          instance,
          filteredInstanceComponent,
          fuseFilter
        )
      ) {
        return false
      }
      return (
        matchesWorkerFilter(workerFilter, target.component, instance, workerLabels) &&
        matchesFuseFilter(fuseFilter, target.component, instance, { instance: target.instance }) &&
        matchesTransferFilter(transferFilter, target.component, instance, {
          instance: target.instance
        })
      )
    },
    [targetFilter, instanceFilter, filteredInstanceComponent, fuseFilter, workerFilter, transferFilter]
  )

  const filteredTargets = useMemo(
    () => targets.filter((target) => matchesRowFilter(target)),
    [targets, matchesRowFilter]
  )

  const instanceOptions = useMemo(() => {
    const instances = new Set<string>()
    targets.forEach((target) => {
      if (target.instance) instances.add(target.instance)
    })
    return Array.from(instances).sort()
  }, [targets])

  const workerOptions = useMemo<FilterOption[]>(
    () =>
      targets
        .filter((target) => target.component === 'worker' && target.instance)
        .map((target) => ({
          value: target.instance,
          label: workerFilterOptionLabel(target.instance, target.worker_id ?? undefined)
        }))
        .sort((left, right) => left.label.localeCompare(right.label)),
    [targets]
  )

  const fuseOptions = useMemo<FilterOption[]>(
    () =>
      targets
        .filter((target) => target.component === 'fuse' && target.instance)
        .map((target) => ({
          value: target.instance,
          label: fuseFilterOptionLabel(target.instance)
        }))
        .sort((left, right) => left.label.localeCompare(right.label)),
    [targets]
  )

  const transferOptions = useMemo<FilterOption[]>(
    () =>
      targets
        .filter((target) => target.component === 'transfer' && target.instance)
        .map((target) => ({
          value: target.instance,
          label: transferFilterOptionLabel(target.instance)
        }))
        .sort((left, right) => left.label.localeCompare(right.label)),
    [targets]
  )

  const showWorkerFilter = targetFilter === 'all' || targetFilter === 'worker'
  const showFuseFilter = targetFilter === 'all' || targetFilter === 'fuse'
  const showTransferFilter = targetFilter === 'all' || targetFilter === 'transfer'
  const needsInstanceSelection = targetFilter === 'instance' && !instanceFilter

  // Vue `watch: { instanceOptions, workerOptions, fuseOptions, transferOptions }`
  // — one effect per watched option list, skipped on mount (the guard makes the
  // mount run a no-op, exactly like a non-immediate Vue watcher).
  useEffect(() => {
    if (targetFilter !== 'instance') return
    if (instanceFilter && instanceOptions.includes(instanceFilter)) return
    setInstanceFilter(instanceOptions[0] || '')
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [instanceOptions])

  useEffect(() => {
    if (!showWorkerFilter) {
      setWorkerFilter('')
      return
    }
    if (!workerFilter) return
    if (!workerOptions.some((item) => item.value === workerFilter)) setWorkerFilter('')
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workerOptions])

  useEffect(() => {
    if (!showFuseFilter) {
      setFuseFilter('')
      return
    }
    if (!fuseFilter) return
    if (!fuseOptions.some((item) => item.value === fuseFilter)) setFuseFilter('')
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fuseOptions])

  useEffect(() => {
    if (!showTransferFilter) {
      setTransferFilter('')
      return
    }
    if (!transferFilter) return
    if (!transferOptions.some((item) => item.value === transferFilter)) setTransferFilter('')
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [transferOptions])

  // Vue `watch: { targetFilter }` — cascade that resets downstream filters.
  useEffect(() => {
    if (targetFilter !== 'instance') {
      setInstanceFilter('')
    } else if (!instanceFilter || !instanceOptions.includes(instanceFilter)) {
      setInstanceFilter(instanceOptions[0] || '')
    }
    if (targetFilter === 'master' || targetFilter === 'instance') setWorkerFilter('')
    if (targetFilter === 'fuse' || targetFilter === 'instance') setFuseFilter('')
    if (targetFilter !== 'all' && targetFilter !== 'transfer') setTransferFilter('')
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [targetFilter])

  // Vue `watch: { selectedRange }` — reset the drag-to-zoom window; the
  // dashboard refetch itself is driven by the `loadDashboard` identity change.
  useEffect(() => {
    setChartZoomStartMs(0)
    setChartZoomEndMs(0)
  }, [selectedRange])

  // Vue `watch: { showEmptyPanels }` — persisted preference.
  useEffect(() => {
    try {
      localStorage.setItem(SHOW_EMPTY_PANELS_KEY, showEmptyPanels ? '1' : '0')
    } catch {
      // Storage can be unavailable (private mode); the preference is best-effort.
    }
  }, [showEmptyPanels])

  // Vue `fetchDashboard()` called `syncChartWindowEnd()` + both ensure* helpers
  // on every successful dashboard payload.
  useEffect(() => {
    const dashWindow = dashboard?.window
    const fromStart = Number(dashWindow?.start_ms)
    const fromEnd = Number(dashWindow?.end_ms)
    const fromHealth = Number(healthRef.current?.last_collect_ms)
    if (Number.isFinite(fromStart) && fromStart > 0) {
      setChartWindowStartMs(fromStart)
    } else if (Number.isFinite(fromEnd) && fromEnd > 0) {
      setChartWindowStartMs(fromEnd - rangeToMs(selectedRange))
    }
    const endCandidate = Number.isFinite(fromEnd) && fromEnd > 0 ? fromEnd : fromHealth
    setChartWindowEndMs(
      Number.isFinite(endCandidate) && endCandidate > 0 ? endCandidate : Date.now()
    )
    if (targetFilter === 'instance' && (!instanceFilter || !instanceOptions.includes(instanceFilter))) {
      setInstanceFilter(instanceOptions[0] || '')
    }
    if (showWorkerFilter && workerFilter && !workerOptions.some((item) => item.value === workerFilter)) {
      setWorkerFilter('')
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dashboard, selectedRange])

  // ---- Data coverage / hints ----------------------------------------------
  const dataCoverageMs = useMemo(() => {
    const groups = dashboard?.groups
    if (!Array.isArray(groups)) return 0
    let minTs = Infinity
    let maxTs = -Infinity
    groups.forEach((group) => {
      ;(group.panels || []).forEach((panel) => {
        if (panel.kind !== 'timeseries') return
        ;(panel.series || []).forEach((series) => {
          ;(series.points || []).forEach((point) => {
            const ts = pointTimestamp(point)
            if (!Number.isFinite(ts)) return
            minTs = Math.min(minTs, ts)
            maxTs = Math.max(maxTs, ts)
          })
        })
      })
    })
    if (!Number.isFinite(minTs) || !Number.isFinite(maxTs) || maxTs <= minTs) return 0
    return maxTs - minTs
  }, [dashboard])

  const modeDirty = health?.mode ? draftMode !== health.mode : false
  const hasProviderMessages = Boolean(modeError || modeNotice || health?.fallback_reason)

  const retentionRangeHint = useMemo(() => {
    if (!health || health.active_provider === 'prometheus') return ''
    const windowMs = rangeToMs(selectedRange)
    if (windowMs <= health.retention_ms) return ''
    return t('observability.hints.retentionExceeded', {
      retention: formatDuration(health.retention_ms)
    })
  }, [health, selectedRange, t])

  const sparseDataHint = useMemo(() => {
    const dashWindow = dashboard?.window
    if (!dashWindow || !dataCoverageMs) return ''
    const querySpan = Number(dashWindow.end_ms) - Number(dashWindow.start_ms)
    if (querySpan <= 0 || dataCoverageMs >= querySpan * 0.25) return ''
    const rangeLabel =
      timeRanges.find((item) => item.value === (dashWindow.range || selectedRange))?.label ||
      dashWindow.range ||
      selectedRange
    return t('observability.hints.sparseData', {
      coverage: formatDuration(dataCoverageMs),
      range: rangeLabel
    })
  }, [dashboard, dataCoverageMs, selectedRange, timeRanges, t])

  const chartZoomActive =
    Number.isFinite(chartZoomStartMs) &&
    chartZoomStartMs > 0 &&
    Number.isFinite(chartZoomEndMs) &&
    chartZoomEndMs > chartZoomStartMs

  const providerStatusDetails = useMemo<DetailItem[]>(() => {
    if (!health) return []
    const details: DetailItem[] = []
    const active = health.active_provider || 'builtin'
    const configured = health.mode || ''
    if (configured && configured !== active) {
      details.push({ label: t('observability.provider.configured'), value: configured })
    }
    if (
      health.prometheus_url &&
      (active === 'prometheus' || configured === 'prometheus' || configured === 'auto')
    ) {
      details.push({ label: t('observability.provider.endpoint'), value: health.prometheus_url })
    }
    if (health.last_collect_ms) {
      details.push({
        label: t('observability.provider.lastCollect'),
        value: formatObservabilityRelative(t, health.last_collect_ms)
      })
    }
    if (health.last_discovery_ms) {
      details.push({
        label: t('observability.provider.lastDiscovery'),
        value: formatObservabilityRelative(t, health.last_discovery_ms)
      })
    }
    return details
  }, [health, t])

  /** Vue `filterDashboardSeries()`. */
  const filterDashboardSeries = useCallback(
    (series: DashboardSeries[]): DashboardSeries[] =>
      (series || [])
        .map((item) => ({
          name: item.name,
          labels: item.labels || {},
          points: normalizePoints(item.points),
          summary: normalizeSummary(item.summary)
        }))
        .filter((item) => {
          const labels = item.labels || {}
          const seriesKey = seriesInstanceKey(item)
          if (
            !matchesTargetFilter(
              targetFilter,
              instanceFilter,
              labels.component,
              seriesKey,
              filteredInstanceComponent,
              fuseFilter
            )
          ) {
            return false
          }
          return (
            matchesWorkerFilter(workerFilter, labels.component, seriesKey, labels) &&
            matchesFuseFilter(fuseFilter, labels.component, seriesKey, labels) &&
            matchesTransferFilter(transferFilter, labels.component, seriesKey, labels)
          )
        }),
    [targetFilter, instanceFilter, filteredInstanceComponent, fuseFilter, workerFilter, transferFilter]
  )

  /** Vue `buildGroupSections()`. */
  const buildGroupSections = useCallback(
    (groupId: string, panels: ChartPanel[]): ChartSection[] => {
      const defs = DASHBOARD_SECTION_DEFS[groupId] || []
      const buckets = new Map<string, ChartPanel[]>()
      panels.forEach((panel) => {
        const sectionId = panel.section || 'other'
        const bucket = buckets.get(sectionId)
        if (bucket) bucket.push(panel)
        else buckets.set(sectionId, [panel])
      })
      const sections: ChartSection[] = defs
        .map((def) => ({
          id: def.id,
          title: t(def.titleKey),
          panels: buckets.get(def.id) || []
        }))
        .filter((section) => section.panels.length > 0)
      const otherPanels = buckets.get('other') || []
      if (otherPanels.length) {
        sections.push({
          id: 'other',
          title: t('observability.sections.other'),
          panels: otherPanels
        })
      }
      return sections
    },
    [t]
  )

  /** Vue `chartGroups`. */
  const chartGroups = useMemo<ChartGroup[]>(() => {
    const groups = Array.isArray(dashboard?.groups) ? dashboard.groups : []
    return groups
      .map((group) => {
        const timeseriesPanels = (group.panels || [])
          .filter((panel) => panel.kind === 'timeseries')
          .map((panel) => ({
            id: panel.id,
            title: panel.title,
            subtitle: panel.subtitle,
            section: panel.section || null,
            series: filterDashboardSeries(panel.series || []),
            error: panel.error || '',
            valueFormatter: panelValueFormatter(panel.id)
          }))
          .filter((panel) => shouldShowDashboardPanel(panel, { showEmptyPanels, dashboardLoading }))
        const tablePanels = (group.panels || [])
          .filter((panel) => panel.kind === 'table')
          .map((panel) => ({
            id: panel.id,
            title: panel.title,
            subtitle: panel.subtitle,
            rows: panel.rows || [],
            error: panel.error || '',
            series: panel.series || []
          }))
          .filter((panel) => shouldShowDashboardPanel(panel, { showEmptyPanels, dashboardLoading }))
        return {
          id: group.id,
          title: group.title,
          sections: buildGroupSections(group.id, timeseriesPanels),
          tablePanels
        }
      })
      .filter((group) => {
        if (
          !groupMatchesTargetFilter(group.id, targetFilter, instanceFilter, targets)
        ) {
          return false
        }
        return group.sections.length > 0
      })
  }, [
    dashboard,
    showEmptyPanels,
    dashboardLoading,
    filterDashboardSeries,
    buildGroupSections,
    targetFilter,
    instanceFilter,
    targets
  ])

  // ---- Fetch orchestration -------------------------------------------------
  /**
   * Vue `fetchAll()`: refresh every source, then the dashboard when the
   * collector produced new samples (or nothing has been loaded yet). `silent`
   * is used by the refresh signal so an auto-refresh tick does not flash the
   * loading skeletons over data the user is already looking at.
   */
  const fetchAll = useCallback(
    async (silent = false) => {
      const prevCollectMs = healthRef.current?.last_collect_ms
      const hasDashboard = dashboardRef.current !== null
      const [, , nextHealth] = await Promise.all([
        loadCollectorSettings(),
        silent ? namesState.refresh() : namesState.reload(),
        fetchHealth(),
        silent ? targetsState.refresh() : targetsState.reload()
      ])
      const collectChanged = nextHealth?.last_collect_ms !== prevCollectMs
      if (collectChanged || !hasDashboard) {
        await (silent ? dashboardState.refresh() : dashboardState.reload())
      }
    },
    [
      loadCollectorSettings,
      namesState.refresh,
      namesState.reload,
      fetchHealth,
      targetsState.refresh,
      targetsState.reload,
      dashboardState.refresh,
      dashboardState.reload
    ]
  )

  // Latest-ref indirection keeps the refresh subscription stable: a changing
  // callback identity would re-run `useRefreshSignal`'s effect every render.
  const fetchAllRef = useRef(fetchAll)
  fetchAllRef.current = fetchAll
  const refreshAll = useCallback(() => fetchAllRef.current(true), [])
  useRefreshSignal(refreshAll)

  // ---- Actions -------------------------------------------------------------
  const onChartZoomChange = useCallback((range: { startMs: number; endMs: number } | null) => {
    if (!range) {
      setChartZoomStartMs(0)
      setChartZoomEndMs(0)
      return
    }
    const start = Number(range.startMs)
    const end = Number(range.endMs)
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) return
    setChartZoomStartMs(start)
    setChartZoomEndMs(end)
  }, [])

  const resetChartZoom = useCallback(() => {
    setChartZoomStartMs(0)
    setChartZoomEndMs(0)
  }, [])

  /**
   * The window the charts actually draw. Everything here is client-side: the dashboard
   * endpoint queries by preset duration, so a dragged or typed range narrows the data that
   * preset loaded rather than re-querying.
   */
  const loadedWindow = useMemo(
    () =>
      chartWindowStartMs > 0 && chartWindowEndMs > chartWindowStartMs
        ? { startMs: chartWindowStartMs, endMs: chartWindowEndMs }
        : null,
    [chartWindowEndMs, chartWindowStartMs]
  )
  const displayedWindow = chartZoomActive
    ? { startMs: chartZoomStartMs, endMs: chartZoomEndMs }
    : loadedWindow

  const applyAbsoluteRange = useCallback(
    (range: { startMs: number; endMs: number }) => {
      setChartZoomStartMs(range.startMs)
      setChartZoomEndMs(range.endMs)
    },
    []
  )

  const onSettingsUpdated = useCallback(
    async (settings: ObservabilitySettings) => {
      setCollectorSettings(settings)
      await Promise.all([
        fetchHealth(),
        dashboardState.reload(),
        namesState.reload()
      ])
    },
    [fetchHealth, dashboardState.reload, namesState.reload]
  )

  async function applyMode() {
    if (!modeDirty) return
    modeSavingRef.current = true
    setModeSaving(true)
    setModeError('')
    setModeNotice('')
    try {
      const nextHealth = await api.updateObservabilityMode(draftMode)
      setHealth(nextHealth)
      setHealthError('')
      setModeNotice(t('observability.messages.modeSet', { mode: draftMode }))
      await Promise.all([
        namesState.reload(),
        targetsState.reload(),
        dashboardState.reload(),
        loadCollectorSettings()
      ])
    } catch (err) {
      setModeError(errorMessage(err))
      if (health?.mode) setDraftMode(health.mode)
    } finally {
      modeSavingRef.current = false
      setModeSaving(false)
    }
  }

  function providerLabel(value: string): string {
    const key = `observability.provider.${String(value || 'builtin').toLowerCase()}`
    return te(key) ? t(key) : value
  }

  function toggleGroup(groupId: string) {
    setCollapsedGroups((current) => ({ ...current, [groupId]: !current[groupId] }))
  }

  function toggleSection(groupId: string, sectionId: string) {
    const key = `${groupId}:${sectionId}`
    setCollapsedSections((current) => ({ ...current, [key]: !current[key] }))
  }

  const toolbarLabelClass = 'text-xs font-semibold whitespace-nowrap text-muted-foreground'
  const collapseLabelClass =
    'shrink-0 rounded-md bg-muted px-2 py-1 text-[11px] font-bold tracking-wide text-muted-foreground uppercase max-[900px]:hidden'
  const emptyPanelMessage = t('observability.collector.emptyPanel')

  return (
    <div
      className="flex flex-col gap-4"
      style={
        stickyHeight === null
          ? undefined
          : ({ '--panel-zoom-top-inset': `${stickyHeight}px` } as CSSProperties)
      }
    >
      <PageHeader
        eyebrow={t('nav.groupObservability')}
        title={t('nav.observability')}
        description={t('nav.observabilitySubtitle')}
        actions={
          <Button
            variant="outline"
            size="sm"
            onClick={() => void fetchAll(false)}
            data-testid="observability-refresh"
          >
            {t('common.refresh')}
          </Button>
        }
      />

      <section
        className="rounded-xl border border-border bg-card p-4"
        data-testid="observability-toolbar"
      >
        <div className="flex flex-col gap-2.5">
          <div className="order-2 flex w-full flex-wrap items-center gap-2.5 border-t border-border pt-3">
            <div className="flex flex-wrap items-center gap-2.5">
              <span className={toolbarLabelClass}>{t('observability.toolbar.targetFilter')}</span>
              <ToggleGroup
                type="single"
                value={targetFilter}
                onValueChange={(value) => {
                  if (value) setTargetFilter(value)
                }}
                data-testid="observability-target-filter"
              >
                {targetFilters.map((item) => (
                  <ToggleGroupItem
                    key={item.value}
                    value={item.value}
                    size="sm"
                    data-testid={`observability-target-${item.value}`}
                  >
                    {item.label}
                  </ToggleGroupItem>
                ))}
              </ToggleGroup>
            </div>

            {showWorkerFilter && (
              <div className="flex flex-wrap items-center gap-2.5">
                <span className={toolbarLabelClass}>{t('observability.toolbar.worker')}</span>
                <Select
                  value={workerFilter === '' ? ALL_OPTION_VALUE : workerFilter}
                  onValueChange={(value) => setWorkerFilter(value === ALL_OPTION_VALUE ? '' : value)}
                >
                  <SelectTrigger
                    className="min-w-[210px] max-w-[340px]"
                    data-testid="observability-filter-worker"
                  >
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value={ALL_OPTION_VALUE}>
                      {t('observability.toolbar.allWorkers')}
                    </SelectItem>
                    {workerOptions.map((item) => (
                      <SelectItem key={item.value} value={item.value}>
                        {item.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}

            {showFuseFilter && (
              <div className="flex flex-wrap items-center gap-2.5">
                <span className={toolbarLabelClass}>{t('observability.toolbar.fuse')}</span>
                <Select
                  value={fuseFilter === '' ? ALL_OPTION_VALUE : fuseFilter}
                  onValueChange={(value) => setFuseFilter(value === ALL_OPTION_VALUE ? '' : value)}
                >
                  <SelectTrigger
                    className="min-w-[210px] max-w-[340px]"
                    data-testid="observability-filter-fuse"
                  >
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value={ALL_OPTION_VALUE}>
                      {t('observability.toolbar.allFuseMounts')}
                    </SelectItem>
                    {fuseOptions.map((item) => (
                      <SelectItem key={item.value} value={item.value}>
                        {item.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}

            {showTransferFilter && (
              <div className="flex flex-wrap items-center gap-2.5">
                <span className={toolbarLabelClass}>{t('observability.toolbar.transfer')}</span>
                <Select
                  value={transferFilter === '' ? ALL_OPTION_VALUE : transferFilter}
                  onValueChange={(value) =>
                    setTransferFilter(value === ALL_OPTION_VALUE ? '' : value)
                  }
                >
                  <SelectTrigger
                    className="min-w-[210px] max-w-[340px]"
                    data-testid="observability-filter-transfer"
                  >
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value={ALL_OPTION_VALUE}>
                      {t('observability.toolbar.allTransferInstances')}
                    </SelectItem>
                    {transferOptions.map((item) => (
                      <SelectItem key={item.value} value={item.value}>
                        {item.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}

            {targetFilter === 'instance' && (
              <div className="flex flex-wrap items-center gap-2.5">
                <span className={toolbarLabelClass}>{t('observability.toolbar.instance')}</span>
                <Select
                  value={instanceFilter === '' ? ALL_OPTION_VALUE : instanceFilter}
                  onValueChange={(value) => setInstanceFilter(value === ALL_OPTION_VALUE ? '' : value)}
                >
                  <SelectTrigger
                    className="min-w-[210px] max-w-[340px]"
                    data-testid="observability-filter-instance"
                  >
                    <SelectValue placeholder={t('observability.toolbar.selectInstance')} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value={ALL_OPTION_VALUE}>
                      {t('observability.toolbar.selectInstance')}
                    </SelectItem>
                    {instanceOptions.map((instance) => (
                      <SelectItem key={instance} value={instance}>
                        {instance}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}

            <label className="ml-auto flex items-center gap-2 text-xs whitespace-nowrap text-muted-foreground">
              <Checkbox
                checked={showEmptyPanels}
                onCheckedChange={(checked) => setShowEmptyPanels(checked === true)}
                data-testid="observability-show-empty-panels"
              />
              {t('observability.toolbar.showEmptyPanels')}
            </label>
          </div>

          <div className="order-1 grid gap-3 md:grid-cols-[minmax(0,1fr)_auto] md:items-center md:gap-x-8">
            <div className="flex min-w-0 flex-wrap items-center gap-3">
              <span className={toolbarLabelClass}>{t('observability.toolbar.providerMode')}</span>
              <Select
                value={draftMode}
                disabled={modeSaving}
                onValueChange={(value) => setDraftMode(value)}
              >
                <SelectTrigger className="w-[124px]" data-testid="observability-provider-mode">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="builtin">{t('observability.provider.builtin')}</SelectItem>
                  <SelectItem value="prometheus" disabled={!health?.prometheus_configured}>
                    {t('observability.provider.prometheus')}
                  </SelectItem>
                  <SelectItem value="auto" disabled={!health?.prometheus_configured}>
                    {t('observability.provider.auto')}
                  </SelectItem>
                </SelectContent>
              </Select>
              <Button
                type="button"
                size="sm"
                disabled={modeSaving || !modeDirty}
                onClick={() => void applyMode()}
                data-testid="observability-apply-mode"
              >
                {modeSaving ? t('common.applying') : t('observability.toolbar.applyMode')}
              </Button>
            </div>

            {health && (
              <div className="flex flex-col gap-1.5 md:items-end">
                <div className="flex flex-wrap items-center gap-2 md:justify-end">
                  <span className={toolbarLabelClass}>{t('observability.toolbar.status')}</span>
                  <StatusBadge
                    value={providerLabel(health.active_provider || 'builtin')}
                    tone={providerStatusTone(health)}
                    data-testid="observability-provider-status"
                  />
                  {health.provider_health && (
                    <StatusBadge
                      value={translateProviderHealth(t, health.provider_health)}
                      tone={providerHealthTone(health.provider_health)}
                      data-testid="observability-provider-health"
                    />
                  )}
                  {!health.enabled && (
                    <Badge
                      variant="outline"
                      className="text-muted-foreground uppercase"
                      data-testid="observability-provider-disabled"
                    >
                      {t('observability.toolbar.disabled')}
                    </Badge>
                  )}
                </div>
                {providerStatusDetails.length > 0 && (
                  <div className="flex flex-wrap items-center gap-x-6 gap-y-1.5 md:justify-end">
                    {providerStatusDetails.map((item) => (
                      <span
                        key={item.label}
                        className="flex min-w-[88px] flex-col gap-0.5 md:items-end"
                      >
                        <span className="text-[11px] leading-tight font-bold text-muted-foreground">
                          {item.label}
                        </span>
                        <span className="text-xs leading-snug break-words text-foreground">
                          {item.value}
                        </span>
                      </span>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>

          {hasProviderMessages && (
            <div className="flex w-full flex-col gap-1.5">
              {modeError ? (
                <p
                  className="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-1.5 text-xs text-destructive"
                  data-testid="observability-mode-error"
                >
                  {modeError}
                </p>
              ) : modeNotice ? (
                <p
                  className="rounded-lg border border-success/25 bg-success/10 px-3 py-1.5 text-xs text-success"
                  data-testid="observability-mode-notice"
                >
                  {modeNotice}
                </p>
              ) : null}
              {health?.fallback_reason && (
                <p
                  className="rounded-lg border border-warning/30 bg-warning/10 px-3 py-1.5 text-xs text-warning"
                  data-testid="observability-fallback-reason"
                >
                  {health.fallback_reason}
                </p>
              )}
            </div>
          )}
        </div>
      </section>

      {/* Chart-scope controls, pinned: reading a chart usually means changing the range, and
          a zoomed panel must not hide the range either — the panel starts below this bar and
          the bar keeps a higher z-index than the panel. The toolbar above leads with the
          provider settings through `order`, keeping a DOM order that reaches the range and
          filters before the rarely-touched settings. */}
      <section
        ref={stickyRef}
        className="sticky top-0 z-[70] flex flex-col gap-2 rounded-xl border border-border bg-card p-3"
        data-testid="observability-chart-controls"
      >
        <div className="flex w-full flex-wrap items-center gap-2.5">
          <div className="flex flex-wrap items-center gap-2.5">
            <span className={toolbarLabelClass}>{t('observability.toolbar.timeRange')}</span>
            <ToggleGroup
              type="single"
              value={selectedRange}
              onValueChange={(value) => {
                if (value) setSelectedRange(value)
              }}
              data-testid="observability-time-range"
            >
              {timeRanges.map((item) => (
                <ToggleGroupItem
                  key={item.value}
                  value={item.value}
                  size="sm"
                  data-testid={`observability-range-${item.value}`}
                >
                  {item.label}
                </ToggleGroupItem>
              ))}
            </ToggleGroup>
          </div>

          {/* Absolute range next to the presets: it edits the same window the presets set,
              and follows a range dragged out on a chart. */}
          <AbsoluteRangePicker
            value={displayedWindow}
            bounds={loadedWindow}
            active={chartZoomActive}
            onApply={applyAbsoluteRange}
            onClear={resetChartZoom}
          />
          {chartZoomActive && (
            <Button
              variant="ghost"
              size="xs"
              onClick={resetChartZoom}
              data-testid="observability-reset-zoom"
            >
              {t('observability.toolbar.resetZoom')}
            </Button>
          )}
        </div>

        {retentionRangeHint && (
          <p className="w-full text-xs text-warning" data-testid="observability-retention-hint">
            {retentionRangeHint}
          </p>
        )}

        {sparseDataHint && (
          <p className="w-full text-xs text-muted-foreground" data-testid="observability-sparse-hint">
            {sparseDataHint}
          </p>
        )}

        <p className="w-full text-xs text-muted-foreground" data-testid="observability-zoom-hint">
          {t('observability.toolbar.zoomHint')}
        </p>
      </section>

      {dashboardError && <ErrorAlert message={dashboardError} />}

      {needsInstanceSelection && (
        <section className="grid gap-4">
          <EmptyNotice title={t('observability.collector.selectInstanceHint')} />
        </section>
      )}

      <CollectorPanel
        health={health}
        healthError={healthError}
        settings={collectorSettings}
        targets={filteredTargets}
        targetsLoading={targetsLoading}
        targetsError={targetsError}
        metricNames={metricNames}
        namesError={namesError}
        onSettingsUpdated={onSettingsUpdated}
      />

      {chartGroups.map((group) => {
        const groupExpanded = !collapsedGroups[group.id]
        return (
          <section key={group.id} className="grid gap-4" data-testid={`observability-group-${group.id}`}>
            <div
              className={cn(
                'overflow-hidden rounded-xl border bg-card transition-[box-shadow,border-color]',
                groupExpanded ? 'border-primary/30 shadow-sm' : 'border-border'
              )}
            >
              <button
                type="button"
                aria-expanded={groupExpanded}
                onClick={() => toggleGroup(group.id)}
                data-testid={`observability-group-toggle-${group.id}`}
                className={cn(
                  'flex w-full items-center gap-3 px-4 py-2.5 text-left text-foreground transition-colors hover:bg-muted/50',
                  groupExpanded ? 'border-b border-border bg-card' : 'bg-muted/40'
                )}
              >
                <ChevronRightIcon
                  aria-hidden
                  className={cn(
                    'size-4 shrink-0 text-muted-foreground transition-transform',
                    groupExpanded && 'rotate-90'
                  )}
                />
                <span className="font-heading text-base font-semibold">
                  {translateGroupTitle(t, te, group.id, group.title)}
                </span>
                <Badge
                  variant="secondary"
                  className="ml-auto shrink-0 rounded-full px-2.5 text-[11px] font-bold"
                  data-testid={`observability-group-count-${group.id}`}
                >
                  {groupChartCount(group.sections)} {t('observability.chart.charts')}
                </Badge>
                <span className={collapseLabelClass}>
                  {groupExpanded
                    ? t('observability.chart.expanded')
                    : t('observability.chart.collapsed')}
                </span>
              </button>

              <div className={cn('grid gap-2.5 bg-muted/20 p-3', !groupExpanded && 'hidden')}>
                {group.sections.map((section) => {
                  const sectionExpanded = !collapsedSections[`${group.id}:${section.id}`]
                  return (
                    <div
                      key={`${group.id}-${section.id}`}
                      className={cn(
                        'overflow-hidden rounded-lg border border-border bg-card',
                        sectionExpanded && 'shadow-sm'
                      )}
                      data-testid={`observability-section-${group.id}-${section.id}`}
                    >
                      <button
                        type="button"
                        aria-expanded={sectionExpanded}
                        onClick={() => toggleSection(group.id, section.id)}
                        data-testid={`observability-section-toggle-${group.id}-${section.id}`}
                        className={cn(
                          'flex w-full items-center gap-3 px-3.5 py-3 text-left text-foreground transition-colors hover:bg-muted/50',
                          sectionExpanded ? 'border-b border-border bg-card' : 'bg-muted/40'
                        )}
                      >
                        <ChevronRightIcon
                          aria-hidden
                          className={cn(
                            'size-3.5 shrink-0 text-muted-foreground transition-transform',
                            sectionExpanded && 'rotate-90'
                          )}
                        />
                        <span className="text-[15px] font-bold">{section.title}</span>
                        <Badge
                          variant="outline"
                          className="ml-auto shrink-0 rounded-full px-2.5 text-[11px] font-bold text-muted-foreground"
                        >
                          {section.panels.length} {t('observability.chart.charts')}
                        </Badge>
                        <span className={cn(collapseLabelClass, 'text-muted-foreground')}>
                          {sectionExpanded
                            ? t('observability.chart.expanded')
                            : t('observability.chart.collapsed')}
                        </span>
                      </button>

                      <div
                        className={cn(
                          'grid grid-cols-1 gap-3 bg-card p-3 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4',
                          !sectionExpanded && 'hidden'
                        )}
                      >
                        {section.panels.map((panel) => (
                          <TimeSeriesChart
                            key={`${panel.id}-${selectedRange}`}
                            title={translatePanelTitle(t, te, panel.id, panel.title)}
                            subtitle={panel.subtitle}
                            series={panel.series}
                            range={selectedRange}
                            windowStartMs={chartWindowStartMs}
                            windowEndMs={chartWindowEndMs}
                            zoomStartMs={chartZoomStartMs}
                            zoomEndMs={chartZoomEndMs}
                            loading={dashboardLoading}
                            error={panel.error}
                            emptyMessage={showEmptyPanels ? emptyPanelMessage : undefined}
                            valueFormatter={panel.valueFormatter}
                            panelId={panel.id}
                            compact
                            onZoomChange={onChartZoomChange}
                          />
                        ))}
                      </div>
                    </div>
                  )
                })}

                {group.tablePanels.map((panel) => (
                  <DashboardDataSourcesTable
                    key={panel.id}
                    title={translatePanelTitle(t, te, panel.id, panel.title)}
                    subtitle={panel.subtitle}
                    rows={panel.rows}
                    loading={dashboardLoading}
                    error={panel.error}
                    embedded
                  />
                ))}
              </div>
            </div>
          </section>
        )
      })}
    </div>
  )
}
