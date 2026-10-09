/**
 * TimeSeriesChart — React 18 + TypeScript port of
 * webui/src/components/observability/TimeSeriesChart.vue.
 *
 * Behaviour-preserving: same ECharts option shape, same palette/palette-indexing,
 * same manual rect-drag time zoom, same sparse-window axis fitting and the same
 * per-series summary table with click-to-isolate.
 *
 * The series label/summary helpers live in `@/observability/metrics` and are
 * imported, so the chart and the dashboard pages resolve labels identically.
 */
import { useCallback, useEffect, useId, useRef, useState } from 'react'
import { flushSync } from 'react-dom'

import * as echarts from 'echarts'
import { Maximize2Icon, Minimize2Icon } from 'lucide-react'

import type { DashboardSeries } from '@/api/types'
import { EmptyNotice, ErrorAlert } from '@/components/data-state'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle
} from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/components/ui/table'
import { useT } from '@/i18n'
import { cn } from '@/lib/utils'
import { useTheme } from '@/theme/theme-context'
import type { SeriesChartLabel } from '@/observability/metrics'
import {
  rangeToMs,
  resolveSeriesChartLabels,
  resolveSeriesSummary,
  seriesHasFinitePoints,
  truncateLabel
} from '@/observability/metrics'

// ---------------------------------------------------------------------------
// Chart constants (unchanged from the Vue component)
// ---------------------------------------------------------------------------

// Coordinated observability palette: brand green first, then soft low-saturation accents.
const CHART_PALETTE = [
  '#6fae3f', // primary green
  '#8fb4d0', // powder blue
  '#c9b07a', // soft wheat
  '#7fbfa0', // seafoam
  '#b3a4cc', // wisteria
  '#98b382', // light sage
  '#a8b5c0', // cool mist
  '#d0a898', // clay blush
  '#88a9c4', // horizon blue
  '#b8c99a' // spring olive
]

// When the actual data span is far shorter than the query window, zoom the x-axis instead of
// leaving a mostly empty chart (common cold-start UX in Grafana/Datadog-style dashboards).
const SPARSE_WINDOW_RATIO = 0.25
const AXIS_FIT_PADDING_MS = 45_000
const MIN_ZOOM_SPAN_MS = 5_000
const MIN_ZOOM_DRAG_PX = 12

/**
 * The shaded band drawn while dragging out a time range.
 *
 * It must start hidden through `ignore`, not `invisible`: zrender's `show()`/`hide()`
 * only ever toggle `ignore`, while `invisible` is a separate flag that `show()` leaves
 * alone — so a rect built with `invisible: true` is never painted, and the drag gives
 * no feedback at all despite `show()` being called on every mousemove.
 */
export function createDragSelectionRect() {
  const rect = new echarts.graphic.Rect({
    ignore: true,
    silent: true,
    z: 100,
    shape: { x: 0, y: 0, width: 0, height: 0 },
    style: {
      fill: 'rgba(111, 174, 63, 0.12)',
      stroke: 'rgba(111, 174, 63, 0.7)',
      lineWidth: 1
    }
  })
  return rect
}

// Match the admin UI font stack; keeps default ECharts time formatting, only improves rendering.
const CHART_AXIS_FONT = "'Geist Variable', ui-sans-serif, system-ui, sans-serif"

/**
 * Axis and grid colours follow the active palette.
 *
 * ECharts bakes these into the option as SVG presentation attributes, where a CSS
 * custom property would not resolve, so the value is read from the `<html>` class
 * at option-build time and the chart rebuilds when the theme changes.
 */
function isDarkTheme(): boolean {
  return typeof document !== 'undefined' && document.documentElement.classList.contains('dark')
}

function axisColor(): string {
  return isDarkTheme() ? '#fafafa' : '#111111'
}

function splitLineColor(): string {
  return isDarkTheme() ? 'oklch(1 0 0 / 12%)' : '#e5e5e5'
}

const TOOLTIP_EXTRA_CSS = [
  'max-width: min(420px, 92vw)',
  'width: max-content',
  'padding: 6px 10px',
  'white-space: normal',
  'overflow-wrap: anywhere',
  'word-break: break-word',
  'font-size: 11px',
  'line-height: 1.4',
  `font-family: ${CHART_AXIS_FONT}`,
  'font-weight: 500',
  // Theme tokens rather than hardcoded colours: the tooltip is appended to the
  // body, so it inherits the custom properties and follows the dark palette.
  'background: var(--card)',
  'color: var(--card-foreground)',
  'border: 1px solid var(--border)',
  'box-shadow: 0 4px 14px rgb(0 0 0 / 0.1)',
  'border-radius: 4px',
  'will-change: auto'
].join(';')

function seriesColor(index: number): string {
  return CHART_PALETTE[index % CHART_PALETTE.length]
}

function readDevicePixelRatio(): number {
  return typeof window !== 'undefined' ? window.devicePixelRatio || 1 : 1
}

type ZoomRange = { startMs: number; endMs: number }

export interface TimeSeriesChartProps {
  title: string
  subtitle?: string
  series?: DashboardSeries[]
  range?: string
  windowStartMs?: number
  windowEndMs?: number
  zoomStartMs?: number
  zoomEndMs?: number
  loading?: boolean
  error?: string
  emptyMessage?: string
  valueFormatter?: ((value: number | string) => string) | null
  panelId?: string
  compact?: boolean
  onZoomChange?: (range: ZoomRange | null) => void
}

/**
 * Mutable, render-independent chart bookkeeping. The ECharts instance must NOT
 * live in React state (it is not serialisable and would force a re-render on
 * every option update) — a ref is the direct equivalent of the Vue `markRaw`.
 */
interface ChartRuntime {
  chart: echarts.EChartsType | null
  selectionRect: echarts.graphic.Rect | null
  dragState: { startX: number; currentX: number } | null
  chartEventsBound: boolean
  renderSeq: number
  devicePixelRatio: number
  resizeObserver: ResizeObserver | null
  rafId: number | null
}

/** The latest render's closures, so stable async handlers never read stale props. */
interface ChartEngine {
  scheduleRender: () => void
  resetChart: () => void
  onSeriesChange: () => void
  disposeChart: () => void
  renderChart: () => void
  handleLayoutChange: () => void
  handleZrMouseDown: (event: echarts.ElementEvent) => void
  handleZrMouseMove: (event: echarts.ElementEvent) => void
  handleZrMouseUp: (event: echarts.ElementEvent) => void
  handleZrDblClick: () => void
  handleWindowMouseUp: () => void
}

export function TimeSeriesChart({
  title,
  subtitle = '',
  series,
  range = '15m',
  windowStartMs = 0,
  windowEndMs = 0,
  zoomStartMs = 0,
  zoomEndMs = 0,
  loading = false,
  error = '',
  emptyMessage = '',
  valueFormatter = null,
  panelId = '',
  compact = false,
  onZoomChange
}: TimeSeriesChartProps) {
  const t = useT()

  const containerRef = useRef<HTMLDivElement | null>(null)
  const runtimeRef = useRef<ChartRuntime>({
    chart: null,
    selectionRect: null,
    dragState: null,
    chartEventsBound: false,
    renderSeq: 0,
    devicePixelRatio: readDevicePixelRatio(),
    resizeObserver: null,
    rafId: null
  })
  const { resolved } = useTheme()
  const engineRef = useRef<ChartEngine | null>(null)

  const [summaryExpanded, setSummaryExpanded] = useState(false)
  const [isolatedSeriesIndex, setIsolatedSeriesIndex] = useState<number | null>(null)

  /**
   * Grafana-style panel zoom: the card itself is pinned over the page rather than
   * re-rendered somewhere else, so the ECharts instance, its drag state and the zoom
   * range all stay exactly as they were — only the box it fills changes. The container's
   * ResizeObserver sees the new box and re-fits the chart.
   */
  const [enlarged, setEnlarged] = useState(false)
  const [hovered, setHovered] = useState(false)
  // Names this card for the View Transitions morph; `useId` keeps it unique because a
  // dashboard renders many charts and duplicate names make the transition bail out.
  const viewTransitionName = `chart-${useId().replace(/:/g, '')}`

  /**
   * Resizing plays as a morph from the card's old box to its new one via the View
   * Transitions API, so the panel visibly stretches open and back. React 18 needs
   * `flushSync` there — the DOM must be updated inside the callback for the browser to
   * capture both states. Without the API, or under reduced motion, the toggle is instant.
   */
  function toggleEnlarged() {
    const next = !enlarged
    const reduced =
      typeof window.matchMedia === 'function' &&
      window.matchMedia('(prefers-reduced-motion: reduce)').matches
    if (typeof document.startViewTransition !== 'function' || reduced) {
      setEnlarged(next)
      return
    }
    document.startViewTransition(() => {
      flushSync(() => setEnlarged(next))
    })
  }

  // Latest-ref pattern, as with `engineRef`: the keyboard effect then keeps its listener
  // bound across renders instead of re-binding on every one.
  const toggleEnlargedRef = useRef(toggleEnlarged)
  toggleEnlargedRef.current = toggleEnlarged

  // The page behind must not scroll while a panel is zoomed: the panel is fixed, so the
  // wheel would otherwise move the list behind it with nothing on screen to show for it.
  useEffect(() => {
    if (!enlarged) return
    const { body } = document
    const previousOverflow = body.style.overflow
    const previousPadding = body.style.paddingRight
    const scrollbar = window.innerWidth - document.documentElement.clientWidth
    body.style.overflow = 'hidden'
    if (scrollbar > 0) body.style.paddingRight = `${scrollbar}px`
    return () => {
      body.style.overflow = previousOverflow
      body.style.paddingRight = previousPadding
    }
  }, [enlarged])

  // `v` toggles, Escape restores. Bound only while the pointer is over the panel (or
  // while it is enlarged), so it cannot swallow the key while reading the rest of the page.
  useEffect(() => {
    if (!hovered && !enlarged) return

    function handleKeyDown(event: KeyboardEvent) {
      const target = event.target as HTMLElement | null
      const tag = target?.tagName?.toLowerCase()
      if (tag === 'input' || tag === 'textarea' || tag === 'select' || target?.isContentEditable) {
        return
      }
      if (event.key === 'Escape' && enlarged) {
        toggleEnlargedRef.current()
        return
      }
      if (event.key === 'v' || event.key === 'V') {
        toggleEnlargedRef.current()
      }
    }

    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [hovered, enlarged])

  // ---- Derived (recomputed each render; cheap and keeps the engine fresh) ----
  const chartSourceSeries = (series ?? []).filter(
    (item) => Array.isArray(item.points) && seriesHasFinitePoints(item.points)
  )
  const hasData = chartSourceSeries.length > 0
  const chartReady = !loading && !error && hasData
  const seriesChartLabels = resolveSeriesChartLabels(chartSourceSeries, panelId)

  const summaryRows = chartSourceSeries.map((item, index) => {
    const chartLabel = seriesChartLabels[index]
    const summary = resolveSeriesSummary(item.summary, item.points)
    return {
      name: chartLabel?.chartName || item.name || '',
      fullName: chartLabel?.fullDetail || item.name || '',
      chartIndex: index,
      current: formatSummaryValue(summary.current),
      min: formatSummaryValue(summary.min),
      max: formatSummaryValue(summary.max),
      avg: formatSummaryValue(summary.avg)
    }
  })
  const summaryCollapseLimit = compact ? 2 : 4
  const canToggleSummary = summaryRows.length > summaryCollapseLimit

  let visibleSummaryRows = summaryRows
  if (!summaryExpanded) {
    const base = summaryRows.slice(0, summaryCollapseLimit)
    if (isolatedSeriesIndex === null) {
      visibleSummaryRows = base
    } else {
      const isolated = summaryRows.find((row) => row.chartIndex === isolatedSeriesIndex)
      visibleSummaryRows =
        !isolated || base.some((row) => row.chartIndex === isolatedSeriesIndex)
          ? base
          : [...base, isolated]
    }
  }
  const hiddenSummaryCount = summaryExpanded
    ? 0
    : Math.max(0, summaryRows.length - visibleSummaryRows.length)

  const renderedChartSeries =
    isolatedSeriesIndex === null
      ? chartSourceSeries
      : chartSourceSeries.filter((_, index) => index === isolatedSeriesIndex)
  const isolatedLabel = isolatedSeriesIndex === null ? undefined : seriesChartLabels[isolatedSeriesIndex]
  const renderedChartLabels =
    isolatedSeriesIndex === null ? seriesChartLabels : isolatedLabel ? [isolatedLabel] : []

  const resolvedEmptyMessage = emptyMessage || t('observability.chart.emptyDefault')

  const toggleSeriesIsolation = (chartIndex: number) => {
    setIsolatedSeriesIndex((current) => (current === chartIndex ? null : chartIndex))
  }
  const clearSeriesIsolation = () => setIsolatedSeriesIndex(null)

  // ---- Stable handlers: identity never changes so zr/window listeners can be added & removed ----
  const stableWindowResize = useCallback(() => {
    engineRef.current?.handleLayoutChange()
  }, [])
  const stableWindowMouseUp = useCallback(() => {
    engineRef.current?.handleWindowMouseUp()
  }, [])
  const stableZrMouseDown = useCallback((event: echarts.ElementEvent) => {
    engineRef.current?.handleZrMouseDown(event)
  }, [])
  const stableZrMouseMove = useCallback((event: echarts.ElementEvent) => {
    engineRef.current?.handleZrMouseMove(event)
  }, [])
  const stableZrMouseUp = useCallback((event: echarts.ElementEvent) => {
    engineRef.current?.handleZrMouseUp(event)
  }, [])
  const stableZrDblClick = useCallback(() => {
    engineRef.current?.handleZrDblClick()
  }, [])
  const setupResizeObserver = useCallback(() => {
    const container = containerRef.current
    if (!container || runtimeRef.current.resizeObserver) return
    const observer = new ResizeObserver(() => {
      engineRef.current?.handleLayoutChange()
    })
    observer.observe(container)
    runtimeRef.current.resizeObserver = observer
  }, [])
  const teardownResizeObserver = useCallback(() => {
    const observer = runtimeRef.current.resizeObserver
    if (observer) {
      observer.disconnect()
      runtimeRef.current.resizeObserver = null
    }
  }, [])

  // ---- ECharts formatting helpers (closures over current props) ----
  function formatValue(value: unknown): string {
    if (valueFormatter) return valueFormatter(value as number | string)
    return Number(value || 0).toLocaleString()
  }

  function formatAxisValue(value: unknown): string {
    if (valueFormatter) {
      return truncateLabel(String(valueFormatter(value as number | string)), 8)
    }
    const num = Number(value || 0)
    if (Math.abs(num) >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`
    if (Math.abs(num) >= 1_000) return `${(num / 1_000).toFixed(1)}k`
    if (Number.isInteger(num)) return String(num)
    return num.toFixed(1)
  }

  function formatSummaryValue(value: number | null | undefined): string {
    if (value == null || !Number.isFinite(Number(value))) return '—'
    return formatValue(Number(value))
  }

  function formatTooltipTime(value: unknown): string {
    const date = new Date(Number(value))
    if (Number.isNaN(date.getTime())) return ''
    if (compact) {
      return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
    }
    return date.toLocaleString()
  }

  function buildSeriesMeta(labels: SeriesChartLabel[]): Record<string, { displayName: string; fullDetail: string }> {
    const metaByName: Record<string, { displayName: string; fullDetail: string }> = {}
    labels.forEach((entry) => {
      metaByName[entry.chartName] = {
        displayName: entry.chartName,
        fullDetail: entry.fullDetail
      }
    })
    return metaByName
  }

  function formatAxisTooltip(
    params: unknown,
    metaByName: Record<string, { displayName: string; fullDetail: string }>
  ): string {
    if (!Array.isArray(params) || !params.length) return ''
    const list = params as Array<{
      axisValue: unknown
      seriesName: string
      marker?: string
      value: unknown
    }>
    const header = `<div style="margin-bottom:4px;font-weight:600;color:#1f2a1c;font-size:11px;white-space:normal">${formatTooltipTime(list[0].axisValue)}</div>`
    const rows = list.map((param) => {
      const meta = metaByName[param.seriesName] || { displayName: '', fullDetail: '' }
      const label = meta.displayName || param.seriesName
      const rawValue = Array.isArray(param.value) ? param.value[1] : param.value
      const value = formatValue(rawValue)
      const detail = meta.fullDetail
      const showDetail = detail && detail !== label && detail !== param.seriesName
      const detailLine = showDetail
        ? `<div style="margin-top:2px;color:#7a8a78;font-size:10px;font-weight:400;line-height:1.35;white-space:normal;overflow-wrap:anywhere">${detail}</div>`
        : ''
      return `<div style="margin-top:3px;line-height:1.35;white-space:normal;overflow-wrap:anywhere">${param.marker} ${label}: <strong>${value}</strong>${detailLine}</div>`
    })
    return header + rows.join('')
  }

  function buildQueryWindow(): { min: number; max: number; span: number } {
    const start = Number(windowStartMs)
    const end = Number(windowEndMs)
    if (Number.isFinite(start) && start > 0 && Number.isFinite(end) && end > start) {
      return { min: start, max: end, span: end - start }
    }
    const rangeMs = rangeToMs(range)
    const maxTs = end > 0 ? end : Date.now()
    return {
      min: maxTs - rangeMs,
      max: maxTs,
      span: rangeMs
    }
  }

  function fitTimeAxisToData(
    queryWindow: { min: number; max: number; span: number },
    dataTimestamps: number[]
  ): { min: number; max: number } {
    if (!Array.isArray(dataTimestamps) || !dataTimestamps.length) {
      return { min: queryWindow.min, max: queryWindow.max }
    }
    const dataMin = Math.min(...dataTimestamps)
    const dataMax = Math.max(...dataTimestamps)
    const dataSpan = Math.max(0, dataMax - dataMin)
    if (dataSpan >= queryWindow.span * SPARSE_WINDOW_RATIO) {
      return { min: queryWindow.min, max: queryWindow.max }
    }
    const padding = Math.max(
      AXIS_FIT_PADDING_MS,
      dataSpan > 0 ? Math.round(dataSpan * 0.12) : AXIS_FIT_PADDING_MS
    )
    return {
      min: Math.max(queryWindow.min, dataMin - padding),
      max: Math.min(queryWindow.max, dataMax + padding)
    }
  }

  function isZoomActive(): boolean {
    const start = Number(zoomStartMs)
    const end = Number(zoomEndMs)
    return Number.isFinite(start) && start > 0 && Number.isFinite(end) && end > start
  }

  function buildTimeAxisRange(dataTimestamps: number[] = []): { min: number; max: number } {
    const queryWindow = buildQueryWindow()
    if (isZoomActive()) {
      return {
        min: Math.max(queryWindow.min, Number(zoomStartMs)),
        max: Math.min(queryWindow.max, Number(zoomEndMs))
      }
    }
    return fitTimeAxisToData(queryWindow, dataTimestamps)
  }

  function buildChartOption(): echarts.EChartsCoreOption {
    const sourceSeries = renderedChartSeries
    const chartLabels = renderedChartLabels.length
      ? renderedChartLabels
      : resolveSeriesChartLabels(sourceSeries, panelId)
    const metaByName = buildSeriesMeta(chartLabels)
    const echartsSeries = sourceSeries.map((item, index) => {
      const sourceIndex = isolatedSeriesIndex === null ? index : isolatedSeriesIndex
      const color = CHART_PALETTE[sourceIndex % CHART_PALETTE.length]
      const chartName = chartLabels[index]?.chartName || item.name || `series-${index}`
      return {
        id: `${panelId}-${index}`,
        name: chartName,
        type: 'line' as const,
        showSymbol: false,
        smooth: false,
        lineStyle: { width: compact ? 1.5 : 2 },
        color,
        areaStyle: compact ? { color, opacity: 0.1 } : undefined,
        data: item.points.map(([ts, value]) => [ts, value] as [number, number])
      }
    })

    const timeAxis = buildTimeAxisRange(
      echartsSeries.flatMap((item) => item.data.map(([ts]) => ts))
    )

    return {
      animation: false,
      color: CHART_PALETTE,
      grid: {
        left: compact ? 4 : 72,
        right: compact ? 8 : 24,
        top: compact ? 6 : 16,
        bottom: compact ? 18 : 42,
        containLabel: true
      },
      tooltip: {
        trigger: 'axis',
        appendToBody: true,
        confine: false,
        extraCssText: TOOLTIP_EXTRA_CSS,
        formatter: (params: unknown) => formatAxisTooltip(params, metaByName)
      },
      legend: { show: false },
      xAxis: {
        type: 'time',
        min: timeAxis.min,
        max: timeAxis.max,
        axisLabel: {
          fontFamily: CHART_AXIS_FONT,
          color: axisColor(),
          fontSize: compact ? 12 : 13,
          hideOverlap: true,
          margin: compact ? 6 : 8
        },
        axisLine: { lineStyle: { color: '#dbe8d4' } }
      },
      yAxis: {
        type: 'value',
        axisLabel: {
          fontFamily: CHART_AXIS_FONT,
          color: axisColor(),
          fontSize: compact ? 12 : 13,
          margin: compact ? 4 : 10,
          formatter: (value: unknown) => formatAxisValue(value)
        },
        splitLine: { lineStyle: { color: splitLineColor() } }
      },
      series: echartsSeries
    }
  }

  // ---- Manual drag-to-zoom (no ECharts dataZoom: the Vue did it by hand) ----
  function coordToMs(value: unknown): number {
    if (value instanceof Date) return value.getTime()
    const numeric = Number(value)
    if (Number.isFinite(numeric)) return numeric
    const parsed = Date.parse(String(value))
    return Number.isFinite(parsed) ? parsed : NaN
  }

  function getGridRect(): { x: number; y: number; width: number; height: number } | null {
    const chart = runtimeRef.current.chart
    if (!chart || chart.isDisposed()) return null
    // `getModel` is public at runtime but typed as private, so it needs a cast.
    const model = (
      chart as unknown as {
        getModel: () => {
          getComponent: (
            mainType: string,
            index: number
          ) =>
            | {
                coordinateSystem?: {
                  getRect?: () => { x: number; y: number; width: number; height: number }
                }
              }
            | undefined
        }
      }
    ).getModel()
    const grid = model.getComponent('grid', 0)
    const rect = grid?.coordinateSystem?.getRect?.()
    if (!rect || rect.width <= 0 || rect.height <= 0) return null
    return rect
  }

  function isPointInGrid(x: number, y: number): boolean {
    const rect = getGridRect()
    if (!rect) return false
    return x >= rect.x && x <= rect.x + rect.width && y >= rect.y && y <= rect.y + rect.height
  }

  function pixelToAxisMs(x: number, y: number): number {
    const chart = runtimeRef.current.chart
    if (!chart || chart.isDisposed()) return NaN
    const coord = chart.convertFromPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [x, y])
    const raw = Array.isArray(coord) ? coord[0] : coord
    return coordToMs(raw)
  }

  function updateSelectionRect() {
    const runtime = runtimeRef.current
    if (!runtime.selectionRect || !runtime.dragState) return
    const gridRect = getGridRect()
    if (!gridRect) return
    const left = Math.min(runtime.dragState.startX, runtime.dragState.currentX)
    const right = Math.max(runtime.dragState.startX, runtime.dragState.currentX)
    const clampedLeft = Math.max(left, gridRect.x)
    const clampedRight = Math.min(right, gridRect.x + gridRect.width)
    const width = Math.max(0, clampedRight - clampedLeft)
    if (width <= 0) {
      hideSelectionRect()
      return
    }
    runtime.selectionRect.setShape({
      x: clampedLeft,
      y: gridRect.y,
      width,
      height: gridRect.height
    })
    runtime.selectionRect.show()
  }

  function hideSelectionRect() {
    const rect = runtimeRef.current.selectionRect
    if (rect) rect.hide()
  }

  function finishDragSelection() {
    window.removeEventListener('mouseup', stableWindowMouseUp, true)
    const runtime = runtimeRef.current
    const dragState = runtime.dragState
    runtime.dragState = null
    hideSelectionRect()
    const chart = runtime.chart
    if (!dragState || !chart || chart.isDisposed()) return

    const gridRect = getGridRect()
    if (!gridRect) return

    const left = Math.min(dragState.startX, dragState.currentX)
    const right = Math.max(dragState.startX, dragState.currentX)
    if (right - left < MIN_ZOOM_DRAG_PX) return

    const clampedLeft = Math.max(left, gridRect.x)
    const clampedRight = Math.min(right, gridRect.x + gridRect.width)
    if (clampedRight - clampedLeft < MIN_ZOOM_DRAG_PX) return

    const midY = gridRect.y + gridRect.height / 2
    let start = pixelToAxisMs(clampedLeft, midY)
    let end = pixelToAxisMs(clampedRight, midY)
    if (!Number.isFinite(start) || !Number.isFinite(end)) return
    if (start > end) {
      const swap = start
      start = end
      end = swap
    }

    const queryWindow = buildQueryWindow()
    start = Math.max(start, queryWindow.min)
    end = Math.min(end, queryWindow.max)
    if (end - start < MIN_ZOOM_SPAN_MS) return

    onZoomChange?.({ startMs: Math.round(start), endMs: Math.round(end) })
  }

  function ensureSelectionRect() {
    const runtime = runtimeRef.current
    const chart = runtime.chart
    if (!chart || chart.isDisposed()) return
    removeSelectionRect()
    const zr = chart.getZr()
    runtime.selectionRect = createDragSelectionRect()
    zr.add(runtime.selectionRect)
  }

  function removeSelectionRect() {
    const runtime = runtimeRef.current
    if (!runtime.selectionRect) return
    const chart = runtime.chart
    if (chart && !chart.isDisposed()) {
      chart.getZr().remove(runtime.selectionRect)
    }
    runtime.selectionRect = null
  }

  // ---- Chart lifecycle ----
  function unbindChartEvents() {
    const runtime = runtimeRef.current
    const chart = runtime.chart
    if (!chart || chart.isDisposed() || !runtime.chartEventsBound) return
    const zr = chart.getZr()
    zr.off('mousedown', stableZrMouseDown)
    zr.off('mousemove', stableZrMouseMove)
    zr.off('mouseup', stableZrMouseUp)
    zr.off('dblclick', stableZrDblClick)
    runtime.chartEventsBound = false
  }

  function bindChartEvents() {
    const runtime = runtimeRef.current
    const chart = runtime.chart
    if (!chart || chart.isDisposed() || runtime.chartEventsBound) return
    const zr = chart.getZr()
    zr.on('mousedown', stableZrMouseDown)
    zr.on('mousemove', stableZrMouseMove)
    zr.on('mouseup', stableZrMouseUp)
    zr.on('dblclick', stableZrDblClick)
    ensureSelectionRect()
    runtime.chartEventsBound = true
  }

  function initChart() {
    const container = containerRef.current
    if (!container) return
    const runtime = runtimeRef.current
    const dpr = readDevicePixelRatio()
    if (runtime.chart && !runtime.chart.isDisposed()) {
      if (dpr === runtime.devicePixelRatio) return
      disposeChart()
    }
    runtime.devicePixelRatio = dpr
    runtime.chart = echarts.init(container, null, { renderer: 'svg', devicePixelRatio: dpr })
  }

  function disposeChart() {
    const runtime = runtimeRef.current
    if (runtime.chart) {
      if (!runtime.chart.isDisposed()) {
        unbindChartEvents()
        removeSelectionRect()
        runtime.chart.dispose()
      }
      runtime.chart = null
    }
    runtime.chartEventsBound = false
    runtime.dragState = null
    if (runtime.rafId != null) {
      cancelAnimationFrame(runtime.rafId)
      runtime.rafId = null
    }
  }

  function resizeChart() {
    const chart = runtimeRef.current.chart
    const container = containerRef.current
    if (!chart || chart.isDisposed() || !container) return
    const { width, height } = container.getBoundingClientRect()
    if (width <= 0 || height <= 0) return
    chart.resize({ width: Math.round(width), height: Math.round(height) })
  }

  function setChartOption() {
    const chart = runtimeRef.current.chart
    if (!chart || chart.isDisposed()) return
    chart.setOption(buildChartOption(), {
      notMerge: true,
      replaceMerge: ['xAxis', 'yAxis', 'series']
    })
    resizeChart()
  }

  function reinitChart() {
    if (!chartReady) return
    disposeChart()
    initChart()
    const chart = runtimeRef.current.chart
    if (!chart || chart.isDisposed()) return
    setChartOption()
    bindChartEvents()
  }

  function handleLayoutChange() {
    const runtime = runtimeRef.current
    const dpr = readDevicePixelRatio()
    if (runtime.chart && !runtime.chart.isDisposed() && dpr !== runtime.devicePixelRatio) {
      runtime.devicePixelRatio = dpr
      reinitChart()
      return
    }
    resizeChart()
  }

  function renderChart() {
    if (!chartReady) return
    initChart()
    const chart = runtimeRef.current.chart
    if (!chart || chart.isDisposed()) return
    setChartOption()
    bindChartEvents()
    handleLayoutChange()
  }

  function handleZrMouseDown(event: echarts.ElementEvent) {
    const chart = runtimeRef.current.chart
    if (!chart || chart.isDisposed()) return
    const native = event.event as
      | { button?: number; preventDefault?: () => void; stopPropagation?: () => void }
      | undefined
    const button = native?.button
    if (button != null && button !== 0) return
    const point: [number, number] = [event.offsetX, event.offsetY]
    if (!isPointInGrid(point[0], point[1])) return
    native?.preventDefault?.()
    native?.stopPropagation?.()
    runtimeRef.current.dragState = { startX: point[0], currentX: point[0] }
    window.addEventListener('mouseup', stableWindowMouseUp, true)
    updateSelectionRect()
  }

  function handleZrMouseMove(event: echarts.ElementEvent) {
    const dragState = runtimeRef.current.dragState
    if (!dragState) return
    dragState.currentX = event.offsetX
    updateSelectionRect()
  }

  function handleZrMouseUp(event: echarts.ElementEvent) {
    const dragState = runtimeRef.current.dragState
    if (!dragState) return
    if (Number.isFinite(event?.offsetX)) {
      dragState.currentX = event.offsetX
    }
    finishDragSelection()
  }

  function handleWindowMouseUp() {
    window.removeEventListener('mouseup', stableWindowMouseUp, true)
    if (!runtimeRef.current.dragState) return
    finishDragSelection()
  }

  function handleZrDblClick() {
    onZoomChange?.(null)
  }

  function resetLegendState() {
    setSummaryExpanded(false)
    setIsolatedSeriesIndex(null)
  }

  function resetChart() {
    resetLegendState()
    disposeChart()
    scheduleRender()
  }

  function onSeriesChange() {
    resetLegendState()
    scheduleRender()
  }

  function scheduleRender() {
    // Coalesce bursts of prop changes into a single animation-frame render, mirroring the
    // Vue `$nextTick` + dedup-by-sequence counter.
    const runtime = runtimeRef.current
    const seq = ++runtime.renderSeq
    runtime.rafId = requestAnimationFrame(() => {
      runtime.rafId = null
      if (seq !== runtime.renderSeq) return
      engineRef.current?.renderChart()
    })
  }

  const engine: ChartEngine = {
    scheduleRender,
    resetChart,
    onSeriesChange,
    disposeChart,
    renderChart,
    handleLayoutChange,
    handleZrMouseDown,
    handleZrMouseMove,
    handleZrMouseUp,
    handleZrDblClick,
    handleWindowMouseUp
  }
  // Latest ref pattern: stable async handlers and effects call through to the freshest closures.
  engineRef.current = engine

  useEffect(() => {
    window.addEventListener('resize', stableWindowResize)
    setupResizeObserver()
    engineRef.current?.scheduleRender()
    return () => {
      window.removeEventListener('resize', stableWindowResize)
      window.removeEventListener('mouseup', stableWindowMouseUp, true)
      teardownResizeObserver()
      engineRef.current?.disposeChart()
    }
  }, [stableWindowResize, stableWindowMouseUp, setupResizeObserver, teardownResizeObserver])

  useEffect(() => {
    engineRef.current?.onSeriesChange()
  }, [series])

  useEffect(() => {
    engineRef.current?.resetChart()
  }, [range, windowStartMs, windowEndMs, compact])

  // Axis/grid colours are baked into the option, so a theme switch needs a rebuild.
  useEffect(() => {
    engineRef.current?.resetChart()
  }, [resolved])

  useEffect(() => {
    engineRef.current?.scheduleRender()
  }, [zoomStartMs, zoomEndMs, loading, error, chartReady, isolatedSeriesIndex])

  return (
    <Card
      data-testid="time-series-chart"
      data-enlarged={enlarged ? 'true' : undefined}
      role={enlarged ? 'dialog' : undefined}
      aria-label={enlarged ? title : undefined}
      size={compact ? 'sm' : 'default'}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      className={cn(
        'min-w-0 rounded-xl',
        compact && 'gap-2 py-2.5',
        enlarged && 'fixed right-3 bottom-3 left-3 z-[60] shadow-2xl md:right-6 md:bottom-6 md:left-6'
      )}
      style={{
        viewTransitionName,
        // A page that pins controls above the chart area (the observability toolbar)
        // publishes their height here, so a zoomed panel opens below them instead of
        // covering the time range. `0px` when nothing is pinned.
        ...(enlarged ? { top: 'calc(var(--panel-zoom-top-inset, 0px) + 0.75rem)' } : null)
      }}
    >
      <CardHeader className={cn('gap-1', compact && 'gap-0')}>
        <CardTitle
          title={subtitle || title}
          className={cn('truncate font-heading', compact ? 'text-[13px] font-bold' : 'text-base font-semibold')}
        >
          {title}
        </CardTitle>
        {subtitle && !compact && <CardDescription className="truncate">{subtitle}</CardDescription>}
        <CardAction>
          <Button
            variant="ghost"
            size="icon-xs"
            aria-expanded={enlarged}
            aria-label={enlarged ? t('observability.chart.restore') : t('observability.chart.enlarge')}
            title={`${enlarged ? t('observability.chart.restore') : t('observability.chart.enlarge')} (V)`}
            data-testid="time-series-chart-enlarge"
            onClick={toggleEnlarged}
          >
            {enlarged ? <Minimize2Icon /> : <Maximize2Icon />}
          </Button>
        </CardAction>
      </CardHeader>

      <CardContent
        className={cn('flex min-w-0 flex-col gap-2', enlarged && 'min-h-0 flex-1 overflow-hidden')}
      >
        <div
          className={cn(
            'relative',
            // No `min-h-0` when enlarged: that let the flex item shrink below the canvas's
            // own floor, and the canvas then painted over the summary table below it.
            enlarged
              ? 'min-h-[280px] flex-1'
              : cn(
                  'min-h-[320px] max-md:min-h-[220px]',
                  compact && 'min-h-[168px] max-md:min-h-[160px]'
                )
          )}
        >
          <div
            ref={containerRef}
            data-testid="time-series-chart-canvas"
            className={cn(
              'w-full max-w-full cursor-crosshair overflow-hidden antialiased',
              enlarged
                ? 'h-full min-h-[280px]'
                : compact
                  ? 'h-[168px] min-h-[148px] max-md:h-[160px] max-md:min-h-[140px]'
                  : 'h-[320px] min-h-[280px] max-md:h-[220px] max-md:min-h-[180px]'
            )}
          />
          {error ? (
            <div
              data-testid="time-series-chart-error"
              className="absolute inset-0 z-[1] grid place-items-center bg-card p-3"
            >
              <ErrorAlert message={error} className="w-full max-w-md" />
            </div>
          ) : loading ? (
            <div
              data-testid="time-series-chart-loading"
              className="absolute inset-0 z-[1] grid place-items-center bg-card p-4"
            >
              <Skeleton className="h-[55%] w-full rounded-lg" />
            </div>
          ) : !hasData ? (
            <div
              data-testid="time-series-chart-empty"
              className="absolute inset-0 z-[1] grid place-items-center bg-card p-3"
            >
              <EmptyNotice title={resolvedEmptyMessage} className="w-full max-w-md" />
            </div>
          ) : null}
        </div>

        {summaryRows.length > 0 && (
          <div
            data-testid="time-series-chart-summary"
            // Enlarged, a panel with many series has more rows than height: the list gets
            // its own scroll area and shrinks instead of pushing the plot off the card.
            className={cn(
              'flex flex-col gap-1 overflow-x-auto',
              enlarged && 'min-h-0 shrink overflow-y-auto'
            )}
          >
            <Table className="table-fixed text-[11px]">
              <TableHeader>
                <TableRow>
                  <TableHead className="w-[38%] text-[10px]">
                    {t('observability.chart.series')}
                  </TableHead>
                  <TableHead className="text-right text-[10px]">
                    {t('observability.chart.current')}
                  </TableHead>
                  <TableHead className="text-right text-[10px]">
                    {t('observability.chart.min')}
                  </TableHead>
                  <TableHead className="text-right text-[10px]">
                    {t('observability.chart.max')}
                  </TableHead>
                  <TableHead className="text-right text-[10px]">
                    {t('observability.chart.avg')}
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {visibleSummaryRows.map((row) => {
                  const isolated = isolatedSeriesIndex === row.chartIndex
                  const dimmed = isolatedSeriesIndex !== null && !isolated
                  return (
                    <TableRow
                      key={`${row.fullName || row.name}-${row.chartIndex}`}
                      data-testid={`time-series-chart-summary-row-${row.chartIndex}`}
                      data-isolated={isolated ? '' : undefined}
                      className={cn(
                        'cursor-pointer',
                        isolated && 'bg-primary/10',
                        dimmed && 'opacity-45 hover:opacity-70'
                      )}
                      onClick={() => toggleSeriesIsolation(row.chartIndex)}
                    >
                      <TableCell>
                        <span className="flex min-w-0 items-center gap-1.5">
                          {/* Palette colour is data-driven (shared with the ECharts series),
                              so it cannot be expressed as a Tailwind token. */}
                          <span
                            aria-hidden
                            className="size-2 shrink-0 rounded-full"
                            style={{ backgroundColor: seriesColor(row.chartIndex) }}
                          />
                          <span className="truncate" title={row.fullName || row.name}>
                            {row.name}
                          </span>
                        </span>
                      </TableCell>
                      <TableCell className="text-right tabular-nums">{row.current}</TableCell>
                      <TableCell className="text-right tabular-nums">{row.min}</TableCell>
                      <TableCell className="text-right tabular-nums">{row.max}</TableCell>
                      <TableCell className="text-right tabular-nums">{row.avg}</TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
            {(canToggleSummary || isolatedSeriesIndex !== null) && (
              <div className="flex items-center gap-2.5">
                {canToggleSummary && (
                  <Button
                    type="button"
                    variant="link"
                    size="xs"
                    data-testid="time-series-chart-summary-toggle"
                    className="h-auto p-0 text-[11px] font-semibold"
                    onClick={() => setSummaryExpanded((current) => !current)}
                  >
                    {summaryExpanded
                      ? t('observability.chart.showLess')
                      : t('observability.chart.showMore', { count: hiddenSummaryCount })}
                  </Button>
                )}
                {isolatedSeriesIndex !== null && (
                  <Button
                    type="button"
                    variant="link"
                    size="xs"
                    data-testid="time-series-chart-clear-isolation"
                    className="h-auto p-0 text-[11px] font-semibold text-muted-foreground"
                    onClick={clearSeriesIsolation}
                  >
                    {t('observability.chart.showAllSeries')}
                  </Button>
                )}
              </div>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
