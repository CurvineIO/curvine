<template>
  <div class="obs-chart-panel" :class="{ 'obs-chart-panel-compact': compact }">
    <div class="obs-chart-heading">
      <h2 :title="subtitle || title">{{ title }}</h2>
      <p v-if="subtitle && !compact">{{ subtitle }}</p>
    </div>
    <div class="obs-chart-body">
      <div ref="chartEl" class="obs-chart-canvas" />
      <div v-if="error" class="obs-chart-overlay obs-chart-error">{{ error }}</div>
      <div v-else-if="loading" class="obs-chart-overlay empty-state">{{ $t('observability.chart.loading') }}</div>
      <div v-else-if="!hasData" class="obs-chart-overlay empty-state">{{ resolvedEmptyMessage }}</div>
    </div>
    <div v-if="summaryRows.length" class="obs-series-summary-wrap">
      <table class="obs-series-summary-table">
        <thead>
          <tr>
            <th>{{ $t('observability.chart.series') }}</th>
            <th>{{ $t('observability.chart.current') }}</th>
            <th>{{ $t('observability.chart.min') }}</th>
            <th>{{ $t('observability.chart.max') }}</th>
            <th>{{ $t('observability.chart.avg') }}</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="(row, index) in visibleSummaryRows"
            :key="`${row.fullName || row.name}-${index}`"
            class="obs-series-summary-row"
            :class="{
              'is-isolated': isolatedSeriesIndex === row.chartIndex,
              'is-dimmed': isolatedSeriesIndex !== null && isolatedSeriesIndex !== row.chartIndex
            }"
            @click="toggleSeriesIsolation(row.chartIndex)"
          >
            <td>
              <span class="obs-series-label">
                <i class="obs-series-dot" :style="{ background: seriesColor(row.chartIndex) }" />
                <span :title="row.fullName || row.name">{{ row.name }}</span>
              </span>
            </td>
            <td>{{ row.current }}</td>
            <td>{{ row.min }}</td>
            <td>{{ row.max }}</td>
            <td>{{ row.avg }}</td>
          </tr>
        </tbody>
      </table>
      <button
        v-if="canToggleSummary"
        type="button"
        class="obs-summary-toggle"
        @click="summaryExpanded = !summaryExpanded"
      >
        {{ summaryExpanded ? $t('observability.chart.showLess') : $t('observability.chart.showMore', { count: hiddenSummaryCount }) }}
      </button>
      <button
        v-if="isolatedSeriesIndex !== null"
        type="button"
        class="obs-summary-reset"
        @click="clearSeriesIsolation"
      >
        {{ $t('observability.chart.showAllSeries') }}
      </button>
    </div>
  </div>
</template>

<script>
import { markRaw } from 'vue'
import * as echarts from 'echarts'
import {
  rangeToMs,
  resolveSeriesChartLabels,
  resolveSeriesSummary,
  seriesHasFinitePoints,
  truncateLabel
} from '@/views/observability/metrics'

// Coordinated observability palette: brand green first, then soft low-saturation accents
// that match the admin surface (#f6f8f3 / #6fae3f theme).
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

// When actual data span is far shorter than the query window, zoom the x-axis instead of
// leaving a mostly empty chart (common cold-start UX in Grafana/Datadog-style dashboards).
const SPARSE_WINDOW_RATIO = 0.25
const AXIS_FIT_PADDING_MS = 45_000
const MIN_ZOOM_SPAN_MS = 5_000
const MIN_ZOOM_DRAG_PX = 12

// Match admin UI font stack; keeps default ECharts time formatting, only improves rendering.
const CHART_AXIS_FONT = 'Inter, "Segoe UI", Arial, sans-serif'
const CHART_AXIS_COLOR = '#1b261f'

const TOOLTIP_EXTRA_CSS = [
  'max-width: min(420px, 92vw)',
  'width: max-content',
  'padding: 6px 10px',
  'white-space: normal',
  'overflow-wrap: anywhere',
  'word-break: break-word',
  'font-size: 11px',
  'line-height: 1.4',
  'font-family: Inter, Segoe UI, Arial, sans-serif',
  'font-weight: 500',
  'background: #ffffff',
  'border: 1px solid #dbe8d4',
  'box-shadow: 0 4px 14px rgba(15, 23, 42, 0.1)',
  'border-radius: 4px',
  'will-change: auto'
].join(';')

export default {
  name: 'TimeSeriesChart',
  props: {
    title: { type: String, required: true },
    subtitle: { type: String, default: '' },
    series: { type: Array, default: () => [] },
    range: { type: String, default: '15m' },
    windowStartMs: { type: Number, default: 0 },
    windowEndMs: { type: Number, default: 0 },
    zoomStartMs: { type: Number, default: 0 },
    zoomEndMs: { type: Number, default: 0 },
    loading: { type: Boolean, default: false },
    error: { type: String, default: '' },
    emptyMessage: { type: String, default: '' },
    valueFormatter: { type: Function, default: null },
    panelId: { type: String, default: '' },
    compact: { type: Boolean, default: false }
  },
  data() {
    return {
      summaryExpanded: false,
      isolatedSeriesIndex: null
    }
  },
  emits: ['zoom-change'],
  created() {
    this.chart = null
    this.chartEventsBound = false
    this.resizeObserver = null
    this.renderSeq = 0
    this.devicePixelRatio = this.readDevicePixelRatio()
    this.selectionRect = null
    this.dragState = null
    this.onWindowResize = () => this.handleLayoutChange()
    this.onChartDblClick = () => this.$emit('zoom-change', null)
    this.onZrMouseDown = (event) => this.handleZrMouseDown(event)
    this.onZrMouseMove = (event) => this.handleZrMouseMove(event)
    this.onZrMouseUp = (event) => this.handleZrMouseUp(event)
    this.onWindowMouseUp = () => this.handleWindowMouseUp()
  },
  computed: {
    resolvedEmptyMessage() {
      return this.emptyMessage || this.$t('observability.chart.emptyDefault')
    },
    hasData() {
      return this.chartSourceSeries.length > 0
    },
    chartReady() {
      return !this.loading && !this.error && this.hasData
    },
    chartSourceSeries() {
      if (!Array.isArray(this.series)) return []
      return this.series.filter(
        (item) => Array.isArray(item.points) && seriesHasFinitePoints(item.points)
      )
    },
    seriesChartLabels() {
      return resolveSeriesChartLabels(this.chartSourceSeries, this.panelId)
    },
    summaryRows() {
      const labels = this.seriesChartLabels
      return this.chartSourceSeries.map((item, index) => {
        const chartLabel = labels[index] || {}
        const summary = resolveSeriesSummary(item.summary, item.points)
        return {
          name: chartLabel.chartName || item.name || '',
          fullName: chartLabel.fullDetail || item.name || '',
          labels: item.labels || {},
          chartIndex: index,
          current: this.formatSummaryValue(summary.current),
          min: this.formatSummaryValue(summary.min),
          max: this.formatSummaryValue(summary.max),
          avg: this.formatSummaryValue(summary.avg)
        }
      })
    },
    summaryCollapseLimit() {
      return this.compact ? 2 : 4
    },
    canToggleSummary() {
      return this.summaryRows.length > this.summaryCollapseLimit
    },
    visibleSummaryRows() {
      if (this.summaryExpanded) return this.summaryRows
      const base = this.summaryRows.slice(0, this.summaryCollapseLimit)
      if (this.isolatedSeriesIndex === null) return base
      const isolated = this.summaryRows.find((row) => row.chartIndex === this.isolatedSeriesIndex)
      if (!isolated || base.some((row) => row.chartIndex === this.isolatedSeriesIndex)) {
        return base
      }
      return [...base, isolated]
    },
    hiddenSummaryCount() {
      if (this.summaryExpanded) return 0
      return Math.max(0, this.summaryRows.length - this.visibleSummaryRows.length)
    },
    renderedChartSeries() {
      if (this.isolatedSeriesIndex === null) return this.chartSourceSeries
      return this.chartSourceSeries.filter((_, index) => index === this.isolatedSeriesIndex)
    },
    renderedChartLabels() {
      if (this.isolatedSeriesIndex === null) return this.seriesChartLabels
      const label = this.seriesChartLabels[this.isolatedSeriesIndex]
      return label ? [label] : []
    }
  },
  watch: {
    series: { deep: true, handler() { this.resetLegendState(); this.scheduleRender() } },
    isolatedSeriesIndex() { this.scheduleRender() },
    range() { this.resetChart() },
    windowStartMs() { this.resetChart() },
    windowEndMs() { this.resetChart() },
    zoomStartMs() { this.scheduleRender() },
    zoomEndMs() { this.scheduleRender() },
    loading() { this.scheduleRender() },
    error() { this.scheduleRender() },
    chartReady() { this.scheduleRender() },
    compact() { this.resetChart() }
  },
  mounted() {
    window.addEventListener('resize', this.onWindowResize)
    this.setupResizeObserver()
    this.scheduleRender()
  },
  beforeUnmount() {
    window.removeEventListener('resize', this.onWindowResize)
    window.removeEventListener('mouseup', this.onWindowMouseUp, true)
    this.teardownResizeObserver()
    this.disposeChart()
  },
  methods: {
    seriesColor(index) {
      return CHART_PALETTE[index % CHART_PALETTE.length]
    },
    resetLegendState() {
      this.summaryExpanded = false
      this.isolatedSeriesIndex = null
    },
    resetChart() {
      this.resetLegendState()
      this.disposeChart()
      this.scheduleRender()
    },
    toggleSeriesIsolation(chartIndex) {
      this.isolatedSeriesIndex = this.isolatedSeriesIndex === chartIndex ? null : chartIndex
    },
    clearSeriesIsolation() {
      this.isolatedSeriesIndex = null
    },
    scheduleRender() {
      const seq = ++this.renderSeq
      this.$nextTick(() => {
        requestAnimationFrame(() => {
          if (seq !== this.renderSeq) return
          this.renderChart()
        })
      })
    },
    disposeChart() {
      if (this.chart) {
        if (!this.chart.isDisposed()) {
          this.unbindChartEvents()
          this.removeSelectionRect()
          this.chart.dispose()
        }
        this.chart = null
      }
      this.chartEventsBound = false
      this.dragState = null
    },
    unbindChartEvents() {
      if (!this.chart || this.chart.isDisposed() || !this.chartEventsBound) return
      const zr = this.chart.getZr()
      zr.off('mousedown', this.onZrMouseDown)
      zr.off('mousemove', this.onZrMouseMove)
      zr.off('mouseup', this.onZrMouseUp)
      zr.off('dblclick', this.onChartDblClick)
      this.chartEventsBound = false
    },
    bindChartEvents() {
      if (!this.chart || this.chart.isDisposed() || this.chartEventsBound) return
      const zr = this.chart.getZr()
      zr.on('mousedown', this.onZrMouseDown)
      zr.on('mousemove', this.onZrMouseMove)
      zr.on('mouseup', this.onZrMouseUp)
      zr.on('dblclick', this.onChartDblClick)
      this.ensureSelectionRect()
      this.chartEventsBound = true
    },
    ensureSelectionRect() {
      if (!this.chart || this.chart.isDisposed()) return
      this.removeSelectionRect()
      const zr = this.chart.getZr()
      this.selectionRect = new echarts.graphic.Rect({
        invisible: true,
        silent: true,
        z: 100,
        shape: { x: 0, y: 0, width: 0, height: 0 },
        style: {
          fill: 'rgba(111, 174, 63, 0.12)',
          stroke: 'rgba(111, 174, 63, 0.7)',
          lineWidth: 1
        }
      })
      zr.add(this.selectionRect)
    },
    removeSelectionRect() {
      if (!this.selectionRect) return
      const zr = this.chart?.getZr?.()
      if (zr) {
        zr.remove(this.selectionRect)
      }
      this.selectionRect = null
    },
    getGridRect() {
      if (!this.chart || this.chart.isDisposed()) return null
      const grid = this.chart.getModel().getComponent('grid', 0)
      const rect = grid?.coordinateSystem?.getRect?.()
      if (!rect || rect.width <= 0 || rect.height <= 0) return null
      return rect
    },
    isPointInGrid(x, y) {
      const rect = this.getGridRect()
      if (!rect) return false
      return x >= rect.x && x <= rect.x + rect.width
        && y >= rect.y && y <= rect.y + rect.height
    },
    coordToMs(value) {
      if (value instanceof Date) return value.getTime()
      const numeric = Number(value)
      if (Number.isFinite(numeric)) return numeric
      const parsed = Date.parse(String(value))
      return Number.isFinite(parsed) ? parsed : NaN
    },
    pixelToAxisMs(x, y) {
      if (!this.chart || this.chart.isDisposed()) return NaN
      const coord = this.chart.convertFromPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [x, y])
      const raw = Array.isArray(coord) ? coord[0] : coord
      return this.coordToMs(raw)
    },
    handleZrMouseDown(event) {
      if (!this.chart || this.chart.isDisposed()) return
      const button = event.event?.button
      if (button != null && button !== 0) return
      const point = [event.offsetX, event.offsetY]
      if (!this.isPointInGrid(point[0], point[1])) return
      event.event?.preventDefault?.()
      event.event?.stopPropagation?.()
      this.dragState = {
        startX: point[0],
        currentX: point[0]
      }
      window.addEventListener('mouseup', this.onWindowMouseUp, true)
      this.updateSelectionRect()
    },
    handleZrMouseMove(event) {
      if (!this.dragState) return
      this.dragState.currentX = event.offsetX
      this.updateSelectionRect()
    },
    handleZrMouseUp(event) {
      if (!this.dragState) return
      if (Number.isFinite(event?.offsetX)) {
        this.dragState.currentX = event.offsetX
      }
      this.finishDragSelection()
    },
    handleWindowMouseUp() {
      window.removeEventListener('mouseup', this.onWindowMouseUp, true)
      if (!this.dragState) return
      this.finishDragSelection()
    },
    updateSelectionRect() {
      if (!this.selectionRect || !this.dragState) return
      const gridRect = this.getGridRect()
      if (!gridRect) return
      const left = Math.min(this.dragState.startX, this.dragState.currentX)
      const right = Math.max(this.dragState.startX, this.dragState.currentX)
      const clampedLeft = Math.max(left, gridRect.x)
      const clampedRight = Math.min(right, gridRect.x + gridRect.width)
      const width = Math.max(0, clampedRight - clampedLeft)
      if (width <= 0) {
        this.hideSelectionRect()
        return
      }
      this.selectionRect.setShape({
        x: clampedLeft,
        y: gridRect.y,
        width,
        height: gridRect.height
      })
      this.selectionRect.show()
    },
    hideSelectionRect() {
      if (this.selectionRect) {
        this.selectionRect.hide()
      }
    },
    finishDragSelection() {
      window.removeEventListener('mouseup', this.onWindowMouseUp, true)
      const dragState = this.dragState
      this.dragState = null
      this.hideSelectionRect()
      if (!dragState || !this.chart || this.chart.isDisposed()) return

      const gridRect = this.getGridRect()
      if (!gridRect) return

      const left = Math.min(dragState.startX, dragState.currentX)
      const right = Math.max(dragState.startX, dragState.currentX)
      if (right - left < MIN_ZOOM_DRAG_PX) return

      const clampedLeft = Math.max(left, gridRect.x)
      const clampedRight = Math.min(right, gridRect.x + gridRect.width)
      if (clampedRight - clampedLeft < MIN_ZOOM_DRAG_PX) return

      const midY = gridRect.y + gridRect.height / 2
      let start = this.pixelToAxisMs(clampedLeft, midY)
      let end = this.pixelToAxisMs(clampedRight, midY)
      if (!Number.isFinite(start) || !Number.isFinite(end)) return
      if (start > end) {
        const swap = start
        start = end
        end = swap
      }

      const queryWindow = this.buildQueryWindow()
      start = Math.max(start, queryWindow.min)
      end = Math.min(end, queryWindow.max)
      if (end - start < MIN_ZOOM_SPAN_MS) return

      this.$emit('zoom-change', {
        startMs: Math.round(start),
        endMs: Math.round(end)
      })
    },
    isZoomActive() {
      const start = Number(this.zoomStartMs)
      const end = Number(this.zoomEndMs)
      return Number.isFinite(start) && start > 0
        && Number.isFinite(end) && end > start
    },
    setupResizeObserver() {
      if (this.resizeObserver || !this.$refs.chartEl) return
      this.resizeObserver = new ResizeObserver(() => {
        this.handleLayoutChange()
      })
      this.resizeObserver.observe(this.$refs.chartEl)
    },
    teardownResizeObserver() {
      if (this.resizeObserver) {
        this.resizeObserver.disconnect()
        this.resizeObserver = null
      }
    },
    readDevicePixelRatio() {
      return window.devicePixelRatio || 1
    },
    chartInitOptions() {
      return {
        renderer: 'svg',
        devicePixelRatio: this.readDevicePixelRatio()
      }
    },
    handleLayoutChange() {
      const dpr = this.readDevicePixelRatio()
      if (this.chart && !this.chart.isDisposed() && dpr !== this.devicePixelRatio) {
        this.devicePixelRatio = dpr
        this.reinitChart()
        return
      }
      this.resizeChart()
    },
    reinitChart() {
      if (!this.chartReady) return
      this.disposeChart()
      this.initChart()
      if (!this.chart || this.chart.isDisposed()) return
      this.setChartOption()
      this.bindChartEvents()
    },
    resizeChart() {
      if (!this.chart || this.chart.isDisposed()) return
      const el = this.$refs.chartEl
      if (!el) return
      const { width, height } = el.getBoundingClientRect()
      if (width <= 0 || height <= 0) return
      this.chart.resize({
        width: Math.round(width),
        height: Math.round(height)
      })
    },
    initChart() {
      if (!this.$refs.chartEl) return
      const dpr = this.readDevicePixelRatio()
      if (this.chart && !this.chart.isDisposed()) {
        if (dpr === this.devicePixelRatio) return
        this.disposeChart()
      }
      this.devicePixelRatio = dpr
      this.chart = markRaw(echarts.init(this.$refs.chartEl, null, this.chartInitOptions()))
    },
    formatTooltipTime(value) {
      const date = new Date(Number(value))
      if (Number.isNaN(date.getTime())) return ''
      if (this.compact) {
        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
      }
      return date.toLocaleString()
    },
    buildSeriesMeta(labels) {
      const metaByName = {}
      labels.forEach((entry) => {
        metaByName[entry.chartName] = {
          displayName: entry.chartName,
          fullDetail: entry.fullDetail
        }
      })
      return metaByName
    },
    formatAxisTooltip(params, metaByName) {
      if (!Array.isArray(params) || !params.length) return ''
      const header = `<div style="margin-bottom:4px;font-weight:600;color:#1f2a1c;font-size:11px;white-space:normal">${this.formatTooltipTime(params[0].axisValue)}</div>`
      const rows = params.map((param) => {
        const meta = metaByName[param.seriesName] || {}
        const label = meta.displayName || param.seriesName
        const rawValue = Array.isArray(param.value) ? param.value[1] : param.value
        const value = this.formatValue(rawValue)
        const detail = meta.fullDetail
        const showDetail = detail && detail !== label && detail !== param.seriesName
        const detailLine = showDetail
          ? `<div style="margin-top:2px;color:#7a8a78;font-size:10px;font-weight:400;line-height:1.35;white-space:normal;overflow-wrap:anywhere">${detail}</div>`
          : ''
        return `<div style="margin-top:3px;line-height:1.35;white-space:normal;overflow-wrap:anywhere">${param.marker} ${label}: <strong>${value}</strong>${detailLine}</div>`
      })
      return header + rows.join('')
    },
    formatAxisValue(value) {
      if (this.valueFormatter) {
        const formatted = this.valueFormatter(value)
        return truncateLabel(String(formatted), 8)
      }
      const num = Number(value || 0)
      if (Math.abs(num) >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`
      if (Math.abs(num) >= 1_000) return `${(num / 1_000).toFixed(1)}k`
      if (Number.isInteger(num)) return String(num)
      return num.toFixed(1)
    },
    formatValue(value) {
      if (this.valueFormatter) return this.valueFormatter(value)
      return Number(value || 0).toLocaleString()
    },
    formatSummaryValue(value) {
      if (value == null || !Number.isFinite(Number(value))) return '—'
      return this.formatValue(Number(value))
    },
    buildQueryWindow() {
      const start = Number(this.windowStartMs)
      const end = Number(this.windowEndMs)
      if (Number.isFinite(start) && start > 0 && Number.isFinite(end) && end > start) {
        return { min: start, max: end, span: end - start }
      }
      const rangeMs = rangeToMs(this.range)
      const maxTs = end > 0 ? end : Date.now()
      return {
        min: maxTs - rangeMs,
        max: maxTs,
        span: rangeMs
      }
    },
    fitTimeAxisToData(queryWindow, dataTimestamps) {
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
    },
    buildTimeAxisRange(dataTimestamps = []) {
      const queryWindow = this.buildQueryWindow()
      if (this.isZoomActive()) {
        return {
          min: Math.max(queryWindow.min, Number(this.zoomStartMs)),
          max: Math.min(queryWindow.max, Number(this.zoomEndMs))
        }
      }
      return this.fitTimeAxisToData(queryWindow, dataTimestamps)
    },
    buildChartOption() {
      const sourceSeries = this.renderedChartSeries
      const chartLabels = this.renderedChartLabels.length
        ? this.renderedChartLabels
        : resolveSeriesChartLabels(sourceSeries, this.panelId)
      const metaByName = this.buildSeriesMeta(chartLabels)
      const echartsSeries = sourceSeries.map((item, index) => {
        const sourceIndex = this.isolatedSeriesIndex === null
          ? index
          : this.isolatedSeriesIndex
        const color = CHART_PALETTE[sourceIndex % CHART_PALETTE.length]
        const chartName = chartLabels[index]?.chartName || item.name || `series-${index}`
        return {
          id: `${this.panelId}-${index}`,
          name: chartName,
          type: 'line',
          showSymbol: false,
          smooth: false,
          lineStyle: { width: this.compact ? 1.5 : 2 },
          color,
          areaStyle: this.compact ? { color, opacity: 0.1 } : undefined,
          data: item.points.map(([ts, value]) => [ts, value])
        }
      })

      const timeAxis = this.buildTimeAxisRange(
        echartsSeries.flatMap((item) => item.data.map(([ts]) => ts))
      )
      const compact = this.compact

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
          formatter: (params) => this.formatAxisTooltip(params, metaByName)
        },
        legend: { show: false },
        xAxis: {
          type: 'time',
          min: timeAxis.min,
          max: timeAxis.max,
          axisLabel: {
            fontFamily: CHART_AXIS_FONT,
            color: CHART_AXIS_COLOR,
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
            color: CHART_AXIS_COLOR,
            fontSize: compact ? 12 : 13,
            margin: compact ? 4 : 10,
            formatter: (value) => this.formatAxisValue(value)
          },
          splitLine: { lineStyle: { color: '#edf4e9' } }
        },
        series: echartsSeries
      }
    },
    setChartOption() {
      if (!this.chart || this.chart.isDisposed()) return
      this.chart.setOption(this.buildChartOption(), {
        notMerge: true,
        replaceMerge: ['xAxis', 'yAxis', 'series']
      })
      this.resizeChart()
    },
    renderChart() {
      if (!this.chartReady) return

      this.initChart()
      if (!this.chart || this.chart.isDisposed()) return

      this.setChartOption()
      this.bindChartEvents()
      this.handleLayoutChange()
    }
  }
}
</script>

<style scoped>
.obs-chart-panel {
  background: var(--admin-surface);
  border: 1px solid var(--admin-border);
  border-radius: var(--admin-radius);
  padding: 16px;
  min-width: 0;
}
.obs-chart-panel-compact {
  padding: 10px 12px 8px;
}
.obs-chart-heading {
  margin-bottom: 8px;
  min-width: 0;
}
.obs-chart-heading h2 {
  margin: 0;
  font-size: 16px;
  line-height: 1.3;
  font-weight: 760;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.obs-chart-panel-compact .obs-chart-heading {
  margin-bottom: 6px;
}
.obs-chart-panel-compact .obs-chart-heading h2 {
  font-size: 13px;
  font-weight: 700;
}
.obs-chart-heading p {
  margin: 4px 0 0;
  color: var(--admin-muted);
  font-size: 12px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.obs-chart-body {
  position: relative;
  min-height: 320px;
}
.obs-chart-panel-compact .obs-chart-body {
  min-height: 168px;
}
.obs-chart-canvas {
  width: 100%;
  min-height: 280px;
  height: 320px;
  max-width: 100%;
  overflow: hidden;
  cursor: crosshair;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}
.obs-chart-panel-compact .obs-chart-canvas {
  height: 168px;
  min-height: 148px;
}
.obs-chart-overlay {
  position: absolute;
  inset: 0;
  z-index: 1;
  display: grid;
  place-items: center;
  background: var(--admin-surface);
  font-size: 12px;
}
.obs-chart-error {
  padding: 12px;
  border-radius: var(--admin-radius);
  background: var(--admin-danger-soft);
  color: var(--admin-danger);
  font-size: 12px;
  align-content: center;
}
.obs-series-summary-wrap {
  margin-top: 8px;
  max-width: 100%;
  overflow-x: auto;
}
.obs-series-summary-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 11px;
  table-layout: fixed;
}
.obs-series-summary-table th,
.obs-series-summary-table td {
  padding: 2px 4px;
  text-align: right;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.obs-series-summary-table th:first-child,
.obs-series-summary-table td:first-child {
  text-align: left;
  width: 38%;
}
.obs-series-summary-table thead th {
  color: var(--admin-muted);
  font-weight: 600;
  font-size: 10px;
}
.obs-series-summary-row {
  cursor: pointer;
  transition: background-color 0.12s ease;
}
.obs-series-summary-row:hover {
  background: #f3f8ef;
}
.obs-series-summary-row.is-isolated {
  background: #edf6e6;
}
.obs-series-summary-row.is-dimmed {
  opacity: 0.45;
}
.obs-series-summary-row.is-dimmed:hover {
  opacity: 0.72;
}
.obs-series-label {
  display: flex;
  align-items: center;
  gap: 5px;
  min-width: 0;
}
.obs-series-dot {
  flex: 0 0 8px;
  width: 8px;
  height: 8px;
  border-radius: 999px;
}
.obs-series-label span:last-child {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.obs-summary-toggle {
  margin-top: 4px;
  border: 0;
  background: transparent;
  color: var(--admin-accent);
  font-size: 11px;
  font-weight: 650;
  cursor: pointer;
  padding: 0;
}
.obs-summary-toggle:hover { text-decoration: underline; }
.obs-summary-reset {
  margin-top: 4px;
  margin-left: 10px;
  border: 0;
  background: transparent;
  color: var(--admin-muted);
  font-size: 11px;
  font-weight: 600;
  cursor: pointer;
  padding: 0;
}
.obs-summary-reset:hover {
  color: var(--admin-accent);
  text-decoration: underline;
}
@media (max-width: 760px) {
  .obs-chart-body { min-height: 220px; }
  .obs-chart-canvas { height: 220px; min-height: 180px; }
  .obs-chart-panel-compact .obs-chart-body { min-height: 160px; }
  .obs-chart-panel-compact .obs-chart-canvas { height: 160px; min-height: 140px; }
}
</style>
