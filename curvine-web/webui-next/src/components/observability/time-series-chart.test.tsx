import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'

import { TimeSeriesChart, createDragSelectionRect } from './time-series-chart'
import { I18nProvider } from '@/i18n'
import { ThemeProvider } from '@/theme/theme-context'
import type { DashboardSeries } from '@/api/types'

/** A panel with more series than the collapsed limit, i.e. a long summary list. */
function manySeries(count: number): DashboardSeries[] {
  return Array.from({ length: count }, (_, index) => ({
    name: `worker-${index}.cpu`,
    labels: { instance: `worker-${index}:8995` },
    points: [
      [1_750_000_000_000, 1],
      [1_750_000_015_000, 4]
    ] as [number, number][],
    summary: { current: 2, min: 1, max: 4, avg: 2.5 }
  }))
}

function series(): DashboardSeries[] {
  return [
    {
      name: 'master.cpu',
      labels: { instance: 'master-1:9010' },
      points: [
        [1_750_000_000_000, 1],
        [1_750_000_015_000, 4],
        [1_750_000_030_000, 2]
      ],
      summary: { current: 2, min: 1, max: 4, avg: 2.33 }
    }
  ]
}

function renderChart(props: Partial<React.ComponentProps<typeof TimeSeriesChart>> = {}) {
  return render(
    <ThemeProvider>
      <I18nProvider>
        <TimeSeriesChart
          title="CPU"
          subtitle="usage"
          series={series()}
          range="15m"
          windowStartMs={1_750_000_000_000}
          windowEndMs={1_750_000_030_000}
          loading={false}
          error=""
          emptyMessage="no data"
          panelId="cpu"
          {...props}
        />
      </I18nProvider>
    </ThemeProvider>
  )
}

describe('TimeSeriesChart', () => {
  it('renders the title and mounts the chart surface for a populated series', () => {
    renderChart()
    expect(screen.getByText('CPU')).toBeInTheDocument()
    expect(screen.getByTestId('time-series-chart-canvas')).toBeInTheDocument()
  })

  it('shows the error state instead of a chart', () => {
    renderChart({ error: 'boom' })
    expect(screen.getByText('boom')).toBeInTheDocument()
  })

  it('shows the empty state when there are no series', () => {
    renderChart({ series: [] })
    expect(screen.getByText('no data')).toBeInTheDocument()
  })

  it('renders the per-series summary row', () => {
    renderChart()
    // current / min / max / avg are rendered from the backend-provided summary.
    expect(screen.getByTestId('time-series-chart-summary-row-0')).toBeInTheDocument()
  })
})

/*
 * zrender's `show()`/`hide()` toggle `ignore`, while `invisible` is a separate flag
 * `show()` never clears. The rect used to be built with `invisible: true`, so the
 * drag-to-zoom band was never painted and dragging gave no feedback at all.
 */
describe('drag selection mask', () => {
  it('is hidden until shown, and paintable once shown', () => {
    const rect = createDragSelectionRect()

    expect(rect.shouldBePainted(1000, 1000, false, false)).toBe(false)

    rect.show()

    expect(rect.shouldBePainted(1000, 1000, false, false)).toBe(true)
    expect(rect.invisible).not.toBe(true)
  })

  it('covers the dragged range and hides again when the drag ends', () => {
    const rect = createDragSelectionRect()

    rect.setShape({ x: 12, y: 0, width: 48, height: 200 })
    rect.show()

    expect(rect.shape).toMatchObject({ x: 12, width: 48 })

    rect.hide()
    expect(rect.shouldBePainted(1000, 1000, false, false)).toBe(false)
  })
})

describe('panel zoom', () => {
  it('enlarges and restores from the header control', async () => {
    renderChart()
    const card = screen.getByTestId('time-series-chart')
    const toggle = screen.getByTestId('time-series-chart-enlarge')

    expect(card).not.toHaveAttribute('data-enlarged')

    await userEvent.click(toggle)

    expect(card).toHaveAttribute('data-enlarged', 'true')
    expect(card.className).toContain('fixed')
    expect(card).toHaveAttribute('role', 'dialog')

    await userEvent.click(toggle)

    expect(card).not.toHaveAttribute('data-enlarged')
    expect(card.className).not.toContain('fixed')
  })

  it('toggles with v over the panel, and restores with Escape', async () => {
    renderChart()
    const card = screen.getByTestId('time-series-chart')

    // The shortcut is scoped to the panel, so it stays out of the way elsewhere.
    await userEvent.keyboard('v')
    expect(card).not.toHaveAttribute('data-enlarged')

    await userEvent.hover(card)
    await userEvent.keyboard('v')
    expect(card).toHaveAttribute('data-enlarged', 'true')

    await userEvent.keyboard('{Escape}')
    expect(card).not.toHaveAttribute('data-enlarged')
  })

  it('keeps the same chart surface mounted, so the zoom range survives', async () => {
    renderChart()
    const canvas = screen.getByTestId('time-series-chart-canvas')

    await userEvent.click(screen.getByTestId('time-series-chart-enlarge'))

    // Same node: enlarging re-boxes the existing ECharts instance rather than
    // re-creating it, so a selected range is not lost.
    expect(screen.getByTestId('time-series-chart-canvas')).toBe(canvas)
  })

  it('locks the page behind while the panel is enlarged', async () => {
    renderChart()
    const toggle = screen.getByTestId('time-series-chart-enlarge')

    await userEvent.click(toggle)
    expect(document.body.style.overflow).toBe('hidden')

    await userEvent.click(toggle)
    expect(document.body.style.overflow).toBe('')
  })

  it('scrolls a long series list inside the panel instead of over the plot', async () => {
    // More series than the collapsed limit, so the list is a real block of rows.
    renderChart({ series: manySeries(12) })
    await userEvent.click(screen.getByTestId('time-series-chart-enlarge'))

    const plot = screen.getByTestId('time-series-chart-canvas').parentElement
    const summary = screen.getByTestId('time-series-chart-summary')

    // `min-h-0` here let the flex item shrink below the canvas's own floor, and the
    // canvas then painted over the list below it.
    expect(plot?.className).toContain('min-h-[280px]')
    expect(plot?.className).not.toContain('min-h-0')
    expect(summary.className).toContain('min-h-0')
    expect(summary.className).toContain('overflow-y-auto')
  })

  it('morphs the resize when the browser supports view transitions', async () => {
    const startViewTransition = vi.fn((callback: () => void) => {
      callback()
      return {
        finished: Promise.resolve(),
        ready: Promise.resolve(),
        updateCallbackDone: Promise.resolve(),
        skipTransition: () => {}
      } as unknown as ViewTransition
    })
    Object.defineProperty(document, 'startViewTransition', {
      value: startViewTransition,
      configurable: true
    })

    try {
      renderChart()
      const window = screen.getByTestId('time-series-chart')

      await userEvent.click(screen.getByTestId('time-series-chart-enlarge'))

      expect(startViewTransition).toHaveBeenCalledTimes(1)
      // The state still lands, so a browser that plays the morph ends up in the same place.
      expect(window).toHaveAttribute('data-enlarged', 'true')
    } finally {
      delete (document as { startViewTransition?: unknown }).startViewTransition
    }
  })
})
