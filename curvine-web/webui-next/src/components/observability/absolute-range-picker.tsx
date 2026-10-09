import { useEffect, useState } from 'react'
import { CalendarClockIcon } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { useT } from '@/i18n'
import { cn } from '@/lib/utils'

export interface TimeWindow {
  startMs: number
  endMs: number
}

/** Milliseconds → the `YYYY-MM-DDTHH:mm` a `datetime-local` input wants, in local time. */
export function msToLocalInput(ms: number): string {
  const date = new Date(Number(ms))
  if (!Number.isFinite(ms) || Number.isNaN(date.getTime())) return ''
  const pad = (value: number) => String(value).padStart(2, '0')
  return [
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`,
    `${pad(date.getHours())}:${pad(date.getMinutes())}`
  ].join('T')
}

/**
 * `datetime-local` value → milliseconds. The form has no timezone, and `Date.parse`
 * treats a date-time without one as local time — which is exactly what the input means.
 */
export function localInputToMs(value: string): number {
  if (!value) return NaN
  const parsed = Date.parse(value)
  return Number.isFinite(parsed) ? parsed : NaN
}

/** Trigger label: seconds matter at the low end of the presets, dates at the high end. */
export function formatWindowLabel(window: TimeWindow, spansDays = false): string {
  const time = (ms: number, withDate: boolean) => {
    const date = new Date(Number(ms))
    if (Number.isNaN(date.getTime())) return '-'
    const pad = (value: number) => String(value).padStart(2, '0')
    const clock = `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`
    const day = `${pad(date.getMonth() + 1)}-${pad(date.getDate())}`
    return withDate ? `${day} ${clock}` : clock
  }
  const withDate = spansDays || window.endMs - window.startMs > 24 * 60 * 60 * 1000
  return `${time(window.startMs, withDate)} – ${time(window.endMs, withDate)}`
}

/**
 * Grafana-style absolute time range: the window the charts display, as an editable
 * start/end pair. `datetime-local` gives both a picker and free typing.
 *
 * Applying narrows the displayed window inside the data already loaded (`bounds`); a
 * wider window needs a wider preset, because the dashboard endpoint queries by preset
 * duration only and has no way to ask for an arbitrary window.
 *
 * The draft follows `value` while the popover is closed, so dragging a range on a chart
 * is reflected here — opening the popover shows the dragged window ready to adjust.
 */
export function AbsoluteRangePicker({
  value,
  bounds,
  active,
  onApply,
  onClear,
  testIdPrefix = 'observability-absolute-range'
}: {
  value: TimeWindow | null
  bounds: TimeWindow | null
  active: boolean
  onApply(range: TimeWindow): void
  onClear(): void
  testIdPrefix?: string
}) {
  const t = useT()
  const [open, setOpen] = useState(false)
  const [from, setFrom] = useState('')
  const [to, setTo] = useState('')

  const startMs = localInputToMs(from)
  const endMs = localInputToMs(to)
  const valid = Number.isFinite(startMs) && Number.isFinite(endMs) && endMs > startMs

  useEffect(() => {
    if (open) return
    const source = value ?? bounds
    setFrom(source ? msToLocalInput(source.startMs) : '')
    setTo(source ? msToLocalInput(source.endMs) : '')
  }, [open, value?.startMs, value?.endMs, bounds?.startMs, bounds?.endMs])

  const label = active && value ? formatWindowLabel(value) : t('observability.toolbar.absoluteRange')
  const boundsLabel = bounds ? formatWindowLabel(bounds, true) : ''

  function apply() {
    if (!valid) return
    // Clamped to the loaded window: outside it there are no samples to draw.
    const start = bounds ? Math.max(startMs, bounds.startMs) : startMs
    const end = bounds ? Math.min(endMs, bounds.endMs) : endMs
    if (end <= start) return
    onApply({ startMs: start, endMs: end })
    setOpen(false)
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          aria-label={t('observability.toolbar.absoluteRange')}
          data-active={active ? 'true' : undefined}
          data-testid={testIdPrefix}
          className={cn('gap-1.5 tabular-nums', active && 'border-primary/50 text-foreground')}
        >
          <CalendarClockIcon className="size-3.5 shrink-0" />
          {label}
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-80" data-testid={`${testIdPrefix}-popover`}>
        <div className="flex flex-col gap-3">
          <p className="font-heading text-sm font-semibold">
            {t('observability.toolbar.absoluteRange')}
          </p>

          <label className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground">
              {t('observability.toolbar.from')}
            </span>
            <Input
              type="datetime-local"
              value={from}
              min={bounds ? msToLocalInput(bounds.startMs) : undefined}
              max={bounds ? msToLocalInput(bounds.endMs) : undefined}
              onChange={(event) => setFrom(event.target.value)}
              data-testid={`${testIdPrefix}-from`}
            />
          </label>

          <label className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground">{t('observability.toolbar.to')}</span>
            <Input
              type="datetime-local"
              value={to}
              min={bounds ? msToLocalInput(bounds.startMs) : undefined}
              max={bounds ? msToLocalInput(bounds.endMs) : undefined}
              onChange={(event) => setTo(event.target.value)}
              data-testid={`${testIdPrefix}-to`}
            />
          </label>

          {boundsLabel && (
            <p className="text-[11px] leading-snug text-muted-foreground">
              {t('observability.toolbar.absoluteNote', { window: boundsLabel })}
            </p>
          )}

          <div className="flex items-center justify-end gap-2">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => {
                onClear()
                setOpen(false)
              }}
              data-testid={`${testIdPrefix}-clear`}
            >
              {t('observability.toolbar.clearRange')}
            </Button>
            <Button
              size="sm"
              disabled={!valid}
              onClick={apply}
              data-testid={`${testIdPrefix}-apply`}
            >
              {t('common.apply')}
            </Button>
          </div>
        </div>
      </PopoverContent>
    </Popover>
  )
}
