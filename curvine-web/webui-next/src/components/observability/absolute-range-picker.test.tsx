import { fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  AbsoluteRangePicker,
  formatWindowLabel,
  localInputToMs,
  msToLocalInput
} from './absolute-range-picker'
import { I18nProvider, LOCALE_STORAGE_KEY } from '@/i18n'

const START = new Date('2026-09-19T10:00:00').getTime()
const END = new Date('2026-09-19T11:00:00').getTime()
const LOADED = { startMs: START - 3_600_000, endMs: END + 3_600_000 }

type PickerProps = React.ComponentProps<typeof AbsoluteRangePicker>

function renderPicker(props: Partial<PickerProps> = {}) {
  const onApply = vi.fn()
  const onClear = vi.fn()
  const view = render(
    <I18nProvider>
      <AbsoluteRangePicker
        value={{ startMs: START, endMs: END }}
        bounds={LOADED}
        active={false}
        onApply={onApply}
        onClear={onClear}
        {...props}
      />
    </I18nProvider>
  )
  const rerender = (next: Partial<PickerProps>) =>
    view.rerender(
      <I18nProvider>
        <AbsoluteRangePicker
          value={{ startMs: START, endMs: END }}
          bounds={LOADED}
          active={false}
          onApply={onApply}
          onClear={onClear}
          {...props}
          {...next}
        />
      </I18nProvider>
    )
  return { onApply, onClear, rerender }
}

const trigger = () => screen.getByTestId('observability-absolute-range')

describe('absolute range helpers', () => {
  it('round-trips a timestamp through the input format', () => {
    expect(localInputToMs(msToLocalInput(START))).toBe(START)
  })

  it('ignores an empty or unparseable input', () => {
    expect(Number.isNaN(localInputToMs(''))).toBe(true)
    expect(Number.isNaN(localInputToMs('not a date'))).toBe(true)
  })

  it('labels a short window by clock time and a long one by date', () => {
    expect(formatWindowLabel({ startMs: START, endMs: END })).toBe('10:00:00 – 11:00:00')
    expect(formatWindowLabel({ startMs: START, endMs: START + 30 * 3_600_000 })).toContain('09-19')
  })
})

describe('AbsoluteRangePicker', () => {
  beforeEach(() => {
    window.localStorage.setItem(LOCALE_STORAGE_KEY, 'en')
  })

  it('names the control until a window is applied', () => {
    renderPicker()
    expect(trigger()).toHaveTextContent('Absolute range')
    expect(trigger()).not.toHaveAttribute('data-active')
  })

  it('shows the applied window and follows one set elsewhere', () => {
    const { rerender } = renderPicker({ active: true })

    expect(trigger()).toHaveTextContent('10:00:00 – 11:00:00')
    expect(trigger()).toHaveAttribute('data-active', 'true')

    // A range dragged out on a chart arrives as a new value; the control follows it.
    rerender({ value: { startMs: START + 300_000, endMs: START + 900_000 } })

    expect(trigger()).toHaveTextContent('10:05:00 – 10:15:00')
  })

  it('opens on the current window and applies the edited range', async () => {
    const { onApply } = renderPicker({ active: true })
    await userEvent.click(trigger())

    expect(screen.getByTestId('observability-absolute-range-from')).toHaveValue('2026-09-19T10:00')
    expect(screen.getByTestId('observability-absolute-range-to')).toHaveValue('2026-09-19T11:00')

    // Typed by hand, which is why the control is a real input rather than a calendar.
    fireEvent.change(screen.getByTestId('observability-absolute-range-from'), {
      target: { value: '2026-09-19T10:30' }
    })
    await userEvent.click(screen.getByTestId('observability-absolute-range-apply'))

    expect(onApply).toHaveBeenCalledWith({
      startMs: new Date('2026-09-19T10:30').getTime(),
      endMs: END
    })
  })

  it('refuses a window that ends before it starts', async () => {
    const { onApply } = renderPicker()
    await userEvent.click(trigger())

    fireEvent.change(screen.getByTestId('observability-absolute-range-to'), {
      target: { value: '2026-09-19T09:00' }
    })

    expect(screen.getByTestId('observability-absolute-range-apply')).toBeDisabled()
    await userEvent.click(screen.getByTestId('observability-absolute-range-apply'))
    expect(onApply).not.toHaveBeenCalled()
  })

  it('clamps an applied window to the data that was loaded', async () => {
    const { onApply } = renderPicker()
    await userEvent.click(trigger())

    fireEvent.change(screen.getByTestId('observability-absolute-range-from'), {
      target: { value: '2020-01-01T00:00' }
    })
    await userEvent.click(screen.getByTestId('observability-absolute-range-apply'))

    expect(onApply).toHaveBeenCalledWith({ startMs: LOADED.startMs, endMs: END })
  })

  it('clears the range', async () => {
    const { onClear } = renderPicker({ active: true })
    await userEvent.click(trigger())
    await userEvent.click(screen.getByTestId('observability-absolute-range-clear'))

    expect(onClear).toHaveBeenCalledTimes(1)
  })
})
