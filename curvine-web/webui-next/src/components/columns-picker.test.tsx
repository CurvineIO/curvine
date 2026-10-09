import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { useState } from 'react'
import { describe, expect, it, vi } from 'vitest'

import { ColumnsPicker, type ColumnOption } from './columns-picker'
import { I18nProvider } from '@/i18n'

type Key = 'alpha' | 'beta' | 'gamma'

const COLUMNS: ColumnOption<Key>[] = [
  { key: 'alpha', labelKey: 'common.columns.button' },
  { key: 'beta', labelKey: 'common.columns.title' },
  { key: 'gamma', labelKey: 'common.columns.hint' }
]
const DEFAULTS: Key[] = ['alpha']

/** Drives the picker the way a page does, so the reported state is observable. */
function Harness({ onChange }: { onChange(next: Key[]): void }) {
  const [visible, setVisible] = useState<Key[]>(DEFAULTS)
  return (
    <I18nProvider>
      <ColumnsPicker
        columns={COLUMNS}
        visible={visible}
        defaultVisible={DEFAULTS}
        testIdPrefix="test-columns"
        onChange={(next) => {
          setVisible(next)
          onChange(next)
        }}
      />
      <output data-testid="visible">{visible.join(',')}</output>
    </I18nProvider>
  )
}

async function openPicker() {
  await userEvent.click(screen.getByTestId('test-columns-trigger'))
}

describe('ColumnsPicker', () => {
  it('reports the selection in declaration order, not tick order', async () => {
    const onChange = vi.fn()
    render(<Harness onChange={onChange} />)
    await openPicker()

    // Ticked back-to-front on purpose.
    await userEvent.click(screen.getByTestId('test-columns-option-gamma'))
    await userEvent.click(screen.getByTestId('test-columns-option-beta'))

    expect(onChange).toHaveBeenLastCalledWith(['alpha', 'beta', 'gamma'])
    expect(screen.getByTestId('visible')).toHaveTextContent('alpha,beta,gamma')
  })

  it('unticks a column that is currently visible', async () => {
    const onChange = vi.fn()
    render(<Harness onChange={onChange} />)
    await openPicker()

    await userEvent.click(screen.getByTestId('test-columns-option-alpha'))

    expect(onChange).toHaveBeenLastCalledWith([])
    expect(screen.getByTestId('visible')).toHaveTextContent('')
  })

  it('resets to the defaults the page declared', async () => {
    const onChange = vi.fn()
    render(<Harness onChange={onChange} />)
    await openPicker()
    await userEvent.click(screen.getByTestId('test-columns-option-gamma'))
    expect(screen.getByTestId('visible')).toHaveTextContent('alpha,gamma')

    await userEvent.click(screen.getByTestId('test-columns-reset'))

    expect(onChange).toHaveBeenLastCalledWith(['alpha'])
    expect(screen.getByTestId('visible')).toHaveTextContent('alpha')
  })

  it('lists every declared column with its translated label', async () => {
    render(<Harness onChange={() => {}} />)
    await openPicker()

    for (const column of COLUMNS) {
      expect(screen.getByTestId(`test-columns-option-${column.key}`)).toBeInTheDocument()
    }
  })
})
