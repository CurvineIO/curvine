import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'

import { EmptyNotice } from './data-state'
import { ResourceTable, StatusBadge, statusTone, workerStatusTone } from './resource-table'
import { TableRow, TableCell } from './resource-table'

describe('statusTone', () => {
  it('maps the Curvine status vocabulary to tones', () => {
    expect(statusTone('live')).toBe('positive')
    expect(statusTone('Healthy')).toBe('positive')
    expect(statusTone('running')).toBe('positive')
    expect(statusTone('standby')).toBe('info')
    expect(statusTone('decommission')).toBe('info')
    expect(statusTone('waiting')).toBe('warning')
    expect(statusTone('blacklist')).toBe('warning')
    expect(statusTone('lost')).toBe('negative')
    expect(statusTone('failed')).toBe('negative')
    expect(statusTone('skipped')).toBe('neutral')
  })

  it('falls back to neutral for unknown or empty values', () => {
    expect(statusTone('something-new')).toBe('neutral')
    expect(statusTone('')).toBe('neutral')
    expect(statusTone(null)).toBe('neutral')
  })
})

describe('workerStatusTone', () => {
  // Keyed on the raw backend state, so the colour survives localisation.
  it('colours online green and offline red', () => {
    expect(workerStatusTone('Live')).toBe('positive')
    expect(workerStatusTone('lost')).toBe('negative')
  })

  it('distinguishes the managed states', () => {
    expect(workerStatusTone('Blacklist')).toBe('warning')
    expect(workerStatusTone('Decommission')).toBe('info')
  })

  it('falls back to neutral', () => {
    expect(workerStatusTone('')).toBe('neutral')
    expect(workerStatusTone(null)).toBe('neutral')
    expect(workerStatusTone('something-new')).toBe('neutral')
  })
})

describe('StatusBadge', () => {
  it('renders the value and exposes its tone', () => {
    render(<StatusBadge value="lost" />)
    const badge = screen.getByText('lost')
    expect(badge).toHaveAttribute('data-tone', 'negative')
  })
})

describe('ResourceTable', () => {
  const columns = ['Name', 'State']

  it('renders an error alert instead of the table', () => {
    render(<ResourceTable columns={columns} rows={null} loading={false} error="boom" emptyTitle="Empty" />)
    expect(screen.getByText('boom')).toBeInTheDocument()
    expect(screen.queryByRole('table')).not.toBeInTheDocument()
  })

  it('renders skeleton rows while loading', () => {
    render(<ResourceTable columns={columns} rows={null} loading error={null} emptyTitle="Empty" />)
    expect(screen.getByRole('table')).toBeInTheDocument()
    expect(screen.getByText('Name')).toBeInTheDocument()
  })

  it('renders the empty notice when there are no rows', () => {
    render(<ResourceTable columns={columns} rows={[]} loading={false} error={null} emptyTitle="暂无数据" />)
    expect(screen.getByText('暂无数据')).toBeInTheDocument()
    expect(screen.queryByRole('table')).not.toBeInTheDocument()
  })

  it('renders the supplied rows', () => {
    render(
      <ResourceTable
        columns={columns}
        loading={false}
        error={null}
        emptyTitle="Empty"
        rows={
          <TableRow>
            <TableCell>worker-01</TableCell>
            <TableCell>live</TableCell>
          </TableRow>
        }
      />
    )
    expect(screen.getByText('worker-01')).toBeInTheDocument()
  })
})

describe('EmptyNotice', () => {
  it('renders title and description', () => {
    render(<EmptyNotice title="Nothing here" description="Create one first." />)
    expect(screen.getByText('Nothing here')).toBeInTheDocument()
    expect(screen.getByText('Create one first.')).toBeInTheDocument()
  })
})
