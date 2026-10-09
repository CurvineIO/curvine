import { describe, expect, it } from 'vitest'
import en from '@/i18n/messages/en'
import zhCN from '@/i18n/messages/zh-CN'
import { readStoredColumns } from './column-visibility'
import {
  DEFAULT_VISIBLE_TRANSFER_COLUMNS,
  OPTIONAL_TRANSFER_COLUMNS,
  PINNED_TRANSFER_COLUMNS,
  TRANSFER_COLUMNS,
  normalizeTransferColumns,
  orderTransferColumns
} from './transferColumns'

function hasKey(tree: unknown, key: string): boolean {
  let node: unknown = tree
  for (const part of key.split('.')) {
    if (node === null || typeof node !== 'object') return false
    node = (node as Record<string, unknown>)[part]
  }
  return node !== undefined
}

describe('transfer column definitions', () => {
  // These labels are resolved through a dynamic `t(column.labelKey)`, so the i18n
  // static-key scan cannot see them — assert them here instead.
  it('labels every column in both locales', () => {
    for (const key of TRANSFER_COLUMNS.map((column) => column.labelKey)) {
      expect(hasKey(en, key), `en is missing ${key}`).toBe(true)
      expect(hasKey(zhCN, key), `zh-CN is missing ${key}`).toBe(true)
    }
  })

  it('starts with every column visible', () => {
    expect(DEFAULT_VISIBLE_TRANSFER_COLUMNS).toEqual(TRANSFER_COLUMNS.map((column) => column.key))
  })

  it('keeps declaration order regardless of the order columns were ticked', () => {
    expect(orderTransferColumns(['updated', 'kind']).map((column) => column.key)).toEqual([
      'kind',
      'updated'
    ])
    expect(orderTransferColumns([])).toEqual([])
  })

  it('gives every optional column a width', () => {
    for (const column of TRANSFER_COLUMNS) {
      expect(column.width, `${column.key} needs a width`).toMatch(/^\d+(\.\d+)?(%|rem)$/)
    }
  })

  it('leaves room for the two fixed columns when nothing is hidden', () => {
    // Job ID (14%) and Actions (10%) are not in TRANSFER_COLUMNS, so the optional
    // widths must not fill the table on their own.
    const optional = TRANSFER_COLUMNS.reduce((sum, column) => sum + Number.parseFloat(column.width), 0)
    expect(optional).toBeLessThanOrEqual(100 - 14 - 10)
  })

  it('offers the state column to the picker but keeps it in the layout', () => {
    expect(OPTIONAL_TRANSFER_COLUMNS.map((column) => column.key)).not.toContain('status')
    expect(PINNED_TRANSFER_COLUMNS).toEqual(['status'])
  })

  it('re-adds the pinned columns to any selection, in declaration order', () => {
    // Nothing ticked, and a selection that predates the pin: both keep State.
    expect(normalizeTransferColumns([])).toEqual(['status'])
    expect(normalizeTransferColumns(['updated', 'kind'])).toEqual(['kind', 'status', 'updated'])
    // Already-pinned keys are not duplicated.
    expect(normalizeTransferColumns(['status', 'status'])).toEqual(['status'])
  })
})

describe('restoring a stored column selection', () => {
  const KEY = 'curvine.web.transfers.columns.test'

  it('drops keys that no longer exist and re-orders the rest', () => {
    window.localStorage.setItem(KEY, JSON.stringify(['updated', 'gone', 'kind', 42]))

    expect(readStoredColumns(KEY, TRANSFER_COLUMNS)).toEqual(['kind', 'updated'])
  })

  it('honours an empty selection instead of falling back to the defaults', () => {
    window.localStorage.setItem(KEY, JSON.stringify([]))

    expect(readStoredColumns(KEY, TRANSFER_COLUMNS)).toEqual([])
  })

  it('ignores junk rather than throwing', () => {
    window.localStorage.setItem(KEY, 'not json')
    expect(readStoredColumns(KEY, TRANSFER_COLUMNS)).toBeNull()

    window.localStorage.setItem(KEY, JSON.stringify({ kind: true }))
    expect(readStoredColumns(KEY, TRANSFER_COLUMNS)).toBeNull()

    window.localStorage.removeItem(KEY)
    expect(readStoredColumns(KEY, TRANSFER_COLUMNS)).toBeNull()
  })
})
