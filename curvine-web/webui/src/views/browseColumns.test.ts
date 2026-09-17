import { describe, expect, it } from 'vitest'
import en from '@/i18n/locales/en'
import zhCN from '@/i18n/locales/zh-CN'
import {
  BROWSABLE_COLUMNS,
  DEFAULT_VISIBLE_COLUMNS,
  columnText,
  computeColumnWidths,
  isBrowseColumnKey,
  orderColumns
} from './browseColumns'

function hasKey(tree: unknown, key: string): boolean {
  let node: unknown = tree
  for (const part of key.split('.')) {
    if (node === null || typeof node !== 'object') return false
    node = (node as Record<string, unknown>)[part]
  }
  return node !== undefined
}

describe('browse column definitions', () => {
  // These labels are resolved through a dynamic `$t(col.labelKey)`, so the i18n
  // static-key scan cannot see them — assert them here instead.
  it('labels every column in both locales', () => {
    for (const column of BROWSABLE_COLUMNS) {
      expect(hasKey(en, column.labelKey), `en is missing ${column.labelKey}`).toBe(true)
      expect(hasKey(zhCN, column.labelKey), `zh-CN is missing ${column.labelKey}`).toBe(true)
    }
  })

  it('shows replicas by default and only known columns', () => {
    expect(DEFAULT_VISIBLE_COLUMNS).toEqual(['replicas'])
    for (const key of DEFAULT_VISIBLE_COLUMNS) {
      expect(isBrowseColumnKey(key)).toBe(true)
    }
    expect(isBrowseColumnKey('nope')).toBe(false)
  })

  it('orders columns by declaration, not by selection order', () => {
    expect(orderColumns(['atime', 'replicas']).map((column) => column.key)).toEqual([
      'replicas',
      'atime'
    ])
  })
})

describe('columnText', () => {
  const file = {
    is_dir: false,
    replicas: 3,
    mode: 0o755,
    owner: 'alice',
    group: 'staff',
    nlink: 1,
    block_size: 134_217_728,
    atime: 1_725_000_000_000,
    storage_policy: { storage_type: 'Ssd', ttl_ms: 86_400_000 }
  }
  const dir = {
    is_dir: true,
    replicas: 0,
    mode: 0o755,
    owner: 'alice',
    group: 'staff',
    nlink: 2,
    block_size: 0,
    atime: 1_725_000_000_000,
    storage_policy: { storage_type: 'Ssd', ttl_ms: 0 }
  }

  it('renders file attributes', () => {
    expect(columnText(file, 'replicas')).toBe('3')
    expect(columnText(file, 'permission')).toBe('rwxr-xr-x')
    expect(columnText(file, 'owner')).toBe('alice')
    expect(columnText(file, 'group')).toBe('staff')
    expect(columnText(file, 'nlink')).toBe('1')
    expect(columnText(file, 'blockSize')).toBe('128.00 MB')
    expect(columnText(file, 'storageType')).toBe('Ssd')
    expect(columnText(file, 'ttl')).toBe('1d')
    expect(columnText(file, 'atime')).not.toBe('-')
  })

  it('renders a dash for attributes a directory cannot have', () => {
    expect(columnText(dir, 'replicas')).toBe('-')
    expect(columnText(dir, 'blockSize')).toBe('-')
    expect(columnText(dir, 'ttl')).toBe('-')
  })

  it('still reports permissions and ownership for directories', () => {
    expect(columnText(dir, 'permission')).toBe('rwxr-xr-x')
    expect(columnText(dir, 'owner')).toBe('alice')
    expect(columnText(dir, 'group')).toBe('staff')
  })

  it('renders a dash instead of a misleading default for UFS-only entries', () => {
    // browse_v1 backfills storage_policy/block_size/replicas but not mode/owner/group.
    const ufsOnly = { is_dir: false, replicas: 0, mode: 0, owner: '', group: '', nlink: 0, block_size: 0 }
    expect(columnText(ufsOnly, 'permission')).toBe('-')
    expect(columnText(ufsOnly, 'owner')).toBe('-')
    expect(columnText(ufsOnly, 'group')).toBe('-')
    expect(columnText(ufsOnly, 'replicas')).toBe('-')
    expect(columnText(ufsOnly, 'blockSize')).toBe('-')
    expect(columnText(ufsOnly, 'ttl')).toBe('-')
  })

  it('tolerates a missing entry or a missing storage policy', () => {
    expect(columnText(null, 'owner')).toBe('-')
    expect(columnText(undefined, 'replicas')).toBe('-')
    expect(columnText({ is_dir: false }, 'storageType')).toBe('-')
  })
})

describe('computeColumnWidths', () => {
  const allKeys = BROWSABLE_COLUMNS.map((column) => column.key)

  it('totals 100% and keeps the name column readable for every subset', () => {
    for (let mask = 0; mask < 1 << allKeys.length; mask += 1) {
      const visible = allKeys.filter((_, index) => mask & (1 << index))
      const widths = computeColumnWidths(visible)
      const total = Object.values(widths).reduce((sum, value) => sum + value, 0)
      expect(total, `subset ${visible.join(',')}`).toBeCloseTo(100, 9)
      expect(widths.name, `subset ${visible.join(',')}`).toBeGreaterThanOrEqual(18)
    }
  })

  it('keeps the default layout close to the previous four column look', () => {
    const widths = computeColumnWidths(DEFAULT_VISIBLE_COLUMNS)
    expect(widths.name).toBe(24)
    expect(widths.replicas).toBe(8)
    expect(widths.actions).toBe(24)
  })

  it('shrinks the optional columns once the name budget is exhausted', () => {
    const widths = computeColumnWidths(allKeys)
    expect(widths.name).toBeGreaterThanOrEqual(18)
    const optionalTotal = allKeys.reduce((sum, key) => sum + widths[key], 0)
    expect(optionalTotal).toBeLessThanOrEqual(14)
  })
})
