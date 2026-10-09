import { describe, expect, it } from 'vitest'
import en from '@/i18n/messages/en'
import zhCN from '@/i18n/messages/zh-CN'
import {
  BROWSABLE_COLUMNS,
  DEFAULT_VISIBLE_COLUMNS,
  columnText,
  expireTimeMs,
  isBrowseColumnKey,
  isExpired,
  orderColumns,
  storageLocation
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
    const extraKeys = [
      'browse.table.expireTime',
      'browse.expire.never',
      'browse.storageLocation.both',
      'browse.storageLocation.cv',
      'browse.storageLocation.ufs',
      'browse.columns.expireHint',
      'browse.columns.locationHint'
    ]
    for (const key of [...BROWSABLE_COLUMNS.map((column) => column.labelKey), ...extraKeys]) {
      expect(hasKey(en, key), `en is missing ${key}`).toBe(true)
      expect(hasKey(zhCN, key), `zh-CN is missing ${key}`).toBe(true)
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

describe('expire time and storage location', () => {
  const mtime = 1_700_000_000_000
  const file = {
    is_dir: false,
    mtime,
    storage_policy: { ttl_ms: 86_400_000, ttl_action: 'Delete', state: 'Both' }
  }

  it('computes expire time as mtime + ttl_ms when a TTL action is set', () => {
    expect(expireTimeMs(file)).toBe(mtime + 86_400_000)
    expect(isExpired(file, mtime + 86_400_000 - 1)).toBe(false)
    expect(isExpired(file, mtime + 86_400_000 + 1)).toBe(true)
  })

  it('treats ttl_ms=0 or ttl_action=None as never expires', () => {
    expect(expireTimeMs({ ...file, storage_policy: { ttl_ms: 0, ttl_action: 'Delete', state: 'Cv' } })).toBeNull()
    expect(expireTimeMs({ ...file, storage_policy: { ttl_ms: 86_400_000, ttl_action: 'None', state: 'Cv' } })).toBeNull()
    expect(expireTimeMs({ ...file, storage_policy: { ttl_ms: 86_400_000, ttl_action: 'none', state: 'Cv' } })).toBeNull()
    expect(isExpired({ ...file, storage_policy: { ttl_ms: 0, ttl_action: 'Delete' } }, mtime + 1)).toBe(false)
  })

  it('computes expire time for directories the same way as files', () => {
    expect(expireTimeMs({ is_dir: true, mtime, storage_policy: { ttl_ms: 3_600_000, ttl_action: 'Free', state: 'Cv' } }))
      .toBe(mtime + 3_600_000)
  })

  it('maps StorageState to the three Browse location values', () => {
    expect(storageLocation(file)).toBe('both')
    expect(storageLocation({ storage_policy: { state: 'Cv' } })).toBe('cv')
    expect(storageLocation({ storage_policy: { state: 'Ufs' } })).toBe('ufs')
    expect(storageLocation({ storage_policy: { state: 3 } })).toBe('both')
    expect(storageLocation({ storage_policy: { state: 1 } })).toBe('cv')
    expect(storageLocation({ storage_policy: { state: 2 } })).toBe('ufs')
    expect(storageLocation({ storage_policy: {} })).toBeNull()
    expect(storageLocation(undefined)).toBeNull()
  })
})
