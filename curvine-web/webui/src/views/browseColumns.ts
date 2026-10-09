import { formatBytes, formatPermission, formatTime, formatTtl } from '@/utils/format'

export type BrowseColumnKey =
  | 'replicas'
  | 'permission'
  | 'owner'
  | 'group'
  | 'nlink'
  | 'blockSize'
  | 'storageType'
  | 'ttl'
  | 'atime'

export interface BrowseColumnDef {
  key: BrowseColumnKey
  /** i18n key for the column header; asserted against the en locale by tests. */
  labelKey: string
  /** Right align numeric values. */
  numeric?: boolean
}

export type StorageLocation = 'both' | 'cv' | 'ufs'

/** A row from `GET /api/v1/fs/list` (a serialised `FileStatus`). */
export interface BrowseEntry {
  is_dir?: boolean
  replicas?: number | null
  mode?: number | null
  owner?: string | null
  group?: string | null
  nlink?: number | null
  block_size?: number | null
  mtime?: number | null
  atime?: number | null
  storage_policy?: {
    storage_type?: string | null
    ttl_ms?: number | null
    ttl_action?: string | number | null
    state?: string | number | null
  } | null
}

export const BROWSABLE_COLUMNS: BrowseColumnDef[] = [
  { key: 'replicas', labelKey: 'browse.table.replicas', numeric: true },
  { key: 'permission', labelKey: 'browse.table.permission' },
  { key: 'owner', labelKey: 'browse.table.owner' },
  { key: 'group', labelKey: 'browse.table.group' },
  { key: 'nlink', labelKey: 'browse.table.nlink', numeric: true },
  { key: 'blockSize', labelKey: 'browse.table.blockSize' },
  { key: 'storageType', labelKey: 'browse.table.storageType' },
  { key: 'ttl', labelKey: 'browse.table.ttl' },
  { key: 'atime', labelKey: 'browse.table.atime' }
]

/** Replicas show by default; every other attribute is opt-in. */
export const DEFAULT_VISIBLE_COLUMNS: BrowseColumnKey[] = ['replicas']

const COLUMN_KEYS = BROWSABLE_COLUMNS.map((column) => column.key)

/** Keep declaration order regardless of the order attributes were ticked in. */
export function orderColumns(visible: BrowseColumnKey[]): BrowseColumnDef[] {
  const selected = new Set(visible)
  return BROWSABLE_COLUMNS.filter((column) => selected.has(column.key))
}

export function isBrowseColumnKey(value: unknown): value is BrowseColumnKey {
  return typeof value === 'string' && COLUMN_KEYS.includes(value as BrowseColumnKey)
}

function textOrDash(value: unknown): string {
  if (value === null || value === undefined) return '-'
  const text = String(value).trim()
  return text || '-'
}

function positiveNumber(value: unknown): number | null {
  const number = Number(value)
  if (!Number.isFinite(number) || number <= 0) return null
  return number
}

const INACTIVE_TTL_ACTIONS = new Set(['', 'none', '0'])

/**
 * TTL is active only when both a positive duration and a non-`None` action are
 * set, matching `FileStatus::is_expired` / the Browse expire-time rule.
 */
export function hasActiveTtl(policy: BrowseEntry['storage_policy']): boolean {
  if (!policy) return false
  if (positiveNumber(policy.ttl_ms) === null) return false
  if (policy.ttl_action === null || policy.ttl_action === undefined) return false
  return !INACTIVE_TTL_ACTIONS.has(String(policy.ttl_action).trim().toLowerCase())
}

/** Expire timestamp in ms (`mtime + ttl_ms`), or `null` when the entry never expires. */
export function expireTimeMs(item: BrowseEntry | null | undefined): number | null {
  if (!item || !hasActiveTtl(item.storage_policy)) return null
  const mtime = Number(item.mtime)
  if (!Number.isFinite(mtime) || mtime <= 0) return null
  return mtime + Number(item.storage_policy!.ttl_ms)
}

export function isExpired(
  item: BrowseEntry | null | undefined,
  nowMs: number = Date.now()
): boolean {
  const expireAt = expireTimeMs(item)
  return expireAt !== null && expireAt < nowMs
}

/**
 * Map `storage_policy.state` to the three Browse location values.
 * Accepts the serde variant names (`Both`/`Cv`/`Ufs`) and the i8 discriminants.
 */
export function storageLocation(item: BrowseEntry | null | undefined): StorageLocation | null {
  const state = item?.storage_policy?.state
  if (state === null || state === undefined || state === '') return null
  const normalized = String(state).trim().toLowerCase()
  if (normalized === 'both' || normalized === '3') return 'both'
  if (normalized === 'cv' || normalized === '1') return 'cv'
  if (normalized === 'ufs' || normalized === '2') return 'ufs'
  return null
}

/**
 * Render one optional attribute for a listing row.
 *
 * Values the backend does not populate render as `-` rather than as a
 * misleading default: directories have no replicas/block size/TTL, and UFS-only
 * entries are backfilled with `storage_policy`/`block_size`/`replicas` but not
 * with `mode`/`owner`/`group` (see `browse_v1`), so those stay unset.
 */
export function columnText(item: BrowseEntry | null | undefined, key: BrowseColumnKey): string {
  if (!item) return '-'
  switch (key) {
    case 'replicas':
      if (item.is_dir) return '-'
      return positiveNumber(item.replicas)?.toString() ?? '-'
    case 'permission':
      if (!item.mode) return '-'
      return formatPermission(item.mode)
    case 'owner':
      return textOrDash(item.owner)
    case 'group':
      return textOrDash(item.group)
    case 'nlink':
      return positiveNumber(item.nlink)?.toString() ?? '-'
    case 'blockSize':
      if (item.is_dir) return '-'
      return positiveNumber(item.block_size) === null ? '-' : formatBytes(item.block_size)
    case 'storageType':
      return textOrDash(item.storage_policy?.storage_type)
    case 'ttl':
      return formatTtl(item.storage_policy?.ttl_ms)
    case 'atime':
      return formatTime(item.atime)
    default:
      return '-'
  }
}
