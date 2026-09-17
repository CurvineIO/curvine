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
  /** Percentage width used when the column is visible. */
  width: number
  /** Right align numeric values. */
  numeric?: boolean
}

/** A row from `GET /api/v1/fs/list` (a serialised `FileStatus`). */
export interface BrowseEntry {
  is_dir?: boolean
  replicas?: number | null
  mode?: number | null
  owner?: string | null
  group?: string | null
  nlink?: number | null
  block_size?: number | null
  atime?: number | null
  storage_policy?: {
    storage_type?: string | null
    ttl_ms?: number | null
  } | null
}

export const BROWSABLE_COLUMNS: BrowseColumnDef[] = [
  { key: 'replicas', labelKey: 'browse.table.replicas', width: 8, numeric: true },
  { key: 'permission', labelKey: 'browse.table.permission', width: 12 },
  { key: 'owner', labelKey: 'browse.table.owner', width: 9 },
  { key: 'group', labelKey: 'browse.table.group', width: 9 },
  { key: 'nlink', labelKey: 'browse.table.nlink', width: 7, numeric: true },
  { key: 'blockSize', labelKey: 'browse.table.blockSize', width: 9 },
  { key: 'storageType', labelKey: 'browse.table.storageType', width: 9 },
  { key: 'ttl', labelKey: 'browse.table.ttl', width: 8 },
  { key: 'atime', labelKey: 'browse.table.atime', width: 12 }
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

const BASE_COLUMN_WIDTHS = { name: 32, type: 12, size: 14, modified: 18 }
const ACTIONS_WIDTH = 24
const NAME_MIN_WIDTH = 18
/** Percentage the name column can give up to the optional attributes. */
const OPTIONAL_BUDGET = BASE_COLUMN_WIDTHS.name - NAME_MIN_WIDTH

/**
 * Final percentage width for every column (name/type/size/modified/actions plus
 * each visible optional column).
 *
 * The table is `table-layout: fixed`, so `<col>` widths are authoritative and
 * must add up to 100%: optional columns are paid for out of the name column
 * (which keeps at least `NAME_MIN_WIDTH`), and once that budget is exhausted
 * every optional column shrinks proportionally.
 */
export function computeColumnWidths(visible: BrowseColumnKey[]): Record<string, number> {
  const columns = orderColumns(visible)
  const requested = columns.reduce((sum, column) => sum + column.width, 0)
  const scale = requested > OPTIONAL_BUDGET ? OPTIONAL_BUDGET / requested : 1

  // Floor to hundredths so the optional columns can never exceed their budget
  // and the name column never drops below NAME_MIN_WIDTH.
  const optionalWidths = columns.map((column) => ({
    key: column.key,
    width: Math.floor(column.width * scale * 100) / 100
  }))
  const optionalTotal = optionalWidths.reduce((sum, column) => sum + column.width, 0)

  const widths: Record<string, number> = {
    name:
      100 -
      BASE_COLUMN_WIDTHS.type -
      BASE_COLUMN_WIDTHS.size -
      BASE_COLUMN_WIDTHS.modified -
      ACTIONS_WIDTH -
      optionalTotal,
    type: BASE_COLUMN_WIDTHS.type,
    size: BASE_COLUMN_WIDTHS.size,
    modified: BASE_COLUMN_WIDTHS.modified,
    actions: ACTIONS_WIDTH
  }
  for (const column of optionalWidths) {
    widths[column.key] = column.width
  }
  return widths
}
