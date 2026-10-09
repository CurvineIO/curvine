import { orderVisibleColumns } from '@/lib/column-visibility'

export type TransferColumnKey =
  | 'owner'
  | 'kind'
  | 'path'
  | 'status'
  | 'progress'
  | 'files'
  | 'duration'
  | 'updated'

export interface TransferColumnDef {
  key: TransferColumnKey
  /** i18n key for the column header; asserted against the en locale by tests. */
  labelKey: string
  /** Width under `table-fixed`; hidden columns leave their share to the others. */
  width: string
}

/**
 * The optional columns, in render order. Job ID (first) and Actions (last) are
 * fixed: the id identifies the row and Actions holds its controls, so neither is
 * something you would want to switch off.
 *
 * `owner` carries the submitter and submit time that used to head the row; it is
 * optional so the id can stand alone.
 *
 * The widths add up with the fixed 14% + 10% to exactly 100%, so the table looks
 * the same as before until a column is hidden.
 */
export const TRANSFER_COLUMNS: TransferColumnDef[] = [
  { key: 'owner', labelKey: 'transfers.list.headerOwner', width: '10%' },
  { key: 'kind', labelKey: 'transfers.list.headerKind', width: '6%' },
  { key: 'path', labelKey: 'transfers.list.headerPath', width: '18%' },
  { key: 'status', labelKey: 'transfers.list.headerStatus', width: '9%' },
  { key: 'progress', labelKey: 'transfers.list.headerProgress', width: '9%' },
  { key: 'files', labelKey: 'transfers.list.headerFiles', width: '6%' },
  { key: 'duration', labelKey: 'transfers.list.headerDuration', width: '8%' },
  { key: 'updated', labelKey: 'transfers.list.headerUpdated', width: '10%' }
]

/** Everything starts visible, so the picker does not change the view by itself. */
export const DEFAULT_VISIBLE_TRANSFER_COLUMNS: TransferColumnKey[] = TRANSFER_COLUMNS.map(
  (column) => column.key
)

/**
 * Columns that are always rendered, whatever the stored selection says.
 *
 * Actions holds the row's controls and State is what the list is for — a transfer
 * whose execution state you cannot see is not a usable row, so neither is worth
 * being able to switch off. They keep their declared position, so hiding another
 * column never moves them.
 */
export const PINNED_TRANSFER_COLUMNS: TransferColumnKey[] = ['status']

/** What the picker offers: the columns that can actually be turned off. */
export const OPTIONAL_TRANSFER_COLUMNS: TransferColumnDef[] = TRANSFER_COLUMNS.filter(
  (column) => !PINNED_TRANSFER_COLUMNS.includes(column.key)
)

export const DEFAULT_VISIBLE_OPTIONAL_TRANSFER_COLUMNS: TransferColumnKey[] =
  OPTIONAL_TRANSFER_COLUMNS.map((column) => column.key)

/** Re-adds the pinned columns to a selection, in declaration order. */
export function normalizeTransferColumns(visible: TransferColumnKey[]): TransferColumnKey[] {
  return TRANSFER_COLUMNS.filter(
    (column) => PINNED_TRANSFER_COLUMNS.includes(column.key) || visible.includes(column.key)
  ).map((column) => column.key)
}

/** Keep declaration order regardless of the order columns were ticked in. */
export function orderTransferColumns(visible: TransferColumnKey[]): TransferColumnDef[] {
  return orderVisibleColumns(TRANSFER_COLUMNS, visible)
}
