/**
 * Column visibility for tables whose column set the user can choose — currently
 * the file browser (`browseColumns.ts`) and the transfer list
 * (`transferColumns.ts`).
 *
 * The key type is derived from the column definitions (`Def['key']`) rather than
 * inferred as its own parameter, so a read/write pair is checked against the very
 * keys that table declares instead of widening to `string`.
 *
 * The stored selection is untrusted: it comes from localStorage and survives
 * deploys that may add or remove columns, so anything unrecognised is dropped
 * rather than rendered.
 */

export function orderVisibleColumns<Def extends { key: string }>(
  columns: Def[],
  visible: readonly Def['key'][]
): Def[] {
  const selected = new Set<string>(visible)
  return columns.filter((column) => selected.has(column.key))
}

export function isVisibleColumnKey<Def extends { key: string }>(
  columns: Def[],
  value: unknown
): value is Def['key'] {
  return typeof value === 'string' && columns.some((column) => column.key === value)
}

/** Re-ordered to declaration order, or null when nothing usable was stored. */
export function readStoredColumns<Def extends { key: string }>(
  storageKey: string,
  columns: Def[]
): Def['key'][] | null {
  try {
    const raw = window.localStorage.getItem(storageKey)
    if (!raw) return null
    const parsed: unknown = JSON.parse(raw)
    if (!Array.isArray(parsed)) return null
    const valid = parsed.filter((value) => isVisibleColumnKey(columns, value))
    return orderVisibleColumns(columns, valid).map((column) => column.key)
  } catch {
    return null
  }
}

export function writeStoredColumns(storageKey: string, visible: readonly string[]): void {
  try {
    window.localStorage.setItem(storageKey, JSON.stringify(visible))
  } catch {
    // Persisting the selection is best-effort.
  }
}
