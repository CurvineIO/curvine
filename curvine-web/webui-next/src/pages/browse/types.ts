import type { BrowseEntry } from '@/lib/browseColumns'

export type { TrackedUploadBatch, UfsSyncBatch, UfsSyncJob } from '@/lib/ufs-sync/types'

/**
 * A listing row as `GET /v1/fs/list` returns it: the shared attribute fields
 * plus the identity/size fields the browser needs.
 */
export interface BrowseRow extends BrowseEntry {
  path?: string | null
  name?: string | null
  len?: number | null
}

export interface BrowseSnapshot {
  items: unknown[]
  saved_at: number
}
