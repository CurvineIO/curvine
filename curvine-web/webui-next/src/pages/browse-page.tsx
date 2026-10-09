import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { CheckIcon, CopyIcon, FileIcon, FolderIcon } from 'lucide-react'
import { useNavigate, useSearchParams } from 'react-router-dom'

import { useApi } from '@/api/api-context'
import type { BrowseData, MountInfo } from '@/api/types'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { EmptyNotice, ErrorAlert } from '@/components/data-state'
import { PageHeader } from '@/components/page-header'
import { MonoText, StatusBadge } from '@/components/resource-table'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Checkbox } from '@/components/ui/checkbox'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger
} from '@/components/ui/dropdown-menu'
import { Input } from '@/components/ui/input'
import { Spinner } from '@/components/ui/spinner'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { useToast } from '@/components/ui/toast'
import { useT } from '@/i18n'
import {
  BROWSABLE_COLUMNS,
  DEFAULT_VISIBLE_COLUMNS,
  columnText,
  expireTimeMs,
  isExpired,
  orderColumns,
  storageLocation,
  type BrowseColumnKey
} from '@/lib/browseColumns'
import { useCopy } from '@/components/copy-fallback'
import { readStoredColumns, writeStoredColumns } from '@/lib/column-visibility'
import { formatBytes, formatTime } from '@/lib/format'
import { useSlidingSheet } from '@/lib/use-sliding-sheet'
import { useRefreshSignal } from '@/refresh/refresh-context'
import { ColumnsPicker } from '@/components/columns-picker'
import { MkdirSheet, PathBreadcrumb } from './browse/browse-controls'
import { BlocksSheet } from './browse/blocks-sheet'
import { PreviewSheet } from './browse/preview-sheet'
import { UfsSyncPanel, UploadBatchesPanel } from './browse/sync-panels'
import type { BrowseRow, TrackedUploadBatch, UfsSyncBatch, UfsSyncJob } from './browse/types'
import {
  fileName,
  isReadOnlyMount,
  isTerminalSyncState,
  joinPath,
  normalizePath,
  syncState,
  writeTypeLabel
} from './browse/utils'

const BROWSE_SNAPSHOT_TTL_MS = 10 * 60 * 1000
const UPLOAD_BATCH_KEEP_MS = 24 * 60 * 60 * 1000
const UPLOAD_BATCH_STALE_MS = 10 * 60 * 1000
const UFS_SYNC_KEEP_MS = 30 * 60 * 1000
const UFS_SYNC_RESTORE_KEEP_MS = 24 * 60 * 60 * 1000
const MAX_POLL_ATTEMPTS = 240

/**
 * Entries per listing request. A directory with tens of thousands of files used
 * to be fetched in one response, which overran the RPC frame cap and the request
 * timeout; the server now honours this window and the table pages with it.
 */
const BROWSE_PAGE = 500

const BROWSE_COLUMNS_STORAGE_KEY = 'curvine.web.fs.browseColumns.v1'
const UPLOAD_BATCH_STORAGE_KEY = 'curvine.web.fs.uploadBatches.v1'
const UFS_SYNC_STORAGE_KEY = 'curvine.web.fs.ufsSyncJobs.v3'
const UFS_SYNC_DISMISSED_STORAGE_KEY = 'curvine.web.fs.ufsSyncDismissed.v1'
const UFS_SYNC_PANEL_STORAGE_KEY = 'curvine.web.fs.ufsSyncPanelExpanded.v1'

function readJson<T>(key: string, fallback: T): T {
  try {
    const raw = window.localStorage.getItem(key)
    return raw ? ((JSON.parse(raw) as T) ?? fallback) : fallback
  } catch {
    return fallback
  }
}

function writeJson(key: string, value: unknown) {
  try {
    window.localStorage.setItem(key, JSON.stringify(value))
  } catch {
    // Storage may be full or unavailable; tracking is best-effort.
  }
}

function browseSnapshotKey(cacheOnly: boolean, path: string): string {
  return `curvine.web.fs.browseSnapshot.v1:${cacheOnly ? 'cache' : 'all'}:${normalizePath(path)}`
}

function normalizeUploadBatch(
  batch: Partial<TrackedUploadBatch> | undefined,
  now: number
): TrackedUploadBatch | undefined {
  if (!batch) return undefined
  const updatedAt = Number(batch.updated_at || 0)
  if (batch.done || updatedAt <= 0 || now - updatedAt <= UPLOAD_BATCH_STALE_MS) {
    return batch as TrackedUploadBatch
  }
  return {
    ...(batch as TrackedUploadBatch),
    state: 'Interrupted',
    done: true
  }
}

export function BrowsePage() {
  const t = useT()
  const api = useApi()
  const toast = useToast()
  const { copy } = useCopy()
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const pathFromUrl = normalizePath(searchParams.get('path') || '/')

  const [browsePath, setBrowsePath] = useState(pathFromUrl)
  const [cacheOnly, setCacheOnly] = useState(false)
  const [items, setItems] = useState<BrowseRow[]>([])
  const [loading, setLoading] = useState(false)
  const [loadingMore, setLoadingMore] = useState(false)
  const [hasMore, setHasMore] = useState(false)
  const [nextStartAfter, setNextStartAfter] = useState('')
  const [loadError, setLoadError] = useState<string | null>(null)
  const [copiedPath, setCopiedPath] = useState('')
  const [mounts, setMounts] = useState<MountInfo[]>([])
  const [visibleColumns, setVisibleColumns] = useState<BrowseColumnKey[]>(DEFAULT_VISIBLE_COLUMNS)
  const [uploadBatches, setUploadBatches] = useState<Record<string, TrackedUploadBatch>>({})
  const [ufsSyncJobs, setUfsSyncJobs] = useState<Record<string, UfsSyncJob>>({})
  const [ufsSyncPanelExpanded, setUfsSyncPanelExpanded] = useState(true)
  const [mkdirOpen, setMkdirOpen] = useState(false)
  const mkdirSheet = useSlidingSheet(mkdirOpen)
  // Clicking a file previews it; the 块 button opens the block locations. Both are drawers
  // kept mounted by `useSlidingSheet` so their close animation is not cut short.
  const [previewTarget, setPreviewTarget] = useState<BrowseRow | null>(null)
  const previewSheet = useSlidingSheet(previewTarget !== null, previewTarget)
  const [blocksPath, setBlocksPath] = useState('')
  const blocksSheet = useSlidingSheet(blocksPath !== '', blocksPath)
  const [deleteTarget, setDeleteTarget] = useState<BrowseRow | null>(null)
  const [freeTarget, setFreeTarget] = useState<{ path: string; recursive: boolean; message: string } | null>(
    null
  )
  // Batch selection. Held as paths (not rows) so a refresh or a "load more" keeps
  // the ticks on the entries that are still listed.
  const [selectedPaths, setSelectedPaths] = useState<string[]>([])
  const [bulkFreeOpen, setBulkFreeOpen] = useState(false)
  const [bulkDeleteOpen, setBulkDeleteOpen] = useState(false)
  const [submitting, setSubmitting] = useState(false)

  const browsePathRef = useRef(browsePath)
  const cacheOnlyRef = useRef(cacheOnly)
  const itemsRef = useRef<BrowseRow[]>([])
  const mountsRef = useRef<MountInfo[]>([])
  const mountsLoadedRef = useRef(false)
  const uploadBatchesRef = useRef(uploadBatches)
  const ufsSyncJobsRef = useRef(ufsSyncJobs)
  const ufsSyncDismissedRef = useRef<Record<string, number>>({})
  const summaryLoadToken = useRef(0)
  const ufsSyncDiscoverToken = useRef(0)
  const ufsSyncTimers = useRef<Record<string, number>>({})
  const ufsSyncBatchTimers = useRef<Record<string, number>>({})
  const ufsSyncSaveTimer = useRef<number | null>(null)
  const uploadStorageTimer = useRef<number | null>(null)
  const ufsStorageTimer = useRef<number | null>(null)
  const ufsDiscoverTimer = useRef<number | null>(null)
  const fileInputRef = useRef<HTMLInputElement | null>(null)
  const folderInputRef = useRef<HTMLInputElement | null>(null)
  // Keeps `fetchData` identity stable across locale switches so the listing is
  // not re-fetched just because the language changed.
  const tRef = useRef(t)
  tRef.current = t

  browsePathRef.current = browsePath
  cacheOnlyRef.current = cacheOnly
  itemsRef.current = items
  mountsRef.current = mounts
  uploadBatchesRef.current = uploadBatches
  ufsSyncJobsRef.current = ufsSyncJobs

  /* ---------------------------------------------------------------- mounts */

  const isPathInside = useCallback(
    (path: string, base: string) => {
      const normalized = normalizePath(path)
      const normalizedBase = normalizePath(base)
      return normalized === normalizedBase || normalized.startsWith(`${normalizedBase}/`)
    },
    []
  )

  const isMountedPath = useCallback(
    (path: string) => mounts.some((mount) => mount?.cv_path && isPathInside(path, mount.cv_path)),
    [mounts, isPathInside]
  )

  const isFsModePath = useCallback(
    (path: string, source: MountInfo[] = mounts) =>
      source.some(
        (mount) =>
          mount?.cv_path &&
          isPathInside(path, mount.cv_path) &&
          writeTypeLabel(mount.write_type) === 'fs_mode'
      ),
    [mounts, isPathInside]
  )

  const isMountRoot = useCallback(
    (path: string) =>
      mounts.some((mount) => mount?.cv_path && normalizePath(path) === normalizePath(mount.cv_path)),
    [mounts]
  )

  const isReadOnlyMountedPath = useCallback(
    (path: string, source: MountInfo[] = mounts) => {
      const owner = source
        .filter((mount) => mount?.cv_path && isPathInside(path, mount.cv_path))
        .sort(
          (left, right) =>
            normalizePath(right.cv_path).length - normalizePath(left.cv_path).length
        )[0]
      return isReadOnlyMount(owner)
    },
    [mounts, isPathInside]
  )

  const ensureMounts = useCallback(async () => {
    if (mountsLoadedRef.current) return mountsRef.current
    try {
      const data = await api.fetchMountsData()
      const list = (Array.isArray(data) ? data : (data as { items?: MountInfo[] })?.items || []) as MountInfo[]
      setMounts(list)
      mountsRef.current = list
      return list
    } catch {
      setMounts([])
      mountsRef.current = []
      return []
    } finally {
      mountsLoadedRef.current = true
    }
  }, [api])

  /* ----------------------------------------------------------- ufs tracking */

  const isUfsSyncDismissed = useCallback((path: string, job?: UfsSyncJob | null) => {
    const normalized = normalizePath(path)
    const dismissedAt = Number(ufsSyncDismissedRef.current[normalized] || 0)
    if (!dismissedAt) return false
    if (!job) return true
    const state = job.state || job.status || ''
    const done = Boolean(job.done) || isTerminalSyncState(state)
    if (!done) return false
    const serverUpdated = Number(job.update_time_ms || job.updated_at || 0)
    return !serverUpdated || serverUpdated <= dismissedAt
  }, [])

  const flushUfsSyncJobsToStorage = useCallback(() => {
    ufsSyncSaveTimer.current = null
    try {
      const now = Date.now()
      const stored = readJson<Record<string, UfsSyncJob>>(UFS_SYNC_STORAGE_KEY, {})
      const jobs: Record<string, UfsSyncJob> = { ...stored }
      for (const [path, job] of Object.entries(ufsSyncJobsRef.current)) {
        const state = syncState(job)
        if (job.done || isTerminalSyncState(state)) {
          delete jobs[path]
          continue
        }
        const currentUpdatedAt = Number(job.updated_at || 0)
        const storedUpdatedAt = Number(jobs[path]?.updated_at || 0)
        if (now - Number(job.updated_at || now) <= UFS_SYNC_KEEP_MS && currentUpdatedAt >= storedUpdatedAt) {
          jobs[path] = job
        }
      }
      for (const [path, job] of Object.entries(jobs)) {
        const state = syncState(job)
        if (
          !job ||
          job.done ||
          isTerminalSyncState(state) ||
          now - Number(job.updated_at || now) > UFS_SYNC_KEEP_MS ||
          isUfsSyncDismissed(path, job)
        ) {
          delete jobs[path]
        }
      }
      window.localStorage.setItem(UFS_SYNC_STORAGE_KEY, JSON.stringify(jobs))
    } catch {
      // Best-effort persistence.
    }
  }, [isUfsSyncDismissed])

  const saveUfsSyncJobs = useCallback(() => {
    if (ufsSyncSaveTimer.current) window.clearTimeout(ufsSyncSaveTimer.current)
    ufsSyncSaveTimer.current = window.setTimeout(flushUfsSyncJobsToStorage, 400)
  }, [flushUfsSyncJobsToStorage])

  const setUfsSyncJob = useCallback(
    (path: string, job: Partial<UfsSyncJob>) => {
      const normalized = normalizePath(path)
      const nextJob: UfsSyncJob = {
        ...(ufsSyncJobsRef.current[normalized] || ({} as UfsSyncJob)),
        ...job,
        path: normalized,
        updated_at: Date.now()
      }
      if (isUfsSyncDismissed(normalized, nextJob)) return
      const next = { ...ufsSyncJobsRef.current, [normalized]: nextJob }
      ufsSyncJobsRef.current = next
      setUfsSyncJobs(next)
      saveUfsSyncJobs()
    },
    [isUfsSyncDismissed, saveUfsSyncJobs]
  )

  const removeUfsSyncJob = useCallback((path: string) => {
    const normalized = normalizePath(path)
    const next = { ...ufsSyncJobsRef.current }
    delete next[normalized]
    ufsSyncJobsRef.current = next
    setUfsSyncJobs(next)
    try {
      const stored = readJson<Record<string, UfsSyncJob>>(UFS_SYNC_STORAGE_KEY, {})
      delete stored[normalized]
      window.localStorage.setItem(UFS_SYNC_STORAGE_KEY, JSON.stringify(stored))
    } catch {
      // Best-effort persistence.
    }
  }, [])

  const aggregateBatchUfsSyncJobs = useCallback(
    (serverJobs: UfsSyncJob[], previous: UfsSyncJob): Partial<UfsSyncJob> => {
      let completedFiles = 0
      let failedFiles = 0
      let syncingFiles = 0
      let waitingFiles = 0
      let loadedSize = 0
      let message = ''
      for (const job of serverJobs) {
        const state = syncState(job)
        const totalFiles = Math.max(1, Number(job.total_files || 1))
        const completed = state === 'Completed' ? totalFiles : Number(job.completed_files || 0)
        completedFiles += Math.max(0, Math.min(totalFiles, completed))
        loadedSize += Number(job.loaded_size || 0)
        if (['Failed', 'Canceled', 'Unknown', 'Timed out'].includes(state)) {
          failedFiles += 1
          if (!message && job.message) message = String(job.message)
        } else if (['Pending', 'Loading'].includes(state)) {
          syncingFiles += 1
        } else if (!isTerminalSyncState(state)) {
          waitingFiles += 1
        }
      }
      const totalFiles = Math.max(Number(previous.total_files || 0), serverJobs.length)
      let activeState = 'Waiting for UFS sync'
      if (completedFiles >= totalFiles) activeState = 'Completed'
      else if (failedFiles > 0) activeState = 'Needs attention'
      else if (syncingFiles > 0) activeState = 'Syncing to UFS'
      else if (waitingFiles > 0) activeState = 'Waiting for UFS sync'
      const done = activeState === 'Completed' || (failedFiles > 0 && completedFiles + failedFiles >= totalFiles)
      return {
        state: activeState,
        status: activeState,
        total_files: totalFiles,
        completed_files: completedFiles,
        loaded_size: loadedSize,
        total_size: Number(previous.total_size || 0),
        done,
        message
      }
    },
    []
  )

  const batchUfsSyncPollIntervalMs = useCallback((jobCount: number) => {
    if (jobCount > 200) return 8000
    if (jobCount > 50) return 5000
    return 3000
  }, [])

  const ufsSyncPollIntervalMs = useCallback(() => {
    const active = Object.keys(ufsSyncTimers.current).length
    if (active > 20) return 5000
    if (active > 5) return 3000
    return 2000
  }, [])

  const pollBatchUfsSync = useCallback(
    (batchId: string, pathPrefix: string) => {
      if (ufsSyncBatchTimers.current[batchId]) {
        window.clearTimeout(ufsSyncBatchTimers.current[batchId])
        delete ufsSyncBatchTimers.current[batchId]
      }
      const tick = async () => {
        const previous = ufsSyncJobsRef.current[pathPrefix] || ({ path: pathPrefix } as UfsSyncJob)
        const attempts = Number(previous.attempts || 0) + 1
        try {
          const data = await api.fetchUfsSyncJobs(pathPrefix, 500, true)
          const serverJobs = (Array.isArray(data)
            ? data
            : (data as { items?: UfsSyncJob[] })?.items || []) as UfsSyncJob[]
          const aggregate = aggregateBatchUfsSyncJobs(serverJobs, previous)
          setUfsSyncJob(pathPrefix, {
            ...previous,
            ...aggregate,
            path: pathPrefix,
            batch_id: batchId,
            batch_aggregate: true,
            attempts
          })
          if (!aggregate.done && attempts < MAX_POLL_ATTEMPTS) {
            ufsSyncBatchTimers.current[batchId] = window.setTimeout(
              tick,
              batchUfsSyncPollIntervalMs(serverJobs.length)
            )
          } else if (!aggregate.done) {
            setUfsSyncJob(pathPrefix, {
              state: 'Timed out',
              status: 'Timed out',
              message: t('browse.ufsSync.messages.timedOut'),
              done: true
            })
            delete ufsSyncBatchTimers.current[batchId]
          } else {
            delete ufsSyncBatchTimers.current[batchId]
          }
        } catch (error) {
          const message = String(error)
          if (message.includes('Request URI Not Found') || message.includes('Expected JSON API response')) {
            setUfsSyncJob(pathPrefix, {
              state: 'Unsupported',
              status: 'Unsupported',
              message: t('browse.ufsSync.messages.unsupported'),
              done: true
            })
            delete ufsSyncBatchTimers.current[batchId]
            return
          }
          if (attempts < MAX_POLL_ATTEMPTS) {
            ufsSyncBatchTimers.current[batchId] = window.setTimeout(tick, batchUfsSyncPollIntervalMs(0))
          } else {
            setUfsSyncJob(pathPrefix, {
              state: 'Timed out',
              status: 'Timed out',
              message: t('browse.ufsSync.messages.timedOutRecovery'),
              done: true
            })
            delete ufsSyncBatchTimers.current[batchId]
          }
        }
      }
      ufsSyncBatchTimers.current[batchId] = window.setTimeout(tick, 500)
    },
    [api, aggregateBatchUfsSyncJobs, batchUfsSyncPollIntervalMs, setUfsSyncJob, t]
  )

  const pollUfsSync = useCallback(
    async (path: string) => {
      if (ufsSyncTimers.current[path]) {
        window.clearTimeout(ufsSyncTimers.current[path])
        delete ufsSyncTimers.current[path]
      }
      const previous = ufsSyncJobsRef.current[path] || ({ path } as UfsSyncJob)
      try {
        const data = (await api.fetchUfsSyncStatus(path)) as UfsSyncJob | null
        const attempts = Number(previous.attempts || 0) + 1
        const done = Boolean(data?.done) || isTerminalSyncState(data && (data.state || data.status))
        setUfsSyncJob(path, { ...(data || {}), attempts, done })
        if (!done && attempts < MAX_POLL_ATTEMPTS) {
          ufsSyncTimers.current[path] = window.setTimeout(
            () => void pollUfsSync(path),
            ufsSyncPollIntervalMs()
          )
        } else if (!done) {
          setUfsSyncJob(path, {
            state: 'Timed out',
            status: 'Timed out',
            message: t('browse.ufsSync.messages.timedOut'),
            done: true
          })
        }
      } catch (error) {
        const message = String(error)
        if (message.includes('Request URI Not Found') || message.includes('Expected JSON API response')) {
          setUfsSyncJob(path, {
            state: 'Unsupported',
            status: 'Unsupported',
            message: t('browse.ufsSync.messages.unsupported'),
            done: true
          })
          return
        }
        const attempts = Number(previous.attempts || 0) + 1
        const keepState = previous.state || previous.status || 'Checking UFS sync'
        setUfsSyncJob(path, {
          ...previous,
          path,
          state: keepState,
          status: keepState,
          message: t('browse.ufsSync.messages.statusCheckFailed', { attempt: attempts, message }),
          attempts,
          done: false
        })
        if (attempts < MAX_POLL_ATTEMPTS) {
          ufsSyncTimers.current[path] = window.setTimeout(
            () => void pollUfsSync(path),
            ufsSyncPollIntervalMs()
          )
        } else {
          setUfsSyncJob(path, {
            state: 'Timed out',
            status: 'Timed out',
            message: t('browse.ufsSync.messages.timedOutRecovery'),
            done: true
          })
        }
      }
    },
    [api, setUfsSyncJob, t, ufsSyncPollIntervalMs]
  )

  const trackUfsSync = useCallback(
    (path: string) => {
      const normalized = normalizePath(path)
      const previous = ufsSyncJobsRef.current[normalized] || ({} as UfsSyncJob)
      setUfsSyncJob(normalized, {
        path: normalized,
        batch_id: '',
        batch_label: '',
        state: 'Checking UFS sync',
        status: 'Checking UFS sync',
        progress: Number(previous.progress || 0),
        total_files: Number(previous.total_files || 0),
        completed_files: Number(previous.completed_files || 0),
        loaded_size: Number(previous.loaded_size || 0),
        total_size: Number(previous.total_size || 0),
        done: false,
        attempts: 0
      })
      void pollUfsSync(normalized)
    },
    [pollUfsSync, setUfsSyncJob]
  )

  const trackBatchUfsSync = useCallback(
    (batchId: string, pathPrefix: string, batchLabel: string, totalFiles: number, totalSize: number) => {
      const normalized = normalizePath(pathPrefix)
      setUfsSyncJob(normalized, {
        path: normalized,
        batch_id: batchId,
        batch_label: batchLabel,
        batch_aggregate: true,
        state: 'Waiting for UFS sync',
        status: 'Waiting for UFS sync',
        progress: 0,
        total_files: totalFiles,
        completed_files: 0,
        loaded_size: 0,
        total_size: totalSize,
        done: false,
        attempts: 0
      })
      pollBatchUfsSync(batchId, normalized)
    },
    [pollBatchUfsSync, setUfsSyncJob]
  )

  const mergeUfsSyncJobFromServer = useCallback(
    (path: string, job: UfsSyncJob) => {
      if (isUfsSyncDismissed(path, job)) return
      const existing = ufsSyncJobsRef.current[path] || ({} as UfsSyncJob)
      const state = job.state || job.status || ''
      const done = Boolean(job.done) || isTerminalSyncState(state)
      if (syncState(existing) === 'Uploading to Curvine' && !done) {
        setUfsSyncJob(path, { ...existing, ...job, path, done: Boolean(existing.done) })
        return
      }
      const serverUpdated = Number(job.update_time_ms || job.updated_at || 0)
      const localUpdated = Number(existing.updated_at || 0)
      if (serverUpdated && localUpdated && serverUpdated < localUpdated && !done) return
      setUfsSyncJob(path, { ...job, path, done })
    },
    [isUfsSyncDismissed, setUfsSyncJob]
  )

  /* ------------------------------------------------------------- data load */

  const saveBrowseSnapshot = useCallback((path: string, cache: boolean, nextItems: BrowseRow[]) => {
    writeJson(browseSnapshotKey(cache, path), { items: nextItems, saved_at: Date.now() })
  }, [])

  const restoreBrowseSnapshot = useCallback((path: string, cache: boolean) => {
    const snapshot = readJson<{ items?: BrowseRow[]; saved_at?: number } | null>(
      browseSnapshotKey(cache, path),
      null
    )
    if (!snapshot || !Array.isArray(snapshot.items)) return
    if (Date.now() - Number(snapshot.saved_at || 0) > BROWSE_SNAPSHOT_TTL_MS) return
    setItems(snapshot.items)
    itemsRef.current = snapshot.items
  }, [])

  const fetchVisibleUfsSyncJobs = useCallback(
    async (token: number, options: { pathPrefix?: string; limit?: number; includeFinished?: boolean } = {}) => {
      const pathPrefix = options.pathPrefix || browsePathRef.current
      const limit = options.limit || 100
      const includeFinished = options.includeFinished !== undefined ? options.includeFinished : true
      try {
        const data = await api.fetchUfsSyncJobs(pathPrefix, limit, includeFinished, {
          includeTasks: false
        })
        if (token !== summaryLoadToken.current && token !== ufsSyncDiscoverToken.current) return
        const jobs = (Array.isArray(data) ? data : (data as { items?: UfsSyncJob[] })?.items || []) as UfsSyncJob[]
        for (const job of jobs) {
          if (!job || !job.path) continue
          mergeUfsSyncJobFromServer(normalizePath(job.path), job)
        }
      } catch {
        // Discovery is opportunistic; a failure must not break the listing.
      }
    },
    [api, mergeUfsSyncJobFromServer]
  )

  const fetchVisiblePathUfsSyncStatuses = useCallback(
    async (token: number, sourceItems: BrowseRow[]) => {
      if (Object.keys(ufsSyncBatchTimers.current).length > 0) return
      const visibleFiles = sourceItems
        .filter((item) => item && !item.is_dir && item.path && isFsModePath(String(item.path)))
        .slice(0, 20)
      for (const item of visibleFiles) {
        if (token !== summaryLoadToken.current && token !== ufsSyncDiscoverToken.current) return
        try {
          const job = (await api.fetchUfsSyncStatus(String(item.path))) as UfsSyncJob | null
          if (token !== summaryLoadToken.current && token !== ufsSyncDiscoverToken.current) return
          if (!job || !job.path) continue
          const normalized = normalizePath(job.path)
          const state = syncState(job)
          const hasExisting = Boolean(ufsSyncJobsRef.current[normalized])
          if (!hasExisting && ['NotStarted', 'NotMounted', 'Unsupported'].includes(state)) continue
          mergeUfsSyncJobFromServer(normalized, job)
        } catch {
          // Per-file status is best-effort.
        }
      }
    },
    [api, isFsModePath, mergeUfsSyncJobFromServer]
  )

  const fetchData = useCallback(
    async (overrides: { path?: string; cacheOnly?: boolean } = {}) => {
      const path = normalizePath(overrides.path ?? browsePathRef.current)
      const cache = overrides.cacheOnly ?? cacheOnlyRef.current
      const token = (summaryLoadToken.current += 1)
      restoreBrowseSnapshot(path, cache)
      setLoadError(null)
      setLoading(true)
      try {
        await ensureMounts()
        const data = await api.fetchBrowseData({ path, cache_only: cache, limit: BROWSE_PAGE })
        const nextItems = (Array.isArray(data) ? data : data?.items || []) as BrowseRow[]
        setItems(nextItems)
        itemsRef.current = nextItems
        setHasMore(Boolean((data as BrowseData)?.has_more))
        setNextStartAfter(String((data as BrowseData)?.next_start_after || ''))
        saveBrowseSnapshot(path, cache, nextItems)
        void fetchVisibleUfsSyncJobs(token, { includeFinished: false })
        void fetchVisiblePathUfsSyncStatuses(token, nextItems)
      } catch (error) {
        setLoadError(error instanceof Error ? error.message : tRef.current('common.error'))
      } finally {
        setLoading(false)
      }
    },
    [
      api,
      ensureMounts,
      fetchVisiblePathUfsSyncStatuses,
      fetchVisibleUfsSyncJobs,
      restoreBrowseSnapshot,
      saveBrowseSnapshot
    ]
  )

  /** Append the next window; the server returns the cursor as the last entry's name. */
  const loadMore = useCallback(async () => {
    if (loadingMore || !hasMore || !nextStartAfter) return
    const path = normalizePath(browsePathRef.current)
    const cache = cacheOnlyRef.current
    const token = summaryLoadToken.current
    setLoadingMore(true)
    try {
      const data = await api.fetchBrowseData({
        path,
        cache_only: cache,
        limit: BROWSE_PAGE,
        start_after: nextStartAfter
      })
      if (token !== summaryLoadToken.current) return
      const more = (Array.isArray(data) ? data : data?.items || []) as BrowseRow[]
      const previous = itemsRef.current
      const seen = new Set(previous.map((item) => String(item.path || '')))
      const merged = [...previous, ...more.filter((item) => !seen.has(String(item.path || '')))]
      setItems(merged)
      itemsRef.current = merged
      setHasMore(Boolean((data as BrowseData)?.has_more))
      setNextStartAfter(String((data as BrowseData)?.next_start_after || ''))
      saveBrowseSnapshot(path, cache, merged)
    } catch (error) {
      setLoadError(error instanceof Error ? error.message : tRef.current('common.error'))
    } finally {
      setLoadingMore(false)
    }
  }, [api, hasMore, loadingMore, nextStartAfter, saveBrowseSnapshot])

  useRefreshSignal(useCallback(() => fetchData(), [fetchData]))

  /* ----------------------------------------------------------- restore once */

  useEffect(() => {
    ufsSyncDismissedRef.current = readJson<Record<string, number>>(UFS_SYNC_DISMISSED_STORAGE_KEY, {})
    setUfsSyncPanelExpanded(readJson<boolean | null>(UFS_SYNC_PANEL_STORAGE_KEY, null) !== false)

    const storedColumns = readStoredColumns(BROWSE_COLUMNS_STORAGE_KEY, BROWSABLE_COLUMNS)
    if (storedColumns) setVisibleColumns(storedColumns)

    const now = Date.now()
    const restoredBatches = readJson<Record<string, TrackedUploadBatch>>(UPLOAD_BATCH_STORAGE_KEY, {})
    const batches: Record<string, TrackedUploadBatch> = {}
    for (const [id, batch] of Object.entries(restoredBatches)) {
      if (!batch || now - Number(batch.updated_at || now) > UPLOAD_BATCH_KEEP_MS) continue
      const normalized = normalizeUploadBatch(batch, now)
      if (normalized) batches[id] = normalized
    }
    uploadBatchesRef.current = batches
    setUploadBatches(batches)
    writeJson(UPLOAD_BATCH_STORAGE_KEY, batches)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    // Restore persisted UFS sync jobs, then resume polling for the non-terminal ones.
    const now = Date.now()
    const restored = readJson<Record<string, UfsSyncJob>>(UFS_SYNC_STORAGE_KEY, {})
    const jobs: Record<string, UfsSyncJob> = {}
    for (const [path, job] of Object.entries(restored)) {
      if (!job || now - Number(job.updated_at || now) > UFS_SYNC_RESTORE_KEEP_MS) continue
      if (isUfsSyncDismissed(path, job)) continue
      const state = syncState(job)
      if (job.done || isTerminalSyncState(state)) continue
      jobs[path] = job
    }
    ufsSyncJobsRef.current = jobs
    setUfsSyncJobs(jobs)
    for (const [path, job] of Object.entries(jobs)) {
      if (job.done || syncState(job) === 'Queued') continue
      if (job.batch_aggregate && job.batch_id) {
        pollBatchUfsSync(job.batch_id, path)
      } else if (!job.batch_id) {
        void pollUfsSync(path)
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    // Route change drives the listing, mirroring the Vue $route.query.path watcher.
    setBrowsePath(pathFromUrl)
    browsePathRef.current = pathFromUrl
    // The ticks belong to the directory that was on screen, not to the next one.
    setSelectedPaths([])
    void fetchData({ path: pathFromUrl })
  }, [pathFromUrl, fetchData])

  useEffect(() => {
    // `/blocks?path=…` redirects here as `?open=blocks&file=…`, so an old link or bookmark
    // still lands on the block locations. Read once, on arrival.
    if (searchParams.get('open') !== 'blocks') return
    const file = searchParams.get('file')
    if (!file) return
    setBlocksPath(joinPath(pathFromUrl, file))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  /* ------------------------------------------------- storage sync + discovery */

  useEffect(() => {
    const refreshUploads = () => {
      const stored = readJson<Record<string, TrackedUploadBatch>>(UPLOAD_BATCH_STORAGE_KEY, {})
      const next = { ...uploadBatchesRef.current }
      let changed = false
      const now = Date.now()
      for (const [id, batch] of Object.entries(stored)) {
        if (!batch) continue
        const normalized = normalizeUploadBatch(batch, now)
        if (!normalized) continue
        const currentUpdatedAt = Number(next[id]?.updated_at || 0)
        const storedUpdatedAt = Number(normalized.updated_at || 0)
        if (
          !next[id] ||
          storedUpdatedAt > currentUpdatedAt ||
          next[id].state !== normalized.state ||
          next[id].done !== normalized.done
        ) {
          next[id] = normalized
          changed = true
        }
      }
      if (!changed) return
      uploadBatchesRef.current = next
      setUploadBatches(next)
    }
    const tick = () => {
      refreshUploads()
      uploadStorageTimer.current = window.setTimeout(tick, 1000)
    }
    uploadStorageTimer.current = window.setTimeout(tick, 1000)
    return () => {
      if (uploadStorageTimer.current) window.clearTimeout(uploadStorageTimer.current)
    }
  }, [])

  useEffect(() => {
    const refreshJobs = () => {
      const stored = readJson<Record<string, UfsSyncJob>>(UFS_SYNC_STORAGE_KEY, {})
      const next = { ...ufsSyncJobsRef.current }
      let changed = false
      for (const [path, job] of Object.entries(stored)) {
        if (!job) continue
        if (isUfsSyncDismissed(path, job)) continue
        const state = syncState(job)
        if (job.done || isTerminalSyncState(state)) continue
        const currentUpdatedAt = Number(next[path]?.updated_at || 0)
        const storedUpdatedAt = Number(job.updated_at || 0)
        if (!next[path] || storedUpdatedAt > currentUpdatedAt) {
          next[path] = job
          changed = true
        }
      }
      if (!changed) return
      ufsSyncJobsRef.current = next
      setUfsSyncJobs(next)
    }
    const tick = () => {
      refreshJobs()
      ufsStorageTimer.current = window.setTimeout(tick, 1000)
    }
    ufsStorageTimer.current = window.setTimeout(tick, 1000)
    return () => {
      if (ufsStorageTimer.current) window.clearTimeout(ufsStorageTimer.current)
    }
  }, [isUfsSyncDismissed])

  const ufsSyncActiveCount = useMemo(
    () =>
      Object.values(ufsSyncJobs).filter(
        (job) => job && !job.done && !isTerminalSyncState(syncState(job))
      ).length,
    [ufsSyncJobs]
  )

  const ufsSyncDiscoverIntervalMs = useCallback(() => {
    if (ufsSyncActiveCount > 100) return 12000
    if (ufsSyncActiveCount > 20) return 8000
    return 4000
  }, [ufsSyncActiveCount])

  useEffect(() => {
    const token = ufsSyncDiscoverToken.current
    let cancelled = false
    const tick = async () => {
      await fetchVisibleUfsSyncJobs(token, { pathPrefix: '/', limit: 50, includeFinished: false })
      await fetchVisiblePathUfsSyncStatuses(token, itemsRef.current)
      if (!cancelled && token === ufsSyncDiscoverToken.current) {
        ufsDiscoverTimer.current = window.setTimeout(() => void tick(), ufsSyncDiscoverIntervalMs())
      }
    }
    ufsDiscoverTimer.current = window.setTimeout(() => void tick(), 500)
    return () => {
      cancelled = true
      if (ufsDiscoverTimer.current) window.clearTimeout(ufsDiscoverTimer.current)
    }
  }, [fetchVisiblePathUfsSyncStatuses, fetchVisibleUfsSyncJobs, ufsSyncDiscoverIntervalMs])

  useEffect(
    () => () => {
      Object.values(ufsSyncTimers.current).forEach((timer) => window.clearTimeout(timer))
      Object.values(ufsSyncBatchTimers.current).forEach((timer) => window.clearTimeout(timer))
      ufsSyncTimers.current = {}
      ufsSyncBatchTimers.current = {}
      if (ufsSyncSaveTimer.current) window.clearTimeout(ufsSyncSaveTimer.current)
    },
    []
  )

  /* ------------------------------------------------------------- derived UI */

  const visibleColumnList = useMemo(() => orderColumns(visibleColumns), [visibleColumns])
  const uploadBatchList = useMemo(() => Object.values(uploadBatches), [uploadBatches])

  const ufsSyncList = useMemo(
    () => Object.values(ufsSyncJobs).filter((job) => !job.batch_id),
    [ufsSyncJobs]
  )

  const ufsSyncBatchList = useMemo<UfsSyncBatch[]>(() => {
    const batches: Record<string, UfsSyncBatch> = {}
    for (const job of Object.values(ufsSyncJobs)) {
      if (!job.batch_id) continue
      const firstPath = String(job.path || job.source_path || '')
      const batch: UfsSyncBatch =
        batches[job.batch_id] ||
        ({
          id: job.batch_id,
          label: job.batch_label || firstPath,
          subtitle: firstPath,
          title: firstPath,
          totalFiles: 0,
          syncedFiles: 0,
          terminalFiles: 0,
          uploadFiles: 0,
          waitingFiles: 0,
          syncingFiles: 0,
          failedFiles: 0,
          syncedSize: 0,
          totalSize: 0,
          progressSize: 0,
          progress: 0,
          state: 'Queued',
          message: ''
        } as UfsSyncBatch)
      const state = syncState(job)
      const progress = Number(job.progress || 0)
      const totalSize = Number(job.total_size || 0)
      const totalFiles = Math.max(1, Number(job.total_files || 0))
      const completedFiles = state === 'Completed' ? totalFiles : Number(job.completed_files || 0)
      batch.totalFiles += totalFiles
      batch.syncedFiles += Math.max(0, Math.min(totalFiles, completedFiles))
      batch.totalSize += totalSize
      batch.progressSize += totalSize > 0 ? (totalSize * progress) / 100 : 0
      if (state === 'Completed') batch.syncedSize += totalSize || Number(job.loaded_size || 0)
      if (['Uploading to Curvine', 'Queued'].includes(state)) batch.uploadFiles += 1
      else if (['Waiting for UFS sync', 'Checking UFS sync', 'NotStarted'].includes(state)) {
        batch.waitingFiles += 1
      } else if (['Pending', 'Loading'].includes(state)) batch.syncingFiles += 1
      if (Boolean(job.done) || isTerminalSyncState(state)) batch.terminalFiles += 1
      if (['Failed', 'Canceled', 'Unknown', 'Timed out'].includes(state)) batch.failedFiles += 1
      if (!batch.message && job.message) batch.message = String(job.message)
      batches[job.batch_id] = batch
    }
    return Object.values(batches).map((batch) => {
      const progress =
        batch.totalSize > 0
          ? Math.max(0, Math.min(100, Math.round((batch.progressSize / batch.totalSize) * 100)))
          : batch.syncedFiles === batch.totalFiles
            ? 100
            : 0
      if (batch.syncedFiles >= batch.totalFiles) return { ...batch, state: 'Completed', progress: 100 }
      if (batch.failedFiles > 0) return { ...batch, state: 'Needs attention', progress }
      if (batch.syncingFiles > 0) return { ...batch, state: 'Syncing to UFS', progress }
      if (batch.waitingFiles > 0) return { ...batch, state: 'Waiting for UFS sync', progress }
      if (batch.uploadFiles > 0) return { ...batch, state: 'Uploading to Curvine', progress }
      if (batch.terminalFiles >= batch.totalFiles) {
        return { ...batch, state: 'Finished with skipped files', progress }
      }
      return { ...batch, progress }
    })
  }, [ufsSyncJobs])

  const ufsSyncJobCount = ufsSyncList.length + ufsSyncBatchList.length
  const ufsSyncSummaryText = ufsSyncJobCount
    ? ufsSyncActiveCount > 0
      ? t('browse.ufsSync.activeTotal', { active: ufsSyncActiveCount, total: ufsSyncJobCount })
      : t('browse.ufsSync.tracked', { total: ufsSyncJobCount })
    : ''

  const canFreeCurrentPath = isMountedPath(browsePath)
  const showUfsPanel = ufsSyncJobCount > 0

  /* ------------------------------------------------------------- selection */

  const selectablePaths = useMemo(
    () => items.map((item) => String(item.path || '')).filter(Boolean),
    [items]
  )
  // Intersected with the listing, so entries that vanished from a refresh drop out.
  const selectedSet = useMemo(
    () => new Set(selectedPaths.filter((path) => selectablePaths.includes(path))),
    [selectedPaths, selectablePaths]
  )
  const selectedItems = useMemo(
    () => items.filter((item) => item.path && selectedSet.has(String(item.path))),
    [items, selectedSet]
  )
  const allSelected = selectablePaths.length > 0 && selectedSet.size === selectablePaths.length
  const partiallySelected = selectedSet.size > 0 && !allSelected

  /**
   * Free only removes data Curvine actually holds: an entry that lives in UFS
   * alone has no cache to drop, so it is skipped rather than failing.
   */
  const freeableSelected = useMemo(
    () => selectedItems.filter((item) => storageLocation(item) !== 'ufs'),
    [selectedItems]
  )
  const skippedByFree = selectedItems.length - freeableSelected.length

  /** Mount roots are managed from the Mounts page, so batch delete passes over them. */
  const readOnlyDeleteSelected = useMemo(
    () =>
      selectedItems.filter(
        (item) =>
          item.path &&
          !isMountRoot(String(item.path)) &&
          isReadOnlyMountedPath(String(item.path))
      ),
    [selectedItems, isMountRoot, isReadOnlyMountedPath]
  )
  const deletableSelected = useMemo(
    () =>
      selectedItems.filter(
        (item) =>
          item.path &&
          !isMountRoot(String(item.path)) &&
          !isReadOnlyMountedPath(String(item.path))
      ),
    [selectedItems, isMountRoot, isReadOnlyMountedPath]
  )
  const mountedDeleteCount = deletableSelected.filter(
    (item) => item.path && isMountedPath(String(item.path))
  ).length
  const skippedByDelete = selectedItems.filter(
    (item) => item.path && isMountRoot(String(item.path))
  ).length

  /* -------------------------------------------------------------- handlers */

  useEffect(() => {
    // Auto-expand the panel when new work appears while it is collapsed.
    if (ufsSyncActiveCount > 0 && !ufsSyncPanelExpanded) {
      setUfsSyncPanelExpanded(true)
      writeJson(UFS_SYNC_PANEL_STORAGE_KEY, true)
    }
  }, [ufsSyncActiveCount, ufsSyncPanelExpanded])

  function navigateTo(path: string) {
    const normalized = normalizePath(path)
    // Compare against the route's path, not `browsePathRef`: that ref mirrors the
    // path input, so typing a path and pressing Go would compare the target with
    // itself, skip the navigation, and leave the URL (and the breadcrumb) behind.
    if (normalized === pathFromUrl) {
      void fetchData({ path: normalized })
      return
    }
    navigate({ pathname: '/browse', search: `?path=${encodeURIComponent(normalized)}` })
  }

  function setColumns(next: BrowseColumnKey[]) {
    setVisibleColumns(next)
    writeStoredColumns(BROWSE_COLUMNS_STORAGE_KEY, next)
  }

  function setBatch(batchId: string, patch: Partial<TrackedUploadBatch>) {
    const previous = uploadBatchesRef.current[batchId] || ({} as TrackedUploadBatch)
    const next = {
      ...uploadBatchesRef.current,
      [batchId]: { ...previous, ...patch, id: batchId, updated_at: Date.now() }
    } as Record<string, TrackedUploadBatch>
    uploadBatchesRef.current = next
    setUploadBatches(next)
    writeJson(UPLOAD_BATCH_STORAGE_KEY, next)
  }

  function clearCompletedUploadBatches() {
    const next: Record<string, TrackedUploadBatch> = {}
    for (const [id, batch] of Object.entries(uploadBatchesRef.current)) {
      if (!batch || batch.done) continue
      next[id] = batch
    }
    uploadBatchesRef.current = next
    setUploadBatches(next)
    writeJson(UPLOAD_BATCH_STORAGE_KEY, next)
  }

  function clearUploadBatches() {
    uploadBatchesRef.current = {}
    setUploadBatches({})
    writeJson(UPLOAD_BATCH_STORAGE_KEY, {})
  }

  function clearCompletedUfsSyncJobs() {
    const next: Record<string, UfsSyncJob> = {}
    for (const [path, job] of Object.entries(ufsSyncJobsRef.current)) {
      if (!job) continue
      const isDone = Boolean(job.done) || isTerminalSyncState(syncState(job))
      if (isDone) {
        const normalized = normalizePath(path)
        const stamp = Math.max(
          Number(ufsSyncDismissedRef.current[normalized] || 0),
          Number(job.update_time_ms || job.updated_at || 0),
          Date.now()
        )
        ufsSyncDismissedRef.current = { ...ufsSyncDismissedRef.current, [normalized]: stamp }
        if (ufsSyncTimers.current[path]) {
          window.clearTimeout(ufsSyncTimers.current[path])
          delete ufsSyncTimers.current[path]
        }
        if (job.batch_id && ufsSyncBatchTimers.current[job.batch_id]) {
          window.clearTimeout(ufsSyncBatchTimers.current[job.batch_id])
          delete ufsSyncBatchTimers.current[job.batch_id]
        }
        continue
      }
      next[path] = job
    }
    ufsSyncJobsRef.current = next
    setUfsSyncJobs(next)
    writeJson(UFS_SYNC_DISMISSED_STORAGE_KEY, ufsSyncDismissedRef.current)
    writeJson(UFS_SYNC_STORAGE_KEY, next)
  }

  function toggleUfsSyncPanel() {
    const next = !ufsSyncPanelExpanded
    setUfsSyncPanelExpanded(next)
    writeJson(UFS_SYNC_PANEL_STORAGE_KEY, next)
  }

  async function copyPath(path: string, message: string) {
    // On plain HTTP the browser will not write the clipboard and cannot be asked to confirm
    // it: the hook opens a preselected field for a real Ctrl/Cmd+C instead, and the check mark
    // would then be a lie.
    const outcome = await copy(path, message)
    if (outcome !== 'copied') return
    setCopiedPath(path)
    window.setTimeout(() => setCopiedPath((current) => (current === path ? '' : current)), 1600)
  }

  async function createDirectory(target: string) {
    setSubmitting(true)
    try {
      await api.createDirectory(target, true)
      setMkdirOpen(false)
      toast.success(t('browse.mkdirAction.success', { path: target }))
      await fetchData()
    } finally {
      setSubmitting(false)
    }
  }

  async function confirmDelete() {
    const item = deleteTarget
    if (!item || !item.path) return
    setSubmitting(true)
    try {
      await api.deletePath(String(item.path), Boolean(item.is_dir))
      setDeleteTarget(null)
      toast.success(t('browse.delete.success', { path: item.path }))
      await fetchData()
    } catch (error) {
      toast.error(t('browse.delete.failed', { error: String(error) }))
      setDeleteTarget(null)
    } finally {
      setSubmitting(false)
    }
  }

  function requestDelete(item: BrowseRow) {
    if (item.path && isMountRoot(String(item.path))) {
      toast.error(t('browse.delete.mountRoot'))
      return
    }
    setDeleteTarget(item)
  }

  function deleteTargetMessage(item: BrowseRow): string {
    const recursive = Boolean(item.is_dir)
    const mounted = item.path ? isMountedPath(String(item.path)) : false
    if (recursive) {
      return mounted
        ? t('browse.delete.confirmDirMounted', { path: item.path })
        : t('browse.delete.confirmDir', { path: item.path })
    }
    return mounted
      ? t('browse.delete.confirmFileMounted', { path: item.path })
      : t('browse.delete.confirmFile', { path: item.path })
  }

  function deleteTargetWarning(item: BrowseRow): string {
    return item.path && isMountedPath(String(item.path))
      ? t('browse.delete.warningMounted')
      : t('browse.delete.warningCurvine')
  }

  async function performFree(path: string, recursive: boolean) {
    setSubmitting(true)
    try {
      const data = (await api.freePath(path, recursive)) as { bytes?: number; inodes?: number }
      setFreeTarget(null)
      toast.success(
        t('browse.freeAction.result', {
          inodes: data?.inodes || 0,
          space: formatBytes(data?.bytes || 0)
        })
      )
      await fetchData()
    } catch (error) {
      setFreeTarget(null)
      toast.error(t('browse.freeAction.failed', { error: String(error) }))
    } finally {
      setSubmitting(false)
    }
  }

  function requestFreeItem(item: BrowseRow) {
    if (!item.path || !isMountedPath(String(item.path))) {
      toast.error(t('browse.freeAction.onlyMount'))
      return
    }
    const recursive = Boolean(item.is_dir)
    setFreeTarget({
      path: String(item.path),
      recursive,
      message: recursive
        ? t('browse.freeAction.confirmDir', { path: item.path })
        : t('browse.freeAction.confirmFile', { path: item.path })
    })
  }

  function requestFreeCurrentPath() {
    if (!canFreeCurrentPath) {
      toast.error(t('browse.freeAction.onlyMount'))
      return
    }
    setFreeTarget({
      path: browsePath,
      recursive: true,
      message: t('browse.freeAction.confirmCurrent', { path: browsePath })
    })
  }

  /* --------------------------------------------------------- batch actions */

  function toggleSelected(path: string) {
    setSelectedPaths((current) =>
      current.includes(path) ? current.filter((value) => value !== path) : [...current, path]
    )
  }

  function toggleSelectAll() {
    setSelectedPaths(allSelected ? [] : selectablePaths)
  }

  function bulkFreeMessage(): string {
    const base = t('browse.bulk.freeConfirm', { count: freeableSelected.length })
    return skippedByFree
      ? `${base} ${t('browse.bulk.freeSkipped', { count: skippedByFree })}`
      : base
  }

  function bulkDeleteMessage(): string {
    const base = t('browse.bulk.deleteConfirm', { count: deletableSelected.length })
    const messages = [base]
    if (skippedByDelete) {
      messages.push(t('browse.bulk.deleteSkipped', { count: skippedByDelete }))
    }
    if (readOnlyDeleteSelected.length) {
      messages.push(
        t('browse.bulk.deleteReadOnlySkipped', { count: readOnlyDeleteSelected.length })
      )
    }
    return messages.join(' ')
  }

  function bulkDeleteDisabledTitle(): string | undefined {
    if (deletableSelected.length > 0) return undefined
    return readOnlyDeleteSelected.length > 0
      ? t('browse.bulk.deleteNothingReadOnly')
      : t('browse.bulk.deleteNothing')
  }

  function bulkDeleteWarning(): string {
    return mountedDeleteCount > 0
      ? t('browse.bulk.deleteWarningMounted', { count: mountedDeleteCount })
      : t('browse.bulk.deleteWarningCurvine')
  }

  /**
   * Batch operations run one request at a time and tally the outcome: a single
   * failure (a locked file, a mount that changed under us) must not abandon the
   * rest of the selection, and the toast has to say what actually happened.
   */
  async function confirmBulkFree() {
    setSubmitting(true)
    let freed = 0
    let failed = 0
    let inodes = 0
    let bytes = 0
    try {
      for (const item of freeableSelected) {
        try {
          const data = (await api.freePath(String(item.path), Boolean(item.is_dir))) as {
            bytes?: number
            inodes?: number
          }
          freed += 1
          inodes += Number(data?.inodes || 0)
          bytes += Number(data?.bytes || 0)
        } catch {
          failed += 1
        }
      }
    } finally {
      setSubmitting(false)
      setBulkFreeOpen(false)
    }
    setSelectedPaths([])
    if (failed) {
      toast.error(t('browse.bulk.freeFailed', { failed, done: freed }))
    } else {
      toast.success(
        t('browse.bulk.freeSuccess', { count: freed, inodes, space: formatBytes(bytes) })
      )
    }
    await fetchData()
  }

  async function confirmBulkDelete() {
    setSubmitting(true)
    let deleted = 0
    let failed = 0
    try {
      for (const item of deletableSelected) {
        try {
          await api.deletePath(String(item.path), Boolean(item.is_dir))
          deleted += 1
        } catch {
          failed += 1
        }
      }
    } finally {
      setSubmitting(false)
      setBulkDeleteOpen(false)
    }
    setSelectedPaths([])
    if (failed) {
      toast.error(t('browse.bulk.deleteFailed', { failed, done: deleted }))
    } else {
      toast.success(t('browse.bulk.deleteSuccess', { count: deleted }))
    }
    await fetchData()
  }

  async function onFilePicked(event: React.ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0]
    if (!file) return
    const remotePath = joinPath(browsePathRef.current, file.name)
    setUfsSyncJob(remotePath, {
      path: remotePath,
      state: 'Uploading to Curvine',
      status: 'Uploading to Curvine',
      progress: 0,
      total_files: 1,
      completed_files: 0,
      loaded_size: 0,
      total_size: Number(file.size || 0),
      done: false,
      attempts: 0
    })
    try {
      const result = (await api.uploadFile(remotePath, file, true, (progressEvent) => {
        const total = Number(progressEvent.total || file.size || 0)
        const loaded = Number(progressEvent.loaded || 0)
        setUfsSyncJob(remotePath, {
          state: 'Uploading to Curvine',
          status: 'Uploading to Curvine',
          loaded_size: loaded,
          total_size: total,
          total_files: 1,
          completed_files: 0,
          done: false
        })
      })) as { ufs_sync?: UfsSyncJob } | null

      if (result?.ufs_sync) {
        setUfsSyncJob(remotePath, {
          ...(result.ufs_sync || {}),
          state: result.ufs_sync.state || result.ufs_sync.status || 'Waiting for UFS sync',
          status: result.ufs_sync.status || result.ufs_sync.state || 'Waiting for UFS sync',
          loaded_size: Number(file.size || 0),
          total_size: Number(file.size || 0),
          total_files: 1,
          completed_files: 0,
          done: false
        })
        trackUfsSync(remotePath)
      } else {
        removeUfsSyncJob(remotePath)
      }
      toast.success(t('browse.upload.uploaded', { path: remotePath }))
      await fetchData()
    } catch (error) {
      setUfsSyncJob(remotePath, {
        state: 'Upload failed',
        status: 'Upload failed',
        message: String(error),
        done: true
      })
      toast.error(t('browse.upload.failed', { error: String(error) }))
    }
  }

  async function uploadFolderBatch(
    fileItems: { file: File; relativePath: string }[],
    folderName: string
  ) {
    const batchId = `folder-${Date.now()}`
    const totalFiles = fileItems.length
    const totalSize = fileItems.reduce((sum, item) => sum + Number(item.file?.size || 0), 0)
    const trackSync = isFsModePath(normalizePath(browsePathRef.current))
    const folderPath = normalizePath(joinPath(browsePathRef.current, folderName))
    const folderLabel = folderName || t('browse.upload.folderUpload')
    toast.info(t('browse.upload.uploadingFiles', { count: totalFiles }))
    await ensureMounts()
    setBatch(batchId, {
      label: folderLabel,
      state: 'Uploading to Curvine',
      totalFiles,
      uploadedFiles: 0,
      uploadedSize: 0,
      totalSize,
      progress: 0,
      done: false,
      message: ''
    })

    let uploadedFiles = 0
    let uploadedSize = 0
    let lastUiUpdateMs = 0
    try {
      for (const { file, relativePath } of fileItems) {
        const remotePath = normalizePath(joinPath(browsePathRef.current, relativePath))
        // Folder uploads use batch-level UFS sync tracking to avoid per-file polling storms.
        await api.uploadFile(remotePath, file, true, null, 300000, false)
        uploadedFiles += 1
        uploadedSize += Number(file.size || 0)
        const now = Date.now()
        if (now - lastUiUpdateMs >= 500 || uploadedFiles === totalFiles) {
          lastUiUpdateMs = now
          setBatch(batchId, {
            state: uploadedFiles === totalFiles ? 'Uploaded to Curvine' : 'Uploading to Curvine',
            uploadedFiles,
            uploadedSize,
            progress:
              totalFiles > 0 ? Math.max(0, Math.min(100, Math.round((uploadedFiles / totalFiles) * 100))) : 100,
            done: uploadedFiles === totalFiles
          })
          await new Promise((resolve) => window.requestAnimationFrame(() => resolve(null)))
        }
      }

      toast.success(t('browse.upload.uploadedFolder', { count: uploadedFiles }))
      if (trackSync) {
        trackBatchUfsSync(batchId, folderPath, folderLabel, totalFiles, totalSize)
      }
      await fetchData()
    } catch (error) {
      setBatch(batchId, {
        state: 'Upload failed',
        uploadedFiles,
        uploadedSize,
        progress:
          totalFiles > 0 ? Math.max(0, Math.min(100, Math.round((uploadedFiles / totalFiles) * 100))) : 0,
        done: true,
        message: String(error)
      })
      toast.error(t('browse.upload.folderFailed', { error: String(error) }))
    }
  }

  async function onFolderPicked(event: React.ChangeEvent<HTMLInputElement>) {
    const files = Array.from(event.target.files || [])
    if (files.length === 0) return
    const folderName = files[0].webkitRelativePath
      ? files[0].webkitRelativePath.split('/')[0]
      : t('browse.upload.folderUpload')
    await uploadFolderBatch(
      files.map((file) => ({ file, relativePath: file.webkitRelativePath || file.name })),
      folderName
    )
  }

  function downloadItem(item: BrowseRow) {
    const path = String(item.path || '')
    const base = (import.meta.env.VITE_API_BASE || '/api').replace(/\/+$/, '')
    const url = `${base}/v1/fs/download?path=${encodeURIComponent(path)}`
    const frame = document.createElement('iframe')
    frame.style.display = 'none'
    frame.src = url
    document.body.appendChild(frame)
    frame.onload = () => {
      if (frame.parentNode) frame.parentNode.removeChild(frame)
    }
    // Avoid aborting large downloads by removing the iframe too early.
    window.setTimeout(() => {
      if (frame.parentNode) frame.parentNode.removeChild(frame)
    }, 30 * 60 * 1000)
    toast.info(
      item.is_dir
        ? t('browse.download.folder', { path })
        : t('browse.download.file', { path })
    )
  }

  function itemSizeText(item: BrowseRow): string {
    if (!item) return '-'
    if (item.is_dir) return '-'
    return formatBytes(Number(item.len || 0))
  }

  function expireText(item: BrowseRow): string {
    const expireAt = expireTimeMs(item)
    return expireAt == null ? t('browse.expire.never') : formatTime(expireAt)
  }

  function deleteItemTitle(item: BrowseRow): string {
    if (item.path && isMountRoot(String(item.path))) return t('browse.delete.mountRoot')
    if (item.path && isReadOnlyMountedPath(String(item.path))) {
      return t('browse.delete.readOnlyMount')
    }
    if (item.path && isMountedPath(String(item.path))) return t('browse.delete.mounted')
    return item.is_dir ? t('browse.delete.directory') : t('browse.delete.file')
  }

  function freeItemTitle(item: BrowseRow): string {
    if (!item.path || !isMountedPath(String(item.path))) return t('browse.freeAction.onlyMount')
    return item.is_dir ? t('browse.freeAction.itemDir') : t('browse.freeAction.itemFile')
  }

  return (
    <>
      <PageHeader
        eyebrow={t('nav.groupFilesystem')}
        title={t('browse.title')}
        description={t('browse.subtitle')}
      />

      {/* Navigation row. The trail is the primary way around — its 🏠 Root and
          every ancestor crumb cover what the old Root / Up buttons did — so the
          row is trail + path input: clicking on the left, pasting on the right.
          The edit affordance is kept because a path that is not reachable by
          clicking still has to be reachable by typing. */}
      <div className="flex flex-wrap items-center gap-2 rounded-lg border border-border bg-muted/30 px-3 py-2">
        <div className="min-w-48 flex-1">
          <PathBreadcrumb path={pathFromUrl} onNavigate={navigateTo} />
        </div>
        <form
          className="flex items-center gap-2"
          onSubmit={(event) => {
            event.preventDefault()
            navigateTo(browsePath)
          }}
        >
          <Input
            value={browsePath}
            onChange={(event) => setBrowsePath(event.target.value)}
            placeholder={t('browse.pathPlaceholder')}
            aria-label={t('browse.pathPlaceholder')}
            className="w-64"
            data-testid="browse-path"
          />
          <Button type="submit" size="sm" data-testid="browse-go">
            {t('common.go')}
          </Button>
        </form>
      </div>

      {/* Operations on the current directory. */}
      <div className="flex flex-wrap items-center gap-2">
        <label className="flex items-center gap-1.5 text-xs whitespace-nowrap">
          <Checkbox
            checked={cacheOnly}
            onCheckedChange={(checked) => {
              const next = checked === true
              setCacheOnly(next)
              cacheOnlyRef.current = next
              void fetchData({ cacheOnly: next })
            }}
            data-testid="browse-cache-only"
          />
          {t('browse.cacheOnly')}
        </label>
        <ColumnsPicker
          columns={BROWSABLE_COLUMNS}
          visible={visibleColumns}
          defaultVisible={DEFAULT_VISIBLE_COLUMNS}
          onChange={setColumns}
          testIdPrefix="browse-columns"
        />
        <Button variant="outline" size="sm" onClick={() => setMkdirOpen(true)} data-testid="browse-mkdir-open">
          {t('browse.mkdir')}
        </Button>
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            if (fileInputRef.current) {
              fileInputRef.current.value = ''
              fileInputRef.current.click()
            }
          }}
          data-testid="browse-upload-file"
        >
          {t('browse.uploadFile')}
        </Button>
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            if (folderInputRef.current) {
              folderInputRef.current.value = ''
              folderInputRef.current.click()
            }
          }}
          data-testid="browse-upload-folder"
        >
          {t('browse.uploadFolder')}
        </Button>
        <Button
          variant="outline"
          size="sm"
          disabled={!canFreeCurrentPath}
          title={canFreeCurrentPath ? t('browse.freeAction.currentTitle') : t('browse.freeAction.onlyMount')}
          onClick={requestFreeCurrentPath}
          data-testid="browse-free-current"
        >
          {t('browse.free')}
        </Button>
        <input ref={fileInputRef} type="file" className="hidden" onChange={(event) => void onFilePicked(event)} />
        <input
          ref={(node) => {
            folderInputRef.current = node
            if (node) {
              // Directory picking has no typed React prop; set the two vendor attributes directly.
              node.setAttribute('webkitdirectory', '')
              node.setAttribute('directory', '')
            }
          }}
          type="file"
          className="hidden"
          multiple
          onChange={(event) => void onFolderPicked(event)}
        />
      </div>

      {selectedSet.size > 0 && (
        <div
          className="flex flex-wrap items-center gap-2 rounded-lg border border-border bg-muted/30 px-3 py-2"
          data-testid="browse-bulk-bar"
        >
          <span className="text-xs font-medium" data-testid="browse-bulk-count">
            {t('browse.bulk.selected', { count: selectedSet.size })}
          </span>
          <Button
            variant="outline"
            size="sm"
            disabled={freeableSelected.length === 0}
            title={
              freeableSelected.length === 0 ? t('browse.bulk.freeNothing') : t('browse.freeAction.itemFile')
            }
            onClick={() => setBulkFreeOpen(true)}
            data-testid="browse-bulk-free"
          >
            {t('browse.free')}
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="text-destructive"
            disabled={deletableSelected.length === 0}
            title={bulkDeleteDisabledTitle()}
            onClick={() => setBulkDeleteOpen(true)}
            data-testid="browse-bulk-delete"
          >
            {t('common.delete')}
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setSelectedPaths([])}
            data-testid="browse-bulk-clear"
          >
            {t('browse.bulk.clear')}
          </Button>
        </div>
      )}

      {loadError && <ErrorAlert message={loadError} />}

      {uploadBatchList.length > 0 && (
        <UploadBatchesPanel
          batches={uploadBatchList}
          onClearCompleted={clearCompletedUploadBatches}
          onClear={clearUploadBatches}
        />
      )}

      {showUfsPanel && (
        <UfsSyncPanel
          batches={ufsSyncBatchList}
          jobs={ufsSyncList}
          browsePath={browsePath}
          expanded={ufsSyncPanelExpanded}
          summaryText={ufsSyncSummaryText}
          onToggle={toggleUfsSyncPanel}
          onClearCompleted={clearCompletedUfsSyncJobs}
          onViewJobs={() => navigate('/jobs')}
        />
      )}

      <Card className="gap-0 overflow-hidden py-0">
        <Table>
          <TableHeader className="sticky top-0 z-10 bg-card">
            <TableRow>
              <TableHead className="w-10">
                <Checkbox
                  checked={allSelected ? true : partiallySelected ? 'indeterminate' : false}
                  onCheckedChange={toggleSelectAll}
                  disabled={selectablePaths.length === 0}
                  aria-label={t('browse.bulk.selectAll')}
                  data-testid="browse-select-all"
                />
              </TableHead>
              <TableHead>{t('browse.table.name')}</TableHead>
              <TableHead>{t('browse.table.type')}</TableHead>
              <TableHead className="text-right">{t('browse.table.size')}</TableHead>
              <TableHead>{t('browse.table.modified')}</TableHead>
              <TableHead title={t('browse.columns.expireHint')}>
                {t('browse.table.expireTime')}
              </TableHead>
              {visibleColumnList.map((column) => (
                <TableHead
                  key={column.key}
                  className={column.numeric ? 'text-right' : undefined}
                  title={column.key === 'replicas' ? t('browse.columns.replicasHint') : undefined}
                >
                  {t(column.labelKey)}
                </TableHead>
              ))}
              <TableHead className="text-right">{t('common.actions')}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {items.map((item) => {
              const expired = isExpired(item)
              const location = storageLocation(item)
              const itemPath = String(item.path || '')
              return (
                <TableRow
                  key={itemPath}
                  className={expired ? 'bg-warning/5' : undefined}
                  data-state={selectedSet.has(itemPath) ? 'selected' : undefined}
                  data-testid={`browse-row-${itemPath}`}
                >
                  <TableCell className="w-10">
                    <Checkbox
                      checked={selectedSet.has(itemPath)}
                      onCheckedChange={() => toggleSelected(itemPath)}
                      aria-label={t('browse.bulk.selectRow', { path: itemPath })}
                      data-testid={`browse-select-${itemPath}`}
                    />
                  </TableCell>
                  <TableCell>
                    <div className="flex max-w-96 min-w-0 items-center gap-2">
                      {item.is_dir ? (
                        <FolderIcon
                          className="size-4 shrink-0 text-muted-foreground"
                          aria-hidden
                          data-testid={`browse-icon-dir-${itemPath}`}
                        />
                      ) : (
                        <FileIcon
                          className="size-4 shrink-0 text-muted-foreground"
                          aria-hidden
                          data-testid={`browse-icon-file-${itemPath}`}
                        />
                      )}
                      <button
                        type="button"
                        className="min-w-0 truncate text-left font-medium hover:text-primary"
                        title={itemPath}
                        data-testid={`browse-preview-${itemPath}`}
                        onClick={() =>
                          item.is_dir ? navigateTo(itemPath) : setPreviewTarget(item)
                        }
                      >
                        {fileName(itemPath)}
                      </button>
                      {location && (
                        /* Fixed-width slot so the cv/both/ufs tag lines up
                           across rows regardless of file-name length. */
                        <span className="ml-auto flex w-14 shrink-0 items-center">
                          <StatusBadge
                            value={t(`browse.storageLocation.${location}`)}
                            tone={
                              location === 'both' ? 'info' : location === 'cv' ? 'positive' : 'warning'
                            }
                          />
                        </span>
                      )}
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon-xs"
                            className="shrink-0 text-muted-foreground"
                            title={t('browse.copyPath')}
                            data-testid={`browse-copy-${itemPath}`}
                          >
                            {copiedPath === itemPath ? <CheckIcon /> : <CopyIcon />}
                            <span className="sr-only">{t('browse.copyPath')}</span>
                          </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="start">
                          <DropdownMenuItem
                            onSelect={() => void copyPath(itemPath, t('browse.copy.pathCopied'))}
                            data-testid={`browse-copy-path-${itemPath}`}
                          >
                            {t('browse.copyPath')}
                          </DropdownMenuItem>
                          <DropdownMenuItem
                            onSelect={() =>
                              void copyPath(fileName(itemPath), t('browse.copy.nameCopied'))
                            }
                            data-testid={`browse-copy-name-${itemPath}`}
                          >
                            {t('browse.copyName')}
                          </DropdownMenuItem>
                        </DropdownMenuContent>
                      </DropdownMenu>
                    </div>
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {item.is_dir ? t('browse.itemType.directory') : t('browse.itemType.file')}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{itemSizeText(item)}</TableCell>
                  <TableCell className="text-muted-foreground">{formatTime(item.mtime)}</TableCell>
                  <TableCell className={expired ? 'font-medium text-warning' : 'text-muted-foreground'}>
                    {expireText(item)}
                  </TableCell>
                  {visibleColumnList.map((column) => (
                    <TableCell
                      key={column.key}
                      className={column.numeric ? 'text-right tabular-nums' : undefined}
                    >
                      {column.key === 'permission' ? (
                        <MonoText>{columnText(item, column.key)}</MonoText>
                      ) : (
                        columnText(item, column.key)
                      )}
                    </TableCell>
                  ))}
                  <TableCell>
                    <div className="flex items-center justify-end gap-1">
                      <Button
                        variant="ghost"
                        size="xs"
                        className="text-primary"
                        data-testid={`browse-blocks-${itemPath}`}
                        onClick={() => (item.is_dir ? navigateTo(itemPath) : setBlocksPath(itemPath))}
                      >
                        {item.is_dir ? t('common.open') : t('common.blocks')}
                      </Button>
                      <Button
                        variant="ghost"
                        size="xs"
                        className="text-primary"
                        onClick={() => downloadItem(item)}
                        data-testid={`browse-download-${itemPath}`}
                      >
                        {t('common.download')}
                      </Button>
                      <Button
                        variant="ghost"
                        size="xs"
                        className="text-primary"
                        disabled={!item.path || !isMountedPath(itemPath)}
                        title={freeItemTitle(item)}
                        onClick={() => requestFreeItem(item)}
                      >
                        {t('browse.free')}
                      </Button>
                      <Button
                        variant="ghost"
                        size="xs"
                        className="text-destructive"
                        disabled={isMountRoot(itemPath) || isReadOnlyMountedPath(itemPath)}
                        title={deleteItemTitle(item)}
                        onClick={() => requestDelete(item)}
                        data-testid={`browse-delete-${itemPath}`}
                      >
                        {t('common.delete')}
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              )
            })}
          </TableBody>
        </Table>
        {items.length === 0 && !loading && (
          <div className="border-t border-border p-2">
            <EmptyNotice
              title={cacheOnly ? t('browse.empty.cacheOnly') : t('browse.empty.directory')}
            />
          </div>
        )}
        {loading && (
          <div className="flex items-center justify-center gap-2 border-t border-border p-3 text-xs text-muted-foreground">
            <Spinner className="size-3.5" />
            {t('common.loading')}
          </div>
        )}
        {hasMore && !loading && (
          <div className="flex items-center justify-center gap-2 border-t border-border p-2">
            <Button
              variant="outline"
              size="sm"
              disabled={loadingMore}
              onClick={() => void loadMore()}
              data-testid="browse-load-more"
            >
              {loadingMore ? <Spinner data-icon="inline-start" /> : null}
              {t('common.pagination.loadMore')}
            </Button>
          </div>
        )}
      </Card>

      {mkdirSheet.mounted && (
        <MkdirSheet
          open={mkdirSheet.open}
          defaultPath={joinPath(browsePath, t('browse.mkdirAction.defaultName'))}
          onClose={() => setMkdirOpen(false)}
          onSubmit={createDirectory}
        />
      )}

      {previewSheet.mounted && (
        <PreviewSheet
          open={previewSheet.open}
          item={previewSheet.value ?? null}
          onClose={() => setPreviewTarget(null)}
          onDownload={downloadItem}
          onShowBlocks={(target) => {
            setPreviewTarget(null)
            setBlocksPath(String(target.path || ''))
          }}
        />
      )}

      {blocksSheet.mounted && (
        <BlocksSheet
          open={blocksSheet.open}
          path={blocksSheet.value ?? ''}
          onClose={() => setBlocksPath('')}
          onNavigate={(next) => {
            setBlocksPath('')
            navigateTo(next)
          }}
        />
      )}

      <ConfirmDialog
        open={deleteTarget !== null}
        title={t('common.delete')}
        description={deleteTarget ? deleteTargetMessage(deleteTarget) : ''}
        warning={deleteTarget ? deleteTargetWarning(deleteTarget) : ''}
        confirmLabel={t('common.delete')}
        cancelLabel={t('common.cancel')}
        pending={submitting}
        destructive
        onOpenChange={(open) => {
          if (!open) setDeleteTarget(null)
        }}
        onConfirm={() => void confirmDelete()}
        testId="browse-delete-confirm"
      />

      <ConfirmDialog
        open={freeTarget !== null}
        title={t('browse.free')}
        description={freeTarget?.message || ''}
        confirmLabel={t('common.confirm')}
        cancelLabel={t('common.cancel')}
        pending={submitting}
        onOpenChange={(open) => {
          if (!open) setFreeTarget(null)
        }}
        onConfirm={() => {
          if (freeTarget) void performFree(freeTarget.path, freeTarget.recursive)
        }}
        testId="browse-free-confirm"
      />

      <ConfirmDialog
        open={bulkFreeOpen}
        title={t('browse.free')}
        description={bulkFreeMessage()}
        confirmLabel={t('common.confirm')}
        cancelLabel={t('common.cancel')}
        pending={submitting}
        onOpenChange={(open) => {
          if (!open) setBulkFreeOpen(false)
        }}
        onConfirm={() => void confirmBulkFree()}
        testId="browse-bulk-free-confirm"
      />

      <ConfirmDialog
        open={bulkDeleteOpen}
        title={t('common.delete')}
        description={bulkDeleteMessage()}
        warning={bulkDeleteWarning()}
        confirmLabel={t('common.delete')}
        cancelLabel={t('common.cancel')}
        pending={submitting}
        destructive
        onOpenChange={(open) => {
          if (!open) setBulkDeleteOpen(false)
        }}
        onConfirm={() => void confirmBulkDelete()}
        testId="browse-bulk-delete-confirm"
      />
    </>
  )
}
