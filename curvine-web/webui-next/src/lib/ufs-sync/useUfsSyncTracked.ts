import { useCallback, useEffect, useRef, useState } from 'react'
import type { MutableRefObject } from 'react'

import type { UfsSyncJob } from './types'
import { isTerminalSyncState, normalizePath, syncState } from '@/pages/browse/utils'
import {
  flushTrackedUfsSyncJobs,
  isUfsSyncDismissed as isJobDismissed,
  readRestorableUfsSyncJobs,
  readTrackedUfsSyncJobs,
  readUfsSyncDismissed,
  removeStoredUfsSyncJob,
  writeTrackedUfsSyncJobs,
  writeUfsSyncDismissed
} from './storage'

export interface UfsSyncTracked {
  jobs: Record<string, UfsSyncJob>
  jobsRef: MutableRefObject<Record<string, UfsSyncJob>>
  dismissedRef: MutableRefObject<Record<string, number>>
  /** True once the persisted rows have been restored on mount. */
  restored: boolean
  setJob(path: string, job: Partial<UfsSyncJob>): void
  removeJob(path: string): void
  dismiss(path: string, stamp: number): void
  replaceAll(jobs: Record<string, UfsSyncJob>): void
  isDismissed(path: string, job?: UfsSyncJob | null): boolean
}

/**
 * The shared UFS sync tracker: restores persisted rows on mount, keeps them in
 * step with other tabs through the storage key (1s), and persists changes with
 * a debounced flush. Pages that seed uploads and pages that only display the
 * rows mount the same hook, so both see one state machine.
 */
export function useUfsSyncTracked(): UfsSyncTracked {
  const [jobs, setJobs] = useState<Record<string, UfsSyncJob>>({})
  const jobsRef = useRef<Record<string, UfsSyncJob>>({})
  const dismissedRef = useRef<Record<string, number>>({})
  const saveTimer = useRef<number | null>(null)
  const [restored, setRestored] = useState(false)

  const isDismissed = useCallback(
    (path: string, job?: UfsSyncJob | null) => isJobDismissed(dismissedRef.current, path, job),
    []
  )

  const flushToStorage = useCallback(() => {
    saveTimer.current = null
    flushTrackedUfsSyncJobs(jobsRef.current, dismissedRef.current)
  }, [])

  const scheduleSave = useCallback(() => {
    if (saveTimer.current) window.clearTimeout(saveTimer.current)
    saveTimer.current = window.setTimeout(flushToStorage, 400)
  }, [flushToStorage])

  const setJob = useCallback(
    (path: string, job: Partial<UfsSyncJob>) => {
      const normalized = normalizePath(path)
      const nextJob: UfsSyncJob = {
        ...(jobsRef.current[normalized] || ({} as UfsSyncJob)),
        ...job,
        path: normalized,
        updated_at: Date.now()
      }
      if (isJobDismissed(dismissedRef.current, normalized, nextJob)) return
      const next = { ...jobsRef.current, [normalized]: nextJob }
      jobsRef.current = next
      setJobs(next)
      scheduleSave()
    },
    [scheduleSave]
  )

  const removeJob = useCallback((path: string) => {
    const normalized = normalizePath(path)
    const next = { ...jobsRef.current }
    delete next[normalized]
    jobsRef.current = next
    setJobs(next)
    removeStoredUfsSyncJob(normalized)
  }, [])

  const dismiss = useCallback((path: string, stamp: number) => {
    const normalized = normalizePath(path)
    dismissedRef.current = {
      ...dismissedRef.current,
      [normalized]: Math.max(Number(dismissedRef.current[normalized] || 0), stamp)
    }
    writeUfsSyncDismissed(dismissedRef.current)
  }, [])

  const replaceAll = useCallback((next: Record<string, UfsSyncJob>) => {
    jobsRef.current = next
    setJobs(next)
    writeTrackedUfsSyncJobs(next)
  }, [])

  useEffect(() => {
    dismissedRef.current = readUfsSyncDismissed()
    jobsRef.current = readRestorableUfsSyncJobs()
    setJobs(jobsRef.current)
    setRestored(true)
    return () => {
      if (saveTimer.current) window.clearTimeout(saveTimer.current)
    }
  }, [])

  useEffect(() => {
    let timer: number | undefined
    const refreshJobs = () => {
      const stored = readTrackedUfsSyncJobs()
      const next = { ...jobsRef.current }
      let changed = false
      for (const [path, job] of Object.entries(stored)) {
        if (!job) continue
        if (isJobDismissed(dismissedRef.current, path, job)) continue
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
      jobsRef.current = next
      setJobs(next)
    }
    const tick = () => {
      refreshJobs()
      timer = window.setTimeout(tick, 1000)
    }
    timer = window.setTimeout(tick, 1000)
    return () => {
      if (timer) window.clearTimeout(timer)
    }
  }, [])

  return { jobs, jobsRef, dismissedRef, restored, setJob, removeJob, dismiss, replaceAll, isDismissed }
}
