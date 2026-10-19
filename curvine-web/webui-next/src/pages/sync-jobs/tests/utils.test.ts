import { describe, expect, it } from 'vitest'

import type { UfsSyncJob } from '@/lib/ufs-sync/types'
import {
  compareSyncJobsByUpdated,
  isTerminalSyncJob,
  relatedJobHref,
  relatedJobRef,
  syncJobBucket,
  syncJobSearchText,
  syncJobUpdatedMs,
  taskProgressPercent
} from '../utils'

describe('syncJobBucket', () => {
  it('maps running and waiting states to active', () => {
    expect(syncJobBucket({ state: 'Syncing to UFS' } as UfsSyncJob)).toBe('active')
    expect(syncJobBucket({ state: 'Uploading to Curvine' } as UfsSyncJob)).toBe('active')
    expect(syncJobBucket({ state: 'Waiting for UFS sync' } as UfsSyncJob)).toBe('active')
    expect(syncJobBucket({ status: 'Pending' } as UfsSyncJob)).toBe('active')
  })

  it('maps terminal states to completed or failed', () => {
    expect(syncJobBucket({ state: 'Completed' } as UfsSyncJob)).toBe('completed')
    expect(syncJobBucket({ state: 'Failed' } as UfsSyncJob)).toBe('failed')
    expect(syncJobBucket({ state: 'Canceled' } as UfsSyncJob)).toBe('failed')
  })

  it('maps skipped-file outcomes to attention', () => {
    expect(syncJobBucket({ state: 'Unsupported' } as UfsSyncJob)).toBe('attention')
    expect(syncJobBucket({ state: 'Finished with skipped files' } as UfsSyncJob)).toBe('attention')
  })
})

describe('isTerminalSyncJob', () => {
  it('treats done flags and terminal states as finished', () => {
    expect(isTerminalSyncJob({ state: 'Loading', done: true } as UfsSyncJob)).toBe(true)
    expect(isTerminalSyncJob({ state: 'Completed' } as UfsSyncJob)).toBe(true)
    expect(isTerminalSyncJob({ state: 'Syncing to UFS' } as UfsSyncJob)).toBe(false)
    expect(isTerminalSyncJob(null)).toBe(false)
  })
})

describe('relatedJobRef', () => {
  const TRANSFER_ID = '12345678-1234-1234-1234-123456789abc'
  const LOAD_ID = 'job_0123456789abcdef0123456789abcdef'

  it('prefers the backend-resolved related_job_kind and id', () => {
    // A transfer job id can belong to a load job when the backend says so.
    expect(
      relatedJobRef({ related_job_kind: 'load', related_job_id: TRANSFER_ID } as UfsSyncJob)
    ).toEqual({ kind: 'load', jobId: TRANSFER_ID })
    expect(
      relatedJobRef({ related_job_kind: 'transfer', related_job_id: 'job_x' } as UfsSyncJob)
    ).toEqual({ kind: 'transfer', jobId: 'job_x' })
  })

  it('falls back to the job id shape when related fields are absent', () => {
    expect(relatedJobRef({ job_id: TRANSFER_ID } as UfsSyncJob)).toEqual({
      kind: 'transfer',
      jobId: TRANSFER_ID
    })
    expect(relatedJobRef({ job_id: LOAD_ID } as UfsSyncJob)).toEqual({ kind: 'load', jobId: LOAD_ID })
    expect(relatedJobRef({ id: LOAD_ID } as UfsSyncJob)).toEqual({ kind: 'load', jobId: LOAD_ID })
  })

  it('ignores an unknown related_job_kind', () => {
    expect(
      relatedJobRef({ related_job_kind: 'other', related_job_id: LOAD_ID } as UfsSyncJob)
    ).toEqual({ kind: 'load', jobId: LOAD_ID })
  })

  it('returns null when there is no id at all', () => {
    expect(relatedJobRef({ path: '/a' } as UfsSyncJob)).toBeNull()
  })
})

describe('relatedJobHref', () => {
  it('routes load jobs to the jobs page and transfers to the transfers page', () => {
    expect(relatedJobHref({ kind: 'load', jobId: 'job_abc' })).toBe('/jobs?job=job_abc')
    expect(relatedJobHref({ kind: 'transfer', jobId: '12345678-1234-1234-1234-123456789abc' })).toBe(
      '/transfers?job=12345678-1234-1234-1234-123456789abc'
    )
  })

  it('escapes ids that need it', () => {
    expect(relatedJobHref({ kind: 'load', jobId: 'job a+b' })).toBe('/jobs?job=job%20a%2Bb')
  })
})

describe('syncJobSearchText', () => {
  it('joins every searchable field lowercased', () => {
    const text = syncJobSearchText({
      path: '/Lake/A.parquet',
      target_path: 's3://bucket/a.parquet',
      job_id: 'job_ABC',
      message: 'Waiting for Scheduling'
    } as UfsSyncJob)
    expect(text).toContain('/lake/a.parquet')
    expect(text).toContain('s3://bucket/a.parquet')
    expect(text).toContain('job_abc')
    expect(text).toContain('waiting for scheduling')
  })
})

describe('sync job ordering helpers', () => {
  it('sorts newest first and sinks rows without a timestamp', () => {
    const older = { update_time_ms: 100 } as UfsSyncJob
    const newer = { update_time_ms: 200 } as UfsSyncJob
    const untimed = {} as UfsSyncJob
    expect(compareSyncJobsByUpdated(older, newer)).toBeGreaterThan(0)
    expect(compareSyncJobsByUpdated(newer, older)).toBeLessThan(0)
    expect(compareSyncJobsByUpdated(older, untimed)).toBeLessThan(0)
  })

  it('prefers update_time_ms and falls back to updated_at', () => {
    expect(syncJobUpdatedMs({ update_time_ms: 5, updated_at: 9 } as UfsSyncJob)).toBe(5)
    expect(syncJobUpdatedMs({ updated_at: 9 } as UfsSyncJob)).toBe(9)
    expect(syncJobUpdatedMs({} as UfsSyncJob)).toBe(0)
  })
})

describe('taskProgressPercent', () => {
  it('derives the percent from the task byte counters', () => {
    expect(taskProgressPercent({ total_size: 200, loaded_size: 50 })).toBe(25)
    expect(taskProgressPercent({ total_size: 100, loaded_size: 100 })).toBe(100)
  })

  it('clamps and rounds out-of-range counters', () => {
    expect(taskProgressPercent({ total_size: 3, loaded_size: 1 })).toBe(33)
    expect(taskProgressPercent({ total_size: 100, loaded_size: 250 })).toBe(100)
  })

  it('reports completed tasks as 100% even without sizes', () => {
    expect(taskProgressPercent({ state: 'Completed' })).toBe(100)
    expect(taskProgressPercent({ state: 'Loading' })).toBeNull()
    expect(taskProgressPercent({})).toBeNull()
  })
})
