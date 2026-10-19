import { describe, expect, it } from 'vitest'

import type { Translate, TranslateParams } from '@/i18n'

import {
  aggregateBatchUfsSyncJobs,
  batchUfsSyncPollIntervalMs,
  buildUfsSyncBatches,
  countActiveUfsSyncJobs,
  ufsSyncPollIntervalMs,
  ufsSyncSummaryText
} from './aggregate'
import type { UfsSyncJob } from './types'

function makeT(): Translate {
  const fn = (key: string, params?: TranslateParams) =>
    params ? `${key} ${JSON.stringify(params)}` : key
  return fn as unknown as Translate
}

function job(patch: Partial<UfsSyncJob>): UfsSyncJob {
  return { path: '/mnt/fs/a', ...patch } as UfsSyncJob
}

describe('aggregateBatchUfsSyncJobs', () => {
  const previous = job({ path: '/mnt/fs/folder', total_files: 3, total_size: 1000 })

  it('marks the batch done when every file completed', () => {
    const next = aggregateBatchUfsSyncJobs(
      [
        job({ state: 'Completed', total_files: 2, completed_files: 0, loaded_size: 400 }),
        job({ state: 'Completed', total_files: 1, completed_files: 1, loaded_size: 600 })
      ],
      previous
    )
    expect(next.state).toBe('Completed')
    expect(next.status).toBe('Completed')
    expect(next.completed_files).toBe(3)
    expect(next.total_files).toBe(3)
    expect(next.done).toBe(true)
    expect(next.loaded_size).toBe(1000)
  })

  it('flags needs-attention and finishes once failures account for the rest', () => {
    const next = aggregateBatchUfsSyncJobs(
      [
        job({ state: 'Completed', total_files: 2, completed_files: 2 }),
        job({ state: 'Pending', total_files: 1, completed_files: 0 }),
        job({ state: 'Failed', total_files: 1, completed_files: 0, message: 'boom' })
      ],
      previous
    )
    expect(next.state).toBe('Needs attention')
    expect(next.message).toBe('boom')
    expect(next.done).toBe(true)
  })

  it('keeps the batch running while any member is still syncing', () => {
    const next = aggregateBatchUfsSyncJobs(
      [
        job({ state: 'Completed', total_files: 2, completed_files: 2 }),
        job({ state: 'Pending', total_files: 1, completed_files: 0 })
      ],
      previous
    )
    expect(next.state).toBe('Syncing to UFS')
    expect(next.done).toBe(false)
  })

  it('falls back to waiting when nothing has started', () => {
    const next = aggregateBatchUfsSyncJobs([job({ state: 'Queued', total_files: 1 })], previous)
    expect(next.state).toBe('Waiting for UFS sync')
    expect(next.done).toBe(false)
  })

  it('keeps the larger of the tracked and observed file counts', () => {
    const next = aggregateBatchUfsSyncJobs(
      [job({ state: 'Completed', total_files: 2, completed_files: 2 })],
      previous
    )
    expect(next.total_files).toBe(3)
    expect(next.total_size).toBe(1000)
  })
})

describe('poll intervals', () => {
  it('scales the batch poll interval with the member count', () => {
    expect(batchUfsSyncPollIntervalMs(10)).toBe(3000)
    expect(batchUfsSyncPollIntervalMs(51)).toBe(5000)
    expect(batchUfsSyncPollIntervalMs(201)).toBe(8000)
  })

  it('scales the per-path poll interval with the active poll count', () => {
    expect(ufsSyncPollIntervalMs(0)).toBe(2000)
    expect(ufsSyncPollIntervalMs(6)).toBe(3000)
    expect(ufsSyncPollIntervalMs(21)).toBe(5000)
  })
})

describe('countActiveUfsSyncJobs', () => {
  it('counts only rows that are neither done nor terminal', () => {
    const jobs = {
      '/a': job({ path: '/a', state: 'Loading' }),
      '/b': job({ path: '/b', state: 'Completed' }),
      '/c': job({ path: '/c', state: 'Failed' }),
      '/d': job({ path: '/d', state: 'Checking UFS sync', done: false })
    }
    expect(countActiveUfsSyncJobs(jobs)).toBe(2)
    expect(countActiveUfsSyncJobs({})).toBe(0)
  })
})

describe('buildUfsSyncBatches', () => {
  it('groups members by batch id with size-weighted progress', () => {
    const batches = buildUfsSyncBatches({
      '/a': job({
        path: '/a',
        batch_id: 'b1',
        batch_label: 'Folder',
        state: 'Loading',
        progress: 50,
        total_size: 400,
        total_files: 2,
        completed_files: 1
      }),
      '/b': job({
        path: '/b',
        batch_id: 'b1',
        state: 'Completed',
        progress: 100,
        total_size: 600,
        loaded_size: 600,
        total_files: 2
      })
    })
    expect(batches).toHaveLength(1)
    const batch = batches[0]
    expect(batch.label).toBe('Folder')
    expect(batch.totalFiles).toBe(4)
    expect(batch.syncedFiles).toBe(3)
    expect(batch.totalSize).toBe(1000)
    expect(batch.syncedSize).toBe(600)
    expect(batch.progress).toBe(80)
    expect(batch.state).toBe('Syncing to UFS')
    expect(batch.syncingFiles).toBe(1)
  })

  it('reports a completed batch at 100 percent', () => {
    const batches = buildUfsSyncBatches({
      '/a': job({
        path: '/a',
        batch_id: 'b1',
        state: 'Completed',
        progress: 100,
        total_size: 500,
        loaded_size: 500,
        total_files: 1
      })
    })
    expect(batches[0].state).toBe('Completed')
    expect(batches[0].progress).toBe(100)
  })

  it('falls back to file counts when sizes are unknown', () => {
    const batches = buildUfsSyncBatches({
      '/a': job({ path: '/a', batch_id: 'b1', state: 'Completed', total_files: 1 }),
      '/b': job({ path: '/b', batch_id: 'b1', state: 'Pending', total_files: 1 })
    })
    expect(batches[0].progress).toBe(0)
    expect(batches[0].state).toBe('Syncing to UFS')
  })

  it('reports an all-completed size-less batch at 100 percent', () => {
    const batches = buildUfsSyncBatches({
      '/a': job({ path: '/a', batch_id: 'b1', state: 'Completed', total_files: 1 }),
      '/b': job({ path: '/b', batch_id: 'b1', state: 'Completed', total_files: 1 })
    })
    expect(batches[0].progress).toBe(100)
    expect(batches[0].state).toBe('Completed')
  })

  it('needs attention as soon as one member failed', () => {
    const batches = buildUfsSyncBatches({
      '/a': job({ path: '/a', batch_id: 'b1', state: 'Completed', total_files: 1 }),
      '/b': job({ path: '/b', batch_id: 'b1', state: 'Failed', total_files: 1, message: 'x' })
    })
    expect(batches[0].state).toBe('Needs attention')
    expect(batches[0].failedFiles).toBe(1)
    expect(batches[0].message).toBe('x')
  })

  it('keeps the first message seen across members', () => {
    const batches = buildUfsSyncBatches({
      '/a': job({ path: '/a', batch_id: 'b1', state: 'Pending', message: 'first' }),
      '/b': job({ path: '/b', batch_id: 'b1', state: 'Pending', message: 'second' })
    })
    expect(batches[0].message).toBe('first')
  })
})

describe('ufsSyncSummaryText', () => {
  it('counts one row per path plus one per batch', () => {
    const jobs = {
      '/a': job({ path: '/a', state: 'Loading' }),
      '/b': job({ path: '/b', batch_id: 'b1', state: 'Loading' }),
      '/c': job({ path: '/c', batch_id: 'b1', state: 'Loading' })
    }
    expect(ufsSyncSummaryText(makeT(), jobs)).toBe(
      'browse.ufsSync.activeTotal {"active":3,"total":2}'
    )
  })

  it('uses the tracked wording when nothing is active', () => {
    const jobs = {
      '/a': job({ path: '/a', state: 'Completed' }),
      '/b': job({ path: '/b', batch_id: 'b1', state: 'Completed' })
    }
    expect(ufsSyncSummaryText(makeT(), jobs)).toBe('browse.ufsSync.tracked {"total":2}')
  })

  it('is empty without tracked rows', () => {
    expect(ufsSyncSummaryText(makeT(), {})).toBe('')
  })
})
