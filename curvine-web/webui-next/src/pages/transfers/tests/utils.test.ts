import { describe, expect, it } from 'vitest'

import type { TransferInfo } from '@/api/types'
import { failedTaskCount, runningTaskCount } from '../utils'

describe('transfer task counts', () => {
  it('uses full summary counts instead of the loaded task page', () => {
    const transfer = {
      state: 'running',
      failed_files: 12,
      running_files: 45,
      tasks: [
        { task_id: 'failed-1', state: 'failed' },
        { task_id: 'running-1', state: 'running' }
      ]
    } as TransferInfo

    expect(failedTaskCount(transfer)).toBe(12)
    expect(runningTaskCount(transfer)).toBe(45)
  })

  it('keeps a known zero from the summary when loaded task rows disagree', () => {
    const transfer = {
      state: 'running',
      failed_files: 0,
      running_files: 0,
      tasks: [{ task_id: 'failed-1', state: 'failed' }]
    } as TransferInfo

    expect(failedTaskCount(transfer)).toBe(0)
    expect(runningTaskCount(transfer)).toBe(0)
  })

  it('falls back to task rows when the summary is unavailable', () => {
    const transfer = {
      state: 'running',
      tasks: [
        { task_id: 'failed-1', state: 'failed' },
        { task_id: 'pending-1', state: 'pending' },
        { task_id: 'running-1', state: 'running' }
      ]
    } as TransferInfo

    expect(failedTaskCount(transfer)).toBe(1)
    expect(runningTaskCount(transfer)).toBe(2)
  })
})
