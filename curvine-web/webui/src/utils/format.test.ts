import { describe, expect, it } from 'vitest'
import {
  formatBytes,
  formatBytesPerSec,
  getUptimeParts,
  parseDateTime,
  versionsMismatch,
  workerShortVersion
} from './format'

describe('formatBytes', () => {
  it('formats zero and invalid values', () => {
    expect(formatBytes(0)).toBe('0 B')
    expect(formatBytes(-5)).toBe('0 B')
    expect(formatBytes(Number.NaN)).toBe('0 B')
    expect(formatBytes(null)).toBe('0 B')
  })

  it('formats sub-byte values without undefined units', () => {
    expect(formatBytes(0.5)).toBe('0.5 B')
    expect(formatBytes(0.05)).toBe('0.05 B')
    expect(formatBytesPerSec(0.5)).toBe('0.5 B/s')
  })

  it('formats common byte scales', () => {
    expect(formatBytes(100)).toBe('100 B')
    expect(formatBytes(1024)).toBe('1.00 KB')
    expect(formatBytes(1_500_000)).toBe('1.43 MB')
  })
})

describe('getUptimeParts', () => {
  const start = '2026-08-18 10:00:00'
  const base = parseDateTime(start)!.getTime()

  it('returns minutes under one hour', () => {
    expect(getUptimeParts(start, base + 45 * 60_000)).toEqual({ kind: 'min', count: 45 })
  })

  it('returns hours under one day', () => {
    expect(getUptimeParts(start, base + 5 * 3_600_000)).toEqual({ kind: 'hours', count: 5 })
  })

  it('returns days without leftover hours', () => {
    expect(getUptimeParts(start, base + 48 * 3_600_000)).toEqual({ kind: 'days', count: 2 })
  })

  it('returns days with leftover hours', () => {
    expect(getUptimeParts(start, base + (26 * 3_600_000))).toEqual({
      kind: 'daysHours',
      days: 1,
      hours: 2
    })
  })

  it('returns null for missing start time', () => {
    expect(getUptimeParts(null)).toBeNull()
    expect(getUptimeParts('')).toBeNull()
  })

  it('prefers start_time_ms over local datetime parsing', () => {
    const now = 1_725_000_000_000
    expect(getUptimeParts('2020-01-01 00:00:00', now, now - 90 * 60_000)).toEqual({
      kind: 'hours',
      count: 1
    })
  })
})

describe('workerShortVersion / versionsMismatch', () => {
  it('reads software_version from worker payload', () => {
    expect(workerShortVersion({ software_version: '0.4.0' })).toBe('0.4.0')
    expect(workerShortVersion({ version: '1.2.3' })).toBe('1.2.3')
    expect(workerShortVersion({ software_version: '  ' })).toBe('')
  })

  it('detects mismatch only when both sides are present', () => {
    expect(versionsMismatch('0.4.0', '0.3.0')).toBe(true)
    expect(versionsMismatch('0.4.0', '0.4.0')).toBe(false)
    expect(versionsMismatch('0.4.0', '')).toBe(false)
    expect(versionsMismatch('', '0.4.0')).toBe(false)
  })
})
