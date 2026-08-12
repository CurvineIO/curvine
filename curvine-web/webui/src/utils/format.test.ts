import { describe, expect, it } from 'vitest'
import { formatBytes, formatBytesPerSec } from './format'

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
