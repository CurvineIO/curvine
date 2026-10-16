import { describe, expect, it } from 'vitest'

import type { MountInfo } from '@/api/types'
import { buildMountTree, matchesCvPath, ttlTypeLabel } from './utils'

function mount(cvPath: string): MountInfo {
  return { cv_path: cvPath, ufs_path: `file://${cvPath}` }
}

describe('ttlTypeLabel', () => {
  it('maps proto values to ttl type labels', () => {
    expect(ttlTypeLabel(0)).toBe('mtime')
    expect(ttlTypeLabel(1)).toBe('atime')
    expect(ttlTypeLabel('Mtime')).toBe('mtime')
    expect(ttlTypeLabel('Atime')).toBe('atime')
    expect(ttlTypeLabel('mtime')).toBe('mtime')
    expect(ttlTypeLabel('atime')).toBe('atime')
  })

  it('defaults missing or unknown values to mtime', () => {
    expect(ttlTypeLabel(undefined)).toBe('mtime')
    expect(ttlTypeLabel(null)).toBe('mtime')
    expect(ttlTypeLabel('')).toBe('mtime')
    expect(ttlTypeLabel('bogus')).toBe('mtime')
  })
})

describe('matchesCvPath', () => {
  it('matches a path fragment and requires every token', () => {
    expect(matchesCvPath('/lake/raw', 'lake')).toBe(true)
    expect(matchesCvPath('/lake/raw', 'lake/raw')).toBe(true)
    expect(matchesCvPath('/lake/raw', 'lake curated')).toBe(false)
  })

  it('excludes paths containing a `-` or `!` token', () => {
    expect(matchesCvPath('/lake/tmp/x', 'lake -tmp')).toBe(false)
    expect(matchesCvPath('/lake/raw', 'lake -tmp')).toBe(true)
    expect(matchesCvPath('/lake/tmp/x', '!tmp')).toBe(false)
  })

  it('treats an exclusion-only query as "not containing that token"', () => {
    expect(matchesCvPath('/lake/raw', '-tmp')).toBe(true)
    expect(matchesCvPath('/lake/tmp', '-tmp')).toBe(false)
  })

  it('accepts everything for an empty query', () => {
    expect(matchesCvPath('/lake/raw', '')).toBe(true)
  })
})

describe('buildMountTree', () => {
  it('nests mounts under their shared path segments', () => {
    const tree = buildMountTree([
      mount('/lake/raw'),
      mount('/lake/curated'),
      mount('/warehouse')
    ])

    expect(tree.map((node) => node.name)).toEqual(['lake', 'warehouse'])
    const lake = tree[0]
    expect(lake.mount).toBeUndefined()
    expect(lake.children.map((node) => node.name)).toEqual(['curated', 'raw'])
    expect(lake.children[1].mount?.cv_path).toBe('/lake/raw')
    expect(lake.children[1].path).toBe('/lake/raw')
    expect(tree[1].mount?.cv_path).toBe('/warehouse')
  })

  it('marks an intermediate node as a mount when one sits on it', () => {
    const tree = buildMountTree([mount('/lake'), mount('/lake/raw')])
    expect(tree).toHaveLength(1)
    expect(tree[0].mount?.cv_path).toBe('/lake')
    expect(tree[0].children[0].mount?.cv_path).toBe('/lake/raw')
  })
})
