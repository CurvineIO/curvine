import type { MountInfo } from '@/api/types'

export const SECRET_KEY_PATTERN =
  /(secret|password|token|credential|accesskeysecret|credentials\.secret)/i

export interface PropertyRow {
  id: string
  key: string
  value: string
}

let propertyRowSeed = 0

export function nextPropertyRowId(): string {
  propertyRowSeed += 1
  return `property-${propertyRowSeed}`
}

export function rowsFromProperties(properties: Record<string, unknown> | null | undefined): PropertyRow[] {
  return Object.entries(properties || {}).map(([key, value]) => ({
    id: nextPropertyRowId(),
    key,
    value: String(value ?? '')
  }))
}

export function writeTypeLabel(value: unknown): string {
  if (value === 1 || value === 'FsMode') return 'fs_mode'
  if (value === 0 || value === 'CacheMode') return 'cache_mode'
  return String(value || 'fs_mode').toLowerCase()
}

export function accessModeLabel(value: unknown): string {
  if (value === 0 || value === 'ReadOnly' || value === 'read_only') return 'read_only'
  if (value === 1 || value === 'ReadWrite' || value === 'read_write') return 'read_write'
  return String(value || 'read_only').toLowerCase()
}

export function ttlTypeLabel(value: unknown): 'mtime' | 'atime' {
  if (value === 1 || value === 'Atime' || value === 'atime') return 'atime'
  return 'mtime'
}

export function providerLabel(value: unknown): string {
  if (!value) return 'auto'
  if (value === 'OssHdfs') return 'oss-hdfs'
  if (value === 'Opendal') return 'opendal'
  if (value === 'Auto') return 'auto'
  return String(value).toLowerCase()
}

export function schemeOf(path: unknown): string {
  return String(path || '').split(':')[0]
}

export function formatMountTtl(ms: unknown): string {
  const value = Number(ms || 0)
  if (!value) return '0ms'
  if (value % 86400000 === 0) return `${value / 86400000}d`
  if (value % 3600000 === 0) return `${value / 3600000}h`
  if (value % 60000 === 0) return `${value / 60000}m`
  if (value % 1000 === 0) return `${value / 1000}s`
  return `${value}ms`
}

export function normalizeUfsPath(path: unknown): string {
  const value = String(path || '').trim()
  return value.startsWith('/') ? `file://${value}` : value
}

/**
 * Case-insensitive path match with optional exclusions.
 *
 * A query is split on whitespace and `/`. A token prefixed `-` or `!` must NOT
 * appear in the path; every other token must appear. So `lake -tmp` keeps the
 * paths under `lake` that do not mention `tmp`, which is how the mount table
 * hides generated subtrees.
 */
export function matchesCvPath(cvPath: unknown, query: unknown): boolean {
  const path = String(cvPath || '').toLowerCase()
  const normalized = String(query || '')
    .trim()
    .toLowerCase()
    .replace(/\\/g, '/')
  if (!normalized) return true

  const tokens = normalized.split(/[\s/]+/).filter(Boolean)
  if (!tokens.length) return true

  const excludes = tokens
    .filter((token) => /^[-!]/.test(token))
    .map((token) => token.slice(1))
    .filter(Boolean)
  if (excludes.some((token) => path.includes(token))) return false

  const includes = tokens.filter((token) => !/^[-!]/.test(token))
  if (!includes.length) return true
  // A slash-joined query ("lake/raw") also matches as one fragment.
  if (path.includes(normalized)) return true
  return includes.every((token) => path.includes(token))
}

export interface MountTreeNode {
  /** Segment name, e.g. `raw`. */
  name: string
  /** Full Curvine path, e.g. `/lake/raw`. */
  path: string
  /** The mount declared at this path, when one exists. */
  mount?: MountInfo
  children: MountTreeNode[]
}

/**
 * Organise mounts by the nesting of their Curvine paths, so `/lake/raw` and
 * `/lake/curated` share a `/lake` node. Intermediate nodes with no mount are
 * kept — they are how the hierarchy stays readable.
 */
export function buildMountTree(mounts: MountInfo[]): MountTreeNode[] {
  const roots: MountTreeNode[] = []
  const index = new Map<string, MountTreeNode>()

  for (const mount of mounts) {
    const segments = String(mount.cv_path || '/')
      .split('/')
      .filter(Boolean)
    if (!segments.length) {
      roots.push({ name: '/', path: '/', mount, children: [] })
      continue
    }
    let siblings = roots
    let prefix = ''
    let node: MountTreeNode | undefined
    for (const segment of segments) {
      prefix = `${prefix}/${segment}`
      let existing = index.get(prefix)
      if (!existing) {
        existing = { name: segment, path: prefix, children: [] }
        index.set(prefix, existing)
        siblings.push(existing)
      }
      node = existing
      siblings = existing.children
    }
    if (node) node.mount = mount
  }

  sortMountNodes(roots)
  return roots
}

function sortMountNodes(nodes: MountTreeNode[]): void {
  nodes.sort((left, right) => left.name.localeCompare(right.name))
  for (const node of nodes) sortMountNodes(node.children)
}

export function parsePropertiesText(
  text: string,
  invalidProperty: (line: string) => string
): Record<string, string> {
  const props: Record<string, string> = {}
  for (const line of String(text || '').split('\n')) {
    const value = line.trim()
    if (!value || value.startsWith('#')) continue
    const index = value.indexOf('=')
    if (index <= 0) throw new Error(invalidProperty(value))
    props[value.slice(0, index).trim()] = value.slice(index + 1).trim()
  }
  return props
}

export function textFromProperties(properties: Record<string, unknown> | null | undefined): string {
  return Object.entries(properties || {})
    .map(([key, value]) => `${key}=${value}`)
    .join('\n')
}

export function isSecretKey(key: unknown): boolean {
  return SECRET_KEY_PATTERN.test(String(key || ''))
}

export function propertiesHintKey(provider: string, scheme: string): string {
  if (provider === 'oss-hdfs' || scheme === 'oss') return 'mounts.propertiesHints.ossHdfs'
  if (scheme === 'hdfs') return 'mounts.propertiesHints.hdfs'
  if (scheme === 'webhdfs' || scheme === 'http' || scheme === 'https') {
    return 'mounts.propertiesHints.webhdfs'
  }
  if (scheme === 'file' || scheme === '') return 'mounts.propertiesHints.file'
  return 'mounts.propertiesHints.s3'
}

export function propertyKeyPlaceholder(provider: string, scheme: string): string {
  if (provider === 'oss-hdfs' || scheme === 'oss') return 'oss.endpoint'
  if (scheme === 'hdfs') return 'hdfs.namenode'
  if (scheme === 'webhdfs' || scheme === 'http' || scheme === 'https') return 'webhdfs.endpoint'
  return 's3.endpoint_url'
}

export function propertiesTextPlaceholder(key: string): string {
  if (key === 'oss.endpoint') {
    return `${key}=https://oss-cn-hangzhou.aliyuncs.com\noss.accessKeyId=\noss.accessKeySecret=`
  }
  if (key === 'hdfs.namenode') {
    return `${key}=hdfs://namenode:8020\nhdfs.user=`
  }
  if (key === 'webhdfs.endpoint') {
    return `${key}=http://namenode:9870\nwebhdfs.root=/`
  }
  return `${key}=https://s3.example.com\ns3.credentials.access=\ns3.credentials.secret=\ns3.region_name=`
}

export function defaultPropertyKeys(provider: string, scheme: string): string[] {
  if (provider === 'oss-hdfs' || scheme === 'oss') {
    return ['oss.endpoint', 'oss.accessKeyId', 'oss.accessKeySecret', 'oss.region']
  }
  if (scheme === 'hdfs') {
    return ['hdfs.namenode', 'hdfs.user', 'hdfs.root']
  }
  if (scheme === 'webhdfs' || scheme === 'http' || scheme === 'https') {
    return ['webhdfs.endpoint', 'webhdfs.root']
  }
  return ['s3.endpoint_url', 's3.credentials.access', 's3.credentials.secret', 's3.region_name']
}
