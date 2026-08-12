import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join, relative } from 'node:path'

export function flattenLocaleKeys(
  obj: Record<string, unknown>,
  prefix = ''
): string[] {
  const keys: string[] = []
  for (const [key, value] of Object.entries(obj)) {
    const path = prefix ? `${prefix}.${key}` : key
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      keys.push(...flattenLocaleKeys(value as Record<string, unknown>, path))
    } else {
      keys.push(path)
    }
  }
  return keys.sort()
}

export function hasLocaleKey(messages: Record<string, unknown>, key: string): boolean {
  const parts = key.split('.')
  let current: unknown = messages
  for (const part of parts) {
    if (!current || typeof current !== 'object' || !(part in current)) {
      return false
    }
    current = (current as Record<string, unknown>)[part]
  }
  return true
}

const SOURCE_ROOT = join(__dirname, '..')
const SOURCE_SCAN_ROOTS = ['components', 'views', 'utils', 'i18n']

function collectSourceFiles(dir: string, files: string[] = []): string[] {
  for (const name of readdirSync(dir)) {
    const path = join(dir, name)
    const stat = statSync(path)
    if (stat.isDirectory()) {
      if (name === 'node_modules' || name === 'dist') continue
      collectSourceFiles(path, files)
      continue
    }
    if (!/\.(vue|ts)$/.test(name)) continue
    if (/\.test\.(vue|ts)$/.test(name)) continue
    files.push(path)
  }
  return files
}

const TRANSLATION_KEY_PATTERN = /\$t\(\s*['"]([a-z][\w.-]*(?:\.[\w.-]+)+)['"]/g

export function collectStaticTranslationKeys(): string[] {
  const keys = new Set<string>()
  for (const root of SOURCE_SCAN_ROOTS) {
    const absoluteRoot = join(SOURCE_ROOT, root)
    try {
      for (const file of collectSourceFiles(absoluteRoot)) {
        const content = readFileSync(file, 'utf8')
        for (const match of content.matchAll(TRANSLATION_KEY_PATTERN)) {
          const key = match[1]
          if (!key.includes('${')) {
            keys.add(key)
          }
        }
      }
    } catch {
      // Optional scan roots may be absent in trimmed workspaces.
    }
  }
  return Array.from(keys).sort()
}

export const ZH_ONLY_LOCALE_PREFIXES = [
  'observability.panels.',
  'operations.checks.items.',
  'operations.alerts.targetTitle.',
  'operations.targets.reasons.'
] as const

export function isZhOnlyLocaleKey(key: string): boolean {
  return ZH_ONLY_LOCALE_PREFIXES.some((prefix) => key.startsWith(prefix))
}

export function relativeSourcePath(path: string): string {
  return relative(SOURCE_ROOT, path)
}
