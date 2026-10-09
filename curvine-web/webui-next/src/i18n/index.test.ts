import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'

import en from './messages/en'
import zhCN from './messages/zh-CN'
import { LOCALE_STORAGE_KEY, detectLocale, translateWith } from './index'

function flattenKeys(source: Record<string, unknown>, prefix = ''): string[] {
  const keys: string[] = []
  for (const [key, value] of Object.entries(source)) {
    const path = prefix ? `${prefix}.${key}` : key
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      keys.push(...flattenKeys(value as Record<string, unknown>, path))
    } else {
      keys.push(path)
    }
  }
  return keys.sort()
}

describe('translateWith', () => {
  it('resolves dotted keys and interpolates params', () => {
    expect(translateWith('en', 'login.username')).toBe('Username')
    expect(translateWith('en', 'overview.metrics.liveLost', { live: 3, lost: 1 })).toBe(
      '3 live, 1 lost'
    )
  })

  it('keeps unmatched placeholders so the gap is visible', () => {
    expect(translateWith('en', 'overview.metrics.liveLost', { live: 3 })).toContain('{lost}')
  })

  it('uses the second string argument as a fallback', () => {
    // The fallback is itself resolved as a key first (vue-i18n semantics), then
    // used verbatim — which is how an unmapped status still renders something.
    expect(translateWith('en', 'status.live', 'live')).toBe('Live')
    expect(translateWith('en', 'status.totally_absent', 'raw-value')).toBe('raw-value')
  })

  it('returns the key itself when nothing resolves', () => {
    expect(translateWith('en', 'does.not.exist')).toBe('does.not.exist')
  })

  it('translates the same key differently per locale', () => {
    expect(translateWith('zh-CN', 'login.signIn')).not.toBe(translateWith('en', 'login.signIn'))
  })
})

describe('keys used by the source', () => {
  /**
   * Every `t('a.b')` in the app must resolve. `translateWith` returns the key itself when
   * nothing matches, which renders as literal `common.close` in the UI — the parity test
   * above cannot see this, because a key nothing defines is missing from both locales.
   */
  function walk(dir: string): string[] {
    return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
      if (entry.name === 'node_modules') return []
      const path = join(dir, entry.name)
      if (entry.isDirectory()) return walk(path)
      if (!/\.tsx?$/.test(entry.name) || /\.test\.tsx?$/.test(entry.name)) return []
      return [path]
    })
  }

  const STATIC_KEY = /\bt\(\s*'([A-Za-z0-9_.]+)'/g

  function usedKeys(): string[] {
    const found = new Set<string>()
    // `process.cwd()` is the package root under vitest; `import.meta.url` is not a file URL.
    for (const file of walk(join(process.cwd(), 'src'))) {
      const source = readFileSync(file, 'utf8')
      for (const match of source.matchAll(STATIC_KEY)) found.add(match[1])
    }
    return Array.from(found).sort()
  }

  it('finds keys to check', () => {
    expect(usedKeys().length).toBeGreaterThan(200)
  })

  it('resolves every one of them in both locales', () => {
    const missing: string[] = []
    for (const key of usedKeys()) {
      if (translateWith('en', key) === key) missing.push(`en:${key}`)
      if (translateWith('zh-CN', key) === key) missing.push(`zh-CN:${key}`)
    }
    expect(missing).toEqual([])
  })
})

describe('locale message parity', () => {
  // en is the fallback locale and must be fully covered by zh-CN. The reverse is
  // not required: zh-CN carries extra keys for titles the backend only sends in
  // Chinese (observability panels, operations checks/alerts).
  it('covers every en key in zh-CN', () => {
    const enKeys = flattenKeys(en as Record<string, unknown>)
    const zhKeys = new Set(flattenKeys(zhCN as Record<string, unknown>))
    expect(enKeys.filter((key) => !zhKeys.has(key))).toEqual([])
  })

  it('has no empty strings', () => {
    for (const locale of ['en', 'zh-CN'] as const) {
      const source = locale === 'en' ? en : zhCN
      for (const key of flattenKeys(source as Record<string, unknown>)) {
        const value = key
          .split('.')
          .reduce<unknown>(
            (acc, part) => (acc as Record<string, unknown>)[part],
            source as Record<string, unknown>
          )
        expect(String(value).trim(), `${locale}:${key}`).not.toBe('')
      }
    }
  })
})

describe('detectLocale', () => {
  // Node 26 ships its own `localStorage` that needs --localstorage-file, and jsdom
  // may not expose one either; stub a real Storage so the detection logic is what
  // gets exercised.
  function stubStorage(initial: Record<string, string> = {}) {
    const store = new Map(Object.entries(initial))
    vi.stubGlobal('localStorage', {
      getItem: (key: string) => store.get(key) ?? null,
      setItem: (key: string, value: string) => void store.set(key, value),
      removeItem: (key: string) => void store.delete(key),
      clear: () => store.clear(),
      key: (index: number) => Array.from(store.keys())[index] ?? null,
      get length() {
        return store.size
      }
    })
  }

  it('prefers a stored supported locale', () => {
    stubStorage({ [LOCALE_STORAGE_KEY]: 'zh-CN' })
    expect(detectLocale()).toBe('zh-CN')
    vi.unstubAllGlobals()
  })

  it('ignores an unsupported stored value and falls back to the browser language', () => {
    stubStorage({ [LOCALE_STORAGE_KEY]: 'fr' })
    vi.spyOn(navigator, 'language', 'get').mockReturnValue('zh-Hans-CN')
    expect(detectLocale()).toBe('zh-CN')

    vi.spyOn(navigator, 'language', 'get').mockReturnValue('en-US')
    expect(detectLocale()).toBe('en')
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
  })
})
