import * as React from 'react'

import en from './messages/en'
import zhCN from './messages/zh-CN'

export const LOCALE_STORAGE_KEY = 'curvine-locale'
export const SUPPORTED_LOCALES = ['en', 'zh-CN'] as const
export type AppLocale = (typeof SUPPORTED_LOCALES)[number]

export const localeOptions: Array<{ value: AppLocale; label: string }> = [
  { value: 'en', label: 'English' },
  { value: 'zh-CN', label: '简体中文' }
]

export type TranslateParams = Record<string, string | number | null | undefined>
export type Translate = (key: string, fallbackOrParams?: string | TranslateParams) => string
/** Whether a key resolves to a message — mirrors vue-i18n's `te`. */
export type HasKey = (key: string) => boolean

type Messages = Record<string, unknown>

const messages: Record<AppLocale, Messages> = {
  en: en as Messages,
  'zh-CN': zhCN as Messages
}

function isSupportedLocale(value: string | null | undefined): value is AppLocale {
  return SUPPORTED_LOCALES.includes(value as AppLocale)
}

function hasBrowserStorage(): boolean {
  return typeof window !== 'undefined' && typeof localStorage !== 'undefined'
}

export function detectLocale(): AppLocale {
  if (hasBrowserStorage()) {
    try {
      const saved = localStorage.getItem(LOCALE_STORAGE_KEY)
      if (isSupportedLocale(saved)) {
        return saved
      }
    } catch {
      // Storage can be unavailable (private mode); fall through to detection.
    }
  }
  if (typeof navigator !== 'undefined' && navigator.language.toLowerCase().startsWith('zh')) {
    return 'zh-CN'
  }
  return 'en'
}

export function applyDocumentLocale(locale: AppLocale) {
  if (typeof document !== 'undefined') {
    document.documentElement.lang = locale === 'zh-CN' ? 'zh-CN' : 'en'
  }
}

function lookup(source: Messages, key: string): unknown {
  return key
    .split('.')
    .reduce<unknown>(
      (accumulator, part) =>
        accumulator && typeof accumulator === 'object'
          ? (accumulator as Messages)[part]
          : undefined,
      source
    )
}

function interpolate(template: string, params?: TranslateParams): string {
  if (!params) return template
  return template.replace(/\{(\w+)\}/g, (match, name: string) => {
    const value = params[name]
    return value === undefined || value === null ? match : String(value)
  })
}

/**
 * Look up `key` in `locale`.
 *
 * The second argument mirrors vue-i18n's `t(key, fallback)`: a string is a
 * fallback (itself looked up as a key first, then used verbatim), an object is
 * interpolation params. A missing key renders the fallback, else the key itself
 * so untranslated entries are visible instead of silently blank.
 */
export function translateWith(
  locale: AppLocale,
  key: string,
  fallbackOrParams?: string | TranslateParams
): string {
  const fallback = typeof fallbackOrParams === 'string' ? fallbackOrParams : undefined
  const params =
    typeof fallbackOrParams === 'object' && fallbackOrParams !== null
      ? fallbackOrParams
      : undefined

  const raw = lookup(messages[locale], key)
  if (typeof raw === 'string') return interpolate(raw, params)

  if (fallback !== undefined) {
    const fallbackRaw = lookup(messages[locale], fallback)
    if (typeof fallbackRaw === 'string') return interpolate(fallbackRaw, params)
    return fallback
  }

  return key
}

export function hasMessage(locale: AppLocale, key: string): boolean {
  return typeof lookup(messages[locale], key) === 'string'
}

interface I18nContextValue {
  locale: AppLocale
  setLocale(locale: AppLocale): void
  t: Translate
  te: HasKey
}

const I18nContext = React.createContext<I18nContextValue | null>(null)

export function I18nProvider({ children }: { children: React.ReactNode }) {
  const [locale, setLocaleState] = React.useState<AppLocale>(() => detectLocale())

  React.useEffect(() => {
    applyDocumentLocale(locale)
  }, [locale])

  const setLocale = React.useCallback((next: AppLocale) => {
    if (!isSupportedLocale(next)) return
    setLocaleState(next)
    if (hasBrowserStorage()) {
      try {
        localStorage.setItem(LOCALE_STORAGE_KEY, next)
      } catch {
        // Persisting the locale is best-effort.
      }
    }
  }, [])

  const t = React.useCallback<Translate>(
    (key, fallbackOrParams) => translateWith(locale, key, fallbackOrParams),
    [locale]
  )

  const te = React.useCallback<HasKey>((key) => hasMessage(locale, key), [locale])

  const value = React.useMemo<I18nContextValue>(
    () => ({ locale, setLocale, t, te }),
    [locale, setLocale, t, te]
  )

  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>
}

export function useI18n(): I18nContextValue {
  const context = React.useContext(I18nContext)
  if (!context) {
    throw new Error('useI18n must be used within I18nProvider')
  }
  return context
}

export function useT(): Translate {
  return useI18n().t
}

export function useTe(): HasKey {
  return useI18n().te
}
