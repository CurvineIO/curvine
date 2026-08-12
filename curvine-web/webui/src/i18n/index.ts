/*
 * Copyright 2025 OPPO.
 *
 * Licensed under the Apache License, Version 2.0.
 */

import { createI18n } from 'vue-i18n'
import en from './locales/en'
import zhCN from './locales/zh-CN'

export const LOCALE_STORAGE_KEY = 'curvine-locale'
export const SUPPORTED_LOCALES = ['en', 'zh-CN'] as const
export type AppLocale = (typeof SUPPORTED_LOCALES)[number]

export const localeOptions: Array<{ value: AppLocale; label: string }> = [
  { value: 'en', label: 'English' },
  { value: 'zh-CN', label: '简体中文' }
]

function isSupportedLocale(value: string | null | undefined): value is AppLocale {
  return SUPPORTED_LOCALES.includes(value as AppLocale)
}

function hasBrowserStorage(): boolean {
  return typeof window !== 'undefined' && typeof localStorage !== 'undefined'
}

export function detectLocale(): AppLocale {
  if (hasBrowserStorage()) {
    const saved = localStorage.getItem(LOCALE_STORAGE_KEY)
    if (isSupportedLocale(saved)) {
      return saved
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

export const i18n = createI18n({
  legacy: false,
  locale: detectLocale(),
  fallbackLocale: 'en',
  messages: {
    en,
    'zh-CN': zhCN
  }
})

export function setLocale(locale: AppLocale) {
  if (!isSupportedLocale(locale)) {
    return
  }
  i18n.global.locale.value = locale
  if (hasBrowserStorage()) {
    localStorage.setItem(LOCALE_STORAGE_KEY, locale)
  }
  applyDocumentLocale(locale)
}
