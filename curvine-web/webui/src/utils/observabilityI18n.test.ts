import { describe, expect, it } from 'vitest'
import { createI18n } from 'vue-i18n'
import en from '@/i18n/locales/en'
import zhCN from '@/i18n/locales/zh-CN'
import {
  formatObservabilityRelative,
  translateGroupTitle,
  translatePanelTitle
} from './observabilityI18n'

function createTestI18n(locale: 'en' | 'zh-CN') {
  return createI18n({
    legacy: false,
    locale,
    fallbackLocale: 'en',
    messages: { en, 'zh-CN': zhCN }
  }).global
}

describe('observabilityI18n', () => {
  it('uses zh-CN panel titles when defined and falls back to backend titles in English', () => {
    const zh = createTestI18n('zh-CN')
    const english = createTestI18n('en')

    expect(
      translatePanelTitle(zh.t, zh.te, 'cluster-capacity', 'Cluster Capacity')
    ).toBe('集群容量')
    expect(
      translatePanelTitle(english.t, english.te, 'cluster-capacity', 'Cluster Capacity')
    ).toBe('Cluster Capacity')
    expect(english.te('observability.panels.cluster-capacity')).toBe(false)
  })

  it('falls back to the backend title for unknown panel ids', () => {
    const zh = createTestI18n('zh-CN')
    expect(
      translatePanelTitle(zh.t, zh.te, 'unknown-panel', 'Custom Backend Title')
    ).toBe('Custom Backend Title')
  })

  it('translates group titles and relative timestamps', () => {
    const zh = createTestI18n('zh-CN')
    expect(translateGroupTitle(zh.t, zh.te, 'workers', 'Workers')).toBe('Worker')

    const recent = Date.now() - 45_000
    expect(formatObservabilityRelative(zh.t, recent)).toBe('45 秒前')
    expect(formatObservabilityRelative(zh.t, 0)).toBe('从未')
  })
})
