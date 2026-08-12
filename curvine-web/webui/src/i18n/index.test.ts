import { describe, expect, it } from 'vitest'
import { createI18n } from 'vue-i18n'
import en from './locales/en'
import zhCN from './locales/zh-CN'
import {
  collectStaticTranslationKeys,
  flattenLocaleKeys,
  hasLocaleKey,
  isZhOnlyLocaleKey
} from './testUtils'
import { formatObservabilityRelative } from '@/utils/observabilityI18n'

describe('i18n locales', () => {
  it('keeps English and Chinese locale trees aligned except zh-only panel titles', () => {
    const enKeys = new Set(flattenLocaleKeys(en as Record<string, unknown>))
    const zhKeys = flattenLocaleKeys(zhCN as Record<string, unknown>)

    const missingInZh = Array.from(enKeys).filter((key) => !zhKeys.includes(key))
    expect(missingInZh, `missing zh-CN keys: ${missingInZh.join(', ')}`).toEqual([])

    const unexpectedZhOnly = zhKeys.filter((key) => !enKeys.has(key) && !isZhOnlyLocaleKey(key))
    expect(
      unexpectedZhOnly,
      `unexpected zh-CN-only keys: ${unexpectedZhOnly.join(', ')}`
    ).toEqual([])

    const zhPanelKeys = zhKeys.filter((key) => key.startsWith('observability.panels.'))
    expect(zhPanelKeys.length).toBeGreaterThan(0)
    expect(Array.from(enKeys).some((key) => key.startsWith('observability.panels.'))).toBe(false)
  })

  it('defines every statically referenced translation key in English messages', () => {
    const referencedKeys = collectStaticTranslationKeys()
    const missing = referencedKeys.filter((key) => !hasLocaleKey(en as Record<string, unknown>, key))
    expect(missing, `missing en keys: ${missing.join(', ')}`).toEqual([])
  })

  it('translates navigation labels', () => {
    const i18n = createI18n({
      legacy: false,
      locale: 'zh-CN',
      messages: { en, 'zh-CN': zhCN }
    })
    expect(i18n.global.t('nav.observability')).toBe('可观测性')
    expect(i18n.global.t('login.signIn')).toBe('登录')
  })

  it('renders Chinese observability labels, table headers, and relative time', () => {
    const i18n = createI18n({
      legacy: false,
      locale: 'zh-CN',
      messages: { en, 'zh-CN': zhCN }
    })
    const { t, te } = i18n.global

    expect(t('observability.collector.title')).toBe('采集器与数据源')
    expect(t('observability.chart.series')).toBe('序列')
    expect(t('observability.chart.current')).toBe('当前')
    expect(t('observability.toolbar.resetZoom')).toBe('重置缩放')
    expect(t('observability.panels.cluster-capacity')).toBe('集群容量')
    expect(te('observability.panels.cluster-capacity')).toBe(true)

    const recent = Date.now() - 45_000
    expect(formatObservabilityRelative(t, recent)).toBe('45 秒前')
    expect(formatObservabilityRelative(t, 0)).toBe('从未')
  })
})
