import type { HasKey, Translate } from '@/i18n'

function pick(t: Translate, te: HasKey, key: string, fallback: string): string {
  return te(key) ? t(key) : fallback
}

export function translatePanelTitle(
  t: Translate,
  te: HasKey,
  panelId: string,
  fallback: string
): string {
  return pick(t, te, `observability.panels.${panelId}`, fallback)
}

export function translateGroupTitle(
  t: Translate,
  te: HasKey,
  groupId: string,
  fallback: string
): string {
  return pick(t, te, `observability.groups.${groupId}`, fallback)
}

export function translateProviderHealth(t: Translate, status: string): string {
  const normalized = String(status || '').toLowerCase()
  const key = `observability.providerHealth.${normalized}`
  return t(key, status)
}

export function formatObservabilityRelative(
  t: Translate,
  value: number | string | null | undefined
): string {
  if (!value) return t('common.time.never')
  const delta = Date.now() - Number(value)
  if (!Number.isFinite(delta) || delta < 0) return t('common.unknown')
  if (delta < 60_000) {
    return t('common.time.secondsAgo', { count: Math.max(1, Math.round(delta / 1000)) })
  }
  if (delta < 3_600_000) {
    return t('common.time.minutesAgo', { count: Math.round(delta / 60_000) })
  }
  if (delta < 86_400_000) {
    return t('common.time.hoursAgo', { count: Math.round(delta / 3_600_000) })
  }
  return t('common.time.daysAgo', { count: Math.round(delta / 86_400_000) })
}
