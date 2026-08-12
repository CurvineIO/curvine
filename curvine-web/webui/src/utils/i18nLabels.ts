import type { Composer } from 'vue-i18n'

type Translate = Composer['t']

export function translateStatus(t: Translate, status: string): string {
  const key = `status.${String(status || 'unknown').toLowerCase()}`
  return t(key, String(status || ''))
}

export function translateWorkerAction(t: Translate, action: string): string {
  const key = `workers.actionVerb.${action}`
  return t(key, action)
}

export function translateRole(t: Translate, role: string): string {
  const normalized = String(role || '').toLowerCase()
  if (normalized === 'active' || normalized === 'standby') {
    return t(`overview.role.${normalized}`)
  }
  return t('common.unknown')
}

export function translateDelta(t: Translate, value: string): string {
  const key = `delta.${String(value || '').toLowerCase()}`
  if (key.includes('.')) {
    const translated = t(key)
    if (translated !== key) return translated
  }
  return value
}
