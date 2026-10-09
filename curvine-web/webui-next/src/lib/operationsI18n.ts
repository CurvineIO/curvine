import type { HasKey, Translate, TranslateParams } from '@/i18n'

const SUGGESTED_FIX_KEYS: Record<string, string> = {
  'Verify the target endpoint and metrics web port, then inspect Observability / Data Sources.':
    'operations.alerts.suggestedFix.builtin',
  'Verify the target endpoint, component label and Prometheus monitor selector.':
    'operations.alerts.suggestedFix.prometheus',
  'Verify master RPC connectivity and worker registration, then refresh operations.':
    'operations.alerts.suggestedFix.discovery',
  'Verify the Prometheus URL, network policy, monitor selectors, and /api/v1/targets access.':
    'operations.alerts.suggestedFix.prometheusUrl'
}

function pick(t: Translate, te: HasKey, key: string, fallback: string): string {
  return te(key) ? t(key) : fallback
}

export function translateCheckStatus(t: Translate, status: string): string {
  return t(`operations.checkStatus.${status}`, status)
}

export function translateSeverity(t: Translate, severity: string): string {
  return t(`operations.severity.${severity}`, severity)
}

export function translateOperationsSource(t: Translate, value: string): string {
  const normalized = String(value || 'diagnostics')
  return t(`operations.sources.${normalized}`, normalized.replace(/-/g, ' '))
}

export function translateCheckField(
  t: Translate,
  te: HasKey,
  check: { id?: string; status?: string },
  field: 'title' | 'summary' | 'detail' | 'suggested_fix',
  fallback: string,
  params: TranslateParams = {}
): string {
  const id = String(check?.id || '')
  const status = String(check?.status || 'pass')
  const statusKey = `operations.checks.items.${id}.${field}.${status}`
  const genericKey = `operations.checks.items.${id}.${field}`
  if (te(statusKey)) return t(statusKey, params)
  if (te(genericKey)) return t(genericKey, params)
  return fallback
}

export function translateAlertField(
  t: Translate,
  te: HasKey,
  alert: { id?: string },
  field: 'title' | 'summary' | 'suggested_fix',
  fallback: string,
  params: TranslateParams = {}
): string {
  const rawId = String(alert?.id || '')
  const diagnosticId = rawId.startsWith('diagnostic:') ? rawId.slice('diagnostic:'.length) : rawId
  const keys = [
    `operations.alerts.items.${rawId}.${field}`,
    `operations.alerts.items.${diagnosticId}.${field}`,
    `operations.checks.items.${diagnosticId}.${field}`
  ]
  for (const key of keys) {
    if (te(key)) return t(key, params)
  }
  if (field === 'summary') {
    return translateAlertSummary(t, te, fallback)
  }
  if (field === 'suggested_fix') {
    return translateSuggestedFix(t, te, fallback)
  }
  return fallback
}

export function translateSuggestedFix(t: Translate, te: HasKey, fallback: string): string {
  const mapped = SUGGESTED_FIX_KEYS[fallback]
  if (mapped && te(mapped)) return t(mapped)
  return fallback
}

export function translateAlertSummary(t: Translate, te: HasKey, fallback: string): string {
  const match = String(fallback || '').match(/^([^:]+):\s*(.+)$/)
  if (!match) return fallback
  return `${match[1]}: ${translateTargetReason(t, te, match[2])}`
}

export function translateTargetReason(t: Translate, te: HasKey, reason: string): string {
  const raw = String(reason || '').trim()
  if (!raw) return reason
  const directKey = `operations.targets.reasons.${raw}`
  if (te(directKey)) return t(directKey)
  const normalized = raw.toLowerCase()
  const normalizedKey = `operations.targets.reasons.${normalized}`
  if (te(normalizedKey)) return t(normalizedKey)
  const noHealthyMatch = normalized.match(/^(\w+) has no healthy matching scrape target$/)
  if (noHealthyMatch) {
    const providerKey = 'operations.targets.reasons.noHealthyScrape'
    if (te(providerKey)) return t(providerKey, { provider: noHealthyMatch[1] })
  }
  return reason
}

export function translateTargetAlertTitle(
  t: Translate,
  te: HasKey,
  component: string,
  fallback: string
): string {
  return pick(t, te, `operations.alerts.targetTitle.${component}`, fallback)
}
