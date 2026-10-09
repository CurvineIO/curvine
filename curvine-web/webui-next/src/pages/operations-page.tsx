/**
 * OperationsPage — React 18 + TypeScript port of
 * webui/src/views/Operations.vue.
 *
 * Behaviour-preserving: the same computed derivations (coverage percentage,
 * alert counts, overall status, check ordering, missing-target rows), the same
 * summary banner / coverage ring / diagnostic-check list / alert list /
 * missing-targets table, the same empty / loading / error branches and the exact
 * same i18n keys, resolved through the `@/lib/operationsI18n` helpers so the
 * zh-only `operations.checks.items.*` / `operations.alerts.items.*` entries
 * still win over the English payload strings.
 *
 * The Vue component imported `fetchObservabilityOperations` from the API module
 * singleton and re-fetched on the mitt `admin-refresh` event; here the client is
 * injected via `useApi()` and the global refresh signal replaces the event bus.
 * The Vue `operationsLoader` prop (used by tests and `scenarios.ts` to inject a
 * payload) is reproduced as an optional React prop.
 */
import { useCallback } from 'react'

import { RefreshCwIcon } from 'lucide-react'

import { useApi } from '@/api/api-context'
import type {
  DiagnosticCheck,
  DiagnosticStatus,
  MissingTarget,
  OperationsAlert,
  OperationsPayload
} from '@/api/types'
import { EmptyNotice, ErrorAlert } from '@/components/data-state'
import { PageHeader } from '@/components/page-header'
import { MonoText, StatusBadge, type StatusTone } from '@/components/resource-table'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/components/ui/table'
import { useT, useTe } from '@/i18n'
import {
  translateAlertField,
  translateCheckField,
  translateCheckStatus,
  translateOperationsSource,
  translateSeverity,
  translateSuggestedFix,
  translateTargetAlertTitle,
  translateTargetReason
} from '@/lib/operationsI18n'
import { cn } from '@/lib/utils'
import { useAsync } from '@/lib/use-async'
import { useRefreshSignal } from '@/refresh/refresh-context'

/** Vue `const STATUS_ORDER = { fail: 0, warning: 1, pass: 2, skipped: 3 }`. */
const STATUS_ORDER: Record<DiagnosticStatus, number> = {
  fail: 0,
  warning: 1,
  pass: 2,
  skipped: 3
}

/** Vue `checkIcon(status)` glyphs. */
const CHECK_GLYPH: Record<DiagnosticStatus, string> = {
  pass: '✓',
  fail: '✕',
  warning: '!',
  skipped: '−'
}

const SUMMARY_ACCENT: Record<DiagnosticStatus, string> = {
  pass: 'border-l-primary',
  warning: 'border-l-warning',
  fail: 'border-l-destructive',
  skipped: 'border-l-border'
}

const HEALTH_BADGE: Record<DiagnosticStatus, string> = {
  pass: 'border-primary/35 bg-primary/10 text-primary',
  warning: 'border-warning/35 bg-warning/10 text-warning',
  fail: 'border-destructive/35 bg-destructive/10 text-destructive',
  skipped: 'border-border bg-card text-foreground'
}

const CHECK_ITEM: Record<DiagnosticStatus, string> = {
  pass: 'border-border bg-muted/40',
  warning: 'border-warning/30 bg-warning/10',
  fail: 'border-destructive/25 bg-destructive/10',
  skipped: 'border-border bg-muted/40'
}

const CHECK_MARK: Record<DiagnosticStatus, string> = {
  pass: 'bg-success/10 text-success',
  warning: 'bg-warning/10 text-warning',
  fail: 'bg-destructive/10 text-destructive',
  skipped: 'bg-muted text-muted-foreground'
}

const CHECK_TONE: Record<DiagnosticStatus, StatusTone> = {
  pass: 'positive',
  warning: 'warning',
  fail: 'negative',
  skipped: 'neutral'
}

const ALERT_ITEM: Record<string, string> = {
  critical: 'border-destructive/25 border-l-destructive bg-destructive/10',
  warning: 'border-warning/30 border-l-warning bg-warning/10'
}

const PANEL_BADGE_OK = 'border-success/25 bg-success/10 text-success'
const PANEL_BADGE_WARN = 'border-warning/30 bg-warning/10 text-warning'

export interface OperationsPageProps {
  /** Vue `operationsLoader` prop — injects the payload in tests / scenarios. */
  operationsLoader?: () => Promise<OperationsPayload>
}

function ListSkeleton() {
  return (
    <div className="flex flex-col gap-2.5" data-testid="operations-loading">
      <Skeleton className="h-20 w-full" />
      <Skeleton className="h-20 w-full" />
    </div>
  )
}

export function OperationsPage({ operationsLoader }: OperationsPageProps = {}) {
  const t = useT()
  const te = useTe()
  const api = useApi()

  const load = useCallback(
    () => (operationsLoader ? operationsLoader() : api.fetchObservabilityOperations()),
    [api, operationsLoader]
  )

  const { data, loading, error, reload, refresh } = useAsync(load, [load])
  useRefreshSignal(refresh)

  /* ------------------------------------------------------------- derived */

  const payload = data?.diagnostics ?? null

  const rawChecks = payload?.checks
  const checks: DiagnosticCheck[] = Array.isArray(rawChecks) ? rawChecks : []

  const orderedChecks: DiagnosticCheck[] = [...checks].sort(
    (a, b) => (STATUS_ORDER[a.status] ?? 9) - (STATUS_ORDER[b.status] ?? 9)
  )

  const rawMissingTargets = payload?.missing_targets
  const missingTargets: MissingTarget[] = Array.isArray(rawMissingTargets)
    ? rawMissingTargets
    : []

  const rawAlerts = data?.alerts
  const alerts: OperationsAlert[] = Array.isArray(rawAlerts) ? rawAlerts : []

  const overallStatus: DiagnosticStatus =
    payload?.overall_status || (alerts.length ? 'warning' : 'skipped')
  const activeProvider = payload?.active_provider || 'provider pending'
  const expectedTargets = Number(payload?.expected_targets || 0)
  const coveredTargets = Number(payload?.active_covered_targets || 0)
  const masterCount = Number(payload?.discovered_master_targets || 0)
  const workerCount = Number(payload?.discovered_live_worker_targets || 0)
  const transferCount = Number(payload?.discovered_transfer_targets || 0)

  const criticalCount = alerts.filter((item) => item.severity === 'critical').length
  const warningCount = alerts.filter((item) => item.severity === 'warning').length
  const passedCheckCount = checks.filter((item) => item.status === 'pass').length
  const actionableCheckCount = checks.filter((item) => isActionable(item.status)).length
  const generatedMs = Number(payload?.generated_ms || 0)

  const coveragePercent = expectedTargets
    ? Math.round((coveredTargets / expectedTargets) * 100)
    : 0
  const coverageLabel = expectedTargets ? `${coveragePercent}%` : '—'
  const ringPercent = expectedTargets ? coveragePercent : 0
  const ringColor =
    ringPercent >= 100
      ? 'var(--color-primary)'
      : ringPercent > 0
        ? 'var(--color-warning)'
        : 'var(--color-muted)'

  const statusChipLabel = (() => {
    if (overallStatus === 'pass') return t('operations.status.healthy')
    if (overallStatus === 'fail') return t('operations.status.critical')
    if (overallStatus === 'warning') return t('operations.status.degraded')
    return t('operations.status.pending')
  })()

  const statusHeadline = (() => {
    if (overallStatus === 'fail') return t('operations.status.headlineFail')
    if (overallStatus === 'warning') return t('operations.status.headlineWarning')
    if (overallStatus === 'pass') return t('operations.status.headlinePass')
    return t('operations.status.headlinePending')
  })()

  const statusDescription = (() => {
    if (overallStatus === 'fail') {
      return t('operations.status.descFail', {
        critical: criticalCount,
        actionable: actionableCheckCount
      })
    }
    if (overallStatus === 'warning') return t('operations.status.descWarning')
    if (overallStatus === 'pass') return t('operations.status.descPass', { count: checks.length })
    return t('operations.status.descPending')
  })()

  /* ------------------------------------------------------------- helpers */

  function isActionable(status: DiagnosticStatus): boolean {
    return status === 'fail' || status === 'warning'
  }

  function checkSummaryParams(item: { id?: string; summary?: string }) {
    if (item.id === 'builtin-coverage' || item.id === 'prometheus-coverage') {
      return { covered: coveredTargets, expected: expectedTargets }
    }
    const match = String(item.summary || '').match(/(\d+)/)
    return { count: match ? Number(match[1]) : 0 }
  }

  function checkTitle(item: DiagnosticCheck): string {
    return translateCheckField(t, te, item, 'title', item.title)
  }

  function checkSummary(item: DiagnosticCheck): string {
    return translateCheckField(t, te, item, 'summary', item.summary, checkSummaryParams(item))
  }

  function checkDetail(item: DiagnosticCheck): string {
    return translateCheckField(
      t,
      te,
      item,
      'detail',
      item.detail || '',
      checkSummaryParams(item)
    )
  }

  function checkSuggestedFix(item: DiagnosticCheck): string {
    const translated = translateCheckField(
      t,
      te,
      item,
      'suggested_fix',
      item.suggested_fix || '',
      checkSummaryParams(item)
    )
    return translateSuggestedFix(t, te, translated)
  }

  function translateCheckStatusLabel(status: DiagnosticStatus): string {
    return translateCheckStatus(t, status)
  }

  function translateSeverityLabel(severity: OperationsAlert['severity']): string {
    return translateSeverity(t, severity)
  }

  function alertTitle(alert: OperationsAlert): string {
    if (alert.target_id) {
      const component = String(alert.title || '').split(' ')[0]
      return translateTargetAlertTitle(t, te, component, alert.title)
    }
    return translateAlertField(t, te, alert, 'title', alert.title)
  }

  function alertSummary(alert: OperationsAlert): string {
    return translateAlertField(
      t,
      te,
      alert,
      'summary',
      alert.summary,
      checkSummaryParams({ id: alert.id.replace('diagnostic:', ''), summary: alert.summary })
    )
  }

  function alertSuggestedFix(alert: OperationsAlert): string {
    return translateAlertField(
      t,
      te,
      alert,
      'suggested_fix',
      alert.suggested_fix || t('operations.alerts.reviewChecks')
    )
  }

  function targetReason(reason: string): string {
    return translateTargetReason(t, te, reason)
  }

  function formatSource(value: string): string {
    return translateOperationsSource(t, value)
  }

  function formatProvider(value: string): string {
    const label = String(value || '').trim()
    if (!label || label === 'provider pending') return t('operations.summary.providerPending')
    return label.charAt(0).toUpperCase() + label.slice(1)
  }

  function formatTime(value: number): string {
    if (!value) return t('operations.time.notAvailable')
    const date = new Date(Number(value))
    return Number.isNaN(date.getTime())
      ? t('operations.time.notAvailable')
      : date.toLocaleString()
  }

  const checksLoading = loading && orderedChecks.length === 0
  const alertsLoading = loading && alerts.length === 0

  return (
    <>
      <PageHeader
        eyebrow={t('nav.groupObservability')}
        title={t('nav.operations')}
        description={t('nav.operationsSubtitle')}
        actions={
          <Button
            variant="outline"
            size="sm"
            disabled={loading}
            onClick={() => void reload()}
            data-testid="operations-refresh"
          >
            <RefreshCwIcon data-icon="inline-start" />
            {t('common.refresh')}
          </Button>
        }
      />

      {error && <ErrorAlert message={error} className="border-destructive/25" />}

      {/* -------------------------------------------------- summary banner */}
      <Card
        className={cn('gap-0 overflow-hidden border-l-4 py-0', SUMMARY_ACCENT[overallStatus])}
        data-testid="operations-summary"
      >
        <div className="flex flex-col gap-4 p-4 md:flex-row md:items-center md:justify-between">
          <div className="flex min-w-0 items-center gap-4">
            <div
              className="grid aspect-square w-22 shrink-0 place-items-center rounded-full"
              style={{
                background: `conic-gradient(${ringColor} 0 ${ringPercent}%, var(--color-muted) ${ringPercent}% 100%)`
              }}
              aria-hidden="true"
            >
              <div className="grid size-[calc(100%-1.25rem)] place-items-center rounded-full bg-card text-center">
                <div>
                  <strong className="block font-heading text-lg leading-tight font-semibold tabular-nums">
                    {coverageLabel}
                  </strong>
                  <span className="mt-0.5 block text-[10px] font-medium tracking-wide text-muted-foreground uppercase">
                    {t('operations.summary.coverage')}
                  </span>
                </div>
              </div>
            </div>

            <div className="min-w-0">
              <div className="mb-1.5 flex flex-wrap items-center gap-1.5 text-xs text-muted-foreground">
                <span className="font-semibold text-foreground">{formatProvider(activeProvider)}</span>
                <span aria-hidden>·</span>
                <time className="tabular-nums">{formatTime(generatedMs)}</time>
              </div>
              <h2 className="font-heading text-lg leading-tight font-semibold tracking-tight">
                {statusHeadline}
              </h2>
              <p className="mt-1.5 max-w-[58ch] text-sm text-muted-foreground">
                {statusDescription}
              </p>
            </div>
          </div>

          <div
            className={cn(
              'flex shrink-0 flex-col items-center gap-0.5 rounded-xl border px-4 py-3 text-center',
              HEALTH_BADGE[overallStatus]
            )}
            data-testid="operations-health-badge"
          >
            <span className="text-[11px] font-semibold tracking-wider text-muted-foreground uppercase">
              {t('operations.summary.status')}
            </span>
            <strong className="text-lg leading-tight font-semibold">{statusChipLabel}</strong>
          </div>
        </div>

        <div className="grid grid-cols-1 gap-px border-t border-border bg-border sm:grid-cols-2 xl:grid-cols-4">
          <div className="flex flex-col gap-0.5 bg-card p-4">
            <span className="text-xs font-medium text-muted-foreground">
              {t('operations.summary.checksPassed')}
            </span>
            <strong className="font-heading text-xl leading-none font-semibold tabular-nums">
              {passedCheckCount}/{checks.length}
            </strong>
            <small className="text-xs text-muted-foreground">
              {t('operations.summary.needAttention', { count: actionableCheckCount })}
            </small>
          </div>

          <div className="flex flex-col gap-0.5 bg-card p-4">
            <span className="text-xs font-medium text-muted-foreground">
              {t('operations.summary.targetsCovered')}
            </span>
            <strong className="font-heading text-xl leading-none font-semibold tabular-nums">
              {coveredTargets}/{expectedTargets}
            </strong>
            <small className="text-xs text-muted-foreground">
              {transferCount
                ? t('operations.summary.masterWorkerTransfer', {
                    master: masterCount,
                    worker: workerCount,
                    transfer: transferCount
                  })
                : t('operations.summary.masterWorker', {
                    master: masterCount,
                    worker: workerCount
                  })}
            </small>
          </div>

          <div className="flex flex-col gap-0.5 bg-card p-4">
            <span className="text-xs font-medium text-muted-foreground">
              {t('operations.summary.activeAlerts')}
            </span>
            <strong
              className={cn(
                'font-heading text-xl leading-none font-semibold tabular-nums',
                alerts.length > 0 && 'text-warning'
              )}
            >
              {error ? t('common.notAvailable') : alerts.length}
            </strong>
            <small className="text-xs text-muted-foreground">
              {t('operations.summary.criticalWarning', {
                critical: criticalCount,
                warning: warningCount
              })}
            </small>
          </div>

          <div className="flex flex-col gap-0.5 bg-card p-4">
            <span className="text-xs font-medium text-muted-foreground">
              {t('operations.summary.providerMode')}
            </span>
            <strong className="font-heading text-base leading-tight font-semibold capitalize">
              {formatProvider(activeProvider)}
            </strong>
            <small className="text-xs text-muted-foreground">
              {t('operations.summary.discoveredTargets', { count: expectedTargets || 0 })}
            </small>
          </div>
        </div>
      </Card>

      {/* ------------------------------------------------ checks + alerts */}
      <section className="grid gap-4 xl:grid-cols-2">
        <Card className="gap-0 py-0" data-testid="operations-checks">
          <div className="flex flex-wrap items-start justify-between gap-3 border-b border-border p-3.5">
            <div className="flex flex-col gap-0.5">
              <h2 className="font-heading text-base font-medium">
                {t('operations.checks.title')}
              </h2>
              <p className="text-xs text-muted-foreground">
                {t('operations.checks.subtitle')}
              </p>
            </div>
            <Badge
              variant="outline"
              className={actionableCheckCount ? PANEL_BADGE_WARN : PANEL_BADGE_OK}
            >
              {t('operations.checks.passed', {
                passed: passedCheckCount,
                total: checks.length
              })}
            </Badge>
          </div>

          <div className="flex-1 p-3.5">
            {checksLoading ? (
              <ListSkeleton />
            ) : !error && orderedChecks.length === 0 ? (
              <EmptyNotice title={t('operations.checks.empty')} className="min-h-45" />
            ) : (
              <ul className="flex flex-col gap-2.5">
                {orderedChecks.map((item) => (
                  <li
                    key={item.id}
                    className={cn(
                      'grid grid-cols-[auto_minmax(0,1fr)] gap-3 rounded-lg border p-3.5',
                      CHECK_ITEM[item.status]
                    )}
                    data-testid={`operations-check-${item.id}`}
                  >
                    <span
                      aria-hidden
                      className={cn(
                        'grid size-8 place-items-center rounded-lg text-sm font-bold',
                        CHECK_MARK[item.status]
                      )}
                    >
                      {CHECK_GLYPH[item.status]}
                    </span>
                    <div className="min-w-0">
                      <div className="flex flex-wrap items-start justify-between gap-2.5">
                        <h3 className="font-heading text-sm leading-snug font-semibold">
                          {checkTitle(item)}
                        </h3>
                        <StatusBadge
                          value={translateCheckStatusLabel(item.status)}
                          tone={CHECK_TONE[item.status]}
                        />
                      </div>
                      <p className="mt-1.5 text-sm leading-relaxed text-muted-foreground">
                        {checkSummary(item)}
                      </p>
                      {item.detail && (
                        <small className="mt-1 block text-xs leading-relaxed break-words text-muted-foreground">
                          {checkDetail(item)}
                        </small>
                      )}
                      {item.suggested_fix && isActionable(item.status) && (
                        <div className="mt-2.5 rounded-lg border border-border bg-card p-2.5">
                          <strong className="block text-xs font-semibold text-foreground">
                            {t('operations.checks.howToFix')}
                          </strong>
                          <span className="mt-1 block text-xs leading-relaxed text-muted-foreground">
                            {checkSuggestedFix(item)}
                          </span>
                        </div>
                      )}
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </Card>

        <Card className="gap-0 py-0" data-testid="operations-alerts">
          <div className="flex flex-wrap items-start justify-between gap-3 border-b border-border p-3.5">
            <div className="flex flex-col gap-0.5">
              <h2 className="font-heading text-base font-medium">
                {t('operations.alerts.title')}
              </h2>
              <p className="text-xs text-muted-foreground">
                {t('operations.alerts.subtitle')}
              </p>
            </div>
            <Badge
              variant="outline"
              className={alerts.length ? PANEL_BADGE_WARN : PANEL_BADGE_OK}
            >
              {t('operations.alerts.active', { count: alerts.length })}
            </Badge>
          </div>

          <div className="flex-1 p-3.5">
            {alertsLoading ? (
              <ListSkeleton />
            ) : !error && alerts.length === 0 ? (
              <div className="flex min-h-45 items-center gap-3.5 rounded-lg border border-dashed border-border bg-muted/40 p-4 text-left">
                <span
                  aria-hidden
                  className="grid size-10 shrink-0 place-items-center rounded-xl bg-success/10 text-lg font-bold text-success"
                >
                  ✓
                </span>
                <div>
                  <strong className="block text-sm text-foreground">
                    {t('operations.alerts.allClear')}
                  </strong>
                  <p className="mt-1 text-sm leading-relaxed text-muted-foreground">
                    {t('operations.alerts.allClearDesc')}
                  </p>
                </div>
              </div>
            ) : (
              <ul className="flex flex-col gap-2.5">
                {alerts.map((alert) => (
                  <li
                    key={alert.id}
                    className={cn(
                      'rounded-lg border border-l-4 p-3.5',
                      ALERT_ITEM[alert.severity] ?? ALERT_ITEM.warning
                    )}
                    data-testid={`operations-alert-${alert.id}`}
                  >
                    <div className="mb-2 flex flex-wrap items-center justify-between gap-2.5">
                      <StatusBadge
                        value={translateSeverityLabel(alert.severity)}
                        tone={alert.severity === 'critical' ? 'negative' : 'warning'}
                      />
                      <span className="text-xs text-muted-foreground capitalize">
                        {formatSource(alert.source)}
                      </span>
                    </div>
                    <h3 className="font-heading text-base leading-snug">{alertTitle(alert)}</h3>
                    <p className="mt-1.5 text-sm leading-relaxed text-muted-foreground">
                      {alertSummary(alert)}
                    </p>
                    <div className="mt-2.5 rounded-lg border border-border bg-card p-2.5">
                      <strong className="block text-xs font-semibold text-foreground">
                        {t('operations.alerts.recommendedAction')}
                      </strong>
                      <span className="mt-1 block text-xs leading-relaxed text-muted-foreground">
                        {alertSuggestedFix(alert)}
                      </span>
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </Card>
      </section>

      {/* -------------------------------------------- missing target table */}
      {missingTargets.length > 0 && (
        <Card className="gap-0 py-0" data-testid="operations-missing-targets">
          <div className="flex flex-wrap items-start justify-between gap-3 border-b border-border p-3.5">
            <div className="flex flex-col gap-0.5">
              <h2 className="font-heading text-base font-medium">
                {t('operations.targets.title')}
              </h2>
              <p className="text-xs text-muted-foreground">
                {t('operations.targets.subtitle')}
              </p>
            </div>
            <StatusBadge
              value={t('operations.targets.missing', { count: missingTargets.length })}
              tone="negative"
            />
          </div>

          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>{t('operations.targets.source')}</TableHead>
                <TableHead>{t('operations.targets.component')}</TableHead>
                <TableHead>{t('operations.targets.instance')}</TableHead>
                <TableHead>{t('operations.targets.endpoint')}</TableHead>
                <TableHead>{t('operations.targets.reason')}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {missingTargets.map((target) => (
                <TableRow
                  key={`${target.source}:${target.id}`}
                  data-testid={`operations-target-${target.source}-${target.id}`}
                >
                  <TableCell>
                    <StatusBadge value={formatSource(target.source)} tone="neutral" />
                  </TableCell>
                  <TableCell className="font-medium">{target.component}</TableCell>
                  <TableCell className="text-muted-foreground">{target.instance}</TableCell>
                  <TableCell>
                    <MonoText>{target.url}</MonoText>
                  </TableCell>
                  <TableCell className="text-destructive break-words">
                    {targetReason(target.reason)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Card>
      )}
    </>
  )
}
