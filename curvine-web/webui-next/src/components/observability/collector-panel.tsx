/**
 * CollectorPanel — React 18 + TypeScript port of
 * webui/src/components/observability/CollectorPanel.vue.
 *
 * Behaviour-preserving: identical props (health / healthError / settings /
 * targets / targetsLoading / targetsError / metricNames / namesError), the same
 * `settings-updated` emit exposed as `onSettingsUpdated`, the same collapse
 * state machine, the same derived `dataSourceRows` / `problemTargets` /
 * `targetsUpLabel` / `settingsDirty`, the same helper formatting and the exact
 * same i18n keys.
 *
 * The Vue component saved settings by importing `updateObservabilitySettings`
 * from the API module singleton; the React app injects the client through
 * context, so it is resolved with `useApi()` here.
 */
import { useEffect, useState } from 'react'

import { ChevronDownIcon, ChevronRightIcon } from 'lucide-react'

import { useApi } from '@/api/api-context'
import type { ObservabilityHealth, ObservabilitySettings, TargetHealth } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/components/ui/select'
import { useT } from '@/i18n'
import { translateStatus } from '@/lib/i18nLabels'
import { formatObservabilityRelative } from '@/lib/observabilityI18n'
import { cn } from '@/lib/utils'

/** Flattened scrape target, mirroring the Vue `dataSourceRows` computed. */
interface DataSourceRow {
  id: string
  component: string
  instance: string
  scrape_up: boolean
  worker_id?: number | null
  worker_state?: string | null
  detail: string
}

export interface CollectorPanelProps {
  health?: ObservabilityHealth | null
  healthError?: string
  settings?: ObservabilitySettings | null
  targets?: TargetHealth[]
  targetsLoading?: boolean
  targetsError?: string
  metricNames?: string[]
  namesError?: string
  /** Vue `emit('settings-updated', updated)`. */
  onSettingsUpdated?: (settings: ObservabilitySettings) => void
}

function formatSampleCount(value: number | null | undefined): string {
  const n = Number(value || 0)
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1000) return `${(n / 1000).toFixed(1)}k`
  return String(n)
}

function formatDuration(ms: number | null | undefined): string {
  const value = Number(ms || 0)
  if (!value) return '-'
  if (value < 60_000) return `${Math.round(value / 1000)}s`
  if (value < 3_600_000) return `${Math.round(value / 60_000)}m`
  if (value < 86_400_000) return `${Math.round(value / 3_600_000)}h`
  return `${Math.round(value / 86_400_000)}d`
}

function SectionChevron({ open }: { open: boolean }) {
  return open ? (
    <ChevronDownIcon aria-hidden className="size-4 shrink-0 text-muted-foreground" />
  ) : (
    <ChevronRightIcon aria-hidden className="size-4 shrink-0 text-muted-foreground" />
  )
}

const SCRAPE_PILL_CLASS = {
  up: 'border-success/25 bg-success/10 text-success',
  down: 'border-destructive/20 bg-destructive/10 text-destructive'
} as const

export function CollectorPanel({
  health = null,
  healthError = '',
  settings = null,
  targets = [],
  targetsLoading = false,
  targetsError = '',
  metricNames = [],
  namesError = '',
  onSettingsUpdated
}: CollectorPanelProps) {
  const t = useT()
  const api = useApi()

  const [expanded, setExpanded] = useState(false)
  const [settingsOpen, setSettingsOpen] = useState(true)
  const [issuesOpen, setIssuesOpen] = useState(true)
  const [metricNamesOpen, setMetricNamesOpen] = useState(false)
  const [draftScrapeIntervalMs, setDraftScrapeIntervalMs] = useState(60_000)
  const [draftRetentionMs, setDraftRetentionMs] = useState(24 * 60 * 60 * 1000)
  const [settingsSaving, setSettingsSaving] = useState(false)
  const [settingsError, setSettingsError] = useState('')
  const [settingsNotice, setSettingsNotice] = useState('')

  // Vue `watch: { settings: { immediate: true, handler } }` — seed/reseed drafts.
  useEffect(() => {
    if (!settings) return
    setDraftScrapeIntervalMs(settings.scrape_interval_ms)
    setDraftRetentionMs(settings.retention_ms)
  }, [settings])

  const dataSourceRows: DataSourceRow[] = targets.map((target) => ({
    id: target.id,
    component: target.component,
    instance: target.instance,
    scrape_up: target.up,
    worker_id: target.worker_id,
    worker_state: target.worker_state,
    detail: [target.last_warning, target.last_error].filter(Boolean).join('; ')
  }))

  const targetsUpLabel = dataSourceRows.length
    ? t('observability.collector.targetsUp', {
        up: dataSourceRows.filter((row) => row.scrape_up).length,
        total: dataSourceRows.length
      })
    : t('observability.collector.collectorLabel')

  const problemTargets = dataSourceRows.filter((row) => !row.scrape_up || !!row.detail)

  const activeProvider = (health?.active_provider || '').toLowerCase()
  const showBuiltinSettings = activeProvider === 'builtin' || activeProvider === ''

  const settingsDirty = settings
    ? draftScrapeIntervalMs !== settings.scrape_interval_ms ||
      draftRetentionMs !== settings.retention_ms
    : false

  function shortInstance(row: DataSourceRow): string {
    const instance = row.instance || ''
    const host = instance.split(':')[0] || instance
    if (row.component === 'worker') {
      const workerNum = row.id?.startsWith('worker:') ? row.id.slice('worker:'.length) : null
      return workerNum ? `worker-${workerNum}` : host
    }
    return host
  }

  function formatRelative(value: number | string | null | undefined): string {
    return formatObservabilityRelative(t, value)
  }

  function workerStateLabel(state: string | null | undefined): string {
    return translateStatus(t, state ?? '')
  }

  async function applySettings() {
    if (!settingsDirty) return
    setSettingsSaving(true)
    setSettingsError('')
    setSettingsNotice('')
    try {
      const updated = await api.updateObservabilitySettings({
        scrape_interval_ms: draftScrapeIntervalMs,
        retention_ms: draftRetentionMs
      })
      onSettingsUpdated?.(updated)
      setSettingsNotice(t('observability.collector.settingsSaved'))
    } catch (err) {
      setSettingsError(String((err as { message?: string } | null)?.message || err))
    } finally {
      setSettingsSaving(false)
    }
  }

  const collapseLabelClass =
    'shrink-0 rounded-md bg-muted px-2 py-1 text-[11px] font-bold tracking-wide text-muted-foreground uppercase max-[900px]:hidden'
  const sectionCountBadgeClass =
    'ml-auto shrink-0 rounded-full border-border px-2.5 text-[11px] font-bold text-muted-foreground'

  return (
    <section className="mt-5 grid gap-4" data-testid="collector-panel">
      <div
        className={cn(
          'overflow-hidden rounded-xl border bg-card transition-[box-shadow,border-color]',
          expanded ? 'border-primary/30 shadow-sm' : 'border-border'
        )}
      >
        <button
          type="button"
          data-testid="collector-panel-toggle"
          aria-expanded={expanded}
          onClick={() => setExpanded((value) => !value)}
          className={cn(
            'flex w-full items-center gap-3 px-4 py-2.5 text-left text-foreground transition-colors',
            expanded ? 'border-b border-border bg-card' : 'bg-muted/40 hover:bg-muted/60'
          )}
        >
          <SectionChevron open={expanded} />
          <span className="font-heading text-base font-semibold">
            {t('observability.collector.title')}
          </span>
          <Badge
            variant="secondary"
            className="ml-auto shrink-0 rounded-full px-2.5 text-[11px] font-bold"
            data-testid="collector-targets-up-label"
          >
            {targetsUpLabel}
          </Badge>
          <span className={collapseLabelClass}>
            {expanded ? t('observability.chart.expanded') : t('observability.chart.collapsed')}
          </span>
        </button>

        {expanded && (
          <div className="grid gap-2.5 bg-muted/20 p-3">
            {/* ---- Settings (built-in provider only) ---- */}
            {showBuiltinSettings && (
              <div className="overflow-hidden rounded-lg border border-border bg-card">
                <button
                  type="button"
                  data-testid="collector-settings-toggle"
                  aria-expanded={settingsOpen}
                  onClick={() => setSettingsOpen((value) => !value)}
                  className={cn(
                    'flex w-full items-center gap-3 px-3.5 py-3 text-left text-foreground transition-colors hover:bg-muted/50',
                    settingsOpen ? 'border-b border-border' : 'bg-muted/40'
                  )}
                >
                  <SectionChevron open={settingsOpen} />
                  <span className="text-[15px] font-bold">
                    {t('observability.collector.settings')}
                  </span>
                  <Badge variant="outline" className={sectionCountBadgeClass}>
                    {t('observability.collector.settingsBadge')}
                  </Badge>
                  <span className={collapseLabelClass}>
                    {settingsOpen
                      ? t('observability.chart.expanded')
                      : t('observability.chart.collapsed')}
                  </span>
                </button>

                {settingsOpen && (
                  <div className="bg-card p-3">
                    <form
                      className="flex flex-wrap items-start gap-3"
                      onSubmit={(event) => {
                        event.preventDefault()
                        void applySettings()
                      }}
                    >
                      <div className="grid min-w-40 gap-1.5">
                        <span className="min-h-3.5 text-xs leading-tight font-bold text-muted-foreground">
                          {t('observability.collector.scrapeInterval')}
                        </span>
                        <Select
                          value={String(draftScrapeIntervalMs)}
                          disabled={settingsSaving}
                          onValueChange={(value) => setDraftScrapeIntervalMs(Number(value))}
                        >
                          <SelectTrigger
                            className="w-full"
                            data-testid="collector-scrape-interval"
                          >
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            {(settings?.scrape_interval_options_ms || []).map((value) => (
                              <SelectItem key={`scrape-${value}`} value={String(value)}>
                                {formatDuration(value)}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>

                      <div className="grid min-w-40 gap-1.5">
                        <span className="min-h-3.5 text-xs leading-tight font-bold text-muted-foreground">
                          {t('observability.collector.retention')}
                        </span>
                        <Select
                          value={String(draftRetentionMs)}
                          disabled={settingsSaving}
                          onValueChange={(value) => setDraftRetentionMs(Number(value))}
                        >
                          <SelectTrigger className="w-full" data-testid="collector-retention">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            {(settings?.retention_options_ms || []).map((value) => (
                              <SelectItem key={`retention-${value}`} value={String(value)}>
                                {formatDuration(value)}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>

                      <div className="grid min-w-25 gap-1.5">
                        <span
                          aria-hidden
                          className="invisible min-h-3.5 text-xs leading-tight font-bold text-muted-foreground"
                        >
                          {t('observability.collector.action')}
                        </span>
                        <Button
                          type="submit"
                          size="sm"
                          className="w-full"
                          data-testid="collector-settings-apply"
                          disabled={settingsSaving || !settingsDirty}
                        >
                          {settingsSaving ? t('common.saving') : t('common.apply')}
                        </Button>
                      </div>
                    </form>

                    {health && (
                      <p
                        className="mt-2.5 text-xs text-muted-foreground"
                        data-testid="collector-health-meta"
                      >
                        {t('observability.collector.seriesSamples', {
                          series: health.series_count,
                          samples: formatSampleCount(health.sample_count)
                        })}
                        {' · '}
                        {t('observability.collector.lastScrape', {
                          time: formatRelative(health.last_collect_ms)
                        })}
                      </p>
                    )}
                    {settingsError ? (
                      <p className="mt-2 text-xs text-destructive" data-testid="collector-settings-error">
                        {settingsError}
                      </p>
                    ) : settingsNotice ? (
                      <p className="mt-2 text-xs text-success" data-testid="collector-settings-notice">
                        {settingsNotice}
                      </p>
                    ) : null}
                    {healthError && (
                      <p className="mt-2 text-xs text-destructive" data-testid="collector-health-error">
                        {healthError}
                      </p>
                    )}
                  </div>
                )}
              </div>
            )}

            {/* ---- Issues (problem targets) ---- */}
            {(targetsLoading || targetsError || problemTargets.length > 0) && (
              <div className="overflow-hidden rounded-lg border border-border bg-card">
                <button
                  type="button"
                  data-testid="collector-issues-toggle"
                  aria-expanded={issuesOpen}
                  onClick={() => setIssuesOpen((value) => !value)}
                  className={cn(
                    'flex w-full items-center gap-3 px-3.5 py-3 text-left text-foreground transition-colors hover:bg-muted/50',
                    issuesOpen ? 'border-b border-border' : 'bg-muted/40'
                  )}
                >
                  <SectionChevron open={issuesOpen} />
                  <span className="text-[15px] font-bold">
                    {t('observability.collector.issues')}
                  </span>
                  <Badge variant="outline" className={sectionCountBadgeClass}>
                    {t('observability.collector.alertsCount', {
                      count: problemTargets.length
                    })}
                  </Badge>
                  <span className={collapseLabelClass}>
                    {issuesOpen
                      ? t('observability.chart.expanded')
                      : t('observability.chart.collapsed')}
                  </span>
                </button>

                {issuesOpen && (
                  <div className="bg-card p-3">
                    {targetsError ? (
                      <p className="text-xs text-destructive" data-testid="collector-targets-error">
                        {targetsError}
                      </p>
                    ) : targetsLoading ? (
                      <div
                        className="text-xs text-muted-foreground"
                        data-testid="collector-targets-loading"
                      >
                        {t('observability.collector.loadingTargets')}
                      </div>
                    ) : (
                      <ul
                        className="overflow-hidden rounded-md border border-border"
                        data-testid="collector-problem-targets"
                      >
                        {problemTargets.map((row) => (
                          <li
                            key={row.id}
                            data-testid={`collector-problem-target-${row.id}`}
                            className={cn(
                              'flex items-center gap-2 border-b border-border px-2.5 py-2 text-xs last:border-b-0',
                              !row.scrape_up && 'bg-destructive/5'
                            )}
                          >
                            <span
                              className="min-w-30 truncate font-semibold"
                              title={row.instance}
                            >
                              {shortInstance(row)}
                            </span>
                            <Badge
                              variant="outline"
                              className={cn(
                                'shrink-0 rounded-md',
                                row.scrape_up ? SCRAPE_PILL_CLASS.up : SCRAPE_PILL_CLASS.down
                              )}
                            >
                              {row.scrape_up
                                ? t('observability.targets.up')
                                : t('observability.targets.down')}
                            </Badge>
                            {row.worker_state && row.component === 'worker' && (
                              <span className="shrink-0 rounded-full bg-muted px-2 py-0.5 text-[11px] text-muted-foreground">
                                {workerStateLabel(row.worker_state)}
                              </span>
                            )}
                            {row.detail && (
                              <span
                                className="ml-auto min-w-0 truncate text-destructive"
                                title={row.detail}
                              >
                                {row.detail}
                              </span>
                            )}
                          </li>
                        ))}
                      </ul>
                    )}
                  </div>
                )}
              </div>
            )}

            {/* ---- Metric names ---- */}
            {(metricNames.length > 0 || !!namesError) && (
              <div className="overflow-hidden rounded-lg border border-border bg-card">
                <button
                  type="button"
                  data-testid="collector-metric-names-toggle"
                  aria-expanded={metricNamesOpen}
                  onClick={() => setMetricNamesOpen((value) => !value)}
                  className={cn(
                    'flex w-full items-center gap-3 px-3.5 py-3 text-left text-foreground transition-colors hover:bg-muted/50',
                    metricNamesOpen ? 'border-b border-border' : 'bg-muted/40'
                  )}
                >
                  <SectionChevron open={metricNamesOpen} />
                  <span className="text-[15px] font-bold">
                    {t('observability.collector.metricNames')}
                  </span>
                  <Badge variant="outline" className={sectionCountBadgeClass}>
                    {t('observability.collector.namesCount', { count: metricNames.length })}
                  </Badge>
                  <span className={collapseLabelClass}>
                    {metricNamesOpen
                      ? t('observability.chart.expanded')
                      : t('observability.chart.collapsed')}
                  </span>
                </button>

                {metricNamesOpen && (
                  <div className="bg-card p-3">
                    {namesError ? (
                      <p className="text-xs text-destructive" data-testid="collector-names-error">
                        {namesError}
                      </p>
                    ) : !metricNames.length ? (
                      <div
                        className="text-xs text-muted-foreground"
                        data-testid="collector-names-empty"
                      >
                        {t('observability.collector.noMetrics')}
                      </div>
                    ) : (
                      <div
                        className="flex max-h-40 flex-wrap gap-2 overflow-auto"
                        data-testid="collector-metric-names"
                      >
                        {metricNames.map((name) => (
                          <span
                            key={name}
                            className="rounded-full border border-border bg-muted px-2.5 py-1 text-xs text-foreground"
                          >
                            {name}
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </section>
  )
}
