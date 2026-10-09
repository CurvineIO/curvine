import { useCallback, useEffect, useRef, useState } from 'react'

import { useApi } from '@/api/api-context'
import type { MasterHaData, MasterHaNode, OverviewData } from '@/api/types'
import { DetailList, DetailRow } from '@/components/detail-list'
import { ErrorAlert } from '@/components/data-state'
import { Meter } from '@/components/meter'
import { MetricCard, type MetricTone } from '@/components/metric-card'
import { PageHeader } from '@/components/page-header'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle
} from '@/components/ui/alert-dialog'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Spinner } from '@/components/ui/spinner'
import { TableCell, TableRow } from '@/components/ui/table'
import { ResourceTable, StatusBadge } from '@/components/resource-table'
import { useT } from '@/i18n'
import {
  displayMasterAddr,
  formatBytes,
  formatCompactNumber,
  formatNumber,
  isLoopbackHost,
  normalizeCount
} from '@/lib/format'
import { translateDelta, translateRole } from '@/lib/i18nLabels'
import { useMasterRuntime } from '@/master-runtime/master-runtime-context'
import { useRefreshSignal } from '@/refresh/refresh-context'
import { useAsync } from '@/lib/use-async'

const FAILOVER_POLL_ATTEMPTS = 30
const FAILOVER_POLL_INTERVAL_MS = 1000

function sleep(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms))
}

function percentOf(value: unknown, total: unknown): number {
  const base = Number(total || 0)
  if (!base) return 0
  return Math.min(100, Math.round((Number(value || 0) / base) * 100))
}

function percentLabel(value: unknown, total: unknown): string {
  if (!total || !value) return '0%'
  const percent = (Number(value) / Number(total)) * 100
  if (percent > 0 && percent < 0.01) return '<0.01%'
  if (percent < 1) return `${percent.toFixed(2)}%`
  return `${Math.min(100, Math.round(percent))}%`
}

function hostnameFromAddr(addr: string | null | undefined): string {
  const match = String(addr || '').match(/^([^:]+):/)
  return match ? match[1] : ''
}

function isTargetActive(ha: MasterHaData, targetAddr: string): boolean {
  const nodes = Array.isArray(ha.nodes) ? ha.nodes : []
  const node = nodes.find((item) => item.addr === targetAddr)
  if (node && (node.current || node.role === 'Active')) return true
  const configured = ha.active_master_configured || ha.active_master
  if (configured && configured === targetAddr) return true
  if (node && node.raft_id != null && ha.active_raft_id != null) {
    return node.raft_id === ha.active_raft_id
  }
  return false
}

export function OverviewPage() {
  const t = useT()
  const api = useApi()
  const { setFromOverview } = useMasterRuntime()

  const load = useCallback(async () => {
    const [overviewResult, haResult] = await Promise.allSettled([
      api.fetchOverviewData(),
      api.fetchMasterHa()
    ])
    if (overviewResult.status === 'rejected' && haResult.status === 'rejected') {
      throw overviewResult.reason
    }
    return {
      overview: (overviewResult.status === 'fulfilled' ? overviewResult.value : {}) ?? {},
      masterHa: (haResult.status === 'fulfilled' ? haResult.value : {}) ?? {}
    }
  }, [api])

  const { data, loading, error, reload, refresh } = useAsync(load, [load])
  useRefreshSignal(refresh)

  const [failoverTarget, setFailoverTarget] = useState<MasterHaNode | null>(null)
  const [switching, setSwitching] = useState<string | null>(null)
  const [failoverError, setFailoverError] = useState<string | null>(null)
  const [haRefreshing, setHaRefreshing] = useState(false)
  const switchingRef = useRef(false)

  const overview: OverviewData = data?.overview ?? {}
  const masterHa: MasterHaData = data?.masterHa ?? {}

  useEffect(() => {
    if (data?.overview) setFromOverview(data.overview)
  }, [data, setFromOverview])

  const masterNodes: MasterHaNode[] = Array.isArray(masterHa.nodes) ? masterHa.nodes : []
  const activeMaster =
    masterHa.active_master_configured ||
    masterHa.active_master ||
    overview.active_master ||
    ''

  const preferredMasterHostname =
    [
      overview.local_hostname,
      masterHa.local_hostname,
      hostnameFromAddr(activeMaster),
      masterNodes
        .map((node) => hostnameFromAddr(node.display_addr || node.addr))
        .find((host) => host && !isLoopbackHost(host))
    ].find((host) => host && !isLoopbackHost(host)) || ''

  const activeMasterDisplay = displayMasterAddr(
    overview.active_master_display || masterHa.active_master_display || activeMaster || '',
    preferredMasterHostname
  )
  const configuredMasterDisplay = displayMasterAddr(
    masterHa.configured_master || overview.master_addr || '-',
    preferredMasterHostname
  )

  const masterState = (() => {
    const state = overview.master_state || masterHa.master_state
    if (state === 'Active' || state === 'Standby') return state
    if (!activeMaster) return 'Unknown'
    return activeMaster === (masterHa.current_master || overview.master_addr || '')
      ? 'Active'
      : 'Standby'
  })()
  const masterStateTone: MetricTone =
    masterState === 'Active' ? 'good' : masterState === 'Standby' ? 'warn' : 'neutral'

  const standbyCount = masterNodes.filter((node) => node.role === 'Standby').length
  const capacityUsedPercent = percentOf(overview.fs_used, overview.capacity)
  const capacityUsedLabel = percentLabel(overview.fs_used, overview.capacity)
  const workerTotal = (overview.live_workers || 0) + (overview.lost_workers || 0)
  const namespaceTotal = normalizeCount(overview.files_total) + normalizeCount(overview.dir_total)

  const delta = (value: string) => translateDelta(t, value)

  async function waitForActiveMaster(targetAddr: string) {
    let lastError = ''
    for (let index = 0; index < FAILOVER_POLL_ATTEMPTS; index += 1) {
      await sleep(FAILOVER_POLL_INTERVAL_MS)
      try {
        const nextHa = await api.fetchMasterHa()
        if (isTargetActive(nextHa ?? {}, targetAddr)) return
      } catch (nextError) {
        lastError = nextError instanceof Error ? nextError.message : String(nextError)
      }
    }
    const suffix = lastError ? t('overview.errors.lastError', { error: lastError }) : ''
    throw new Error(t('overview.errors.switchTimeout', { target: targetAddr, suffix }))
  }

  async function confirmFailover() {
    const target = failoverTarget
    if (!target || switchingRef.current) return
    switchingRef.current = true
    setSwitching(target.addr)
    setFailoverError(null)
    try {
      await api.requestMasterFailover(target.addr)
      await waitForActiveMaster(target.addr)
      setFailoverTarget(null)
      await reload()
    } catch (nextError) {
      setFailoverError(nextError instanceof Error ? nextError.message : String(nextError))
    } finally {
      switchingRef.current = false
      setSwitching(null)
    }
  }

  async function refreshHa() {
    setHaRefreshing(true)
    try {
      await refresh()
    } finally {
      setHaRefreshing(false)
    }
  }

  function switchTitle(node: MasterHaNode): string {
    if (!masterHa.failover_supported) return t('overview.tooltips.failoverUnavailable')
    if (switching && switching !== node.addr) {
      return t('overview.tooltips.switchingTo', { target: switching })
    }
    if (node.current || node.role === 'Active') return t('overview.tooltips.alreadyActive')
    if (!node.reachable) {
      return node.reachable_error || t('overview.tooltips.targetUnreachable')
    }
    return ''
  }

  return (
    <>
      <PageHeader
        eyebrow={t('nav.groupOverview')}
        title={t('nav.overview')}
        description={t('nav.overviewSubtitle')}
        actions={
          <Button
            variant="outline"
            size="sm"
            onClick={() => void reload()}
            disabled={loading}
            data-testid="overview-refresh"
          >
            {t('common.refresh')}
          </Button>
        }
      />

      {error && <ErrorAlert message={error} />}

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
        <MetricCard
          label={t('overview.metrics.master')}
          value={masterState === 'Unknown' ? t('common.unknown') : translateRole(t, masterState)}
          meta={activeMasterDisplay || '-'}
          delta={overview.cluster_id || '-'}
          tone={masterStateTone}
        />
        <MetricCard
          label={t('overview.metrics.haNodes')}
          value={masterNodes.length || '-'}
          meta={
            masterNodes.length
              ? t('overview.metrics.standbyMeta', { standby: standbyCount, active: activeMasterDisplay })
              : t('overview.metrics.notConfigured')
          }
          delta={delta(masterHa.failover_supported ? 'switchable' : 'readonly')}
          tone={masterNodes.length > 1 ? 'good' : 'neutral'}
        />
        <MetricCard
          label={t('overview.metrics.workers')}
          value={workerTotal}
          meta={t('overview.metrics.liveLost', {
            live: overview.live_workers || 0,
            lost: overview.lost_workers || 0
          })}
          delta={delta(overview.lost_workers ? 'attention' : 'healthy')}
          tone={overview.lost_workers ? 'warn' : 'good'}
        />
        <MetricCard
          label={t('overview.metrics.capacity')}
          value={formatBytes(overview.capacity)}
          meta={t('overview.metrics.availableMeta', { amount: formatBytes(overview.available) })}
          delta={t('overview.metrics.usedDelta', { percent: capacityUsedLabel })}
          tone="neutral"
        />
        <MetricCard
          label={t('overview.metrics.namespace')}
          value={formatCompactNumber(namespaceTotal)}
          meta={t('overview.metrics.filesMeta', {
            count: formatCompactNumber(overview.files_total)
          })}
          delta={t('overview.metrics.dirsDelta', { count: formatCompactNumber(overview.dir_total) })}
          tone="good"
        />
      </section>

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1.15fr)_minmax(0,0.85fr)]">
        <Card size="sm">
          <CardHeader>
            <CardTitle>{t('overview.capacity.title')}</CardTitle>
            <p className="text-xs text-muted-foreground">{t('overview.capacity.subtitle')}</p>
          </CardHeader>
          <CardContent className="grid gap-5 sm:grid-cols-[9rem_minmax(0,1fr)] sm:items-center">
            <div
              className="grid aspect-square w-36 place-items-center rounded-full"
              style={{
                background: `conic-gradient(var(--color-primary) 0 ${capacityUsedPercent}%, var(--color-muted) ${capacityUsedPercent}% 100%)`
              }}
            >
              <div className="grid size-[calc(100%-2.25rem)] place-items-center rounded-full bg-card text-center">
                <div>
                  <strong className="block font-heading text-xl font-semibold tabular-nums">
                    {capacityUsedLabel}
                  </strong>
                  <span className="text-xs text-muted-foreground">
                    {t('overview.capacity.used')}
                  </span>
                </div>
              </div>
            </div>
            <div className="flex flex-col gap-3">
              <CapacityRow
                label={t('overview.capacity.fsUsed')}
                value={formatBytes(overview.fs_used)}
                percent={percentOf(overview.fs_used, overview.capacity)}
              />
              <CapacityRow
                label={t('overview.capacity.available')}
                value={formatBytes(overview.available)}
                percent={percentOf(overview.available, overview.capacity)}
              />
              <CapacityRow
                label={t('overview.capacity.reserved')}
                value={formatBytes(overview.reserved_bytes)}
                percent={percentOf(overview.reserved_bytes, overview.capacity)}
              />
            </div>
          </CardContent>
        </Card>

        <Card size="sm">
          <CardHeader>
            <CardTitle>{t('overview.masterDetails.title')}</CardTitle>
            <p className="text-xs text-muted-foreground">{t('overview.masterDetails.subtitle')}</p>
          </CardHeader>
          <CardContent>
            <DetailList>
              <DetailRow label={t('overview.masterDetails.clusterId')}>
                {overview.cluster_id || '-'}
              </DetailRow>
              <DetailRow label={t('overview.masterDetails.configuredMaster')}>
                {configuredMasterDisplay || '-'}
              </DetailRow>
              <DetailRow label={t('overview.masterDetails.activeMaster')}>
                {activeMasterDisplay || '-'}
              </DetailRow>
              <DetailRow label={t('overview.masterDetails.version')}>
                <span>{overview.version || t('common.notReported')}</span>
                {overview.version_commit && (
                  <span className="block text-xs text-muted-foreground">{overview.version_commit}</span>
                )}
              </DetailRow>
              <DetailRow label={t('overview.masterDetails.startTime')}>
                {overview.start_time || t('common.notReported')}
              </DetailRow>
              <DetailRow label={t('overview.masterDetails.blockTotal')}>
                {formatNumber(overview.block_total)}
              </DetailRow>
            </DetailList>
          </CardContent>
        </Card>
      </section>

      <section className="flex flex-col gap-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="flex flex-col gap-0.5">
            <h2 className="font-heading text-base font-medium">{t('overview.ha.title')}</h2>
            <p className="text-xs text-muted-foreground">{t('overview.ha.subtitle')}</p>
          </div>
          <Button
            variant="outline"
            size="sm"
            disabled={haRefreshing}
            onClick={() => void refreshHa()}
            data-testid="overview-ha-refresh"
          >
            {t('common.refresh')}
          </Button>
        </div>

        <ResourceTable
          loading={loading}
          error={null}
          emptyTitle={t('overview.ha.empty')}
          columns={[
            t('overview.ha.master'),
            t('overview.ha.journal'),
            t('overview.ha.role'),
            t('overview.ha.version'),
            t('overview.ha.startTime'),
            t('overview.ha.reachable'),
            t('overview.ha.current'),
            t('overview.ha.action')
          ]}
          rows={masterNodes.map((node) => (
            <TableRow key={node.addr}>
              <TableCell className="font-medium">{node.display_addr || node.addr}</TableCell>
              <TableCell className="text-muted-foreground">{node.journal_addr || '-'}</TableCell>
              <TableCell>
                <StatusBadge
                  value={translateRole(t, node.role || '')}
                  tone={node.role === 'Active' ? 'positive' : node.role === 'Standby' ? 'warning' : 'neutral'}
                />
              </TableCell>
              <TableCell className="text-muted-foreground">
                {node.version || t('common.notReported')}
              </TableCell>
              <TableCell className="text-muted-foreground">
                {node.start_time || t('common.notReported')}
              </TableCell>
              <TableCell>
                <span title={node.reachable_error || ''}>
                  <StatusBadge
                    value={node.reachable ? t('common.reachable') : t('common.unreachable')}
                    tone={node.reachable ? 'positive' : 'negative'}
                  />
                </span>
              </TableCell>
              <TableCell className="text-muted-foreground">
                {node.current ? t('overview.ha.activeTarget') : '-'}
              </TableCell>
              <TableCell>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-6 px-1 text-primary"
                  disabled={Boolean(switching) || !node.switchable}
                  title={switchTitle(node)}
                  onClick={() => {
                    setFailoverError(null)
                    setFailoverTarget(node)
                  }}
                  data-testid={`overview-failover-${node.addr}`}
                >
                  {switching === node.addr ? t('common.switching') : t('common.switch')}
                </Button>
              </TableCell>
            </TableRow>
          ))}
        />

        <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
          <strong className="text-foreground">{t('overview.ha.failoverApi')}</strong>
          <span>
            {masterHa.failover_supported ? t('overview.ha.enabled') : t('overview.ha.unavailable')}
          </span>
        </div>

        {failoverError && <ErrorAlert message={failoverError} />}
      </section>

      <AlertDialog
        open={failoverTarget !== null}
        onOpenChange={(open) => {
          if (!open) setFailoverTarget(null)
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t('common.switch')}</AlertDialogTitle>
            <AlertDialogDescription>
              {t('overview.confirm.switchMaster', {
                from: activeMasterDisplay || '-',
                to: failoverTarget?.addr || '-'
              })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={Boolean(switching)}>{t('common.cancel')}</AlertDialogCancel>
            <AlertDialogAction
              disabled={Boolean(switching)}
              onClick={(event) => {
                event.preventDefault()
                void confirmFailover()
              }}
              data-testid="overview-failover-confirm"
            >
              {switching && <Spinner data-icon="inline-start" />}
              {switching ? t('common.switching') : t('common.confirm')}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}

function CapacityRow({
  label,
  value,
  percent
}: {
  label: string
  value: string
  percent: number
}) {
  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-baseline justify-between gap-3 text-xs">
        <span className="text-muted-foreground">{label}</span>
        <strong className="font-medium tabular-nums">{value}</strong>
      </div>
      <Meter percent={percent} />
    </div>
  )
}
