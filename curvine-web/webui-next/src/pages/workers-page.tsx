import { useCallback, useMemo, useState } from 'react'
import { ServerIcon } from 'lucide-react'
import { useNavigate, useParams } from 'react-router-dom'

import { useApi } from '@/api/api-context'
import type { WorkerAction, WorkerInfo } from '@/api/types'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { ErrorAlert } from '@/components/data-state'
import { Meter, usagePercent } from '@/components/meter'
import { MetricCard } from '@/components/metric-card'
import { PageHeader } from '@/components/page-header'
import { ResourceTable, StatusBadge, workerStatusTone } from '@/components/resource-table'
import { Input } from '@/components/ui/input'
import { TableCell, TableRow } from '@/components/ui/table'
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'
import { useT } from '@/i18n'
import {
  formatBytes,
  formatTime,
  versionsMismatch,
  workerAddress,
  workerHostname,
  workerId,
  workerShortVersion
} from '@/lib/format'
import { translateDelta, translateStatus, translateWorkerAction } from '@/lib/i18nLabels'
import { useMasterRuntime } from '@/master-runtime/master-runtime-context'
import { useRefreshSignal } from '@/refresh/refresh-context'
import { useAsync } from '@/lib/use-async'
import { useSlidingSheet } from '@/lib/use-sliding-sheet'
import { WorkerDetailSheet } from './worker-detail-sheet'
import { ProfileDialog } from './profile-dialog'

type WorkerRow = WorkerInfo & { rowStatus: string }

const FILTERS = ['all', 'live', 'blacklist', 'decommission', 'lost'] as const
type StatusFilter = (typeof FILTERS)[number]

function ipAddress(worker: WorkerInfo): string {
  return worker.display_ip || worker.address?.ip_addr || worker.ip_addr || '-'
}

function rpcPort(worker: WorkerInfo): string | number {
  return worker.address?.rpc_port || '-'
}

export function WorkersPage() {
  const t = useT()
  const api = useApi()
  const navigate = useNavigate()
  const { id: detailSelector } = useParams()
  const detail = useSlidingSheet(Boolean(detailSelector), detailSelector)
  const { runtime } = useMasterRuntime()

  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all')
  const [query, setQuery] = useState('')
  const [pendingAction, setPendingAction] = useState<{ worker: WorkerRow; action: WorkerAction } | null>(
    null
  )
  const [submitting, setSubmitting] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)
  // Which worker's flame graph is open, if any. Kept as the worker object
  // rather than an id so the dialog does not re-resolve it after the list
  // refreshes behind the modal.
  const [profileWorker, setProfileWorker] = useState<WorkerInfo | null>(null)

  const load = useCallback(() => api.fetchWorkersData(), [api])
  const { data, loading, error, reload, refresh } = useAsync(load, [load])
  useRefreshSignal(refresh)

  const groups = useMemo(() => {
    const withStatus = (workers: WorkerInfo[] | undefined, rowStatus: string): WorkerRow[] =>
      (workers || []).map((worker) => ({ ...worker, rowStatus }))
    return {
      live: withStatus(data?.live_workers, 'live'),
      blacklist: withStatus(data?.blacklist_workers, 'blacklist'),
      decommission: withStatus(data?.decommission_workers, 'decommission'),
      lost: withStatus(data?.lost_workers, 'lost')
    }
  }, [data])

  const allWorkers = useMemo(
    () => [...groups.live, ...groups.blacklist, ...groups.decommission, ...groups.lost],
    [groups]
  )

  const filtered = useMemo(() => {
    const needle = query.toLowerCase()
    return allWorkers.filter(
      (worker) =>
        (statusFilter === 'all' || worker.rowStatus === statusFilter) &&
        `${workerHostname(worker)} ${workerId(worker)} ${workerAddress(worker)} ${ipAddress(worker)} ${workerShortVersion(
          worker
        )}`
          .toLowerCase()
          .includes(needle)
    )
  }, [allWorkers, statusFilter, query])

  const activeWorkers = useMemo(
    () => [...groups.live, ...groups.blacklist, ...groups.decommission],
    [groups]
  )
  const totalCapacity = activeWorkers.reduce((sum, worker) => sum + Number(worker.capacity || 0), 0)
  const totalAvailable = activeWorkers.reduce((sum, worker) => sum + Number(worker.available || 0), 0)
  const clusterUsage = usagePercent(totalCapacity, totalAvailable)

  const delta = (value: string) => translateDelta(t, value)

  function workerVersionIssues(worker: WorkerInfo) {
    return versionsMismatch(workerShortVersion(worker), runtime.version)
  }

  async function confirmAction() {
    const target = pendingAction
    if (!target) return
    setSubmitting(true)
    setActionError(null)
    try {
      await api.updateWorkerAction(workerAddress(target.worker), target.action)
      setPendingAction(null)
      await reload()
    } catch (nextError) {
      setActionError(nextError instanceof Error ? nextError.message : t('common.error'))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <>
      <PageHeader
        eyebrow={t('nav.groupRuntime')}
        title={t('nav.workers')}
        description={t('nav.workersSubtitle')}
      />

      {error && <ErrorAlert message={error} />}

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          label={t('workers.metrics.liveWorkers')}
          value={groups.live.length}
          meta={t('workers.metrics.healthyHeartbeat')}
          delta={delta('online')}
          tone="good"
        />
        <MetricCard
          label={t('workers.metrics.managed')}
          value={groups.blacklist.length + groups.decommission.length}
          meta={t('workers.metrics.blacklistDecommission')}
          delta={`${groups.blacklist.length} / ${groups.decommission.length}`}
          tone={groups.blacklist.length + groups.decommission.length ? 'warn' : 'neutral'}
        />
        <MetricCard
          label={t('workers.metrics.totalCapacity')}
          value={formatBytes(totalCapacity)}
          meta={t('workers.metrics.activeWorkers')}
          delta={t('workers.metrics.usedPercent', { percent: clusterUsage })}
          tone="neutral"
        />
        <MetricCard
          label={t('workers.metrics.lostWorkers')}
          value={groups.lost.length}
          meta={t('workers.metrics.requiresAttention')}
          delta={delta(groups.lost.length ? 'check' : 'none')}
          tone={groups.lost.length ? 'bad' : 'good'}
        />
      </section>

      <section className="flex flex-col gap-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="flex flex-col gap-0.5">
            <h2 className="font-heading text-base font-medium">{t('workers.list.title')}</h2>
            <p className="text-xs text-muted-foreground">{t('workers.list.subtitle')}</p>
          </div>
          <ToggleGroup
            type="single"
            value={statusFilter}
            onValueChange={(value) => {
              if (value) setStatusFilter(value as StatusFilter)
            }}
            data-testid="workers-filter"
          >
            {FILTERS.map((filter) => (
              <ToggleGroupItem
                key={filter}
                value={filter}
                size="sm"
                data-testid={`workers-filter-${filter}`}
              >
                {t(`workers.filters.${filter}`)}
              </ToggleGroupItem>
            ))}
          </ToggleGroup>
        </div>

        <div className="flex flex-wrap items-center gap-2 rounded-lg border border-border bg-muted/30 px-3 py-2">
          <Input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder={t('workers.list.searchPlaceholder')}
            className="max-w-sm"
            aria-label={t('workers.list.searchPlaceholder')}
            data-testid="workers-search"
          />
          <span className="text-xs text-muted-foreground tabular-nums">
            {filtered.length} / {allWorkers.length}
          </span>
        </div>

        {actionError && <ErrorAlert message={actionError} />}

        <ResourceTable
          loading={loading}
          error={null}
          emptyTitle={t('workers.list.empty')}
          columnWidths={['20%', '8%', '21%', '15%', '5%', '11%', '20%']}
          columns={[
            t('workers.list.headerWorker'),
            t('workers.list.headerStatus'),
            t('workers.list.headerVersion'),
            t('workers.list.headerUsage'),
            t('workers.list.headerBlocks'),
            t('workers.list.headerHeartbeat'),
            t('workers.list.headerActions')
          ]}
          rows={filtered.map((worker) => {
            const usage = usagePercent(worker.capacity, worker.available)
            const versionIssue = workerVersionIssues(worker)
            const selector = workerAddress(worker)
            return (
              <TableRow key={`${worker.rowStatus}-${selector}`} data-testid={`worker-row-${selector}`}>
                <TableCell>
                  <div className="flex w-full min-w-0 items-center gap-2">
                    <ServerIcon
                      className="size-4 shrink-0 text-muted-foreground"
                      aria-hidden
                      data-testid={`worker-icon-${selector}`}
                    />
                    <button
                      type="button"
                      className="flex min-w-0 flex-1 items-baseline gap-1.5 text-left"
                      onClick={() => navigate(`/workers/${encodeURIComponent(selector)}`)}
                      data-testid={`worker-open-${selector}`}
                    >
                      <strong className="truncate font-medium hover:text-primary">
                        {workerHostname(worker)}
                      </strong>
                      <span className="truncate font-mono text-xs text-muted-foreground">
                        {t('workers.list.rpc', 'RPC')} {rpcPort(worker)}
                        {ipAddress(worker) !== '-' ? ` · ${ipAddress(worker)}` : ''}
                      </span>
                    </button>
                  </div>
                </TableCell>
                <TableCell>
                  <StatusBadge
                    value={translateStatus(t, worker.rowStatus)}
                    tone={workerStatusTone(worker.rowStatus)}
                  />
                </TableCell>
                <TableCell>
                  {/* Its own column: the reported version carries the build
                      commit and branch, so it needs more room than a subtitle —
                      and it is proportional, like the detail page's copy, because
                      monospace would push the branch past the column. */}
                  <span
                    className={`block truncate text-xs ${versionIssue ? 'text-warning' : undefined}`}
                    title={versionIssue ? t('workers.detail.runtime.versionMismatch') : undefined}
                    data-testid={`worker-version-${selector}`}
                  >
                    {workerShortVersion(worker) || t('workers.detail.runtime.notReported')}
                  </span>
                </TableCell>
                <TableCell>
                  {/* One line: `used` is derived from capacity - available, so
                      showing used/capacity keeps every figure recoverable. */}
                  <div className="flex w-full items-center gap-2">
                    <strong className="shrink-0 text-xs tabular-nums">{usage}%</strong>
                    <Meter
                      percent={usage}
                      tone={usage >= 90 ? 'destructive' : usage >= 75 ? 'warning' : 'primary'}
                      className="flex-1"
                    />
                    <span className="shrink-0 text-xs whitespace-nowrap text-muted-foreground tabular-nums">
                      {formatBytes(Math.max(0, Number(worker.capacity || 0) - Number(worker.available || 0)))}
                      {' / '}
                      {formatBytes(worker.capacity)}
                    </span>
                  </div>
                </TableCell>
                <TableCell className="tabular-nums">{worker.block_num || 0}</TableCell>
                <TableCell
                  className="truncate text-xs text-muted-foreground"
                  title={formatTime(worker.last_update_ms || worker.last_update)}
                >
                  {formatTime(worker.last_update_ms || worker.last_update)}
                </TableCell>
                <TableCell>
                  <div className="flex items-center gap-1">
                    <WorkerActionButton
                      label={t('workers.actions.detail')}
                      onClick={() => navigate(`/workers/${encodeURIComponent(selector)}`)}
                    />
                    <WorkerActionButton
                      label={t('workers.actions.profile')}
                      onClick={() => setProfileWorker(worker)}
                    />
                    {worker.rowStatus !== 'lost' && worker.rowStatus !== 'blacklist' && (
                      <WorkerActionButton
                        label={t('workers.actions.addBlacklist')}
                        onClick={() => setPendingAction({ worker, action: 'blacklist' })}
                      />
                    )}
                    {worker.rowStatus === 'blacklist' && (
                      <WorkerActionButton
                        label={t('workers.actions.allow')}
                        onClick={() => setPendingAction({ worker, action: 'allow' })}
                      />
                    )}
                    {worker.rowStatus !== 'lost' && worker.rowStatus !== 'decommission' && (
                      <WorkerActionButton
                        label={t('workers.actions.decommission')}
                        onClick={() => setPendingAction({ worker, action: 'decommission' })}
                      />
                    )}
                    {worker.rowStatus === 'decommission' && (
                      <WorkerActionButton
                        label={t('workers.actions.recommission')}
                        onClick={() => setPendingAction({ worker, action: 'recommission' })}
                      />
                    )}
                  </div>
                </TableCell>
              </TableRow>
            )
          })}
        />

        <ConfirmDialog
          open={pendingAction !== null}
          title={translateWorkerAction(t, pendingAction?.action ?? '')}
          description={t('workers.actions.confirm', {
            action: translateWorkerAction(t, pendingAction?.action ?? ''),
            worker: pendingAction ? workerAddress(pendingAction.worker) : ''
          })}
          confirmLabel={t('common.confirm')}
          cancelLabel={t('common.cancel')}
          pending={submitting}
          destructive={pendingAction?.action === 'decommission' || pendingAction?.action === 'blacklist'}
          onOpenChange={(open) => {
            if (!open) setPendingAction(null)
          }}
          onConfirm={() => void confirmAction()}
          testId="workers-action-confirm"
        />
      </section>

      {/* The detail lives on the same route as the list (`/workers/:id?`), so it
          renders over it: the list stays mounted behind the panel, and a deep
          link or a back button lands on the same state. */}
      {detail.mounted && detail.value && (
        <WorkerDetailSheet
          open={detail.open}
          selector={detail.value}
          onClose={() => navigate('/workers')}
        />
      )}

      <ProfileDialog
        host={
          profileWorker
            ? profileWorker.display_ip ||
              profileWorker.address?.ip_addr ||
              profileWorker.address?.hostname ||
              profileWorker.ip_addr
            : null
        }
        webPort={profileWorker?.address?.web_port}
        target={
          profileWorker
            ? `${t('nav.workers')} ${workerHostname(profileWorker)}:${rpcPort(profileWorker)}`
            : undefined
        }
        open={profileWorker !== null}
        onOpenChange={(open) => {
          if (!open) setProfileWorker(null)
        }}
      />
    </>
  )
}

function WorkerActionButton({ label, onClick }: { label: string; onClick(): void }) {
  return (
    <button
      type="button"
      className="rounded-md px-1.5 py-0.5 text-xs font-medium text-primary transition-colors hover:bg-accent hover:text-accent-foreground"
      onClick={onClick}
    >
      {label}
    </button>
  )
}
