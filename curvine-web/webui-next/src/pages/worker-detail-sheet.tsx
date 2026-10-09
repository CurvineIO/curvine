import { useCallback, useState } from 'react'
import {
  CloudIcon,
  HardDriveIcon,
  MemoryStickIcon,
  ServerIcon,
  ZapIcon,
  type LucideIcon
} from 'lucide-react'

import { useApi } from '@/api/api-context'
import type { WorkerAction, WorkerStorageInfo } from '@/api/types'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { ErrorAlert } from '@/components/data-state'
import { DetailList, DetailRow } from '@/components/detail-list'
import { Meter, usagePercent } from '@/components/meter'
import { ResourceTable, StatusBadge, workerStatusTone } from '@/components/resource-table'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { TableCell, TableRow } from '@/components/ui/table'
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
import { translateStatus, translateWorkerAction } from '@/lib/i18nLabels'
import { useMasterRuntime } from '@/master-runtime/master-runtime-context'
import { cn } from '@/lib/utils'
import { PANEL_FALLBACK_CLASS, PANEL_SHARE, useContentWidth } from '@/lib/use-content-width'
import { useAsync } from '@/lib/use-async'

/**
 * Icon per `StorageType` variant (Mem / Ssd / Hdd / Ufs / Disk / SpdkDisk).
 * Disk and Hdd share the spinning-disk glyph since they are the same medium;
 * SSD gets a flash glyph, UFS a cloud, NVMe-oF a remote-host glyph.
 */
const STORAGE_TYPE_ICONS: Record<string, LucideIcon> = {
  mem: MemoryStickIcon,
  ssd: ZapIcon,
  hdd: HardDriveIcon,
  disk: HardDriveIcon,
  ufs: CloudIcon,
  spdkdisk: ServerIcon
}

function storageTypeIcon(value: unknown): LucideIcon {
  return STORAGE_TYPE_ICONS[String(value ?? '').trim().toLowerCase()] ?? HardDriveIcon
}

/**
 * Worker detail as a right-hand panel over the list rather than its own page.
 *
 * It stays reachable at `/workers/<selector>` — `WorkersPage` owns that route and
 * renders this when the selector is present — so deep links and the browser's back
 * button keep working while the list stays visible behind the overlay.
 */
export function WorkerDetailSheet({
  open,
  selector,
  onClose
}: {
  open: boolean
  selector: string
  onClose(): void
}) {
  const t = useT()
  const api = useApi()
  const { runtime } = useMasterRuntime()
  const contentWidth = useContentWidth()

  const [pendingAction, setPendingAction] = useState<WorkerAction | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)

  const load = useCallback(() => api.fetchWorkerDetail(selector), [api, selector])
  const { data, loading, error, reload } = useAsync(load, [load])

  const worker = data?.worker ?? {}
  const rawStatus = data?.state || worker.status || ''
  const address = workerAddress(worker)
  const shortVersion = String(data?.version || workerShortVersion(worker) || '').trim()
  const versionIssue = versionsMismatch(shortVersion, runtime.version)
  const storageItems: WorkerStorageInfo[] = Object.values(worker.storage_map || {})
  const usage = usagePercent(worker.capacity, worker.available)
  const displayIp = worker.display_ip || worker.address?.ip_addr || '-'
  const manageable = rawStatus !== 'Lost'

  const titleParts = [workerHostname(worker), workerAddress(worker)]
  if (displayIp !== '-') titleParts.push(`${t('workers.detail.runtime.ip')} ${displayIp}`)

  async function confirmAction() {
    if (!pendingAction) return
    setSubmitting(true)
    setActionError(null)
    try {
      await api.updateWorkerAction(address, pendingAction)
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
      <Sheet
        open={open}
        onOpenChange={(next) => {
          if (!next) onClose()
        }}
      >
        {/* The variant caps every side panel at `sm:max-w-sm` (24rem), which is what
            made this one too narrow to read. `sm:max-w-none` drops that cap and the
            measured width takes over; the class fallback covers the first paint,
            before the measurement lands.
            Only the width is set: the panel keeps the variant's `right-0`, so it is
            flush with the window edge exactly like every other side panel. Insetting
            it instead left a strip of background down the right-hand side. */}
        <SheetContent
          side="right"
          data-testid="worker-detail-sheet"
          className={cn(
            'gap-0 p-0 sm:max-w-none',
            contentWidth === null && PANEL_FALLBACK_CLASS
          )}
          style={contentWidth === null ? undefined : { width: contentWidth * PANEL_SHARE }}
        >
          <SheetHeader className="gap-2 border-b border-border pr-12">
            <div className="flex flex-wrap items-center gap-2">
              <div className="flex min-w-0 flex-col gap-0.5">
                <SheetTitle>{t('workers.detail.title')}</SheetTitle>
                <SheetDescription className="truncate">{titleParts.join(' - ')}</SheetDescription>
              </div>
              <div className="ml-auto flex flex-wrap items-center gap-2">
                {manageable && rawStatus !== 'Blacklist' && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setPendingAction('blacklist')}
                    data-testid="worker-detail-blacklist"
                  >
                    {t('workers.actions.addBlacklist')}
                  </Button>
                )}
                {rawStatus === 'Blacklist' && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setPendingAction('allow')}
                    data-testid="worker-detail-allow"
                  >
                    {t('workers.actions.allow')}
                  </Button>
                )}
                {manageable && rawStatus !== 'Decommission' && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setPendingAction('decommission')}
                    data-testid="worker-detail-decommission"
                  >
                    {t('workers.actions.decommission')}
                  </Button>
                )}
                {rawStatus === 'Decommission' && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setPendingAction('recommission')}
                    data-testid="worker-detail-recommission"
                  >
                    {t('workers.actions.recommission')}
                  </Button>
                )}
              </div>
            </div>
          </SheetHeader>
  
          <div className="flex flex-1 flex-col gap-4 overflow-y-auto p-4">
            {error && <ErrorAlert message={error} />}
            {actionError && <ErrorAlert message={actionError} />}
  
              <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                <SummaryTile label={t('workers.list.headerStatus')}>
                  <StatusBadge
                    value={rawStatus ? translateStatus(t, rawStatus) : '-'}
                    tone={workerStatusTone(rawStatus)}
                  />
                </SummaryTile>
                <SummaryTile label={t('workers.list.headerUsage')}>
                  <div className="flex flex-col gap-1.5">
                    <strong className="font-heading text-lg tabular-nums">{usage}%</strong>
                    <Meter percent={usage} tone={usage >= 90 ? 'destructive' : usage >= 75 ? 'warning' : 'primary'} />
                  </div>
                </SummaryTile>
                <SummaryTile label={t('common.capacity')}>
                  <strong className="font-heading text-lg tabular-nums">{formatBytes(worker.capacity)}</strong>
                </SummaryTile>
                <SummaryTile label={t('common.available')}>
                  <strong className="font-heading text-lg tabular-nums">{formatBytes(worker.available)}</strong>
                </SummaryTile>
              </section>
        
              <section className="grid gap-4 xl:grid-cols-2">
                <Card size="sm">
                  <CardHeader>
                    <CardTitle>{t('workers.detail.runtime.title')}</CardTitle>
                    <p className="text-xs text-muted-foreground">{t('workers.detail.runtime.subtitle')}</p>
                  </CardHeader>
                  <CardContent>
                    <DetailList>
                      <DetailRow label={t('workers.detail.runtime.hostname')}>{workerHostname(worker)}</DetailRow>
                      <DetailRow label={t('workers.detail.runtime.workerId')}>{workerId(worker)}</DetailRow>
                      <DetailRow label={t('workers.detail.runtime.rpcAddress')}>{workerAddress(worker)}</DetailRow>
                      <DetailRow label={t('workers.detail.runtime.webPort')}>
                        {worker.address?.web_port || '-'}
                      </DetailRow>
                      <DetailRow label={t('workers.detail.runtime.ip')}>{displayIp}</DetailRow>
                      <DetailRow label={t('workers.detail.runtime.heartbeat')}>
                        {formatTime(worker.last_update_ms || worker.last_update)}
                      </DetailRow>
                      <DetailRow label={t('workers.detail.runtime.version')}>
                        <span
                          className={versionIssue ? 'text-warning' : undefined}
                          title={versionIssue ? t('workers.detail.runtime.versionMismatch') : undefined}
                        >
                          {shortVersion || t('workers.detail.runtime.notReported')}
                        </span>
                      </DetailRow>
                      <DetailRow label={t('workers.detail.runtime.versionCommit')}>
                        {String(data?.version_commit || '').trim() || t('workers.detail.runtime.notReported')}
                      </DetailRow>
                      <DetailRow label={t('workers.detail.runtime.files')}>
                        {data?.file_count ?? t('workers.detail.runtime.notReported')}
                      </DetailRow>
                      <DetailRow label={t('workers.detail.runtime.blocks')}>{worker.block_num || 0}</DetailRow>
                    </DetailList>
                  </CardContent>
                </Card>
        
                <Card size="sm">
                  <CardHeader>
                    <CardTitle>{t('workers.detail.storage.title')}</CardTitle>
                    <p className="text-xs text-muted-foreground">{t('workers.detail.storage.subtitle')}</p>
                  </CardHeader>
                  <CardContent>
                    <DetailList>
                      <DetailRow label={t('workers.detail.storage.curvineUsed')}>
                        {formatBytes(worker.fs_used)}
                      </DetailRow>
                      <DetailRow label={t('workers.detail.storage.nonFsUsed')}>
                        {formatBytes(worker.non_fs_used)}
                      </DetailRow>
                      <DetailRow label={t('workers.detail.storage.reserved')}>
                        {formatBytes(worker.reserved_bytes)}
                      </DetailRow>
                      <DetailRow label={t('workers.detail.storage.storageDirs')}>
                        {storageItems.length}
                      </DetailRow>
                    </DetailList>
                  </CardContent>
                </Card>
              </section>
        
              <section className="flex flex-col gap-2">
                <div className="flex flex-col gap-0.5">
                  <h2 className="font-heading text-base font-medium">
                    {t('workers.detail.storage.directoriesTitle')}
                  </h2>
                  <p className="text-xs text-muted-foreground">
                    {t('workers.detail.storage.directoriesSubtitle')}
                  </p>
                </div>
        
                <ResourceTable
                  loading={loading}
                  error={null}
                  emptyTitle={t('workers.detail.storage.empty')}
                  columnWidths={['36%', '12%', '26%', '9%', '17%']}
                  columns={[
                    t('workers.detail.storage.directoriesTitle'),
                    t('workers.detail.storage.type'),
                    t('workers.list.headerUsage'),
                    t('workers.list.headerBlocks'),
                    t('workers.list.headerStatus')
                  ]}
                  rows={storageItems.map((item) => {
                    const itemUsage = usagePercent(item.capacity, item.available)
                    const StorageIcon = storageTypeIcon(item.storage_type)
                    return (
                      <TableRow key={item.storage_id}>
                        <TableCell>
                          <div className="flex w-full min-w-0 flex-col gap-0.5">
                            <strong className="truncate font-medium">{item.dir_path}</strong>
                            <span className="truncate font-mono text-xs text-muted-foreground">
                              {item.storage_id}
                            </span>
                          </div>
                        </TableCell>
                        <TableCell className="text-muted-foreground">
                          <span className="flex items-center gap-1.5">
                            <StorageIcon className="size-3.5 shrink-0" aria-hidden />
                            {item.storage_type || '-'}
                          </span>
                        </TableCell>
                        <TableCell>
                          <div className="flex w-full flex-col gap-1">
                            <div className="flex items-baseline justify-between gap-2 text-xs">
                              <strong className="tabular-nums">{itemUsage}%</strong>
                              <span className="text-muted-foreground tabular-nums">
                                {formatBytes(Number(item.capacity || 0) - Number(item.available || 0))}
                                {' / '}
                                {formatBytes(item.capacity)}
                              </span>
                            </div>
                            <Meter
                              percent={itemUsage}
                              tone={item.failed ? 'destructive' : itemUsage >= 90 ? 'destructive' : itemUsage >= 75 ? 'warning' : 'primary'}
                            />
                          </div>
                        </TableCell>
                        <TableCell className="tabular-nums">{item.block_num || 0}</TableCell>
                        <TableCell>
                          <StatusBadge
                            value={translateStatus(t, item.failed ? 'failed' : 'healthy')}
                            tone={item.failed ? 'negative' : 'positive'}
                          />
                        </TableCell>
                      </TableRow>
                    )
                  })}
                />
              </section>
          </div>
        </SheetContent>
      </Sheet>

      <ConfirmDialog
        open={pendingAction !== null}
        title={translateWorkerAction(t, pendingAction ?? '')}
        description={t('workers.actions.confirm', {
          action: translateWorkerAction(t, pendingAction ?? ''),
          worker: address
        })}
        confirmLabel={t('common.confirm')}
        cancelLabel={t('common.cancel')}
        pending={submitting}
        destructive={pendingAction === 'decommission' || pendingAction === 'blacklist'}
        onOpenChange={(open) => {
          if (!open) setPendingAction(null)
        }}
        onConfirm={() => void confirmAction()}
        testId="worker-detail-action-confirm"
      />
    </>
  )
}

function SummaryTile({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <Card size="sm" className="gap-1.5">
      <div className="flex flex-col gap-1.5 px-(--card-spacing)">
        <span className="text-xs font-medium text-muted-foreground">{label}</span>
        <div>{children}</div>
      </div>
    </Card>
  )
}
