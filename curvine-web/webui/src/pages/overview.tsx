import { useCallback, useEffect, useState, type ReactNode } from 'react'
import { Activity, HardDrive, Monitor, PieChart } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { api } from '@/lib/api'
import { useAutoRefreshEffect } from '@/lib/auto-refresh'
import { formatCount, formatDate, formatGB } from '@/lib/format'
import { masterStartMillis, uptimeText } from '@/lib/uptime'
import type { Overview } from '@/lib/types'
import { cn } from '@/lib/utils'

const RING_RADIUS = 45
const RING_CIRCUMFERENCE = 2 * Math.PI * RING_RADIUS

function versionLabel(version: string | undefined): string {
  if (!version || version === 'unknown') {
    return 'Unknown'
  }
  return `v${version.replace(/^v/, '')}`
}

function versionTitle(data: Overview | null): string {
  const parts: string[] = []
  if (data?.git_commit && data.git_commit !== 'unknown') {
    parts.push(`commit ${data.git_commit}`)
  }
  if (data?.git_branch && data.git_branch !== 'unknown') {
    parts.push(`branch ${data.git_branch}`)
  }
  return parts.join(', ')
}

export function OverviewPage() {
  const [data, setData] = useState<Overview | null>(null)

  const load = useCallback(() => {
    api.overview().then(setData).catch(() => {})
  }, [])

  useEffect(() => {
    load()
  }, [load])
  useAutoRefreshEffect(load)

  const active = data?.master_state === 'Active'
  const liveWorkers = data?.live_workers ?? 0
  const lostWorkers = data?.lost_workers ?? 0
  const totalWorkers = liveWorkers + lostWorkers
  const workerPercent = (count: number) =>
    totalWorkers === 0 ? 0 : Math.round((count / totalWorkers) * 100)

  const capacity = data?.capacity ?? 0
  const available = data?.available ?? 0
  const fsUsed = data?.fs_used ?? 0
  const usedPercent =
    capacity === 0 ? 0 : Math.min(100, Math.max(0, Math.round(((capacity - available) / capacity) * 100)))
  const nonFsUsed = capacity === 0 ? 0 : Math.max(0, capacity - available - fsUsed)
  const dashOffset = RING_CIRCUMFERENCE - (usedPercent / 100) * RING_CIRCUMFERENCE

  return (
    <div className="space-y-6">
      <Card>
        <CardContent className="flex flex-wrap items-center justify-between gap-6 py-6">
          <div className="flex items-center gap-4">
            <span className="relative flex h-4 w-4">
              {active && (
                <span className="absolute inline-flex h-full w-full motion-safe:animate-ping rounded-full bg-emerald-400 opacity-60" />
              )}
              <span
                className={cn(
                  'relative inline-flex h-4 w-4 rounded-full',
                  active ? 'bg-emerald-400' : 'bg-muted-foreground',
                )}
              />
            </span>
            <div>
              <p className="text-xs uppercase tracking-widest text-muted-foreground">Master state</p>
              <p className="text-lg font-semibold">{data?.master_state || 'Unknown'}</p>
              <p className="font-mono text-xs text-muted-foreground">ID: {data?.cluster_id || 'N/A'}</p>
            </div>
          </div>
          <div className="text-right">
            <p className="text-xs uppercase tracking-widest text-muted-foreground">Uptime</p>
            <p className="font-mono text-2xl font-semibold text-sky-400">
              {uptimeText(masterStartMillis(data))}
            </p>
          </div>
        </CardContent>
      </Card>

      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-sm uppercase tracking-widest text-muted-foreground">
              <Monitor className="h-4 w-4 text-sky-400" />
              System information
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <InfoRow label="Master address" value={data?.master_addr || 'N/A'} />
            <InfoRow
              label="Version"
              value={
                <Badge variant="secondary" title={versionTitle(data)}>
                  {versionLabel(data?.version)}
                </Badge>
              }
            />
            <InfoRow label="Started" value={formatDate(masterStartMillis(data))} />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-sm uppercase tracking-widest text-muted-foreground">
              <Activity className="h-4 w-4 text-sky-400" />
              Workers status
            </CardTitle>
          </CardHeader>
          <CardContent className="grid grid-cols-3 gap-3">
            <WorkerStat
              value={liveWorkers}
              label="Running"
              percent={workerPercent(liveWorkers)}
              barClassName="bg-emerald-400"
              valueClassName="text-emerald-400"
            />
            <WorkerStat
              value={lostWorkers}
              label="Lost"
              percent={workerPercent(lostWorkers)}
              barClassName="bg-red-400"
              valueClassName="text-red-400"
            />
            <WorkerStat
              value={`${formatGB(capacity)}GB`}
              label="Total capacity"
              percent={100}
              barClassName="bg-sky-400"
              valueClassName="text-sky-400"
            />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-sm uppercase tracking-widest text-muted-foreground">
              <HardDrive className="h-4 w-4 text-sky-400" />
              Storage metrics
            </CardTitle>
          </CardHeader>
          <CardContent className="grid grid-cols-2 gap-3">
            <StorageStat icon="📁" value={formatCount(data?.files_total)} label="Total files" />
            <StorageStat icon="📂" value={formatCount(data?.dir_total)} label="Directories" />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-sm uppercase tracking-widest text-muted-foreground">
              <PieChart className="h-4 w-4 text-sky-400" />
              Capacity utilization
            </CardTitle>
          </CardHeader>
          <CardContent className="flex flex-wrap items-center gap-8">
            <div className="relative h-36 w-36 shrink-0">
              <svg viewBox="0 0 100 100" className="h-full w-full -rotate-90">
                <circle
                  cx="50"
                  cy="50"
                  r={RING_RADIUS}
                  fill="none"
                  strokeWidth="8"
                  className="stroke-border"
                />
                <circle
                  cx="50"
                  cy="50"
                  r={RING_RADIUS}
                  fill="none"
                  strokeWidth="8"
                  strokeLinecap="round"
                  strokeDasharray={RING_CIRCUMFERENCE}
                  strokeDashoffset={dashOffset}
                  className="stroke-sky-400 transition-[stroke-dashoffset] duration-500"
                />
              </svg>
              <div className="absolute inset-0 flex flex-col items-center justify-center">
                <span className="font-mono text-2xl font-semibold text-sky-400">{usedPercent}%</span>
                <span className="text-xs uppercase text-muted-foreground">Total used</span>
              </div>
            </div>
            <div className="min-w-48 flex-1">
              <DetailRow label="Available" value={`${formatGB(available)}GB`} valueClassName="text-emerald-400" />
              <DetailRow label="FS used" value={`${formatGB(fsUsed, 4)}GB`} valueClassName="text-red-400" />
              <DetailRow label="System used" value={`${formatGB(nonFsUsed)}GB`} valueClassName="text-sky-400" />
              <DetailRow
                label="Reserved"
                value={`${formatGB(data?.reserved_bytes ?? 0, 4)}GB`}
                valueClassName="text-violet-400"
              />
              <DetailRow label="Total" value={`${formatGB(capacity)}GB`} valueClassName="text-sky-400" />
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

function InfoRow({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-4 rounded-lg border bg-secondary/40 px-4 py-3">
      <span className="text-xs uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className="text-right font-mono text-sm font-medium">{value}</span>
    </div>
  )
}

function WorkerStat({
  value,
  label,
  percent,
  barClassName,
  valueClassName,
}: {
  value: number | string
  label: string
  percent: number
  barClassName: string
  valueClassName: string
}) {
  return (
    <div className="rounded-lg border bg-secondary/40 p-4 text-center">
      <div className={cn('font-mono text-2xl font-bold', valueClassName)}>{value}</div>
      <div className="mb-3 text-xs uppercase tracking-wide text-muted-foreground">{label}</div>
      <div className="h-1 overflow-hidden rounded-full bg-border">
        <div className={cn('h-full transition-[width] duration-300', barClassName)} style={{ width: `${percent}%` }} />
      </div>
    </div>
  )
}

function StorageStat({ icon, value, label }: { icon: string; value: string; label: string }) {
  return (
    <div className="flex items-center gap-3 rounded-lg border bg-secondary/40 p-4">
      <span className="text-2xl grayscale">{icon}</span>
      <div>
        <div className="font-mono text-xl font-bold">{value}</div>
        <div className="text-xs uppercase tracking-wide text-muted-foreground">{label}</div>
      </div>
    </div>
  )
}

function DetailRow({
  label,
  value,
  valueClassName,
}: {
  label: string
  value: string
  valueClassName?: string
}) {
  return (
    <div className="flex items-center justify-between border-b py-2.5 last:border-b-0">
      <span className="text-xs uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className={cn('font-mono text-sm font-semibold', valueClassName)}>{value}</span>
    </div>
  )
}
