import { useCallback, useEffect, useMemo, useState } from 'react'
import { ArrowDown, ArrowUp, ArrowUpDown, Search, X } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { api } from '@/lib/api'
import { useAutoRefreshEffect } from '@/lib/auto-refresh'
import { formatGB, formatTimestamp } from '@/lib/format'
import type { WorkerInfo, WorkersResponse } from '@/lib/types'
import { cn } from '@/lib/utils'

type SortField =
  | 'status'
  | 'worker_id'
  | 'hostname'
  | 'ip_addr'
  | 'heartbeat'
  | 'capacity'
  | 'available'
  | 'used'
  | 'usage'

type SortDirection = 'asc' | 'desc'

const COLUMNS: { field: SortField; label: string; className?: string }[] = [
  { field: 'status', label: 'Status', className: 'w-16 text-center' },
  { field: 'worker_id', label: 'Worker ID' },
  { field: 'hostname', label: 'Hostname' },
  { field: 'ip_addr', label: 'IP address' },
  { field: 'heartbeat', label: 'Last heartbeat' },
  { field: 'capacity', label: 'Capacity', className: 'text-right' },
  { field: 'available', label: 'Available', className: 'text-right' },
  { field: 'used', label: 'Used', className: 'text-right' },
  { field: 'usage', label: 'Usage' },
]

function usagePercent(worker: WorkerInfo): number {
  if (!worker.capacity) {
    return 0
  }
  return Math.round((worker.fs_used / worker.capacity) * 100)
}

// /api/workers lists blacklisted and decommissioning workers with the live
// ones (they still send heartbeats); the Overview counts only Live workers.
const STATUS_LABELS: Record<string, string> = {
  Live: 'Live',
  Blacklist: 'Blacklisted',
  Decommission: 'Decommissioning',
  Lost: 'Lost',
}

function workerStatus(worker: WorkerInfo): string {
  return worker.status || 'Live'
}

function statusLabel(worker: WorkerInfo): string {
  const status = workerStatus(worker)
  return STATUS_LABELS[status] ?? status
}

function countByStatus(workers: WorkerInfo[], status: string): number {
  return workers.filter(worker => workerStatus(worker) === status).length
}

function sortValue(worker: WorkerInfo, field: SortField): string | number {
  switch (field) {
    case 'status':
      return worker.status || ''
    case 'worker_id':
      return worker.address.worker_id
    case 'hostname':
      return worker.address.hostname || ''
    case 'ip_addr':
      return worker.address.ip_addr || ''
    case 'heartbeat':
      return worker.last_update || 0
    case 'capacity':
      return worker.capacity || 0
    case 'available':
      return worker.available || 0
    case 'used':
      return worker.fs_used || 0
    case 'usage':
      return usagePercent(worker)
    default:
      return ''
  }
}

function totalOf(workers: WorkerInfo[], pick: (worker: WorkerInfo) => number): number {
  return workers.reduce((total, worker) => total + pick(worker), 0)
}

export function WorkersPage() {
  const [data, setData] = useState<WorkersResponse | null>(null)
  const [search, setSearch] = useState('')
  const [sortField, setSortField] = useState<SortField | null>(null)
  const [sortDirection, setSortDirection] = useState<SortDirection>('asc')

  const load = useCallback(() => {
    api.workers().then(setData).catch(() => {})
  }, [])

  useEffect(() => {
    load()
  }, [load])
  useAutoRefreshEffect(load)

  const filterAndSort = useCallback(
    (workers: WorkerInfo[]) => {
      const query = search.trim().toLowerCase()
      const filtered = query
        ? workers.filter(
            worker =>
              String(worker.address.worker_id).toLowerCase().includes(query) ||
              worker.address.hostname.toLowerCase().includes(query) ||
              worker.address.ip_addr.toLowerCase().includes(query),
          )
        : workers
      if (!sortField) {
        return filtered
      }
      return [...filtered].sort((a, b) => {
        const left = sortValue(a, sortField)
        const right = sortValue(b, sortField)
        const comparison =
          typeof left === 'string' && typeof right === 'string'
            ? left.localeCompare(right)
            : Number(left) - Number(right)
        return sortDirection === 'asc' ? comparison : -comparison
      })
    },
    [search, sortField, sortDirection],
  )

  const liveWorkers = useMemo(() => filterAndSort(data?.live_workers ?? []), [data, filterAndSort])
  const lostWorkers = useMemo(() => filterAndSort(data?.lost_workers ?? []), [data, filterAndSort])
  const totalFound = liveWorkers.length + lostWorkers.length

  const toggleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(direction => (direction === 'asc' ? 'desc' : 'asc'))
      return
    }
    setSortField(field)
    setSortDirection('asc')
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardContent className="flex flex-wrap items-center gap-4 py-4">
          <div className="relative min-w-64 flex-1">
            <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <input
              value={search}
              onChange={event => setSearch(event.target.value)}
              placeholder="Search workers by ID, hostname, or IP address..."
              className="h-10 w-full rounded-md border bg-secondary/40 pl-9 pr-9 font-mono text-sm outline-none placeholder:text-muted-foreground/70 focus:border-sky-500/60 focus:ring-2 focus:ring-sky-500/20"
            />
            {search && (
              <button
                type="button"
                aria-label="Clear search"
                className="absolute right-2 top-1/2 -translate-y-1/2 rounded p-1 text-muted-foreground hover:bg-accent hover:text-foreground"
                onClick={() => setSearch('')}
              >
                <X className="h-4 w-4" />
              </button>
            )}
          </div>
          <span className="text-xs uppercase tracking-widest text-muted-foreground">
            {totalFound} workers found
          </span>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex-row flex-wrap items-center justify-between gap-4 border-b">
          <CardTitle className="flex items-center gap-3 text-base">
            <span className="relative flex h-2.5 w-2.5">
              <span className="absolute inline-flex h-full w-full motion-safe:animate-ping rounded-full bg-emerald-400 opacity-60" />
              <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-emerald-400" />
            </span>
            Live workers
            <Badge variant="info" title="Workers in the Live state, as counted on the Overview page">
              {countByStatus(liveWorkers, 'Live')}
            </Badge>
            {countByStatus(liveWorkers, 'Blacklist') > 0 && (
              <Badge variant="warning">{countByStatus(liveWorkers, 'Blacklist')} blacklisted</Badge>
            )}
            {countByStatus(liveWorkers, 'Decommission') > 0 && (
              <Badge variant="warning">{countByStatus(liveWorkers, 'Decommission')} decommissioning</Badge>
            )}
          </CardTitle>
          <div className="flex gap-6">
            <HeaderStat label="Total capacity" value={`${formatGB(totalOf(liveWorkers, w => w.capacity))}GB`} />
            <HeaderStat
              label="Available"
              value={`${formatGB(totalOf(liveWorkers, w => w.available))}GB`}
              valueClassName="text-emerald-400"
            />
          </div>
        </CardHeader>
        <CardContent className="p-0">
          {liveWorkers.length > 0 ? (
            <WorkerTable
              workers={liveWorkers}
              live
              sortField={sortField}
              sortDirection={sortDirection}
              onSort={toggleSort}
            />
          ) : (
            <EmptyState icon="📡" text="No live workers found" />
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex-row flex-wrap items-center justify-between gap-4 border-b">
          <CardTitle className="flex items-center gap-3 text-base">
            <span className="inline-flex h-2.5 w-2.5 rounded-full bg-red-400" />
            Lost workers
            <Badge variant="danger">{lostWorkers.length}</Badge>
          </CardTitle>
          {lostWorkers.length > 0 && (
            <HeaderStat
              label="Lost capacity"
              value={`${formatGB(totalOf(lostWorkers, w => w.capacity))}GB`}
              valueClassName="text-red-400"
            />
          )}
        </CardHeader>
        <CardContent className="p-0">
          {lostWorkers.length > 0 ? (
            <WorkerTable
              workers={lostWorkers}
              live={false}
              sortField={sortField}
              sortDirection={sortDirection}
              onSort={toggleSort}
            />
          ) : (
            <EmptyState icon="✅" text="No lost workers - All systems operational" />
          )}
        </CardContent>
      </Card>
    </div>
  )
}

function HeaderStat({
  label,
  value,
  valueClassName,
}: {
  label: string
  value: string
  valueClassName?: string
}) {
  return (
    <div className="text-right">
      <div className="text-xs uppercase tracking-widest text-muted-foreground">{label}</div>
      <div className={cn('font-mono text-lg font-semibold text-sky-400', valueClassName)}>{value}</div>
    </div>
  )
}

function WorkerTable({
  workers,
  live,
  sortField,
  sortDirection,
  onSort,
}: {
  workers: WorkerInfo[]
  live: boolean
  sortField: SortField | null
  sortDirection: SortDirection
  onSort: (field: SortField) => void
}) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[900px] text-sm">
        <thead>
          <tr className="border-b bg-secondary/40">
            {COLUMNS.map(column => {
              const active = sortField === column.field
              const SortIcon = active ? (sortDirection === 'asc' ? ArrowUp : ArrowDown) : ArrowUpDown
              return (
                <th
                  key={column.field}
                  className={cn('px-4 py-3 text-left text-xs uppercase tracking-wider text-muted-foreground', column.className)}
                >
                  <button
                    type="button"
                    className="inline-flex items-center gap-1.5 hover:text-foreground"
                    onClick={() => onSort(column.field)}
                  >
                    {column.label}
                    <SortIcon className={cn('h-3 w-3', active ? 'text-sky-400' : 'text-muted-foreground/50')} />
                  </button>
                </th>
              )
            })}
          </tr>
        </thead>
        <tbody>
          {workers.map(worker => {
            const usage = usagePercent(worker)
            const healthy = live && workerStatus(worker) === 'Live'
            return (
              <tr key={worker.address.worker_id} className="border-b last:border-b-0 hover:bg-accent/40">
                <td className="px-4 py-3" title={statusLabel(worker)}>
                  <span className="flex justify-center">
                    <span className="relative flex h-3 w-3">
                      {healthy && (
                        <span className="absolute inline-flex h-full w-full motion-safe:animate-ping rounded-full bg-emerald-400 opacity-60" />
                      )}
                      <span
                        className={cn(
                          'relative inline-flex h-3 w-3 rounded-full',
                          healthy ? 'bg-emerald-400' : live ? 'bg-amber-400' : 'bg-red-400',
                        )}
                      />
                    </span>
                  </span>
                </td>
                <td className="px-4 py-3 font-mono font-semibold">{worker.address.worker_id}</td>
                <td className="whitespace-nowrap px-4 py-3">
                  <span className="font-mono text-sky-400">{worker.address.hostname}</span>
                  <span className="font-mono text-xs text-muted-foreground">:{worker.address.rpc_port}</span>
                  {live && !healthy && (
                    <Badge variant="warning" className="ml-2 px-1.5 py-0 text-[10px] uppercase tracking-wide">
                      {statusLabel(worker)}
                    </Badge>
                  )}
                </td>
                <td className="px-4 py-3 font-mono text-sm">{worker.address.ip_addr}</td>
                <td className="px-4 py-3 font-mono text-xs text-muted-foreground">
                  {formatTimestamp(worker.last_update)}
                </td>
                <td className="px-4 py-3 text-right font-mono">{formatGB(worker.capacity)}GB</td>
                <td className="px-4 py-3 text-right font-mono text-emerald-400">
                  {formatGB(worker.available)}GB
                </td>
                <td className="px-4 py-3 text-right font-mono text-amber-400">{formatGB(worker.fs_used)}GB</td>
                <td className="px-4 py-3">
                  <div className="flex items-center gap-2">
                    <div className="h-1.5 w-full min-w-10 overflow-hidden rounded-full bg-border">
                      <div
                        className="h-full rounded-full bg-gradient-to-r from-emerald-400 via-amber-400 to-red-400 transition-[width] duration-300"
                        style={{ width: `${Math.min(100, usage)}%` }}
                      />
                    </div>
                    <span className="w-9 shrink-0 text-right font-mono text-xs text-muted-foreground">
                      {usage}%
                    </span>
                  </div>
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function EmptyState({ icon, text }: { icon: string; text: string }) {
  return (
    <div className="flex flex-col items-center gap-3 px-6 py-16 text-center">
      <span className="text-4xl grayscale">{icon}</span>
      <span className="text-sm text-muted-foreground">{text}</span>
    </div>
  )
}
