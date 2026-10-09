/**
 * DashboardDataSourcesTable — React 18 + TypeScript port of
 * webui/src/components/observability/DashboardDataSourcesTable.vue.
 *
 * Behaviour-preserving: same props (title / subtitle / rows / loading / error /
 * embedded), the same `workerIdLabel` / `workerStateTone` helpers and the same
 * heading / error / loading / empty / table states. The Vue markup used
 * hard-coded English literals and introduced no i18n keys, so those strings are
 * preserved verbatim.
 */
import type { DashboardTableRow } from '@/api/types'
import { EmptyNotice, ErrorAlert } from '@/components/data-state'
import { Badge } from '@/components/ui/badge'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/components/ui/table'
import { cn } from '@/lib/utils'

export interface DashboardDataSourcesTableProps {
  title?: string
  subtitle?: string
  rows?: DashboardTableRow[]
  loading?: boolean
  error?: string
  embedded?: boolean
}

function workerIdLabel(row: DashboardTableRow): string {
  return row.worker_id != null ? `ID ${row.worker_id}` : '—'
}

function workerStateTone(state: string | null | undefined): string {
  const normalized = String(state || '').toLowerCase()
  if (normalized === 'live') return 'live'
  if (normalized === 'lost') return 'lost'
  if (normalized === 'blacklist') return 'warning'
  if (normalized === 'decommission') return 'waiting'
  return 'neutral'
}

const WORKER_STATE_TONE_CLASS: Record<string, string> = {
  live: 'border-success/25 bg-success/10 text-success',
  lost: 'border-destructive/20 bg-destructive/10 text-destructive',
  warning: 'border-warning/30 bg-warning/10 text-warning',
  waiting: 'border-info/25 bg-info/10 text-info',
  neutral: 'border-border bg-muted/60 text-muted-foreground'
}

const SCRAPE_PILL_CLASS = {
  up: 'border-success/25 bg-success/10 text-success',
  down: 'border-destructive/20 bg-destructive/10 text-destructive'
} as const

export function DashboardDataSourcesTable({
  title = 'Data Sources',
  subtitle = '',
  rows = [],
  loading = false,
  error = '',
  embedded = false
}: DashboardDataSourcesTableProps) {
  return (
    <div
      className={cn(
        'min-w-0',
        embedded ? 'p-0' : 'rounded-xl border border-border bg-card p-4'
      )}
      data-testid="dashboard-data-sources-table"
    >
      {!embedded ? (
        <div className="mb-3 flex flex-col gap-0.5">
          <h2 className="font-heading text-base font-semibold">{title}</h2>
          <p className="text-sm text-muted-foreground">{subtitle}</p>
        </div>
      ) : subtitle ? (
        <div className="mb-2.5 text-sm text-muted-foreground" data-testid="dashboard-data-sources-subtitle">
          {subtitle}
        </div>
      ) : null}

      {error ? (
        <ErrorAlert message={error} />
      ) : loading ? (
        <p
          className="py-6 text-center text-sm text-muted-foreground"
          data-testid="dashboard-data-sources-loading"
        >
          Loading dashboard data sources…
        </p>
      ) : !rows.length ? (
        <EmptyNotice title="No scrape targets in dashboard payload." />
      ) : (
        <Table className="table-fixed">
          <TableHeader>
            <TableRow>
              <TableHead className="w-[18%]">ID</TableHead>
              <TableHead className="w-[10%]">Component</TableHead>
              <TableHead className="w-[24%]">Instance</TableHead>
              <TableHead className="w-[8%]">Scrape</TableHead>
              <TableHead className="w-[14%]">Worker</TableHead>
              <TableHead className="w-[26%]">Detail</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row) => (
              <TableRow
                key={row.id}
                data-testid={`dashboard-data-source-row-${row.id}`}
                className={cn(!row.scrape_up && 'bg-destructive/5')}
              >
                <TableCell>
                  <strong className="block truncate font-medium" title={row.id}>
                    {row.id}
                  </strong>
                </TableCell>
                <TableCell>{row.component}</TableCell>
                <TableCell>
                  <span className="block truncate" title={row.instance}>
                    {row.instance}
                  </span>
                </TableCell>
                <TableCell>
                  <Badge
                    variant="outline"
                    className={cn(
                      'rounded-md',
                      row.scrape_up ? SCRAPE_PILL_CLASS.up : SCRAPE_PILL_CLASS.down
                    )}
                  >
                    {row.scrape_up ? 'Up' : 'Down'}
                  </Badge>
                </TableCell>
                <TableCell>
                  {row.component === 'worker' ? (
                    <span className="flex flex-col gap-1">
                      <span className="text-[11px] text-muted-foreground">
                        {workerIdLabel(row)}
                      </span>
                      <Badge
                        variant="outline"
                        className={cn(
                          'w-fit rounded-md',
                          WORKER_STATE_TONE_CLASS[workerStateTone(row.worker_state)]
                        )}
                      >
                        {row.worker_state || 'unknown'}
                      </Badge>
                    </span>
                  ) : (
                    <span className="text-xs text-muted-foreground">—</span>
                  )}
                </TableCell>
                <TableCell>
                  {row.detail ? (
                    <span className="block truncate text-xs text-foreground" title={row.detail}>
                      {row.detail}
                    </span>
                  ) : (
                    <span className="text-xs text-muted-foreground">—</span>
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </div>
  )
}
