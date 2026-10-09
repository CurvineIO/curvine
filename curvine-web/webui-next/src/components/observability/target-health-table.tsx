/**
 * TargetHealthTable — React 18 + TypeScript port of
 * webui/src/components/observability/TargetHealthTable.vue.
 *
 * Behaviour-preserving: same props (targets / loading / error / subtitle), the
 * same derived helpers (`workerIdLabel`, `workerStateTone`, `formatTs`,
 * `formatRelative`, `successAgeClass`) and the same error / loading / empty /
 * table states. The Vue markup used hard-coded English literals and introduced
 * no i18n keys, so those strings are preserved verbatim.
 */
import type { TargetHealth } from '@/api/types'
import { EmptyNotice, ErrorAlert } from '@/components/data-state'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/components/ui/table'
import { cn } from '@/lib/utils'

export interface TargetHealthTableProps {
  targets?: TargetHealth[]
  loading?: boolean
  error?: string
  subtitle?: string
}

function workerIdLabel(target: TargetHealth): string {
  return target.worker_id != null ? `ID ${target.worker_id}` : '—'
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

function formatTs(value: number | null | undefined): string {
  if (!value) return '-'
  const date = new Date(Number(value))
  if (Number.isNaN(date.getTime())) return '-'
  return date.toLocaleString()
}

function formatRelative(value: number | null | undefined): string {
  if (!value) return 'never'
  const delta = Date.now() - Number(value)
  if (!Number.isFinite(delta) || delta < 0) return formatTs(value)
  if (delta < 60_000) return `${Math.max(1, Math.round(delta / 1000))}s ago`
  if (delta < 3_600_000) return `${Math.round(delta / 60_000)}m ago`
  if (delta < 86_400_000) return `${Math.round(delta / 3_600_000)}h ago`
  return `${Math.round(delta / 86_400_000)}d ago`
}

function successAgeClass(target: TargetHealth): string {
  if (!target.last_success_ms) return 'text-destructive'
  if (
    !target.up &&
    target.last_scrape_ms &&
    target.last_success_ms < target.last_scrape_ms - 30_000
  ) {
    return 'text-destructive'
  }
  return ''
}

const SCRAPE_PILL_CLASS = {
  up: 'border-success/25 bg-success/10 text-success',
  down: 'border-destructive/20 bg-destructive/10 text-destructive'
} as const

export function TargetHealthTable({
  targets = [],
  loading = false,
  error = '',
  subtitle = 'Scrape target health from built-in collector discovery'
}: TargetHealthTableProps) {
  return (
    <Card className="min-w-0" data-testid="target-health-table">
      <CardHeader>
        <CardTitle className="font-heading">Data Sources</CardTitle>
        <CardDescription>{subtitle}</CardDescription>
      </CardHeader>

      <CardContent className="flex flex-col gap-3">
        {error ? (
          <ErrorAlert message={error} />
        ) : loading ? (
          <p
            className="py-6 text-center text-sm text-muted-foreground"
            data-testid="target-health-table-loading"
          >
            Loading scrape targets…
          </p>
        ) : !targets.length ? (
          <EmptyNotice title="No scrape targets discovered yet." />
        ) : (
          <Table className="table-fixed">
            {/* Widths sum to exactly 100%: with `table-fixed` an over-declared row is
                the one thing that still widens the table past its container, and the
                page then scrolls sideways. */}
            <TableHeader>
              <TableRow>
                <TableHead className="w-[11%]">ID</TableHead>
                <TableHead className="w-[8%]">Component</TableHead>
                <TableHead className="w-[13%]">Instance</TableHead>
                <TableHead className="w-[7%]">Scrape</TableHead>
                <TableHead className="w-[9%]">Worker</TableHead>
                <TableHead className="w-[9%]">Last Scrape</TableHead>
                <TableHead className="w-[9%]">Last Success</TableHead>
                <TableHead className="w-[15%]">Warning / Error</TableHead>
                <TableHead className="w-[8%]">Samples / Parse Errors</TableHead>
                <TableHead className="w-[11%]">URL</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {targets.map((target) => (
                <TableRow
                  key={target.id}
                  data-testid={`target-health-row-${target.id}`}
                  className={cn(!target.up && 'bg-destructive/5')}
                >
                  <TableCell>
                    <strong className="block truncate font-medium" title={target.id}>
                      {target.id}
                    </strong>
                  </TableCell>
                  <TableCell>
                    <span className="block truncate">{target.component}</span>
                  </TableCell>
                  <TableCell>
                    <span className="block truncate" title={target.instance}>
                      {target.instance}
                    </span>
                  </TableCell>
                  <TableCell>
                    <Badge
                      variant="outline"
                      className={cn(
                        'rounded-md',
                        target.up ? SCRAPE_PILL_CLASS.up : SCRAPE_PILL_CLASS.down
                      )}
                    >
                      {target.up ? 'Up' : 'Down'}
                    </Badge>
                  </TableCell>
                  <TableCell>
                    {target.component === 'worker' ? (
                      <span className="flex flex-col gap-1">
                        <span className="text-[11px] text-muted-foreground">
                          {workerIdLabel(target)}
                        </span>
                        <Badge
                          variant="outline"
                          className={cn(
                            'w-fit rounded-md',
                            WORKER_STATE_TONE_CLASS[workerStateTone(target.worker_state)]
                          )}
                        >
                          {target.worker_state || 'unknown'}
                        </Badge>
                      </span>
                    ) : (
                      <span className="text-xs text-muted-foreground">—</span>
                    )}
                  </TableCell>
                  <TableCell>
                    <span className="tabular-nums" title={formatTs(target.last_scrape_ms)}>
                      {formatRelative(target.last_scrape_ms)}
                    </span>
                  </TableCell>
                  <TableCell>
                    <span
                      className={cn('tabular-nums', successAgeClass(target))}
                      title={formatTs(target.last_success_ms)}
                    >
                      {formatRelative(target.last_success_ms)}
                    </span>
                  </TableCell>
                  <TableCell>
                    <span className="flex flex-col gap-0.5">
                      {target.last_warning && (
                        <span className="block truncate text-xs text-warning" title={target.last_warning}>
                          {target.last_warning}
                        </span>
                      )}
                      {target.last_error && (
                        <span
                          className="truncate text-xs text-destructive"
                          title={target.last_error}
                        >
                          {target.last_error}
                        </span>
                      )}
                      {!target.last_warning && !target.last_error && (
                        <span className="text-xs text-muted-foreground">—</span>
                      )}
                    </span>
                  </TableCell>
                  <TableCell>
                    <span className="tabular-nums">
                      samples {target.sample_count} / parse errors {target.parse_error_count ?? 0}
                    </span>
                  </TableCell>
                  <TableCell>
                    <span className="block truncate" title={target.url}>
                      {target.url}
                    </span>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  )
}
