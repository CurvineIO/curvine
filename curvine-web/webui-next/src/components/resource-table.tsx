import { Children, type ReactNode } from 'react'

import { EmptyNotice, ErrorAlert, TableSkeletonRows } from '@/components/data-state'
import { Badge } from '@/components/ui/badge'
import { Card } from '@/components/ui/card'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/components/ui/table'
import { cn } from '@/lib/utils'

export function ResourceTable({
  columns,
  columnWidths,
  rows,
  loading,
  error,
  emptyTitle,
  emptyDescription
}: {
  columns: ReactNode[]
  /**
   * Optional per-column widths as CSS widths (e.g. `22%`, `13rem`).
   *
   * Applied as an inline style, not a utility class: these are computed values,
   * and a dynamic `w-[${…}]` class would never survive Tailwind's static scan —
   * passing them as `className` silently did nothing and left `table-fixed`
   * dividing every column equally.
   *
   * Providing them switches the table to `table-fixed`, which makes the widths
   * authoritative: a long value then truncates inside its own column instead of
   * widening the table. Without them the layout stays automatic, where
   * `max-width` on a cell is ignored by the browser.
   */
  columnWidths?: (string | undefined)[]
  rows: ReactNode
  loading: boolean
  error: string | null
  emptyTitle: string
  emptyDescription?: string
}) {
  const tableClassName = cn(columnWidths && 'table-fixed')
  const header = (
    <TableHeader>
      <TableRow>
        {columns.map((column, index) => (
          <TableHead key={index} style={{ width: columnWidths?.[index] }}>
            {column}
          </TableHead>
        ))}
      </TableRow>
    </TableHeader>
  )

  if (error) {
    return <ErrorAlert message={error} />
  }
  if (loading) {
    return (
      <Card className="overflow-hidden py-0">
        <Table className={tableClassName}>
          {header}
          <TableBody>
            <TableSkeletonRows columns={columns.length} />
          </TableBody>
        </Table>
      </Card>
    )
  }
  if (Children.count(rows) === 0) {
    return <EmptyNotice title={emptyTitle} description={emptyDescription} />
  }
  return (
    <Card className="overflow-hidden py-0">
      <Table className={tableClassName}>
        {header}
        <TableBody>{rows}</TableBody>
      </Table>
    </Card>
  )
}

export type StatusTone = 'positive' | 'info' | 'warning' | 'negative' | 'neutral'

/**
 * Status string → tone. Extend by adding keys here only; never restyle call sites.
 * Keeps the Curvine vocabulary (worker states, job/transfer states, mount health,
 * diagnostic statuses) alongside the generic set.
 */
const statusTones: Record<string, StatusTone> = {
  // positive
  healthy: 'positive',
  ready: 'positive',
  active: 'positive',
  available: 'positive',
  succeeded: 'positive',
  success: 'positive',
  bound: 'positive',
  live: 'positive',
  pass: 'positive',
  completed: 'positive',
  running: 'positive',
  // info
  standby: 'info',
  dispatching: 'info',
  info: 'info',
  normal: 'info',
  // warning
  pending: 'warning',
  waiting: 'warning',
  cancelling: 'warning',
  degraded: 'warning',
  unverified: 'warning',
  deprecated: 'warning',
  warning: 'warning',
  blacklist: 'warning',
  attention: 'warning',
  // info
  decommission: 'info',
  decommissioning: 'info',
  // negative
  failed: 'negative',
  failure: 'negative',
  timed_out: 'negative',
  unavailable: 'negative',
  unhealthy: 'negative',
  denied: 'negative',
  lost: 'negative',
  suspended: 'negative',
  error: 'negative',
  critical: 'negative',
  expired: 'negative',
  // neutral
  draft: 'neutral',
  cancelled: 'neutral',
  canceled: 'neutral',
  unknown: 'neutral',
  skipped: 'neutral',
  none: 'neutral'
}

const statusToneClasses: Record<StatusTone, string> = {
  positive: 'border-success/25 bg-success/10 text-success dark:border-success/30 dark:bg-success/15',
  info: 'border-info/25 bg-info/10 text-info dark:border-info/30 dark:bg-info/15',
  warning: 'border-warning/30 bg-warning/10 text-warning dark:border-warning/30 dark:bg-warning/15',
  negative: 'border-destructive/20 bg-destructive/10 text-destructive dark:bg-destructive/20',
  neutral: 'border-border bg-muted/60 text-muted-foreground'
}

const statusDotClasses: Record<StatusTone, string> = {
  positive: 'bg-success',
  info: 'bg-info',
  warning: 'bg-warning',
  negative: 'bg-destructive',
  neutral: 'bg-muted-foreground'
}

export function statusTone(value: string | null | undefined): StatusTone {
  return statusTones[String(value ?? '').trim().toLowerCase()] ?? 'neutral'
}

/**
 * Worker state → tone, keyed on the raw backend state.
 *
 * Always pass this (rather than relying on the translated badge text) so the
 * colour survives localisation: `StatusBadge` infers the tone from its value,
 * which becomes e.g. "在线" in zh-CN and would otherwise fall back to neutral.
 */
export function workerStatusTone(status: string | null | undefined): StatusTone {
  switch (String(status ?? '').trim().toLowerCase()) {
    case 'live':
      return 'positive'
    case 'lost':
      return 'negative'
    case 'blacklist':
      return 'warning'
    case 'decommission':
      return 'info'
    default:
      return 'neutral'
  }
}

export function StatusBadge({
  value,
  className,
  tone
}: {
  value: string
  className?: string
  tone?: StatusTone
}) {
  const resolved = tone ?? statusTone(value)
  return (
    <Badge
      variant="outline"
      className={cn('gap-1.5', statusToneClasses[resolved], className)}
      data-tone={resolved}
    >
      <span aria-hidden className={cn('size-1.5 rounded-full', statusDotClasses[resolved])} />
      {value}
    </Badge>
  )
}

export function PrimaryCell({ title, subtitle }: { title: ReactNode; subtitle?: ReactNode }) {
  return (
    <div className="flex min-w-0 flex-col gap-0.5">
      <span className="truncate font-medium">{title}</span>
      {subtitle && (
        <span className="truncate font-mono text-xs text-muted-foreground">{subtitle}</span>
      )}
    </div>
  )
}

export function MonoText({
  children,
  className,
  title
}: {
  children: ReactNode
  className?: string
  title?: string
}) {
  return (
    <span className={cn('font-mono text-xs break-all', className)} title={title}>
      {children}
    </span>
  )
}

export function MutedLine({ children }: { children: ReactNode }) {
  return <span className="block text-xs text-muted-foreground">{children}</span>
}

export { TableCell, TableRow }
