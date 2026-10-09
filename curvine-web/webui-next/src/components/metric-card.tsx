import { Card } from '@/components/ui/card'
import { cn } from '@/lib/utils'

export type MetricTone = 'good' | 'warn' | 'bad' | 'neutral'

const toneClasses: Record<MetricTone, string> = {
  good: 'text-success',
  warn: 'text-warning',
  bad: 'text-destructive',
  neutral: 'text-muted-foreground'
}

export function MetricCard({
  label,
  value,
  meta,
  delta,
  tone = 'neutral',
  wrapValue = false,
  className
}: {
  label: string
  value: string | number
  meta?: string
  delta?: string
  tone?: MetricTone
  wrapValue?: boolean
  className?: string
}) {
  return (
    <Card size="sm" className={cn('gap-2', className)} data-testid="metric-card">
      <div className="flex flex-col gap-1.5 px-(--card-spacing)">
        <span className="text-xs font-medium text-muted-foreground">{label}</span>
        <strong
          className={cn(
            'font-heading font-semibold tracking-tight tabular-nums',
            wrapValue ? 'text-base leading-snug break-words' : 'truncate text-2xl leading-none'
          )}
        >
          {value}
        </strong>
      </div>
      {(meta || delta) && (
        <div className="flex items-baseline justify-between gap-2 px-(--card-spacing) text-xs">
          <small className="min-w-0 truncate text-muted-foreground">{meta}</small>
          {delta && (
            <em className={cn('shrink-0 font-semibold not-italic', toneClasses[tone])}>{delta}</em>
          )}
        </div>
      )}
    </Card>
  )
}
