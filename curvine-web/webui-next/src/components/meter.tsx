import { cn } from '@/lib/utils'

export type MeterTone = 'primary' | 'warning' | 'destructive'

const toneClasses: Record<MeterTone, string> = {
  primary: 'bg-primary',
  warning: 'bg-warning',
  destructive: 'bg-destructive'
}

export function clampPercent(value: number): number {
  if (!Number.isFinite(value)) return 0
  return Math.max(0, Math.min(100, Math.round(value)))
}

export function Meter({
  percent,
  tone = 'primary',
  className
}: {
  percent: number
  tone?: MeterTone
  className?: string
}) {
  return (
    <div className={cn('h-1.5 w-full overflow-hidden rounded-full bg-muted', className)}>
      <div
        className={cn('h-full rounded-full transition-all', toneClasses[tone])}
        style={{ width: `${clampPercent(percent)}%` }}
      />
    </div>
  )
}

/** Percent used for a capacity/available pair; 0 when capacity is unknown. */
export function usagePercent(capacity: unknown, available: unknown): number {
  const total = Number(capacity || 0)
  if (!total) return 0
  const used = total - Number(available || 0)
  return clampPercent((used / total) * 100)
}
