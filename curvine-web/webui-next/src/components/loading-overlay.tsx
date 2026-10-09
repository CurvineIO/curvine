import { Spinner } from '@/components/ui/spinner'
import { cn } from '@/lib/utils'

/** Inline overlay used while a page-level fetch is in flight and no data is shown yet. */
export function LoadingOverlay({ label, className }: { label?: string; className?: string }) {
  return (
    <div
      className={cn(
        'flex items-center justify-center gap-2 rounded-xl border border-dashed border-border bg-muted/30 p-6 text-sm text-muted-foreground',
        className
      )}
      data-testid="loading-overlay"
    >
      <Spinner className="size-4" />
      {label && <span>{label}</span>}
    </div>
  )
}
