import type { ReactNode } from 'react'

export function DetailList({ className, children }: { className?: string; children: ReactNode }) {
  return <dl className={className}>{children}</dl>
}

export function DetailRow({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="grid grid-cols-[minmax(6.5rem,10rem)_1fr] items-baseline gap-3 border-b border-border py-2 text-sm last:border-b-0">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="min-w-0 break-words">{children}</dd>
    </div>
  )
}
