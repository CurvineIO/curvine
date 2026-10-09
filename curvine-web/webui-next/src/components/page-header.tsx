export function PageHeader({
  eyebrow,
  title,
  description,
  actions
}: {
  eyebrow?: string
  title: string
  description?: string
  actions?: React.ReactNode
}) {
  return (
    <div className="flex flex-col gap-3 border-b border-border pb-4 md:flex-row md:items-end md:justify-between">
      <div className="flex min-w-0 flex-col gap-1">
        {eyebrow && (
          <p className="text-xs font-medium tracking-wider text-muted-foreground uppercase">
            {eyebrow}
          </p>
        )}
        <h1 className="font-heading text-xl font-semibold tracking-tight text-balance">{title}</h1>
        {description && <p className="max-w-3xl text-sm text-muted-foreground">{description}</p>}
      </div>
      {actions && <div className="flex shrink-0 flex-wrap items-center gap-2">{actions}</div>}
    </div>
  )
}
