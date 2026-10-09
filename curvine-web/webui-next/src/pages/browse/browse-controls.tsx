import { Fragment, useState } from 'react'
import { HomeIcon, MoreHorizontalIcon } from 'lucide-react'

import { SubmitButton, TextField } from '@/components/form-fields'
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator
} from '@/components/ui/breadcrumb'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger
} from '@/components/ui/dropdown-menu'
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle
} from '@/components/ui/sheet'
import { useT } from '@/i18n'

/**
 * Clickable directory trail. Every level is a jump target; the last segment is
 * the current directory and stays inert. Sits alongside the path input, which
 * still supports pasting an arbitrary path.
 *
 * Deep paths collapse: only the last `MAX_TAIL_LEVELS` levels stay visible and
 * the ones above them move into a menu behind an ellipsis. Rendering every level
 * would push the current directory off the end of a scrolled strip, which is the
 * part you actually need to see.
 */
const MAX_TAIL_LEVELS = 3

export function PathBreadcrumb({
  path,
  onNavigate
}: {
  path: string
  onNavigate(next: string): void
}) {
  const t = useT()
  const segments = String(path || '/').split('/').filter(Boolean)

  const collapsed = segments.length > MAX_TAIL_LEVELS
  const hiddenCount = collapsed ? segments.length - MAX_TAIL_LEVELS : 0
  const visibleCount = collapsed ? MAX_TAIL_LEVELS : segments.length

  const targetAt = (index: number) => `/${segments.slice(0, index + 1).join('/')}`

  return (
    <nav
      aria-label={t('browse.pathPlaceholder')}
      className="min-w-0 overflow-x-auto"
      data-testid="browse-breadcrumb"
    >
      <Breadcrumb>
        <BreadcrumbList className="flex-nowrap">
          <BreadcrumbItem>
            <BreadcrumbLink asChild>
              <button
                type="button"
                className="inline-flex items-center gap-1 rounded px-1 py-0.5 whitespace-nowrap transition-colors hover:bg-accent hover:text-accent-foreground"
                onClick={() => onNavigate('/')}
                data-testid="browse-crumb-root"
              >
                <HomeIcon className="size-3.5" />
                {t('common.root')}
              </button>
            </BreadcrumbLink>
          </BreadcrumbItem>

          {collapsed && (
            <>
              <BreadcrumbSeparator />
              <BreadcrumbItem>
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <button
                      type="button"
                      className="flex size-6 items-center justify-center rounded transition-colors hover:bg-accent hover:text-accent-foreground"
                      title={t('browse.breadcrumbHidden', { count: hiddenCount })}
                      aria-label={t('browse.breadcrumbHidden', { count: hiddenCount })}
                      data-testid="browse-crumb-hidden"
                    >
                      <MoreHorizontalIcon className="size-3.5" />
                    </button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="start">
                    {segments.slice(0, hiddenCount).map((segment, index) => (
                      <DropdownMenuItem
                        key={targetAt(index)}
                        onSelect={() => onNavigate(targetAt(index))}
                        data-testid={`browse-crumb-hidden-${index}`}
                      >
                        {segment}
                      </DropdownMenuItem>
                    ))}
                  </DropdownMenuContent>
                </DropdownMenu>
              </BreadcrumbItem>
            </>
          )}

          {segments.slice(hiddenCount).map((segment, offset) => {
            const index = hiddenCount + offset
            const isLast = offset === visibleCount - 1
            return (
              <Fragment key={targetAt(index)}>
                <BreadcrumbSeparator />
                <BreadcrumbItem>
                  {isLast ? (
                    <BreadcrumbPage className="whitespace-nowrap" data-testid="browse-crumb-current">
                      {segment}
                    </BreadcrumbPage>
                  ) : (
                    <BreadcrumbLink asChild>
                      <button
                        type="button"
                        className="rounded px-1 py-0.5 whitespace-nowrap transition-colors hover:bg-accent hover:text-accent-foreground"
                        onClick={() => onNavigate(targetAt(index))}
                        data-testid={`browse-crumb-${index}`}
                      >
                        {segment}
                      </button>
                    </BreadcrumbLink>
                  )}
                </BreadcrumbItem>
              </Fragment>
            )
          })}
        </BreadcrumbList>
      </Breadcrumb>
    </nav>
  )
}

export function MkdirSheet({
  open,
  defaultPath,
  onClose,
  onSubmit
}: {
  open: boolean
  defaultPath: string
  onClose(): void
  onSubmit(path: string): Promise<void>
}) {
  const t = useT()
  const [path, setPath] = useState(defaultPath)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!path.trim()) return
    setSubmitting(true)
    setError(null)
    try {
      await onSubmit(path)
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : t('common.error'))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Sheet open={open} onOpenChange={(next) => !next && onClose()}>
      <SheetContent side="right" className="data-[side=right]:sm:max-w-md" data-testid="browse-mkdir">
        <SheetHeader>
          <SheetTitle>{t('browse.mkdir')}</SheetTitle>
          <SheetDescription>{t('browse.mkdirAction.prompt')}</SheetDescription>
        </SheetHeader>
        <form className="flex min-h-0 flex-1 flex-col gap-4 overflow-auto px-4 pb-4" onSubmit={submit}>
          <TextField
            label={t('browse.table.name')}
            value={path}
            onChange={setPath}
            required
            autoFocus
            testId="browse-mkdir-path"
          />
          {error && <p className="text-xs text-destructive">{error}</p>}
          <div>
            <SubmitButton
              pending={submitting}
              pendingLabel={t('common.working')}
              testId="browse-mkdir-submit"
            >
              {t('common.confirm')}
            </SubmitButton>
          </div>
        </form>
      </SheetContent>
    </Sheet>
  )
}
