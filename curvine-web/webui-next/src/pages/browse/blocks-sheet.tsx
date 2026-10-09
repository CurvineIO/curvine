import { useCallback } from 'react'

import { useApi } from '@/api/api-context'
import { ErrorAlert } from '@/components/data-state'
import { Button } from '@/components/ui/button'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Skeleton } from '@/components/ui/skeleton'
import { useT } from '@/i18n'
import { useAsync } from '@/lib/use-async'
import { useRefreshSignal } from '@/refresh/refresh-context'
import { PathBreadcrumb } from './browse-controls'
import { fileName } from './utils'

/**
 * Block locations for one file, as a drawer over the list rather than a page of its own.
 *
 * Uses the skill's Sheet contract `{ open, onClose }`: the parent renders it while
 * `useSlidingSheet` reports mounted, so the close animation is not cut short.
 */
export function BlocksSheet({
  open,
  path,
  onClose,
  onNavigate
}: {
  open: boolean
  path: string
  onClose(): void
  /** Any level in the trail closes the drawer and opens that directory. */
  onNavigate(next: string): void
}) {
  const t = useT()
  const api = useApi()

  const load = useCallback(
    () => (path ? api.fetchBlocksData({ path }) : Promise.resolve(null)),
    [api, path]
  )
  const { data, loading, error, reload } = useAsync(load, [load])
  useRefreshSignal(reload)

  const formatted = JSON.stringify(data || {}, null, 2)

  return (
    <Sheet open={open} onOpenChange={(next) => !next && onClose()}>
      <SheetContent
        side="right"
        className="data-[side=right]:sm:max-w-2xl"
        data-testid="browse-blocks-sheet"
      >
        <SheetHeader>
          <SheetTitle className="truncate">{t('blocks.title')}</SheetTitle>
          <SheetDescription className="truncate" title={path}>
            {fileName(path) || path}
          </SheetDescription>
        </SheetHeader>

        <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-auto px-4">
          <div className="flex min-w-0 items-center gap-2 rounded-lg border border-border bg-muted/30 px-3 py-2">
            <PathBreadcrumb path={path} onNavigate={onNavigate} />
          </div>

          {error && <ErrorAlert message={error} />}

          {loading ? (
            <Skeleton className="h-64 w-full" />
          ) : (
            // Muted panel with normal text: the tokens used to be swapped here, which is
            // what made this black in light mode and white in dark mode.
            <pre
              className="max-h-[70vh] overflow-auto rounded-lg bg-muted p-3 font-mono text-xs text-foreground"
              data-testid="blocks-json"
            >
              {formatted}
            </pre>
          )}

          <div className="flex items-center justify-end gap-2">
            <Button variant="ghost" size="sm" onClick={onClose} data-testid="browse-blocks-close">
              {t('common.close')}
            </Button>
          </div>
        </div>
      </SheetContent>
    </Sheet>
  )
}
