import { useCallback, useEffect, useState, type FormEvent } from 'react'
import { FileText, Folder, FolderOpen, Navigation, SearchX, ChevronRight } from 'lucide-react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { api } from '@/lib/api'
import { useAutoRefreshEffect } from '@/lib/auto-refresh'
import { formatBytes, formatTimestamp } from '@/lib/format'
import type { FileStatus } from '@/lib/types'
import { cn } from '@/lib/utils'

/** Keeps `/` readable in the query string while still escaping anything else. */
function pathLink(page: string, path: string): string {
  return `${page}?path=${encodeURIComponent(path).replace(/%2F/g, '/')}`
}

function normalizePath(path: string): string {
  return path.replace('//', '/')
}

function fileName(path: string): string {
  if (!path) {
    return ''
  }
  const parts = path.split('/')
  return parts[parts.length - 1] || parts[parts.length - 2] || path
}

/** Splits a path into clickable ancestors, from the root down to the current directory. */
function breadcrumbs(path: string): { label: string; path: string }[] {
  const parts = path.split('/').filter(Boolean)
  const crumbs = [{ label: 'Root', path: '/' }]
  let current = ''
  for (const part of parts) {
    current += `/${part}`
    crumbs.push({ label: part, path: current })
  }
  return crumbs
}

export function BrowsePage() {
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  const currentPath = searchParams.get('path') || '/'
  const [pathInput, setPathInput] = useState(currentPath)
  const [items, setItems] = useState<FileStatus[]>([])

  useEffect(() => {
    setPathInput(currentPath)
  }, [currentPath])

  const load = useCallback(() => {
    api.browse(currentPath).then(setItems).catch(() => {})
  }, [currentPath])

  useEffect(() => {
    load()
  }, [load])
  useAutoRefreshEffect(load)

  const submit = (event: FormEvent) => {
    event.preventDefault()
    const next = pathInput.trim() || '/'
    if (next === currentPath) {
      load()
      return
    }
    navigate(pathLink('/browse', next))
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader className="flex-row flex-wrap items-center justify-between gap-4">
          <CardTitle className="flex items-center gap-3 text-base">
            <FolderOpen className="h-5 w-5 text-sky-400" />
            File system browser
          </CardTitle>
          <Link
            to={pathLink('/browse', '/')}
            className="inline-flex items-center gap-2 rounded-md bg-emerald-500/15 px-4 py-2 text-sm font-medium text-emerald-400 transition-colors hover:bg-emerald-500/25"
          >
            🏠 Root
          </Link>
        </CardHeader>
        <CardContent>
          <form onSubmit={submit} className="flex flex-wrap items-center gap-3">
            <label className="text-xs uppercase tracking-widest text-muted-foreground" htmlFor="browse-path">
              Current path
            </label>
            <div className="flex min-w-64 flex-1 gap-3">
              <input
                id="browse-path"
                value={pathInput}
                onChange={event => setPathInput(event.target.value)}
                placeholder="Enter file system path..."
                className="h-10 w-full rounded-md border bg-secondary/40 px-3 font-mono text-sm outline-none placeholder:text-muted-foreground/70 focus:border-sky-500/60 focus:ring-2 focus:ring-sky-500/20"
              />
              <button
                type="submit"
                className="inline-flex shrink-0 items-center gap-2 rounded-md bg-sky-500/15 px-4 text-sm font-medium text-sky-400 transition-colors hover:bg-sky-500/25"
              >
                <Navigation className="h-4 w-4" />
                Navigate
              </button>
            </div>
          </form>
          <nav aria-label="Breadcrumb" className="mt-4 flex flex-wrap items-center gap-1 border-t pt-3 font-mono text-sm">
            {breadcrumbs(currentPath).map((crumb, index, all) => {
              const isLast = index === all.length - 1
              return (
                <span key={crumb.path} className="flex items-center gap-1">
                  {index > 0 && <ChevronRight className="h-3.5 w-3.5 shrink-0 text-muted-foreground/60" />}
                  {isLast ? (
                    <span className="max-w-[18rem] truncate font-semibold">{crumb.label}</span>
                  ) : (
                    <Link
                      to={pathLink('/browse', crumb.path)}
                      title={crumb.path}
                      className="max-w-[14rem] truncate rounded px-1 text-sky-400 transition-colors hover:bg-sky-500/10 hover:underline"
                    >
                      {crumb.label}
                    </Link>
                  )}
                </span>
              )
            })}
          </nav>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex-row flex-wrap items-center justify-between gap-3 border-b">
          <CardTitle className="text-sm font-medium text-muted-foreground">{items.length} items</CardTitle>
          <span className="truncate font-mono text-xs text-muted-foreground">{currentPath}</span>
        </CardHeader>
        <CardContent className="p-4">
          {items.length > 0 ? (
            <div className="grid grid-cols-[repeat(auto-fill,minmax(17rem,1fr))] gap-4">
              {items.map(item => (
                <FileCard key={item.id} item={item} />
              ))}
            </div>
          ) : (
            <div className="flex flex-col items-center gap-2 px-6 py-16 text-center">
              <SearchX className="h-8 w-8 text-muted-foreground" />
              <span className="text-sm font-medium">Directory is empty</span>
              <span className="text-xs text-muted-foreground">No files or folders found in this location</span>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

function FileCard({ item }: { item: FileStatus }) {
  const name = fileName(item.path)
  const target = pathLink(item.is_dir ? '/browse' : '/blocks', normalizePath(item.path))

  return (
    <div
      className={cn(
        'flex flex-col gap-3 rounded-lg border bg-secondary/30 p-4 transition-colors hover:border-sky-500/40',
        item.is_dir && 'border-sky-500/20',
      )}
    >
      <div className="flex items-center justify-between">
        <span
          className={cn(
            'flex h-10 w-10 items-center justify-center rounded-lg',
            item.is_dir ? 'bg-sky-500/15 text-sky-400' : 'bg-secondary text-muted-foreground',
          )}
        >
          {item.is_dir ? <Folder className="h-5 w-5" /> : <FileText className="h-5 w-5" />}
        </span>
        <Badge variant={item.is_dir ? 'info' : 'muted'}>{item.is_dir ? 'DIR' : 'FILE'}</Badge>
      </div>

      <Link
        to={target}
        className={cn(
          'truncate font-mono text-sm font-semibold hover:underline',
          item.is_dir ? 'text-sky-400' : 'text-foreground',
        )}
        title={item.path}
      >
        {name}
      </Link>

      <div className="space-y-1 text-xs">
        {item.is_dir ? (
          <DetailRow label="Children" value={String(item.children_num || 0)} />
        ) : (
          <>
            <DetailRow label="Size" value={formatBytes(item.len)} />
            <DetailRow label="Block size" value={formatBytes(item.block_size)} />
            <DetailRow label="Replicas" value={String(item.replicas || 0)} />
          </>
        )}
      </div>

      <div className="space-y-1 border-t pt-3 text-xs">
        <DetailRow label="Modified" value={formatTimestamp(item.mtime)} />
        {!item.is_dir && (
          <DetailRow
            label="Complete"
            value={item.is_complete ? '✓' : '✗'}
            valueClassName={item.is_complete ? 'text-emerald-400' : 'text-red-400'}
          />
        )}
        {!item.is_dir && item.storage_policy && (
          <DetailRow label="Storage" value={item.storage_policy.storage_type || 'N/A'} />
        )}
      </div>
    </div>
  )
}

function DetailRow({
  label,
  value,
  valueClassName,
}: {
  label: string
  value: string
  valueClassName?: string
}) {
  return (
    <div className="flex items-center justify-between gap-3">
      <span className="uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className={cn('truncate font-mono', valueClassName)}>{value}</span>
    </div>
  )
}
