import { useCallback, useEffect, useRef, useState } from 'react'
import { Boxes, FolderOpen } from 'lucide-react'
import { Link, useSearchParams } from 'react-router-dom'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { api } from '@/lib/api'
import { useAutoRefreshEffect } from '@/lib/auto-refresh'
import { formatBytes } from '@/lib/format'
import type { FileBlocks } from '@/lib/types'

export function BlocksPage() {
  const [searchParams] = useSearchParams()
  const path = searchParams.get('path') || ''
  // Block data is kept with the path it was loaded for: an answer for a file
  // the user already left (or an older refresh) must not replace the current
  // one, and a file that fails to load must not keep showing the previous one.
  const [loaded, setLoaded] = useState<{ path: string; data: FileBlocks } | null>(null)
  const [failedPath, setFailedPath] = useState<string | null>(null)
  const latestRequest = useRef(0)

  const load = useCallback(() => {
    const request = ++latestRequest.current
    if (!path) {
      setLoaded(null)
      return
    }
    api
      .blockLocations(path)
      .then(result => {
        if (request === latestRequest.current) {
          setLoaded({ path, data: result })
          setFailedPath(null)
        }
      })
      .catch(() => {
        if (request === latestRequest.current) {
          setFailedPath(path)
        }
      })
  }, [path])

  useEffect(() => {
    load()
  }, [load])
  useAutoRefreshEffect(load)

  const data = loaded !== null && loaded.path === path ? loaded.data : null
  const blocks = data?.block_locs ?? []

  return (
    <Card>
      <CardHeader className="flex-row flex-wrap items-center justify-between gap-4 border-b">
        <div className="space-y-1">
          <CardTitle className="flex items-center gap-2 text-base">
            <Boxes className="h-5 w-5 text-sky-400" />
            File block locations
          </CardTitle>
          <p className="break-all font-mono text-xs text-muted-foreground">{path || 'No file selected'}</p>
        </div>
        <div className="flex items-center gap-3">
          {data && (
            <Badge variant="muted">
              {blocks.length} blocks · {formatBytes(data.status.len)}
            </Badge>
          )}
          <Link
            to="/browse"
            className="inline-flex items-center gap-2 rounded-md border px-3 py-1.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          >
            <FolderOpen className="h-3.5 w-3.5" />
            Browse
          </Link>
        </div>
      </CardHeader>
      <CardContent className="p-0">
        {!path ? (
          <EmptyState text="Open a file from Browse to see where its blocks live." />
        ) : blocks.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full min-w-[720px] text-sm">
              <thead>
                <tr className="border-b bg-secondary/40 text-xs uppercase tracking-wider text-muted-foreground">
                  <th className="px-4 py-3 text-left">Block ID</th>
                  <th className="px-4 py-3 text-right">Block size</th>
                  <th className="px-4 py-3 text-left">File type</th>
                  <th className="px-4 py-3 text-left">Storage type</th>
                  <th className="px-4 py-3 text-left">Locations</th>
                </tr>
              </thead>
              <tbody>
                {blocks.map(located => (
                  <tr key={located.block.id} className="border-b last:border-b-0 hover:bg-accent/40">
                    <td className="px-4 py-3 font-mono font-semibold">{located.block.id}</td>
                    <td
                      className="px-4 py-3 text-right font-mono"
                      title={`${located.block.len} bytes`}
                    >
                      {formatBytes(located.block.len)}
                    </td>
                    <td className="px-4 py-3">
                      <Badge variant="muted">{located.block.file_type}</Badge>
                    </td>
                    <td className="px-4 py-3 font-mono text-xs uppercase text-muted-foreground">
                      {located.block.storage_type}
                    </td>
                    <td className="px-4 py-3 font-mono text-xs">
                      {located.locs.length > 0
                        ? located.locs.map(loc => `${loc.hostname}_${loc.rpc_port}`).join(', ')
                        : 'No locations'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : data ? (
          <EmptyState text="This file has no blocks." />
        ) : failedPath === path ? (
          <EmptyState text="Could not load the block locations of this file." />
        ) : (
          <EmptyState text="Loading…" />
        )}
      </CardContent>
    </Card>
  )
}

function EmptyState({ text }: { text: string }) {
  return <div className="px-6 py-16 text-center text-sm text-muted-foreground">{text}</div>
}
