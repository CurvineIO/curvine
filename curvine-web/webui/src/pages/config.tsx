import { useCallback, useEffect, useMemo, useState } from 'react'
import { Search, Settings, X } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { api } from '@/lib/api'
import { useAutoRefreshEffect } from '@/lib/auto-refresh'
import type { ConfigMap } from '@/lib/types'

interface ConfigEntry {
  key: string
  value: unknown
}

/** Turns the nested `ClusterConf` tree into dotted property keys. */
function flattenConfig(data: ConfigMap, parentKey = ''): ConfigEntry[] {
  const entries: ConfigEntry[] = []
  for (const [key, value] of Object.entries(data)) {
    const property = parentKey ? `${parentKey}.${key}` : key
    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
      entries.push(...flattenConfig(value as ConfigMap, property))
    } else {
      entries.push({ key: property, value })
    }
  }
  return entries
}

function displayValue(value: unknown): string {
  if (value === null || value === undefined) {
    return ''
  }
  return String(value)
}

export function ConfigPage() {
  const [config, setConfig] = useState<ConfigMap | null>(null)
  const [loading, setLoading] = useState(true)
  const [search, setSearch] = useState('')

  const load = useCallback(() => {
    api
      .config()
      .then(setConfig)
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [])

  useEffect(() => {
    load()
  }, [load])
  useAutoRefreshEffect(load)

  const entries = useMemo(() => (config ? flattenConfig(config) : []), [config])
  const query = search.trim().toLowerCase()
  const filtered = useMemo(
    () => (query ? entries.filter(entry => entry.key.toLowerCase().includes(query)) : entries),
    [entries, query],
  )

  return (
    <Card>
      <CardHeader className="flex-row flex-wrap items-center justify-between gap-4 border-b">
        <CardTitle className="flex items-center gap-2 text-base">
          <Settings className="h-5 w-5 text-sky-400" />
          Curvine configuration
          <Badge variant="muted">{entries.length} properties</Badge>
        </CardTitle>
        <div className="relative w-full sm:w-80">
          <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <input
            value={search}
            onChange={event => setSearch(event.target.value)}
            placeholder="Search by property"
            className="h-9 w-full rounded-md border bg-secondary/40 pl-9 pr-9 font-mono text-sm outline-none placeholder:text-muted-foreground/70 focus:border-sky-500/60 focus:ring-2 focus:ring-sky-500/20"
          />
          {search && (
            <button
              type="button"
              aria-label="Clear search"
              className="absolute right-2 top-1/2 -translate-y-1/2 rounded p-1 text-muted-foreground hover:bg-accent hover:text-foreground"
              onClick={() => setSearch('')}
            >
              <X className="h-4 w-4" />
            </button>
          )}
        </div>
      </CardHeader>
      <CardContent className="p-0">
        {filtered.length > 0 ? (
          <div className="max-h-[70vh] overflow-auto">
            <table className="w-full min-w-[640px] text-sm">
              <thead className="sticky top-0 z-10 bg-card">
                <tr className="border-b text-xs uppercase tracking-wider text-muted-foreground">
                  <th className="px-4 py-3 text-left">Property</th>
                  <th className="px-4 py-3 text-left">Value</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map(entry => (
                  <tr key={entry.key} className="border-b last:border-b-0 hover:bg-accent/40">
                    <td className="whitespace-nowrap px-4 py-2.5 font-mono text-sky-400">{entry.key}</td>
                    <td className="break-all px-4 py-2.5 font-mono text-muted-foreground">
                      {displayValue(entry.value)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="px-6 py-16 text-center text-sm text-muted-foreground">
            {loading ? 'Loading configuration…' : query ? 'Not found matched properties.' : 'No configuration available.'}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
