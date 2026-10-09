import { useCallback, useMemo, useState } from 'react'

import { useApi } from '@/api/api-context'
import type { ConfigSource, ConfigValue } from '@/api/types'
import { ErrorAlert } from '@/components/data-state'
import { MetricCard } from '@/components/metric-card'
import { PageHeader } from '@/components/page-header'
import { MonoText, StatusBadge, type StatusTone } from '@/components/resource-table'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/components/ui/select'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/components/ui/table'
import { useT } from '@/i18n'
import { translateDelta } from '@/lib/i18nLabels'
import { useRefreshSignal } from '@/refresh/refresh-context'
import { useAsync } from '@/lib/use-async'

const ALL_GROUPS = '__all__'
const ALL_SOURCES = '__all__'
const CONFIG_SOURCES: ConfigSource[] = ['default', 'file', 'env', 'cli']

interface ConfigItem {
  key: string
  value: string
  multiline: boolean
  source: ConfigSource
}

const SOURCE_TONES: Record<ConfigSource, StatusTone> = {
  default: 'neutral',
  file: 'info',
  env: 'warning',
  cli: 'positive'
}

function formatValue(value: ConfigValue): { value: string; multiline: boolean } {
  if (Array.isArray(value)) {
    if (value.length === 0) return { value: '[]', multiline: false }
    const lines = value.map((item) => formatArrayItem(item))
    return {
      value: lines.join('\n'),
      multiline: lines.length > 1 || lines.some((line) => line.length > 80)
    }
  }
  if (value && typeof value === 'object') return { value: JSON.stringify(value), multiline: false }
  if (value === null || value === undefined) return { value: '', multiline: false }
  return { value: String(value), multiline: false }
}

function formatArrayItem(item: ConfigValue): string {
  if (!item || typeof item !== 'object' || Array.isArray(item)) return String(item)
  const record = item as Record<string, ConfigValue>
  if ('hostname' in record && 'port' in record) {
    const prefix = 'id' in record ? `${record.id}: ` : ''
    return `${prefix}${record.hostname}:${record.port}`
  }
  return Object.entries(record)
    .map(([key, value]) => `${key}=${value}`)
    .join(', ')
}

function configSourceFor(key: string, sources: Record<string, ConfigSource>): ConfigSource {
  const source = sources[key]
  return source === 'file' || source === 'env' || source === 'cli' ? source : 'default'
}

function flattenConfig(
  data: ConfigValue,
  sources: Record<string, ConfigSource>,
  parent = ''
): ConfigItem[] {
  const items: ConfigItem[] = []
  for (const key of Object.keys(data || {})) {
    const name = parent ? `${parent}.${key}` : key
    const value = (data as Record<string, ConfigValue>)[key]
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      items.push(...flattenConfig(value, sources, name))
    } else {
      const formatted = formatValue(value)
      items.push({
        key: name,
        value: formatted.value,
        multiline: formatted.multiline,
        source: configSourceFor(name, sources)
      })
    }
  }
  return items
}

function groupOf(key: string): string {
  return String(key || '').split('.')[0]
}

export function ConfigPage() {
  const t = useT()
  const api = useApi()

  const [query, setQuery] = useState('')
  const [groupFilter, setGroupFilter] = useState(ALL_GROUPS)
  const [sourceFilter, setSourceFilter] = useState(ALL_SOURCES)

  const load = useCallback(() => api.fetchConfigData(), [api])
  const { data, loading, error, reload } = useAsync(load, [load])
  useRefreshSignal(reload)

  const configItems = useMemo(
    () => flattenConfig(data?.values || {}, data?.sources || {}),
    [data]
  )
  const groups = useMemo(
    () => [...new Set(configItems.map((item) => groupOf(item.key)).filter(Boolean))].sort(),
    [configItems]
  )

  const filtered = useMemo(() => {
    const needle = query.toLowerCase()
    return configItems.filter(
      (item) =>
        (groupFilter === ALL_GROUPS || groupOf(item.key) === groupFilter) &&
        (sourceFilter === ALL_SOURCES || item.source === sourceFilter) &&
        item.key.toLowerCase().includes(needle)
    )
  }, [configItems, query, groupFilter, sourceFilter])

  const groupCount = (group: string) =>
    configItems.filter((item) => groupOf(item.key) === group).length
  const sourceCount = (source: ConfigSource) =>
    configItems.filter((item) => item.source === source).length

  const delta = (value: string) => translateDelta(t, value)

  return (
    <>
      <PageHeader
        eyebrow={t('nav.groupRuntime')}
        title={t('config.title')}
        description={t('config.subtitle')}
      />

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          label={t('config.metrics.entries')}
          value={filtered.length}
          meta={t('config.metrics.entriesMeta')}
          delta={delta('readonly')}
          tone="good"
        />
        <MetricCard
          label={t('config.metrics.groups')}
          value={groups.length}
          meta={t('config.metrics.groupsMeta')}
          delta={delta('auto')}
          tone="neutral"
        />
        <MetricCard
          label={t('config.metrics.settings')}
          value={configItems.length}
          meta={t('config.metrics.settingsMeta')}
          delta={delta('cluster')}
          tone="good"
        />
        <MetricCard
          label={t('config.metrics.api')}
          value="/api/v1"
          meta={t('config.metrics.apiMeta')}
          delta={delta('enabled')}
          tone="good"
        />
      </section>

      <Card className="gap-3" size="sm">
        <CardHeader>
          <h2 className="font-heading text-base font-medium">{t('config.title')}</h2>
          <p className="text-xs text-muted-foreground">{t('config.subtitle')}</p>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <div className="grid gap-3 border-b border-border pb-3 lg:grid-cols-[minmax(0,1fr)_minmax(0,18rem)_minmax(0,14rem)]">
            <label className="flex flex-col gap-1.5">
              <span className="text-xs font-medium text-muted-foreground">
                {t('config.searchLabel')}
              </span>
              <Input
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder={t('config.searchPlaceholder')}
                data-testid="config-search"
              />
            </label>
            <label className="flex flex-col gap-1.5">
              <span className="text-xs font-medium text-muted-foreground">
                {t('config.groupLabel')}
              </span>
              <Select value={groupFilter} onValueChange={setGroupFilter}>
                <SelectTrigger className="w-full" data-testid="config-group">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    <SelectItem value={ALL_GROUPS}>
                      {t('config.allGroups', { count: configItems.length })}
                    </SelectItem>
                    {groups.map((group) => (
                      <SelectItem key={group} value={group}>
                        {t('config.groupOption', { group, count: groupCount(group) })}
                      </SelectItem>
                    ))}
                  </SelectGroup>
                </SelectContent>
              </Select>
            </label>
            <label className="flex flex-col gap-1.5">
              <span className="text-xs font-medium text-muted-foreground">
                {t('config.sourceLabel')}
              </span>
              <Select value={sourceFilter} onValueChange={setSourceFilter}>
                <SelectTrigger className="w-full" data-testid="config-source">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    <SelectItem value={ALL_SOURCES}>
                      {t('config.allSources', { count: configItems.length })}
                    </SelectItem>
                    {CONFIG_SOURCES.map((source) => (
                      <SelectItem key={source} value={source}>
                        {t('config.sourceOption', {
                          source: t(`config.sources.${source}`),
                          count: sourceCount(source)
                        })}
                      </SelectItem>
                    ))}
                  </SelectGroup>
                </SelectContent>
              </Select>
            </label>
          </div>

          {error && <ErrorAlert message={error} />}

          {loading ? (
            <div className="flex flex-col gap-2">
              {Array.from({ length: 8 }, (_, index) => (
                <Skeleton key={index} className="h-6 w-full" />
              ))}
            </div>
          ) : filtered.length === 0 ? (
            <p className="rounded-lg border border-dashed border-border p-6 text-center text-sm text-muted-foreground">
              {t('config.empty')}
            </p>
          ) : (
            <div className="max-h-[65vh] overflow-auto rounded-lg border border-border">
              <Table>
                <TableHeader className="sticky top-0 z-10 bg-card">
                  <TableRow>
                    <TableHead className="w-[34%]">{t('config.property')}</TableHead>
                    <TableHead className="w-[12%] whitespace-nowrap">{t('config.source')}</TableHead>
                    <TableHead>{t('config.value')}</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filtered.map((item) => (
                    <TableRow key={item.key}>
                      <TableCell>
                        <MonoText>{item.key}</MonoText>
                      </TableCell>
                      <TableCell>
                        <StatusBadge
                          value={t(`config.sources.${item.source}`)}
                          tone={SOURCE_TONES[item.source]}
                        />
                      </TableCell>
                      <TableCell className="break-words">
                        {item.multiline ? (
                          <pre className="m-0 font-mono text-xs whitespace-pre-wrap">
                            {item.value}
                          </pre>
                        ) : (
                          <span className="text-sm">{item.value}</span>
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>
    </>
  )
}
