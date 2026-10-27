import { useCallback, useMemo, useState } from 'react'
import { CircleAlertIcon, FlameIcon, PencilIcon, XIcon } from 'lucide-react'

import { useApi } from '@/api/api-context'
import type { ConfigRuntimeEntry, ConfigSource, ConfigValue } from '@/api/types'
import { ErrorAlert } from '@/components/data-state'
import { MetricCard } from '@/components/metric-card'
import { PageHeader } from '@/components/page-header'
import { MonoText, StatusBadge, type StatusTone } from '@/components/resource-table'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Checkbox } from '@/components/ui/checkbox'
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
import { useToast } from '@/components/ui/toast'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useT } from '@/i18n'
import { translateDelta } from '@/lib/i18nLabels'
import { useRefreshSignal } from '@/refresh/refresh-context'
import { useAsync } from '@/lib/use-async'

const ALL_GROUPS = '__all__'
const ALL_SOURCES = '__all__'
const CONFIG_SOURCES: ConfigSource[] = ['default', 'file', 'env', 'cli', 'derived', 'runtime']

interface ConfigItem {
  key: string
  value: string
  multiline: boolean
  source: ConfigSource
  /** The registry key backing this path, when the path is hot-tunable. */
  runtime?: ConfigRuntimeEntry
  /** True when an override is active, so the row also offers "revert". */
  overridden?: boolean
}

const SOURCE_TONES: Record<ConfigSource, StatusTone> = {
  default: 'neutral',
  file: 'info',
  env: 'warning',
  cli: 'positive',
  derived: 'neutral',
  runtime: 'positive'
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

/**
 * Only the sources the table can render. An unknown string must degrade to
 * `default` rather than reach `t('config.sources.…')` or `SOURCE_TONES`, but
 * every source the backend can emit has to be listed here — dropping one
 * silently relabels those rows as Default, which is the exact failure the
 * runtime source was added to fix.
 */
const KNOWN_SOURCES: ConfigSource[] = ['file', 'env', 'cli', 'derived', 'runtime']

function configSourceFor(key: string, sources: Record<string, ConfigSource>): ConfigSource {
  const source = sources[key]
  return source && KNOWN_SOURCES.includes(source) ? source : 'default'
}

/**
 * Flattens the published tree into table rows, attaching the runtime entry to
 * the path it backs.
 *
 * The join is by *configuration path*, not by registry key: a key like
 * `master.dir_space_enabled` backs `master.dir_space.enabled`, and the tree only
 * contains the latter. Matching on the key would leave every tunable row
 * unmatched and the edit affordance unreachable.
 */
function flattenConfig(
  data: ConfigValue,
  sources: Record<string, ConfigSource>,
  entriesByPath: Map<string, ConfigRuntimeEntry>,
  parent = ''
): ConfigItem[] {
  const items: ConfigItem[] = []
  for (const key of Object.keys(data || {})) {
    const name = parent ? `${parent}.${key}` : key
    const value = (data as Record<string, ConfigValue>)[key]
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      items.push(...flattenConfig(value, sources, entriesByPath, name))
    } else {
      const formatted = formatValue(value)
      const entry = entriesByPath.get(name)
      items.push({
        key: name,
        value: formatted.value,
        multiline: formatted.multiline,
        source: configSourceFor(name, sources),
        runtime: entry,
        overridden: entry?.override_value !== undefined && entry?.override_value !== null
      })
    }
  }
  return items
}

function groupOf(key: string): string {
  return String(key || '').split('.')[0]
}

/**
 * Edit buffer for one row, in the spelling the backend parser accepts.
 *
 * Scalars go in bare, but an array or an object has to stay JSON: the registry
 * parser for a list-typed knob runs `serde_json::from_str`, so seeding the input
 * with the humanised rendering from the table (`/a` newline `/b`) would fail on
 * save. The buffer stays a raw string so a half-typed value is not rejected
 * mid-keystroke — the parser is the single authority on what it means.
 */
function draftOf(value: ConfigValue): string {
  if (value === null || value === undefined) return ''
  if (Array.isArray(value) || typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function initialDraft(item: ConfigItem): string {
  const entry = item.runtime
  return draftOf(entry ? entry.effective_value : null)
}

export function ConfigPage() {
  const t = useT()
  const api = useApi()
  const toast = useToast()

  const [query, setQuery] = useState('')
  const [groupFilter, setGroupFilter] = useState(ALL_GROUPS)
  const [sourceFilter, setSourceFilter] = useState(ALL_SOURCES)
  const [tunableOnly, setTunableOnly] = useState(false)
  const [editingKey, setEditingKey] = useState<string | null>(null)
  const [draft, setDraft] = useState('')
  const [saving, setSaving] = useState(false)

  const load = useCallback(() => api.fetchConfigData(), [api])
  const { data, loading, error, reload, refresh } = useAsync(load, [load])
  useRefreshSignal(reload)

  const runtimeEntries = useMemo(() => data?.runtime || [], [data])
  const entriesByPath = useMemo(() => {
    const map = new Map<string, ConfigRuntimeEntry>()
    for (const entry of runtimeEntries) map.set(entry.path, entry)
    return map
  }, [runtimeEntries])

  const configItems = useMemo(
    () => flattenConfig(data?.values || {}, data?.sources || {}, entriesByPath),
    [data, entriesByPath]
  )
  const groups = useMemo(
    () => [...new Set(configItems.map((item) => groupOf(item.key)).filter(Boolean))].sort(),
    [configItems]
  )

  const tunableCount = useMemo(
    () => configItems.filter((item) => item.runtime).length,
    [configItems]
  )

  const filtered = useMemo(() => {
    const needle = query.toLowerCase()
    return configItems.filter(
      (item) =>
        (!tunableOnly || item.runtime !== undefined) &&
        (groupFilter === ALL_GROUPS || groupOf(item.key) === groupFilter) &&
        (sourceFilter === ALL_SOURCES || item.source === sourceFilter) &&
        item.key.toLowerCase().includes(needle)
    )
  }, [configItems, query, groupFilter, sourceFilter, tunableOnly])

  const groupCount = (group: string) =>
    configItems.filter((item) => groupOf(item.key) === group).length
  const sourceCount = (source: ConfigSource) =>
    configItems.filter((item) => item.source === source).length

  const delta = (value: string) => translateDelta(t, value)

  const unresolved = data?.unresolved || []

  const startEdit = (item: ConfigItem) => {
    setEditingKey(item.key)
    setDraft(initialDraft(item))
  }

  const cancelEdit = () => {
    setEditingKey(null)
    setDraft('')
  }

  const save = async (item: ConfigItem, value: string | null) => {
    if (!item.runtime) return
    setSaving(true)
    try {
      await api.updateConfig({ key: item.runtime.key, value })
      toast.success(
        value === null
          ? t('config.runtimeReverted', { key: item.runtime.key })
          : t('config.runtimeSaved', { key: item.runtime.key })
      )
      cancelEdit()
      // Silent refresh keeps the table from collapsing to skeletons on every
      // save, which would also drop the row the operator just changed.
      await refresh()
    } catch (err) {
      toast.error(err instanceof Error ? err.message : String(err))
    } finally {
      setSaving(false)
    }
  }

  return (
    <>
      <PageHeader
        eyebrow={t('nav.groupRuntime')}
        title={t('config.title')}
        description={t('config.subtitle')}
      />

      {(data?.role || data?.node) && (
        <section className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
          {data?.role && (
            <span data-testid="config-role">
              <StatusBadge value={data.role} tone="info" />
            </span>
          )}
          {data?.node && (
            <span data-testid="config-node">
              {t('config.node')}: <MonoText>{data.node}</MonoText>
            </span>
          )}
          {data?.persistent === false && <span>{t('config.volatileNote')}</span>}
        </section>
      )}

      {unresolved.length > 0 && (
        <Alert variant="destructive" data-testid="config-unresolved">
          <CircleAlertIcon />
          <AlertTitle>{t('config.unresolvedTitle')}</AlertTitle>
          <AlertDescription>
            <p>{t('config.unresolvedBody')}</p>
            <ul className="mt-1 list-disc pl-5 font-mono text-xs">
              {unresolved.map((key) => (
                <li key={key}>{key}</li>
              ))}
            </ul>
          </AlertDescription>
        </Alert>
      )}

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
          label={t('config.metrics.tunable')}
          value={tunableCount}
          meta={t('config.metrics.tunableMeta')}
          delta={delta('hot')}
          tone={tunableCount > 0 ? 'good' : 'neutral'}
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

          {/* The tunable filter and the source filter answer the same question
              from opposite ends — "only what I can change" versus "only where
              this value came from" — so they sit together rather than as a
              second, separately-maintained table. */}
          <div className="flex flex-wrap items-center justify-between gap-3">
            <label className="flex items-center gap-2 text-xs text-muted-foreground">
              <Checkbox
                checked={tunableOnly}
                onCheckedChange={(checked) => setTunableOnly(checked === true)}
                data-testid="config-tunable-only"
              />
              <span>
                {t('config.tunableOnly')}
                {tunableCount > 0 ? ` (${tunableCount})` : ''}
              </span>
            </label>
            <p className="text-xs text-muted-foreground">{t('config.editHint')}</p>
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
            <div
              className="max-h-[65vh] overflow-auto rounded-lg border border-border"
              data-testid="config-values-table"
            >
              <Table>
                <TableHeader className="sticky top-0 z-10 bg-card">
                  <TableRow>
                    <TableHead className="w-[34%]">{t('config.property')}</TableHead>
                    <TableHead className="w-[12%] whitespace-nowrap">{t('config.source')}</TableHead>
                    <TableHead>{t('config.value')}</TableHead>
                    <TableHead className="w-[9rem] text-right">{t('config.actions')}</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filtered.map((item) => {
                    const editing = editingKey === item.key
                    return (
                      <TableRow key={item.key} data-testid="config-row">
                        <TableCell>
                          <MonoText>{item.key}</MonoText>
                          {item.runtime && item.runtime.path !== item.key && (
                            <div className="mt-0.5 text-[11px] text-muted-foreground">
                              <MonoText>{item.runtime.key}</MonoText>
                            </div>
                          )}
                        </TableCell>
                        <TableCell data-testid="config-source-cell">
                          <StatusBadge
                            value={t(`config.sources.${item.source}`)}
                            tone={SOURCE_TONES[item.source]}
                          />
                        </TableCell>
                        <TableCell className="break-words">
                          {editing ? (
                            <Input
                              value={draft}
                              autoFocus
                              disabled={saving}
                              onChange={(event) => setDraft(event.target.value)}
                              onKeyDown={(event) => {
                                if (event.key === 'Enter') void save(item, draft)
                                if (event.key === 'Escape') cancelEdit()
                              }}
                              className="font-mono text-xs"
                              data-testid="config-edit-input"
                            />
                          ) : item.multiline ? (
                            <pre className="m-0 font-mono text-xs whitespace-pre-wrap">
                              {item.value}
                            </pre>
                          ) : (
                            <span className="text-sm">{item.value}</span>
                          )}
                          {/* The file value is what the override replaced, so it
                              only earns a line when the two actually differ. */}
                          {item.overridden && !editing && (
                            <div
                              className="mt-0.5 text-[11px] text-muted-foreground"
                              data-testid="config-file-value"
                            >
                              {t('config.runtimeFile')}:{' '}
                              {formatValue(item.runtime?.file_value ?? null).value}
                            </div>
                          )}
                        </TableCell>
                        <TableCell className="text-right whitespace-nowrap">
                          {!item.runtime ? (
                            <span className="text-xs text-muted-foreground">
                              {t('config.readonly')}
                            </span>
                          ) : editing ? (
                            <span className="flex items-center justify-end gap-1">
                              <Button
                                size="xs"
                                variant="default"
                                disabled={saving}
                                onClick={() => void save(item, draft)}
                                data-testid="config-save"
                              >
                                {t('common.save')}
                              </Button>
                              <Button
                                size="xs"
                                variant="ghost"
                                disabled={saving}
                                onClick={cancelEdit}
                                data-testid="config-cancel"
                              >
                                {t('common.cancel')}
                              </Button>
                            </span>
                          ) : (
                            <span className="flex items-center justify-end gap-1">
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <Button
                                    size="icon-xs"
                                    variant="ghost"
                                    onClick={() => startEdit(item)}
                                    aria-label={t('config.edit')}
                                    data-testid="config-edit"
                                  >
                                    <PencilIcon />
                                  </Button>
                                </TooltipTrigger>
                                <TooltipContent>{t('config.edit')}</TooltipContent>
                              </Tooltip>
                              {item.overridden && (
                                <Tooltip>
                                  <TooltipTrigger asChild>
                                    <Button
                                      size="icon-xs"
                                      variant="ghost"
                                      disabled={saving}
                                      onClick={() => void save(item, null)}
                                      aria-label={t('config.revert')}
                                      data-testid="config-revert"
                                    >
                                      <XIcon />
                                    </Button>
                                  </TooltipTrigger>
                                  <TooltipContent>{t('config.revert')}</TooltipContent>
                                </Tooltip>
                              )}
                            </span>
                          )}
                        </TableCell>
                      </TableRow>
                    )
                  })}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Explains the two provenance labels an operator cannot infer from the
          value alone. Kept as prose because it answers "what does this word
          mean", which a per-row tooltip would hide behind a hover. */}
      <Card className="gap-3" size="sm" data-testid="config-source-legend">
        <CardHeader>
          <h2 className="flex items-center gap-2 font-heading text-base font-medium">
            <FlameIcon className="size-4" />
            {t('config.legendTitle')}
          </h2>
        </CardHeader>
        <CardContent className="flex flex-col gap-2 text-sm">
          <p>
            <StatusBadge value={t('config.sources.derived')} tone="neutral" />{' '}
            {t('config.legendDerived')}
          </p>
          <p>
            <StatusBadge value={t('config.sources.runtime')} tone="positive" />{' '}
            {t('config.legendRuntime')}
          </p>
        </CardContent>
      </Card>
    </>
  )
}
