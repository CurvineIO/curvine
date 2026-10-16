import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { ChevronDownIcon, ChevronLeftIcon, ChevronRightIcon, FolderIcon, FolderSymlinkIcon } from 'lucide-react'

import { useApi } from '@/api/api-context'
import type { MountInfo } from '@/api/types'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { EmptyNotice, ErrorAlert } from '@/components/data-state'
import { MetricCard } from '@/components/metric-card'
import { PageHeader } from '@/components/page-header'
import { MonoText, StatusBadge } from '@/components/resource-table'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/components/ui/select'
import { Spinner } from '@/components/ui/spinner'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { useToast } from '@/components/ui/toast'
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'
import { useT } from '@/i18n'
import { translateDelta } from '@/lib/i18nLabels'
import { useRefreshSignal } from '@/refresh/refresh-context'
import { useAsync } from '@/lib/use-async'
import { useSlidingSheet } from '@/lib/use-sliding-sheet'
import { MountFormSheet } from './mounts/mount-form-sheet'
import {
  buildMountTree,
  formatMountTtl,
  matchesCvPath,
  providerLabel,
  schemeOf,
  ttlTypeLabel,
  writeTypeLabel,
  type MountTreeNode
} from './mounts/utils'

interface ResyncTask {
  id?: string
  status?: string
  done?: boolean
  message?: string
  scanned?: number
  recreated?: number
  skipped?: number
  failed?: number
  pending_dirs?: number
}

const MODE_FILTERS = ['all', 'fs_mode', 'cache_mode'] as const
type ModeFilter = (typeof MODE_FILTERS)[number]

const VIEW_MODES = ['flat', 'tree'] as const
type ViewMode = (typeof VIEW_MODES)[number]

const LIST_PAGE_SIZES = [10, 25, 50, 100]
const LIST_PAGE_DEFAULT = 25

export function MountsPage() {
  const t = useT()
  const api = useApi()
  const toast = useToast()

  const [modeFilter, setModeFilter] = useState<ModeFilter>('all')
  const [viewMode, setViewMode] = useState<ViewMode>('flat')
  const [pathQuery, setPathQuery] = useState('')
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(LIST_PAGE_DEFAULT)
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const [sheetMount, setSheetMount] = useState<MountInfo | null | undefined>(undefined)
  const formSheet = useSlidingSheet(sheetMount !== undefined, sheetMount)
  const [deleteTarget, setDeleteTarget] = useState<MountInfo | null>(null)
  const [rowValidating, setRowValidating] = useState('')
  const [busy, setBusy] = useState(false)
  const [resyncTasks, setResyncTasks] = useState<Record<string, ResyncTask>>({})
  const resyncTimers = useRef<Record<string, number>>({})

  const load = useCallback(async () => {
    const data = await api.fetchMountsData()
    return (Array.isArray(data) ? data : (data as { items?: MountInfo[] })?.items || []) as MountInfo[]
  }, [api])

  const { data, loading, error, reload, refresh } = useAsync(load, [load])
  useRefreshSignal(refresh)

  const mounts = data ?? []

  useEffect(
    () => () => {
      Object.values(resyncTimers.current).forEach((timer) => window.clearTimeout(timer))
      resyncTimers.current = {}
    },
    []
  )

  const fsModeCount = mounts.filter((mount) => writeTypeLabel(mount.write_type) === 'fs_mode').length
  const cacheModeCount = mounts.filter(
    (mount) => writeTypeLabel(mount.write_type) === 'cache_mode'
  ).length
  const providerCount = useMemo(
    () =>
      new Set(
        mounts
          .map((mount) => providerLabel(mount.provider) || schemeOf(mount.ufs_path))
          .filter((value) => value && value !== 'auto')
      ).size,
    [mounts]
  )

  const filteredMounts = useMemo(
    () =>
      mounts.filter((mount) => {
        if (modeFilter !== 'all' && writeTypeLabel(mount.write_type) !== modeFilter) return false
        if (!pathQuery.trim()) return true
        return matchesCvPath(mount.cv_path, pathQuery)
      }),
    [mounts, modeFilter, pathQuery]
  )

  const totalPages = Math.max(1, Math.ceil(filteredMounts.length / pageSize))
  // Deleting or filtering can leave the cursor past the end; clamp rather than
  // render an empty page.
  const safePage = Math.min(page, totalPages)
  const pagedMounts = useMemo(
    () => filteredMounts.slice((safePage - 1) * pageSize, safePage * pageSize),
    [filteredMounts, safePage, pageSize]
  )

  const treeNodes = useMemo(() => buildMountTree(filteredMounts), [filteredMounts])
  const visibleTreeRows = useMemo(
    () => flattenMountTree(treeNodes, expanded),
    [treeNodes, expanded]
  )

  function toggleNode(path: string) {
    setExpanded((current) => {
      const next = new Set(current)
      if (next.has(path)) next.delete(path)
      else next.add(path)
      return next
    })
  }

  const delta = (value: string) => translateDelta(t, value)

  function setResyncTask(cvPath: string, task: ResyncTask) {
    setResyncTasks((current) => ({ ...current, [cvPath]: task }))
  }

  function resyncLabel(task: ResyncTask): string {
    return t('mounts.resync.label', {
      status: task.status || 'running',
      scanned: task.scanned || 0,
      recreated: task.recreated || 0,
      skipped: task.skipped || 0,
      failed: task.failed || 0,
      pending: task.pending_dirs || 0
    })
  }

  async function pollResync(cvPath: string, taskId: string) {
    try {
      const task = (await api.fetchMountResync(taskId)) as ResyncTask
      setResyncTask(cvPath, task)
      if (!task.done) {
        resyncTimers.current[cvPath] = window.setTimeout(
          () => void pollResync(cvPath, taskId),
          1000
        )
      } else {
        await reload()
      }
    } catch (error) {
      setResyncTask(cvPath, {
        id: taskId,
        status: 'failed',
        done: true,
        message: error instanceof Error ? error.message : String(error),
        scanned: 0,
        recreated: 0,
        skipped: 0,
        failed: 1,
        pending_dirs: 0
      })
    }
  }

  async function resyncMount(mount: MountInfo) {
    try {
      const task = (await api.startMountResync(mount.cv_path)) as ResyncTask
      setResyncTask(mount.cv_path, task)
      if (task?.id) void pollResync(mount.cv_path, task.id)
    } catch (error) {
      toast.error(
        t('mounts.alerts.resyncFailed', {
          error: error instanceof Error ? error.message : String(error)
        })
      )
    }
  }

  async function validateExisting(mount: MountInfo) {
    setRowValidating(mount.cv_path)
    try {
      const data = (await api.validateMount({
        ufs_path: mount.ufs_path,
        provider: mount.provider || null,
        properties: (mount as { properties?: Record<string, string> }).properties || {}
      })) as { is_dir?: boolean; entries?: number } | null
      const detail = data?.is_dir
        ? t('mounts.validation.existingDir', { count: Number(data?.entries || 0) })
        : t('mounts.validation.existingFile')
      toast.success(t('mounts.validation.existingSuccess', { path: mount.ufs_path, detail }))
    } catch (error) {
      toast.error(
        t('mounts.validation.existingFailed', {
          message: error instanceof Error ? error.message.replace(/^Error:\s*/i, '') : String(error)
        })
      )
    } finally {
      setRowValidating('')
    }
  }

  async function confirmDelete() {
    const mount = deleteTarget
    if (!mount) return
    setBusy(true)
    try {
      await api.deleteMount(mount.cv_path)
      setDeleteTarget(null)
      toast.success(t('mounts.alerts.deleted', { path: mount.cv_path }))
      await reload()
    } catch (error) {
      setDeleteTarget(null)
      toast.error(
        t('mounts.alerts.deleteFailed', {
          error: error instanceof Error ? error.message : String(error)
        })
      )
    } finally {
      setBusy(false)
    }
  }

  async function handleSaved() {
    toast.success(sheetMount ? t('mounts.alerts.updated') : t('mounts.alerts.created'))
    setSheetMount(undefined)
    await reload()
  }

  const emptyMessage = mounts.length
    ? t('mounts.table.emptySearch')
    : t('mounts.table.empty')

  /** Chevron that expands a tree node; intermediate and leaf nodes share it. */
  function treeToggle(node: MountTreeNode) {
    if (!node.children.length) return <span className="inline-block size-4 shrink-0" aria-hidden />
    const isOpen = expanded.has(node.path)
    return (
      <button
        type="button"
        className="flex size-4 shrink-0 items-center justify-center rounded text-muted-foreground hover:text-foreground"
        onClick={() => toggleNode(node.path)}
        aria-expanded={isOpen}
        aria-label={node.name}
        data-testid={`mount-tree-toggle-${node.path}`}
      >
        {isOpen ? <ChevronDownIcon className="size-3.5" /> : <ChevronRightIcon className="size-3.5" />}
      </button>
    )
  }

  /** One mount row; `treeNode` swaps the full path for the segment name + indent. */
  function renderMountRow(
    mount: MountInfo,
    options: { key: string; treeNode?: MountTreeNode; depth?: number }
  ) {
    const { key, treeNode, depth = 0 } = options
    const task = resyncTasks[mount.cv_path]
    const running = Boolean(task && !task.done)
    const fsMode = writeTypeLabel(mount.write_type) === 'fs_mode'
    return (
      <TableRow key={key} data-testid={`mount-row-${mount.cv_path}`}>
        <TableCell>
          <div
            className="flex min-w-0 items-center gap-2"
            style={treeNode ? { paddingLeft: depth * 16 } : undefined}
          >
            {treeNode ? treeToggle(treeNode) : null}
            <FolderSymlinkIcon
              className="size-4 shrink-0 text-muted-foreground"
              aria-hidden
              data-testid={`mount-icon-${mount.cv_path}`}
            />
            <span className="min-w-0 truncate font-medium" title={mount.cv_path}>
              {treeNode ? treeNode.name : mount.cv_path}
            </span>
          </div>
        </TableCell>
        <TableCell>
          <MonoText className="block truncate" title={String(mount.ufs_path || '')}>
            {String(mount.ufs_path || '-')}
          </MonoText>
        </TableCell>
        <TableCell>
          <StatusBadge
            value={writeTypeLabel(mount.write_type)}
            tone={fsMode ? 'positive' : 'info'}
          />
        </TableCell>
        <TableCell className="tabular-nums text-muted-foreground">
          {formatMountTtl(mount.ttl_ms)} · {t(`mounts.form.${ttlTypeLabel(mount.ttl_type)}`)}
        </TableCell>
        <TableCell className="text-muted-foreground">
          {providerLabel(mount.provider) || schemeOf(mount.ufs_path) || 'auto'}
        </TableCell>
        <TableCell>
          {fsMode ? (
            <Button
              variant="ghost"
              size="xs"
              className="text-primary"
              disabled={running}
              onClick={() => void resyncMount(mount)}
              data-testid={`mount-resync-${mount.cv_path}`}
            >
              {t('mounts.table.resync')}
            </Button>
          ) : (
            <span className="text-xs text-muted-foreground">
              {t('mounts.table.cacheModeLabel')}
            </span>
          )}
          {task && (
            <span
              className="mt-1 block truncate text-xs text-muted-foreground"
              title={resyncLabel(task)}
            >
              {resyncLabel(task)}
            </span>
          )}
        </TableCell>
        <TableCell>
          <div className="flex items-center justify-end gap-1">
            <Button
              variant="ghost"
              size="xs"
              className="text-primary"
              onClick={() => setSheetMount(mount)}
              data-testid={`mount-edit-${mount.cv_path}`}
            >
              {t('common.edit')}
            </Button>
            <Button
              variant="ghost"
              size="xs"
              className="text-primary"
              disabled={rowValidating === mount.cv_path}
              onClick={() => void validateExisting(mount)}
              data-testid={`mount-validate-${mount.cv_path}`}
            >
              {rowValidating === mount.cv_path && <Spinner data-icon="inline-start" />}
              {t('common.validate')}
            </Button>
            <Button
              variant="ghost"
              size="xs"
              className="text-destructive"
              onClick={() => setDeleteTarget(mount)}
              data-testid={`mount-delete-${mount.cv_path}`}
            >
              {t('common.delete')}
            </Button>
          </div>
        </TableCell>
      </TableRow>
    )
  }

  /** A path node with no mount of its own, kept only to hold the hierarchy. */
  function renderIntermediateRow(node: MountTreeNode, depth: number) {
    return (
      <TableRow key={`dir-${node.path}`} data-testid={`mount-tree-dir-${node.path}`}>
        <TableCell>
          <div className="flex min-w-0 items-center gap-2" style={{ paddingLeft: depth * 16 }}>
            {treeToggle(node)}
            <FolderIcon className="size-4 shrink-0 text-muted-foreground" aria-hidden />
            <span className="min-w-0 truncate text-muted-foreground" title={node.path}>
              {node.name}
            </span>
          </div>
        </TableCell>
        <TableCell colSpan={6} />
      </TableRow>
    )
  }

  return (
    <>
      <PageHeader
        eyebrow={t('nav.groupFilesystem')}
        title={t('mounts.table.title')}
        description={t('mounts.table.subtitle')}
        actions={
          <Button size="sm" onClick={() => setSheetMount(null)} data-testid="mounts-add">
            {t('mounts.table.addMount')}
          </Button>
        }
      />

      {error && <ErrorAlert message={error} />}

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          label={t('mounts.metrics.mountPoints')}
          value={mounts.length}
          meta={t('mounts.metrics.mountPointsMeta')}
          delta={delta('current')}
          tone="good"
        />
        <MetricCard
          label={t('mounts.metrics.fsMode')}
          value={fsModeCount}
          meta={t('mounts.metrics.fsModeMeta')}
          delta={delta('resync')}
          tone="neutral"
        />
        <MetricCard
          label={t('mounts.metrics.cacheMode')}
          value={cacheModeCount}
          meta={t('mounts.metrics.cacheModeMeta')}
          delta={delta('cache')}
          tone="neutral"
        />
        <MetricCard
          label={t('mounts.metrics.providers')}
          value={providerCount}
          meta={t('mounts.metrics.providersMeta')}
          delta={delta('auto')}
          tone="neutral"
        />
      </section>

      <section className="flex flex-col gap-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="flex flex-wrap items-center gap-2">
            <ToggleGroup
              type="single"
              value={modeFilter}
              onValueChange={(value) => {
                if (!value) return
                setPage(1)
                setModeFilter(value as ModeFilter)
              }}
              aria-label={t('mounts.filters.ariaLabel')}
              data-testid="mounts-mode-filter"
            >
              {MODE_FILTERS.map((filter) => (
                <ToggleGroupItem
                  key={filter}
                  value={filter}
                  size="sm"
                  data-testid={`mounts-mode-${filter}`}
                >
                  {t(`mounts.filters.${filter === 'all' ? 'all' : filter === 'fs_mode' ? 'fsMode' : 'cacheMode'}`)}{' '}
                  {filter === 'all' ? mounts.length : filter === 'fs_mode' ? fsModeCount : cacheModeCount}
                </ToggleGroupItem>
              ))}
            </ToggleGroup>

            <ToggleGroup
              type="single"
              value={viewMode}
              onValueChange={(value) => {
                if (!value) return
                setPage(1)
                setViewMode(value as ViewMode)
              }}
              aria-label={t('mounts.filters.viewAria')}
              data-testid="mounts-view-filter"
            >
              {VIEW_MODES.map((mode) => (
                <ToggleGroupItem
                  key={mode}
                  value={mode}
                  size="sm"
                  data-testid={`mounts-view-${mode}`}
                >
                  {t(mode === 'flat' ? 'mounts.filters.viewFlat' : 'mounts.filters.viewTree')}
                </ToggleGroupItem>
              ))}
            </ToggleGroup>
          </div>

          <Input
            value={pathQuery}
            onChange={(event) => {
              setPage(1)
              setPathQuery(event.target.value)
            }}
            placeholder={t('mounts.filters.pathSearchPlaceholder')}
            aria-label={t('mounts.filters.pathSearchAria')}
            className="max-w-md"
            data-testid="mounts-search"
          />
        </div>

        <Card className="gap-0 overflow-hidden py-0">
          {/* table-fixed makes the header widths authoritative, so a long path
              truncates inside its own column instead of widening the table. */}
          <Table className="table-fixed">
            <TableHeader className="bg-card">
              <TableRow>
                <TableHead className="w-[18%]">{t('mounts.table.curvinePath')}</TableHead>
                <TableHead className="w-[24%]">{t('mounts.table.ufsUri')}</TableHead>
                <TableHead className="w-[9%]">{t('mounts.table.mode')}</TableHead>
                <TableHead className="w-[9%]">{t('mounts.table.ttl')}</TableHead>
                <TableHead className="w-[10%]">{t('mounts.table.provider')}</TableHead>
                <TableHead className="w-[17%]">{t('mounts.table.resync')}</TableHead>
                <TableHead className="w-[13%] text-right">{t('common.actions')}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {viewMode === 'tree'
                ? visibleTreeRows.map(({ node, depth }) =>
                    node.mount
                      ? renderMountRow(node.mount, { key: node.path, treeNode: node, depth })
                      : renderIntermediateRow(node, depth)
                  )
                : pagedMounts.map((mount) =>
                    renderMountRow(mount, { key: String(mount.mount_id || mount.cv_path) })
                  )}
            </TableBody>
          </Table>

          {filteredMounts.length === 0 && !loading && (
            <div className="border-t border-border p-2">
              <EmptyNotice title={emptyMessage} />
            </div>
          )}
          {loading && (
            <div className="flex items-center justify-center gap-2 border-t border-border p-3 text-xs text-muted-foreground">
              <Spinner className="size-3.5" />
              {t('common.loading')}
            </div>
          )}

          {/* The tree shows the whole hierarchy, so paging only applies to the flat list. */}
          {viewMode === 'flat' && (
            <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border p-2">
              <div className="flex items-center gap-2">
                <span className="text-xs text-muted-foreground">
                  {t('common.pagination.pageSize')}
                </span>
                <Select
                  value={String(pageSize)}
                  onValueChange={(value) => {
                    setPage(1)
                    setPageSize(Number(value))
                  }}
                >
                  <SelectTrigger size="sm" className="w-20" data-testid="mounts-page-size">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectGroup>
                      {LIST_PAGE_SIZES.map((size) => (
                        <SelectItem key={size} value={String(size)}>
                          {size}
                        </SelectItem>
                      ))}
                    </SelectGroup>
                  </SelectContent>
                </Select>
                <span className="text-xs tabular-nums text-muted-foreground">
                  {t('common.pagination.total', { count: filteredMounts.length })}
                </span>
              </div>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  disabled={safePage <= 1 || loading}
                  onClick={() => setPage((current) => Math.max(1, current - 1))}
                  data-testid="mounts-prev-page"
                >
                  <ChevronLeftIcon data-icon="inline-start" />
                  {t('common.pagination.previous')}
                </Button>
                <span className="text-xs tabular-nums text-muted-foreground">
                  {t('common.pagination.pageOf', { page: safePage, pages: totalPages })}
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={safePage >= totalPages || loading}
                  onClick={() => setPage((current) => current + 1)}
                  data-testid="mounts-next-page"
                >
                  {t('common.pagination.next')}
                  <ChevronRightIcon data-icon="inline-end" />
                </Button>
              </div>
            </div>
          )}
        </Card>
      </section>

      {formSheet.mounted && (
        <MountFormSheet
          key={formSheet.value ? formSheet.value.cv_path : 'new'}
          open={formSheet.open}
          mount={formSheet.value ?? null}
          onClose={() => setSheetMount(undefined)}
          onSaved={() => void handleSaved()}
        />
      )}

      <ConfirmDialog
        open={deleteTarget !== null}
        title={t('common.delete')}
        description={
          deleteTarget ? t('mounts.alerts.deleteConfirm', { path: deleteTarget.cv_path }) : ''
        }
        confirmLabel={t('common.delete')}
        cancelLabel={t('common.cancel')}
        pending={busy}
        destructive
        onOpenChange={(open) => {
          if (!open) setDeleteTarget(null)
        }}
        onConfirm={() => void confirmDelete()}
        testId="mounts-delete-confirm"
      />
    </>
  )
}

/**
 * Depth-first walk of the tree that stops at collapsed nodes, so only what the
 * user has opened is rendered. Everything starts collapsed.
 */
function flattenMountTree(
  nodes: MountTreeNode[],
  expanded: Set<string>,
  depth = 0
): Array<{ node: MountTreeNode; depth: number }> {
  const rows: Array<{ node: MountTreeNode; depth: number }> = []
  for (const node of nodes) {
    rows.push({ node, depth })
    if (node.children.length && expanded.has(node.path)) {
      rows.push(...flattenMountTree(node.children, expanded, depth + 1))
    }
  }
  return rows
}
