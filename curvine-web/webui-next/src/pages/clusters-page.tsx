/**
 * ClustersPage — `/clusters`.
 *
 * Lists every cluster the console can target (the process default plus the
 * files in the clusters directory) and owns the lifecycle around them: switch
 * the active cluster, probe a master, import a TOML document, edit one in place
 * and delete one.
 *
 * The list endpoint only projects a summary, so editing reads the file back
 * through `getClusterConfig` before prefilling the sheet. That read is the only
 * thing that can fail on its own: when it does, the sheet stays open on an empty
 * textarea and says the document has to be pasted in full, because a PUT always
 * replaces the whole file.
 */
import { useCallback, useEffect, useMemo, useState } from 'react'

import {
  LayersIcon,
  PencilIcon,
  PlusIcon,
  PlugZapIcon,
  RefreshCwIcon,
  RotateCwIcon,
  Trash2Icon
} from 'lucide-react'

import { useApi } from '@/api/api-context'
import { ApiError } from '@/api/error'
import type { ClusterProbe, ClusterStatus, ClusterSummary, ClusterValidation } from '@/api/types'
import { useCluster } from '@/cluster/cluster-context'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { EmptyNotice, ErrorAlert, TableSkeletonRows } from '@/components/data-state'
import { TextAreaField } from '@/components/form-fields'
import { PageHeader } from '@/components/page-header'
import { MonoText, MutedLine, StatusBadge, type StatusTone } from '@/components/resource-table'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle
} from '@/components/ui/sheet'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/components/ui/table'
import { useToast } from '@/components/ui/toast'
import { UnsavedChangesDialog } from '@/components/unsaved-changes-dialog'
import { useT } from '@/i18n'
import { hasUnsavedChanges } from '@/lib/form-dirty'
import { cn } from '@/lib/utils'
import { useAsync } from '@/lib/use-async'
import { useRefresh, useRefreshSignal } from '@/refresh/refresh-context'

/** `ready` is the only healthy state; `unreachable` is a warning, not a failure. */
const STATUS_TONE: Record<ClusterStatus, StatusTone> = {
  ready: 'positive',
  invalid: 'negative',
  unreachable: 'warning'
}

const SOURCE_CLASS: Record<ClusterSummary['source'], string> = {
  default: 'border-primary/35 bg-primary/10 text-primary',
  file: 'border-border bg-muted/40 text-muted-foreground'
}

/** Master addresses are a list; the table shows one per line. */
function MasterAddresses({ addresses }: { addresses: string[] }) {
  if (!addresses.length) return <MutedLine>—</MutedLine>
  return (
    <div className="flex min-w-0 flex-col gap-0.5" data-testid="cluster-masters">
      {addresses.map((address) => (
        <MonoText key={address}>{address}</MonoText>
      ))}
    </div>
  )
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

interface EditorState {
  mode: 'create' | 'edit'
  /** Present for `edit`, absent for `create`. */
  cluster?: ClusterSummary
}

/**
 * A config read that pasting cannot repair: the document belongs to the default
 * cluster, so a replacement would be refused by the backend as well.
 */
function isForbiddenConfigRead(error: unknown): boolean {
  return error instanceof ApiError && (error.code === 'not_removable' || error.status === 409)
}

/**
 * Import / replace sheet.
 *
 * Validation is a separate, explicit step: `保存` stays disabled until the
 * backend accepted the exact text that is on screen. Editing the textarea
 * invalidates the previous verdict, so a stale `ok` can never be saved.
 */
function ClusterEditorSheet({
  mode,
  cluster,
  onClose,
  onSaved
}: {
  mode: 'create' | 'edit'
  cluster?: ClusterSummary
  onClose(): void
  onSaved(saved: ClusterSummary): void
}) {
  const t = useT()
  const api = useApi()
  const [toml, setToml] = useState('')
  /**
   * Text the sheet opened with. Discard prompts compare against this, so merely
   * opening an edit (which prefills the stored document) is not a change.
   */
  const [baseline, setBaseline] = useState('')
  const [configLoading, setConfigLoading] = useState(mode === 'edit')
  /** Detail of a failed read; `null` while the read has not failed. */
  const [configError, setConfigError] = useState<string | null>(null)
  /** The document is not editable at all (default cluster), so pasting cannot help. */
  const [configBlocked, setConfigBlocked] = useState(false)
  const [validation, setValidation] = useState<ClusterValidation | null>(null)
  const [validating, setValidating] = useState(false)
  const [saving, setSaving] = useState(false)
  const [confirmDiscard, setConfirmDiscard] = useState(false)

  /**
   * Editing starts from the stored document, not from an empty box: the sheet
   * loads it once when it opens. A failed read must not close the sheet — the
   * user is then told to paste the complete file instead.
   */
  useEffect(() => {
    if (mode !== 'edit' || !cluster) return
    let cancelled = false
    setConfigLoading(true)
    setConfigError(null)
    setConfigBlocked(false)
    api
      .getClusterConfig(cluster.id)
      .then((document) => {
        if (cancelled) return
        setToml(document)
        setBaseline(document)
        // A prefilled document is still unvalidated on screen.
        setValidation(null)
      })
      .catch((loadFailure: unknown) => {
        if (cancelled) return
        setConfigError(errorMessage(loadFailure))
        setConfigBlocked(isForbiddenConfigRead(loadFailure))
      })
      .finally(() => {
        if (!cancelled) setConfigLoading(false)
      })
    return () => {
      cancelled = true
    }
    // Prefill happens once per opened cluster, not on every list refresh.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [api, cluster?.id, mode])

  function updateToml(next: string) {
    setToml(next)
    // The verdict was about the previous text.
    setValidation(null)
  }

  function requestClose() {
    if (hasUnsavedChanges(baseline, toml)) {
      setConfirmDiscard(true)
      return
    }
    onClose()
  }

  async function runValidation() {
    setValidating(true)
    try {
      const result = await api.validateCluster(toml)
      setValidation(result)
    } catch (error) {
      setValidation({ ok: false, errors: [t('clusters.validateFailed', { error: errorMessage(error) })], warnings: [] })
    } finally {
      setValidating(false)
    }
  }

  async function save() {
    if (!validation?.ok) return
    setSaving(true)
    try {
      const saved = mode === 'create' ? await api.createCluster(toml) : await api.updateCluster(cluster!.id, toml)
      onSaved(saved)
    } catch (error) {
      setValidation({
        ok: false,
        errors: [t('clusters.importFailed', { error: errorMessage(error) })],
        warnings: []
      })
    } finally {
      setSaving(false)
    }
  }

  return (
    <Sheet open onOpenChange={(next) => !next && requestClose()}>
      <SheetContent
        side="right"
        className="data-[side=right]:sm:max-w-2xl"
        onInteractOutside={(event) => event.preventDefault()}
      >
        <SheetHeader>
          <SheetTitle>{mode === 'create' ? t('clusters.importTitle') : t('clusters.editTitle')}</SheetTitle>
          <SheetDescription>
            {mode === 'create' ? t('clusters.importHint') : t('clusters.editHint')}
          </SheetDescription>
        </SheetHeader>

        <form
          className="flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto p-4"
          onSubmit={(event) => {
            event.preventDefault()
            void save()
          }}
        >
          {mode === 'edit' && cluster && (
            <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
              <span className="font-semibold text-foreground">{cluster.display_name}</span>
              <MonoText>{cluster.id}</MonoText>
            </div>
          )}

          <TextAreaField
            label={t('clusters.tomlLabel')}
            value={toml}
            onChange={updateToml}
            disabled={configLoading || configBlocked}
            placeholder={configLoading ? t('clusters.configLoading') : t('clusters.tomlPlaceholder')}
            description={configLoading ? t('clusters.configLoadingHint') : t('clusters.tomlHint')}
            rows={16}
            testId="cluster-toml"
          />

          {configError !== null && (
            <Alert
              variant={configBlocked ? 'destructive' : 'default'}
              className={cn(!configBlocked && 'border-warning/35 bg-warning/10 text-warning')}
              data-testid="cluster-config-error"
            >
              <AlertTitle>
                {configBlocked ? t('clusters.editUnavailable') : t('clusters.configLoadFailed')}
              </AlertTitle>
              <AlertDescription>{configError}</AlertDescription>
            </Alert>
          )}

          {validation && (
            <div className="flex flex-col gap-2" data-testid="cluster-validation">
              {validation.ok && (
                <Alert className="border-primary/30 bg-primary/5">
                  <AlertTitle>{t('clusters.validationOk')}</AlertTitle>
                  {validation.cluster && (
                    <AlertDescription>
                      <span className="block">{t('clusters.summaryId', { id: validation.cluster.id })}</span>
                      <span className="block">
                        {t('clusters.summaryName', { name: validation.cluster.display_name })}
                      </span>
                      <span className="block">
                        {t('clusters.summaryMasters', {
                          masters: validation.cluster.master_addrs.join(', ') || '—'
                        })}
                      </span>
                    </AlertDescription>
                  )}
                </Alert>
              )}

              {validation.errors.length > 0 && (
                <Alert variant="destructive" data-testid="cluster-validation-errors">
                  <AlertTitle>{t('clusters.validationErrors')}</AlertTitle>
                  <AlertDescription>
                    <ul className="list-disc pl-4">
                      {validation.errors.map((item) => (
                        <li key={item} className="break-words">
                          {item}
                        </li>
                      ))}
                    </ul>
                  </AlertDescription>
                </Alert>
              )}

              {validation.warnings.length > 0 && (
                <Alert
                  className="border-warning/35 bg-warning/10 text-warning"
                  data-testid="cluster-validation-warnings"
                >
                  <AlertTitle>{t('clusters.validationWarnings')}</AlertTitle>
                  <AlertDescription>
                    <ul className="list-disc pl-4">
                      {validation.warnings.map((item) => (
                        <li key={item} className="break-words">
                          {item}
                        </li>
                      ))}
                    </ul>
                  </AlertDescription>
                </Alert>
              )}
            </div>
          )}

          <SheetFooter className="mt-auto flex-row justify-between gap-2 px-0">
            <Button type="button" variant="ghost" onClick={requestClose} data-testid="cluster-cancel">
              {t('common.cancel')}
            </Button>
            <div className="flex flex-wrap items-center gap-2">
              <Button
                type="button"
                variant="outline"
                disabled={validating || configLoading || configBlocked || !toml.trim()}
                onClick={() => void runValidation()}
                data-testid="cluster-validate"
              >
                {validating ? t('clusters.validating') : t('clusters.validate')}
              </Button>
              <Button
                type="submit"
                disabled={!validation?.ok || saving || configLoading || configBlocked}
                data-testid="cluster-save"
              >
                {saving ? t('clusters.saving') : t('clusters.save')}
              </Button>
            </div>
          </SheetFooter>
        </form>

        <UnsavedChangesDialog
          open={confirmDiscard}
          onDiscard={() => {
            setConfirmDiscard(false)
            onClose()
          }}
          onKeepEditing={() => setConfirmDiscard(false)}
        />
      </SheetContent>
    </Sheet>
  )
}

export function ClustersPage() {
  const t = useT()
  const api = useApi()
  const toast = useToast()
  const { clusterId, setClusterId, refreshClusters } = useCluster()
  const { triggerRefresh } = useRefresh()

  const load = useCallback(() => api.fetchClusters(), [api])
  const { data, loading, error, reload, refresh } = useAsync(load, [load])
  useRefreshSignal(refresh)

  const clusters = useMemo<ClusterSummary[]>(() => (Array.isArray(data) ? data : []), [data])

  /**
   * An empty selection means "follow the backend default", so the default row
   * is the current one when nothing is pinned.
   */
  const currentId = clusterId || clusters.find((cluster) => cluster.source === 'default')?.id || ''

  const [editor, setEditor] = useState<EditorState | null>(null)
  const [deleteTarget, setDeleteTarget] = useState<ClusterSummary | null>(null)
  const [deleteInput, setDeleteInput] = useState('')
  const [deleteError, setDeleteError] = useState<string | null>(null)
  const [deleting, setDeleting] = useState(false)
  const [rescanning, setRescanning] = useState(false)
  const [probingId, setProbingId] = useState<string | null>(null)

  function handleSwitch(cluster: ClusterSummary) {
    setClusterId(cluster.id)
    triggerRefresh()
    toast.success(t('clusters.switched', { id: cluster.id }))
  }

  async function handleProbe(cluster: ClusterSummary) {
    setProbingId(cluster.id)
    try {
      const probe: ClusterProbe = await api.probeCluster(cluster.id)
      if (probe.reachable) {
        toast.success(
          t('clusters.probeReachable', {
            master: probe.active_master || t('common.notAvailable'),
            latency: probe.latency_ms ?? '—'
          })
        )
      } else {
        toast.error(
          t('clusters.probeUnreachable', { message: probe.message || t('common.unknown') })
        )
      }
    } catch (probeError) {
      toast.error(t('clusters.probeFailed', { error: errorMessage(probeError) }))
    } finally {
      setProbingId(null)
    }
  }

  async function handleRescan() {
    setRescanning(true)
    try {
      const list = await api.reloadClusters()
      toast.success(t('clusters.rescanned', { count: Array.isArray(list) ? list.length : 0 }))
      await reload()
      refreshClusters()
    } catch (rescanError) {
      toast.error(t('clusters.rescanFailed', { error: errorMessage(rescanError) }))
    } finally {
      setRescanning(false)
    }
  }

  function openDelete(cluster: ClusterSummary) {
    setDeleteTarget(cluster)
    setDeleteInput('')
    setDeleteError(null)
  }

  function closeDelete() {
    setDeleteTarget(null)
    setDeleteInput('')
    setDeleteError(null)
  }

  async function handleDelete() {
    if (!deleteTarget) return
    // Deleting a cluster file is not undoable, so the id itself is the confirmation.
    if (deleteInput.trim() !== deleteTarget.id) {
      setDeleteError(t('clusters.deleteMismatch'))
      return
    }
    setDeleting(true)
    try {
      await api.deleteCluster(deleteTarget.id)
      toast.success(t('clusters.deleted', { id: deleteTarget.id }))
      closeDelete()
      await reload()
      refreshClusters()
    } catch (deleteFailure) {
      setDeleteError(t('clusters.deleteFailed', { error: errorMessage(deleteFailure) }))
    } finally {
      setDeleting(false)
    }
  }

  function handleSaved(saved: ClusterSummary) {
    setEditor(null)
    toast.success(t('clusters.saved', { id: saved.id }))
    void reload()
    refreshClusters()
  }

  return (
    <>
      <PageHeader
        eyebrow={t('nav.groupFilesystem')}
        title={t('nav.clusters')}
        description={t('nav.clustersSubtitle')}
        actions={
          <div className="flex flex-wrap items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              disabled={rescanning}
              onClick={() => void handleRescan()}
              data-testid="clusters-rescan"
            >
              <RotateCwIcon data-icon="inline-start" />
              {rescanning ? t('clusters.rescanning') : t('clusters.rescan')}
            </Button>
            <Button
              variant="outline"
              size="sm"
              disabled={loading}
              onClick={() => void reload()}
              data-testid="clusters-refresh"
            >
              <RefreshCwIcon data-icon="inline-start" />
              {t('common.refresh')}
            </Button>
            <Button size="sm" onClick={() => setEditor({ mode: 'create' })} data-testid="cluster-import">
              <PlusIcon data-icon="inline-start" />
              {t('clusters.import')}
            </Button>
          </div>
        }
      />

      <div className="flex flex-col gap-4" data-testid="clusters-page">
        {error && <ErrorAlert message={error} className="border-destructive/25" />}

        <Card className="gap-0 py-0">
          {!loading && !error && clusters.length === 0 ? (
            <EmptyNotice
              title={t('clusters.empty')}
              description={t('clusters.emptyHint')}
              icon={LayersIcon}
              className="m-4"
            />
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>{t('clusters.table.id')}</TableHead>
                  <TableHead>{t('clusters.table.displayName')}</TableHead>
                  <TableHead>{t('clusters.table.masters')}</TableHead>
                  <TableHead>{t('clusters.table.status')}</TableHead>
                  <TableHead>{t('clusters.table.source')}</TableHead>
                  <TableHead className="text-right">{t('clusters.table.actions')}</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {loading && clusters.length === 0 ? (
                  <TableSkeletonRows columns={6} />
                ) : (
                  clusters.map((cluster) => {
                    const isCurrent = cluster.id === currentId
                    return (
                      <TableRow
                        key={cluster.id}
                        data-testid={`cluster-row-${cluster.id}`}
                        data-current={isCurrent ? 'true' : undefined}
                        className={cn(isCurrent && 'bg-primary/5')}
                      >
                        <TableCell>
                          <MonoText title={cluster.id}>{cluster.id}</MonoText>
                          {cluster.error && (
                            <MutedLine>
                              <span className="text-destructive">{cluster.error}</span>
                            </MutedLine>
                          )}
                        </TableCell>
                        <TableCell className="font-medium">{cluster.display_name}</TableCell>
                        <TableCell>
                          <MasterAddresses addresses={cluster.master_addrs} />
                        </TableCell>
                        <TableCell>
                          <StatusBadge
                            value={t(`clusters.status.${cluster.status}`)}
                            tone={STATUS_TONE[cluster.status] ?? 'neutral'}
                          />
                        </TableCell>
                        <TableCell>
                          <Badge variant="outline" className={cn('gap-1', SOURCE_CLASS[cluster.source])}>
                            {t(`clusters.source.${cluster.source}`)}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <div className="flex flex-wrap items-center justify-end gap-1">
                            <Button
                              size="xs"
                              variant={isCurrent ? 'secondary' : 'ghost'}
                              disabled={isCurrent}
                              onClick={() => handleSwitch(cluster)}
                              data-testid={`cluster-switch-${cluster.id}`}
                            >
                              {isCurrent ? t('clusters.current') : t('clusters.switch')}
                            </Button>
                            <Button
                              size="xs"
                              variant="ghost"
                              disabled={probingId === cluster.id}
                              onClick={() => void handleProbe(cluster)}
                              data-testid={`cluster-probe-${cluster.id}`}
                            >
                              <PlugZapIcon data-icon="inline-start" />
                              {probingId === cluster.id ? t('clusters.probing') : t('clusters.probe')}
                            </Button>
                            {cluster.editable && cluster.source !== 'default' && (
                              <Button
                                size="xs"
                                variant="ghost"
                                onClick={() => setEditor({ mode: 'edit', cluster })}
                                data-testid={`cluster-edit-${cluster.id}`}
                              >
                                <PencilIcon data-icon="inline-start" />
                                {t('common.edit')}
                              </Button>
                            )}
                            {cluster.removable && (
                              <Button
                                size="xs"
                                variant="ghost"
                                className="text-destructive hover:text-destructive"
                                onClick={() => openDelete(cluster)}
                                data-testid={`cluster-delete-${cluster.id}`}
                              >
                                <Trash2Icon data-icon="inline-start" />
                                {t('clusters.delete')}
                              </Button>
                            )}
                          </div>
                        </TableCell>
                      </TableRow>
                    )
                  })
                )}
              </TableBody>
            </Table>
          )}
        </Card>
      </div>

      {editor && (
        <ClusterEditorSheet
          mode={editor.mode}
          cluster={editor.cluster}
          onClose={() => setEditor(null)}
          onSaved={handleSaved}
        />
      )}

      <ConfirmDialog
        open={deleteTarget !== null}
        title={t('clusters.deleteTitle')}
        description={deleteTarget ? `${deleteTarget.id} — ${deleteTarget.display_name}` : ''}
        warning={
          deleteTarget && (
            <div className="flex flex-col gap-2">
              <span>{t('clusters.deleteConfirmHint')}</span>
              <label className="flex flex-col gap-1" htmlFor="cluster-delete-input">
                <span className="text-xs font-medium">{t('clusters.deleteInputLabel')}</span>
                <Input
                  id="cluster-delete-input"
                  value={deleteInput}
                  autoComplete="off"
                  spellCheck={false}
                  className="font-mono"
                  onChange={(event) => {
                    setDeleteInput(event.target.value)
                    setDeleteError(null)
                  }}
                  data-testid="cluster-delete-input"
                />
              </label>
              {deleteError && (
                <span className="text-xs font-medium" data-testid="cluster-delete-error">
                  {deleteError}
                </span>
              )}
            </div>
          )
        }
        confirmLabel={deleting ? t('common.working') : t('clusters.delete')}
        cancelLabel={t('common.cancel')}
        pending={deleting}
        destructive
        onOpenChange={(next) => {
          if (!next) closeDelete()
        }}
        onConfirm={() => void handleDelete()}
        testId="cluster-delete-confirm"
      />
    </>
  )
}
