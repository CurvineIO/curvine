import { useCallback, useEffect, useState, type FormEvent, type ReactNode } from 'react'
import { ArrowLeftRight, Plus, RefreshCw, Users } from 'lucide-react'
import { Link } from 'react-router-dom'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Dialog } from '@/components/ui/dialog'
import { TransferStateBadge } from '@/components/transfer-state-badge'
import { api } from '@/lib/api'
import { useAutoRefreshEffect } from '@/lib/auto-refresh'
import { formatBytes, formatDuration, formatTimestamp } from '@/lib/format'
import {
  canCancel,
  canRetry,
  deriveTransferTarget,
  displayState,
  jobDurationMs,
  progressPercent,
  shortId,
  TRANSFER_STATES,
} from '@/lib/transfer'
import type { TransferJob, TransferKind, TransferState, TransferSubmitResponse } from '@/lib/transfer-types'
import type { MountInfo } from '@/lib/types'
import { cn } from '@/lib/utils'

interface Filters {
  kind: TransferKind | ''
  state: TransferState | ''
  submitter: string
  tenant: string
  path: string
  limit: number
}

const INITIAL_FILTERS: Filters = {
  kind: '',
  state: '',
  submitter: '',
  tenant: '',
  path: '',
  limit: 20,
}

const inputClassName =
  'h-9 w-full rounded-md border bg-secondary/40 px-3 text-sm outline-none placeholder:text-muted-foreground/70 focus:border-sky-500/60 focus:ring-2 focus:ring-sky-500/20'

export function TransfersPage() {
  const [jobs, setJobs] = useState<TransferJob[]>([])
  const [loaded, setLoaded] = useState(false)
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Filters>(INITIAL_FILTERS)
  const [applied, setApplied] = useState<Filters>(INITIAL_FILTERS)
  const [pageIndex, setPageIndex] = useState(0)
  const [pageTokens, setPageTokens] = useState<(string | null)[]>([null])
  const [nextPageToken, setNextPageToken] = useState<string | null>(null)
  const [refreshKey, setRefreshKey] = useState(0)
  const [submitOpen, setSubmitOpen] = useState(false)
  const [mounts, setMounts] = useState<MountInfo[]>([])

  useEffect(() => {
    let cancelled = false
    api
      .transferJobs({
        limit: applied.limit,
        page_token: pageTokens[pageIndex],
        kind: applied.kind,
        state: applied.state,
        submitter: applied.submitter,
        tenant: applied.tenant,
        path: applied.path,
      })
      .then(data => {
        if (!cancelled) {
          setJobs(data.jobs ?? [])
          setNextPageToken(data.next_page_token ?? null)
        }
      })
      .catch(() => {})
      .finally(() => {
        if (!cancelled) {
          setLoaded(true)
        }
      })
    return () => {
      cancelled = true
    }
  }, [applied, pageIndex, pageTokens, refreshKey])

  const autoRefresh = useCallback(() => setRefreshKey(key => key + 1), [])
  useAutoRefreshEffect(autoRefresh)

  const updateFilter = <K extends keyof Filters>(key: K, value: Filters[K]) => {
    setFilters(previous => ({ ...previous, [key]: value }))
  }

  const resetPaging = () => {
    setPageTokens([null])
    setPageIndex(0)
  }

  const applyFilters = (event: FormEvent) => {
    event.preventDefault()
    setNotice('')
    resetPaging()
    setApplied({ ...filters })
    setRefreshKey(key => key + 1)
  }

  const refresh = () => {
    setNotice('')
    resetPaging()
    setRefreshKey(key => key + 1)
  }

  const nextPage = () => {
    if (!nextPageToken) {
      return
    }
    setPageTokens(tokens => [...tokens.slice(0, pageIndex + 1), nextPageToken])
    setPageIndex(index => index + 1)
  }

  const prevPage = () => {
    setPageIndex(index => Math.max(0, index - 1))
  }

  const cancel = async (job: TransferJob) => {
    if (!window.confirm(`Cancel transfer ${job.job_id}?`)) {
      return
    }
    try {
      const result = await api.cancelTransfer(job.job_id)
      setNotice(`Cancel requested for ${result.job_id} (${result.state})`)
      setRefreshKey(key => key + 1)
    } catch {
      // The shared banner shows the message.
    }
  }

  const openSubmit = () => {
    setSubmitOpen(true)
    // Quiet: the target preview is optional, older masters may lack /api/mounts.
    api.mounts({ quiet: true }).then(setMounts).catch(() => {})
  }

  const onSubmitted = (result: TransferSubmitResponse) => {
    setSubmitOpen(false)
    setNotice(
      result.created
        ? `Transfer ${result.job_id} created (${displayState(result.state)})`
        : `Existing transfer ${result.job_id} returned (${displayState(result.state)})`,
    )
    resetPaging()
    setRefreshKey(key => key + 1)
  }

  const retry = async (job: TransferJob) => {
    if (!window.confirm(`Retry transfer ${job.job_id}? A new job will be created.`)) {
      return
    }
    try {
      const created = await api.retryTransfer(job.job_id)
      setNotice(`Retry submitted as ${created.job_id}`)
      setRefreshKey(key => key + 1)
    } catch {
      // The shared banner shows the message.
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h2 className="flex items-center gap-2 text-2xl font-bold tracking-tight">
            <ArrowLeftRight className="h-5 w-5 text-sky-400" />
            Transfers
          </h2>
          <p className="mt-1 text-sm text-muted-foreground">
            Load and export jobs executed by the transfer service
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={openSubmit}
            className="inline-flex items-center gap-2 rounded-md bg-emerald-500/15 px-3 py-2 text-sm font-medium text-emerald-400 transition-colors hover:bg-emerald-500/25"
          >
            <Plus className="h-4 w-4" />
            New Transfer
          </button>
          <Link
            to="/transfer-tenants"
            className="inline-flex items-center gap-2 rounded-md border px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          >
            <Users className="h-4 w-4" />
            Tenant Summary
          </Link>
          <button
            type="button"
            onClick={refresh}
            className="inline-flex items-center gap-2 rounded-md bg-sky-500/15 px-3 py-2 text-sm font-medium text-sky-400 transition-colors hover:bg-sky-500/25"
          >
            <RefreshCw className="h-4 w-4" />
            Refresh
          </button>
        </div>
      </div>

      <Card>
        <CardContent className="py-4">
          <form onSubmit={applyFilters} className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-7">
            <FilterField label="Kind">
              <select
                value={filters.kind}
                onChange={event => updateFilter('kind', event.target.value as TransferKind | '')}
                className={inputClassName}
              >
                <option value="">All</option>
                <option value="load">Load</option>
                <option value="export">Export</option>
              </select>
            </FilterField>
            <FilterField label="State">
              <select
                value={filters.state}
                onChange={event => updateFilter('state', event.target.value as TransferState | '')}
                className={inputClassName}
              >
                <option value="">All</option>
                {TRANSFER_STATES.map(state => (
                  <option key={state} value={state}>
                    {displayState(state)}
                  </option>
                ))}
              </select>
            </FilterField>
            <FilterField label="Submitter">
              <input
                value={filters.submitter}
                onChange={event => updateFilter('submitter', event.target.value)}
                placeholder="any"
                className={inputClassName}
              />
            </FilterField>
            <FilterField label="Tenant">
              <input
                value={filters.tenant}
                onChange={event => updateFilter('tenant', event.target.value)}
                placeholder="any"
                className={inputClassName}
              />
            </FilterField>
            <FilterField label="Target">
              <input
                value={filters.path}
                onChange={event => updateFilter('path', event.target.value)}
                placeholder="target path contains…"
                className={inputClassName}
              />
            </FilterField>
            <FilterField label="Page size">
              <select
                value={filters.limit}
                onChange={event => updateFilter('limit', Number(event.target.value))}
                className={inputClassName}
              >
                {[20, 50, 100].map(size => (
                  <option key={size} value={size}>
                    {size}
                  </option>
                ))}
              </select>
            </FilterField>
            <div className="flex items-end">
              <button
                type="submit"
                className="h-9 w-full rounded-md bg-sky-500/20 text-sm font-medium text-sky-300 transition-colors hover:bg-sky-500/30"
              >
                Apply
              </button>
            </div>
          </form>
        </CardContent>
      </Card>

      {notice && (
        <div className="rounded-lg border border-sky-500/40 bg-sky-500/10 px-4 py-2.5 text-sm text-sky-300">
          {notice}
        </div>
      )}

      <Card>
        <CardHeader className="flex-row items-center justify-between border-b py-4">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            {jobs.length} jobs on this page
          </CardTitle>
          <Badge variant="muted">Page {pageIndex + 1}</Badge>
        </CardHeader>
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <table className="w-full min-w-[1060px] text-sm">
              <thead>
                <tr className="border-b bg-secondary/40 text-xs uppercase tracking-wider text-muted-foreground">
                  <th className="px-4 py-3 text-left">Job</th>
                  <th className="px-4 py-3 text-left">Kind</th>
                  <th className="px-4 py-3 text-left">State</th>
                  <th className="px-4 py-3 text-left">Progress</th>
                  <th className="px-4 py-3 text-left">Submitter / Tenant</th>
                  <th className="px-4 py-3 text-left">Updated</th>
                  <th className="px-4 py-3 text-left">Duration</th>
                  <th className="px-4 py-3 text-right">Actions</th>
                </tr>
              </thead>
              <tbody>
                {jobs.length === 0 ? (
                  <tr>
                    <td colSpan={8} className="px-4 py-16 text-center text-muted-foreground">
                      {loaded ? 'No transfer jobs' : 'Loading transfer jobs…'}
                    </td>
                  </tr>
                ) : (
                  jobs.map(job => {
                    const percent = progressPercent(job.progress)
                    return (
                      <tr key={job.job_id} className="border-b last:border-b-0 hover:bg-accent/40">
                        <td className="px-4 py-3">
                          <Link
                            to={`/transfers/${encodeURIComponent(job.job_id)}`}
                            title={job.job_id}
                            className="font-mono text-sky-400 hover:underline"
                          >
                            {shortId(job.job_id)}
                          </Link>
                        </td>
                        <td className="px-4 py-3 text-xs uppercase tracking-wide text-muted-foreground">
                          {job.kind}
                        </td>
                        <td className="px-4 py-3">
                          <TransferStateBadge state={job.state} />
                        </td>
                        <td className="px-4 py-3">
                          <div className="h-1.5 w-full min-w-40 overflow-hidden rounded-full bg-border">
                            <div
                              className={cn(
                                'h-full rounded-full transition-[width] duration-300',
                                job.state === 'completed'
                                  ? 'bg-emerald-400'
                                  : job.state === 'failed'
                                    ? 'bg-red-400'
                                    : 'bg-sky-400',
                              )}
                              style={{ width: `${percent}%` }}
                            />
                          </div>
                          <div className="mt-1 font-mono text-xs text-muted-foreground">
                            {formatBytes(job.progress.loaded_size)} / {formatBytes(job.progress.total_size)} (
                            {percent}%)
                          </div>
                        </td>
                        <td className="px-4 py-3">
                          <div>{job.submitter || '-'}</div>
                          <div className="text-xs text-muted-foreground">{job.tenant || '-'}</div>
                        </td>
                        <td className="whitespace-nowrap px-4 py-3 font-mono text-xs text-muted-foreground">
                          {formatTimestamp(job.updated_at)}
                        </td>
                        <td className="whitespace-nowrap px-4 py-3 font-mono text-xs text-muted-foreground">
                          {formatDuration(jobDurationMs(job))}
                        </td>
                        <td className="whitespace-nowrap px-4 py-3 text-right">
                          {canCancel(job.state) && (
                            <button
                              type="button"
                              onClick={() => cancel(job)}
                              className="rounded-md border border-amber-500/40 px-2.5 py-1 text-xs font-medium text-amber-300 transition-colors hover:bg-amber-500/15"
                            >
                              Cancel
                            </button>
                          )}
                          {canRetry(job.state) && (
                            <button
                              type="button"
                              onClick={() => retry(job)}
                              className="ml-2 rounded-md border border-sky-500/40 px-2.5 py-1 text-xs font-medium text-sky-300 transition-colors hover:bg-sky-500/15"
                            >
                              Retry
                            </button>
                          )}
                        </td>
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>
          <div className="flex items-center justify-end gap-3 border-t px-4 py-3">
            <button
              type="button"
              onClick={prevPage}
              disabled={pageIndex === 0}
              className="rounded-md border px-3 py-1.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground disabled:cursor-not-allowed disabled:opacity-40"
            >
              Previous
            </button>
            <span className="text-xs text-muted-foreground">Page {pageIndex + 1}</span>
            <button
              type="button"
              onClick={nextPage}
              disabled={!nextPageToken}
              className="rounded-md border px-3 py-1.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground disabled:cursor-not-allowed disabled:opacity-40"
            >
              Next
            </button>
          </div>
        </CardContent>
      </Card>

      {submitOpen && (
        <SubmitTransferDialog
          mounts={mounts}
          onClose={() => setSubmitOpen(false)}
          onSubmitted={onSubmitted}
        />
      )}
    </div>
  )
}

function FilterField({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="flex flex-col gap-1.5">
      <span className="text-xs uppercase tracking-wide text-muted-foreground">{label}</span>
      {children}
    </label>
  )
}

/** Load/export submission form mirroring `cv load` / `cv export`. The target
 * path is derived from the mount table by the transfer service; the preview
 * here mirrors that mapping (load: UFS -> Curvine, export: Curvine -> UFS). */
function SubmitTransferDialog({
  mounts,
  onClose,
  onSubmitted,
}: {
  mounts: MountInfo[]
  onClose: () => void
  onSubmitted: (result: TransferSubmitResponse) => void
}) {
  const [kind, setKind] = useState<TransferKind>('load')
  const [sourcePath, setSourcePath] = useState('')
  const [filesText, setFilesText] = useState('')
  const [overwrite, setOverwrite] = useState(true)
  const [submitter, setSubmitter] = useState('')
  const [tenant, setTenant] = useState('')
  const [busy, setBusy] = useState(false)

  const source = sourcePath.trim()
  const target = deriveTransferTarget(kind, source, mounts)

  const submit = async (event: FormEvent) => {
    event.preventDefault()
    if (!source || busy) {
      return
    }
    setBusy(true)
    try {
      const result = await api.submitTransfer({
        kind,
        source_path: source,
        files: kind === 'load' && filesText.trim() ? filesText.split('\n') : undefined,
        overwrite,
        submitter: submitter.trim() || undefined,
        tenant: tenant.trim() || undefined,
      })
      onSubmitted(result)
    } catch {
      // The shared banner shows the message.
    } finally {
      setBusy(false)
    }
  }

  return (
    <Dialog
      open
      onClose={onClose}
      title="New transfer"
      className="max-w-xl"
      footer={
        <>
          <button
            type="button"
            onClick={onClose}
            disabled={busy}
            className="rounded-md bg-secondary px-4 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-accent disabled:cursor-not-allowed disabled:opacity-60"
          >
            Cancel
          </button>
          <button
            type="submit"
            form="transfer-submit-form"
            disabled={busy || !source}
            className="rounded-md bg-sky-500/15 px-4 py-2 text-sm font-medium text-sky-400 transition-colors hover:bg-sky-500/25 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {busy ? 'Submitting…' : 'Submit transfer'}
          </button>
        </>
      }
    >
      <form id="transfer-submit-form" onSubmit={submit}>
        <fieldset disabled={busy} className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-2">
            <FilterField label="Kind">
              <select
                value={kind}
                onChange={event => setKind(event.target.value as TransferKind)}
                className={inputClassName}
              >
                <option value="load">Load (UFS → Curvine)</option>
                <option value="export">Export (Curvine → UFS)</option>
              </select>
            </FilterField>
            <FilterField label="Submitter">
              <input
                value={submitter}
                onChange={event => setSubmitter(event.target.value)}
                placeholder="http-api (default)"
                className={inputClassName}
              />
            </FilterField>
          </div>
          <FilterField label={kind === 'load' ? 'Source path (UFS)' : 'Source path (Curvine)'}>
            <input
              value={sourcePath}
              onChange={event => setSourcePath(event.target.value)}
              placeholder={kind === 'load' ? 's3://bucket/data or hdfs://namespace/dir' : '/data'}
              className={inputClassName}
            />
          </FilterField>
          <div className="rounded-md border bg-secondary/30 px-3 py-2 text-xs">
            <span className="uppercase tracking-wide text-muted-foreground">Target (preview): </span>
            {!source ? (
              <span className="text-muted-foreground">enter a source path</span>
            ) : target ? (
              <span className="font-mono text-sky-400">{target}</span>
            ) : mounts.length > 0 ? (
              <span className="text-amber-300">no matching mount; the transfer service will reject this source</span>
            ) : (
              <span className="text-muted-foreground">derived by the transfer service</span>
            )}
          </div>
          {kind === 'load' && (
            <FilterField label="File list (optional)">
              <textarea
                value={filesText}
                onChange={event => setFilesText(event.target.value)}
                rows={4}
                placeholder={
                  'One file per line, relative to the source path.\nBlank lines and # comments are ignored.\nLeave empty to load everything under the source.'
                }
                className="w-full rounded-md border bg-secondary/40 px-3 py-2 font-mono text-xs outline-none placeholder:text-muted-foreground/70 focus:border-sky-500/60 focus:ring-2 focus:ring-sky-500/20"
              />
            </FilterField>
          )}
          <div className="grid gap-4 sm:grid-cols-2">
            <FilterField label="Tenant">
              <input
                value={tenant}
                onChange={event => setTenant(event.target.value)}
                placeholder="default"
                className={inputClassName}
              />
            </FilterField>
            <label className="flex items-end gap-2 pb-2 text-sm text-muted-foreground">
              <input
                type="checkbox"
                checked={overwrite}
                onChange={event => setOverwrite(event.target.checked)}
                className="h-3.5 w-3.5 accent-sky-500"
              />
              Overwrite existing files
            </label>
          </div>
        </fieldset>
      </form>
    </Dialog>
  )
}
