import { useCallback, useEffect, useState, type ReactNode } from 'react'
import { ArrowLeft, RefreshCw } from 'lucide-react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { TransferStateBadge } from '@/components/transfer-state-badge'
import { api } from '@/lib/api'
import { useAutoRefreshEffect } from '@/lib/auto-refresh'
import { formatBytes, formatTimestamp } from '@/lib/format'
import { canCancel, canRetry, isTerminalState, progressPercent } from '@/lib/transfer'
import type {
  TransferJob,
  TransferTask,
  TransferTaskSummary,
} from '@/lib/transfer-types'
import { cn } from '@/lib/utils'

const TASK_PAGE_SIZE = 50

const SUMMARY_ITEMS: {
  key: keyof TransferTaskSummary
  label: string
  className?: string
  bytes?: boolean
}[] = [
  { key: 'pending', label: 'pending' },
  { key: 'running', label: 'running', className: 'border-sky-500/40 text-sky-400' },
  { key: 'completed', label: 'completed', className: 'border-emerald-500/40 text-emerald-400' },
  { key: 'failed', label: 'failed', className: 'border-red-500/40 text-red-400' },
  { key: 'canceled', label: 'canceled' },
  { key: 'stale', label: 'stale' },
  { key: 'completed_size', label: 'completed size', className: 'text-sky-400', bytes: true },
]

export function TransferDetailRoute() {
  const { jobId = '' } = useParams()
  return <TransferDetailPage key={jobId} jobId={jobId} />
}

function TransferDetailPage({ jobId }: { jobId: string }) {
  const navigate = useNavigate()
  const [job, setJob] = useState<TransferJob | null>(null)
  const [summary, setSummary] = useState<TransferTaskSummary | null>(null)
  const [tasks, setTasks] = useState<TransferTask[]>([])
  const [loaded, setLoaded] = useState(false)
  const [notice, setNotice] = useState('')
  const [taskPageIndex, setTaskPageIndex] = useState(0)
  const [taskPageTokens, setTaskPageTokens] = useState<(string | null)[]>([null])
  const [nextTaskPageToken, setNextTaskPageToken] = useState<string | null>(null)
  const [refreshKey, setRefreshKey] = useState(0)

  useEffect(() => {
    let cancelled = false
    api
      .transferJob(jobId, {
        limit: TASK_PAGE_SIZE,
        page_token: taskPageTokens[taskPageIndex],
      })
      .then(data => {
        if (!cancelled) {
          setJob(data.job)
          setSummary(data.task_summary)
          setTasks(data.tasks ?? [])
          setNextTaskPageToken(data.next_page_token ?? null)
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
  }, [jobId, taskPageIndex, taskPageTokens, refreshKey])

  // A finished job only changes through a retry, so polling it is pointless.
  const autoRefresh = useCallback(() => {
    if (job && isTerminalState(job.state)) {
      return
    }
    setRefreshKey(key => key + 1)
  }, [job])
  useAutoRefreshEffect(autoRefresh)

  const refresh = () => {
    setNotice('')
    setRefreshKey(key => key + 1)
  }

  const nextTaskPage = () => {
    if (!nextTaskPageToken) {
      return
    }
    setTaskPageTokens(tokens => [...tokens.slice(0, taskPageIndex + 1), nextTaskPageToken])
    setTaskPageIndex(index => index + 1)
  }

  const prevTaskPage = () => {
    setTaskPageIndex(index => Math.max(0, index - 1))
  }

  const cancel = async () => {
    if (!window.confirm(`Cancel transfer ${jobId}?`)) {
      return
    }
    try {
      const result = await api.cancelTransfer(jobId)
      setNotice(`Cancel requested (${result.state})`)
      setRefreshKey(key => key + 1)
    } catch {
      // The shared banner shows the message.
    }
  }

  const retry = async () => {
    if (!window.confirm('Retry this transfer? A new job will be created.')) {
      return
    }
    try {
      const created = await api.retryTransfer(jobId)
      navigate(`/transfers/${encodeURIComponent(created.job_id)}`)
    } catch {
      // The shared banner shows the message.
    }
  }

  if (!job) {
    return (
      <div className="rounded-lg border border-dashed px-6 py-16 text-center text-sm text-muted-foreground">
        {loaded ? 'Transfer job not found.' : 'Loading transfer…'}
      </div>
    )
  }

  const percent = progressPercent(job.progress)

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div className="min-w-0">
          <Link
            to="/transfers"
            className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-sky-400"
          >
            <ArrowLeft className="h-3.5 w-3.5" />
            Transfers
          </Link>
          <h2 className="mt-1 break-all font-mono text-xl font-bold">{job.job_id}</h2>
          <div className="mt-1.5 flex flex-wrap items-center gap-3 text-xs text-muted-foreground">
            <span className="uppercase tracking-wide">{job.kind}</span>
            <TransferStateBadge state={job.state} />
            <span>run {job.run_id}</span>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {canCancel(job.state) && (
            <button
              type="button"
              onClick={cancel}
              className="rounded-md border border-amber-500/40 px-3 py-2 text-sm font-medium text-amber-300 transition-colors hover:bg-amber-500/15"
            >
              Cancel
            </button>
          )}
          {canRetry(job.state) && (
            <button
              type="button"
              onClick={retry}
              className="rounded-md border border-sky-500/40 px-3 py-2 text-sm font-medium text-sky-300 transition-colors hover:bg-sky-500/15"
            >
              Retry
            </button>
          )}
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

      {notice && (
        <div className="rounded-lg border border-sky-500/40 bg-sky-500/10 px-4 py-2.5 text-sm text-sky-300">
          {notice}
        </div>
      )}

      <Card>
        <CardContent className="space-y-3 py-5">
          <div className="flex flex-wrap items-center justify-between gap-2 font-mono text-xs text-muted-foreground">
            <span className="uppercase tracking-widest">Progress</span>
            <span>
              {formatBytes(job.progress.loaded_size)} / {formatBytes(job.progress.total_size)} ({percent}%)
            </span>
          </div>
          <div className="h-2 w-full overflow-hidden rounded-full bg-border">
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
          {job.progress.message && (
            <p className="text-xs text-muted-foreground">{job.progress.message}</p>
          )}
          {summary && (
            <div className="flex flex-wrap gap-2 pt-1">
              {SUMMARY_ITEMS.map(item => (
                <span
                  key={item.key}
                  className={cn(
                    'rounded-md border bg-secondary/40 px-2 py-0.5 text-xs text-muted-foreground',
                    item.className,
                  )}
                >
                  {item.label}: {item.bytes ? formatBytes(summary[item.key]) : summary[item.key]}
                </span>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="border-b py-4">
          <CardTitle className="text-sm uppercase tracking-widest text-sky-400">Details</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 py-5 sm:grid-cols-2 xl:grid-cols-3">
          <Meta label="Source" value={job.source_path} mono />
          <Meta label="Target" value={job.target_path} mono />
          <Meta label="Submitter" value={job.submitter || '-'} />
          <Meta label="Tenant" value={job.tenant || '-'} />
          <Meta label="Owner" value={job.owner || '-'} />
          <Meta label="Lease epoch" value={job.lease_epoch ?? '-'} />
          <Meta
            label="Lease expires"
            value={job.lease_expire_at ? formatTimestamp(job.lease_expire_at) : '-'}
          />
          <Meta label="Metadata epoch" value={job.cv_metadata_epoch ?? '-'} />
          <Meta label="Created" value={formatTimestamp(job.created_at)} mono />
          <Meta label="Updated" value={formatTimestamp(job.updated_at)} mono />
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex-row items-center justify-between border-b py-4">
          <CardTitle className="text-sm uppercase tracking-widest text-sky-400">Tasks</CardTitle>
          {tasks.length > 0 && (
            <Badge variant="muted">Page {taskPageIndex + 1}</Badge>
          )}
        </CardHeader>
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <table className="w-full min-w-[960px] text-sm">
              <thead>
                <tr className="border-b bg-secondary/40 text-xs uppercase tracking-wider text-muted-foreground">
                  <th className="px-4 py-3 text-left">Task</th>
                  <th className="px-4 py-3 text-left">State</th>
                  <th className="px-4 py-3 text-left">Worker</th>
                  <th className="px-4 py-3 text-left">Attempt</th>
                  <th className="px-4 py-3 text-left">Progress</th>
                  <th className="px-4 py-3 text-left">Updated</th>
                  <th className="px-4 py-3 text-left">Message</th>
                </tr>
              </thead>
              <tbody>
                {tasks.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="px-4 py-14 text-center text-muted-foreground">
                      {loaded ? 'No tasks in this page' : 'Loading tasks…'}
                    </td>
                  </tr>
                ) : (
                  tasks.map(task => {
                    const taskPercent = progressPercent(task.progress)
                    return (
                      <tr key={task.task_id} className="border-b last:border-b-0 hover:bg-accent/40">
                        <td className="px-4 py-3 font-mono text-xs" title={task.task_id}>
                          {task.task_id ? task.task_id.slice(0, 12) : '-'}
                        </td>
                        <td className="px-4 py-3">
                          <TransferStateBadge state={task.state} />
                        </td>
                        <td className="px-4 py-3">
                          <div>{task.worker_id || '-'}</div>
                          <div className="text-xs text-muted-foreground">{task.worker_session_id || ''}</div>
                        </td>
                        <td className="px-4 py-3">
                          {task.attempt_id}
                          {task.retry_count > 0 && (
                            <span className="text-xs text-muted-foreground"> ({task.retry_count} retries)</span>
                          )}
                        </td>
                        <td className="px-4 py-3">
                          <div className="h-1.5 w-full min-w-32 overflow-hidden rounded-full bg-border">
                            <div
                              className="h-full rounded-full bg-sky-400 transition-[width] duration-300"
                              style={{ width: `${taskPercent}%` }}
                            />
                          </div>
                          <div className="mt-1 font-mono text-xs text-muted-foreground">
                            {formatBytes(task.progress.loaded_size)} / {formatBytes(task.progress.total_size)}
                          </div>
                        </td>
                        <td className="whitespace-nowrap px-4 py-3 font-mono text-xs text-muted-foreground">
                          {formatTimestamp(task.updated_at)}
                        </td>
                        <td className="max-w-80 break-words px-4 py-3 text-xs text-muted-foreground">
                          {task.progress.message || '-'}
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
              onClick={prevTaskPage}
              disabled={taskPageIndex === 0}
              className="rounded-md border px-3 py-1.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground disabled:cursor-not-allowed disabled:opacity-40"
            >
              Previous
            </button>
            <button
              type="button"
              onClick={nextTaskPage}
              disabled={!nextTaskPageToken}
              className="rounded-md border px-3 py-1.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground disabled:cursor-not-allowed disabled:opacity-40"
            >
              Next
            </button>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

function Meta({
  label,
  value,
  mono,
}: {
  label: string
  value: ReactNode
  mono?: boolean
}) {
  return (
    <div className="min-w-0">
      <div className="text-xs uppercase tracking-wide text-muted-foreground">{label}</div>
      <div className={cn('mt-0.5 break-all text-sm', mono && 'font-mono')}>{value}</div>
    </div>
  )
}
