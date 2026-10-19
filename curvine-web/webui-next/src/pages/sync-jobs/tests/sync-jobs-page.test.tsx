import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiProvider } from '@/api/api-context'
import type { ApiClient } from '@/api/client'
import type { JobInfo } from '@/api/types'
import { ApiError } from '@/api/error'
import { ToastProvider } from '@/components/ui/toast'
import { CopyFallbackProvider } from '@/components/copy-fallback'
import { TooltipProvider } from '@/components/ui/tooltip'
import { I18nProvider, LOCALE_STORAGE_KEY } from '@/i18n'
import type { UfsSyncJob } from '@/lib/ufs-sync/types'
import { UFS_SYNC_STORAGE_KEY } from '@/lib/ufs-sync/storage'
import { RefreshProvider } from '@/refresh/refresh-context'
import { SyncJobsPage } from '../../sync-jobs-page'

const LOAD_ID = 'job_0123456789abcdef0123456789abcdef'
const TRANSFER_ID = '12345678-1234-1234-1234-123456789abc'

function makeClient(
  items: UfsSyncJob[],
  options: { total?: number; jobStatus?: (jobId: string) => Promise<JobInfo> } = {}
) {
  const fetchUfsSyncJobs = vi.fn(async () => ({
    items,
    total: options.total ?? items.length
  }))
  const fetchJobStatus = vi.fn(options.jobStatus ?? (async () => ({}) as JobInfo))
  const client = { fetchUfsSyncJobs, fetchJobStatus } as unknown as ApiClient
  return { client, fetchUfsSyncJobs, fetchJobStatus }
}

function seedTrackedJobs(jobs: Record<string, UfsSyncJob>) {
  window.localStorage.setItem(UFS_SYNC_STORAGE_KEY, JSON.stringify(jobs))
}

function renderPage(client: ApiClient) {
  window.localStorage.setItem(LOCALE_STORAGE_KEY, 'en')
  return render(
    <MemoryRouter initialEntries={['/sync-jobs']}>
      <ApiProvider client={client}>
        <I18nProvider>
          <RefreshProvider>
            <ToastProvider>
              <CopyFallbackProvider>
                <TooltipProvider>
                  <SyncJobsPage />
                </TooltipProvider>
              </CopyFallbackProvider>
            </ToastProvider>
          </RefreshProvider>
        </I18nProvider>
      </ApiProvider>
    </MemoryRouter>
  )
}

describe('SyncJobsPage', () => {
  // The tracker persists rows on a debounce; a leak between tests would put
  // uploads into the next render.
  beforeEach(() => {
    window.localStorage.clear()
  })

  it('lists server jobs next to locally tracked uploads', async () => {
    seedTrackedJobs({
      '/up/local.bin': {
        path: '/up/local.bin',
        state: 'Uploading to Curvine',
        progress: 40,
        updated_at: Date.now()
      }
    })
    const { client } = makeClient([
      {
        path: '/lake/a.parquet',
        state: 'Completed',
        job_id: LOAD_ID,
        target_path: 's3://bucket/a.parquet',
        update_time_ms: 300
      } as UfsSyncJob
    ])
    renderPage(client)

    await waitFor(() =>
      expect(screen.getByTestId('sync-job-row-/lake/a.parquet')).toBeInTheDocument()
    )
    expect(screen.getByTestId('sync-job-row-/up/local.bin')).toBeInTheDocument()

    // The local-only row is what the "Locally tracked" metric counts.
    const localCard = screen.getByText('Locally tracked').closest('[data-testid="metric-card"]')
    expect(localCard).toHaveTextContent('1')
  })

  it('fetches once on mount and leaves further polls to the timer', async () => {
    const { client, fetchUfsSyncJobs } = makeClient([
      { path: '/lake/a.parquet', state: 'Completed', update_time_ms: 300 } as UfsSyncJob
    ])
    renderPage(client)

    await screen.findByTestId('sync-job-row-/lake/a.parquet')
    // Re-renders must not re-trigger the load effect; only the initial call
    // is allowed inside the 3s poll window.
    await new Promise((resolve) => setTimeout(resolve, 80))
    expect(fetchUfsSyncJobs).toHaveBeenCalledTimes(1)
  })

  it('stops polling when the poll toggle is switched off', async () => {
    const { client, fetchUfsSyncJobs } = makeClient([
      { path: '/lake/a.parquet', state: 'Completed', update_time_ms: 300 } as UfsSyncJob
    ])
    renderPage(client)

    await screen.findByTestId('sync-job-row-/lake/a.parquet')
    await userEvent.click(screen.getByTestId('sync-jobs-poll-toggle'))

    // Past the 3s poll interval with polling off: no extra fetch.
    await new Promise((resolve) => setTimeout(resolve, 3200))
    expect(fetchUfsSyncJobs).toHaveBeenCalledTimes(1)
  }, 10000)

  it('drops terminal rows that only exist in local storage', async () => {
    seedTrackedJobs({
      '/up/old.bin': {
        path: '/up/old.bin',
        state: 'Completed',
        done: true,
        updated_at: Date.now()
      }
    })
    const { client } = makeClient([])
    renderPage(client)

    await waitFor(() =>
      expect(screen.getByText(/No UFS sync jobs tracked yet/)).toBeInTheDocument()
    )
    expect(screen.queryByTestId('sync-job-row-/up/old.bin')).not.toBeInTheDocument()
  })

  it('expands a row and loads its job detail on demand', async () => {
    const { client, fetchJobStatus } = makeClient(
      [
        {
          path: '/lake/a.parquet',
          state: 'Syncing to UFS',
          job_id: LOAD_ID,
          update_time_ms: 300
        } as UfsSyncJob
      ],
      {
        jobStatus: async () =>
          ({
            job_id: LOAD_ID,
            state: 'running',
            tasks: [
              {
                task_id: 'task-1',
                source_path: '/lake/a.parquet',
                target_path: 's3://bucket/a.parquet',
                worker: 'worker-01',
                state: 'Completed'
              }
            ]
          }) as JobInfo
      }
    )
    renderPage(client)

    const toggle = await screen.findByTestId('sync-job-row-/lake/a.parquet-toggle')
    await userEvent.click(toggle)

    const detail = await screen.findByTestId('sync-job-row-/lake/a.parquet-detail')
    expect(fetchJobStatus).toHaveBeenCalledWith(LOAD_ID)
    expect(within(detail).getByText('task-1')).toBeInTheDocument()
    expect(within(detail).getByText('s3://bucket/a.parquet')).toBeInTheDocument()
  })

  it('explains when the master has not created the job yet instead of failing', async () => {
    const { client, fetchJobStatus } = makeClient([
      { path: '/lake/pending.parquet', state: 'Waiting for UFS sync', update_time_ms: 300 } as UfsSyncJob
    ])
    renderPage(client)

    await userEvent.click(await screen.findByTestId('sync-job-row-/lake/pending.parquet-toggle'))

    const detail = await screen.findByTestId('sync-job-row-/lake/pending.parquet-detail')
    expect(within(detail).getByText(/Waiting for the job to be created on the master/)).toBeInTheDocument()
    expect(fetchJobStatus).not.toHaveBeenCalled()
  })

  it('shows the same waiting message when the job detail 404s', async () => {
    const { client } = makeClient(
      [
        {
          path: '/lake/a.parquet',
          state: 'Syncing to UFS',
          job_id: LOAD_ID,
          update_time_ms: 300
        } as UfsSyncJob
      ],
      {
        jobStatus: async () => {
          throw new ApiError(404, 'job not found')
        }
      }
    )
    renderPage(client)

    await userEvent.click(await screen.findByTestId('sync-job-row-/lake/a.parquet-toggle'))

    const detail = await screen.findByTestId('sync-job-row-/lake/a.parquet-detail')
    expect(within(detail).getByText(/Waiting for the job to be created on the master/)).toBeInTheDocument()
  })

  it('filters rows through the debounced search box', async () => {
    const { client } = makeClient([
      { path: '/lake/a.parquet', state: 'Completed', update_time_ms: 300 } as UfsSyncJob,
      { path: '/lake/b.parquet', state: 'Completed', update_time_ms: 200 } as UfsSyncJob
    ])
    renderPage(client)

    await screen.findByTestId('sync-job-row-/lake/a.parquet')
    await userEvent.type(screen.getByTestId('sync-jobs-search'), 'b.parquet')

    await waitFor(() => {
      expect(screen.queryByTestId('sync-job-row-/lake/a.parquet')).not.toBeInTheDocument()
      expect(screen.getByTestId('sync-job-row-/lake/b.parquet')).toBeInTheDocument()
    })

    await userEvent.clear(screen.getByTestId('sync-jobs-search'))
    await waitFor(() => expect(screen.getByTestId('sync-job-row-/lake/a.parquet')).toBeInTheDocument())

    await userEvent.type(screen.getByTestId('sync-jobs-search'), 'nothing-matches')
    await waitFor(() =>
      expect(screen.getByText(/No sync jobs matched the current filters/)).toBeInTheDocument()
    )
  })

  it('filters rows by the status chips', async () => {
    const { client } = makeClient([
      { path: '/lake/a.parquet', state: 'Completed', update_time_ms: 300 } as UfsSyncJob,
      { path: '/lake/b.parquet', state: 'Syncing to UFS', update_time_ms: 200 } as UfsSyncJob
    ])
    renderPage(client)

    await screen.findByTestId('sync-job-row-/lake/b.parquet')
    await userEvent.click(screen.getByTestId('sync-jobs-status-completed'))

    expect(screen.getByTestId('sync-job-row-/lake/a.parquet')).toBeInTheDocument()
    expect(screen.queryByTestId('sync-job-row-/lake/b.parquet')).not.toBeInTheDocument()
  })

  it('routes the related job link by backend kind first, then id shape', async () => {
    const { client } = makeClient([
      {
        path: '/lake/load.parquet',
        state: 'Completed',
        job_id: LOAD_ID,
        update_time_ms: 400
      } as UfsSyncJob,
      {
        path: '/lake/transfer.parquet',
        state: 'Completed',
        job_id: TRANSFER_ID,
        update_time_ms: 300
      } as UfsSyncJob,
      {
        path: '/lake/override.parquet',
        state: 'Completed',
        job_id: LOAD_ID,
        related_job_kind: 'transfer',
        related_job_id: 'tx-1',
        update_time_ms: 200
      } as UfsSyncJob
    ])
    renderPage(client)

    expect(await screen.findByTestId('sync-job-row-/lake/load.parquet-related')).toHaveAttribute(
      'href',
      `/jobs?job=${LOAD_ID}`
    )
    expect(screen.getByTestId('sync-job-row-/lake/transfer.parquet-related')).toHaveAttribute(
      'href',
      `/transfers?job=${TRANSFER_ID}`
    )
    // The backend-resolved kind wins over the load-shaped job id.
    expect(screen.getByTestId('sync-job-row-/lake/override.parquet-related')).toHaveAttribute(
      'href',
      '/transfers?job=tx-1'
    )
  })

  it('shows the truncated banner when the server cut the list window', async () => {
    const { client } = makeClient(
      [{ path: '/lake/a.parquet', state: 'Completed', update_time_ms: 300 } as UfsSyncJob],
      { total: 2 }
    )
    renderPage(client)

    await screen.findByTestId('sync-job-row-/lake/a.parquet')
    expect(screen.getByTestId('sync-jobs-truncated')).toBeInTheDocument()
  })

  it('groups a folder upload batch into one row listing its member paths', async () => {
    seedTrackedJobs({
      '/up/a.bin': {
        path: '/up/a.bin',
        batch_id: 'batch-1',
        batch_label: 'Folder upload',
        state: 'Uploading to Curvine',
        progress: 50,
        updated_at: Date.now()
      },
      '/up/b.bin': {
        path: '/up/b.bin',
        batch_id: 'batch-1',
        batch_label: 'Folder upload',
        state: 'Uploading to Curvine',
        progress: 60,
        updated_at: Date.now()
      }
    })
    const { client } = makeClient([])
    renderPage(client)

    // The member rows collapse into the single batch row.
    const batchRow = await screen.findByTestId('sync-job-row-batch-batch-1')
    expect(batchRow).toHaveTextContent('Folder upload')
    expect(screen.queryByTestId('sync-job-row-/up/a.bin')).not.toBeInTheDocument()

    await userEvent.click(screen.getByTestId('sync-job-row-batch-batch-1-toggle'))

    const detail = await screen.findByTestId('sync-job-row-batch-batch-1-detail')
    expect(within(detail).getByText('/up/a.bin')).toBeInTheDocument()
    expect(within(detail).getByText('/up/b.bin')).toBeInTheDocument()
  })

  it('reports load failures without leaving the spinner up', async () => {
    const fetchUfsSyncJobs = vi.fn(async () => {
      throw new Error('boom')
    })
    const client = { fetchUfsSyncJobs, fetchJobStatus: vi.fn() } as unknown as ApiClient
    renderPage(client)

    expect(await screen.findByText(/Failed to load sync jobs: boom/)).toBeInTheDocument()
    expect(screen.queryByTestId('sync-jobs-status-filter')).toBeInTheDocument()
  })
})
