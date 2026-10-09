import { render, screen, waitFor } from '@testing-library/react'
import { describe, expect, it } from 'vitest'

import { ApiProvider } from '@/api/api-context'
import type { ApiClient } from '@/api/client'
import type { JobInfo } from '@/api/types'
import { ToastProvider } from '@/components/ui/toast'
import { TooltipProvider } from '@/components/ui/tooltip'
import { I18nProvider } from '@/i18n'
import { RefreshProvider } from '@/refresh/refresh-context'
import { JobsPage } from './jobs-page'

const JOB_ID = 'job_0123456789abcdef0123456789abcdef'

const job = {
  job_id: JOB_ID,
  state: 'running',
  status: 'running',
  source_type: 'load',
  path: '/source/a.parquet',
  target_path: '/target/a.parquet',
  progress: 42,
  total_files: 10,
  completed_files: 4,
  submitter: 'alice',
  update_time_ms: 1_750_000_000_000
} as JobInfo

function stubClient(items: JobInfo[]): ApiClient {
  return {
    fetchMountsData: async () => [],
    fetchJobsData: async () => ({ items, total: items.length, page_size: 500 }),
    fetchJobStatus: async () => job,
    cancelLoadJob: async () => job
  } as unknown as ApiClient
}

function renderPage(items: JobInfo[]) {
  return render(
    <ApiProvider client={stubClient(items)}>
      <I18nProvider>
        <RefreshProvider>
          <ToastProvider>
            <TooltipProvider>
              <JobsPage />
            </TooltipProvider>
          </ToastProvider>
        </RefreshProvider>
      </I18nProvider>
    </ApiProvider>
  )
}

describe('JobsPage rows', () => {
  it('keeps a job message out of the visible row, behind a warning icon', async () => {
    const message = 'waiting for scheduling capacity'
    const warned = { ...job, message } as JobInfo
    renderPage([warned])

    await waitFor(() => expect(screen.getByTestId(`job-row-${JOB_ID}`)).toBeInTheDocument())

    // The row is one line per cell now, so the message must not be rendered as
    // text; the icon's hover title is what keeps it reachable.
    expect(screen.queryByText(message)).not.toBeInTheDocument()
    const icon = screen.getByTestId(`job-message-${JOB_ID}`)
    expect(icon.closest('span')).toHaveAttribute('title', message)
  })

  it('shows no warning icon when the job has no message', async () => {
    renderPage([job])

    await waitFor(() => expect(screen.getByTestId(`job-row-${JOB_ID}`)).toBeInTheDocument())
    expect(screen.queryByTestId(`job-message-${JOB_ID}`)).not.toBeInTheDocument()
  })

  it('keeps the full job id reachable from the row title and the copy action', async () => {
    renderPage([job])

    await waitFor(() => expect(screen.getByTestId(`job-row-${JOB_ID}`)).toBeInTheDocument())
    const copy = screen.getByTestId(`job-copy-${JOB_ID}`)
    expect(copy).toHaveAttribute('title', expect.stringContaining(JOB_ID))
  })
})
