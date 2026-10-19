import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, useSearchParams } from 'react-router-dom'
import { describe, expect, it } from 'vitest'

import { ApiProvider } from '@/api/api-context'
import type { ApiClient } from '@/api/client'
import type { JobInfo } from '@/api/types'
import { ToastProvider } from '@/components/ui/toast'
import { CopyFallbackProvider } from '@/components/copy-fallback'
import { TooltipProvider } from '@/components/ui/tooltip'
import { I18nProvider, LOCALE_STORAGE_KEY } from '@/i18n'
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

function renderPage(items: JobInfo[], route = '/jobs') {
  return render(
    <MemoryRouter initialEntries={[route]}>
      <ApiProvider client={stubClient(items)}>
        <I18nProvider>
          <RefreshProvider>
            <ToastProvider>
              <CopyFallbackProvider>
                <TooltipProvider>
                  <JobsPage />
                </TooltipProvider>
              </CopyFallbackProvider>
            </ToastProvider>
          </RefreshProvider>
        </I18nProvider>
      </ApiProvider>
    </MemoryRouter>
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

function SearchProbe() {
  const [searchParams] = useSearchParams()
  return <div data-testid="search-probe">{searchParams.toString()}</div>
}

describe('JobsPage deep link', () => {
  it('opens the diagnostics sheet from a ?job= link and clears the param on close', async () => {
    renderPage([job], `/jobs?job=${JOB_ID}`)

    expect(await screen.findByTestId('job-diagnostics-sheet')).toBeInTheDocument()

    // Closing the sheet drops the URL parameter, so a reload does not reopen it.
    await userEvent.keyboard('{Escape}')
    await waitFor(() => expect(screen.queryByTestId('job-diagnostics-sheet')).not.toBeInTheDocument())
  })

  it('still exposes the deep link in the address bar while the sheet is open', async () => {
    window.localStorage.setItem(LOCALE_STORAGE_KEY, 'en')
    render(
      <MemoryRouter initialEntries={[`/jobs?job=${JOB_ID}`]}>
        <ApiProvider client={stubClient([job])}>
          <I18nProvider>
            <RefreshProvider>
              <ToastProvider>
                <CopyFallbackProvider>
                  <TooltipProvider>
                    <JobsPage />
                    <SearchProbe />
                  </TooltipProvider>
                </CopyFallbackProvider>
              </ToastProvider>
            </RefreshProvider>
          </I18nProvider>
        </ApiProvider>
      </MemoryRouter>
    )

    await screen.findByTestId('job-diagnostics-sheet')
    expect(screen.getByTestId('search-probe')).toHaveTextContent(`job=${JOB_ID}`)

    await userEvent.keyboard('{Escape}')
    await waitFor(() => expect(screen.getByTestId('search-probe')).toHaveTextContent(''))
  })
})
