import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import type { TransferInfo } from '@/api/types'
import { I18nProvider, LOCALE_STORAGE_KEY } from '@/i18n'
import { TransferDiagnosticsSheet } from '../transfer-diagnostics-sheet'

describe('TransferDiagnosticsSheet', () => {
  it('shows retained summary counts when task details have expired', async () => {
    window.localStorage.setItem(LOCALE_STORAGE_KEY, 'en')
    const transfer = {
      job_id: '12345678-1234-1234-1234-123456789abc',
      state: 'failed',
      failed_files: 12,
      running_files: 0,
      tasks: [],
      task_details_expired: true
    } as TransferInfo

    render(
      <I18nProvider>
        <TransferDiagnosticsSheet
          open
          transfer={transfer}
          loading={false}
          busy={false}
          taskNextPageToken=""
          taskPageSize={50}
          taskLoadingMore={false}
          onClose={vi.fn()}
          onCopy={vi.fn()}
          onRetry={vi.fn()}
          onLoadMore={vi.fn()}
        />
      </I18nProvider>
    )

    expect(await screen.findByText('Task details have expired; the summary counts are retained.'))
      .toBeInTheDocument()
    expect(screen.getByText('12')).toBeInTheDocument()
  })
})
