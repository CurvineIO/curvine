import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'

import { ApiProvider } from '@/api/api-context'
import type { ApiClient } from '@/api/client'
import { I18nProvider, LOCALE_STORAGE_KEY } from '@/i18n'
import { SubmitTransferSheet } from '../submit-transfer-sheet'

describe('SubmitTransferSheet', () => {
  it('does not overwrite an existing destination unless requested', async () => {
    window.localStorage.setItem(LOCALE_STORAGE_KEY, 'en')
    const submitTransfer = vi.fn(async () => ({ job_id: 'job-test' }))
    const client = { submitTransfer } as unknown as ApiClient

    render(
      <ApiProvider client={client}>
        <I18nProvider>
          <SubmitTransferSheet open onClose={vi.fn()} onSubmitted={vi.fn()} />
        </I18nProvider>
      </ApiProvider>
    )

    expect(screen.getByTestId('transfer-overwrite')).toHaveTextContent(
      'Fail if destination exists'
    )
    expect(screen.getByText(/Overwriting may replace existing destination files/)).toBeInTheDocument()

    await userEvent.type(screen.getByTestId('transfer-source-path'), 's3://bucket/source')
    await userEvent.click(screen.getByTestId('transfer-submit'))

    await waitFor(() => expect(submitTransfer).toHaveBeenCalledWith({
      kind: 'load',
      source_path: 's3://bucket/source',
      target_path: null,
      overwrite: false
    }))
  })
})
