import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { CopyFallbackProvider, useCopy } from './copy-fallback'
import { I18nProvider } from '@/i18n'
import { ToastProvider } from '@/components/ui/toast'

function Trigger({ text = '/demo/docs/hello.txt' }: { text?: string }) {
  const { copy } = useCopy()
  return (
    <button type="button" onClick={() => void copy(text, 'Copied')}>
      copy it
    </button>
  )
}

function renderTrigger(text?: string) {
  return render(
    <I18nProvider>
      <ToastProvider>
        <CopyFallbackProvider>
          <Trigger text={text} />
        </CopyFallbackProvider>
      </ToastProvider>
    </I18nProvider>
  )
}

/** Plain HTTP: no clipboard API, and the legacy path's word cannot be taken. */
function setSecureContext(value: boolean) {
  Object.defineProperty(window, 'isSecureContext', { value, configurable: true })
}

const originalClipboard = Object.getOwnPropertyDescriptor(navigator, 'clipboard')

afterEach(() => {
  Object.defineProperty(navigator, 'clipboard', originalClipboard!)
  setSecureContext(true)
})

describe('CopyFallbackProvider', () => {
  it('hands the text over, focused and selected, when the browser will not copy', async () => {
    setSecureContext(false)
    Object.defineProperty(navigator, 'clipboard', { value: undefined, configurable: true })
    Object.defineProperty(document, 'execCommand', { value: vi.fn(() => true), configurable: true })
    renderTrigger()

    await userEvent.click(screen.getByRole('button', { name: 'copy it' }))

    const field = await screen.findByTestId('copy-fallback-value')
    expect(field).toHaveValue('/demo/docs/hello.txt')

    // One keystroke is the point: focused, and the whole value already selected.
    await waitFor(() => expect(document.activeElement).toBe(field))
    const input = field as HTMLInputElement
    expect(input.selectionStart).toBe(0)
    expect(input.selectionEnd).toBe('/demo/docs/hello.txt'.length)

    await userEvent.click(screen.getByTestId('copy-fallback-close'))
    await waitFor(() => expect(screen.queryByTestId('copy-fallback')).not.toBeInTheDocument())
  })

  it('says nothing about it when the browser copied on its own', async () => {
    // The test setup is a secure context with a working clipboard, i.e. an HTTPS deployment.
    renderTrigger()

    await userEvent.click(screen.getByRole('button', { name: 'copy it' }))

    expect(await screen.findByTestId('toast-success')).toHaveTextContent('Copied')
    expect(screen.queryByTestId('copy-fallback')).not.toBeInTheDocument()
  })
})
