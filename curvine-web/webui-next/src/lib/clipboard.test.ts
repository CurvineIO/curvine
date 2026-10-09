import { afterEach, describe, expect, it, vi } from 'vitest'

import { copyToClipboard } from './clipboard'

/** Chromium and Firefox take the textarea path on every desktop OS. */
const CHROMIUM_UA =
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
const FIREFOX_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0'
/** Safari answers `execCommand('copy')` with true and copies nothing unless it gets a Range. */
const SAFARI_UA =
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15'

const originalClipboard = Object.getOwnPropertyDescriptor(navigator, 'clipboard')

function setUserAgent(agent: string) {
  Object.defineProperty(navigator, 'userAgent', { value: agent, configurable: true })
}

function setClipboard(value: unknown) {
  Object.defineProperty(navigator, 'clipboard', { value, configurable: true })
}

/** jsdom ships no `execCommand`; the legacy path can only run once a test installs one. */
function setExecCommand(implementation: (command: string) => boolean) {
  const spy = vi.fn(implementation)
  Object.defineProperty(document, 'execCommand', { value: spy, configurable: true })
  return spy
}

/** A plain-HTTP page is not a secure context, which is where the fallback matters. */
function setSecureContext(value: boolean) {
  Object.defineProperty(window, 'isSecureContext', { value, configurable: true })
}

afterEach(() => {
  Object.defineProperty(navigator, 'clipboard', originalClipboard!)
  delete (navigator as unknown as Record<string, unknown>).userAgent
  delete (document as unknown as Record<string, unknown>).execCommand
  setSecureContext(true)
  document.getSelection()?.removeAllRanges()
  document.body.innerHTML = ''
})

describe('copyToClipboard', () => {
  it('uses the async clipboard API when the browser exposes it', async () => {
    const writeText = vi.fn(async () => {})
    setClipboard({ writeText })
    const execCommand = setExecCommand(() => true)

    await expect(copyToClipboard('/data/sales.csv')).resolves.toBe('copied')

    expect(writeText).toHaveBeenCalledWith('/data/sales.csv')
    // The secure path must not fall through to the legacy one.
    expect(execCommand).not.toHaveBeenCalled()
    expect(document.body.childElementCount).toBe(0)
  })

  it('asks for a manual copy on a page without a secure context', async () => {
    // The deployment case: plain HTTP, where `navigator.clipboard` does not exist and the
    // legacy path answers whatever it likes. Measured in a browser, it answered true while
    // the clipboard stayed empty, so its result cannot be reported as a copy.
    setSecureContext(false)
    setUserAgent(CHROMIUM_UA)
    setClipboard(undefined)
    const execCommand = setExecCommand(() => true)

    await expect(copyToClipboard('/data/plain-http.csv')).resolves.toBe('manual')

    // It still tried: where the legacy path works, the user just dismisses the field.
    expect(execCommand).toHaveBeenCalledWith('copy')
    expect(document.body.childElementCount).toBe(0)
  })

  it('asks for a manual copy when the async API rejects and the legacy path reports success', async () => {
    setUserAgent(CHROMIUM_UA)
    setClipboard({
      writeText: vi.fn(async () => {
        throw new DOMException('Document is not focused', 'NotAllowedError')
      })
    })
    let scratchValue: string | null = null
    const execCommand = setExecCommand(() => {
      scratchValue = (document.querySelector('textarea') as HTMLTextAreaElement | null)?.value ?? null
      return true
    })

    await expect(copyToClipboard('/data/fallback.csv')).resolves.toBe('manual')

    expect(execCommand).toHaveBeenCalledWith('copy')
    expect(scratchValue).toBe('/data/fallback.csv')
  })

  it('copies through a hidden textarea on Chromium and Firefox', async () => {
    for (const agent of [CHROMIUM_UA, FIREFOX_UA]) {
      setUserAgent(agent)
      setClipboard(undefined)
      let selected: { value: string; position: string; left: string } | null = null
      setExecCommand(() => {
        const node = document.querySelector('textarea') as HTMLTextAreaElement
        selected = { value: node.value, position: node.style.position, left: node.style.left }
        return true
      })

      await expect(copyToClipboard('/data/report.parquet')).resolves.toBe('manual')

      // Keeping the element on-screen is what WebKit needs and what keeps focus() from
      // dragging the page; an off-viewport node is the trick that stopped working.
      expect(selected!.position).toBe('fixed')
      expect(selected!.left).toBe('0px')
      expect(selected!.value).toBe('/data/report.parquet')
      expect(document.body.childElementCount).toBe(0)
    }
  })

  it('selects through an editable host on Safari instead of a readonly textarea', async () => {
    setUserAgent(SAFARI_UA)
    setClipboard(undefined)
    let selectedText: string | null = null
    let editable = false
    const execCommand = setExecCommand(() => {
      const node = document.querySelector('[contenteditable]')
      editable = node !== null
      selectedText = document.getSelection()?.toString() ?? null
      return true
    })

    await expect(copyToClipboard('/data/safari.csv')).resolves.toBe('manual')

    expect(execCommand).toHaveBeenCalledWith('copy')
    expect(editable).toBe(true)
    // WebKit copies the Range, so the selection — not just the element — has to hold the text.
    expect(selectedText).toBe('/data/safari.csv')
    expect(document.querySelector('textarea')).toBeNull()
    expect(document.body.childElementCount).toBe(0)
  })

  it('restores the selection the user had before the legacy copy', async () => {
    setUserAgent(CHROMIUM_UA)
    setClipboard(undefined)
    setExecCommand(() => true)

    const paragraph = document.createElement('p')
    paragraph.textContent = 'selected before'
    document.body.appendChild(paragraph)
    const range = document.createRange()
    range.selectNodeContents(paragraph)
    document.getSelection()!.addRange(range)

    await copyToClipboard('/data/whatever')

    expect(document.getSelection()?.toString()).toBe('selected before')
    expect(document.querySelector('textarea')).toBeNull()
  })

  it('asks for a manual copy when the browser blocks the legacy one', async () => {
    setUserAgent(CHROMIUM_UA)
    setClipboard(undefined)
    setExecCommand(() => false)

    await expect(copyToClipboard('/data/blocked')).resolves.toBe('manual')
    expect(document.body.childElementCount).toBe(0)
  })

  it('asks for a manual copy when the engine has no execCommand left and no async API', async () => {
    setUserAgent(CHROMIUM_UA)
    setClipboard(undefined)

    await expect(copyToClipboard('/data/blocked')).resolves.toBe('manual')
  })

  it('cleans up and asks for a manual copy when execCommand throws', async () => {
    setUserAgent(CHROMIUM_UA)
    setClipboard(undefined)
    setExecCommand(() => {
      throw new DOMException('The command is not supported', 'NotSupportedError')
    })

    await expect(copyToClipboard('/data/throws')).resolves.toBe('manual')
    expect(document.body.childElementCount).toBe(0)
  })

  it('rejects blank values without touching the clipboard', async () => {
    const writeText = vi.fn(async () => {})
    setClipboard({ writeText })
    const execCommand = setExecCommand(() => true)

    for (const blank of ['', '   ', '-']) {
      await expect(copyToClipboard(blank)).rejects.toThrow('Nothing to copy')
    }

    expect(writeText).not.toHaveBeenCalled()
    expect(execCommand).not.toHaveBeenCalled()
  })

  it('copies the trimmed value', async () => {
    const writeText = vi.fn(async () => {})
    setClipboard({ writeText })

    await copyToClipboard('  /data/spaced.csv  ')

    expect(writeText).toHaveBeenCalledWith('/data/spaced.csv')
  })
})
