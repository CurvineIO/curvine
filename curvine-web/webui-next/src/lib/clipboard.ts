/**
 * Clipboard writes for the console.
 *
 * The console is served over plain HTTP (`http://<host>:<port>`, see `curvine-web`'s static
 * handler), and `navigator.clipboard` is only exposed in a secure context. In a real
 * deployment the async API is therefore missing and every copy has to go through the legacy
 * `document.execCommand('copy')` path — which is where the platforms disagree, because
 * `execCommand` copies whatever the DOM selection currently holds and each engine has its own
 * rule about where that selection may live:
 *
 * - Chromium (Windows/Linux/macOS) and Firefox copy the selection of a hidden `<textarea>`.
 * - WebKit (Safari on macOS/iOS, every WKWebView) ignores `setSelectionRange()` on a readonly
 *   textarea. It answers `execCommand('copy')` with `true` and puts nothing on the clipboard:
 *   the copy toasted "copied" while the clipboard stayed empty. WebKit needs a genuinely
 *   editable host and a DOM Range instead.
 *
 * The scratch element is kept inside the viewport (WebKit will not copy from one that is
 * scrolled out of it) and whatever the user had selected is put back afterwards.
 */

const SCRATCH_CSS =
  'position:fixed;top:0;left:0;width:1px;height:1px;margin:0;padding:0;border:0;opacity:0;pointer-events:none'

interface Scratch {
  element: HTMLElement
  select(): void
}

/**
 * Safari, iOS and every other WKWebView — the engines that need the editable-host path.
 * Chrome, Edge and Opera carry `AppleWebKit` in their UA string too, so they are excluded by
 * name; they copy reliably from the textarea.
 */
function needsEditableSelection() {
  const agent = navigator.userAgent
  return /AppleWebKit/.test(agent) && !/(Chrome|Chromium|CriOS|Edg|OPR|Android)/.test(agent)
}

function textareaScratch(value: string): Scratch {
  const element = document.createElement('textarea')
  element.value = value
  // Readonly keeps the on-screen keyboard down; the desktop engines still copy the selection.
  element.setAttribute('readonly', '')
  element.style.cssText = SCRATCH_CSS

  return {
    element,
    select() {
      element.focus()
      element.select()
      element.setSelectionRange(0, value.length)
    }
  }
}

function editableScratch(value: string): Scratch {
  const element = document.createElement('div')
  element.textContent = value
  element.setAttribute('contenteditable', 'true')
  element.style.cssText = SCRATCH_CSS

  return {
    element,
    select() {
      element.focus()
      const range = document.createRange()
      range.selectNodeContents(element)
      const selection = document.getSelection()
      selection?.removeAllRanges()
      selection?.addRange(range)
    }
  }
}

function legacyCopy(value: string): boolean {
  // Engines that dropped the command have no legacy path at all; reporting that is better
  // than pretending the copy happened.
  if (typeof document.execCommand !== 'function') return false

  const scratch = needsEditableSelection() ? editableScratch(value) : textareaScratch(value)
  const selection = document.getSelection()
  const previousRanges = selection
    ? Array.from({ length: selection.rangeCount }, (_, index) => selection.getRangeAt(index))
    : []
  const previousFocus = document.activeElement

  document.body.appendChild(scratch.element)
  let copied = false
  try {
    scratch.select()
    copied = document.execCommand('copy')
  } catch {
    copied = false
  } finally {
    scratch.element.remove()
    if (selection) {
      selection.removeAllRanges()
      for (const range of previousRanges) selection.addRange(range)
    }
    // Selecting the scratch element took focus; hand it back so a keyboard-triggered copy
    // does not strand the user on <body>. A menu restores focus to its trigger right after.
    if (previousFocus instanceof HTMLElement && previousFocus.isConnected) previousFocus.focus()
  }

  return copied
}

export type CopyOutcome = 'copied' | 'manual'

/**
 * `manual` means the caller should offer the text for a real Ctrl/Cmd+C.
 *
 * That is not a failure to report: on a plain-HTTP origin `navigator.clipboard` does not exist
 * and the legacy path cannot vouch for itself — measured, `execCommand` answered `true` while
 * the clipboard stayed empty — and there is no read-back API to check it with. Rather than
 * toast a success that may not have happened, the caller gets a result it can act on.
 */
export async function copyToClipboard(text: string): Promise<CopyOutcome> {
  const value = String(text || '').trim()
  if (!value || value === '-') {
    throw new Error('Nothing to copy')
  }

  if (navigator.clipboard?.writeText && window.isSecureContext) {
    try {
      await navigator.clipboard.writeText(value)
      return 'copied'
    } catch {
      // Reachable on a plain-HTTP page seen by a browser that still exposes the API, inside
      // an iframe whose Permissions-Policy withholds clipboard-write, and when the click's
      // user activation was already spent. Fall through rather than failing outright.
    }
  }

  // Attempted anyway: where it works, the user can simply dismiss the fallback.
  legacyCopy(value)
  return 'manual'
}
