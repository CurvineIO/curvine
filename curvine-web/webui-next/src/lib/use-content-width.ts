import { useEffect, useState } from 'react'

/**
 * Share of the content box a side panel covers. The remainder is what shows through
 * behind it — a strip of the list, which is what reads as a layer rather than a page.
 */
export const PANEL_SHARE = 0.8

/**
 * First-paint width, before the measurement lands. Roughly `PANEL_SHARE` of the
 * content box for the common case: an expanded 16rem sidebar plus the shell's
 * horizontal padding.
 */
export const PANEL_FALLBACK_CLASS = 'w-4/5 sm:w-[calc(80vw-14.4rem)]'

/**
 * Width of the console shell's content box's content area — the width a full page's
 * content fills, and the thing that sets a side panel's size.
 *
 * Measured rather than derived from the sidebar width because the panel is
 * portalled to `document.body`: the shell's `--sidebar-width` variable is not in
 * scope there, and the sidebar is 16rem, 3rem or off-canvas depending on its
 * collapsed state and the breakpoint. Measuring gets all three right.
 */
export function useContentWidth() {
  const [width, setWidth] = useState<number | null>(null)

  useEffect(() => {
    const shell = document.querySelector('[data-console-content]')
    if (!shell) return

    const measure = () => {
      const style = getComputedStyle(shell)
      const padLeft = parseFloat(style.paddingLeft || '0')
      const padRight = parseFloat(style.paddingRight || '0')
      setWidth(shell.getBoundingClientRect().width - padLeft - padRight)
    }

    measure()
    const observer = new ResizeObserver(measure)
    observer.observe(shell)
    window.addEventListener('resize', measure)
    return () => {
      observer.disconnect()
      window.removeEventListener('resize', measure)
    }
  }, [])

  return width
}
