import { useEffect, useRef, useState } from 'react'

/** The longest close animation on a sheet (the panel slides out over 200ms), plus a margin. */
const EXIT_MS = 260

/**
 * Lifecycle for a slide-out panel whose visibility is owned by its parent.
 *
 * Radix does wait for the close animation before tearing the sheet down, but it can
 * only do that while it is still mounted. A parent written as
 *
 *     {open && <Sheet open>…</Sheet>}
 *
 * removes the whole sheet the instant `open` goes false — before the animation can
 * start — so the panel blinks out instead of sliding away. `mounted` stays true for
 * the length of that animation, which gives the sheet the time it needs.
 *
 * `value` is whatever the panel is currently showing (a mount, a job, a worker).
 * Parents usually clear that selection as they close the panel, which would leave it
 * rendering an empty state mid-slide, so the last value seen while open is kept until
 * the panel is really gone.
 *
 * Usage: render on `mounted`, pass `open` through, and read `value` instead of the
 * parent's selection.
 */
export function useSlidingSheet<T>(open: boolean, value?: T, ms = EXIT_MS) {
  const [mounted, setMounted] = useState(open)
  const retained = useRef(value)

  useEffect(() => {
    if (open) retained.current = value
  }, [open, value])

  useEffect(() => {
    if (open) {
      setMounted(true)
      return
    }
    const timer = window.setTimeout(() => setMounted(false), ms)
    return () => window.clearTimeout(timer)
  }, [open, ms])

  return { mounted, open, value: open ? value : retained.current }
}
