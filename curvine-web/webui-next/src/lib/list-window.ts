/**
 * The job list endpoints answer with `{ items, page, page_size, total }`.
 *
 * `page_size` is the window the server actually used, which is not always the one
 * requested: it clamps (`JOB_LIST_MAX_FETCH` 500 for jobs, `TRANSFER_LIST_MAX_FETCH`
 * 1000 for transfers). Neither endpoint offers a next-page token on the list, so a
 * client that asks for the window and gets it back cannot otherwise tell whether
 * older rows were left behind — the page silently looks complete.
 *
 * Returns the row limit when the response filled that window, and null when the
 * list is complete, so the caller can say so instead of implying the data is whole.
 */
export function truncatedListLimit(response: unknown, received: number): number | null {
  if (received <= 0) return null

  const payload = response as { total?: unknown; page_size?: unknown } | null | undefined
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return null

  const total = Number(payload.total)
  const serverPageSize = Number(payload.page_size)
  const window =
    Number.isFinite(serverPageSize) && serverPageSize > 0 ? serverPageSize : null

  // More rows exist than were sent. Without a reported window, `received` is the
  // only figure available, so report that rather than inventing a limit.
  if (Number.isFinite(total) && total > received) return window ?? received

  // The window came back full, so older rows may exist beyond it. This needs the
  // server's own page size: comparing against a guessed limit would fire on every
  // response that lacks one.
  if (window !== null && received >= window) return window

  return null
}
