import { describe, expect, it } from 'vitest'

import { truncatedListLimit } from './list-window'

describe('truncatedListLimit', () => {
  it('stays quiet when the whole list fits', () => {
    expect(truncatedListLimit({ items: [], total: 3, page_size: 500 }, 3)).toBeNull()
    expect(truncatedListLimit({ total: 0, page_size: 500 }, 0)).toBeNull()
  })

  it('reports the window when the response was paged', () => {
    expect(truncatedListLimit({ total: 3000, page_size: 1000 }, 1000)).toBe(1000)
    expect(truncatedListLimit({ total: 501, page_size: 500 }, 500)).toBe(500)
  })

  it('reports the window when it came back exactly full', () => {
    // The jobs endpoint caps at 500 rows and cannot see past that window, so a
    // full page is the only signal that older jobs were left out.
    expect(truncatedListLimit({ total: 500, page_size: 500 }, 500)).toBe(500)
    expect(truncatedListLimit({ total: 1000, page_size: 1000 }, 1000)).toBe(1000)
  })

  it('does not claim truncation when the count matches the rows sent', () => {
    // A window larger than the result set means nothing was withheld.
    expect(truncatedListLimit({ total: 500, page_size: 1000 }, 500)).toBeNull()
  })

  it('uses the server page size, not the requested one', () => {
    // A client asking for 1000 against the 500-row cap is still told 500.
    expect(truncatedListLimit({ total: 4000, page_size: 500 }, 500)).toBe(500)
  })

  it('is safe on a bare array or a missing envelope', () => {
    expect(truncatedListLimit([{ job_id: 'a' }], 1)).toBeNull()
    expect(truncatedListLimit(undefined, 2)).toBeNull()
    expect(truncatedListLimit({}, 2)).toBeNull()
  })
})
