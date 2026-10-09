import { describe, expect, it } from 'vitest'

import { linkStyle } from './pdf-preview'

/** A 300x200 page at scale 1, which is the shape pdf.js hands over. */
const viewport = {
  rawDims: { pageWidth: 300, pageHeight: 200, pageX: 0, pageY: 0 }
}
const view = [0, 0, 300, 200]

describe('linkStyle', () => {
  it('flips the y axis and reports percentages of the page', () => {
    // A rect 20..160 across and 20..40 up from the bottom of the page: near the bottom of
    // the rendered image, which is where the annotation actually sits.
    const style = linkStyle(viewport, [20, 20, 160, 40], view)

    expect(style.left).toBe(`${(100 * 20) / 300}%`)
    expect(style.width).toBe(`${(100 * 140) / 300}%`)
    expect(style.height).toBe(`${(100 * 20) / 200}%`)
    // y=40 from the bottom on a 200pt page is 160 down from the top.
    expect(style.top).toBe(`${(100 * 160) / 200}%`)
  })

  it('normalises a rect given the other way round', () => {
    expect(linkStyle(viewport, [160, 40, 20, 20], view)).toEqual(
      linkStyle(viewport, [20, 20, 160, 40], view)
    )
  })
})
