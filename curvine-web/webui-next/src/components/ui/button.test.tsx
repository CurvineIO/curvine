import { createRef } from 'react'
import { render, screen } from '@testing-library/react'
import { AlertDialog as AlertDialogPrimitive, Dialog as SheetPrimitive } from 'radix-ui'
import { describe, expect, it } from 'vitest'

import { Badge } from './badge'
import { Button } from './button'
import { Toggle } from './toggle'
import { SheetOverlay } from './sheet'
import { AlertDialogOverlay } from './alert-dialog'

/*
 * Radix triggers are used with `asChild` and clone their child to attach a ref
 * for popper anchoring. React 18 strips `ref` from a plain function component's
 * props, so a primitive that does not forward it leaves the popper with no
 * reference element: the menu mounts into a portal and nothing appears in the
 * browser (jsdom still renders it, which is why this needs its own assertion).
 *
 * The overlay wrappers are cloned the same way, this time by the portal, which is
 * how Radix waits for a closing overlay to finish its exit animation. A lost ref
 * there means the overlay is torn down at once instead of fading out.
 */
describe('primitive ref forwarding', () => {
  it('Button exposes its DOM node through a ref', () => {
    const ref = createRef<HTMLButtonElement>()
    render(<Button ref={ref}>go</Button>)
    expect(ref.current).toBeInstanceOf(HTMLButtonElement)
    expect(ref.current).toBe(screen.getByRole('button', { name: 'go' }))
  })

  it('Toggle exposes its DOM node through a ref', () => {
    const ref = createRef<HTMLButtonElement>()
    render(<Toggle ref={ref}>toggle</Toggle>)
    expect(ref.current).toBeInstanceOf(HTMLButtonElement)
    expect(ref.current).toBe(screen.getByRole('button', { name: 'toggle' }))
  })

  it('Badge exposes its DOM node through a ref', () => {
    const ref = createRef<HTMLSpanElement>()
    render(<Badge ref={ref}>badge</Badge>)
    expect(ref.current).toBeInstanceOf(HTMLSpanElement)
    expect(ref.current).toBe(screen.getByText('badge'))
  })

  it('SheetOverlay exposes its DOM node through a ref', () => {
    const ref = createRef<HTMLDivElement>()
    // An overlay only renders inside its Root's context, which is also the context
    // the portal clones its children in.
    render(
      <SheetPrimitive.Root open>
        <SheetPrimitive.Portal>
          <SheetOverlay ref={ref} />
        </SheetPrimitive.Portal>
      </SheetPrimitive.Root>
    )
    expect(ref.current).toBeInstanceOf(HTMLDivElement)
  })

  it('AlertDialogOverlay exposes its DOM node through a ref', () => {
    const ref = createRef<HTMLDivElement>()
    render(
      <AlertDialogPrimitive.Root open>
        <AlertDialogPrimitive.Portal>
          <AlertDialogOverlay ref={ref} />
        </AlertDialogPrimitive.Portal>
      </AlertDialogPrimitive.Root>
    )
    expect(ref.current).toBeInstanceOf(HTMLDivElement)
  })
})
