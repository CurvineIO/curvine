import * as React from 'react'
import { Slot } from 'radix-ui'
import { cva, type VariantProps } from 'class-variance-authority'

import { cn } from '@/lib/utils'

const badgeVariants = cva(
  'inline-flex w-fit shrink-0 items-center justify-center gap-1 overflow-hidden rounded-4xl border px-2 py-0.5 text-xs font-medium whitespace-nowrap transition-colors [&>svg]:pointer-events-none [&>svg]:size-3',
  {
    variants: {
      variant: {
        default: 'border-transparent bg-primary text-primary-foreground',
        secondary: 'border-transparent bg-secondary text-secondary-foreground',
        destructive: 'border-transparent bg-destructive text-destructive-foreground',
        outline: 'border-border text-foreground'
      }
    },
    defaultVariants: {
      variant: 'default'
    }
  }
)

/** See `button.tsx`: the ref must reach the DOM node so `asChild` composition works. */
const Badge = React.forwardRef<
  HTMLSpanElement,
  React.ComponentProps<'span'> & VariantProps<typeof badgeVariants> & { asChild?: boolean }
>(function Badge({ className, variant, asChild = false, ...props }, ref) {
  const Comp = asChild ? Slot.Root : 'span'
  return (
    <Comp
      ref={ref}
      data-slot="badge"
      className={cn(badgeVariants({ variant }), className)}
      {...props}
    />
  )
})

export { Badge, badgeVariants }
