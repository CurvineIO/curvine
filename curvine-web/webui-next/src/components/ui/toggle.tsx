import * as React from 'react'
import { Toggle as TogglePrimitive } from 'radix-ui'
import { cva, type VariantProps } from 'class-variance-authority'

import { cn } from '@/lib/utils'

const toggleVariants = cva(
  "inline-flex items-center justify-center gap-1.5 rounded-lg text-sm font-medium whitespace-nowrap text-muted-foreground transition-colors outline-none hover:bg-muted hover:text-foreground focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/30 disabled:pointer-events-none disabled:opacity-50 data-[state=on]:bg-accent data-[state=on]:text-accent-foreground [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0",
  {
    variants: {
      variant: {
        default: 'bg-transparent',
        outline: 'border border-input bg-card hover:bg-accent hover:text-accent-foreground'
      },
      size: {
        default: 'h-8 min-w-8 px-2.5',
        sm: 'h-7 min-w-7 px-2 text-[13px]',
        lg: 'h-9 min-w-9 px-3'
      }
    },
    defaultVariants: {
      variant: 'default',
      size: 'default'
    }
  }
)

/** See `button.tsx`: the ref must reach the DOM node so Radix `asChild` triggers can anchor. */
const Toggle = React.forwardRef<
  React.ElementRef<typeof TogglePrimitive.Root>,
  React.ComponentProps<typeof TogglePrimitive.Root> & VariantProps<typeof toggleVariants>
>(function Toggle({ className, variant, size, ...props }, ref) {
  return (
    <TogglePrimitive.Root
      ref={ref}
      data-slot="toggle"
      className={cn(toggleVariants({ variant, size }), className)}
      {...props}
    />
  )
})

export { Toggle, toggleVariants }
