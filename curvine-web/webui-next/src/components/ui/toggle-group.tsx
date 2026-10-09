import * as React from 'react'
import { ToggleGroup as ToggleGroupPrimitive } from 'radix-ui'

import { cn } from '@/lib/utils'
import { toggleVariants } from '@/components/ui/toggle'
import type { VariantProps } from 'class-variance-authority'

function ToggleGroup({
  className,
  variant,
  size,
  children,
  ...props
}: React.ComponentProps<typeof ToggleGroupPrimitive.Root> &
  VariantProps<typeof toggleVariants>) {
  return (
    <ToggleGroupPrimitive.Root
      data-slot="toggle-group"
      data-variant={variant}
      data-size={size}
      className={cn(
        'group/toggle-group inline-flex w-fit items-center gap-0.5 rounded-lg border border-input bg-card p-0.5',
        className
      )}
      {...props}
    >
      {children}
    </ToggleGroupPrimitive.Root>
  )
}

function ToggleGroupItem({
  className,
  variant,
  size,
  ...props
}: React.ComponentProps<typeof ToggleGroupPrimitive.Item> & VariantProps<typeof toggleVariants>) {
  return (
    <ToggleGroupPrimitive.Item
      data-slot="toggle-group-item"
      className={cn(
        toggleVariants({ variant, size }),
        'border-0 bg-transparent hover:bg-muted data-[state=on]:bg-background data-[state=on]:text-primary data-[state=on]:shadow-sm',
        className
      )}
      {...props}
    />
  )
}

export { ToggleGroup, ToggleGroupItem }
