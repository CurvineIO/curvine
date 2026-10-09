import * as React from 'react'

import { cn } from '@/lib/utils'

function Input({ className, type, ...props }: React.ComponentProps<'input'>) {
  return (
    <input
      type={type}
      data-slot="input"
      className={cn(
        'flex h-8 w-full min-w-0 rounded-lg border border-input bg-card px-2.5 py-1 text-sm text-foreground transition-[color,box-shadow] outline-none',
        'placeholder:text-muted-foreground/70',
        'file:mr-2 file:inline-flex file:h-6 file:items-center file:rounded-md file:border-0 file:bg-secondary file:px-2 file:text-xs file:font-medium file:text-secondary-foreground',
        'focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/25',
        'disabled:cursor-not-allowed disabled:opacity-50',
        'aria-invalid:border-destructive aria-invalid:ring-destructive/20',
        className
      )}
      {...props}
    />
  )
}

export { Input }
