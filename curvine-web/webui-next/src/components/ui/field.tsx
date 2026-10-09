import * as React from 'react'
import { cva, type VariantProps } from 'class-variance-authority'

import { Label } from '@/components/ui/label'
import { cn } from '@/lib/utils'

function FieldSet({ className, ...props }: React.ComponentProps<'fieldset'>) {
  return (
    <fieldset
      data-slot="field-set"
      className={cn('flex flex-col gap-5', className)}
      {...props}
    />
  )
}

function FieldLegend({
  className,
  variant = 'legend',
  ...props
}: React.ComponentProps<'legend'> & { variant?: 'legend' | 'label' }) {
  return (
    <legend
      data-slot="field-legend"
      data-variant={variant}
      className={cn('mb-2 font-medium', variant === 'label' ? 'text-sm' : 'text-base', className)}
      {...props}
    />
  )
}

function FieldGroup({ className, ...props }: React.ComponentProps<'div'>) {
  return (
    <div
      data-slot="field-group"
      className={cn('group/field-group flex w-full flex-col gap-5', className)}
      {...props}
    />
  )
}

const fieldVariants = cva('group/field flex w-full gap-2 data-[invalid=true]:text-destructive', {
  variants: {
    orientation: {
      vertical: 'flex-col [&>*]:w-full',
      horizontal: 'flex-row items-center [&>[data-slot=field-label]]:flex-auto',
      responsive: 'flex-col @md/field-group:flex-row @md/field-group:items-center'
    }
  },
  defaultVariants: {
    orientation: 'vertical'
  }
})

function Field({
  className,
  orientation = 'vertical',
  ...props
}: React.ComponentProps<'div'> & VariantProps<typeof fieldVariants>) {
  return (
    <div
      role="group"
      data-slot="field"
      data-orientation={orientation}
      className={cn(fieldVariants({ orientation }), className)}
      {...props}
    />
  )
}

function FieldContent({ className, ...props }: React.ComponentProps<'div'>) {
  return (
    <div
      data-slot="field-content"
      className={cn('group/field-content flex flex-1 flex-col gap-1 leading-snug', className)}
      {...props}
    />
  )
}

function FieldLabel({ className, ...props }: React.ComponentProps<typeof Label>) {
  return (
    <Label
      data-slot="field-label"
      className={cn(
        'group/field-label peer/field-label flex w-fit gap-2 leading-snug group-data-[disabled=true]/field:opacity-50',
        'has-[>[data-slot=field]]:w-full has-[>[data-slot=field]]:rounded-lg has-[>[data-slot=field]]:border has-[>[data-slot=field]]:p-4',
        className
      )}
      {...props}
    />
  )
}

function FieldTitle({ className, ...props }: React.ComponentProps<'div'>) {
  return (
    <div
      data-slot="field-label"
      className={cn('flex w-fit items-center gap-2 text-sm font-medium leading-snug', className)}
      {...props}
    />
  )
}

function FieldDescription({ className, ...props }: React.ComponentProps<'p'>) {
  return (
    <p
      data-slot="field-description"
      className={cn('text-xs leading-normal text-muted-foreground', className)}
      {...props}
    />
  )
}

function FieldSeparator({ className, ...props }: React.ComponentProps<'div'>) {
  return (
    <div
      data-slot="field-separator"
      className={cn('relative -my-2 h-5 text-sm', className)}
      {...props}
    />
  )
}

function FieldError({ className, children, ...props }: React.ComponentProps<'div'>) {
  return (
    <div
      role="alert"
      data-slot="field-error"
      className={cn('text-xs text-destructive', className)}
      {...props}
    >
      {children}
    </div>
  )
}

export {
  Field,
  FieldContent,
  FieldDescription,
  FieldError,
  FieldGroup,
  FieldLabel,
  FieldLegend,
  FieldSeparator,
  FieldSet,
  FieldTitle
}
