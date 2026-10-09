import { useId } from 'react'

import { Button } from '@/components/ui/button'
import { Field, FieldDescription, FieldLabel } from '@/components/ui/field'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/components/ui/select'
import { Spinner } from '@/components/ui/spinner'
import { Textarea } from '@/components/ui/textarea'
import { cn } from '@/lib/utils'

interface FieldShellProps {
  label: string
  description?: string
  className?: string
  children: (id: string) => React.ReactNode
}

export function FieldShell({ label, description, className, children }: FieldShellProps) {
  const id = useId()
  return (
    <Field className={className}>
      <FieldLabel htmlFor={id}>{label}</FieldLabel>
      {children(id)}
      {description && <FieldDescription>{description}</FieldDescription>}
    </Field>
  )
}

export function TextField({
  label,
  value,
  onChange,
  type = 'text',
  required = false,
  placeholder,
  description,
  className,
  testId,
  disabled = false,
  autoComplete,
  autoFocus = false
}: {
  label: string
  value: string
  onChange(value: string): void
  type?: 'text' | 'url' | 'number' | 'password'
  required?: boolean
  placeholder?: string
  description?: string
  className?: string
  testId?: string
  disabled?: boolean
  autoComplete?: string
  autoFocus?: boolean
}) {
  return (
    <FieldShell label={label} description={description} className={className}>
      {(id) => (
        <Input
          id={id}
          type={type}
          value={value}
          required={required}
          placeholder={placeholder}
          disabled={disabled}
          autoComplete={autoComplete}
          autoFocus={autoFocus}
          data-testid={testId}
          onChange={(event) => onChange(event.target.value)}
        />
      )}
    </FieldShell>
  )
}

export function ReadonlyField({
  label,
  value,
  description,
  className
}: {
  label: string
  value: string
  description?: string
  className?: string
}) {
  return (
    <FieldShell label={label} description={description} className={className}>
      {(id) => <Input id={id} readOnly value={value} className="bg-muted/40 text-muted-foreground" />}
    </FieldShell>
  )
}

export function TextAreaField({
  label,
  value,
  onChange,
  required = false,
  placeholder,
  description,
  rows = 10,
  className,
  testId
}: {
  label: string
  value: string
  onChange(value: string): void
  required?: boolean
  placeholder?: string
  description?: string
  rows?: number
  className?: string
  testId?: string
}) {
  return (
    <FieldShell label={label} description={description} className={className}>
      {(id) => (
        <Textarea
          id={id}
          value={value}
          required={required}
          placeholder={placeholder}
          rows={rows}
          spellCheck={false}
          data-testid={testId}
          className="font-mono text-xs"
          onChange={(event) => onChange(event.target.value)}
        />
      )}
    </FieldShell>
  )
}

export function SelectField({
  label,
  value,
  onChange,
  options,
  placeholder,
  description,
  required = false,
  disabled = false,
  className,
  testId
}: {
  label: string
  value: string
  onChange(value: string): void
  options: { value: string; label: string }[]
  placeholder?: string
  description?: string
  required?: boolean
  disabled?: boolean
  className?: string
  testId?: string
}) {
  return (
    <FieldShell label={label} description={description} className={className}>
      {(id) => (
        <Select value={value} onValueChange={onChange} required={required}>
          <SelectTrigger id={id} className="w-full" disabled={disabled} data-testid={testId}>
            <SelectValue placeholder={placeholder} />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              {options.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectGroup>
          </SelectContent>
        </Select>
      )}
    </FieldShell>
  )
}

export function SubmitButton({
  pending,
  pendingLabel,
  children,
  className,
  variant,
  disabled,
  size,
  testId
}: {
  pending: boolean
  pendingLabel: string
  children: React.ReactNode
  className?: string
  variant?: React.ComponentProps<typeof Button>['variant']
  disabled?: boolean
  size?: React.ComponentProps<typeof Button>['size']
  testId?: string
}) {
  return (
    <Button
      type="submit"
      variant={variant}
      size={size}
      disabled={pending || disabled}
      data-testid={testId}
      className={cn(className)}
    >
      {pending && <Spinner data-icon="inline-start" />}
      {pending ? pendingLabel : children}
    </Button>
  )
}
