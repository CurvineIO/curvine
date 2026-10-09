import { CircleAlertIcon, InboxIcon, type LucideIcon } from 'lucide-react'

import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Skeleton } from '@/components/ui/skeleton'
import { TableCell, TableRow } from '@/components/ui/table'
import { cn } from '@/lib/utils'

export function ErrorAlert({ message, className }: { message: string; className?: string }) {
  return (
    <Alert variant="destructive" className={className}>
      <CircleAlertIcon />
      <AlertTitle>请求失败</AlertTitle>
      <AlertDescription>{message}</AlertDescription>
    </Alert>
  )
}

export function EmptyNotice({
  title,
  description,
  icon: Icon = InboxIcon,
  className,
  children
}: {
  title: string
  description?: string
  icon?: LucideIcon
  className?: string
  children?: React.ReactNode
}) {
  return (
    <Empty className={cn('border border-dashed border-border', className)}>
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <Icon />
        </EmptyMedia>
        <EmptyTitle>{title}</EmptyTitle>
        {description && <EmptyDescription>{description}</EmptyDescription>}
      </EmptyHeader>
      {children}
    </Empty>
  )
}

export function TableSkeletonRows({ columns, rows = 4 }: { columns: number; rows?: number }) {
  return (
    <>
      {Array.from({ length: rows }, (_, rowIndex) => (
        <TableRow key={rowIndex}>
          {Array.from({ length: columns }, (_, columnIndex) => (
            <TableCell key={columnIndex}>
              <Skeleton className="h-4 w-full max-w-40" />
            </TableCell>
          ))}
        </TableRow>
      ))}
    </>
  )
}
