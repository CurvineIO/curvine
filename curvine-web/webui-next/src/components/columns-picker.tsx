import { useState } from 'react'
import { ChevronDownIcon } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { useT } from '@/i18n'
import { orderVisibleColumns } from '@/lib/column-visibility'

export interface ColumnOption<Key extends string> {
  key: Key
  /** i18n key for the header; resolved dynamically, so tests assert it exists. */
  labelKey: string
}

/**
 * Popover of checkboxes that toggles which optional columns a table renders.
 *
 * The column set, the defaults and the test ids are props because each table
 * declares its own; the component owns only the popover's open state and reports
 * changes upward in declaration order, so ticking columns out of order never
 * reshuffles the table.
 */
export function ColumnsPicker<Key extends string>({
  columns,
  visible,
  defaultVisible,
  onChange,
  testIdPrefix
}: {
  columns: ColumnOption<Key>[]
  visible: Key[]
  defaultVisible: Key[]
  onChange(next: Key[]): void
  testIdPrefix: string
}) {
  const t = useT()
  const [open, setOpen] = useState(false)

  function toggle(key: Key) {
    const next = visible.includes(key)
      ? visible.filter((item) => item !== key)
      : [...visible, key]
    onChange(orderVisibleColumns(columns, next).map((column) => column.key))
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button variant="outline" size="sm" data-testid={`${testIdPrefix}-trigger`}>
          {t('common.columns.button')}
          <ChevronDownIcon data-icon="inline-end" />
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-64">
        <div className="flex flex-col gap-2">
          <span className="text-xs font-medium text-muted-foreground">
            {t('common.columns.title')}
          </span>
          {columns.map((column) => (
            <label key={column.key} className="flex items-center gap-2 text-sm">
              <Checkbox
                checked={visible.includes(column.key)}
                onCheckedChange={() => toggle(column.key)}
                data-testid={`${testIdPrefix}-option-${column.key}`}
              />
              {t(column.labelKey)}
            </label>
          ))}
          <Button
            variant="ghost"
            size="xs"
            className="mt-1 self-start text-primary"
            onClick={() => onChange([...defaultVisible])}
            data-testid={`${testIdPrefix}-reset`}
          >
            {t('common.columns.reset')}
          </Button>
          <span className="text-xs text-muted-foreground">{t('common.columns.hint')}</span>
        </div>
      </PopoverContent>
    </Popover>
  )
}
