import { Badge, type BadgeProps } from '@/components/ui/badge'
import { displayState, transferStateTone } from '@/lib/transfer'

const TONE_VARIANTS: Record<string, BadgeProps['variant']> = {
  active: 'info',
  success: 'success',
  warning: 'warning',
  danger: 'danger',
  muted: 'muted',
}

export function TransferStateBadge({ state }: { state: string }) {
  return (
    <Badge variant={TONE_VARIANTS[transferStateTone(state)]} className="uppercase tracking-wide">
      {displayState(state)}
    </Badge>
  )
}
