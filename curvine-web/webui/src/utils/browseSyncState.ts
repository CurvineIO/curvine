import type { Composer } from 'vue-i18n'

type Translate = Composer['t']

const SYNC_STATE_KEYS: Record<string, string> = {
  Queued: 'browse.ufsSync.states.queued',
  Completed: 'browse.ufsSync.states.completed',
  'Needs attention': 'browse.ufsSync.states.needsAttention',
  'Syncing to UFS': 'browse.ufsSync.states.syncingToUfs',
  'Waiting for UFS sync': 'browse.ufsSync.states.waitingForUfsSync',
  'Uploading to Curvine': 'browse.ufsSync.states.uploadingToCurvine',
  'Finished with skipped files': 'browse.ufsSync.states.finishedWithSkipped',
  'Uploaded to Curvine': 'browse.ufsSync.states.uploadedToCurvine',
  'Checking UFS sync': 'browse.ufsSync.states.checkingUfsSync',
  'Upload failed': 'browse.ufsSync.states.uploadFailed',
  Interrupted: 'browse.ufsSync.states.interrupted',
  'Timed out': 'browse.ufsSync.states.timedOut',
  Unsupported: 'browse.ufsSync.states.unsupported',
  NotMounted: 'browse.ufsSync.states.notMounted',
  NotStarted: 'browse.ufsSync.states.notStarted',
  Pending: 'browse.ufsSync.states.pending',
  Loading: 'browse.ufsSync.states.loading',
  Failed: 'browse.ufsSync.states.failed',
  Canceled: 'browse.ufsSync.states.canceled',
  Unknown: 'browse.ufsSync.states.unknown'
}

export function translateSyncState(t: Translate, state: string): string {
  const key = SYNC_STATE_KEYS[String(state || '')]
  return key ? String(t(key)) : String(state || '')
}

export function syncStateMessage(t: Translate, state: string, fallback = ''): string {
  const messages: Record<string, string> = {
    Interrupted: 'browse.ufsSync.messages.interrupted',
    'Timed out': 'browse.ufsSync.messages.timedOut',
    Unsupported: 'browse.ufsSync.messages.unsupported'
  }
  const key = messages[state]
  return key ? String(t(key)) : fallback
}
