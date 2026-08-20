import { defineStore } from 'pinia'
import { fetchOverviewData } from '@/api/client'

let masterRuntimeInFlight: Promise<void> | null = null

export const useAppStore = defineStore('app', {
  state: () => ({
    errmsg: '',
    masterVersion: '' as string,
    masterVersionCommit: '' as string,
    masterStartTime: '' as string,
    masterStartTimeMs: null as number | null
  }),
  actions: {
    setErrmsg(message: unknown) {
      this.errmsg = String(message || '')
    },
    setMasterRuntime(payload: {
      version?: string | null
      version_commit?: string | null
      start_time?: string | null
      start_time_ms?: number | null
    } | null | undefined) {
      this.masterVersion = String(payload?.version || '').trim()
      this.masterVersionCommit = String(payload?.version_commit || '').trim()
      this.masterStartTime = String(payload?.start_time || '').trim()
      const startTimeMs = Number(payload?.start_time_ms)
      this.masterStartTimeMs = Number.isFinite(startTimeMs) && startTimeMs > 0
        ? startTimeMs
        : null
    },
    async refreshMasterRuntime() {
      if (masterRuntimeInFlight) return masterRuntimeInFlight
      masterRuntimeInFlight = (async () => {
        try {
          const overview = await fetchOverviewData()
          this.setMasterRuntime(overview || {})
        } catch (_) {
          // Keep last known runtime; sidebar remains usable without overview.
        } finally {
          masterRuntimeInFlight = null
        }
      })()
      return masterRuntimeInFlight
    }
  }
})
