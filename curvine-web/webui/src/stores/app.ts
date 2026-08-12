import { defineStore } from 'pinia'

export const useAppStore = defineStore('app', {
  state: () => ({
    errmsg: ''
  }),
  actions: {
    setErrmsg(message: unknown) {
      this.errmsg = String(message || '')
    }
  }
})
