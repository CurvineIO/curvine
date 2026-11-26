import { useSyncExternalStore } from 'react'

type Listener = () => void

let message = ''
const listeners = new Set<Listener>()

function emit() {
  listeners.forEach(listener => listener())
}

export function setApiError(next: string) {
  if (next === message) {
    return
  }
  message = next
  emit()
}

export function clearApiError() {
  setApiError('')
}

export function subscribeApiError(listener: Listener) {
  listeners.add(listener)
  return () => {
    listeners.delete(listener)
  }
}

export function getApiError() {
  return message
}

/** Message of the most recent failed request, or an empty string. */
export function useApiError(): string {
  return useSyncExternalStore(subscribeApiError, getApiError)
}
