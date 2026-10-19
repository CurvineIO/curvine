import '@testing-library/jest-dom/vitest'
import { configure } from '@testing-library/react'
import { vi } from 'vitest'

// Vitest runs files in parallel workers; under full-suite load the default 1s
// findBy/waitFor budget expires before lazy routes and data effects settle.
configure({ asyncUtilTimeout: 3000 })

// jsdom implements neither the async clipboard API nor execCommand, so without
// this every copy path would report "Browser blocked clipboard access" and the
// copy actions could only be asserted by stubbing them away.
Object.defineProperty(window, 'isSecureContext', { value: true, configurable: true })
Object.defineProperty(navigator, 'clipboard', {
  value: { writeText: vi.fn(async () => {}), readText: vi.fn(async () => '') },
  configurable: true
})

// jsdom has no object URLs either, and the file preview needs them for images, PDFs and
// media. Each call returns a distinct, inspectable URL so tests can assert on what was
// handed to an <img src> or revoked on close.
let objectUrlSeq = 0
Object.defineProperty(URL, 'createObjectURL', {
  value: vi.fn(() => `blob:preview-${++objectUrlSeq}`),
  configurable: true
})
Object.defineProperty(URL, 'revokeObjectURL', { value: vi.fn(), configurable: true })

// jsdom's Blob has `slice` but no `text` (jsdom 26), which the file preview uses to read a
// text file. FileReader is available, so build the missing method on top of it rather than
// writing the component around a gap in the test environment.
if (typeof Blob.prototype.text !== 'function') {
  Object.defineProperty(Blob.prototype, 'text', {
    value: function text(this: Blob) {
      return new Promise<string>((resolve, reject) => {
        const reader = new FileReader()
        reader.onload = () => resolve(String(reader.result ?? ''))
        reader.onerror = () => reject(reader.error ?? new Error('blob read failed'))
        reader.readAsText(this)
      })
    },
    configurable: true,
    writable: true
  })
}

// jsdom has no matchMedia; the sidebar's mobile breakpoint hook needs one.
if (!window.matchMedia) {
  window.matchMedia = (query: string) =>
    ({
      matches: false,
      media: query,
      onchange: null,
      addEventListener: () => {},
      removeEventListener: () => {},
      addListener: () => {},
      removeListener: () => {},
      dispatchEvent: () => false
    }) as unknown as MediaQueryList
}

if (typeof window.ResizeObserver === 'undefined') {
  class ResizeObserverStub {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  window.ResizeObserver = ResizeObserverStub as unknown as typeof ResizeObserver
}

// Radix Select uses pointer capture APIs that jsdom does not implement.
if (!Element.prototype.hasPointerCapture) {
  Element.prototype.hasPointerCapture = () => false
}
if (!Element.prototype.setPointerCapture) {
  Element.prototype.setPointerCapture = () => {}
}
if (!Element.prototype.releasePointerCapture) {
  Element.prototype.releasePointerCapture = () => {}
}
if (!Element.prototype.scrollIntoView) {
  Element.prototype.scrollIntoView = () => {}
}

// Node 26 ships a `localStorage` that requires --localstorage-file and jsdom may
// not expose one at all. Install a plain in-memory Storage so code paths that
// persist (locale, browse columns, sync jobs) behave like a browser.
class MemoryStorage implements Storage {
  private store = new Map<string, string>()

  get length() {
    return this.store.size
  }

  clear() {
    this.store.clear()
  }

  getItem(key: string) {
    return this.store.has(key) ? this.store.get(key)! : null
  }

  key(index: number) {
    return Array.from(this.store.keys())[index] ?? null
  }

  removeItem(key: string) {
    this.store.delete(key)
  }

  setItem(key: string, value: string) {
    this.store.set(key, String(value))
  }
}

const memoryStorage = new MemoryStorage()
for (const target of [globalThis, window] as (typeof globalThis)[]) {
  Object.defineProperty(target, 'localStorage', {
    value: memoryStorage,
    configurable: true,
    writable: true
  })
}
