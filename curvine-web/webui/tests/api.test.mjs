import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { test } from 'node:test'
import vm from 'node:vm'
import ts from 'typescript'

// Transpile with the existing compiler; isolate Vite config and the banner store.
function setup() {
  let source = readFileSync(new URL('../src/lib/api.ts', import.meta.url), 'utf8')
  source = source.replace("import { clearApiError, setApiError } from '@/lib/api-error'", '')
    .replace('import.meta.env.VITE_API_BASE', 'undefined')
  const timers = new Map()
  const banners = []
  let signal
  const context = {
    exports: {}, AbortController, DOMException, URLSearchParams,
    setApiError: message => banners.push(message), clearApiError: () => {},
    setTimeout: (fn, ms) => { const id = {}; timers.set(id, { fn, ms }); return id },
    clearTimeout: id => timers.delete(id),
    fetch: (_url, options) => {
      signal = options.signal
      return new Promise((_resolve, reject) => {
        if (signal?.aborted) reject(signal.reason)
        else signal?.addEventListener('abort', () => reject(signal.reason), { once: true })
      })
    },
  }
  vm.runInNewContext(ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText, context)
  return { context, api: context.exports.api, timers, banners, signal: () => signal }
}

if (process.env.REPRODUCE) {
  const h = setup()
  let settled = false
  h.api.overview().finally(() => { settled = true })
  for (const { fn } of h.timers.values()) fn()
  await new Promise(resolve => setImmediate(resolve))
  console.log(`BEFORE: timers=${h.timers.size}, signal=${h.signal()}, settled=${settled}, bannerUpdates=${h.banners.length}`)
  assert.equal(settled, false)
  assert.equal(h.timers.size, 0)
} else {
  test('hanging fetch times out at 10000ms and updates banner', async () => {
    const h = setup()
    const pending = h.api.overview()
    const timer = [...h.timers.values()][0]
    assert.equal(timer.ms, 10000)
    timer.fn()
    await assert.rejects(pending, { name: 'ApiError', message: 'Request Timeout', status: 408 })
    assert.equal(h.signal().aborted, true)
    assert.deepEqual(h.banners, ['Request Timeout'])
    assert.equal(h.timers.size, 0)
  })
  test('caller cancellation aborts fetch without banner and removes listener', async () => {
    const h = setup()
    const caller = new AbortController()
    let removed = 0
    const remove = caller.signal.removeEventListener.bind(caller.signal)
    caller.signal.removeEventListener = (...args) => { removed++; remove(...args) }
    const pending = h.api.overview({ signal: caller.signal })
    caller.abort()
    await assert.rejects(pending, { name: 'AbortError' })
    assert.equal(h.signal().aborted, true)
    assert.deepEqual(h.banners, [])
    assert.equal(h.timers.size, 0)
    assert.equal(removed, 1)
  })
  test('fast response clears timeout', async () => {
    const h = setup()
    h.context.fetch = async () => ({ ok: true, json: async () => ({ ready: true }) })
    assert.deepEqual(await h.api.overview(), { ready: true })
    assert.equal(h.timers.size, 0)
  })
  test('quiet timeout leaves banner alone', async () => {
    const h = setup()
    const pending = h.api.overview({ quiet: true })
    ;[...h.timers.values()][0].fn()
    await assert.rejects(pending, { status: 408 })
    assert.deepEqual(h.banners, [])
  })
}
