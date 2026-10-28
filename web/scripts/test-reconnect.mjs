import assert from 'node:assert/strict'
import { readFileSync, readdirSync } from 'node:fs'
import ts from 'typescript'

// Exercise the actual component callbacks with native timers and a fake peer.
// No browser, ROS master, or extra test framework is needed.
const source = readFileSync(new URL('../src/main.tsx', import.meta.url), 'utf8')
const compiled = ts.transpileModule(source, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, target: ts.ScriptTarget.ES2022 },
}).outputText
let ready = false
let attempts = 0
let inFlight = 0
let maxInFlight = 0
let maxTimers = 0
let seed = 2
const timers = new Set()
const state = []
const effects = []
const cleanups = []
const pending = new Map()
let holdDelete = false
let finishDelete
let failDelete = false
let holdPost = false
let finishPost
const originalRandom = Math.random
Math.random = () => {
  seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0
  return seed / 2 ** 32
}
const documentEvents = new Map()
const windowEvents = new Map()
globalThis.document = { hidden: false, cookie: '', getElementById: () => ({ dataset: { sourceId: 'camera' } }),
  addEventListener: (name, listener) => documentEvents.set(name, listener),
  removeEventListener: name => documentEvents.delete(name) }
globalThis.window = {
  setTimeout(callback, delay) {
    const timer = setTimeout(() => { timers.delete(timer); callback() }, delay)
    timers.add(timer)
    maxTimers = Math.max(maxTimers, timers.size)
    return timer
  },
  clearTimeout(timer) { clearTimeout(timer); timers.delete(timer) },
  addEventListener: (name, listener) => windowEvents.set(name, listener),
  removeEventListener: name => windowEvents.delete(name),
}
globalThis.fetch = async (path, options) => {
  if (options.method === 'DELETE') {
    if (holdDelete) await new Promise(resolve => { finishDelete = resolve })
    if (failDelete) return { ok: false, status: 502 }
    pending.delete(path.split('/').at(-1)); return { ok: true }
  }
  attempts += 1
  inFlight += 1
  maxInFlight = Math.max(maxInFlight, inFlight)
  if (holdPost) await new Promise(resolve => { finishPost = resolve })
  await new Promise(resolve => setTimeout(resolve, 2))
  inFlight -= 1
  if (!ready) return { ok: false, status: 503, json: async () => ({ error: 'camera source is unavailable' }) }
  const sessionId = String(attempts)
  pending.set(sessionId, true)
  return { ok: true, json: async () => ({ sessionId, sdp: 'answer' }) }
}
globalThis.RTCPeerConnection = class {
  iceGatheringState = 'complete'
  connectionState = 'new'
  listeners = new Map()
  addTransceiver() { return { receiver: {} } }
  addEventListener(event, callback) { this.listeners.set(event, callback) }
  removeEventListener(event) { this.listeners.delete(event) }
  async createOffer() { return { type: 'offer', sdp: 'offer' } }
  async setLocalDescription(offer) { this.localDescription = offer }
  async setRemoteDescription() {
    this.connectionState = 'connected'
    this.listeners.get('connectionstatechange')?.()
  }
  close() { this.connectionState = 'closed'; this.listeners.get('connectionstatechange')?.() }
}
const jsx = (type, props) => ({ type, props })
let skinStore
const react = {
  useRef: value => ({ current: value }),
  useCallback: value => value,
  useState: value => {
    const index = state.length
    state.push(value)
    return [value, update => { state[index] = update }]
  },
  useEffect: effect => effects.push(effect),
}
const requireMock = name => {
  if (name === 'react') return react
  if (name === 'react/jsx-runtime') return { jsx, jsxs: jsx }
  if (name === 'react-dom/client') return { createRoot: () => ({ render: element => element.type() }) }
  if (name === '@xgc2/ui-react') return { initializeSkin: options => { skinStore = options.store }, useSkin: () => ['dark', () => {}] }
  if (name.endsWith('.css')) return {}
  throw new Error(`Unexpected import ${name}`)
}
new Function('require', 'exports', compiled)(requireMock, {})
assert.equal(skinStore.getSnapshot(), 'dark')
await skinStore.setSkin('light')
assert.equal(skinStore.getSnapshot(), 'light')
for (const effect of effects) cleanups.push(effect())
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))
const resources = () => ({ fds: readdirSync('/proc/self/fd').length, threads: readdirSync('/proc/self/task').length })
await sleep(100)
const before = resources()
const cpu = process.cpuUsage()
const firstAttempts = attempts
const duration = process.argv.includes('--long') ? 60_000 : 500
try {
  await sleep(duration)
  const used = process.cpuUsage(cpu)
  const after = resources()
  assert.equal(state[1], 'Unavailable')
  assert.equal(state[2], 'camera source is unavailable')
  assert.equal(maxInFlight, 1)
  assert.equal(maxTimers, 1)
  assert.equal(pending.size, 0)
  assert.ok(attempts - firstAttempts <= 15)
  assert.ok(after.fds <= before.fds)
  assert.ok(after.threads <= before.threads)
  if (duration === 60_000) assert.ok(used.user + used.system < 1_200_000)
  console.log(`Viewer down ${duration / 1000}s: CPU=${((used.user + used.system) / 1e6).toFixed(3)}s attempts=${attempts - firstAttempts} logs=0 fds=${before.fds}->${after.fds} threads=${before.threads}->${after.threads}`)
  ready = true
  const deadline = Date.now() + 31_000
  while (state[0] !== 'connected' && Date.now() < deadline) await sleep(20)
  assert.equal(state[0], 'connected')
  assert.equal(pending.size, 1)
  assert.equal(maxInFlight, 1)
  assert.equal(timers.size, 0)
  console.log('Viewer recovered automatically with one session')
  document.hidden = true
  documentEvents.get('visibilitychange')()
  await sleep(20)
  const hiddenAttempts = attempts
  assert.equal(pending.size, 0)
  assert.equal(timers.size, 0)
  assert.equal(state[1], 'Paused')
  await sleep(100)
  assert.equal(attempts, hiddenAttempts)
  document.hidden = false
  documentEvents.get('visibilitychange')()
  const resumed = Date.now() + 2_000
  while (state[0] !== 'connected' && Date.now() < resumed) await sleep(10)
  assert.equal(state[0], 'connected')
  assert.equal(pending.size, 1)
  holdDelete = true
  document.hidden = true
  documentEvents.get('visibilitychange')()
  await sleep(10)
  assert.equal(pending.size, 1)
  assert.equal(typeof finishDelete, 'function')
  const beforeReveal = attempts
  document.hidden = false
  documentEvents.get('visibilitychange')()
  await sleep(30)
  assert.equal(attempts, beforeReveal, 'reveal issued a new offer before the retired peer joined')
  assert.equal(pending.size, 1)
  holdDelete = false
  finishDelete()
  const joined = Date.now() + 2_000
  while (attempts === beforeReveal && Date.now() < joined) await sleep(10)
  while (pending.size !== 1 && Date.now() < joined) await sleep(10)
  assert.equal(attempts, beforeReveal + 1)
  assert.equal(pending.size, 1)
  console.log('Rapid reveal: zero overlapping offers; one current offer after exact DELETE joins')

  document.hidden = true
  documentEvents.get('visibilitychange')()
  await sleep(10)
  assert.equal(pending.size, 0)
  holdPost = true
  document.hidden = false
  documentEvents.get('visibilitychange')()
  const postStarted = Date.now() + 2_000
  while (!finishPost && Date.now() < postStarted) await sleep(2)
  assert.equal(typeof finishPost, 'function')
  const beforeLate = attempts
  document.hidden = true
  documentEvents.get('visibilitychange')()
  document.hidden = false
  documentEvents.get('visibilitychange')()
  await sleep(30)
  assert.equal(attempts, beforeLate, 'reveal overlapped an unjoined POST')
  holdPost = false
  finishPost()
  const lateJoined = Date.now() + 2_000
  while (attempts === beforeLate && Date.now() < lateJoined) await sleep(10)
  while (pending.size !== 1 && Date.now() < lateJoined) await sleep(10)
  assert.equal(attempts, beforeLate + 1)
  assert.equal(pending.size, 1)
  console.log('Late admitted POST: exact retired session joined before one current offer')

  windowEvents.get('pagehide')()
  await sleep(20)
  assert.equal(pending.size, 0)
  windowEvents.get('pageshow')()
  const shown = Date.now() + 2_000
  while (pending.size !== 1 && Date.now() < shown) await sleep(10)
  assert.equal(pending.size, 1)
  failDelete = true
  document.hidden = true
  documentEvents.get('visibilitychange')()
  await sleep(10)
  const beforeFailure = attempts
  document.hidden = false
  documentEvents.get('visibilitychange')()
  await sleep(30)
  assert.equal(attempts, beforeFailure, 'unconfirmed cleanup caused a blind new offer')
  assert.equal(state[0], 'error')
  assert.equal(state[1], 'Unavailable')
  assert.match(state[2], /session cleanup failed/)
  assert.equal(timers.size, 0)
  pending.clear() // the fixture ends with an explicitly unconfirmed remote owner
} finally {
  for (const cleanup of cleanups) cleanup?.()
  Math.random = originalRandom
  for (const timer of timers) clearTimeout(timer)
}
assert.equal(pending.size, 0)
