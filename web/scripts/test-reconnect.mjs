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
const originalRandom = Math.random
Math.random = () => {
  seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0
  return seed / 2 ** 32
}
globalThis.document = { getElementById: () => ({ dataset: { sourceId: 'camera' } }) }
globalThis.window = {
  setTimeout(callback, delay) {
    const timer = setTimeout(() => { timers.delete(timer); callback() }, delay)
    timers.add(timer)
    maxTimers = Math.max(maxTimers, timers.size)
    return timer
  },
  clearTimeout(timer) { clearTimeout(timer); timers.delete(timer) },
  addEventListener() {}, removeEventListener() {},
}
globalThis.fetch = async (path, options) => {
  if (options.method === 'DELETE') { pending.delete(path.split('/').at(-1)); return { ok: true } }
  attempts += 1
  inFlight += 1
  maxInFlight = Math.max(maxInFlight, inFlight)
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
  if (name === '@xgc2/ui-react') return { initializeSkin() {}, useSkin: () => ['dark', () => {}] }
  if (name.endsWith('.css')) return {}
  throw new Error(`Unexpected import ${name}`)
}
new Function('require', 'exports', compiled)(requireMock, {})
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
} finally {
  for (const cleanup of cleanups) cleanup?.()
  Math.random = originalRandom
  for (const timer of timers) clearTimeout(timer)
}
assert.equal(pending.size, 0)
