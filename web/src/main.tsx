import { useCallback, useEffect, useRef, useState } from 'react'
import { createRoot } from 'react-dom/client'
import {
  AppShell,
  Button,
  Panel,
  ProductBrand,
  SegmentedControl,
  StatusText,
  Topbar,
  initializeSkin,
  type SkinStore,
  type XGCSkin,
  useSkin,
} from '@xgc2/ui-react'
import '@xgc2/ui-react/styles.css'
import './styles.css'

type SessionState = 'starting' | 'connected' | 'error'

const skinCookie = 'xgc2-media-edge-skin'
function readPlayerSkin(): XGCSkin | undefined {
  const value = document.cookie.split(';').map(part => part.trim())
    .find(part => part.startsWith(`${skinCookie}=`))?.slice(skinCookie.length + 1)
  return value === 'light' || value === 'dark' ? value : undefined
}
function createPlayerSkinStore(): SkinStore {
  let current = readPlayerSkin() ?? 'dark'
  const listeners = new Set<() => void>()
  return {
    getSnapshot: () => current,
    subscribe: listener => { listeners.add(listener); return () => listeners.delete(listener) },
    setSkin: async next => {
      document.cookie = `${skinCookie}=${next}; Path=/; Max-Age=31536000; SameSite=Strict`
      if (readPlayerSkin() !== next) throw new Error('The appearance setting could not be saved.')
      current = next
      listeners.forEach(listener => listener())
    },
  }
}
const skinOptions = { store: createPlayerSkinStore() }

initializeSkin(skinOptions)

type SessionAnswer = {
  error?: string
  sdp: string
  sessionId: string
}

function App() {
  const sourceId = document.getElementById('app')?.dataset.sourceId || 'camera'
  const videoRef = useRef<HTMLVideoElement>(null)
  const peerRef = useRef<RTCPeerConnection | null>(null)
  const sessionIdRef = useRef('')
  const generationRef = useRef(0)
  const retryRef = useRef<number | null>(null)
  const backoffRef = useRef(250)
  const mountedRef = useRef(false)
  const pageActiveRef = useRef(true)
  const inFlightRef = useRef(false)
  const flightGenerationRef = useRef(0)
  const resumeAfterJoinRef = useRef(false)
  const retiringRef = useRef<Promise<void> | null>(null)
  const connectRef = useRef<(() => Promise<void>) | null>(null)
  const [skin, setSkin] = useSkin(skinOptions)
  const [sessionState, setSessionState] = useState<SessionState>('starting')
  const [stateLabel, setStateLabel] = useState('Connecting')
  const [message, setMessage] = useState('Negotiating a direct WebRTC session…')
  const [connecting, setConnecting] = useState(true)
  const [appearanceError, setAppearanceError] = useState('')

  const retireRemote = useCallback((sessionId: string) => {
    const previous = retiringRef.current
    const close = fetch(`/api/v1/sessions/${encodeURIComponent(sessionId)}`, {
      method: 'DELETE',
      credentials: 'omit',
      keepalive: true,
    }).then(response => {
      if (!response.ok) throw new Error(`session cleanup failed (${response.status})`)
    })
    const retirement = previous ? Promise.all([previous, close]).then(() => undefined) : close
    retiringRef.current = retirement
    void retirement.then(() => {
      if (retiringRef.current === retirement) retiringRef.current = null
    }, () => undefined)
    return retirement
  }, [])

  const closeSession = useCallback(() => {
    if (retryRef.current !== null) window.clearTimeout(retryRef.current)
    retryRef.current = null
    generationRef.current += 1
    const peer = peerRef.current
    const sessionId = sessionIdRef.current
    peerRef.current = null
    sessionIdRef.current = ''
    if (videoRef.current) videoRef.current.srcObject = null
    peer?.close()
    return sessionId ? retireRemote(sessionId) : retiringRef.current
  }, [retireRemote])

  const finishFlight = useCallback(() => {
    inFlightRef.current = false
    if (!resumeAfterJoinRef.current) return
    resumeAfterJoinRef.current = false
    if (mountedRef.current && pageActiveRef.current && !document.hidden) void connectRef.current?.()
  }, [])

  const scheduleRetry = useCallback(() => {
    if (!mountedRef.current || !pageActiveRef.current || document.hidden || retryRef.current !== null) return
    const delay = Math.random() * backoffRef.current
    backoffRef.current = Math.min(30_000, backoffRef.current * 2)
    retryRef.current = window.setTimeout(() => {
      retryRef.current = null
      void connectRef.current?.()
    }, delay)
  }, [])

  const connect = useCallback(async () => {
    if (!mountedRef.current || !pageActiveRef.current || document.hidden) return
    if (inFlightRef.current) {
      if (flightGenerationRef.current !== generationRef.current) resumeAfterJoinRef.current = true
      return
    }
    inFlightRef.current = true
    const retirement = closeSession()
    const currentGeneration = generationRef.current
    flightGenerationRef.current = currentGeneration
    setConnecting(true)
    setSessionState('starting')
    setStateLabel('Connecting')
    setMessage('Negotiating a direct WebRTC session…')

    if (retirement) {
      try {
        await retirement
      } catch (cause) {
        if (currentGeneration === generationRef.current) {
          setConnecting(false)
          setSessionState('error')
          setStateLabel('Unavailable')
          setMessage(cause instanceof Error ? cause.message : 'The previous camera session could not be closed.')
        }
        finishFlight()
        return
      }
    }
    if (currentGeneration !== generationRef.current || !mountedRef.current || !pageActiveRef.current || document.hidden) {
      finishFlight()
      return
    }

    const connection = new RTCPeerConnection()
    peerRef.current = connection
    const transceiver = connection.addTransceiver('video', { direction: 'recvonly' })
    // This player is for direct edge preview, not buffered playback. Keep the
    // browser's adaptive jitter target at its minimum when the API is present.
    try {
      if ('playoutDelayHint' in transceiver.receiver) {
        transceiver.receiver.playoutDelayHint = 0
      }
      if ('jitterBufferTarget' in transceiver.receiver) {
        transceiver.receiver.jitterBufferTarget = 0
      }
    } catch {
      // Older browsers expose one of these properties as read-only.
    }
    connection.addEventListener('track', (event) => {
      if (currentGeneration !== generationRef.current || !videoRef.current) return
      videoRef.current.srcObject = event.streams[0] || new MediaStream([event.track])
      setMessage('')
      void videoRef.current.play().catch(() => undefined)
    })
    connection.addEventListener('connectionstatechange', () => {
      if (currentGeneration !== generationRef.current) return
      if (connection.connectionState === 'connected') {
        backoffRef.current = 250
        if (retryRef.current !== null) window.clearTimeout(retryRef.current)
        retryRef.current = null
        setConnecting(false)
        setSessionState('connected')
        setStateLabel('Connected')
        setMessage('')
      } else if (connection.connectionState === 'failed' || connection.connectionState === 'disconnected') {
        setConnecting(false)
        setSessionState('error')
        setStateLabel('Connection lost')
        setMessage('The direct WebRTC connection stopped. Reconnecting automatically…')
        scheduleRetry()
      }
    })

    try {
      const offer = await connection.createOffer()
      await connection.setLocalDescription(offer)
      await waitForICEGathering(connection)
      if (currentGeneration !== generationRef.current || !connection.localDescription) return

      const response = await fetch(`/api/v1/sources/${encodeURIComponent(sourceId)}/sessions`, {
        method: 'POST',
        credentials: 'omit',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sdp: connection.localDescription.sdp }),
        signal: AbortSignal.timeout(15_000),
      })
      const answer = await response.json() as SessionAnswer
      if (!response.ok) throw new Error(answer.error || `session request failed (${response.status})`)
      if (currentGeneration !== generationRef.current) {
        await retireRemote(answer.sessionId)
        return
      }
      sessionIdRef.current = answer.sessionId
      await connection.setRemoteDescription({ type: 'answer', sdp: answer.sdp })
    } catch (cause) {
      if (currentGeneration !== generationRef.current) return
      closeSession()
      setConnecting(false)
      setSessionState('error')
      setStateLabel('Unavailable')
      setMessage(cause instanceof Error ? cause.message : 'Unable to open the video session.')
      scheduleRetry()
    } finally {
      finishFlight()
    }
  }, [closeSession, finishFlight, retireRemote, scheduleRetry, sourceId])

  useEffect(() => {
    mountedRef.current = true
    pageActiveRef.current = true
    connectRef.current = connect
    const visibilityChanged = () => {
      if (document.hidden) {
        closeSession()
        setConnecting(false)
        setSessionState('starting')
        setStateLabel('Paused')
        setMessage('Preview is paused while this page is hidden.')
      } else if (pageActiveRef.current) {
        backoffRef.current = 250
        void connect()
      }
    }
    const pageHidden = () => { pageActiveRef.current = false; closeSession() }
    const pageShown = () => { pageActiveRef.current = true; visibilityChanged() }
    document.addEventListener('visibilitychange', visibilityChanged)
    window.addEventListener('pagehide', pageHidden)
    window.addEventListener('pageshow', pageShown)
    visibilityChanged()
    return () => {
      mountedRef.current = false
      document.removeEventListener('visibilitychange', visibilityChanged)
      window.removeEventListener('pagehide', pageHidden)
      window.removeEventListener('pageshow', pageShown)
      closeSession()
    }
  }, [closeSession, connect])

  return (
    <AppShell
      className="media-player-shell"
      contentClassName="media-player-content"
      contentPadding="none"
      topbar={(
        <Topbar
          brand={<ProductBrand product="Media Edge" />}
          actions={(
            <>
              <SegmentedControl
                ariaLabel="Appearance"
                value={skin}
                options={[{ label: 'Light', value: 'light' }, { label: 'Dark', value: 'dark' }]}
                onValueChange={(value) => {
                  void setSkin(value as typeof skin).then(() => setAppearanceError(''), () => setAppearanceError('The appearance setting could not be saved.'))
                }}
              />
              {appearanceError ? <span role="alert">{appearanceError}</span> : null}
            </>
          )}
        />
      )}
    >
      <main className="media-player-page">
        <Panel
          className="media-player-panel"
          bodyLayout="column"
          fill
          padding="none"
          title={sourceId}
          actions={(
            <StatusText
              status={sessionState}
              tone={sessionState === 'error' ? 'danger' : sessionState === 'connected' ? 'success' : 'info'}
            >
              {stateLabel}
            </StatusText>
          )}
        >
          <section className="media-viewport" aria-label={`${sourceId} live video`}>
            <video ref={videoRef} autoPlay muted playsInline controls />
            {message ? <p className="media-message" role={sessionState === 'error' ? 'alert' : 'status'}>{message}</p> : null}
          </section>
          <footer className="media-player-footer">
            <Button className="media-player-reconnect" onClick={() => void connect()} disabled={connecting}>Reconnect</Button>
            <span>H.264 · WebRTC · direct to this edge</span>
          </footer>
        </Panel>
      </main>
    </AppShell>
  )
}

function waitForICEGathering(connection: RTCPeerConnection): Promise<void> {
  if (connection.iceGatheringState === 'complete') return Promise.resolve()
  return new Promise((resolve, reject) => {
    const timeout = window.setTimeout(() => {
      connection.removeEventListener('icegatheringstatechange', changed)
      reject(new Error('browser ICE gathering timed out'))
    }, 15_000)
    function changed() {
      if (connection.iceGatheringState !== 'complete') return
      window.clearTimeout(timeout)
      connection.removeEventListener('icegatheringstatechange', changed)
      resolve()
    }
    connection.addEventListener('icegatheringstatechange', changed)
  })
}

const root = document.getElementById('app')
if (!root) throw new Error('Media Edge player root is unavailable')
createRoot(root).render(<App />)
