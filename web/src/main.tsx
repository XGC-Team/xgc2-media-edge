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
} from '@xgc2/ui-react'
import '@xgc2/ui-react/styles.css'
import './styles.css'

type Skin = 'light' | 'dark'
type SessionState = 'starting' | 'connected' | 'error'

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
  const [skin, setSkin] = useState<Skin>(readSkin)
  const [sessionState, setSessionState] = useState<SessionState>('starting')
  const [stateLabel, setStateLabel] = useState('Connecting')
  const [message, setMessage] = useState('Negotiating a direct WebRTC session…')
  const [connecting, setConnecting] = useState(true)

  useEffect(() => {
    document.documentElement.dataset.skin = skin
    try { localStorage.setItem('xgc2-media-edge.skin', skin) } catch { /* storage can be unavailable */ }
  }, [skin])

  const closeSession = useCallback(() => {
    generationRef.current += 1
    const peer = peerRef.current
    const sessionId = sessionIdRef.current
    peerRef.current = null
    sessionIdRef.current = ''
    if (videoRef.current) videoRef.current.srcObject = null
    peer?.close()
    if (sessionId) {
      void fetch(`/api/v1/sessions/${encodeURIComponent(sessionId)}`, {
        method: 'DELETE',
        credentials: 'omit',
        keepalive: true,
      })
    }
  }, [])

  const connect = useCallback(async () => {
    closeSession()
    const currentGeneration = generationRef.current
    setConnecting(true)
    setSessionState('starting')
    setStateLabel('Connecting')
    setMessage('Negotiating a direct WebRTC session…')

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
        setConnecting(false)
        setSessionState('connected')
        setStateLabel('Connected')
        setMessage('')
      } else if (connection.connectionState === 'failed' || connection.connectionState === 'disconnected') {
        setConnecting(false)
        setSessionState('error')
        setStateLabel('Connection lost')
        setMessage('The direct WebRTC connection stopped. Reconnect to try again.')
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
      })
      const answer = await response.json() as SessionAnswer
      if (!response.ok) throw new Error(answer.error || `session request failed (${response.status})`)
      if (currentGeneration !== generationRef.current) {
        void fetch(`/api/v1/sessions/${encodeURIComponent(answer.sessionId)}`, {
          method: 'DELETE',
          credentials: 'omit',
          keepalive: true,
        })
        return
      }
      sessionIdRef.current = answer.sessionId
      await connection.setRemoteDescription({ type: 'answer', sdp: answer.sdp })
    } catch (cause) {
      if (currentGeneration !== generationRef.current) return
      closeSession()
      setConnecting(false)
      setSessionState('error')
      setStateLabel('Error')
      setMessage(cause instanceof Error ? cause.message : 'Unable to open the video session.')
    }
  }, [closeSession, sourceId])

  useEffect(() => {
    void connect()
    window.addEventListener('pagehide', closeSession)
    return () => {
      window.removeEventListener('pagehide', closeSession)
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
            <SegmentedControl
              ariaLabel="Appearance"
              value={skin}
              options={[{ label: 'Light', value: 'light' }, { label: 'Dark', value: 'dark' }]}
              onValueChange={(value) => setSkin(value as Skin)}
            />
          )}
        />
      )}
    >
      <main className="media-player-page">
        <Panel
          className="media-player-panel"
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
            <Button onClick={() => void connect()} disabled={connecting}>Reconnect</Button>
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

function readSkin(): Skin {
  try { return localStorage.getItem('xgc2-media-edge.skin') === 'light' ? 'light' : 'dark' } catch { return 'dark' }
}

const root = document.getElementById('app')
if (!root) throw new Error('Media Edge player root is unavailable')
createRoot(root).render(<App />)
