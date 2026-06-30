import { useEffect, useState, useRef } from 'react'
import './App.css'

const NODE_PORTS = [9001, 9002, 9003]

function App() {
  const [nodes, setNodes] = useState({})
  const socketsRef = useRef({})

  useEffect(() => {
    NODE_PORTS.forEach((port) => connect(port))
    return () => {
      Object.values(socketsRef.current).forEach((ws) => ws && ws.close())
    }
  }, [])

  function connect(port) {
    const ws = new WebSocket(`ws://localhost:${port}`)
    socketsRef.current[port] = ws

    ws.onopen = () => {
      setNodes((prev) => ({ ...prev, [port]: { ...(prev[port] || {}), connected: true } }))
    }
    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data)
        setNodes((prev) => ({ ...prev, [port]: { ...data, connected: true } }))
      } catch (e) {
        console.error('bad message', e)
      }
    }
    ws.onclose = () => {
      setNodes((prev) => ({ ...prev, [port]: { ...(prev[port] || {}), connected: false, state: 'DOWN' } }))
      setTimeout(() => connect(port), 2000)
    }
    ws.onerror = () => ws.close()
  }

  const stateColor = (state) => {
    if (state === 'LEADER') return '#4ade80'
    if (state === 'CANDIDATE') return '#facc15'
    if (state === 'DOWN') return '#ef4444'
    return '#60a5fa'
  }

  const killNode = async (kvPort) => {
    try {
      await fetch(`http://localhost:7000/kill?port=${kvPort}`)
    } catch (e) {
      console.error('kill failed', e)
    }
  }

  return (
    <div style={{ padding: '40px', fontFamily: 'monospace', background: '#0f172a', minHeight: '100vh', color: 'white' }}>
      <h1 style={{ marginBottom: '8px' }}>⚡ Chaos-KV Live Dashboard</h1>
      <p style={{ color: '#94a3b8', marginBottom: '32px' }}>
        Real-time Raft cluster state — click "Kill Node" and watch re-election happen below
      </p>

      <div style={{ display: 'flex', gap: '24px', flexWrap: 'wrap' }}>
        {NODE_PORTS.map((wsPort) => {
          const kvPort = wsPort - 1000
          const node = nodes[wsPort] || {}
          const state = node.state || 'CONNECTING...'
          return (
            <div
              key={wsPort}
              style={{
                border: `2px solid ${stateColor(state)}`,
                borderRadius: '12px',
                padding: '24px',
                width: '220px',
                background: '#1e293b',
                boxShadow: state === 'LEADER' ? `0 0 24px ${stateColor(state)}` : 'none',
                transition: 'all 0.3s ease'
              }}
            >
              <div style={{ fontSize: '14px', color: '#94a3b8' }}>
                {node.nodeId || `node-${kvPort}`}
              </div>
              <div style={{ fontSize: '24px', fontWeight: 'bold', color: stateColor(state), margin: '8px 0' }}>
                {state}
              </div>
              <div style={{ fontSize: '14px', color: '#cbd5e1' }}>term: {node.term ?? '-'}</div>
              <div style={{ fontSize: '12px', color: node.connected ? '#4ade80' : '#ef4444', marginTop: '8px', marginBottom: '16px' }}>
                {node.connected ? '● connected' : '○ disconnected'}
              </div>
              <button
                onClick={() => killNode(kvPort)}
                disabled={!node.connected}
                style={{
                  width: '100%',
                  padding: '8px',
                  background: node.connected ? '#dc2626' : '#374151',
                  color: 'white',
                  border: 'none',
                  borderRadius: '6px',
                  cursor: node.connected ? 'pointer' : 'not-allowed',
                  fontFamily: 'monospace',
                  fontWeight: 'bold'
                }}
              >
                💥 Kill Node
              </button>
            </div>
          )
        })}
      </div>
    </div>
  )
}

export default App
