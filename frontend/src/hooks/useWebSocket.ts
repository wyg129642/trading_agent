import { useEffect, useRef, useCallback } from 'react'
import { useAuthStore } from '../store/auth'

export function useWebSocket(onMessage: (data: any) => void) {
  const wsRef = useRef<WebSocket | null>(null)
  const token = useAuthStore((s) => s.token)

  const connect = useCallback(() => {
    if (!token) return

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws/feed?token=${token}`)

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data)
        onMessage(data)
      } catch {
        // ignore non-JSON messages like "pong"
      }
    }

    ws.onclose = (event) => {
      if (event.code !== 4001) {
        setTimeout(connect, 5000)
      }
    }

    wsRef.current = ws
  }, [token, onMessage])

  useEffect(() => {
    connect()
    return () => {
      wsRef.current?.close()
    }
  }, [connect])

  return wsRef
}
