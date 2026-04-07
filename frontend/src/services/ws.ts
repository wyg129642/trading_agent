import { useAuthStore } from '../store/auth'

export type WSMessage = {
  type: string
  data: any
}

export function createWebSocket(onMessage: (msg: WSMessage) => void): WebSocket | null {
  const token = useAuthStore.getState().token
  if (!token) return null

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  const ws = new WebSocket(`${protocol}//${window.location.host}/ws/feed?token=${token}`)

  ws.onopen = () => {
    console.log('WebSocket connected')
  }

  ws.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data)
      onMessage(msg)
    } catch {
      console.warn('Invalid WS message:', event.data)
    }
  }

  ws.onclose = (event) => {
    console.log('WebSocket closed:', event.code, event.reason)
    // Auto-reconnect after 5 seconds
    if (event.code !== 4001) {
      setTimeout(() => {
        createWebSocket(onMessage)
      }, 5000)
    }
  }

  ws.onerror = (error) => {
    console.error('WebSocket error:', error)
  }

  return ws
}
