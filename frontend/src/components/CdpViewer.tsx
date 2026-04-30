import { useEffect, useRef, useState } from 'react'
import { Alert, Button, Input, Spin, Tag, Typography } from 'antd'
import { useAuthStore } from '../store/auth'
import api from '../services/api'

interface NetworkEntry {
  seq: number
  ts: number
  method: string
  url: string
  resource_type: string
  post_data: string | null
  status: number | null
  response_preview: string | null
  response_body?: string | null
  response_time_ms: number | null
}

interface CdpViewerProps {
  platformKey: string
  onSuccess: () => void
  onCancel: () => void
  /**
   * "login" (default) — open login URL + poll for credentials + auto-extract button.
   * "viewer" — pre-inject saved credentials and open the platform's main data
   *            page for side-by-side comparison. No extract button.
   */
  mode?: 'login' | 'viewer'
  /** Deep-link section key for viewer mode (e.g. "research" / "summary"). */
  section?: string
}

interface SessionInit {
  session_id: string
  status: string
  message: string
  viewport: [number, number]
}

// Maps a browser KeyboardEvent to a CDP-ish input event. We keep this simple;
// CDP expects `key`, `code`, `text` fields, which KeyboardEvent already has.
function keyEventToCdp(e: KeyboardEvent, action: 'down' | 'up') {
  const mods =
    (e.altKey ? 1 : 0) | (e.ctrlKey ? 2 : 0) | (e.metaKey ? 4 : 0) | (e.shiftKey ? 8 : 0)
  const isPrintable = e.key && e.key.length === 1 && !e.ctrlKey && !e.metaKey && !e.altKey
  return {
    type: 'key',
    action,
    key: e.key,
    code: e.code,
    modifiers: mods,
    text: isPrintable && action === 'down' ? e.key : undefined,
  }
}

function mouseButtonName(btn: number): string {
  if (btn === 0) return 'left'
  if (btn === 1) return 'middle'
  if (btn === 2) return 'right'
  return 'left'
}

export default function CdpViewer({ platformKey, onSuccess, onCancel, mode = 'login', section }: CdpViewerProps) {
  const isViewer = mode === 'viewer'
  const [session, setSession] = useState<SessionInit | null>(null)
  const [frame, setFrame] = useState<string | null>(null)
  const [status, setStatus] = useState('STARTING')
  // Network-panel state (DevTools-lite)
  const [netOpen, setNetOpen] = useState(false)
  const [netEntries, setNetEntries] = useState<NetworkEntry[]>([])
  const [netFilter, setNetFilter] = useState('')
  const [netDetail, setNetDetail] = useState<NetworkEntry | null>(null)
  const [netDetailBody, setNetDetailBody] = useState<string>('')
  const netSinceSeq = useRef(0)
  const [message, setMessage] = useState('正在启动远程浏览器…')
  const [error, setError] = useState<string | null>(null)
  const [extracting, setExtracting] = useState(false)
  const [extractHint, setExtractHint] = useState<string | null>(null)
  const [probeData, setProbeData] = useState<any | null>(null)
  const [backgroundPolling, setBackgroundPolling] = useState(false)
  const wsRef = useRef<WebSocket | null>(null)
  const imgRef = useRef<HTMLImageElement>(null)
  const startedRef = useRef(false)
  const pollTimerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Boot: POST /screencast/start → open WS
  useEffect(() => {
    if (startedRef.current) return
    startedRef.current = true
    ;(async () => {
      try {
        const startPath = isViewer
          ? `/data-sources/${platformKey}/viewer/start${section ? `?section=${encodeURIComponent(section)}` : ''}`
          : `/data-sources/${platformKey}/screencast/start`
        const res = await api.post<SessionInit>(startPath)
        setSession(res.data)
        setStatus(res.data.status)
        setMessage(res.data.message)
        // Open WS. The Vite dev proxy and nginx both forward /api → backend
        // with WebSocket upgrade, so the URL is just our API with ws(s) scheme.
        const proto = window.location.protocol === 'https:' ? 'wss' : 'ws'
        const url = `${proto}://${window.location.host}/api/data-sources/${platformKey}/screencast/${res.data.session_id}/ws`
        const ws = new WebSocket(url)
        ws.binaryType = 'arraybuffer'
        wsRef.current = ws
        // Track the previous Blob URL so we can revoke it after the next
        // frame paints — otherwise a long session leaks dozens of MB of
        // Blob references into the DOM cache.
        let prevBlobUrl: string | null = null
        ws.onmessage = (ev) => {
          // Binary WS message = raw JPEG bytes (server fast-path). Wrap in
          // a Blob URL and let <img> decode on the GPU. ~3-5× faster than
          // the data:image/jpeg;base64,... path which forces a CPU base64
          // decode + the browser's data-URL parser on every frame.
          if (ev.data instanceof ArrayBuffer) {
            const blob = new Blob([ev.data], { type: 'image/jpeg' })
            const url = URL.createObjectURL(blob)
            setFrame(url)
            // Revoke the previous frame's blob URL on the next macrotask so
            // the <img> has time to swap before the GC reclaims memory.
            if (prevBlobUrl) {
              const stale = prevBlobUrl
              setTimeout(() => URL.revokeObjectURL(stale), 0)
            }
            prevBlobUrl = url
            return
          }
          try {
            const msg = JSON.parse(ev.data)
            if (msg.type === 'frame') {
              // Legacy JSON-wrapped base64 fallback (heartbeat / older server).
              setFrame(`data:image/jpeg;base64,${msg.data}`)
            } else if (msg.type === 'status') {
              setStatus(msg.status)
              setMessage(msg.message || '')
              // Don't auto-collapse the drawer on SUCCESS — keep the live
              // browser visible so the user can verify the logged-in state,
              // click around, etc. They close it explicitly with the
              // "完成并保存" button below (which fires onSuccess).
            } else if (msg.type === 'heartbeat') {
              setStatus(msg.status)
              setMessage(msg.message || '')
            } else if (msg.type === 'copy-response') {
              // Remote → local clipboard
              const text = msg.text || ''
              if (text && navigator.clipboard?.writeText) {
                navigator.clipboard.writeText(text).catch(() => {})
              }
            }
          } catch {
            /* ignore malformed */
          }
        }
        ws.onerror = () => setError('WebSocket 连接失败')
        ws.onclose = () => setStatus((s) => (s === 'SUCCESS' ? s : 'CLOSED'))
      } catch (e: any) {
        setError(e.response?.data?.detail || String(e))
      }
    })()

    return () => {
      // Teardown: tell backend to kill the Chromium when component unmounts.
      const ws = wsRef.current
      if (ws) {
        try { ws.close() } catch {}
      }
      if (startedRef.current && session?.session_id) {
        api.delete(`/data-sources/${platformKey}/screencast/${session.session_id}`).catch(() => {})
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [platformKey])

  // Mouse handlers — coordinate mapping from <img> display size → viewport coords.
  const sendInput = (evt: Record<string, any>) => {
    const ws = wsRef.current
    if (!ws || ws.readyState !== WebSocket.OPEN) return
    ws.send(JSON.stringify(evt))
  }

  const mapCoords = (clientX: number, clientY: number): { x: number; y: number } => {
    const img = imgRef.current
    if (!img || !session) return { x: clientX, y: clientY }
    const rect = img.getBoundingClientRect()
    const relX = ((clientX - rect.left) / rect.width) * session.viewport[0]
    const relY = ((clientY - rect.top) / rect.height) * session.viewport[1]
    return { x: relX, y: relY }
  }

  // mousemove fires at the pointer's native sample rate (~120Hz on
  // high-DPI touchpads), but Chromium only renders frames at
  // ~15-30fps so most of those events are wasted bandwidth and just
  // backlog the input_queue. Coalesce via requestAnimationFrame so we
  // ship at most one mousemove per browser paint (~60fps cap; usually
  // matches the screencast rate). The latest coords always win.
  const moveCoordsRef = useRef<{ x: number; y: number } | null>(null)
  const moveRafRef = useRef<number | null>(null)
  const flushMove = () => {
    moveRafRef.current = null
    const c = moveCoordsRef.current
    moveCoordsRef.current = null
    if (!c) return
    sendInput({ type: 'mouse', action: 'move', x: c.x, y: c.y })
  }
  const onMouseMove = (e: React.MouseEvent) => {
    moveCoordsRef.current = mapCoords(e.clientX, e.clientY)
    if (moveRafRef.current === null) {
      moveRafRef.current = requestAnimationFrame(flushMove)
    }
  }
  const onMouseDown = (e: React.MouseEvent) => {
    imgRef.current?.focus()
    const { x, y } = mapCoords(e.clientX, e.clientY)
    sendInput({
      type: 'mouse', action: 'down', x, y,
      button: mouseButtonName(e.button), clickCount: e.detail,
    })
  }
  const onMouseUp = (e: React.MouseEvent) => {
    const { x, y } = mapCoords(e.clientX, e.clientY)
    sendInput({
      type: 'mouse', action: 'up', x, y,
      button: mouseButtonName(e.button), clickCount: e.detail,
    })
  }
  const onWheel = (e: React.WheelEvent) => {
    const { x, y } = mapCoords(e.clientX, e.clientY)
    sendInput({ type: 'wheel', x, y, deltaX: e.deltaX, deltaY: e.deltaY })
  }

  // Global keyboard while viewer is focused.
  useEffect(() => {
    const el = imgRef.current
    if (!el) return
    const isPasteShortcut = (e: KeyboardEvent) =>
      (e.ctrlKey || e.metaKey) && !e.altKey && !e.shiftKey && e.key.toLowerCase() === 'v'
    const isCopyShortcut = (e: KeyboardEvent) =>
      (e.ctrlKey || e.metaKey) && !e.altKey && !e.shiftKey && e.key.toLowerCase() === 'c'

    const onKeyDown = (e: KeyboardEvent) => {
      // Ctrl/Cmd+V → read local clipboard, inject as insertText remotely.
      if (isPasteShortcut(e)) {
        e.preventDefault()
        ;(async () => {
          try {
            const text = await navigator.clipboard.readText()
            if (text) sendInput({ type: 'paste', text })
          } catch {
            // Permission denied / HTTP context — fall through to raw key pass.
            sendInput(keyEventToCdp(e, 'down'))
          }
        })()
        return
      }
      // Ctrl/Cmd+C → ask backend for remote selection, relay to local clipboard.
      if (isCopyShortcut(e)) {
        e.preventDefault()
        sendInput({ type: 'copy-request' })
        return
      }
      e.preventDefault()
      sendInput(keyEventToCdp(e, 'down'))
    }
    const onKeyUp = (e: KeyboardEvent) => {
      if (isPasteShortcut(e) || isCopyShortcut(e)) {
        e.preventDefault()
        return
      }
      e.preventDefault()
      sendInput(keyEventToCdp(e, 'up'))
    }
    // Also catch native `paste` events (right-click → paste, IME paste, etc.).
    const onPaste = (e: ClipboardEvent) => {
      const text = e.clipboardData?.getData('text') || ''
      if (text) {
        e.preventDefault()
        sendInput({ type: 'paste', text })
      }
    }
    el.addEventListener('keydown', onKeyDown)
    el.addEventListener('keyup', onKeyUp)
    el.addEventListener('paste', onPaste as any)
    return () => {
      el.removeEventListener('keydown', onKeyDown)
      el.removeEventListener('keyup', onKeyUp)
      el.removeEventListener('paste', onPaste as any)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [frame, session])

  // Network-panel poller — tails the backend's ring buffer every 1.5s.
  useEffect(() => {
    if (!netOpen || !session) return
    let cancelled = false
    const tick = async () => {
      try {
        const res = await api.get(
          `/data-sources/${platformKey}/screencast/${session.session_id}/network`,
          { params: { since_seq: netSinceSeq.current, limit: 100 } },
        )
        if (cancelled) return
        const fresh: NetworkEntry[] = res.data.entries || []
        if (fresh.length) {
          netSinceSeq.current = Math.max(netSinceSeq.current, ...fresh.map((e) => e.seq))
          setNetEntries((prev) => [...prev, ...fresh].slice(-200))
        }
      } catch {
        /* session may be closed; silent retry */
      }
    }
    tick()
    const iv = setInterval(tick, 1500)
    return () => {
      cancelled = true
      clearInterval(iv)
    }
  }, [netOpen, session, platformKey])

  // Lazy-load full response body when user clicks an entry.
  const openNetDetail = async (entry: NetworkEntry) => {
    setNetDetail(entry)
    setNetDetailBody(entry.response_preview || '(loading…)')
    if (!session) return
    try {
      const res = await api.get(
        `/data-sources/${platformKey}/screencast/${session.session_id}/network`,
        { params: { since_seq: entry.seq - 1, limit: 5, full: 1 } },
      )
      const match = (res.data.entries || []).find((e: NetworkEntry) => e.seq === entry.seq)
      if (match?.response_body) setNetDetailBody(match.response_body)
      else if (match?.response_preview) setNetDetailBody(match.response_preview)
    } catch {
      setNetDetailBody('(响应已过期或体积过大)')
    }
  }

  const filteredNet = netFilter
    ? netEntries.filter((e) => e.url.toLowerCase().includes(netFilter.toLowerCase()))
    : netEntries

  if (error) {
    return <Alert type="error" showIcon message="启动失败" description={error} />
  }

  return (
    <div>
      <Alert
        type={status === 'SUCCESS' && !isViewer ? 'success' : 'info'}
        showIcon
        style={{ marginBottom: 8 }}
        message={
          status === 'SUCCESS' && !isViewer
            ? '✅ 登录成功 — 浏览器已保持开启'
            : isViewer ? '实时平台查看' : '远程浏览器'
        }
        description={
          status === 'SUCCESS' && !isViewer ? (
            <>
              凭证已写入,后台爬虫已自动启动。浏览器**不会自动关闭**,
              你可以继续点页面、查行情、复制数据,做完点右下角【完成并保存】关掉。
            </>
          ) : isViewer ? (
            <>
              已注入本地凭证, 直接进入已登录状态. 可以在右侧对着我们的库做数据比对;
              鼠标 / 键盘 / Ctrl+C / Ctrl+V 都可以用.
            </>
          ) : (
            <>
              页面里嵌着一台在服务器跑的 Chromium — 像真浏览器一样用。
              鼠标点 / 键盘输入 / Ctrl+C / Ctrl+V 都会转发到后端,
              登录成功自动写 token。
            </>
          )
        }
      />
      <div
        style={{
          position: 'relative',
          border: '1px solid #e2e8f0',
          borderRadius: 6,
          background: '#0f172a',
          overflow: 'hidden',
          minHeight: 300,
        }}
      >
        {frame ? (
          <img
            ref={imgRef}
            src={frame}
            tabIndex={0}
            onMouseMove={onMouseMove}
            onMouseDown={onMouseDown}
            onMouseUp={onMouseUp}
            onWheel={onWheel}
            onContextMenu={(e) => e.preventDefault()}
            alt="remote browser"
            style={{
              display: 'block',
              width: '100%',
              cursor: 'crosshair',
              outline: 'none',
              userSelect: 'none',
            }}
            draggable={false}
          />
        ) : (
          <div style={{ padding: 60, textAlign: 'center' }}>
            <Spin tip={message} />
          </div>
        )}
      </div>
      <div style={{ marginTop: 8 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8 }}>
          <Typography.Text type="secondary" style={{ fontSize: 11, flex: 1 }}>
            状态: {status} · {message}
          </Typography.Text>
          {!isViewer && <Button
            type="primary"
            size="small"
            loading={extracting}
            disabled={!session || status === 'SUCCESS' || status === 'CLOSED'}
            onClick={async () => {
              if (!session) return
              setExtracting(true)
              setExtractHint('探测中...(最多 10s)')
              setProbeData(null)
              try {
                const res = await api.post(
                  `/data-sources/${platformKey}/screencast/${session.session_id}/extract-now`,
                )
                if (res.data.ok) {
                  const pid = res.data.crawler_pid
                  setExtractHint(
                    `✅ 凭证已抓取${pid ? ` · 爬虫 PID ${pid} 已启动` : ''}${
                      '\n监控面板将在 10s 内同步, 窗口即将关闭…'
                    }`,
                  )
                  setTimeout(onSuccess, 2500)
                  return
                }
                // Fail — fetch a full probe + keep polling in background.
                setExtractHint(
                  `❌ 10s 内未找到 token。后台继续重试,同时拉取 Chromium 状态用于诊断…`,
                )
                try {
                  const probe = await api.get(
                    `/data-sources/${platformKey}/screencast/${session.session_id}/probe`,
                  )
                  setProbeData(probe.data)
                } catch { /* ignore */ }
                setBackgroundPolling(true)
              } catch (e: any) {
                setExtractHint(e.response?.data?.detail || '请求失败')
              } finally {
                setExtracting(false)
              }
            }}
          >
            ✓ 我已登录,提取凭证
          </Button>}
          <Button
            size="small"
            type={netOpen ? 'primary' : 'default'}
            onClick={() => setNetOpen((v) => !v)}
            title="打开 DevTools 风格的 Network 面板 — 抓 XHR/fetch 请求"
          >
            🔧 Network {netEntries.length > 0 ? `(${netEntries.length})` : ''}
          </Button>
          <Button
            onClick={status === 'SUCCESS' ? onSuccess : onCancel}
            size="small"
            type={status === 'SUCCESS' ? 'primary' : 'default'}
          >
            {status === 'SUCCESS' ? '完成并保存' : '关闭'}
          </Button>
        </div>
        {netOpen && (
          <div
            style={{
              marginTop: 8,
              border: '1px solid #334155',
              borderRadius: 4,
              background: '#0f172a',
              color: '#e2e8f0',
              fontSize: 11,
              fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
            }}
          >
            <div
              style={{
                display: 'flex',
                gap: 8,
                alignItems: 'center',
                padding: '6px 8px',
                borderBottom: '1px solid #334155',
                background: '#1e293b',
              }}
            >
              <span style={{ fontWeight: 600 }}>Network</span>
              <Input
                size="small"
                placeholder="filter URL… e.g. queryOpinionList"
                value={netFilter}
                onChange={(e) => setNetFilter(e.target.value)}
                style={{ flex: 1, background: '#0f172a', color: '#e2e8f0', fontSize: 11 }}
                allowClear
              />
              <Button
                size="small"
                onClick={() => {
                  setNetEntries([])
                  netSinceSeq.current = 0
                }}
              >
                Clear
              </Button>
              <span style={{ color: '#94a3b8' }}>
                total: {netEntries.length} shown: {filteredNet.length}
              </span>
            </div>
            <div style={{ maxHeight: 220, overflowY: 'auto' }}>
              {filteredNet.length === 0 && (
                <div style={{ padding: 10, color: '#64748b' }}>
                  {netOpen ? '等待请求… 在远程浏览器里点击页面触发 XHR' : ''}
                </div>
              )}
              {filteredNet.slice().reverse().map((e) => {
                const statusColor =
                  e.status == null ? '#94a3b8'
                    : e.status < 300 ? '#5dd39e'
                    : e.status < 400 ? '#f0c674'
                    : '#ef6f6c'
                const short = e.url.replace(/^https?:\/\/[^/]+/, '')
                return (
                  <div
                    key={e.seq}
                    onClick={() => openNetDetail(e)}
                    style={{
                      display: 'flex',
                      gap: 8,
                      padding: '3px 8px',
                      borderBottom: '1px solid #1e293b',
                      cursor: 'pointer',
                    }}
                    onMouseEnter={(ev) => ((ev.currentTarget as HTMLElement).style.background = '#1e293b')}
                    onMouseLeave={(ev) => ((ev.currentTarget as HTMLElement).style.background = 'transparent')}
                  >
                    <span style={{ color: statusColor, width: 32 }}>
                      {e.status ?? '…'}
                    </span>
                    <span style={{ color: '#f0c674', width: 40 }}>{e.method}</span>
                    <span style={{ flex: 1, wordBreak: 'break-all', overflow: 'hidden' }}>
                      {short.length > 140 ? short.slice(0, 140) + '…' : short}
                    </span>
                    <span style={{ color: '#94a3b8', width: 50, textAlign: 'right' }}>
                      {e.response_time_ms != null ? `${e.response_time_ms}ms` : ''}
                    </span>
                  </div>
                )
              })}
            </div>
            {netDetail && (
              <div style={{ padding: 8, borderTop: '2px solid #334155', background: '#0a0f1a' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                  <span style={{ color: '#5dd39e' }}>
                    #{netDetail.seq} · {netDetail.method} · status {netDetail.status ?? '-'}
                  </span>
                  <a
                    onClick={() => {
                      setNetDetail(null)
                      setNetDetailBody('')
                    }}
                    style={{ color: '#94a3b8', cursor: 'pointer' }}
                  >
                    ✕
                  </a>
                </div>
                <div style={{ color: '#e2e8f0', wordBreak: 'break-all', marginBottom: 4 }}>
                  {netDetail.url}
                </div>
                {netDetail.post_data && (
                  <details style={{ marginBottom: 4 }}>
                    <summary style={{ color: '#f0c674', cursor: 'pointer' }}>
                      request body ({netDetail.post_data.length} B)
                    </summary>
                    <pre style={{ margin: 0, whiteSpace: 'pre-wrap', color: '#cbd5e1' }}>
                      {netDetail.post_data}
                    </pre>
                  </details>
                )}
                <details open>
                  <summary style={{ color: '#5dd39e', cursor: 'pointer' }}>
                    response body ({(netDetailBody || '').length} B)
                  </summary>
                  <pre
                    style={{
                      margin: 0,
                      whiteSpace: 'pre-wrap',
                      color: '#cbd5e1',
                      maxHeight: 200,
                      overflow: 'auto',
                    }}
                  >
                    {netDetailBody}
                  </pre>
                </details>
              </div>
            )}
          </div>
        )}
        {extractHint && (
          <Typography.Paragraph
            type="secondary"
            style={{ fontSize: 11, marginTop: 6, marginBottom: 0, whiteSpace: 'pre-wrap' }}
          >
            {extractHint}
          </Typography.Paragraph>
        )}
        {probeData && (
          <div
            style={{
              marginTop: 8,
              padding: 8,
              background: '#f1f5f9',
              borderRadius: 4,
              fontSize: 10,
              fontFamily: 'monospace',
              maxHeight: 160,
              overflow: 'auto',
            }}
          >
            <div>URL: {probeData.url}</div>
            <div>cookie_count: {probeData.cookie_count}</div>
            <div>
              localStorage keys (
              {(probeData.localStorage_keys || []).length}):{' '}
              {(probeData.localStorage_keys || []).join(', ') || '(空)'}
            </div>
            {probeData.extract_error && (
              <div style={{ color: '#ef4444' }}>extract 报错: {probeData.extract_error}</div>
            )}
          </div>
        )}
        {backgroundPolling && status !== 'SUCCESS' && (
          <Typography.Text type="secondary" style={{ fontSize: 11, display: 'block', marginTop: 6 }}>
            🔄 后台每 3s 重试中,等 hook 捕获到 token 就自动完成
          </Typography.Text>
        )}
      </div>
    </div>
  )
}
