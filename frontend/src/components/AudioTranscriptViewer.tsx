/**
 * AudioTranscriptViewer — AlphaPai-style reading UI for an ASR-transcribed
 * audio document. Three-zone layout:
 *
 *   ┌── transcript bubbles (left) ──┬── right panel (AI 要点 / 分段摘要) ──┐
 *   │  speaker avatar + timestamp   │  tabs: AI 要点 | 分段摘要           │
 *   │  + paragraph text (editable)  │  auto-generated on first open        │
 *   │  ...                          │                                      │
 *   └───────────────────────────────┴──────────────────────────────────────┘
 *   ┌── audio player bar (scrubber + play + speed) ────────────────────────┐
 *   └──────────────────────────────────────────────────────────────────────┘
 *
 * Differences vs. the previous drawer-oriented version:
 *  - AI summary is auto-generated on first mount (instead of requiring a
 *    "Generate" button click), matching AlphaPai's self-summary-detail UX.
 *  - Second right-panel tab "分段摘要" groups segments into 3–8 chapters
 *    with titles and 2–5 bullets each, sourced from a new backend endpoint.
 *  - Transcript paragraphs are click-to-edit (owner-only); saves go through
 *    a PATCH endpoint and invalidate both caches server-side.
 *  - Speaker avatars alternate color to give the chat-log feel even though
 *    our ASR pipeline doesn't yet emit speaker diarization labels.
 */
import {
  useCallback, useEffect, useMemo, useRef, useState,
} from 'react'
import {
  Alert, Avatar, Button, Empty, Input, Skeleton, Space, Spin, Tabs, Tag,
  Tooltip, Typography, message as antdMessage,
} from 'antd'
import {
  PauseCircleFilled, PlayCircleFilled, DownloadOutlined, ReloadOutlined,
  StepBackwardOutlined, StepForwardOutlined,
  SoundOutlined, FileTextOutlined, BulbOutlined, OrderedListOutlined,
  EditOutlined, CheckOutlined, CloseOutlined,
} from '@ant-design/icons'
import api from '../services/api'

const { Text } = Typography

// ── Types (mirror backend/app/api/user_kb.py) ────────────────

export interface AudioSegment {
  index: number
  start_ms: number
  end_ms: number
  text: string
}

export interface AudioChapter {
  index: number
  title: string
  start_ms: number
  end_ms: number
  start_segment_index: number
  bullets: string[]
}

export interface AudioMeta {
  duration_seconds: number | null
  language: string | null
  segments: AudioSegment[]
  summary: string | null
  summary_generated_at: string | null
  chapters?: AudioChapter[] | null
  chapters_generated_at?: string | null
}

interface AudioSummaryPayload {
  document_id: string
  summary: string
  generated_at: string
  cached: boolean
}

interface AudioChaptersPayload {
  document_id: string
  chapters: AudioChapter[]
  generated_at: string
  cached: boolean
}

interface Props {
  documentId: string
  title: string
  audio: AudioMeta
  filename: string
  canEdit?: boolean
  // Fired after a segment edit succeeds — parent can refresh its copy.
  onSegmentEdited?: (segments: AudioSegment[]) => void
  // Passthrough for the download button so blob logic lives in one place.
  onDownload?: () => void
}

// ── Helpers ──────────────────────────────────────────────────

const PLAYBACK_RATES = [0.75, 1, 1.25, 1.5, 2] as const

// Speaker avatar palette — alternates so adjacent bubbles stay visually
// distinct. Two-color rotation keeps it readable (investor / expert feel)
// without pretending we have real diarization.
const SPEAKER_PALETTE = [
  { bg: '#fef3c7', fg: '#b45309', label: '发言人 A' },
  { bg: '#dbeafe', fg: '#1d4ed8', label: '发言人 B' },
] as const

function formatTime(seconds: number): string {
  if (!isFinite(seconds) || seconds < 0) return '00:00'
  const s = Math.floor(seconds)
  const h = Math.floor(s / 3600)
  const m = Math.floor((s % 3600) / 60)
  const sec = s % 60
  const pad = (n: number) => n.toString().padStart(2, '0')
  return h > 0 ? `${h}:${pad(m)}:${pad(sec)}` : `${pad(m)}:${pad(sec)}`
}

function indexOfSegmentAt(segments: AudioSegment[], timeMs: number): number {
  // Linear scan is fine — a 3-hour meeting is ~180 segments; binary search
  // would be premature optimization.
  for (let i = 0; i < segments.length; i++) {
    const s = segments[i]
    if (timeMs >= s.start_ms && timeMs < s.end_ms) return i
  }
  return segments.length > 0 ? segments.length - 1 : -1
}

function Waveform({
  progress, onSeek, duration,
}: {
  progress: number
  onSeek: (ratio: number) => void
  duration: number
}) {
  const barsRef = useRef<number[] | null>(null)
  if (!barsRef.current) {
    const bars: number[] = []
    let seed = 1337
    for (let i = 0; i < 160; i++) {
      seed = (seed * 9301 + 49297) % 233280
      bars.push(0.25 + (seed / 233280) * 0.75)
    }
    barsRef.current = bars
  }
  const bars = barsRef.current
  const onClick = (e: React.MouseEvent<HTMLDivElement>) => {
    if (!duration) return
    const rect = e.currentTarget.getBoundingClientRect()
    const ratio = (e.clientX - rect.left) / rect.width
    onSeek(Math.max(0, Math.min(1, ratio)))
  }
  return (
    <div
      onClick={onClick}
      style={{
        flex: 1, height: 36, display: 'flex', alignItems: 'center',
        gap: 1.5, cursor: 'pointer', userSelect: 'none', overflow: 'hidden',
      }}
    >
      {bars.map((h, i) => {
        const pos = i / bars.length
        const active = pos <= progress
        return (
          <div
            key={i}
            style={{
              flex: 1, height: `${Math.max(4, h * 100)}%`, minHeight: 3,
              background: active ? '#2ec98a' : '#cbd5e1',
              borderRadius: 1, transition: 'background 120ms',
            }}
          />
        )
      })}
    </div>
  )
}

// Lightweight inline Markdown. The summary backend emits `#`, `##`, `- `,
// numeric lists, and **bold** — no need to pull a full parser in.
function SummaryMarkdown({ text }: { text: string }) {
  const renderInline = (s: string, keyPrefix: string): React.ReactNode[] => {
    const parts: React.ReactNode[] = []
    const re = /\*\*([^*]+)\*\*/g
    let lastIdx = 0
    let m: RegExpExecArray | null
    let i = 0
    while ((m = re.exec(s)) !== null) {
      if (m.index > lastIdx) parts.push(s.slice(lastIdx, m.index))
      parts.push(
        <strong key={`${keyPrefix}-b-${i++}`} style={{ color: '#0f172a' }}>
          {m[1]}
        </strong>,
      )
      lastIdx = re.lastIndex
    }
    if (lastIdx < s.length) parts.push(s.slice(lastIdx))
    return parts
  }
  const lines = text.split('\n')
  const blocks: React.ReactNode[] = []
  let bulletList: string[] = []
  const flushList = (key: string) => {
    if (bulletList.length === 0) return
    blocks.push(
      <ul key={key} style={{ marginTop: 4, marginBottom: 12, paddingLeft: 22 }}>
        {bulletList.map((li, i) => (
          <li key={i} style={{
            lineHeight: 1.8, color: '#334155', fontSize: 13.5,
            marginBottom: 4,
          }}>
            {renderInline(li, `li-${key}-${i}`)}
          </li>
        ))}
      </ul>,
    )
    bulletList = []
  }
  lines.forEach((raw, i) => {
    const line = raw.trimEnd()
    if (line.startsWith('# ')) {
      flushList(`fl-${i}`)
      blocks.push(
        <div key={`h1-${i}`} style={{
          fontSize: 17, fontWeight: 700, color: '#0f172a',
          marginTop: i === 0 ? 0 : 20, marginBottom: 10, paddingBottom: 6,
          borderBottom: '2px solid #2ec98a', display: 'inline-block',
        }}>
          {line.slice(2)}
        </div>,
      )
    } else if (line.startsWith('## ')) {
      flushList(`fl-${i}`)
      blocks.push(
        <div key={`h2-${i}`} style={{
          fontSize: 14, fontWeight: 600, color: '#1e293b',
          marginTop: 14, marginBottom: 6,
        }}>
          {renderInline(line.slice(3), `h2-${i}`)}
        </div>,
      )
    } else if (line.startsWith('- ')) {
      bulletList.push(line.slice(2))
    } else if (/^\d+\.\s/.test(line)) {
      // Convert numbered lists ("1. foo") into bullets so they render cleanly.
      bulletList.push(line.replace(/^\d+\.\s/, ''))
    } else if (line === '') {
      flushList(`fl-${i}`)
    } else {
      flushList(`fl-${i}`)
      blocks.push(
        <div key={`p-${i}`} style={{
          lineHeight: 1.8, color: '#334155', fontSize: 13.5, marginBottom: 6,
        }}>
          {renderInline(line, `p-${i}`)}
        </div>,
      )
    }
  })
  flushList('fl-end')
  return <div>{blocks}</div>
}

// ── Main component ──────────────────────────────────────────

export default function AudioTranscriptViewer({
  documentId, title, audio, filename, canEdit = false,
  onSegmentEdited, onDownload,
}: Props) {
  const audioRef = useRef<HTMLAudioElement | null>(null)
  const segmentListRef = useRef<HTMLDivElement | null>(null)

  const [audioUrl, setAudioUrl] = useState<string>('')
  const [audioLoadError, setAudioLoadError] = useState<string>('')
  const [audioLoading, setAudioLoading] = useState(true)

  const [playing, setPlaying] = useState(false)
  const [currentTimeMs, setCurrentTimeMs] = useState(0)
  const [durationSec, setDurationSec] = useState<number>(
    audio.duration_seconds || 0,
  )
  const [rate, setRate] = useState<number>(1)

  const [segments, setSegments] = useState<AudioSegment[]>(audio.segments || [])
  useEffect(() => { setSegments(audio.segments || []) }, [audio.segments])

  // Active right-panel tab — AI 要点 is shown by default to match AlphaPai.
  const [rightTab, setRightTab] = useState<'summary' | 'chapters'>('summary')

  const [summary, setSummary] = useState<string>(audio.summary || '')
  const [summaryGeneratedAt, setSummaryGeneratedAt] = useState<string>(
    audio.summary_generated_at || '',
  )
  const [summaryLoading, setSummaryLoading] = useState(false)
  const [summaryError, setSummaryError] = useState<string>('')

  const [chapters, setChapters] = useState<AudioChapter[]>(audio.chapters || [])
  const [chaptersGeneratedAt, setChaptersGeneratedAt] = useState<string>(
    audio.chapters_generated_at || '',
  )
  const [chaptersLoading, setChaptersLoading] = useState(false)
  const [chaptersError, setChaptersError] = useState<string>('')

  // Editing state — at most one segment being edited at a time.
  const [editingIdx, setEditingIdx] = useState<number | null>(null)
  const [editDraft, setEditDraft] = useState<string>('')
  const [savingEdit, setSavingEdit] = useState(false)

  // Resync when the parent hands us a refreshed doc (e.g. after reparse).
  useEffect(() => {
    setSummary(audio.summary || '')
    setSummaryGeneratedAt(audio.summary_generated_at || '')
  }, [audio.summary, audio.summary_generated_at])
  useEffect(() => {
    setChapters(audio.chapters || [])
    setChaptersGeneratedAt(audio.chapters_generated_at || '')
  }, [audio.chapters, audio.chapters_generated_at])

  // ── Summary fetch ──
  const loadSummary = useCallback(async (force = false) => {
    setSummaryLoading(true)
    setSummaryError('')
    try {
      const res = await api.post<AudioSummaryPayload>(
        `/user-kb/documents/${documentId}/audio-summary`,
        null,
        { params: { force } },
      )
      setSummary(res.data.summary)
      setSummaryGeneratedAt(res.data.generated_at)
    } catch (err: any) {
      setSummaryError(
        err?.response?.data?.detail || err?.message || '生成失败',
      )
    } finally {
      setSummaryLoading(false)
    }
  }, [documentId])

  const loadChapters = useCallback(async (force = false) => {
    setChaptersLoading(true)
    setChaptersError('')
    try {
      const res = await api.post<AudioChaptersPayload>(
        `/user-kb/documents/${documentId}/audio-chapters`,
        null,
        { params: { force } },
      )
      setChapters(res.data.chapters)
      setChaptersGeneratedAt(res.data.generated_at)
    } catch (err: any) {
      setChaptersError(
        err?.response?.data?.detail || err?.message || '生成失败',
      )
    } finally {
      setChaptersLoading(false)
    }
  }, [documentId])

  // Auto-generate the AI summary on first mount if it's not cached —
  // AlphaPai's reference UI shows the summary immediately, without a
  // manual "生成 AI 要点" click. Only fire when we actually have content
  // to summarize (segments with text) and no existing summary.
  const autoTriedSummaryRef = useRef(false)
  useEffect(() => {
    if (autoTriedSummaryRef.current) return
    if (summary) return
    if (summaryLoading || summaryError) return
    if (!segments.length) return
    const hasText = segments.some((s) => (s.text || '').trim().length > 0)
    if (!hasText) return
    autoTriedSummaryRef.current = true
    loadSummary(false)
  }, [summary, summaryLoading, summaryError, segments, loadSummary])

  // Auto-generate chapter summary the first time the user opens that tab.
  const autoTriedChaptersRef = useRef(false)
  useEffect(() => {
    if (rightTab !== 'chapters') return
    if (autoTriedChaptersRef.current) return
    if (chapters.length > 0) return
    if (chaptersLoading || chaptersError) return
    if (!segments.length) return
    autoTriedChaptersRef.current = true
    loadChapters(false)
  }, [rightTab, chapters, chaptersLoading, chaptersError, segments, loadChapters])

  // ── Fetch the audio bytes as a blob URL (JWT-authed). ──
  //
  // We use ``fetch()`` instead of axios because the m4a payload can be
  // 30-500 MB — axios' 60s default timeout trips over slow WAN links and
  // surfaces as "Network Error" even though the server is still streaming.
  // fetch() has no default timeout; the browser manages the stream.
  useEffect(() => {
    let cancelled = false
    let revokeUrl: string | null = null
    const controller = new AbortController()
    ;(async () => {
      setAudioLoading(true)
      setAudioLoadError('')
      try {
        // Pull the JWT straight from the auth store — same source axios
        // uses in its request interceptor. We resolve it lazily here to
        // avoid pulling zustand into this component file.
        const authRaw = (
          localStorage.getItem('auth-storage') || ''
        )
        let token = ''
        try {
          token = JSON.parse(authRaw)?.state?.token || ''
        } catch { /* ignore — token fallback below */ }
        const resp = await fetch(
          `/api/user-kb/documents/${documentId}/file?inline=true`,
          {
            headers: token ? { Authorization: `Bearer ${token}` } : {},
            signal: controller.signal,
          },
        )
        if (!resp.ok) {
          throw new Error(`HTTP ${resp.status}`)
        }
        const blob = await resp.blob()
        if (cancelled) return
        const url = URL.createObjectURL(blob)
        revokeUrl = url
        setAudioUrl(url)
      } catch (err: any) {
        if (cancelled || err?.name === 'AbortError') return
        setAudioLoadError(err?.message || '加载失败')
      } finally {
        if (!cancelled) setAudioLoading(false)
      }
    })()
    return () => {
      cancelled = true
      controller.abort()
      if (revokeUrl) URL.revokeObjectURL(revokeUrl)
    }
  }, [documentId])

  // ── Media-element bindings. ──
  useEffect(() => {
    const el = audioRef.current
    if (!el) return
    const onPlay = () => setPlaying(true)
    const onPause = () => setPlaying(false)
    const onEnded = () => setPlaying(false)
    const onTimeUpdate = () => setCurrentTimeMs(Math.floor(el.currentTime * 1000))
    const onLoadedMetadata = () => {
      if (isFinite(el.duration) && el.duration > 0) setDurationSec(el.duration)
    }
    el.addEventListener('play', onPlay)
    el.addEventListener('pause', onPause)
    el.addEventListener('ended', onEnded)
    el.addEventListener('timeupdate', onTimeUpdate)
    el.addEventListener('loadedmetadata', onLoadedMetadata)
    el.playbackRate = rate
    return () => {
      el.removeEventListener('play', onPlay)
      el.removeEventListener('pause', onPause)
      el.removeEventListener('ended', onEnded)
      el.removeEventListener('timeupdate', onTimeUpdate)
      el.removeEventListener('loadedmetadata', onLoadedMetadata)
    }
  }, [audioUrl, rate])

  useEffect(() => {
    const el = audioRef.current
    if (el) el.playbackRate = rate
  }, [rate])

  const activeIdx = useMemo(
    () => indexOfSegmentAt(segments, currentTimeMs),
    [segments, currentTimeMs],
  )

  // Auto-scroll the active segment into view, but only gently — if the
  // user is scrolling through the transcript manually, don't fight them.
  const lastAutoScrolledRef = useRef<number>(-1)
  useEffect(() => {
    if (activeIdx < 0 || !playing) return
    if (activeIdx === lastAutoScrolledRef.current) return
    lastAutoScrolledRef.current = activeIdx
    const container = segmentListRef.current
    if (!container) return
    const target = container.querySelector<HTMLDivElement>(
      `[data-seg-idx="${activeIdx}"]`,
    )
    if (!target) return
    target.scrollIntoView({ behavior: 'smooth', block: 'nearest' })
  }, [activeIdx, playing])

  // ── Playback controls. ──
  const togglePlay = useCallback(() => {
    const el = audioRef.current
    if (!el) return
    if (el.paused) {
      el.play().catch((e) => antdMessage.error(`播放失败: ${e.message}`))
    } else {
      el.pause()
    }
  }, [])

  const seekToMs = useCallback((ms: number) => {
    const el = audioRef.current
    if (!el) return
    el.currentTime = Math.max(0, ms / 1000)
    setCurrentTimeMs(ms)
  }, [])

  const skipRelative = useCallback((deltaSec: number) => {
    const el = audioRef.current
    if (!el) return
    el.currentTime = Math.max(0, el.currentTime + deltaSec)
  }, [])

  const onSeekRatio = useCallback((ratio: number) => {
    if (!durationSec) return
    seekToMs(Math.floor(ratio * durationSec * 1000))
  }, [durationSec, seekToMs])

  // ── Segment editing ──
  const startEdit = useCallback((idx: number, text: string) => {
    if (!canEdit) return
    setEditingIdx(idx)
    setEditDraft(text)
  }, [canEdit])

  const cancelEdit = useCallback(() => {
    setEditingIdx(null)
    setEditDraft('')
  }, [])

  const saveEdit = useCallback(async () => {
    if (editingIdx == null) return
    const seg = segments[editingIdx]
    if (!seg) return
    const newText = editDraft.trim()
    if (newText === (seg.text || '').trim()) {
      cancelEdit()
      return
    }
    setSavingEdit(true)
    try {
      await api.patch<AudioMeta>(
        `/user-kb/documents/${documentId}/audio-segments/${seg.index}`,
        { text: newText },
      )
      const next = segments.map((s, i) =>
        i === editingIdx ? { ...s, text: newText } : s,
      )
      setSegments(next)
      onSegmentEdited?.(next)
      // Editing invalidates summary + chapters server-side. Clear locally
      // so the auto-generate effect fires again next time the user looks.
      setSummary('')
      setSummaryGeneratedAt('')
      setChapters([])
      setChaptersGeneratedAt('')
      autoTriedSummaryRef.current = false
      autoTriedChaptersRef.current = false
      setEditingIdx(null)
      setEditDraft('')
      antdMessage.success('已保存，AI 要点将重新生成')
    } catch (err: any) {
      antdMessage.error(
        `保存失败: ${err?.response?.data?.detail || err?.message || ''}`,
      )
    } finally {
      setSavingEdit(false)
    }
  }, [editingIdx, segments, editDraft, documentId, onSegmentEdited, cancelEdit])

  // ── Render ──
  const progress = durationSec ? currentTimeMs / 1000 / durationSec : 0

  const transcriptPanel = (
    <div
      ref={segmentListRef}
      style={{
        overflowY: 'auto', flex: 1, padding: '12px 16px 24px',
        background: '#fafbfc',
      }}
    >
      {segments.length === 0 ? (
        <Empty description="没有可用的转写片段" />
      ) : (
        segments.map((seg, idx) => {
          const active = idx === activeIdx
          const speaker = SPEAKER_PALETTE[idx % SPEAKER_PALETTE.length]
          const isEditing = editingIdx === idx
          return (
            <div
              key={seg.index}
              data-seg-idx={idx}
              style={{
                display: 'flex', gap: 10, marginBottom: 14,
                alignItems: 'flex-start',
              }}
            >
              <Avatar
                size={36}
                style={{
                  backgroundColor: speaker.bg, color: speaker.fg,
                  fontSize: 12, fontWeight: 700, flexShrink: 0,
                }}
              >
                {speaker.label.slice(-1)}
              </Avatar>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{
                  display: 'flex', alignItems: 'center', gap: 8,
                  marginBottom: 4,
                }}>
                  <Text style={{ fontSize: 12, color: '#475569', fontWeight: 600 }}>
                    {speaker.label}
                  </Text>
                  <span
                    role="button"
                    onClick={() => seekToMs(seg.start_ms)}
                    style={{
                      fontSize: 11, color: '#94a3b8', cursor: 'pointer',
                      fontVariantNumeric: 'tabular-nums',
                    }}
                    title="跳转到此时间点"
                  >
                    {formatTime(seg.start_ms / 1000)}
                  </span>
                  {canEdit && !isEditing && (
                    <Tooltip title="修正转写">
                      <Button
                        type="text" size="small"
                        icon={<EditOutlined style={{ fontSize: 12 }} />}
                        onClick={() => startEdit(idx, seg.text || '')}
                        style={{ color: '#94a3b8', padding: '0 4px' }}
                      />
                    </Tooltip>
                  )}
                </div>
                {isEditing ? (
                  <div>
                    <Input.TextArea
                      value={editDraft}
                      onChange={(e) => setEditDraft(e.target.value)}
                      autoSize={{ minRows: 2, maxRows: 10 }}
                      disabled={savingEdit}
                      style={{ fontSize: 14, lineHeight: 1.7 }}
                    />
                    <Space style={{ marginTop: 6 }} size={6}>
                      <Button
                        size="small" type="primary"
                        icon={<CheckOutlined />} loading={savingEdit}
                        onClick={saveEdit}
                        style={{ background: '#2ec98a', borderColor: '#2ec98a' }}
                      >
                        保存
                      </Button>
                      <Button
                        size="small" icon={<CloseOutlined />}
                        disabled={savingEdit} onClick={cancelEdit}
                      >
                        取消
                      </Button>
                    </Space>
                  </div>
                ) : (
                  <div
                    onClick={() => seekToMs(seg.start_ms)}
                    style={{
                      fontSize: 14, lineHeight: 1.8,
                      color: active ? '#0f172a' : '#1e293b',
                      background: active ? '#ecfdf5' : '#fff',
                      border: `1px solid ${active ? '#86efac' : '#e2e8f0'}`,
                      borderLeft: `3px solid ${active ? '#10b981' : '#e2e8f0'}`,
                      padding: '10px 14px', borderRadius: 6,
                      cursor: 'pointer', whiteSpace: 'pre-wrap',
                      wordBreak: 'break-word', transition: 'all 120ms',
                    }}
                  >
                    {seg.text || <Text type="secondary">（本段无语音）</Text>}
                  </div>
                )}
              </div>
            </div>
          )
        })
      )}
    </div>
  )

  const summaryPanel = (
    <div style={{ padding: 18, overflowY: 'auto', maxHeight: '100%' }}>
      {summaryLoading && !summary ? (
        <div>
          <Space size={8} style={{ marginBottom: 12 }}>
            <Spin size="small" />
            <Text type="secondary">AI 正在生成会议要点…</Text>
          </Space>
          <Skeleton active paragraph={{ rows: 8 }} />
        </div>
      ) : summaryError && !summary ? (
        <Alert
          type="error" showIcon
          message="AI 要点生成失败"
          description={summaryError}
          action={
            <Button size="small" onClick={() => loadSummary(true)}>
              重试
            </Button>
          }
        />
      ) : !summary ? (
        <Empty
          image={Empty.PRESENTED_IMAGE_SIMPLE}
          description="尚未生成 AI 要点"
        >
          <Button
            type="primary" icon={<BulbOutlined />}
            onClick={() => loadSummary(false)}
            style={{ background: '#2ec98a', borderColor: '#2ec98a' }}
          >
            生成 AI 要点
          </Button>
        </Empty>
      ) : (
        <>
          <div style={{
            display: 'flex', alignItems: 'center',
            justifyContent: 'space-between', marginBottom: 12,
          }}>
            <Space size={8}>
              {summaryGeneratedAt && (
                <Text type="secondary" style={{ fontSize: 11 }}>
                  生成于 {new Date(summaryGeneratedAt).toLocaleString('zh-CN')}
                </Text>
              )}
            </Space>
            <Tooltip title="基于转写全文重新生成">
              <Button
                size="small" type="text" icon={<ReloadOutlined />}
                loading={summaryLoading}
                onClick={() => loadSummary(true)}
              >
                重新生成
              </Button>
            </Tooltip>
          </div>
          <SummaryMarkdown text={summary} />
        </>
      )}
    </div>
  )

  const chaptersPanel = (
    <div style={{ padding: 18, overflowY: 'auto', maxHeight: '100%' }}>
      {chaptersLoading && chapters.length === 0 ? (
        <div>
          <Space size={8} style={{ marginBottom: 12 }}>
            <Spin size="small" />
            <Text type="secondary">AI 正在切分章节…</Text>
          </Space>
          <Skeleton active paragraph={{ rows: 6 }} />
        </div>
      ) : chaptersError && chapters.length === 0 ? (
        <Alert
          type="error" showIcon
          message="分段摘要生成失败"
          description={chaptersError}
          action={
            <Button size="small" onClick={() => loadChapters(true)}>
              重试
            </Button>
          }
        />
      ) : chapters.length === 0 ? (
        <Empty
          image={Empty.PRESENTED_IMAGE_SIMPLE}
          description="尚未生成分段摘要"
        >
          <Button
            type="primary" icon={<OrderedListOutlined />}
            onClick={() => loadChapters(false)}
            style={{ background: '#2ec98a', borderColor: '#2ec98a' }}
          >
            生成分段摘要
          </Button>
        </Empty>
      ) : (
        <>
          <div style={{
            display: 'flex', alignItems: 'center',
            justifyContent: 'space-between', marginBottom: 12,
          }}>
            {chaptersGeneratedAt && (
              <Text type="secondary" style={{ fontSize: 11 }}>
                生成于 {new Date(chaptersGeneratedAt).toLocaleString('zh-CN')}
              </Text>
            )}
            <Tooltip title="重新切分章节">
              <Button
                size="small" type="text" icon={<ReloadOutlined />}
                loading={chaptersLoading}
                onClick={() => loadChapters(true)}
              >
                重新生成
              </Button>
            </Tooltip>
          </div>
          {chapters.map((ch, i) => {
            const inThisChapter =
              currentTimeMs >= ch.start_ms
              && (i === chapters.length - 1 || currentTimeMs < chapters[i + 1].start_ms)
            return (
              <div
                key={i}
                onClick={() => seekToMs(ch.start_ms)}
                style={{
                  padding: '10px 12px', marginBottom: 10,
                  borderRadius: 8, cursor: 'pointer',
                  background: inThisChapter ? '#ecfdf5' : '#f8fafc',
                  border: `1px solid ${inThisChapter ? '#86efac' : '#e2e8f0'}`,
                  transition: 'all 120ms',
                }}
              >
                <div style={{
                  display: 'flex', alignItems: 'center', gap: 8,
                  marginBottom: 6,
                }}>
                  <span style={{
                    width: 22, height: 22, borderRadius: '50%',
                    background: '#2ec98a', color: '#fff',
                    display: 'inline-flex', alignItems: 'center',
                    justifyContent: 'center', fontSize: 11, fontWeight: 600,
                    flexShrink: 0,
                  }}>
                    {i + 1}
                  </span>
                  <Text strong style={{ fontSize: 14, color: '#0f172a' }}>
                    {ch.title}
                  </Text>
                  <Text type="secondary" style={{
                    fontSize: 11, marginLeft: 'auto',
                    fontVariantNumeric: 'tabular-nums',
                  }}>
                    {formatTime(ch.start_ms / 1000)}
                  </Text>
                </div>
                <ul style={{
                  margin: 0, paddingLeft: 30, color: '#334155', fontSize: 13,
                }}>
                  {(ch.bullets || []).map((b, j) => (
                    <li key={j} style={{ lineHeight: 1.75, marginBottom: 2 }}>
                      {b}
                    </li>
                  ))}
                </ul>
              </div>
            )
          })}
        </>
      )}
    </div>
  )

  return (
    <div style={{
      display: 'flex', flexDirection: 'column',
      height: '100%', minHeight: 520,
      border: '1px solid #e2e8f0', borderRadius: 8, overflow: 'hidden',
      background: '#fff',
    }}>
      {/* Header strip */}
      <div style={{
        padding: '10px 16px', borderBottom: '1px solid #e2e8f0',
        background: '#f8fafc', display: 'flex',
        justifyContent: 'space-between', alignItems: 'center',
      }}>
        <Space size={8}>
          <SoundOutlined style={{ color: '#2ec98a', fontSize: 16 }} />
          <Text strong style={{ fontSize: 14 }}>{title || filename}</Text>
          <Tag color="green" style={{ margin: 0 }}>PAI</Tag>
          {audio.language && (
            <Tag color="blue" style={{ margin: 0 }}>{audio.language}</Tag>
          )}
          <Tag color="default" style={{ margin: 0 }}>
            {segments.length} 段
          </Tag>
          {audio.duration_seconds ? (
            <Tag color="default" style={{ margin: 0 }}>
              {formatTime(audio.duration_seconds)}
            </Tag>
          ) : null}
        </Space>
        {onDownload && (
          <Button
            size="small" icon={<DownloadOutlined />} onClick={onDownload}
          >
            下载原文件
          </Button>
        )}
      </div>

      {/* Two-column body: transcript + summary tabs */}
      <div style={{ flex: 1, display: 'flex', minHeight: 0 }}>
        <div style={{
          flex: '1 1 56%', display: 'flex', flexDirection: 'column',
          borderRight: '1px solid #e2e8f0', minWidth: 0,
        }}>
          <div style={{
            padding: '8px 16px', borderBottom: '1px solid #f1f5f9',
            background: '#fff',
          }}>
            <Space size={6}>
              <FileTextOutlined style={{ color: '#64748b' }} />
              <Text strong style={{ fontSize: 13 }}>音频详情</Text>
              <Tag color="green" style={{ margin: 0, fontSize: 10 }}>
                PAI
              </Tag>
            </Space>
          </div>
          {transcriptPanel}
        </div>
        <div style={{
          flex: '1 1 44%', display: 'flex', flexDirection: 'column',
          minWidth: 0, background: '#fff',
        }}>
          <Tabs
            activeKey={rightTab}
            onChange={(k) => setRightTab(k as 'summary' | 'chapters')}
            size="small"
            tabBarStyle={{
              margin: 0, padding: '0 12px', background: '#f8fafc',
              borderBottom: '1px solid #e2e8f0',
            }}
            items={[
              {
                key: 'summary',
                label: <span><BulbOutlined /> AI 要点</span>,
                children: summaryPanel,
              },
              {
                key: 'chapters',
                label: <span><OrderedListOutlined /> 分段摘要</span>,
                children: chaptersPanel,
              },
            ]}
            style={{ flex: 1, display: 'flex', flexDirection: 'column' }}
          />
        </div>
      </div>

      {/* Bottom audio player bar */}
      <div style={{
        padding: '10px 16px', borderTop: '1px solid #e2e8f0',
        background: '#fff',
      }}>
        {audioLoading ? (
          <Spin size="small" />
        ) : audioLoadError ? (
          <Alert
            type="warning" showIcon
            message={`音频加载失败：${audioLoadError}`}
          />
        ) : (
          <>
            {audioUrl && (
              <audio
                ref={audioRef}
                src={audioUrl}
                preload="metadata"
                style={{ display: 'none' }}
              />
            )}
            <div style={{
              display: 'flex', alignItems: 'center', gap: 12,
            }}>
              <Tooltip title="后退 10 秒">
                <Button
                  type="text" shape="circle" icon={<StepBackwardOutlined />}
                  onClick={() => skipRelative(-10)}
                />
              </Tooltip>
              <Button
                type="primary" shape="circle"
                size="large"
                icon={playing
                  ? <PauseCircleFilled style={{ fontSize: 24 }} />
                  : <PlayCircleFilled style={{ fontSize: 24 }} />}
                onClick={togglePlay}
                style={{
                  width: 44, height: 44, padding: 0,
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  background: '#2ec98a', borderColor: '#2ec98a',
                }}
              />
              <Tooltip title="前进 10 秒">
                <Button
                  type="text" shape="circle" icon={<StepForwardOutlined />}
                  onClick={() => skipRelative(10)}
                />
              </Tooltip>
              <Text style={{
                fontSize: 12, color: '#64748b', fontVariantNumeric: 'tabular-nums',
                minWidth: 48, textAlign: 'right',
              }}>
                {formatTime(currentTimeMs / 1000)}
              </Text>
              <Waveform
                progress={progress}
                onSeek={onSeekRatio}
                duration={durationSec}
              />
              <Text style={{
                fontSize: 12, color: '#64748b', fontVariantNumeric: 'tabular-nums',
                minWidth: 48,
              }}>
                {formatTime(durationSec)}
              </Text>
              <Space size={2}>
                {PLAYBACK_RATES.map((r) => (
                  <Button
                    key={r}
                    size="small"
                    type={rate === r ? 'primary' : 'text'}
                    onClick={() => setRate(r)}
                    style={{
                      minWidth: 40, padding: '0 6px', fontSize: 11,
                      ...(rate === r ? {
                        background: '#2ec98a', borderColor: '#2ec98a',
                      } : {}),
                    }}
                  >
                    {r}×
                  </Button>
                ))}
              </Space>
            </div>
          </>
        )}
      </div>
    </div>
  )
}

// Re-export the extension filter so pages can decide whether a document
// is audio without duplicating the list.
export const AUDIO_EXT_SET: ReadonlySet<string> = new Set([
  'mp3', 'wav', 'm4a', 'flac', 'ogg', 'opus', 'webm', 'aac',
])

export function isAudioDoc(ext: string): boolean {
  return AUDIO_EXT_SET.has((ext || '').toLowerCase())
}
