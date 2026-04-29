import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useParams, useSearchParams, useNavigate } from 'react-router-dom'
import {
  Tag, Typography, Empty, Skeleton, Button, Tooltip, Space,
  Input, message, Drawer, Spin, Alert, Segmented,
} from 'antd'
import {
  FilePdfOutlined, LinkOutlined, ReloadOutlined, ArrowLeftOutlined,
  SearchOutlined, ThunderboltOutlined, BookOutlined, MessageOutlined,
  AudioOutlined, SolutionOutlined, DownloadOutlined, CloseOutlined,
} from '@ant-design/icons'
import api from '../services/api'
import MarkdownRenderer from '../components/MarkdownRenderer'

const { Text, Title } = Typography

// ── Types ────────────────────────────────────────────────

type Category = 'research' | 'commentary' | 'minutes' | 'interview' | 'breaking' | 'all'

interface HubItem {
  id: string
  source: string
  source_label: string
  collection: string
  category: Exclude<Category, 'all'>
  category_label: string
  title: string
  title_zh?: string | null
  release_time: string | null
  release_time_ms: number | null
  url: string | null
  pdf_url: string | null
  preview: string
  preview_zh?: string | null
  organization: string
  sentiment?: string | null
  impact_magnitude?: string | null
  tickers: string[]
}

interface HubResponse {
  canonical_id: string
  stock_name: string | null
  by_category: Record<string, number>
  by_source: Record<string, number>
  total: number
  items: HubItem[]
  next_before_ms: number | null
}

interface DocSection { label: string; markdown: string; markdown_zh?: string | null }

interface DocPdfAttachment {
  index: number
  name: string
  size_bytes: number
  url: string
}

interface LocalAiSummary {
  tldr: string
  bullets: string[]
  model: string
  generated_at: string | null
}

interface DocDetailResponse {
  source: string
  source_label: string
  collection: string
  category: string
  category_label: string
  id: string
  title: string
  title_zh?: string | null
  release_time: string | null
  release_time_ms: number | null
  organization: string
  url: string | null
  pdf_url: string | null
  pdf_urls: DocPdfAttachment[]
  tickers: string[]
  sections: DocSection[]
  local_ai_summary?: LocalAiSummary | null
  sentiment?: string | null
  impact_magnitude?: string | null
}

// ── Constants ────────────────────────────────────────────

const CATEGORY_META: Record<
  Exclude<Category, 'all'>,
  { label: string; color: string; accent: string; icon: React.ReactNode; shortcut: string }
> = {
  research:   { label: '研报',     color: '#2563eb', accent: '#dbeafe', icon: <BookOutlined />,      shortcut: '2' },
  commentary: { label: '点评',     color: '#7c3aed', accent: '#ede9fe', icon: <MessageOutlined />,   shortcut: '3' },
  minutes:    { label: '会议纪要', color: '#0891b2', accent: '#cffafe', icon: <SolutionOutlined />,  shortcut: '4' },
  interview:  { label: '专家访谈', color: '#ea580c', accent: '#ffedd5', icon: <AudioOutlined />,     shortcut: '5' },
  breaking:   { label: '突发新闻', color: '#dc2626', accent: '#fee2e2', icon: <ThunderboltOutlined />, shortcut: '6' },
}

const SOURCE_LABELS: Record<string, string> = {
  alphapai:    'AlphaPai',
  jinmen:      '进门',
  gangtise:    '岗底斯',
  funda:       'Funda',
  alphaengine: 'AlphaEngine',
  acecamp:     '本营',
  meritco:     '久谦中台',
  thirdbridge: '高临',
  newsfeed:    '资讯中心',
}

const SOURCE_COLORS: Record<string, string> = {
  alphapai:    '#1677ff',
  jinmen:      '#fa8c16',
  gangtise:    '#722ed1',
  funda:       '#13c2c2',
  alphaengine: '#52c41a',
  acecamp:     '#eb2f96',
  meritco:     '#f5222d',
  thirdbridge: '#faad14',
  newsfeed:    '#8c8c8c',
}

const MARKET_MAP: Record<string, string> = {
  US: '美股', HK: '港股', SH: '沪A', SZ: '深A', BJ: '北交',
  JP: '日股', KS: '韩股', AU: '澳股', CA: '加股', GB: '英股',
  DE: '德股', FR: '法股', CH: '瑞股', NL: '荷股', SE: '瑞典',
  NO: '挪威', IT: '意股', AT: '奥股', NZ: '新西兰', HE: '芬兰', TW: '台股',
}

// ── Helpers ──────────────────────────────────────────────

function humanTime(iso: string | null): string {
  if (!iso) return ''
  // release_time is "YYYY-MM-DD HH:MM" or "YYYY-MM-DD" — just use as-is
  const d = new Date(iso.replace(' ', 'T'))
  if (isNaN(d.getTime())) return iso
  const now = new Date()
  const diff = (now.getTime() - d.getTime()) / 1000
  if (diff < 60) return '刚刚'
  if (diff < 3600) return `${Math.floor(diff / 60)}分钟前`
  if (diff < 86400) return `${Math.floor(diff / 3600)}小时前`
  if (diff < 86400 * 3) return `${Math.floor(diff / 86400)}天前`
  return iso.slice(0, 16)
}

function groupByDay(items: HubItem[]): Array<{ day: string; items: HubItem[] }> {
  const groups: Record<string, HubItem[]> = {}
  for (const it of items) {
    const day = (it.release_time || '').slice(0, 10) || '未知日期'
    if (!groups[day]) groups[day] = []
    groups[day].push(it)
  }
  return Object.entries(groups)
    .sort(([a], [b]) => (a < b ? 1 : -1))
    .map(([day, items]) => ({ day, items }))
}

function dayLabel(ymd: string): string {
  if (ymd === '未知日期') return ymd
  const today = new Date().toISOString().slice(0, 10)
  if (ymd === today) return `今天 · ${ymd}`
  const d = new Date(ymd)
  const diff = Math.floor((new Date(today).getTime() - d.getTime()) / (1000 * 86400))
  if (diff === 1) return `昨天 · ${ymd}`
  if (diff < 7 && diff > 0) return `${diff}天前 · ${ymd}`
  return ymd
}

// ── Component ────────────────────────────────────────────

export default function StockHub() {
  const { canonicalId = '' } = useParams()
  const [searchParams, setSearchParams] = useSearchParams()
  const navigate = useNavigate()
  const stockNameFromUrl = searchParams.get('name') || null

  const [data, setData] = useState<HubResponse | null>(null)
  const [items, setItems] = useState<HubItem[]>([])
  const [loading, setLoading] = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const [activeCategory, setActiveCategory] = useState<Category>(
    (searchParams.get('cat') as Category) || 'all',
  )
  const [search, setSearch] = useState('')
  const [hoverIndex, setHoverIndex] = useState<number>(-1)
  const cardRefs = useRef<Array<HTMLDivElement | null>>([])

  // Shared 中文/原文 lang preference for both list cards (preview) and the
  // detail drawer. Persisted to localStorage so the choice survives reloads.
  const LANG_PREF_KEY = 'stockhub.detailLang'
  const [lang, setLang] = useState<'zh' | 'orig'>(() => {
    try { return (localStorage.getItem(LANG_PREF_KEY) as 'zh' | 'orig') || 'zh' } catch { return 'zh' }
  })
  useEffect(() => {
    try { localStorage.setItem(LANG_PREF_KEY, lang) } catch { /* noop */ }
  }, [lang])
  const anyTranslatedInList = items.some((it) =>
    ((it.preview_zh || '').trim().length > 0) ||
    ((it.title_zh || '').trim().length > 0),
  )

  // Detail drawer state — fetched lazily on card click.
  // The list endpoint returns only a 320-char preview; the drawer shows
  // everything the backend has (markdown sections + embedded PDF).
  const [detailItem, setDetailItem] = useState<HubItem | null>(null)
  const [detail, setDetail] = useState<DocDetailResponse | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState<string | null>(null)
  const [pdfView, setPdfView] = useState<'sections' | 'pdf'>('sections')
  const [pdfBlobUrl, setPdfBlobUrl] = useState<string | null>(null)
  const [pdfLoading, setPdfLoading] = useState(false)
  const [pdfError, setPdfError] = useState<string | null>(null)
  const [pdfProgress, setPdfProgress] = useState<{ loaded: number; total: number } | null>(null)
  const [activePdfIdx, setActivePdfIdx] = useState(0)  // for meritco multi-attachment
  // Track the in-flight PDF URL so duplicate fetches (StrictMode double-effect,
  // user re-clicking) collapse instead of stacking on the slow GridFS path.
  const inflightPdfRef = useRef<string | null>(null)

  const load = useCallback(
    async (opts?: { category?: Category; append?: boolean; beforeMs?: number }) => {
      const cat = opts?.category ?? activeCategory
      const params: Record<string, string | number> = { limit: 80 }
      if (cat && cat !== 'all') params.category = cat
      if (opts?.beforeMs) params.before_ms = opts.beforeMs
      if (stockNameFromUrl) params.stock_name = stockNameFromUrl
      try {
        if (opts?.append) setLoadingMore(true)
        else setLoading(true)
        const { data: resp } = await api.get<HubResponse>(`/stock-hub/${canonicalId}`, { params })
        if (opts?.append) {
          setItems((prev) => [...prev, ...resp.items])
          setData((prev) => (prev ? { ...prev, next_before_ms: resp.next_before_ms } : resp))
        } else {
          setData(resp)
          setItems(resp.items)
        }
      } catch (e: any) {
        message.error(e?.response?.data?.detail || '加载失败')
      } finally {
        setLoading(false)
        setLoadingMore(false)
      }
    },
    [canonicalId, activeCategory, stockNameFromUrl],
  )

  useEffect(() => {
    load()
  }, [canonicalId])

  // ── Detail drawer ────────────────────────────────────────

  const clearPdf = useCallback(() => {
    setPdfError(null)
    setPdfProgress(null)
    inflightPdfRef.current = null
    setPdfBlobUrl((prev) => {
      if (prev) URL.revokeObjectURL(prev)
      return null
    })
  }, [])

  const openDetail = useCallback(async (item: HubItem) => {
    setDetailItem(item)
    setDetail(null)
    setDetailError(null)
    setDetailLoading(true)
    setPdfView('sections')
    setActivePdfIdx(0)
    clearPdf()
    try {
      const path =
        item.source === 'newsfeed'
          ? `/stock-hub/newsfeed/${encodeURIComponent(item.id)}`
          : `/stock-hub/doc/${encodeURIComponent(item.source)}/${encodeURIComponent(item.collection)}/${encodeURIComponent(item.id)}`
      const { data: resp } = await api.get<DocDetailResponse>(path, { timeout: 30000 })
      setDetail(resp)
    } catch (e: any) {
      const msg = e?.response?.data?.detail || e?.message || '加载详情失败'
      setDetailError(typeof msg === 'string' ? msg : JSON.stringify(msg))
    } finally {
      setDetailLoading(false)
    }
  }, [clearPdf])

  const closeDetail = useCallback(() => {
    setDetailItem(null)
    setDetail(null)
    setDetailError(null)
    clearPdf()
  }, [clearPdf])

  // PDF fetches use the browser's native fetch() rather than axios. axios
  // wraps XMLHttpRequest and surfaces a generic "Network Error" when a
  // streamed blob response stalls or hits transient WAN flakiness, even if
  // the server is still happily writing bytes. fetch() lets the browser
  // manage the stream end-to-end and reports a real status / specific
  // failure cause when something actually breaks. The same workaround is
  // used by AudioTranscriptViewer for the same reason.
  const fetchPdfBlob = useCallback(
    async (path: string, onProgress?: (loaded: number, total: number) => void): Promise<Blob> => {
      // Pull JWT from the auth-storage entry that the zustand store
      // persists. Resolved lazily here to avoid hard-coupling this hook.
      let token = ''
      try {
        const raw = localStorage.getItem('auth-storage') || ''
        if (raw) token = JSON.parse(raw)?.state?.token || ''
      } catch { /* fall through with empty token; backend will 401 */ }

      const url = path.startsWith('/') ? path : `/api/${path.replace(/^\/?api\/?/, '')}`
      const resp = await fetch(url, {
        method: 'GET',
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        credentials: 'same-origin',
      })
      if (!resp.ok) {
        // Try to surface a structured detail from FastAPI (JSON) before
        // falling back to a bare HTTP status.
        let detail = ''
        try {
          const txt = await resp.text()
          try { detail = JSON.parse(txt)?.detail || '' } catch { detail = txt }
        } catch { /* ignore */ }
        throw new Error(detail || `HTTP ${resp.status}`)
      }
      const total = Number(resp.headers.get('content-length') || 0)
      // No progress callback OR no streaming reader → fall through to blob().
      if (!onProgress || !resp.body) {
        return await resp.blob()
      }
      const reader = resp.body.getReader()
      const chunks: BlobPart[] = []
      let received = 0
      // Streaming read for progress reporting. The browser keeps the
      // socket alive even when the server is slow, so this is the
      // reliable path for large GridFS responses.
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        if (value) {
          // Copy into a fresh ArrayBuffer so the Blob constructor's strict
          // BlobPart typing accepts it under TS5 strict mode (Uint8Array
          // can be backed by SharedArrayBuffer, which Blob rejects).
          const copy = new Uint8Array(value.byteLength)
          copy.set(value)
          chunks.push(copy.buffer)
          received += value.byteLength
          onProgress(received, total)
        }
      }
      return new Blob(chunks, { type: 'application/pdf' })
    },
    [],
  )

  const loadPdf = useCallback(async (pdfUrl: string) => {
    // Collapse duplicate concurrent loads for the same URL. The PDF endpoint
    // is on the slow Mongo GridFS path (~50-250 s for ~10 MB) so stacking
    // requests just compounds the wait.
    if (inflightPdfRef.current === pdfUrl) return
    inflightPdfRef.current = pdfUrl
    setPdfLoading(true)
    setPdfError(null)
    setPdfProgress(null)
    try {
      const blob = await fetchPdfBlob(pdfUrl, (loaded, total) => {
        setPdfProgress({ loaded, total })
      })
      // Ignore stale responses if the user moved on.
      if (inflightPdfRef.current !== pdfUrl) return
      const url = URL.createObjectURL(blob)
      setPdfBlobUrl((prev) => {
        if (prev) URL.revokeObjectURL(prev)
        return url
      })
    } catch (err: any) {
      if (inflightPdfRef.current !== pdfUrl) return
      setPdfError(err?.message || '加载 PDF 失败')
    } finally {
      if (inflightPdfRef.current === pdfUrl) {
        inflightPdfRef.current = null
        setPdfLoading(false)
        setPdfProgress(null)
      }
    }
  }, [fetchPdfBlob])

  const downloadPdf = useCallback(async (pdfUrl: string, filename: string) => {
    const hide = message.loading('正在下载 PDF…', 0)
    try {
      // Append download=1 to signal Content-Disposition: attachment on platforms
      // that honor it (alphapai/gangtise/jinmen/alphaengine). Meritco's /pdf
      // already forces download via its route.
      const sep = pdfUrl.includes('?') ? '&' : '?'
      const blob = await fetchPdfBlob(`${pdfUrl}${sep}download=1`)
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `${filename.replace(/[\\/:*?"<>|\r\n\t]/g, '_').slice(0, 120)}.pdf`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      setTimeout(() => URL.revokeObjectURL(url), 1000)
      hide()
      message.success('下载完成')
    } catch (err: any) {
      hide()
      message.error(`下载失败: ${err?.message || '未知错误'}`)
    }
  }, [fetchPdfBlob])

  // Revoke blob URL on unmount
  useEffect(() => {
    return () => {
      if (pdfBlobUrl) URL.revokeObjectURL(pdfBlobUrl)
    }

  }, [])

  // Keyboard shortcuts: 1-6 for filters, j/k to navigate
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.target && (e.target as HTMLElement).tagName === 'INPUT') return
      if (e.target && (e.target as HTMLElement).tagName === 'TEXTAREA') return
      const map: Record<string, Category> = {
        '1': 'all', '2': 'research', '3': 'commentary',
        '4': 'minutes', '5': 'interview', '6': 'breaking',
      }
      if (map[e.key]) {
        e.preventDefault()
        switchCategory(map[e.key])
      } else if (e.key === 'j') {
        const next = Math.min(hoverIndex + 1, items.length - 1)
        setHoverIndex(next)
        cardRefs.current[next]?.scrollIntoView({ behavior: 'smooth', block: 'center' })
      } else if (e.key === 'k') {
        const next = Math.max(hoverIndex - 1, 0)
        setHoverIndex(next)
        cardRefs.current[next]?.scrollIntoView({ behavior: 'smooth', block: 'center' })
      } else if (e.key === 'Enter' && hoverIndex >= 0) {
        const it = items[hoverIndex]
        if (it) openDetail(it)
      } else if (e.key === 'Escape' && detailItem) {
        closeDetail()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [items, hoverIndex, detailItem, openDetail, closeDetail])

  function switchCategory(cat: Category) {
    setActiveCategory(cat)
    setHoverIndex(-1)
    const p = new URLSearchParams(searchParams)
    if (cat === 'all') p.delete('cat')
    else p.set('cat', cat)
    setSearchParams(p)
    load({ category: cat })
  }

  const marketCode = canonicalId.split('.')[1] || ''
  const codeOnly = canonicalId.split('.')[0] || canonicalId
  const stockName = stockNameFromUrl || data?.stock_name || ''
  const marketLabel = MARKET_MAP[marketCode] || marketCode

  const filteredItems = useMemo(() => {
    if (!search) return items
    const q = search.toLowerCase()
    return items.filter(
      (i) =>
        i.title.toLowerCase().includes(q) ||
        i.preview.toLowerCase().includes(q) ||
        i.organization.toLowerCase().includes(q),
    )
  }, [items, search])

  const grouped = useMemo(() => groupByDay(filteredItems), [filteredItems])
  const byCat = data?.by_category || {}
  // "全部" must aggregate across all categories regardless of active filter.
  // The backend collapses `data.total` to the filtered category's count, so
  // recompute from by_category which the backend always populates fully.
  const allTotal = Object.values(byCat).reduce<number>((s, x) => s + (x || 0), 0)
  const filterTotal = data?.total ?? allTotal  // active filter's total (for load-more progress)

  const categoryButton = (c: Category, count: number, label: string, meta?: typeof CATEGORY_META[keyof typeof CATEGORY_META]) => {
    const active = activeCategory === c
    return (
      <button
        key={c}
        onClick={() => switchCategory(c)}
        style={{
          display: 'inline-flex', alignItems: 'center', gap: 6,
          padding: '6px 14px', borderRadius: 999,
          border: '1px solid',
          borderColor: active ? (meta?.color || '#2563eb') : '#e2e8f0',
          background: active ? (meta?.color || '#2563eb') : '#fff',
          color: active ? '#fff' : '#334155',
          cursor: 'pointer', fontSize: 13, fontWeight: 500,
          transition: 'all .15s ease',
          whiteSpace: 'nowrap',
        }}
      >
        {meta?.icon}
        <span>{label}</span>
        <span
          style={{
            background: active ? 'rgba(255,255,255,.25)' : '#f1f5f9',
            color: active ? '#fff' : '#64748b',
            padding: '0 8px', borderRadius: 10,
            fontSize: 11, fontWeight: 600,
          }}
        >
          {count.toLocaleString()}
        </span>
      </button>
    )
  }

  // ── Render ────────────────────────────────────────────

  return (
    <div style={{ maxWidth: 1280, margin: '0 auto', padding: 16 }}>
      {/* Header */}
      <div
        style={{
          display: 'flex', alignItems: 'center', gap: 16,
          padding: '12px 0', marginBottom: 8,
        }}
      >
        <Button
          icon={<ArrowLeftOutlined />}
          onClick={() => navigate('/')}
          type="text"
          style={{ color: '#64748b' }}
        >
          返回
        </Button>
        <div style={{ flex: 1 }}>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 12 }}>
            <span style={{ fontSize: 28, fontWeight: 700, letterSpacing: -0.5, color: '#0f172a' }}>
              {codeOnly}
            </span>
            {marketLabel && (
              <Tag
                color={marketCode === 'US' ? 'blue' : marketCode === 'HK' ? 'purple' : 'red'}
                style={{ fontSize: 12 }}
              >
                {marketLabel}
              </Tag>
            )}
            {stockName && (
              <Title level={4} style={{ margin: 0, color: '#334155', fontWeight: 500 }}>
                {stockName}
              </Title>
            )}
          </div>
          <Text type="secondary" style={{ fontSize: 12 }}>
            汇聚 {allTotal.toLocaleString()} 条资料 · 研报 / 点评 / 会议纪要 / 专家访谈 / 突发新闻
          </Text>
        </div>
        <Space>
          <Input
            allowClear
            placeholder="在结果内搜索..."
            prefix={<SearchOutlined />}
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ width: 220 }}
          />
          <Tooltip title="刷新">
            <Button icon={<ReloadOutlined />} onClick={() => load()} />
          </Tooltip>
        </Space>
      </div>

      {/* Filter chips */}
      <div
        style={{
          position: 'sticky', top: 0, zIndex: 10,
          background: 'rgba(241,245,249,.92)', backdropFilter: 'blur(8px)',
          padding: '10px 0', marginBottom: 12, borderBottom: '1px solid #e2e8f0',
          display: 'flex', gap: 8, flexWrap: 'wrap',
        }}
      >
        {categoryButton('all', allTotal, '全部')}
        {(['research', 'commentary', 'minutes', 'interview', 'breaking'] as const).map((c) =>
          categoryButton(c, byCat[c] || 0, CATEGORY_META[c].label, CATEGORY_META[c]),
        )}
        {anyTranslatedInList && (
          <div style={{ marginLeft: 'auto' }}>
            <Segmented
              size="small"
              value={lang}
              onChange={(v) => setLang(v as 'zh' | 'orig')}
              options={[
                { label: '中文', value: 'zh' },
                { label: '原文', value: 'orig' },
              ]}
            />
          </div>
        )}
      </div>

      {/* Body */}
      {loading ? (
        <div>
          {Array.from({ length: 6 }).map((_, i) => (
            <div
              key={i}
              style={{
                padding: 12, marginBottom: 8, borderRadius: 10,
                background: '#fff', border: '1px solid #f1f5f9',
              }}
            >
              <Skeleton active paragraph={{ rows: 2 }} />
            </div>
          ))}
        </div>
      ) : filteredItems.length === 0 ? (
        <Empty
          style={{ padding: '60px 0' }}
          description={
            search
              ? `搜索 "${search}" 无匹配结果`
              : `该股票暂无${activeCategory === 'all' ? '' : CATEGORY_META[activeCategory as Exclude<Category, 'all'>]?.label || ''}类内容`
          }
        />
      ) : (
        <div>
          {grouped.map((g) => (
            <div key={g.day} style={{ marginBottom: 20 }}>
              <div
                style={{
                  position: 'sticky', top: 52, zIndex: 5,
                  background: '#f1f5f9', padding: '6px 10px', borderRadius: 6,
                  fontSize: 12, color: '#64748b', fontWeight: 600,
                  marginBottom: 8, letterSpacing: 0.3,
                }}
              >
                {dayLabel(g.day)}  ·  {g.items.length} 条
              </div>
              {g.items.map((it) => {
                const idx = items.indexOf(it)
                const meta = CATEGORY_META[it.category]
                const isHover = idx === hoverIndex
                return (
                  <div
                    key={`${it.source}-${it.collection}-${it.id}`}
                    ref={(el) => { cardRefs.current[idx] = el }}
                    onMouseEnter={() => setHoverIndex(idx)}
                    style={{
                      display: 'flex', gap: 0, marginBottom: 8,
                      background: '#fff',
                      border: '1px solid',
                      borderColor: isHover ? meta?.color || '#e2e8f0' : '#f1f5f9',
                      borderRadius: 10, overflow: 'hidden',
                      transition: 'border-color .15s ease, box-shadow .15s ease',
                      boxShadow: isHover ? `0 2px 12px ${meta?.accent || '#f1f5f9'}` : 'none',
                    }}
                  >
                    <div
                      style={{
                        width: 3, background: meta?.color || '#e2e8f0',
                        flexShrink: 0,
                      }}
                    />
                    <div style={{ flex: 1, padding: '12px 14px', minWidth: 0 }}>
                      {/* Meta row */}
                      <div
                        style={{
                          display: 'flex', alignItems: 'center', gap: 8,
                          fontSize: 11, color: '#64748b', marginBottom: 6,
                          flexWrap: 'wrap',
                        }}
                      >
                        <span
                          style={{
                            display: 'inline-flex', alignItems: 'center', gap: 4,
                            padding: '2px 7px', borderRadius: 4,
                            background: meta?.accent, color: meta?.color,
                            fontWeight: 600,
                          }}
                        >
                          {meta?.icon} {it.category_label}
                        </span>
                        <Tag
                          color={SOURCE_COLORS[it.source] || 'default'}
                          style={{ margin: 0, fontSize: 10, lineHeight: '18px' }}
                        >
                          {it.source_label}
                        </Tag>
                        {it.organization && (
                          <span style={{ color: '#94a3b8' }}>
                            · {it.organization}
                          </span>
                        )}
                        <span style={{ marginLeft: 'auto', color: '#94a3b8' }}>
                          {humanTime(it.release_time)}
                        </span>
                      </div>

                      {/* Title — clickable (opens detail drawer with full content) */}
                      <div
                        onClick={() => openDetail(it)}
                        style={{
                          fontSize: 15, fontWeight: 600, color: '#0f172a',
                          marginBottom: 6, cursor: 'pointer',
                          lineHeight: 1.45,
                        }}
                      >
                        {(() => {
                          const zh = (it.title_zh || '').trim()
                          return lang === 'zh' && zh.length > 0 ? zh : it.title
                        })()}
                        {it.sentiment && it.sentiment !== 'neutral' && (
                          <Tag
                            color={it.sentiment === 'bullish' ? 'green' : it.sentiment === 'bearish' ? 'red' : 'default'}
                            style={{ marginLeft: 8, fontSize: 10, lineHeight: '16px' }}
                          >
                            {it.sentiment === 'bullish' ? '利好' : it.sentiment === 'bearish' ? '利空' : it.sentiment}
                          </Tag>
                        )}
                        {it.impact_magnitude === 'critical' && (
                          <Tag color="red" style={{ marginLeft: 4, fontSize: 10, lineHeight: '16px' }}>
                            重大
                          </Tag>
                        )}
                      </div>

                      {/* Preview — show translation when available and lang=zh */}
                      {(() => {
                        const zh = (it.preview_zh || '').trim()
                        const showZh = lang === 'zh' && zh.length > 0
                        const body = showZh ? zh : it.preview
                        if (!body) return null
                        return (
                          <div
                            style={{
                              fontSize: 13, color: '#475569', lineHeight: 1.55,
                              display: '-webkit-box',
                              WebkitLineClamp: 2, WebkitBoxOrient: 'vertical',
                              overflow: 'hidden',
                            }}
                          >
                            {body}
                          </div>
                        )
                      })()}

                      {/* Actions */}
                      <div style={{ marginTop: 8, display: 'flex', gap: 8 }}>
                        <Button
                          size="small"
                          type="primary"
                          ghost
                          onClick={(e) => {
                            e.stopPropagation()
                            openDetail(it)
                          }}
                        >
                          查看详情
                        </Button>
                        {it.pdf_url && (
                          <Button
                            size="small"
                            icon={<FilePdfOutlined />}
                            onClick={(e) => {
                              e.stopPropagation()
                              openDetail(it)
                              // Pre-select PDF tab for the drawer
                              setPdfView('pdf')
                            }}
                          >
                            PDF
                          </Button>
                        )}
                        {it.url && (
                          <Button
                            size="small"
                            icon={<LinkOutlined />}
                            onClick={(e) => {
                              e.stopPropagation()
                              window.open(it.url!, '_blank', 'noopener,noreferrer')
                            }}
                          >
                            原文链接
                          </Button>
                        )}
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>
          ))}

          {/* Load more */}
          {data?.next_before_ms && (
            <div style={{ textAlign: 'center', padding: 20 }}>
              <Button
                onClick={() =>
                  load({ append: true, beforeMs: data.next_before_ms || undefined })
                }
                loading={loadingMore}
              >
                加载更多
              </Button>
              <div style={{ color: '#94a3b8', fontSize: 11, marginTop: 6 }}>
                已加载 {items.length.toLocaleString()} / {filterTotal.toLocaleString()}
              </div>
            </div>
          )}
          {!data?.next_before_ms && items.length > 0 && (
            <div style={{ textAlign: 'center', color: '#94a3b8', fontSize: 12, padding: 20 }}>
              — 已加载全部 {items.length.toLocaleString()} 条 —
            </div>
          )}
        </div>
      )}

      <DetailDrawer
        item={detailItem}
        detail={detail}
        loading={detailLoading}
        error={detailError}
        view={pdfView}
        setView={setPdfView}
        pdfBlobUrl={pdfBlobUrl}
        pdfLoading={pdfLoading}
        pdfError={pdfError}
        pdfProgress={pdfProgress}
        activePdfIdx={activePdfIdx}
        setActivePdfIdx={setActivePdfIdx}
        onClose={closeDetail}
        onLoadPdf={loadPdf}
        onDownloadPdf={downloadPdf}
        clearPdf={clearPdf}
        lang={lang}
        setLang={setLang}
      />
    </div>
  )
}


// ── Detail Drawer ───────────────────────────────────────────
// Fetches full doc on open; tabs between markdown sections and the live
// PDF iframe. PDF loads lazily the first time the PDF tab is selected and
// is cached in a blob URL for the lifetime of the drawer.

interface DetailDrawerProps {
  item: HubItem | null
  detail: DocDetailResponse | null
  loading: boolean
  error: string | null
  view: 'sections' | 'pdf'
  setView: (v: 'sections' | 'pdf') => void
  pdfBlobUrl: string | null
  pdfLoading: boolean
  pdfError: string | null
  pdfProgress: { loaded: number; total: number } | null
  activePdfIdx: number
  setActivePdfIdx: (i: number) => void
  onClose: () => void
  onLoadPdf: (url: string) => void | Promise<void>
  onDownloadPdf: (url: string, filename: string) => void | Promise<void>
  clearPdf: () => void
  lang: 'zh' | 'orig'
  setLang: (v: 'zh' | 'orig') => void
}

// Drawer width is user-resizable via the left-edge handle. Persisted across
// sessions in localStorage; clamped on window resize so it never exceeds the
// viewport.
const DRAWER_WIDTH_KEY = 'stockhub.drawerWidth'
const DRAWER_MIN_WIDTH = 480
const DRAWER_MAX_PADDING = 80  // keep at least this much of the left list visible

function getInitialDrawerWidth(): number {
  const fallback = Math.min(900, Math.floor(window.innerWidth * 0.92))
  try {
    const raw = localStorage.getItem(DRAWER_WIDTH_KEY)
    if (raw) {
      const n = parseInt(raw, 10)
      if (!isNaN(n)) {
        return Math.min(Math.max(DRAWER_MIN_WIDTH, n), window.innerWidth - DRAWER_MAX_PADDING)
      }
    }
  } catch { /* localStorage may be unavailable in some embeds */ }
  return fallback
}

function DetailDrawer({
  item, detail, loading, error,
  view, setView,
  pdfBlobUrl, pdfLoading, pdfError, pdfProgress,
  activePdfIdx, setActivePdfIdx,
  onClose, onLoadPdf, onDownloadPdf, clearPdf,
  lang, setLang,
}: DetailDrawerProps) {
  // ── Resizable width ──────────────────────────────────────
  const [drawerWidth, setDrawerWidth] = useState<number>(getInitialDrawerWidth)
  const [resizing, setResizing] = useState(false)
  const [hoverHandle, setHoverHandle] = useState(false)

  // ── Lang toggle (中文/原文) ──────────────────────────────
  // Lang preference is owned by the parent so list cards and the detail
  // drawer share it; persistence is wired in the parent.
  const hasAnyTranslation = (
    (detail?.title_zh || '').trim().length > 0 ||
    !!detail?.sections?.some(
      (s) => typeof s.markdown_zh === 'string' && s.markdown_zh.trim().length > 0,
    )
  )

  // Persist width
  useEffect(() => {
    try {
      localStorage.setItem(DRAWER_WIDTH_KEY, String(drawerWidth))
    } catch { /* ignore quota / disabled storage */ }
  }, [drawerWidth])

  // Re-clamp on window resize so the drawer never overflows the viewport
  useEffect(() => {
    const onResize = () => {
      setDrawerWidth((w) =>
        Math.min(Math.max(DRAWER_MIN_WIDTH, w), window.innerWidth - DRAWER_MAX_PADDING),
      )
    }
    window.addEventListener('resize', onResize)
    return () => window.removeEventListener('resize', onResize)
  }, [])

  const handleResizePointerDown = useCallback(
    (e: React.PointerEvent<HTMLDivElement>) => {
      e.preventDefault()
      const startX = e.clientX
      const startWidth = drawerWidth
      setResizing(true)
      // Lock body cursor + disable selection while dragging so the cursor
      // doesn't flicker over text.
      const prevCursor = document.body.style.cursor
      const prevUserSelect = document.body.style.userSelect
      document.body.style.cursor = 'col-resize'
      document.body.style.userSelect = 'none'

      const onMove = (ev: PointerEvent) => {
        // Drawer slides in from the right, so dragging LEFT widens it.
        const dx = startX - ev.clientX
        const next = Math.min(
          Math.max(DRAWER_MIN_WIDTH, startWidth + dx),
          window.innerWidth - DRAWER_MAX_PADDING,
        )
        setDrawerWidth(next)
      }
      const onUp = () => {
        setResizing(false)
        document.body.style.cursor = prevCursor
        document.body.style.userSelect = prevUserSelect
        document.removeEventListener('pointermove', onMove)
        document.removeEventListener('pointerup', onUp)
        document.removeEventListener('pointercancel', onUp)
      }
      document.addEventListener('pointermove', onMove)
      document.addEventListener('pointerup', onUp)
      document.addEventListener('pointercancel', onUp)
    },
    [drawerWidth],
  )

  // Resolve which pdf URL to fetch (single-pdf vs. meritco multi-attachment)
  const pdfCandidates: DocPdfAttachment[] = useMemo(() => {
    if (!detail) return []
    if (detail.pdf_urls.length) return detail.pdf_urls
    if (detail.pdf_url) {
      return [{ index: 0, name: 'PDF', size_bytes: 0, url: detail.pdf_url }]
    }
    return []
  }, [detail])

  const activePdf = pdfCandidates[activePdfIdx] || null

  // Track which URL the current blob represents so attachment switches force
  // a reload but otherwise we don't refetch on unrelated rerenders.
  const loadedUrlRef = useRef<string | null>(null)
  useEffect(() => {
    if (pdfBlobUrl) return  // updated below when we successfully load
    loadedUrlRef.current = null
  }, [pdfBlobUrl])

  // Auto-load PDF when the PDF tab becomes active, or when the active
  // attachment changes (meritco multi-PDF). If a previous blob was loaded
  // for a different URL, drop it before fetching the new one.
  useEffect(() => {
    if (view !== 'pdf') return
    if (!activePdf) return
    if (pdfLoading) return
    if (pdfBlobUrl && loadedUrlRef.current === activePdf.url) return
    if (pdfBlobUrl && loadedUrlRef.current !== activePdf.url) clearPdf()
    loadedUrlRef.current = activePdf.url
    onLoadPdf(activePdf.url)
  }, [view, activePdf, pdfBlobUrl, pdfLoading, onLoadPdf, clearPdf])

  const open = item != null
  const metaRow = (label: string, value: React.ReactNode) =>
    value ? (
      <div style={{ display: 'flex', gap: 10, fontSize: 12, color: '#64748b', marginBottom: 4 }}>
        <span style={{ flexShrink: 0, width: 56, color: '#94a3b8' }}>{label}</span>
        <span style={{ color: '#334155' }}>{value}</span>
      </div>
    ) : null

  return (
    <Drawer
      open={open}
      onClose={onClose}
      width={drawerWidth}
      closable={false}
      title={null}
      destroyOnClose
      styles={{ body: { padding: 0, position: 'relative' } }}
    >
      {/* Left-edge resize handle. Hit area is 10px wide for easy grabbing,
          but only a 2px line is drawn (brighter on hover/drag). Sits on top
          of the sticky header via z-index. */}
      <div
        onPointerDown={handleResizePointerDown}
        onMouseEnter={() => setHoverHandle(true)}
        onMouseLeave={() => setHoverHandle(false)}
        title="拖动调整宽度"
        style={{
          position: 'absolute',
          top: 0,
          bottom: 0,
          left: -5,
          width: 10,
          cursor: 'col-resize',
          zIndex: 20,
          background: 'transparent',
          touchAction: 'none',
        }}
      >
        <div
          style={{
            position: 'absolute',
            top: 0,
            bottom: 0,
            left: 4,
            width: 2,
            background: resizing ? '#3b82f6' : hoverHandle ? '#93c5fd' : 'transparent',
            transition: 'background 0.12s ease',
            pointerEvents: 'none',
          }}
        />
      </div>

      {/* Header */}
      <div
        style={{
          padding: '14px 20px 10px', borderBottom: '1px solid #e2e8f0',
          background: '#fff', position: 'sticky', top: 0, zIndex: 5,
        }}
      >
        <div style={{ display: 'flex', alignItems: 'flex-start', gap: 10 }}>
          <div style={{ flex: 1, minWidth: 0 }}>
            {item && (
              <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 6 }}>
                <Tag color={SOURCE_COLORS[item.source] || 'default'} style={{ margin: 0 }}>
                  {item.source_label}
                </Tag>
                {detail?.category_label && (
                  <Tag
                    color={CATEGORY_META[(detail.category as Exclude<Category, 'all'>)]?.color}
                    style={{ margin: 0 }}
                  >
                    {detail.category_label}
                  </Tag>
                )}
                {detail?.organization && (
                  <span style={{ color: '#64748b', fontSize: 12, alignSelf: 'center' }}>
                    · {detail.organization}
                  </span>
                )}
                {detail?.release_time && (
                  <span style={{ color: '#94a3b8', fontSize: 12, alignSelf: 'center' }}>
                    · {detail.release_time}
                  </span>
                )}
              </div>
            )}
            <div style={{ fontSize: 18, fontWeight: 600, color: '#0f172a', lineHeight: 1.4 }}>
              {(() => {
                if (lang === 'zh') {
                  const dz = (detail?.title_zh || '').trim()
                  if (dz) return dz
                  const iz = (item?.title_zh || '').trim()
                  if (iz) return iz
                }
                return detail?.title || item?.title || '...'
              })()}
            </div>
          </div>
          <Button
            type="text"
            icon={<CloseOutlined />}
            onClick={onClose}
            style={{ flexShrink: 0 }}
          />
        </div>

        {/* Tabs + top actions */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginTop: 12 }}>
          <Segmented
            size="small"
            value={view}
            onChange={(v) => setView(v as 'sections' | 'pdf')}
            options={[
              { label: '全文', value: 'sections' },
              ...(pdfCandidates.length ? [{ label: 'PDF', value: 'pdf' as const }] : []),
            ]}
          />
          {hasAnyTranslation && view === 'sections' && (
            <Segmented
              size="small"
              value={lang}
              onChange={(v) => setLang(v as 'zh' | 'orig')}
              options={[
                { label: '中文', value: 'zh' },
                { label: '原文', value: 'orig' },
              ]}
            />
          )}
          <div style={{ flex: 1 }} />
          {detail?.url && (
            <Tooltip title="在新标签打开原文">
              <Button
                size="small"
                icon={<LinkOutlined />}
                onClick={() => window.open(detail.url!, '_blank', 'noopener,noreferrer')}
              >
                原文
              </Button>
            </Tooltip>
          )}
          {activePdf && (
            <Tooltip title="下载 PDF">
              <Button
                size="small"
                icon={<DownloadOutlined />}
                onClick={() => onDownloadPdf(activePdf.url, detail?.title || item?.title || 'document')}
              >
                下载
              </Button>
            </Tooltip>
          )}
        </div>
      </div>

      {/* Body */}
      <div style={{ padding: view === 'pdf' ? 0 : '20px 24px 60px' }}>
        {loading ? (
          <div style={{ padding: 40, textAlign: 'center' }}>
            <Spin tip="加载中..." />
          </div>
        ) : error ? (
          <Alert type="error" showIcon message="加载失败" description={error} style={{ margin: 20 }} />
        ) : !detail ? null : view === 'sections' ? (
          <>
            {/* AI 摘要 — qwen-plus card-preview summary written by the
                local_ai_summary worker. Rendered above the body sections so
                the takeaway is visible without scrolling past disclaimers. */}
            {detail.local_ai_summary && (detail.local_ai_summary.tldr || (detail.local_ai_summary.bullets || []).length > 0) && (
              <section style={{ marginBottom: 24 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
                  <div
                    style={{
                      fontSize: 13, fontWeight: 600, color: '#0369a1',
                      padding: '6px 10px', borderRadius: 4,
                      background: '#e0f2fe',
                      display: 'inline-block',
                    }}
                  >
                    AI 摘要
                  </div>
                  <span style={{
                    fontSize: 11, color: '#64748b',
                    background: '#f1f5f9', padding: '2px 6px', borderRadius: 3,
                  }}>
                    {detail.local_ai_summary.model || 'qwen-plus'}
                  </span>
                </div>
                <div style={{
                  background: '#f8fafc', border: '1px solid #e2e8f0',
                  borderLeft: '3px solid #0ea5e9',
                  padding: '14px 16px', borderRadius: 4,
                }}>
                  {detail.local_ai_summary.tldr && (
                    <div style={{
                      fontSize: 14, color: '#0f172a', lineHeight: 1.7,
                      fontWeight: 500,
                    }}>
                      {detail.local_ai_summary.tldr}
                    </div>
                  )}
                  {(detail.local_ai_summary.bullets || []).length > 0 && (
                    <ul style={{
                      marginTop: detail.local_ai_summary.tldr ? 12 : 0,
                      marginBottom: 0, paddingLeft: 20,
                      fontSize: 13, color: '#334155', lineHeight: 1.65,
                    }}>
                      {detail.local_ai_summary.bullets.map((b, i) => (
                        <li key={i} style={{ marginBottom: 4 }}>{b}</li>
                      ))}
                    </ul>
                  )}
                </div>
              </section>
            )}
            {detail.sections.length === 0 ? (
              <Empty
                description={
                  <div>
                    <div>该文档暂无全文内容可展示</div>
                    {pdfCandidates.length > 0 && (
                      <div style={{ marginTop: 8, color: '#64748b', fontSize: 12 }}>
                        切换至 PDF 标签查看原始文件
                      </div>
                    )}
                  </div>
                }
                style={{ padding: '40px 0' }}
              />
            ) : (
              detail.sections.map((sec, i) => {
                const zh = (sec.markdown_zh || '').trim()
                const showZh = lang === 'zh' && zh.length > 0
                const body = showZh ? zh : sec.markdown
                return (
                  <section key={i} style={{ marginBottom: 28 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
                      <div
                        style={{
                          fontSize: 13, fontWeight: 600, color: '#475569',
                          padding: '6px 10px', borderRadius: 4,
                          background: '#f1f5f9',
                          display: 'inline-block',
                        }}
                      >
                        {sec.label}
                      </div>
                      {showZh && (
                        <span style={{
                          fontSize: 11, color: '#0891b2',
                          background: '#cffafe', padding: '2px 6px', borderRadius: 3,
                        }}>AI 翻译</span>
                      )}
                    </div>
                    <div style={{ fontSize: 14, color: '#1e293b', lineHeight: 1.75 }}>
                      <MarkdownRenderer content={body} />
                    </div>
                  </section>
                )
              })
            )}
            {detail.tickers.length > 0 && (
              <div style={{ marginTop: 20, paddingTop: 16, borderTop: '1px solid #e2e8f0' }}>
                <div style={{ fontSize: 12, color: '#94a3b8', marginBottom: 6 }}>涉及标的</div>
                <Space wrap size={4}>
                  {detail.tickers.map((t) => (
                    <Tag key={t} style={{ margin: 0, fontSize: 11 }}>{t}</Tag>
                  ))}
                </Space>
              </div>
            )}
          </>
        ) : (
          <PdfPane
            attachments={pdfCandidates}
            activeIdx={activePdfIdx}
            setActiveIdx={setActivePdfIdx}
            blobUrl={pdfBlobUrl}
            loading={pdfLoading}
            progress={pdfProgress}
            error={pdfError}
            onRetry={() => activePdf && onLoadPdf(activePdf.url)}
          />
        )}
      </div>
    </Drawer>
  )
}

interface PdfPaneProps {
  attachments: DocPdfAttachment[]
  activeIdx: number
  setActiveIdx: (i: number) => void
  blobUrl: string | null
  loading: boolean
  progress: { loaded: number; total: number } | null
  error: string | null
  onRetry: () => void
}

function PdfPane({
  attachments, activeIdx, setActiveIdx,
  blobUrl, loading, progress, error, onRetry,
}: PdfPaneProps) {
  if (attachments.length === 0) {
    return <Empty description="无 PDF 附件" style={{ padding: 60 }} />
  }
  const fmtMB = (n: number) => `${(n / (1024 * 1024)).toFixed(1)} MB`
  const progressLabel = progress
    ? progress.total
      ? `加载 PDF... ${fmtMB(progress.loaded)} / ${fmtMB(progress.total)} (${Math.floor((progress.loaded / progress.total) * 100)}%)`
      : `加载 PDF... ${fmtMB(progress.loaded)}`
    : '加载 PDF...'
  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: 'calc(100vh - 140px)' }}>
      {attachments.length > 1 && (
        <div
          style={{
            padding: '8px 16px', borderBottom: '1px solid #e2e8f0',
            display: 'flex', gap: 6, flexWrap: 'wrap', background: '#fafafa',
          }}
        >
          {attachments.map((att, i) => (
            <Button
              key={att.index}
              size="small"
              type={i === activeIdx ? 'primary' : 'default'}
              onClick={() => setActiveIdx(i)}
            >
              {att.name}
            </Button>
          ))}
        </div>
      )}
      <div style={{ flex: 1, position: 'relative', background: '#f8fafc' }}>
        {loading ? (
          <div
            style={{
              position: 'absolute', inset: 0, display: 'flex',
              flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
              gap: 12, padding: 24, textAlign: 'center',
            }}
          >
            <Spin size="large" />
            <div style={{ color: '#475569', fontSize: 13 }}>{progressLabel}</div>
            <div style={{ color: '#94a3b8', fontSize: 11, maxWidth: 360 }}>
              正在从本地 SSD 加载；首次大文件偶尔需几秒，缓存命中后秒开。
            </div>
          </div>
        ) : error ? (
          <Alert
            type="error" showIcon
            message="PDF 加载失败"
            description={error}
            action={<Button size="small" onClick={onRetry}>重试</Button>}
            style={{ margin: 20 }}
          />
        ) : blobUrl ? (
          // PDF Open Parameters (#view=FitH&toolbar=1&navpanes=0) are NOT
          // applied here: Chrome PDFium silently drops them on blob: URLs
          // and on some PDFs aborts the iframe load entirely
          // (net::ERR_ABORTED), so the page shows blank even though the
          // bytes are fine and a forced ?download=1 still works. Chrome's
          // PDF viewer picks a sensible default zoom on its own; not worth
          // breaking inline preview for the auto-fit-width nicety.
          <iframe
            src={blobUrl}
            title="PDF"
            style={{ width: '100%', height: '100%', border: 'none' }}
          />
        ) : null}
      </div>
    </div>
  )
}
