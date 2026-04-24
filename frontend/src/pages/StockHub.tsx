import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useParams, useSearchParams, useNavigate } from 'react-router-dom'
import {
  Tag, Typography, Empty, Skeleton, Segmented, Button, Tooltip, Badge, Space,
  Input, message,
} from 'antd'
import {
  FilePdfOutlined, LinkOutlined, ReloadOutlined, ArrowLeftOutlined,
  SearchOutlined, ThunderboltOutlined, BookOutlined, MessageOutlined,
  AudioOutlined, SolutionOutlined,
} from '@ant-design/icons'
import api from '../services/api'

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
  release_time: string | null
  release_time_ms: number | null
  url: string | null
  pdf_url: string | null
  preview: string
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
  gangtise:    '港推',
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
        if (it?.url) window.open(it.url, '_blank')
        else if (it?.pdf_url) window.open(it.pdf_url, '_blank')
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [items, hoverIndex])

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
  const total =
    data?.total ?? Object.values(byCat).reduce((s, x) => s + x, 0)

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
        {meta && (
          <span style={{ opacity: 0.55, fontSize: 10, marginLeft: 2 }}>
            {meta.shortcut}
          </span>
        )}
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
            汇聚 {total.toLocaleString()} 条资料 · 研报 / 点评 / 会议纪要 / 专家访谈 / 突发新闻
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
        {categoryButton('all', total, '全部')}
        {(['research', 'commentary', 'minutes', 'interview', 'breaking'] as const).map((c) =>
          categoryButton(c, byCat[c] || 0, CATEGORY_META[c].label, CATEGORY_META[c]),
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

                      {/* Title — clickable */}
                      <div
                        onClick={() => {
                          const dest = it.pdf_url || it.url
                          if (dest) window.open(dest, '_blank')
                        }}
                        style={{
                          fontSize: 15, fontWeight: 600, color: '#0f172a',
                          marginBottom: 6, cursor: (it.url || it.pdf_url) ? 'pointer' : 'default',
                          lineHeight: 1.45,
                        }}
                      >
                        {it.title}
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

                      {/* Preview */}
                      {it.preview && (
                        <div
                          style={{
                            fontSize: 13, color: '#475569', lineHeight: 1.55,
                            display: '-webkit-box',
                            WebkitLineClamp: 2, WebkitBoxOrient: 'vertical',
                            overflow: 'hidden',
                          }}
                        >
                          {it.preview}
                        </div>
                      )}

                      {/* Actions */}
                      <div style={{ marginTop: 8, display: 'flex', gap: 8 }}>
                        {it.pdf_url && (
                          <Button
                            size="small"
                            icon={<FilePdfOutlined />}
                            onClick={(e) => {
                              e.stopPropagation()
                              window.open(it.pdf_url!, '_blank')
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
                              window.open(it.url!, '_blank')
                            }}
                          >
                            查看原文
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
                已加载 {items.length.toLocaleString()} / {total.toLocaleString()}
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

      {/* Keyboard hint footer */}
      <div
        style={{
          position: 'fixed', bottom: 12, right: 16,
          background: 'rgba(15,23,42,.88)', color: '#cbd5e1',
          padding: '6px 12px', borderRadius: 6, fontSize: 11,
          backdropFilter: 'blur(6px)',
          pointerEvents: 'none',
        }}
      >
        <kbd>1–6</kbd> 切换分类 · <kbd>j/k</kbd> 浏览 · <kbd>Enter</kbd> 打开
      </div>
    </div>
  )
}
