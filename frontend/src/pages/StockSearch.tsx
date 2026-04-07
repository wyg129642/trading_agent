/**
 * Stock Search — trader-oriented unified search across all data sources.
 *
 * Features:
 * - Autocomplete dropdown as user types (stock name/code suggestions)
 * - Unified results from AlphaPai, Jiuqian, and News Center
 * - Source filter tabs with counts
 * - Time range selector
 */
import { useEffect, useState, useCallback, useRef } from 'react'
import { useSearchParams, useNavigate } from 'react-router-dom'
import {
  AutoComplete,
  Card,
  List,
  Tag,
  Space,
  Input,
  Typography,
  Tooltip,
  Empty,
  Select,
  Spin,
} from 'antd'
import {
  SearchOutlined,
  ClockCircleOutlined,
  RiseOutlined,
  FallOutlined,
  ThunderboltOutlined,
  LinkOutlined,
  StockOutlined,
} from '@ant-design/icons'
import api from '../services/api'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'

dayjs.extend(relativeTime)

const { Text } = Typography

// --- Types ---

interface StockSuggestion {
  name: string
  code: string
  market: string
  label: string
}

interface SearchResultItem {
  source: string
  source_label: string
  id: string
  title: string
  original_title?: string
  time: string | null
  summary: string
  tickers: string[]
  sectors: string[]
  sentiment: string
  relevance_score?: number
  market_impact_score?: number
  impact_magnitude?: string
  surprise_factor?: number
  url?: string
  author?: string
  company?: string
  institution?: string
  analyst?: string
  industry?: string
  source_name?: string
}

interface SearchResponse {
  items: SearchResultItem[]
  total: number
  page: number
  page_size: number
  has_next: boolean
  query: string
  search_terms: string[]
  source_counts: Record<string, number>
}

// --- Constants ---

const SENTIMENT_MAP: Record<string, { color: string; label: string; icon: any }> = {
  very_bullish: { color: '#059669', label: '强烈看多', icon: <RiseOutlined /> },
  bullish: { color: '#52c41a', label: '看多', icon: <RiseOutlined /> },
  bearish: { color: '#f59e0b', label: '看空', icon: <FallOutlined /> },
  very_bearish: { color: '#ef4444', label: '强烈看空', icon: <FallOutlined /> },
  neutral: { color: '#d9d9d9', label: '中性', icon: null },
}

const SOURCE_COLORS: Record<string, string> = {
  alphapai_wechat: '#2563eb',
  alphapai_comment: '#7c3aed',
  alphapai_roadshow: '#0891b2',
  jiuqian_forum: '#dc2626',
  jiuqian_minutes: '#ea580c',
  jiuqian_wechat: '#ca8a04',
  news: '#059669',
}

const SOURCE_LABELS: Record<string, string> = {
  alphapai_wechat: 'AlphaPai公众号',
  alphapai_comment: 'AlphaPai点评',
  alphapai_roadshow: 'AlphaPai路演',
  jiuqian_forum: '久谦访谈',
  jiuqian_minutes: '久谦纪要',
  jiuqian_wechat: '久谦公众号',
  news: '资讯中心',
}

const MARKET_COLORS: Record<string, string> = {
  'A股': '#e11d48',
  '美股': '#2563eb',
  '港股': '#7c3aed',
}

const HOURS_OPTIONS = [
  { value: 24, label: '24小时' },
  { value: 48, label: '48小时' },
  { value: 168, label: '7天' },
  { value: 720, label: '30天' },
]

export default function StockSearch() {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const initialQ = searchParams.get('q') || ''

  // Search input state
  const [inputValue, setInputValue] = useState(initialQ)
  const [suggestions, setSuggestions] = useState<StockSuggestion[]>([])
  const [suggestLoading, setSuggestLoading] = useState(false)

  // Active search state
  const [activeQuery, setActiveQuery] = useState(initialQ)
  const [selectedStock, setSelectedStock] = useState<StockSuggestion | null>(null)

  // Results state
  const [allItems, setAllItems] = useState<SearchResultItem[]>([])
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(false)
  const [hours, setHours] = useState(168)
  const [sourceFilter, setSourceFilter] = useState<string>('all')
  const [searchTerms, setSearchTerms] = useState<string[]>([])
  const [sourceCounts, setSourceCounts] = useState<Record<string, number>>({})

  const suggestTimer = useRef<ReturnType<typeof setTimeout>>()

  // --- Autocomplete ---
  const fetchSuggestions = useCallback(async (text: string) => {
    if (!text || text.length < 1) {
      setSuggestions([])
      return
    }
    setSuggestLoading(true)
    try {
      const res = await api.get<StockSuggestion[]>('/stock/suggest', {
        params: { q: text, limit: 10 },
      })
      setSuggestions(res.data)
    } catch {
      setSuggestions([])
    } finally {
      setSuggestLoading(false)
    }
  }, [])

  const onInputChange = (text: string) => {
    setInputValue(text)
    // Debounce suggestions
    if (suggestTimer.current) clearTimeout(suggestTimer.current)
    suggestTimer.current = setTimeout(() => fetchSuggestions(text), 200)
  }

  const onSelectSuggestion = (value: string, option: any) => {
    const stock = option.stock as StockSuggestion
    setSelectedStock(stock)
    setInputValue(stock.label)
    triggerSearch(stock.label)
  }

  const triggerSearch = (q: string) => {
    const trimmed = q.trim()
    if (!trimmed) return
    setActiveQuery(trimmed)
    setPage(1)
    setSourceFilter('all')
    setSearchParams({ q: trimmed })
  }

  // --- Search execution ---
  const doSearch = useCallback(async (q: string) => {
    if (!q.trim()) return
    setLoading(true)
    try {
      const res = await api.get<SearchResponse>('/stock/search', {
        params: { q: q.trim(), hours, page: 1, page_size: 200 },
      })
      setAllItems(res.data.items)
      setSearchTerms(res.data.search_terms)
      setSourceCounts(res.data.source_counts)
    } catch (e) {
      console.error(e)
      setAllItems([])
      setSourceCounts({})
    } finally {
      setLoading(false)
    }
  }, [hours])

  // Re-search when activeQuery or hours changes
  useEffect(() => {
    if (activeQuery) doSearch(activeQuery)
  }, [activeQuery, doSearch])

  // Reset page when filter changes
  useEffect(() => {
    setPage(1)
  }, [sourceFilter])

  // Auto-search if URL has ?q= on mount
  useEffect(() => {
    if (initialQ && !activeQuery) {
      setActiveQuery(initialQ)
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // --- Derived data ---
  const filteredItems =
    sourceFilter === 'all'
      ? allItems
      : allItems.filter((it) => it.source === sourceFilter)
  const totalCount = Object.values(sourceCounts).reduce((a, b) => a + b, 0)
  const pageItems = filteredItems.slice((page - 1) * 20, page * 20)

  // Build autocomplete options
  const autoCompleteOptions = suggestions.map((s) => ({
    value: s.label,
    label: (
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span>
          <StockOutlined style={{ marginRight: 6, color: '#94a3b8' }} />
          <b>{s.name}</b>
          <span style={{ color: '#94a3b8', marginLeft: 6 }}>{s.code}</span>
        </span>
        <Tag
          color={MARKET_COLORS[s.market] || '#94a3b8'}
          style={{ margin: 0, fontSize: 11 }}
        >
          {s.market}
        </Tag>
      </div>
    ),
    stock: s,
  }))

  return (
    <div>
      {/* Search bar */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
          <AutoComplete
            value={inputValue}
            options={autoCompleteOptions}
            onSearch={onInputChange}
            onSelect={onSelectSuggestion}
            style={{ width: 360 }}
            notFoundContent={
              suggestLoading ? <Spin size="small" /> :
              inputValue.length >= 1 ? <Text type="secondary" style={{ fontSize: 12 }}>未找到匹配股票，回车搜索全部内容</Text> : null
            }
          >
            <Input
              prefix={<SearchOutlined />}
              placeholder="输入股票名称或代码，如：宁德时代、NVDA、00700..."
              size="large"
              allowClear
              onPressEnter={() => triggerSearch(inputValue)}
            />
          </AutoComplete>
          <Select
            value={hours}
            onChange={(v) => { setHours(v); setPage(1) }}
            style={{ width: 110 }}
            options={HOURS_OPTIONS}
          />
          {activeQuery && (
            <Text type="secondary">
              {loading ? '搜索中...' : totalCount > 0 ? `共 ${totalCount} 条相关信息` : ''}
            </Text>
          )}
        </div>

        {/* Resolved search terms */}
        {searchTerms.length > 1 && (
          <div style={{ marginTop: 8 }}>
            <Text type="secondary" style={{ fontSize: 12 }}>
              搜索匹配：
            </Text>
            {searchTerms.map((t, i) => (
              <Tag key={i} style={{ fontSize: 11 }}>{t}</Tag>
            ))}
          </div>
        )}
      </Card>

      {/* Source filter tabs */}
      {totalCount > 0 && (
        <Card size="small" style={{ marginBottom: 16 }}>
          <Space wrap size={6}>
            <Tag
              color={sourceFilter === 'all' ? '#2563eb' : undefined}
              style={{ cursor: 'pointer', fontSize: 13, padding: '2px 12px' }}
              onClick={() => setSourceFilter('all')}
            >
              全部 ({totalCount})
            </Tag>
            {Object.entries(sourceCounts).map(([src, count]) => (
              <Tag
                key={src}
                color={sourceFilter === src ? SOURCE_COLORS[src] : undefined}
                style={{ cursor: 'pointer', fontSize: 13, padding: '2px 12px' }}
                onClick={() => setSourceFilter(src === sourceFilter ? 'all' : src)}
              >
                {SOURCE_LABELS[src] || src} ({count})
              </Tag>
            ))}
          </Space>
        </Card>
      )}

      {/* Results list */}
      <List
        loading={loading}
        dataSource={pageItems}
        locale={{
          emptyText: activeQuery ? (
            <Empty
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description={
                <span>
                  未找到与 <b>"{activeQuery}"</b> 相关的信息
                  <br />
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    试试其他关键词，或扩大时间范围
                  </Text>
                </span>
              }
            />
          ) : (
            <Empty
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description={
                <span>
                  输入股票名称或代码开始搜索
                  <br />
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    支持A股、港股、美股，可输入中文名称或股票代码
                  </Text>
                </span>
              }
            />
          ),
        }}
        pagination={
          filteredItems.length > 20
            ? {
                current: page,
                total: filteredItems.length,
                pageSize: 20,
                onChange: setPage,
                showSizeChanger: false,
                showTotal: (total) => `${total} 条`,
              }
            : false
        }
        renderItem={(item) => {
          const sentiment = SENTIMENT_MAP[item.sentiment || '']

          // Determine if this item has an internal analysis page
          const hasAnalysisPage = item.source === 'news'

          const handleCardClick = () => {
            if (hasAnalysisPage) {
              navigate(`/news/${item.id}`)
            }
          }

          return (
            <Card
              size="small"
              style={{
                marginBottom: 10,
                borderLeft: `3px solid ${SOURCE_COLORS[item.source] || '#e2e8f0'}`,
                cursor: hasAnalysisPage ? 'pointer' : 'default',
              }}
              hoverable={hasAnalysisPage}
              onClick={handleCardClick}
            >
              <div>
                {/* Top row */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                  <Tag
                    color={SOURCE_COLORS[item.source]}
                    style={{ margin: 0, fontSize: 11 }}
                  >
                    {item.source_label}
                  </Tag>
                  {sentiment && sentiment.label !== '中性' && (
                    <Tag
                      color={sentiment.color}
                      icon={sentiment.icon}
                      style={{ margin: 0 }}
                    >
                      {sentiment.label}
                    </Tag>
                  )}
                  {item.market_impact_score != null && (
                    <Tooltip title={`市场影响力: ${item.market_impact_score}/10`}>
                      <Tag
                        icon={<ThunderboltOutlined />}
                        color={
                          item.market_impact_score >= 8 ? '#ef4444'
                            : item.market_impact_score >= 6 ? '#f59e0b'
                              : '#94a3b8'
                        }
                        style={{ margin: 0 }}
                      >
                        {item.market_impact_score}
                      </Tag>
                    </Tooltip>
                  )}
                  {item.impact_magnitude && item.impact_magnitude !== 'low' && (
                    <Tag
                      color={
                        item.impact_magnitude === 'critical' ? '#ef4444'
                          : item.impact_magnitude === 'high' ? '#f59e0b'
                            : '#2563eb'
                      }
                      style={{ margin: 0 }}
                    >
                      <ThunderboltOutlined /> {item.impact_magnitude}
                    </Tag>
                  )}
                </div>

                {/* Title */}
                <div style={{ fontWeight: 600, fontSize: 15, marginBottom: 4 }}>
                  <span>{item.title}</span>
                  {item.original_title && item.original_title !== item.title && (
                    <span style={{ fontSize: 12, fontWeight: 400, color: '#8c8c8c', marginLeft: 6 }}>
                      （{item.original_title}）
                    </span>
                  )}
                  {item.url && (
                    <a
                      href={item.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      onClick={(e) => e.stopPropagation()}
                      style={{ marginLeft: 6 }}
                    >
                      <LinkOutlined style={{ fontSize: 12, color: '#94a3b8' }} />
                    </a>
                  )}
                </div>

                {/* Summary */}
                {item.summary && (
                  <Text
                    type="secondary"
                    style={{
                      fontSize: 13,
                      display: '-webkit-box',
                      WebkitLineClamp: 2,
                      WebkitBoxOrient: 'vertical',
                      overflow: 'hidden',
                      marginBottom: 6,
                    }}
                  >
                    {item.summary.substring(0, 200)}
                    {item.summary.length > 200 ? '...' : ''}
                  </Text>
                )}

                {/* Tickers & sectors */}
                {(item.tickers.length > 0 || item.sectors.length > 0) && (
                  <div style={{ marginBottom: 6 }}>
                    {item.tickers.map((tk, i) => (
                      <Tag key={`t-${i}`} color="blue" style={{ fontSize: 11 }}>
                        {tk}
                      </Tag>
                    ))}
                    {item.sectors.slice(0, 3).map((s, i) => (
                      <Tag key={`s-${i}`} color="cyan" style={{ fontSize: 11 }}>
                        {s}
                      </Tag>
                    ))}
                  </div>
                )}

                {/* Meta row */}
                <div style={{ display: 'flex', gap: 12, fontSize: 12, color: '#8c8c8c', flexWrap: 'wrap' }}>
                  {item.time && (
                    <span>
                      <ClockCircleOutlined style={{ marginRight: 3 }} />
                      {dayjs(item.time).tz('Asia/Shanghai').fromNow()}
                    </span>
                  )}
                  {item.author && <span>{item.author}</span>}
                  {item.institution && <span>{item.institution}</span>}
                  {item.analyst && <span>{item.analyst}</span>}
                  {item.company && <span>{item.company}</span>}
                  {item.industry && <span>{item.industry}</span>}
                  {item.source_name && <span>{item.source_name}</span>}
                </div>
              </div>
            </Card>
          )
        }}
      />
    </div>
  )
}
