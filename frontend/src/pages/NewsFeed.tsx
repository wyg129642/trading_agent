import { useEffect, useState, useCallback } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import {
  Card,
  List,
  Tag,
  Select,
  Space,
  Input,
  Segmented,
  Progress,
  Typography,
  Spin,
  Badge,
  Tooltip,
  Switch,
  Tabs,
  Empty,
  Button,
  message,
} from 'antd'
import {
  SearchOutlined,
  ThunderboltOutlined,
  ClockCircleOutlined,
  FilterOutlined,
  StarOutlined,
  PlusOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { useAuthStore } from '../store/auth'
import { useWebSocket } from '../hooks/useWebSocket'
import { useFavorites } from '../hooks/useFavorites'
import FavoriteButton from '../components/FavoriteButton'
import api from '../services/api'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'

dayjs.extend(relativeTime)

const { Text } = Typography

// --- TypeScript interfaces ---

interface NewsItem {
  id: string
  source_name: string
  title: string
  title_zh: string | null
  url: string
  published_at: string | null
  fetched_at: string
  language: string
  sentiment: string | null
  impact_magnitude: string | null
  surprise_factor: number | null
  affected_tickers: string[]
  affected_sectors: string[]
  ticker_sentiments: Record<string, any>
  sector_sentiments: Record<string, any>
  summary: string | null
  category: string | null
  is_read: boolean
  time_type: 'published' | 'crawled'
  is_relevant: boolean
  has_analysis: boolean
}

interface NewsResponse {
  items: NewsItem[]
  total: number
  page: number
  page_size: number
  has_next: boolean
}

interface CategoryOption {
  name: string
  count: number
}

// --- Multi-horizon types & normalization ---

interface HorizonPrediction {
  sentiment: string
  sentiment_score: number
  confidence: number
}

interface MultiHorizonSentiment {
  short_term: HorizonPrediction
  medium_term: HorizonPrediction
  long_term: HorizonPrediction
  reason?: string
}

function normalizeSentiment(val: any): MultiHorizonSentiment {
  if (typeof val === 'string') {
    const scoreMap: Record<string, number> = { very_bullish: 0.8, bullish: 0.5, neutral: 0.0, bearish: -0.5, very_bearish: -0.8 }
    const hp = { sentiment: val, sentiment_score: scoreMap[val] ?? 0, confidence: 1.0 }
    return { short_term: hp, medium_term: hp, long_term: hp }
  }
  const defaults = { sentiment: 'neutral', sentiment_score: 0, confidence: 0.5 }
  return {
    short_term: { ...defaults, ...(val?.short_term || {}) },
    medium_term: { ...defaults, ...(val?.medium_term || {}) },
    long_term: { ...defaults, ...(val?.long_term || {}) },
    reason: val?.reason,
  }
}

// --- Color constants ---

const SENTIMENT_COLORS: Record<string, string> = {
  very_bullish: '#059669',
  bullish: '#10b981',
  neutral: '#94a3b8',
  bearish: '#f59e0b',
  very_bearish: '#ef4444',
}

const IMPACT_COLORS: Record<string, string> = {
  critical: '#ef4444',
  high: '#f59e0b',
  medium: '#2563eb',
  low: '#94a3b8',
}

export default function NewsFeed() {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const user = useAuthStore((s) => s.user)

  const [items, setItems] = useState<NewsItem[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const [categories, setCategories] = useState<CategoryOption[]>([])

  // Filters
  const [searchQuery, setSearchQuery] = useState(searchParams.get('q') || '')
  const [category, setCategory] = useState<string | undefined>(
    searchParams.get('category') || undefined,
  )
  const [sentiment, setSentiment] = useState<string | undefined>(
    searchParams.get('sentiment') || undefined,
  )
  const [impact, setImpact] = useState<string | undefined>(
    searchParams.get('impact') || undefined,
  )
  const [hours, setHours] = useState<number>(24)
  const [unfiltered, setUnfiltered] = useState(false)
  const [watchlistOnly, setWatchlistOnly] = useState(false)
  const [watchedTickers, setWatchedTickers] = useState<Set<string>>(new Set())

  // Load watchlist tickers for quick-add badge display
  const fetchWatchedTickers = useCallback(async () => {
    try {
      const res = await api.get('/watchlists/all-values')
      setWatchedTickers(new Set(res.data.tickers?.map((t: any) => t.value) || []))
    } catch {}
  }, [])

  useEffect(() => {
    fetchWatchedTickers()
  }, [fetchWatchedTickers])

  // Load categories on mount
  useEffect(() => {
    api
      .get<CategoryOption[]>('/news/categories')
      .then((res) => setCategories(res.data))
      .catch(() => {})
  }, [])

  // Reset page when filters change
  useEffect(() => {
    setPage(1)
  }, [searchQuery, category, sentiment, impact, hours, unfiltered, watchlistOnly])

  const fetchNews = useCallback(async () => {
    setLoading(true)
    try {
      let res
      if (searchQuery) {
        res = await api.get<NewsResponse>('/news/search', {
          params: { q: searchQuery, page, page_size: 20 },
        })
      } else {
        const params: Record<string, any> = { page, page_size: 20, hours }
        if (sentiment) params.sentiment = sentiment
        if (impact) params.impact = impact
        if (category) params.category = category
        if (unfiltered) params.unfiltered = true
        if (watchlistOnly) params.watchlist_only = true
        res = await api.get<NewsResponse>('/news', { params })
      }
      setItems(res.data.items)
      setTotal(res.data.total)
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }, [page, sentiment, impact, hours, searchQuery, category, unfiltered, watchlistOnly])

  useEffect(() => {
    fetchNews()
  }, [fetchNews])

  // WebSocket for live updates
  const onWsMessage = useCallback((data: any) => {
    if (data.id) {
      setItems((prev) => [data as NewsItem, ...prev.slice(0, 49)])
      setTotal((prev) => prev + 1)
    }
  }, [])
  useWebSocket(onWsMessage)

  // Build category options
  const categoryOptions = [
    { value: '', label: t('news.allCategories') },
    ...categories.map((c) => ({
      value: c.name,
      label: `${c.name} (${c.count})`,
    })),
  ]

  const { favoriteIds, toggleFavorite } = useFavorites('news')

  const handleQuickAddTicker = async (ticker: string, e: React.MouseEvent) => {
    e.stopPropagation()
    if (watchedTickers.has(ticker)) return
    try {
      // Extract pure ticker code (remove Chinese name in parentheses)
      const pureCode = ticker.replace(/\(.*?\)/, '').trim()
      const displayName = ticker !== pureCode ? ticker : undefined
      await api.post('/watchlists/quick-add', {
        item_type: 'ticker',
        value: pureCode,
        display_name: displayName,
      })
      message.success(`${ticker} ${t('watchlist.addSuccess')}`)
      setWatchedTickers((prev) => new Set([...prev, pureCode]))
    } catch (err: any) {
      if (err.response?.status === 409) {
        message.info(t('news.alreadyWatched'))
      } else {
        message.error(t('common.error'))
      }
    }
  }

  const isAdmin = user?.role === 'admin'

  return (
    <div>
      {/* Filter bar */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap size="middle">
          <Input
            prefix={<SearchOutlined />}
            placeholder={t('news.search')}
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onPressEnter={() => fetchNews()}
            style={{ width: 240 }}
            allowClear
          />
          <Select
            placeholder={t('news.allCategories')}
            value={category || undefined}
            onChange={(v) => setCategory(v || undefined)}
            allowClear
            style={{ width: 160 }}
            options={categoryOptions}
          />
          <Select
            placeholder={t('news.sentiment')}
            value={sentiment}
            onChange={setSentiment}
            allowClear
            style={{ width: 140 }}
            options={[
              { value: 'very_bullish', label: t('sentiment.very_bullish') },
              { value: 'bullish', label: t('sentiment.bullish') },
              { value: 'neutral', label: t('sentiment.neutral') },
              { value: 'bearish', label: t('sentiment.bearish') },
              { value: 'very_bearish', label: t('sentiment.very_bearish') },
            ]}
          />
          <Select
            placeholder={t('news.impact')}
            value={impact}
            onChange={setImpact}
            allowClear
            style={{ width: 120 }}
            options={[
              { value: 'critical', label: t('impact.critical') },
              { value: 'high', label: t('impact.high') },
              { value: 'medium', label: t('impact.medium') },
              { value: 'low', label: t('impact.low') },
            ]}
          />
          <Segmented
            value={hours}
            onChange={(v) => setHours(v as number)}
            options={[
              { value: 1, label: '1h' },
              { value: 4, label: '4h' },
              { value: 24, label: '24h' },
              { value: 168, label: '7d' },
            ]}
          />
          <Tooltip title={t('news.watchlistOnlyTip')}>
            <Button
              size="small"
              type={watchlistOnly ? 'primary' : 'default'}
              icon={<StarOutlined />}
              onClick={() => setWatchlistOnly(!watchlistOnly)}
            >
              {t('news.watchlistOnly')}
            </Button>
          </Tooltip>
          {isAdmin && (
            <Space size={4}>
              <FilterOutlined />
              <Switch
                size="small"
                checked={unfiltered}
                onChange={setUnfiltered}
              />
              <Text type="secondary" style={{ fontSize: 12 }}>
                {t('news.showAll')}
              </Text>
            </Space>
          )}
          <Text type="secondary">
            {total} {t('news.results')}
          </Text>
        </Space>
      </Card>

      {/* News List */}
      <List
        loading={loading}
        dataSource={items}
        locale={{ emptyText: <Empty description={t('news.noData')} /> }}
        pagination={{
          current: page,
          total,
          pageSize: 20,
          onChange: setPage,
          showSizeChanger: false,
        }}
        renderItem={(item) => {
          const cardClassNames = [
            'news-card',
            item.sentiment ? `sentiment-${item.sentiment}` : '',
            item.is_read ? '' : 'unread',
          ]
            .filter(Boolean)
            .join(' ')

          const timeSource =
            item.time_type === 'published' ? item.published_at : item.fetched_at
          const timeLabel =
            item.time_type === 'published'
              ? t('news.publishedTime')
              : t('news.crawledTime')

          return (
            <Card
              className={cardClassNames}
              size="small"
              style={{
                marginBottom: 8,
                cursor: 'pointer',
              }}
              onClick={() => navigate(`/news/${item.id}`)}
              hoverable
            >
              <div
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'flex-start',
                }}
              >
                <div style={{ flex: 1 }}>
                  {/* Top row: per-stock/sector sentiment + impact */}
                  <Space size={4} style={{ marginBottom: 4 }} wrap>
                    {(() => {
                      const ts = item.ticker_sentiments || {}
                      const ss = item.sector_sentiments || {}
                      const hasTicker = Object.keys(ts).length > 0
                      const hasSector = Object.keys(ss).length > 0
                      if (hasTicker) {
                        return Object.entries(ts).slice(0, 4).map(([ticker, val]) => {
                          const data = normalizeSentiment(val)
                          const t1 = data.short_term
                          const t5 = data.medium_term
                          const t1Label = t1.sentiment.includes('bullish') ? '↑' : t1.sentiment.includes('bearish') ? '↓' : '→'
                          const t5Label = t5.sentiment.includes('bullish') ? '↑' : t5.sentiment.includes('bearish') ? '↓' : '→'
                          const t1Color = SENTIMENT_COLORS[t1.sentiment] || '#94a3b8'
                          const t5Color = SENTIMENT_COLORS[t5.sentiment] || '#94a3b8'
                          const tickerCode = ticker.replace(/\(.*?\)/, '').trim()
                          const isWatched = watchedTickers.has(tickerCode)
                          return (
                            <Tooltip
                              key={ticker}
                              title={
                                <div style={{ fontSize: 12 }}>
                                  <div><strong>{ticker}</strong></div>
                                  <div style={{ marginTop: 4 }}>T+1 {t(`sentiment.${t1.sentiment}`)}: score {t1.sentiment_score > 0 ? '+' : ''}{t1.sentiment_score.toFixed(2)}, {t('news.confidence')} {(t1.confidence * 100).toFixed(0)}%</div>
                                  <div>T+5 {t(`sentiment.${t5.sentiment}`)}: score {t5.sentiment_score > 0 ? '+' : ''}{t5.sentiment_score.toFixed(2)}, {t('news.confidence')} {(t5.confidence * 100).toFixed(0)}%</div>
                                  <div>T+20 {t(`sentiment.${data.long_term.sentiment}`)}: score {data.long_term.sentiment_score > 0 ? '+' : ''}{data.long_term.sentiment_score.toFixed(2)}, {t('news.confidence')} {(data.long_term.confidence * 100).toFixed(0)}%</div>
                                  {data.reason && <div style={{ marginTop: 4, opacity: 0.8, borderTop: '1px solid rgba(255,255,255,0.2)', paddingTop: 4 }}>{data.reason}</div>}
                                  {!isWatched && <div style={{ marginTop: 4, color: '#fbbf24' }}>{t('news.clickToWatch')}</div>}
                                </div>
                              }
                            >
                              <Tag style={{ display: 'inline-flex', alignItems: 'center', gap: 2, padding: '0 6px', cursor: isWatched ? 'default' : 'pointer' }}>
                                {!isWatched && (
                                  <PlusOutlined
                                    style={{ fontSize: 10, color: '#1677ff', marginRight: 2 }}
                                    onClick={(e) => handleQuickAddTicker(ticker, e)}
                                  />
                                )}
                                {isWatched && (
                                  <StarOutlined style={{ fontSize: 10, color: '#faad14', marginRight: 2 }} />
                                )}
                                <strong>{ticker}</strong>
                                <span style={{ color: t1Color, fontWeight: 700, fontSize: 13 }}>{t1Label}</span>
                                <span style={{ color: '#bbb', fontSize: 10 }}>T1</span>
                                <span style={{ color: t5Color, fontWeight: 700, fontSize: 13, marginLeft: 2 }}>{t5Label}</span>
                                <span style={{ color: '#bbb', fontSize: 10 }}>T5</span>
                                <span style={{ opacity: 0.5, marginLeft: 2, fontSize: 10 }}>
                                  {(t1.confidence * 100).toFixed(0)}%
                                </span>
                              </Tag>
                            </Tooltip>
                          )
                        })
                      }
                      if (hasSector) {
                        return Object.entries(ss).slice(0, 3).map(([sector, val]) => {
                          const data = normalizeSentiment(val)
                          const t1 = data.short_term
                          const t5 = data.medium_term
                          const t1Label = t1.sentiment.includes('bullish') ? '↑' : t1.sentiment.includes('bearish') ? '↓' : '→'
                          const t5Label = t5.sentiment.includes('bullish') ? '↑' : t5.sentiment.includes('bearish') ? '↓' : '→'
                          const t1Color = SENTIMENT_COLORS[t1.sentiment] || '#94a3b8'
                          const t5Color = SENTIMENT_COLORS[t5.sentiment] || '#94a3b8'
                          return (
                            <Tooltip
                              key={sector}
                              title={
                                <div style={{ fontSize: 12 }}>
                                  <div><strong>{sector}</strong></div>
                                  <div style={{ marginTop: 4 }}>T+1 {t(`sentiment.${t1.sentiment}`)}: score {t1.sentiment_score > 0 ? '+' : ''}{t1.sentiment_score.toFixed(2)}, {t('news.confidence')} {(t1.confidence * 100).toFixed(0)}%</div>
                                  <div>T+5 {t(`sentiment.${t5.sentiment}`)}: score {t5.sentiment_score > 0 ? '+' : ''}{t5.sentiment_score.toFixed(2)}, {t('news.confidence')} {(t5.confidence * 100).toFixed(0)}%</div>
                                  <div>T+20 {t(`sentiment.${data.long_term.sentiment}`)}: score {data.long_term.sentiment_score > 0 ? '+' : ''}{data.long_term.sentiment_score.toFixed(2)}, {t('news.confidence')} {(data.long_term.confidence * 100).toFixed(0)}%</div>
                                  {data.reason && <div style={{ marginTop: 4, opacity: 0.8, borderTop: '1px solid rgba(255,255,255,0.2)', paddingTop: 4 }}>{data.reason}</div>}
                                </div>
                              }
                            >
                              <Tag style={{ display: 'inline-flex', alignItems: 'center', gap: 2, padding: '0 6px' }}>
                                <strong>{sector}</strong>
                                <span style={{ color: t1Color, fontWeight: 700, fontSize: 13 }}>{t1Label}</span>
                                <span style={{ color: '#bbb', fontSize: 10 }}>T1</span>
                                <span style={{ color: t5Color, fontWeight: 700, fontSize: 13, marginLeft: 2 }}>{t5Label}</span>
                                <span style={{ color: '#bbb', fontSize: 10 }}>T5</span>
                                <span style={{ opacity: 0.5, marginLeft: 2, fontSize: 10 }}>
                                  {(t1.confidence * 100).toFixed(0)}%
                                </span>
                              </Tag>
                            </Tooltip>
                          )
                        })
                      }
                      // Fallback: global sentiment + plain ticker tags
                      return (
                        <>
                          {item.sentiment && (
                            <Tag
                              className={`tag-${item.sentiment}`}
                              color={SENTIMENT_COLORS[item.sentiment]}
                            >
                              {t(`sentiment.${item.sentiment}`)}
                            </Tag>
                          )}
                          {item.affected_tickers?.slice(0, 3).map((ticker) => (
                            <Tag key={ticker}>{ticker}</Tag>
                          ))}
                        </>
                      )
                    })()}
                    {item.impact_magnitude && (
                      <Tag
                        className={`tag-impact-${item.impact_magnitude}`}
                        color={IMPACT_COLORS[item.impact_magnitude]}
                      >
                        <ThunderboltOutlined /> {t(`impact.${item.impact_magnitude}`)}
                      </Tag>
                    )}
                    {unfiltered && !item.has_analysis && (
                      <Tag color="default">{t('news.unprocessed')}</Tag>
                    )}
                  </Space>

                  {/* Title */}
                  <div
                    style={{ fontWeight: 600, fontSize: 15, marginBottom: 4 }}
                  >
                    {item.title_zh && item.title_zh !== item.title
                      ? <>
                          {item.title_zh}
                          <span style={{ fontSize: 12, fontWeight: 400, color: '#8c8c8c', marginLeft: 6 }}>
                            （{item.title}）
                          </span>
                        </>
                      : item.title
                    }
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
                      }}
                    >
                      {item.summary.substring(0, 200)}
                      {item.summary.length > 200 ? '...' : ''}
                    </Text>
                  )}

                  {/* Bottom row: source, time, sectors */}
                  <div
                    style={{
                      marginTop: 6,
                      display: 'flex',
                      alignItems: 'center',
                      gap: 12,
                      fontSize: 12,
                      color: '#8c8c8c',
                      flexWrap: 'wrap',
                    }}
                  >
                    <span>{item.source_name}</span>
                    <span>
                      <ClockCircleOutlined />{' '}
                      {timeLabel}：{dayjs(timeSource || item.fetched_at).tz('Asia/Shanghai').format('MM-DD HH:mm')}
                    </span>
                    {item.affected_sectors?.slice(0, 2).map((s) => (
                      <Tag key={s} style={{ fontSize: 11 }}>
                        {s}
                      </Tag>
                    ))}
                  </div>
                </div>

                {/* Favorite + Surprise factor gauge */}
                <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 8, marginLeft: 12 }}>
                <FavoriteButton
                  itemType="news"
                  itemId={item.id}
                  favoriteIds={favoriteIds}
                  onToggle={toggleFavorite}
                />
                {item.surprise_factor != null && (
                  <Tooltip
                    title={`${t('news.surprise') || 'Surprise'}: ${item.surprise_factor.toFixed(2)}`}
                  >
                    <Progress
                      type="circle"
                      percent={Math.round(item.surprise_factor * 100)}
                      size={48}
                      strokeColor={
                        item.surprise_factor > 0.7
                          ? '#ef4444'
                          : item.surprise_factor > 0.4
                            ? '#f59e0b'
                            : '#059669'
                      }
                      format={(p) => `${(p! / 100).toFixed(1)}`}
                    />
                  </Tooltip>
                )}
                </div>
              </div>
            </Card>
          )
        }}
      />
    </div>
  )
}
