import { useEffect, useState, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Card, Row, Col, Tag, Typography, Spin, Segmented, Empty, Select, Badge, Tooltip,
  Timeline, Collapse, Space, Divider,
} from 'antd'
import {
  FundOutlined, RightOutlined, BellOutlined, SyncOutlined,
  ThunderboltOutlined, FireOutlined, AlertOutlined, RiseOutlined, FallOutlined,
  ClockCircleOutlined, SearchOutlined, ExperimentOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import api from '../services/api'

const { Text, Title, Paragraph } = Typography

// ── Types ────────────────────────────────────────────────

interface PortfolioHolding {
  name: string
  url: string
  enabled: boolean
  priority: string
  market: string
  tags: string[]
  stock_ticker: string
  stock_name: string
  stock_market: string
}

interface BreakingNewsItem {
  id: string
  ticker: string
  name_cn: string
  name_en: string
  market: string
  market_label: string
  scan_time: string
  news_materiality: string
  news_summary: string
  new_developments: string[]
  novelty_status: string
  earliest_report_time: string | null
  deep_research_performed: boolean
  research_iterations: number
  key_findings: string[]
  news_timeline: Array<{ time?: string; source?: string; title?: string }>
  referenced_sources: Array<{ url?: string; title?: string; source?: string }>
  historical_precedents: Array<{
    event_date?: string; description?: string; ticker?: string
    return_1d?: number; return_3d?: number; return_5d?: number
  }>
  alert_confidence: number
  alert_rationale: string
  sentiment: string
  impact_magnitude: string
  impact_timeframe: string
  surprise_factor: number
  bull_case: string
  bear_case: string
  recommended_action: string
}

interface NewsSummary {
  news_count: number
  latest_scan: string
  latest_materiality: string
  latest_sentiment: string
  latest_summary: string
}

// ── Constants ────────────────────────────────────────────

const MARKET_GROUPS: Record<string, string[]> = {
  all: [],
  us: ['美股'],
  a: ['科创板', '创业板', '主板'],
  hk: ['港股'],
  kr: ['韩股'],
  jp: ['日股'],
}

const MARKET_TAG_COLORS: Record<string, string> = {
  '美股': 'blue',
  '科创板': 'cyan',
  '创业板': 'green',
  '主板': 'orange',
  '港股': 'red',
  '韩股': 'purple',
  '日股': 'magenta',
}

const HOURS_OPTIONS = [
  { value: 24, label: '24小时' },
  { value: 48, label: '48小时' },
  { value: 168, label: '7天' },
  { value: 720, label: '30天' },
]

const MATERIALITY_CONFIG: Record<string, { color: string; icon: React.ReactNode; label: string }> = {
  critical: { color: '#f5222d', icon: <FireOutlined />, label: '重大' },
  material: { color: '#fa8c16', icon: <AlertOutlined />, label: '重要' },
  routine: { color: '#1890ff', icon: <BellOutlined />, label: '常规' },
  none: { color: '#8c8c8c', icon: <ClockCircleOutlined />, label: '无' },
}

const SENTIMENT_CONFIG: Record<string, { color: string; icon: React.ReactNode; label: string }> = {
  very_bullish: { color: '#389e0d', icon: <RiseOutlined />, label: '强烈看多' },
  bullish: { color: '#52c41a', icon: <RiseOutlined />, label: '看多' },
  neutral: { color: '#8c8c8c', icon: <span>—</span>, label: '中性' },
  bearish: { color: '#f5222d', icon: <FallOutlined />, label: '看空' },
  very_bearish: { color: '#cf1322', icon: <FallOutlined />, label: '强烈看空' },
}

const CACHE_KEY = 'portfolio_counts'

function getCachedCounts(hours: number): Record<string, number> | null {
  try {
    const raw = sessionStorage.getItem(CACHE_KEY)
    if (!raw) return null
    const cached = JSON.parse(raw)
    if (cached.hours === hours && Date.now() - cached.ts < 5 * 60 * 1000) {
      return cached.counts
    }
  } catch { /* ignore */ }
  return null
}

function setCachedCounts(hours: number, counts: Record<string, number>) {
  try {
    sessionStorage.setItem(CACHE_KEY, JSON.stringify({ hours, counts, ts: Date.now() }))
  } catch { /* ignore */ }
}

function timeAgo(isoStr: string): string {
  const diff = Date.now() - new Date(isoStr).getTime()
  const mins = Math.floor(diff / 60000)
  if (mins < 60) return `${mins}分钟前`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}小时前`
  const days = Math.floor(hrs / 24)
  return `${days}天前`
}

function formatReturn(val: number | undefined | null): React.ReactNode {
  if (val == null) return <Text type="secondary">—</Text>
  const pct = (val * 100).toFixed(1)
  const color = val > 0 ? '#52c41a' : val < 0 ? '#f5222d' : '#8c8c8c'
  return <span style={{ color, fontWeight: 500 }}>{val > 0 ? '+' : ''}{pct}%</span>
}

// ── Components ───────────────────────────────────────────

function BreakingNewsCard({ item }: { item: BreakingNewsItem }) {
  const matCfg = MATERIALITY_CONFIG[item.news_materiality] || MATERIALITY_CONFIG.none
  const sentCfg = SENTIMENT_CONFIG[item.sentiment] || SENTIMENT_CONFIG.neutral

  const collapseItems = []

  // Key findings
  if (item.key_findings?.length > 0) {
    collapseItems.push({
      key: 'findings',
      label: <Text strong style={{ fontSize: 13 }}><SearchOutlined style={{ marginRight: 4 }} />关键发现</Text>,
      children: (
        <ul style={{ margin: 0, paddingLeft: 20 }}>
          {item.key_findings.map((f, i) => (
            <li key={i} style={{ fontSize: 13, marginBottom: 4, color: '#434343' }}>{f}</li>
          ))}
        </ul>
      ),
    })
  }

  // Bull/Bear case
  if (item.bull_case || item.bear_case) {
    collapseItems.push({
      key: 'analysis',
      label: <Text strong style={{ fontSize: 13 }}><ExperimentOutlined style={{ marginRight: 4 }} />多空分析</Text>,
      children: (
        <div style={{ fontSize: 13 }}>
          {item.bull_case && (
            <div style={{ marginBottom: 8 }}>
              <Tag color="green" style={{ marginBottom: 4 }}>看多逻辑</Tag>
              <div style={{ color: '#434343' }}>{item.bull_case}</div>
            </div>
          )}
          {item.bear_case && (
            <div>
              <Tag color="red" style={{ marginBottom: 4 }}>看空逻辑</Tag>
              <div style={{ color: '#434343' }}>{item.bear_case}</div>
            </div>
          )}
        </div>
      ),
    })
  }

  // Historical precedents
  if (item.historical_precedents?.length > 0) {
    collapseItems.push({
      key: 'precedents',
      label: <Text strong style={{ fontSize: 13 }}><ClockCircleOutlined style={{ marginRight: 4 }} />历史先例</Text>,
      children: (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', fontSize: 12, borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #f0f0f0' }}>
                <th style={{ textAlign: 'left', padding: '4px 8px', color: '#8c8c8c', fontWeight: 500 }}>日期</th>
                <th style={{ textAlign: 'left', padding: '4px 8px', color: '#8c8c8c', fontWeight: 500 }}>事件</th>
                <th style={{ textAlign: 'right', padding: '4px 8px', color: '#8c8c8c', fontWeight: 500 }}>T+1</th>
                <th style={{ textAlign: 'right', padding: '4px 8px', color: '#8c8c8c', fontWeight: 500 }}>T+3</th>
                <th style={{ textAlign: 'right', padding: '4px 8px', color: '#8c8c8c', fontWeight: 500 }}>T+5</th>
              </tr>
            </thead>
            <tbody>
              {item.historical_precedents.slice(0, 5).map((p, i) => (
                <tr key={i} style={{ borderBottom: '1px solid #fafafa' }}>
                  <td style={{ padding: '4px 8px', whiteSpace: 'nowrap', color: '#595959' }}>{p.event_date || '—'}</td>
                  <td style={{ padding: '4px 8px', color: '#434343', maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{p.description || '—'}</td>
                  <td style={{ padding: '4px 8px', textAlign: 'right' }}>{formatReturn(p.return_1d)}</td>
                  <td style={{ padding: '4px 8px', textAlign: 'right' }}>{formatReturn(p.return_3d)}</td>
                  <td style={{ padding: '4px 8px', textAlign: 'right' }}>{formatReturn(p.return_5d)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ),
    })
  }

  return (
    <Card
      size="small"
      style={{
        borderRadius: 10,
        borderLeft: `4px solid ${matCfg.color}`,
        marginBottom: 0,
      }}
      styles={{ body: { padding: '12px 16px' } }}
    >
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 8 }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <Space size={6} wrap>
            <Tag color={matCfg.color} style={{ margin: 0, fontSize: 11, lineHeight: '20px' }}>
              {matCfg.icon} {matCfg.label}
            </Tag>
            <Tag color="blue" style={{ margin: 0, fontSize: 11, lineHeight: '20px' }}>
              {item.name_cn} {item.ticker}
            </Tag>
            <Tag style={{ margin: 0, fontSize: 11, lineHeight: '20px', color: sentCfg.color, borderColor: sentCfg.color, background: 'transparent' }}>
              {sentCfg.icon} {sentCfg.label}
            </Tag>
            {item.deep_research_performed && (
              <Tag color="purple" style={{ margin: 0, fontSize: 11, lineHeight: '20px' }}>
                深度研究 x{item.research_iterations}
              </Tag>
            )}
          </Space>
        </div>
        <Text type="secondary" style={{ fontSize: 11, whiteSpace: 'nowrap', marginLeft: 8 }}>
          {timeAgo(item.scan_time)}
        </Text>
      </div>

      {/* Summary */}
      <Paragraph style={{ margin: '0 0 8px', fontSize: 13, lineHeight: '20px', color: '#262626' }} ellipsis={{ rows: 3, expandable: true, symbol: '展开' }}>
        {item.news_summary}
      </Paragraph>

      {/* Developments */}
      {item.new_developments?.length > 0 && (
        <div style={{ marginBottom: 8 }}>
          {item.new_developments.slice(0, 3).map((dev, i) => (
            <div key={i} style={{ fontSize: 12, color: '#595959', lineHeight: '18px', marginBottom: 2 }}>
              <ThunderboltOutlined style={{ color: matCfg.color, marginRight: 4, fontSize: 11 }} />
              {dev}
            </div>
          ))}
        </div>
      )}

      {/* Meta bar */}
      <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap', marginBottom: collapseItems.length > 0 ? 4 : 0 }}>
        <Text type="secondary" style={{ fontSize: 11 }}>
          置信度: <span style={{ color: item.alert_confidence >= 0.9 ? '#389e0d' : item.alert_confidence >= 0.8 ? '#fa8c16' : '#8c8c8c', fontWeight: 600 }}>
            {(item.alert_confidence * 100).toFixed(0)}%
          </span>
        </Text>
        <Text type="secondary" style={{ fontSize: 11 }}>
          影响: {item.impact_magnitude === 'critical' ? '重大' : item.impact_magnitude === 'high' ? '高' : item.impact_magnitude === 'medium' ? '中' : '低'}
        </Text>
        <Text type="secondary" style={{ fontSize: 11 }}>
          时间窗口: {item.impact_timeframe === 'long_term' ? '长期' : item.impact_timeframe === 'medium_term' ? '中期' : '短期'}
        </Text>
        {item.recommended_action && (
          <Text style={{ fontSize: 11, color: '#1890ff' }}>
            建议: {item.recommended_action}
          </Text>
        )}
      </div>

      {/* Expandable details */}
      {collapseItems.length > 0 && (
        <Collapse
          ghost
          size="small"
          items={collapseItems}
          style={{ marginLeft: -12, marginRight: -12 }}
        />
      )}
    </Card>
  )
}

// ── Main Component ───────────────────────────────────────

export default function Portfolio() {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const [holdings, setHoldings] = useState<PortfolioHolding[]>([])
  const [loading, setLoading] = useState(true)
  const [marketFilter, setMarketFilter] = useState<string>('all')
  const [counts, setCounts] = useState<Record<string, number>>({})
  const [countsLoading, setCountsLoading] = useState(false)
  const [hours, setHours] = useState(168)

  // Breaking news state
  const [breakingNews, setBreakingNews] = useState<BreakingNewsItem[]>([])
  const [newsLoading, setNewsLoading] = useState(false)
  const [newsSummary, setNewsSummary] = useState<Record<string, NewsSummary>>({})

  useEffect(() => {
    api
      .get('/sources/portfolio')
      .then((res) => setHoldings(res.data.holdings || []))
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [])

  // Fetch article counts (existing logic)
  const fetchCounts = useCallback((holdingsList: PortfolioHolding[], h: number) => {
    if (holdingsList.length === 0) return
    const tickers = [...new Set(holdingsList.map((x) => x.stock_ticker))]

    const cached = getCachedCounts(h)
    if (cached) setCounts(cached)

    setCountsLoading(true)
    api
      .post('/stock/counts', { tickers }, { params: { hours: h } })
      .then((res) => {
        const fresh = res.data.counts || {}
        setCounts(fresh)
        setCachedCounts(h, fresh)
      })
      .catch(console.error)
      .finally(() => setCountsLoading(false))
  }, [])

  // Fetch breaking news
  const fetchBreakingNews = useCallback((h: number, market?: string) => {
    setNewsLoading(true)
    const params: Record<string, string | number> = { hours: h, limit: 50 }
    if (market && market !== 'all') {
      // Map UI market filter to API market values
      const marketMap: Record<string, string> = { us: 'us', a: 'china', hk: 'hk', kr: 'kr', jp: 'jp' }
      if (marketMap[market]) params.market = marketMap[market]
    }
    api
      .get('/portfolio/breaking-news', { params })
      .then((res) => setBreakingNews(res.data.items || []))
      .catch(() => setBreakingNews([]))
      .finally(() => setNewsLoading(false))
  }, [])

  // Fetch per-ticker news summary
  const fetchNewsSummary = useCallback((h: number) => {
    api
      .get('/portfolio/breaking-news/summary', { params: { hours: h } })
      .then((res) => setNewsSummary(res.data.summary || {}))
      .catch(() => setNewsSummary({}))
  }, [])

  useEffect(() => {
    fetchCounts(holdings, hours)
    fetchBreakingNews(hours, marketFilter)
    fetchNewsSummary(hours)
  }, [holdings, hours, fetchCounts, fetchBreakingNews, fetchNewsSummary])

  // Refetch news when market filter changes
  useEffect(() => {
    if (!loading) fetchBreakingNews(hours, marketFilter)
  }, [marketFilter]) // eslint-disable-line react-hooks/exhaustive-deps

  if (loading) {
    return (
      <div style={{ textAlign: 'center', padding: 80 }}>
        <Spin size="large" />
      </div>
    )
  }

  const filtered =
    marketFilter === 'all'
      ? holdings
      : holdings.filter((h) =>
          MARKET_GROUPS[marketFilter]?.includes(h.stock_market),
        )

  const hasBreakingNews = breakingNews.length > 0

  return (
    <div>
      {/* ── Page Header ── */}
      <div style={{ marginBottom: 20 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <Title level={4} style={{ margin: 0 }}>
            <FundOutlined style={{ marginRight: 8 }} />
            {t('portfolio.title')}
          </Title>
          {(countsLoading || newsLoading) && (
            <Tooltip title="正在刷新数据">
              <SyncOutlined spin style={{ color: '#1677ff', fontSize: 14 }} />
            </Tooltip>
          )}
        </div>
        <Text type="secondary" style={{ fontSize: 13 }}>{t('portfolio.subtitle')}</Text>
      </div>

      {/* ── Filters ── */}
      <div style={{ marginBottom: 16, display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
        <Segmented
          value={marketFilter}
          onChange={(v) => setMarketFilter(v as string)}
          options={[
            { value: 'all', label: t('portfolio.allMarkets') },
            { value: 'us', label: t('portfolio.usStocks') },
            { value: 'a', label: t('portfolio.aShares') },
            { value: 'hk', label: t('portfolio.hkStocks') },
            { value: 'kr', label: t('portfolio.krStocks') },
            { value: 'jp', label: t('portfolio.jpStocks') },
          ]}
        />
        <Select
          value={hours}
          onChange={setHours}
          style={{ width: 100 }}
          options={HOURS_OPTIONS}
          size="small"
        />
        <Text type="secondary" style={{ fontSize: 13 }}>
          {t('common.total')} {filtered.length} {t('portfolio.holdings')}
        </Text>
      </div>

      {/* ── Breaking News Section ── */}
      {hasBreakingNews && (
        <div style={{ marginBottom: 24 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
            <ThunderboltOutlined style={{ color: '#fa8c16', fontSize: 16 }} />
            <Text strong style={{ fontSize: 15 }}>
              {t('portfolio.breakingNews')}
            </Text>
            <Tag color="orange" style={{ margin: 0, fontSize: 11 }}>
              {breakingNews.length}
            </Tag>
          </div>
          <Row gutter={[12, 12]}>
            {breakingNews.map((item) => (
              <Col xs={24} lg={12} key={item.id}>
                <BreakingNewsCard item={item} />
              </Col>
            ))}
          </Row>
        </div>
      )}

      {hasBreakingNews && <Divider style={{ margin: '8px 0 20px' }} />}

      {/* ── Holdings Grid ── */}
      <div style={{ marginBottom: 12, display: 'flex', alignItems: 'center', gap: 8 }}>
        <Text strong style={{ fontSize: 15 }}>
          {t('portfolio.holdingsTitle')}
        </Text>
      </div>

      {filtered.length === 0 ? (
        <Empty description={t('news.noData')} />
      ) : (
        <Row gutter={[10, 10]}>
          {filtered.map((h) => {
            const updateCount = counts[h.stock_ticker] || 0
            const hasUpdates = updateCount > 0
            const tickerNews = newsSummary[h.stock_ticker]
            const hasBreaking = !!tickerNews

            // Determine border color based on breaking news
            let borderColor = 'transparent'
            if (hasBreaking) {
              const mat = tickerNews.latest_materiality
              borderColor = mat === 'critical' ? '#f5222d' : mat === 'material' ? '#fa8c16' : '#1677ff'
            } else if (hasUpdates) {
              borderColor = '#1677ff'
            }

            return (
              <Col xs={12} sm={8} md={6} lg={4} xl={4} key={`${h.stock_market}-${h.stock_ticker}`}>
                <Badge count={updateCount} overflowCount={99} offset={[-6, 2]} size="small">
                  <Card
                    size="small"
                    hoverable
                    onClick={() => navigate(`/stock-search?q=${encodeURIComponent(h.stock_ticker)}`)}
                    style={{
                      borderRadius: 10,
                      width: '100%',
                      borderLeft: `3px solid ${borderColor}`,
                      transition: 'border-color 0.3s',
                    }}
                    styles={{ body: { padding: '10px 12px' } }}
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                      <div style={{ minWidth: 0, flex: 1 }}>
                        <div style={{
                          fontWeight: 600,
                          fontSize: 14,
                          lineHeight: '20px',
                          whiteSpace: 'nowrap',
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                        }}>
                          {h.stock_name}
                        </div>
                        <div style={{ marginTop: 4, display: 'flex', alignItems: 'center', gap: 4, flexWrap: 'wrap' }}>
                          <Tag color="blue" style={{ fontSize: 10, margin: 0, lineHeight: '18px', padding: '0 4px' }}>
                            {h.stock_ticker}
                          </Tag>
                          <Tag
                            color={MARKET_TAG_COLORS[h.stock_market] || 'default'}
                            style={{ fontSize: 10, margin: 0, lineHeight: '18px', padding: '0 4px' }}
                          >
                            {h.stock_market}
                          </Tag>
                          {h.tags
                            .filter((tag) => tag !== 'holding')
                            .slice(0, 1)
                            .map((tag) => (
                              <Tag key={tag} style={{ fontSize: 10, margin: 0, lineHeight: '18px', padding: '0 4px', color: '#64748b', background: '#f1f5f9', border: 'none' }}>
                                {tag}
                              </Tag>
                            ))}
                        </div>
                      </div>
                      <RightOutlined style={{ color: '#c0c8d4', fontSize: 10, marginTop: 4, flexShrink: 0 }} />
                    </div>

                    {/* Breaking news indicator */}
                    {hasBreaking && (
                      <Tooltip title={tickerNews.latest_summary}>
                        <div style={{ marginTop: 6, fontSize: 11, color: MATERIALITY_CONFIG[tickerNews.latest_materiality]?.color || '#fa8c16' }}>
                          <ThunderboltOutlined style={{ marginRight: 3 }} />
                          {tickerNews.news_count}条突发
                          {tickerNews.latest_sentiment && tickerNews.latest_sentiment !== 'neutral' && (
                            <span style={{ marginLeft: 4, color: SENTIMENT_CONFIG[tickerNews.latest_sentiment]?.color || '#8c8c8c' }}>
                              {SENTIMENT_CONFIG[tickerNews.latest_sentiment]?.icon}
                            </span>
                          )}
                        </div>
                      </Tooltip>
                    )}

                    {/* General update count (only show if no breaking news indicator) */}
                    {!hasBreaking && hasUpdates && (
                      <div style={{ marginTop: 6, fontSize: 11, color: '#1677ff' }}>
                        <BellOutlined style={{ marginRight: 3 }} />
                        {updateCount}条更新
                      </div>
                    )}
                  </Card>
                </Badge>
              </Col>
            )
          })}
        </Row>
      )}
    </div>
  )
}
