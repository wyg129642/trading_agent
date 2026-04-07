import { useEffect, useState } from 'react'
import {
  Card,
  Col,
  Row,
  Statistic,
  Tag,
  List,
  Typography,
  Spin,
  Space,
  Divider,
} from 'antd'
import {
  FileTextOutlined,
  ThunderboltOutlined,
  ExperimentOutlined,
  RiseOutlined,
  FallOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  StockOutlined,
  ClockCircleOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'
import { useAuthStore } from '../store/auth'
import api from '../services/api'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'

dayjs.extend(relativeTime)

const { Text } = Typography

/* ── Types ── */

interface NewsStats {
  total_today: number
  total_week: number
  analyzed_today: number
  sentiment_distribution: Record<string, number>
  impact_distribution: Record<string, number>
}

interface NewsItem {
  id: string
  title: string
  source_name: string
  sentiment: string | null
  impact_magnitude: string | null
  surprise_factor: number | null
  affected_tickers: string[]
  published_at: string | null
  fetched_at: string
  time_type: string | null
  summary: string | null
}

interface PortfolioHolding {
  stock_name: string
  stock_ticker: string
  stock_market: string
  tags: string[]
}

interface SourceHealth {
  source_name: string
  is_healthy: boolean
  total_items_fetched: number
  consecutive_failures: number
  last_success: string | null
  last_failure: string | null
}

/* ── Color constants ── */

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

/* ── Component ── */

export default function Dashboard() {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const user = useAuthStore((s) => s.user)
  const role = user?.role || 'viewer'

  const [stats, setStats] = useState<NewsStats | null>(null)
  const [signals, setSignals] = useState<NewsItem[]>([])
  const [holdings, setHoldings] = useState<PortfolioHolding[]>([])
  const [sources, setSources] = useState<SourceHealth[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const requests: Promise<any>[] = [
      api.get('/news/stats'),
      api.get('/news', { params: { page_size: 10, impact: 'high' } }),
    ]

    // Portfolio for boss and admin
    if (role === 'boss' || role === 'admin') {
      requests.push(api.get('/sources/portfolio'))
    }

    // Source health for admin only
    if (role === 'admin') {
      requests.push(api.get('/sources/health'))
    }

    Promise.all(requests)
      .then((results) => {
        setStats(results[0].data)
        setSignals(results[1].data.items || [])

        let idx = 2
        if (role === 'boss' || role === 'admin') {
          setHoldings(results[idx]?.data?.holdings || [])
          idx++
        }
        if (role === 'admin') {
          setSources(results[idx]?.data?.sources || [])
        }
      })
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [role])

  if (loading) {
    return (
      <div style={{ textAlign: 'center', padding: 100 }}>
        <Spin size="large" />
      </div>
    )
  }

  /* ── Derived values ── */

  const bullishCount =
    (stats?.sentiment_distribution?.bullish || 0) +
    (stats?.sentiment_distribution?.very_bullish || 0)
  const bearishCount =
    (stats?.sentiment_distribution?.bearish || 0) +
    (stats?.sentiment_distribution?.very_bearish || 0)
  const bullBearRatio =
    bearishCount > 0 ? (bullishCount / bearishCount).toFixed(2) : bullishCount > 0 ? '∞' : '—'
  const highImpactCount =
    (stats?.impact_distribution?.critical || 0) + (stats?.impact_distribution?.high || 0)

  return (
    <div>
      {/* ── Metric Cards ── */}
      <Row gutter={[16, 16]}>
        <Col xs={24} sm={12} lg={6}>
          <Card className="stat-card">
            <Statistic
              title={t('dashboard.todaySignals')}
              value={stats?.total_today || 0}
              prefix={<FileTextOutlined />}
              suffix={
                <Text type="secondary" style={{ fontSize: 13 }}>
                  / {stats?.total_week || 0} {t('dashboard.weekTotal')}
                </Text>
              }
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card className="stat-card">
            <Statistic
              title={t('dashboard.analyzedToday')}
              value={stats?.analyzed_today || 0}
              prefix={<ExperimentOutlined />}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card className="stat-card">
            <Statistic
              title={t('dashboard.bullBearRatio')}
              value={bullBearRatio}
              prefix={<RiseOutlined style={{ color: SENTIMENT_COLORS.bullish }} />}
              suffix={
                <Text type="secondary" style={{ fontSize: 13 }}>
                  {bullishCount} <RiseOutlined style={{ color: SENTIMENT_COLORS.bullish }} /> /{' '}
                  {bearishCount} <FallOutlined style={{ color: SENTIMENT_COLORS.very_bearish }} />
                </Text>
              }
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card className="stat-card">
            <Statistic
              title={t('dashboard.highImpact')}
              value={highImpactCount}
              prefix={<ThunderboltOutlined style={{ color: IMPACT_COLORS.high }} />}
              valueStyle={{ color: highImpactCount > 0 ? IMPACT_COLORS.high : undefined }}
            />
          </Card>
        </Col>
      </Row>

      {/* ── Two-Column: Signals + Distribution ── */}
      <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
        {/* Left: Latest actionable signals */}
        <Col xs={24} lg={14}>
          <Card
            title={t('dashboard.latestSignals')}
            size="small"
            extra={
              <a onClick={() => navigate('/feed?impact=high')}>
                {t('dashboard.viewAll')}
              </a>
            }
          >
            <List
              size="small"
              dataSource={signals}
              locale={{ emptyText: t('news.noData') }}
              renderItem={(item: NewsItem) => {
                const timeLabel =
                  item.time_type === 'published'
                    ? t('news.publishedTime')
                    : t('news.crawledTime')
                const timeValue = dayjs(item.published_at || item.fetched_at).tz('Asia/Shanghai').fromNow()

                return (
                  <List.Item
                    style={{ cursor: 'pointer', padding: '8px 0' }}
                    onClick={() => navigate(`/news/${item.id}`)}
                  >
                    <div style={{ width: '100%' }}>
                      <Space size={4} wrap style={{ marginBottom: 4 }}>
                        {item.impact_magnitude && (
                          <Tag className={`tag-impact-${item.impact_magnitude}`}>
                            {t(`impact.${item.impact_magnitude}`)}
                          </Tag>
                        )}
                        {item.sentiment && (
                          <Tag className={`tag-${item.sentiment}`}>
                            {t(`sentiment.${item.sentiment}`)}
                          </Tag>
                        )}
                        {item.affected_tickers?.slice(0, 3).map((ticker) => (
                          <Tag key={ticker}>{ticker}</Tag>
                        ))}
                      </Space>
                      <div style={{ fontWeight: 600, fontSize: 14, marginBottom: 2 }}>
                        {item.title}
                      </div>
                      {item.summary && (
                        <Text type="secondary" style={{ fontSize: 12 }}>
                          {item.summary.substring(0, 120)}
                          {item.summary.length > 120 ? '...' : ''}
                        </Text>
                      )}
                      <div style={{ marginTop: 4, fontSize: 12, color: '#94a3b8' }}>
                        <ClockCircleOutlined style={{ marginRight: 4 }} />
                        {timeLabel}: {timeValue}
                        <span style={{ marginLeft: 12 }}>{item.source_name}</span>
                      </div>
                    </div>
                  </List.Item>
                )
              }}
            />
          </Card>
        </Col>

        {/* Right: Sentiment + Impact distribution */}
        <Col xs={24} lg={10}>
          <Card title={t('dashboard.sentimentDist')} size="small">
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              {Object.entries(stats?.sentiment_distribution || {}).map(([key, val]) => (
                <Tag
                  key={key}
                  className={`tag-${key}`}
                  style={{ fontSize: 13, padding: '2px 10px' }}
                >
                  {t(`sentiment.${key}`)}: {val}
                </Tag>
              ))}
            </div>

            <Divider style={{ margin: '12px 0' }} />

            <Text strong style={{ display: 'block', marginBottom: 8, fontSize: 13 }}>
              {t('news.impact')}
            </Text>
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              {Object.entries(stats?.impact_distribution || {}).map(([key, val]) => (
                <Tag
                  key={key}
                  className={`tag-impact-${key}`}
                  style={{ fontSize: 13, padding: '2px 10px' }}
                >
                  {t(`impact.${key}`)}: {val}
                </Tag>
              ))}
            </div>
          </Card>

          {/* News volume summary */}
          <Card size="small" style={{ marginTop: 16 }}>
            <Statistic
              title={t('dashboard.newsVolume')}
              value={stats?.total_today || 0}
              suffix={
                <Text type="secondary" style={{ fontSize: 13 }}>
                  / {stats?.total_week || 0} {t('dashboard.weekTotal')}
                </Text>
              }
            />
          </Card>
        </Col>
      </Row>

      {/* ── Portfolio Overview (boss + admin) ── */}
      {(role === 'boss' || role === 'admin') && (
        <Card
          title={t('dashboard.portfolioOverview')}
          size="small"
          style={{ marginTop: 16 }}
          extra={
            <a onClick={() => navigate('/sources')}>
              {t('dashboard.viewAll')}
            </a>
          }
        >
          {holdings.length === 0 ? (
            <Text type="secondary">{t('dashboard.noRecentNews')}</Text>
          ) : (
            <Row gutter={[8, 8]}>
              {holdings.map((h) => (
                <Col key={`${h.stock_market}-${h.stock_ticker}`} xs={12} sm={8} md={6} lg={4}>
                  <Card
                    size="small"
                    hoverable
                    style={{ textAlign: 'center' }}
                    bodyStyle={{ padding: '10px 8px' }}
                  >
                    <StockOutlined style={{ fontSize: 16, color: '#2563eb', marginBottom: 4 }} />
                    <div style={{ fontWeight: 600, fontSize: 13 }}>{h.stock_name}</div>
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      {h.stock_ticker}
                    </Text>
                    <div>
                      <Tag
                        style={{ fontSize: 11, marginTop: 4 }}
                        color={
                          h.stock_market === 'US'
                            ? 'blue'
                            : h.stock_market === 'CN'
                              ? 'red'
                              : h.stock_market === 'HK'
                                ? 'orange'
                                : 'default'
                        }
                      >
                        {h.stock_market}
                      </Tag>
                    </div>
                  </Card>
                </Col>
              ))}
            </Row>
          )}
        </Card>
      )}

      {/* ── Source Health (admin only) ── */}
      {role === 'admin' && (
        <Card
          title={t('dashboard.sourceHealth')}
          size="small"
          style={{ marginTop: 16 }}
          extra={
            <Text type="secondary" style={{ fontSize: 12 }}>
              <CheckCircleOutlined style={{ color: '#10b981', marginRight: 4 }} />
              {sources.filter((s) => s.is_healthy).length} / {sources.length}
              {sources.filter((s) => !s.is_healthy).length > 0 && (
                <span style={{ color: '#ef4444', marginLeft: 8 }}>
                  <CloseCircleOutlined style={{ marginRight: 2 }} />
                  {sources.filter((s) => !s.is_healthy).length} unhealthy
                </span>
              )}
            </Text>
          }
        >
          {/* Unhealthy sources shown prominently first */}
          {sources.filter((s) => !s.is_healthy).length > 0 && (
            <div style={{ marginBottom: 12, padding: '8px 12px', background: '#fef2f2', borderRadius: 6, border: '1px solid #fecaca' }}>
              <Text strong style={{ color: '#dc2626', fontSize: 13 }}>
                <CloseCircleOutlined style={{ marginRight: 4 }} />
                {t('dashboard.unhealthySources') || 'Unhealthy Sources'}
              </Text>
              <div style={{ marginTop: 6 }}>
                {sources.filter((s) => !s.is_healthy).map((s) => (
                  <div key={s.source_name} style={{ padding: '4px 0', fontSize: 12, color: '#374151' }}>
                    <Text strong style={{ color: '#dc2626' }}>{s.source_name}</Text>
                    <Text type="secondary" style={{ marginLeft: 8 }}>
                      {t('dashboard.consecutiveFailures') || 'Failures'}: {s.consecutive_failures}
                    </Text>
                    {s.last_failure && (
                      <Text type="secondary" style={{ marginLeft: 8 }}>
                        {t('dashboard.lastFailure') || 'Last fail'}: {new Date(s.last_failure).toLocaleString()}
                      </Text>
                    )}
                    {s.last_success && (
                      <Text type="secondary" style={{ marginLeft: 8 }}>
                        {t('dashboard.lastSuccess') || 'Last OK'}: {new Date(s.last_success).toLocaleString()}
                      </Text>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}
          {/* All sources as tags */}
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            {sources
              .sort((a, b) => (a.is_healthy === b.is_healthy ? 0 : a.is_healthy ? 1 : -1))
              .map((s) => (
              <Tag
                key={s.source_name}
                icon={
                  s.is_healthy ? (
                    <CheckCircleOutlined />
                  ) : (
                    <CloseCircleOutlined />
                  )
                }
                color={s.is_healthy ? 'success' : 'error'}
                title={s.is_healthy
                  ? `Fetched: ${s.total_items_fetched}`
                  : `Failures: ${s.consecutive_failures}, Last fail: ${s.last_failure || 'N/A'}`
                }
              >
                {s.source_name} {s.is_healthy ? `(${s.total_items_fetched})` : `(${s.consecutive_failures}x fail)`}
              </Tag>
            ))}
          </div>
        </Card>
      )}
    </div>
  )
}
