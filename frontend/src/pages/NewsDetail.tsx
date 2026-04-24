import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  Card,
  Descriptions,
  Tag,
  Typography,
  Spin,
  Button,
  Collapse,
  Progress,
  Divider,
  Space,
  Row,
  Col,
  List,
  Timeline,
  Empty,
  Tooltip,
} from 'antd'
import {
  ArrowLeftOutlined,
  LinkOutlined,
  RiseOutlined,
  FallOutlined,
  BulbOutlined,
  ExperimentOutlined,
  ClockCircleOutlined,
  GlobalOutlined,
  SearchOutlined,
  FileSearchOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'
import { useFavorites } from '../hooks/useFavorites'
import FavoriteButton from '../components/FavoriteButton'
import MarkdownRenderer from '../components/MarkdownRenderer'
import { useAuthStore } from '../store/auth'

dayjs.extend(relativeTime)

const { Title, Text, Paragraph, Link } = Typography

/* ─── color constants ─── */

const SENTIMENT_COLORS: Record<string, string> = {
  very_bullish: '#52c41a',
  bullish: '#73d13d',
  neutral: '#d9d9d9',
  bearish: '#ff7a45',
  very_bearish: '#ff4d4f',
}

const IMPACT_COLORS: Record<string, string> = {
  critical: '#ff4d4f',
  high: '#fa8c16',
  medium: '#1677ff',
  low: '#8c8c8c',
}

const SOURCE_ENGINE_COLORS: Record<string, string> = {
  baidu: '#2932e1',
  tavily: '#6C5CE7',
  google: '#4285f4',
  duckduckgo: '#de5833',
}

/* ─── type definitions ─── */

interface TimelineItem {
  time: string
  source: string
  title: string
  url: string
}

interface CitationItem {
  title: string
  url: string
  snippet: string
  date: string
  source_engine: string
  website: string
  category: string
}

interface ReferencedSource {
  title: string
  url: string
  snippet: string
  source_engine: string
  relevance: string
}

interface DeepResearchData {
  citations: CitationItem[]
  news_timeline: TimelineItem[]
  referenced_sources: ReferencedSource[]
  search_queries: Record<string, string[]>
  total_iterations: number
  total_search_results: number
  total_fetched_pages: number
  fetched_urls: string[]
}

interface NewsDetailData {
  id: string
  source_name: string
  title: string
  url: string
  content: string
  published_at: string | null
  fetched_at: string
  language: string
  market: string
  metadata: Record<string, any>
  time_type: string | null
  filter_result: {
    is_relevant: boolean
    relevance_score: number
    reason: string
  } | null
  analysis: {
    sentiment: string
    impact_magnitude: string
    impact_timeframe: string
    affected_tickers: string[]
    affected_sectors: string[]
    ticker_sentiments: Record<string, any>
    sector_sentiments: Record<string, any>
    category: string
    summary: string
    key_facts: string[]
    bull_case: string
    bear_case: string
    surprise_factor: number
    market_expectation: string
    analyzed_at: string
    model_used: string
  } | null
  research: {
    executive_summary: string
    context: string
    bull_scenario: string
    bear_scenario: string
    recommended_actions: string
    confidence: number
    full_report: string
    deep_research_data: DeepResearchData | null
    researched_at: string
    model_used: string
  } | null
}

/* ─── Multi-horizon types & normalization ─── */

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

/* ─── CSS class helpers for semantic tags ─── */

const sentimentClass = (s: string): string => {
  if (s === 'very_bullish' || s === 'bullish') return 'tag-bullish'
  if (s === 'very_bearish' || s === 'bearish') return 'tag-bearish'
  return ''
}

const impactClass = (m: string): string => {
  const map: Record<string, string> = {
    critical: 'tag-impact-critical',
    high: 'tag-impact-high',
    medium: 'tag-impact-medium',
    low: 'tag-impact-low',
  }
  return map[m] || ''
}

/* ─── sub-components ─── */

function NewsTimelineSection({ timeline }: { timeline: TimelineItem[] }) {
  const { t } = useTranslation()
  if (!timeline || timeline.length === 0) return null

  return (
    <Card
      size="small"
      style={{ marginBottom: 16 }}
      title={
        <span>
          <ClockCircleOutlined style={{ marginRight: 8 }} />
          {t('news.newsTimeline')}
        </span>
      }
    >
      <Text type="secondary" style={{ display: 'block', marginBottom: 12, fontSize: 13 }}>
        {t('news.timelineDesc')}
      </Text>
      <Timeline
        items={timeline.map((item, idx) => ({
          key: idx,
          color: idx === 0 ? 'green' : 'blue',
          children: (
            <div>
              <Text strong style={{ fontSize: 13 }}>
                {item.time || t('news.unknownTime')}
              </Text>
              <Text type="secondary" style={{ marginLeft: 8, fontSize: 12 }}>
                {item.source}
              </Text>
              <div style={{ marginTop: 2 }}>
                {item.url ? (
                  <a
                    href={item.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{ fontSize: 13 }}
                  >
                    {item.title} <LinkOutlined />
                  </a>
                ) : (
                  <Text style={{ fontSize: 13 }}>{item.title}</Text>
                )}
              </div>
            </div>
          ),
        }))}
      />
    </Card>
  )
}

function ReferencedSourcesSection({ sources }: { sources: ReferencedSource[] }) {
  const { t } = useTranslation()
  if (!sources || sources.length === 0) return null

  return (
    <Card
      size="small"
      style={{ marginBottom: 16 }}
      title={
        <span>
          <FileSearchOutlined style={{ marginRight: 8 }} />
          {t('news.referencedSources')}
        </span>
      }
    >
      <Text type="secondary" style={{ display: 'block', marginBottom: 12, fontSize: 13 }}>
        {t('news.referencedSourcesDesc')}
      </Text>
      <List
        size="small"
        dataSource={sources}
        renderItem={(item) => (
          <List.Item style={{ padding: '8px 0', borderBottom: '1px solid #f0f0f0' }}>
            <div style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                <Tag
                  color={SOURCE_ENGINE_COLORS[item.source_engine] || '#666'}
                  style={{ fontSize: 11, margin: 0 }}
                >
                  {item.source_engine}
                </Tag>
                <a
                  href={item.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{ fontWeight: 500, fontSize: 13, flex: 1 }}
                >
                  {item.title} <LinkOutlined />
                </a>
              </div>
              {item.snippet && (
                <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 2 }}>
                  {item.snippet}
                </Text>
              )}
              {item.relevance && (
                <Text
                  style={{
                    fontSize: 12,
                    color: '#1677ff',
                    display: 'block',
                    fontStyle: 'italic',
                  }}
                >
                  {item.relevance}
                </Text>
              )}
            </div>
          </List.Item>
        )}
      />
    </Card>
  )
}

function CitationsSection({ citations }: { citations: CitationItem[] }) {
  const { t } = useTranslation()
  if (!citations || citations.length === 0) return null

  // Group by category
  const groups: Record<string, CitationItem[]> = {}
  citations.forEach((c) => {
    const cat = c.category || 'other'
    if (!groups[cat]) groups[cat] = []
    groups[cat].push(c)
  })

  const categoryLabels: Record<string, string> = {
    news_coverage: t('news.catNewsCoverage'),
    historical_impact: t('news.catHistoricalImpact'),
    stock_performance: t('news.catStockPerformance'),
    stock_info: t('news.catStockPerformance'),
    supplementary: t('news.catSupplementary'),
    other: t('news.catOther'),
  }

  return (
    <Card
      size="small"
      style={{ marginBottom: 16 }}
      title={
        <span>
          <GlobalOutlined style={{ marginRight: 8 }} />
          {t('news.allCitations')} ({citations.length})
        </span>
      }
    >
      {Object.entries(groups).map(([category, items]) => (
        <div key={category} style={{ marginBottom: 16 }}>
          <Divider orientation="left" plain style={{ fontSize: 13, margin: '8px 0' }}>
            {categoryLabels[category] || category}
          </Divider>
          {items.slice(0, 8).map((item, idx) => (
            <div
              key={idx}
              style={{
                padding: '6px 0',
                borderBottom: idx < items.length - 1 ? '1px solid #f5f5f5' : 'none',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <Tag
                  color={SOURCE_ENGINE_COLORS[item.source_engine] || '#666'}
                  style={{ fontSize: 10, margin: 0, padding: '0 4px' }}
                >
                  {item.source_engine}
                </Tag>
                {item.website && (
                  <Text type="secondary" style={{ fontSize: 11 }}>
                    {item.website}
                  </Text>
                )}
                {item.date && (
                  <Text type="secondary" style={{ fontSize: 11 }}>
                    {item.date}
                  </Text>
                )}
              </div>
              <a
                href={item.url}
                target="_blank"
                rel="noopener noreferrer"
                style={{ fontSize: 13, display: 'block', marginTop: 2 }}
              >
                {item.title} <LinkOutlined />
              </a>
              {item.snippet && (
                <Text
                  type="secondary"
                  style={{
                    fontSize: 12,
                    display: '-webkit-box',
                    WebkitLineClamp: 2,
                    WebkitBoxOrient: 'vertical',
                    overflow: 'hidden',
                    marginTop: 2,
                  }}
                >
                  {item.snippet}
                </Text>
              )}
            </div>
          ))}
        </div>
      ))}
    </Card>
  )
}

function SearchQueriesSection({ queries }: { queries: Record<string, string[]> }) {
  const { t } = useTranslation()
  if (!queries || Object.keys(queries).length === 0) return null

  return (
    <div style={{ marginBottom: 16 }}>
      <Text strong style={{ fontSize: 13, display: 'block', marginBottom: 8 }}>
        <SearchOutlined style={{ marginRight: 4 }} />
        {t('news.searchQueriesUsed')}
      </Text>
      {Object.entries(queries).map(([engine, qs]) => (
        <div key={engine} style={{ marginBottom: 8 }}>
          <Tag color={SOURCE_ENGINE_COLORS[engine] || '#666'} style={{ marginBottom: 4 }}>
            {engine.toUpperCase()}
          </Tag>
          <div style={{ paddingLeft: 8 }}>
            {qs.map((q, idx) => (
              <Text
                key={idx}
                type="secondary"
                style={{ display: 'block', fontSize: 12, lineHeight: 1.6 }}
              >
                {idx + 1}. {q}
              </Text>
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}

/* ─── main component ─── */

export default function NewsDetailPage() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const { t } = useTranslation()
  const [news, setNews] = useState<NewsDetailData | null>(null)
  const [loading, setLoading] = useState(true)
  const { favoriteIds, toggleFavorite } = useFavorites('news')
  const user = useAuthStore((s) => s.user)
  const isAdmin = user?.role === 'admin'

  useEffect(() => {
    if (!id) return

    api
      .get(`/news/${id}`)
      .then((res) => setNews(res.data))
      .catch(console.error)
      .finally(() => setLoading(false))

    // Mark as read (fire and forget)
    api.post(`/news/${id}/read`).catch(() => {})
  }, [id])

  if (loading) {
    return (
      <div style={{ textAlign: 'center', padding: 100 }}>
        <Spin size="large" />
      </div>
    )
  }

  if (!news) {
    return <div style={{ textAlign: 'center', padding: 100 }}>Not found</div>
  }

  const a = news.analysis
  const r = news.research
  const f = news.filter_result

  // Parse deep research data
  let drd: DeepResearchData | null = null
  if (r?.deep_research_data && typeof r.deep_research_data === 'object') {
    drd = r.deep_research_data as DeepResearchData
  }
  // Fallback: try parsing from full_report if deep_research_data is empty
  if (
    (!drd || !drd.citations || drd.citations.length === 0) &&
    r?.full_report
  ) {
    try {
      const parsed = JSON.parse(r.full_report)
      if (parsed && parsed.citations) {
        drd = parsed as DeepResearchData
      }
    } catch {
      // ignore
    }
  }

  // Determine the timestamp and its label based on time_type
  const timestamp = news.published_at || news.fetched_at
  const timeLabel =
    news.time_type === 'crawled' || !news.published_at
      ? t('news.crawledTime')
      : t('news.publishedTime')

  return (
    <div style={{ maxWidth: 1000, margin: '0 auto' }}>
      {/* ─── Back button ─── */}
      <Button
        type="text"
        icon={<ArrowLeftOutlined />}
        onClick={() => navigate(-1)}
        style={{ marginBottom: 16, paddingLeft: 0 }}
      >
        {t('news.back')}
      </Button>

      {/* ─── Header card ─── */}
      <Card style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'flex-start', gap: 12 }}>
          <div style={{ flex: 1 }}>
            <Title level={4} style={{ marginBottom: 4 }}>
              {news.metadata?.title_zh && news.metadata.title_zh !== news.title
                ? <>
                    {news.metadata.title_zh}
                    <span style={{ fontSize: 14, fontWeight: 400, color: '#8c8c8c', marginLeft: 8 }}>
                      （{news.title}）
                    </span>
                  </>
                : news.title
              }
            </Title>
          </div>
          <FavoriteButton
            itemType="news"
            itemId={news.id}
            favoriteIds={favoriteIds}
            onToggle={toggleFavorite}
          />
        </div>
        <Space size="middle" wrap>
          <Text type="secondary">{news.source_name}</Text>
          <Text type="secondary">
            {timeLabel}：{dayjs(timestamp).tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm')}
            {' ('}
            {dayjs(timestamp).tz('Asia/Shanghai').fromNow()}
            {')'}
          </Text>
          {news.url && (
            <a href={news.url} target="_blank" rel="noopener noreferrer">
              <LinkOutlined /> {t('news.original')}
            </a>
          )}
        </Space>
      </Card>

      {/* ─── Analysis section ─── */}
      {a && (
        <>
          {/* Core metrics + Surprise gauge */}
          <Row gutter={16} style={{ marginBottom: 16 }}>
            <Col xs={24} md={14}>
              <Card size="small" title={t('news.coreMetrics')} style={{ height: '100%' }}>
                <Descriptions column={1} size="small">
                  {(() => {
                    const ts = a.ticker_sentiments || {}
                    const ss = a.sector_sentiments || {}
                    const hasTicker = Object.keys(ts).length > 0
                    const hasSector = Object.keys(ss).length > 0
                    if (hasTicker) {
                      return (
                        <Descriptions.Item label={t('news.tickerSentiments')}>
                          {Object.entries(ts).map(([ticker, val]) => {
                            const data = normalizeSentiment(val)
                            return (
                              <div key={ticker} style={{ marginBottom: 12 }}>
                                <Text strong style={{ fontSize: 13, display: 'block', marginBottom: 4 }}>{ticker}</Text>
                                {[
                                  { label: t('news.shortTerm'), data: data.short_term },
                                  { label: t('news.mediumTerm'), data: data.medium_term },
                                  { label: t('news.longTerm'), data: data.long_term },
                                ].map(({ label, data: hp }) => (
                                  <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 2 }}>
                                    <Text type="secondary" style={{ fontSize: 12, width: 80 }}>{label}</Text>
                                    <Tag color={SENTIMENT_COLORS[hp.sentiment]} style={{ margin: 0, fontSize: 11 }}>
                                      {t(`sentiment.${hp.sentiment}`)}
                                    </Tag>
                                    <Text style={{ fontSize: 12, fontFamily: 'monospace', width: 40 }}>
                                      {hp.sentiment_score > 0 ? '+' : ''}{hp.sentiment_score.toFixed(2)}
                                    </Text>
                                    <Progress
                                      percent={Math.round(hp.confidence * 100)}
                                      size="small"
                                      strokeColor={hp.confidence >= 0.7 ? '#52c41a' : hp.confidence >= 0.4 ? '#faad14' : '#ff4d4f'}
                                      style={{ width: 80, margin: 0 }}
                                      format={(p) => `${p}%`}
                                    />
                                  </div>
                                ))}
                                {data.reason && (
                                  <Text type="secondary" style={{ fontSize: 12, display: 'block', marginTop: 2 }}>{data.reason}</Text>
                                )}
                              </div>
                            )
                          })}
                        </Descriptions.Item>
                      )
                    }
                    if (hasSector) {
                      return (
                        <Descriptions.Item label={t('news.sectorSentiments')}>
                          {Object.entries(ss).map(([sector, val]) => {
                            const data = normalizeSentiment(val)
                            return (
                              <div key={sector} style={{ marginBottom: 12 }}>
                                <Text strong style={{ fontSize: 13, display: 'block', marginBottom: 4 }}>{sector}</Text>
                                {[
                                  { label: t('news.shortTerm'), data: data.short_term },
                                  { label: t('news.mediumTerm'), data: data.medium_term },
                                  { label: t('news.longTerm'), data: data.long_term },
                                ].map(({ label, data: hp }) => (
                                  <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 2 }}>
                                    <Text type="secondary" style={{ fontSize: 12, width: 80 }}>{label}</Text>
                                    <Tag color={SENTIMENT_COLORS[hp.sentiment]} style={{ margin: 0, fontSize: 11 }}>
                                      {t(`sentiment.${hp.sentiment}`)}
                                    </Tag>
                                    <Text style={{ fontSize: 12, fontFamily: 'monospace', width: 40 }}>
                                      {hp.sentiment_score > 0 ? '+' : ''}{hp.sentiment_score.toFixed(2)}
                                    </Text>
                                    <Progress
                                      percent={Math.round(hp.confidence * 100)}
                                      size="small"
                                      strokeColor={hp.confidence >= 0.7 ? '#52c41a' : hp.confidence >= 0.4 ? '#faad14' : '#ff4d4f'}
                                      style={{ width: 80, margin: 0 }}
                                      format={(p) => `${p}%`}
                                    />
                                  </div>
                                ))}
                                {data.reason && (
                                  <Text type="secondary" style={{ fontSize: 12, display: 'block', marginTop: 2 }}>{data.reason}</Text>
                                )}
                              </div>
                            )
                          })}
                        </Descriptions.Item>
                      )
                    }
                    return (
                      <Descriptions.Item label={t('news.sentiment')}>
                        <Tag
                          color={SENTIMENT_COLORS[a.sentiment]}
                          className={sentimentClass(a.sentiment)}
                        >
                          {t(`sentiment.${a.sentiment}`)}
                        </Tag>
                      </Descriptions.Item>
                    )
                  })()}
                  <Descriptions.Item label={t('news.impact')}>
                    <Tag
                      color={IMPACT_COLORS[a.impact_magnitude]}
                      className={impactClass(a.impact_magnitude)}
                    >
                      {t(`impact.${a.impact_magnitude}`)}
                    </Tag>
                    <Text type="secondary" style={{ marginLeft: 8 }}>
                      {a.impact_timeframe}
                    </Text>
                  </Descriptions.Item>
                  <Descriptions.Item label={t('news.category')}>
                    <Tag>{a.category}</Tag>
                  </Descriptions.Item>
                  <Descriptions.Item label={t('news.model')}>
                    <Text code>{a.model_used}</Text>
                  </Descriptions.Item>
                </Descriptions>
              </Card>
            </Col>
            <Col xs={24} md={10}>
              <Card size="small" title={t('news.surprise')} style={{ height: '100%' }}>
                <div style={{ textAlign: 'center', paddingTop: 8 }}>
                  <Progress
                    type="dashboard"
                    percent={Math.round(a.surprise_factor * 100)}
                    strokeColor={
                      a.surprise_factor > 0.7
                        ? '#ff4d4f'
                        : a.surprise_factor > 0.4
                          ? '#fa8c16'
                          : '#52c41a'
                    }
                    format={(p) => `${(p! / 100).toFixed(2)}`}
                  />
                  {a.market_expectation && (
                    <div style={{ marginTop: 8 }}>
                      <Text type="secondary" strong>
                        {t('news.marketExpectation')}
                      </Text>
                      <Paragraph
                        type="secondary"
                        style={{ marginTop: 4, marginBottom: 0, fontSize: 13 }}
                      >
                        {a.market_expectation}
                      </Paragraph>
                    </div>
                  )}
                </div>
              </Card>
            </Col>
          </Row>

          {/* Related tickers & sectors with per-item sentiment */}
          {(a.affected_tickers?.length > 0 || a.affected_sectors?.length > 0) && (
            <Card size="small" style={{ marginBottom: 16 }}>
              <Space wrap size={[8, 8]}>
                {a.affected_tickers?.length > 0 && (
                  <>
                    <Text type="secondary" style={{ fontSize: 13 }}>
                      {t('news.relatedTickers')}:
                    </Text>
                    {a.affected_tickers.map((ticker) => {
                      const ts = a.ticker_sentiments || {}
                      // Try exact match, then partial match
                      let rawVal = ts[ticker]
                      if (!rawVal) {
                        for (const [k, v] of Object.entries(ts)) {
                          if (k.includes(ticker) || ticker.includes(k)) { rawVal = v; break }
                        }
                      }
                      if (rawVal) {
                        const data = normalizeSentiment(rawVal)
                        const primary = data.short_term
                        return (
                          <Tooltip
                            key={ticker}
                            title={
                              <div style={{ fontSize: 12 }}>
                                <div>{t('news.shortTerm')}: {t(`sentiment.${primary.sentiment}`)} ({(primary.confidence * 100).toFixed(0)}%)</div>
                                <div>{t('news.mediumTerm')}: {t(`sentiment.${data.medium_term.sentiment}`)} ({(data.medium_term.confidence * 100).toFixed(0)}%)</div>
                                <div>{t('news.longTerm')}: {t(`sentiment.${data.long_term.sentiment}`)} ({(data.long_term.confidence * 100).toFixed(0)}%)</div>
                                {data.reason && <div style={{ marginTop: 4, opacity: 0.8 }}>{data.reason}</div>}
                              </div>
                            }
                          >
                            <Tag color={SENTIMENT_COLORS[primary.sentiment]} className={sentimentClass(primary.sentiment)}>
                              {ticker} {t(`sentiment.${primary.sentiment}`)}
                              <span style={{ opacity: 0.7, marginLeft: 4, fontSize: 11 }}>
                                {(primary.confidence * 100).toFixed(0)}%
                              </span>
                            </Tag>
                          </Tooltip>
                        )
                      }
                      return (
                        <Tag key={ticker} color="blue">{ticker}</Tag>
                      )
                    })}
                  </>
                )}
                {a.affected_sectors?.length > 0 && (
                  <>
                    <Text type="secondary" style={{ fontSize: 13, marginLeft: 8 }}>
                      {t('news.relatedSectors')}:
                    </Text>
                    {a.affected_sectors.map((sector) => {
                      const ss = a.sector_sentiments || {}
                      const rawVal = ss[sector]
                      if (rawVal) {
                        const data = normalizeSentiment(rawVal)
                        const primary = data.short_term
                        return (
                          <Tooltip
                            key={sector}
                            title={
                              <div style={{ fontSize: 12 }}>
                                <div>{t('news.shortTerm')}: {t(`sentiment.${primary.sentiment}`)} ({(primary.confidence * 100).toFixed(0)}%)</div>
                                <div>{t('news.mediumTerm')}: {t(`sentiment.${data.medium_term.sentiment}`)} ({(data.medium_term.confidence * 100).toFixed(0)}%)</div>
                                <div>{t('news.longTerm')}: {t(`sentiment.${data.long_term.sentiment}`)} ({(data.long_term.confidence * 100).toFixed(0)}%)</div>
                                {data.reason && <div style={{ marginTop: 4, opacity: 0.8 }}>{data.reason}</div>}
                              </div>
                            }
                          >
                            <Tag color={SENTIMENT_COLORS[primary.sentiment]} className={sentimentClass(primary.sentiment)}>
                              {sector} {t(`sentiment.${primary.sentiment}`)}
                              <span style={{ opacity: 0.7, marginLeft: 4, fontSize: 11 }}>
                                {(primary.confidence * 100).toFixed(0)}%
                              </span>
                            </Tag>
                          </Tooltip>
                        )
                      }
                      return (
                        <Tag key={sector} color="purple">{sector}</Tag>
                      )
                    })}
                  </>
                )}
              </Space>
            </Card>
          )}

          {/* Summary + Key facts */}
          {a.summary && (
            <Card size="small" title={t('news.summary')} style={{ marginBottom: 16 }}>
              <div style={{ marginBottom: a.key_facts?.length > 0 ? 0 : undefined }}>
                <MarkdownRenderer content={a.summary} />
              </div>
              {a.key_facts?.length > 0 && (
                <>
                  <Divider orientation="left" plain style={{ fontSize: 13 }}>
                    {t('news.keyFacts')}
                  </Divider>
                  <List
                    size="small"
                    dataSource={a.key_facts}
                    renderItem={(fact) => (
                      <List.Item style={{ padding: '4px 0', borderBottom: 'none' }}>
                        <Text>&#8226; {fact}</Text>
                      </List.Item>
                    )}
                  />
                </>
              )}
            </Card>
          )}

          {/* Bull / Bear cases */}
          {(a.bull_case || a.bear_case) && (
            <Row gutter={16} style={{ marginBottom: 16 }}>
              <Col xs={24} md={12}>
                <Card
                  size="small"
                  title={
                    <span style={{ color: '#52c41a' }}>
                      <RiseOutlined /> {t('news.bullCase')}
                    </span>
                  }
                  style={{ height: '100%' }}
                  headStyle={{ borderBottom: '2px solid #52c41a' }}
                >
                  <Paragraph style={{ marginBottom: 0 }}>{a.bull_case || '-'}</Paragraph>
                </Card>
              </Col>
              <Col xs={24} md={12}>
                <Card
                  size="small"
                  title={
                    <span style={{ color: '#ff4d4f' }}>
                      <FallOutlined /> {t('news.bearCase')}
                    </span>
                  }
                  style={{ height: '100%' }}
                  headStyle={{ borderBottom: '2px solid #ff4d4f' }}
                >
                  <Paragraph style={{ marginBottom: 0 }}>{a.bear_case || '-'}</Paragraph>
                </Card>
              </Col>
            </Row>
          )}
        </>
      )}

      {/* ─── Research section ─── */}
      {r && (
        <Card
          size="small"
          style={{ marginBottom: 16 }}
          title={
            <span>
              <ExperimentOutlined style={{ marginRight: 8 }} />
              {t('news.deepResearch')}
              {drd && (
                <Text type="secondary" style={{ marginLeft: 12, fontSize: 12, fontWeight: 400 }}>
                  {drd.total_iterations} {t('news.iterations')} | {drd.total_search_results}{' '}
                  {t('news.searchResults')} | {drd.total_fetched_pages} {t('news.pagesFetched')}
                </Text>
              )}
            </span>
          }
        >
          {r.executive_summary && (
            <div style={{ marginBottom: 16 }}>
              <Text strong style={{ display: 'block', marginBottom: 4 }}>
                {t('news.executiveSummary')}
              </Text>
              <MarkdownRenderer content={r.executive_summary} />
            </div>
          )}

          {r.recommended_actions && (
            <div style={{ marginBottom: 16 }}>
              <Text strong style={{ display: 'block', marginBottom: 4 }}>
                <BulbOutlined style={{ marginRight: 4 }} />
                {t('news.recommendedActions')}
              </Text>
              <MarkdownRenderer content={r.recommended_actions} />
            </div>
          )}

          <Descriptions column={{ xs: 1, sm: 2 }} size="small" style={{ marginTop: 8 }}>
            <Descriptions.Item label={t('news.confidence')}>
              <Progress
                percent={Math.round(r.confidence * 100)}
                size="small"
                style={{ width: 160, margin: 0 }}
                strokeColor={
                  r.confidence >= 0.7
                    ? '#52c41a'
                    : r.confidence >= 0.4
                      ? '#fa8c16'
                      : '#ff4d4f'
                }
              />
            </Descriptions.Item>
            <Descriptions.Item label={t('news.model')}>
              <Text code>{r.model_used}</Text>
            </Descriptions.Item>
          </Descriptions>

          {/* Search queries used */}
          {drd?.search_queries && (
            <div style={{ marginTop: 16 }}>
              <SearchQueriesSection queries={drd.search_queries} />
            </div>
          )}
        </Card>
      )}

      {/* ─── News Timeline ─── */}
      {drd?.news_timeline && drd.news_timeline.length > 0 && (
        <NewsTimelineSection timeline={drd.news_timeline} />
      )}

      {/* ─── Referenced Sources (most valuable) ─── */}
      {drd?.referenced_sources && drd.referenced_sources.length > 0 && (
        <ReferencedSourcesSection sources={drd.referenced_sources} />
      )}

      {/* ─── All Citations ─── */}
      {drd?.citations && drd.citations.length > 0 && (
        <CitationsSection citations={drd.citations} />
      )}

      {/* ─── Pipeline trace (admin only) ─── */}
      {isAdmin && (
        <Collapse
          style={{ marginBottom: 24 }}
          items={[
            ...(f
              ? [
                  {
                    key: 'phase1',
                    label: `${t('news.phase1')} (${t('news.phase1Score')}: ${f.relevance_score.toFixed(2)})`,
                    children: (
                      <Descriptions column={1} size="small">
                        <Descriptions.Item label={t('news.phase1Relevant')}>
                          <Tag color={f.is_relevant ? 'green' : 'default'}>
                            {f.is_relevant ? t('news.yes') : t('news.no')}
                          </Tag>
                        </Descriptions.Item>
                        <Descriptions.Item label={t('news.phase1Score')}>
                          {f.relevance_score.toFixed(2)}
                        </Descriptions.Item>
                        <Descriptions.Item label={t('news.phase1Reason')}>
                          {f.reason}
                        </Descriptions.Item>
                      </Descriptions>
                    ),
                  },
                ]
              : []),
            ...(news.content
              ? [
                  {
                    key: 'content',
                    label: `${t('news.articleContent')} (${news.content.length.toLocaleString()} ${t('news.chars')})`,
                    children: (
                      <div
                        style={{
                          maxHeight: 400,
                          overflow: 'auto',
                          marginBottom: 0,
                          fontSize: 13,
                          lineHeight: 1.8,
                        }}
                      >
                        <MarkdownRenderer content={news.content} />
                      </div>
                    ),
                  },
                ]
              : []),
          ]}
        />
      )}
    </div>
  )
}
