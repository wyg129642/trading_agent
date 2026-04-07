/**
 * WeChat Articles page — shows filtered, enriched WeChat public account articles
 * with stock/sector tags, sentiment, AI summaries, and market impact scores.
 */
import { useEffect, useState, useCallback } from 'react'
import {
  Card,
  List,
  Tag,
  Select,
  Space,
  Input,
  Typography,
  Tooltip,
  Empty,
  Slider,
} from 'antd'
import {
  ClockCircleOutlined,
  LinkOutlined,
  RiseOutlined,
  FallOutlined,
  SearchOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import api from '../services/api'
import { useFavorites } from '../hooks/useFavorites'
import FavoriteButton from '../components/FavoriteButton'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'

dayjs.extend(relativeTime)

const { Text } = Typography

interface ArticleItem {
  arc_code: string
  arc_name: string
  author: string | null
  publish_time: string | null
  text_count: number
  url: string
  enrichment: {
    summary?: string
    relevance_score?: number
    market_impact_score?: number
    tickers?: string[]
    sectors?: string[]
    tags?: string[]
    sentiment?: string
    skipped?: boolean
  }
  is_enriched: boolean
}

interface ArticleResponse {
  items: ArticleItem[]
  total: number
  page: number
  page_size: number
  has_next: boolean
}

const SENTIMENT_MAP: Record<string, { color: string; label: string; icon: any }> = {
  bullish: { color: '#52c41a', label: '看多', icon: <RiseOutlined /> },
  bearish: { color: '#ff4d4f', label: '看空', icon: <FallOutlined /> },
  neutral: { color: '#d9d9d9', label: '中性', icon: null },
}

const HOURS_OPTIONS = [
  { value: 24, label: '24小时' },
  { value: 48, label: '48小时' },
  { value: 168, label: '7天' },
]

const SORT_OPTIONS = [
  { value: 'impact', label: '按影响力' },
  { value: 'time', label: '按时间' },
  { value: 'relevance', label: '按相关性' },
]

const IMPACT_COLORS: Record<string, string> = {
  high: '#ef4444',
  medium: '#f59e0b',
  low: '#94a3b8',
}

function getImpactLevel(score: number | undefined): { color: string; label: string } {
  if (score == null) return { color: '#d9d9d9', label: '' }
  if (score >= 8) return { color: '#ef4444', label: '高影响' }
  if (score >= 6) return { color: '#f59e0b', label: '中影响' }
  if (score >= 4) return { color: '#2563eb', label: '低影响' }
  return { color: '#94a3b8', label: '微影响' }
}

export default function AlphaPaiFeed() {
  const { t } = useTranslation()

  const { favoriteIds, toggleFavorite } = useFavorites('wechat')

  const [items, setItems] = useState<ArticleItem[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const [hours, setHours] = useState<number>(48)
  const [searchAuthor, setSearchAuthor] = useState('')
  const [sortBy, setSortBy] = useState<string>('impact')
  const [minImpact, setMinImpact] = useState<number>(5)

  const fetchArticles = useCallback(async () => {
    setLoading(true)
    try {
      const params: Record<string, any> = {
        page,
        page_size: 20,
        hours,
        min_relevance: 0.55,
        min_impact: minImpact,
        sort_by: sortBy,
      }
      if (searchAuthor) params.author = searchAuthor

      const res = await api.get<ArticleResponse>('/alphapai/wechat', { params })
      setItems(res.data.items)
      setTotal(res.data.total)
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }, [page, hours, searchAuthor, sortBy, minImpact])

  useEffect(() => {
    fetchArticles()
  }, [fetchArticles])

  useEffect(() => {
    setPage(1)
  }, [hours, searchAuthor, sortBy, minImpact])

  return (
    <div>
      {/* Filters */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap size="middle">
          <Input
            prefix={<SearchOutlined />}
            placeholder="搜索公众号作者..."
            value={searchAuthor}
            onChange={(e) => setSearchAuthor(e.target.value)}
            onPressEnter={() => fetchArticles()}
            allowClear
            style={{ width: 200 }}
          />
          <Select
            value={hours}
            onChange={setHours}
            style={{ width: 120 }}
            options={HOURS_OPTIONS}
          />
          <Select
            value={sortBy}
            onChange={setSortBy}
            style={{ width: 120 }}
            options={SORT_OPTIONS}
          />
          <Tooltip title="最低市场影响力评分 (0-10)">
            <Space size={4}>
              <ThunderboltOutlined style={{ color: '#f59e0b' }} />
              <Slider
                min={0}
                max={9}
                value={minImpact}
                onChange={setMinImpact}
                style={{ width: 100 }}
                tooltip={{ formatter: (v) => `≥${v}` }}
              />
              <Text type="secondary" style={{ fontSize: 12, minWidth: 20 }}>
                ≥{minImpact}
              </Text>
            </Space>
          </Tooltip>
          <Text type="secondary">
            {total} 篇高价值文章
          </Text>
        </Space>
      </Card>

      {/* Article List */}
      <List
        loading={loading}
        dataSource={items}
        locale={{
          emptyText: (
            <Empty description="暂无高价值公众号文章，系统正在持续分析中..." />
          ),
        }}
        pagination={{
          current: page,
          total,
          pageSize: 20,
          onChange: setPage,
          showSizeChanger: false,
        }}
        renderItem={(item) => {
          const enr = item.enrichment || {}
          const sentiment = SENTIMENT_MAP[enr.sentiment || '']
          const tickers = enr.tickers || []
          const sectors = enr.sectors || []
          const tags = enr.tags || []
          const impactScore = enr.market_impact_score
          const impact = getImpactLevel(impactScore)

          return (
            <Card
              size="small"
              style={{
                marginBottom: 10,
                borderLeft: sentiment
                  ? `3px solid ${sentiment.color}`
                  : '3px solid #e2e8f0',
              }}
              hoverable
            >
              <div>
                {/* Title row */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                  {sentiment && (
                    <Tag
                      color={sentiment.color}
                      icon={sentiment.icon}
                      style={{ margin: 0 }}
                    >
                      {sentiment.label}
                    </Tag>
                  )}
                  {impactScore != null && (
                    <Tooltip title={`市场影响力评分: ${impactScore}/10`}>
                      <Tag
                        icon={<ThunderboltOutlined />}
                        color={impact.color}
                        style={{ margin: 0 }}
                      >
                        {impactScore}
                      </Tag>
                    </Tooltip>
                  )}
                  <span style={{ fontWeight: 600, fontSize: 15, flex: 1 }}>
                    {item.arc_name}
                  </span>
                  {item.url && (
                    <a
                      href={item.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      onClick={(e) => e.stopPropagation()}
                      style={{ flexShrink: 0 }}
                    >
                      <Tag icon={<LinkOutlined />} color="blue">
                        原文
                      </Tag>
                    </a>
                  )}
                  <FavoriteButton
                    itemType="wechat"
                    itemId={item.arc_code}
                    favoriteIds={favoriteIds}
                    onToggle={toggleFavorite}
                  />
                </div>

                {/* AI Summary */}
                {enr.summary && (
                  <div
                    style={{
                      background: '#f8fafc',
                      borderRadius: 4,
                      padding: '8px 12px',
                      marginBottom: 8,
                      fontSize: 13,
                      color: '#475569',
                      lineHeight: 1.6,
                    }}
                  >
                    {enr.summary}
                  </div>
                )}

                {/* Stock & Sector Tags */}
                {(tickers.length > 0 || sectors.length > 0) && (
                  <div style={{ marginBottom: 6 }}>
                    {tickers.map((tk, i) => (
                      <Tag key={`t-${i}`} color="blue" style={{ fontSize: 12 }}>
                        {tk}
                      </Tag>
                    ))}
                    {sectors.map((s, i) => (
                      <Tag key={`s-${i}`} color="cyan" style={{ fontSize: 12 }}>
                        {s}
                      </Tag>
                    ))}
                    {tags.map((tag, i) => (
                      <Tag key={`tag-${i}`} style={{ fontSize: 11 }}>
                        {tag}
                      </Tag>
                    ))}
                  </div>
                )}

                {/* Meta row */}
                <div style={{ display: 'flex', gap: 12, fontSize: 12, color: '#8c8c8c' }}>
                  {item.author && <span>{item.author}</span>}
                  {item.publish_time && (
                    <span>
                      <ClockCircleOutlined style={{ marginRight: 3 }} />
                      {dayjs(item.publish_time).tz('Asia/Shanghai').fromNow()}
                    </span>
                  )}
                  {enr.relevance_score != null && enr.relevance_score > 0 && (
                    <Tooltip title={`AI相关性评分: ${(enr.relevance_score * 100).toFixed(0)}%`}>
                      <Tag
                        color={enr.relevance_score >= 0.7 ? 'green' : enr.relevance_score >= 0.5 ? 'blue' : 'default'}
                        style={{ fontSize: 11, lineHeight: '18px', margin: 0 }}
                      >
                        {(enr.relevance_score * 100).toFixed(0)}%
                      </Tag>
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
