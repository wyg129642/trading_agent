/**
 * Comments page — shows filtered, enriched analyst comments
 * with stock/sector tags, sentiment, and AI summaries.
 */
import { useEffect, useState, useCallback } from 'react'
import {
  Card,
  List,
  Tag,
  Select,
  Space,
  Input,
  Switch,
  Typography,
  Tooltip,
  Empty,
} from 'antd'
import {
  SearchOutlined,
  ClockCircleOutlined,
  StarFilled,
  UserOutlined,
  BankOutlined,
  RiseOutlined,
  FallOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import api from '../services/api'
import { useFavorites } from '../hooks/useFavorites'
import FavoriteButton from '../components/FavoriteButton'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'

dayjs.extend(relativeTime)

const { Text } = Typography

interface CommentItem {
  cmnt_hcode: string
  title: string
  content: string
  psn_name: string | null
  team_cname: string | null
  inst_cname: string | null
  cmnt_date: string | null
  is_new_fortune: boolean
  enrichment: {
    summary?: string
    relevance_score?: number
    tickers?: string[]
    sectors?: string[]
    tags?: string[]
    sentiment?: string
  }
  is_enriched: boolean
}

interface CommentResponse {
  items: CommentItem[]
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

const SENTIMENT_FILTER_OPTIONS = [
  { value: '', label: '全部情绪' },
  { value: 'bullish', label: '看多' },
  { value: 'bearish', label: '看空' },
  { value: 'neutral', label: '中性' },
]

export default function AlphaPaiComments() {
  const { t } = useTranslation()

  const { favoriteIds, toggleFavorite } = useFavorites('comment')

  const [items, setItems] = useState<CommentItem[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)

  // Filters
  const [institution, setInstitution] = useState('')
  const [fortuneOnly, setFortuneOnly] = useState(false)
  const [hours, setHours] = useState<number>(24)
  const [sentimentFilter, setSentimentFilter] = useState('')

  const fetchComments = useCallback(async () => {
    setLoading(true)
    try {
      const params: Record<string, any> = {
        page,
        page_size: 20,
        hours,
        min_relevance: 0.4,
      }
      if (institution) params.institution = institution
      if (fortuneOnly) params.fortune_only = true
      if (sentimentFilter) params.sentiment = sentimentFilter

      const res = await api.get<CommentResponse>('/alphapai/comments', { params })
      setItems(res.data.items)
      setTotal(res.data.total)
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }, [page, institution, fortuneOnly, hours, sentimentFilter])

  useEffect(() => {
    fetchComments()
  }, [fetchComments])

  useEffect(() => {
    setPage(1)
  }, [institution, fortuneOnly, hours, sentimentFilter])

  return (
    <div>
      {/* Filters */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap size="middle">
          <Input
            prefix={<SearchOutlined />}
            placeholder="搜索机构..."
            value={institution}
            onChange={(e) => setInstitution(e.target.value)}
            onPressEnter={() => fetchComments()}
            allowClear
            style={{ width: 200 }}
          />
          <Select
            value={sentimentFilter}
            onChange={setSentimentFilter}
            style={{ width: 120 }}
            options={SENTIMENT_FILTER_OPTIONS}
          />
          <Space size={4}>
            <Text>新财富:</Text>
            <Switch
              checked={fortuneOnly}
              onChange={setFortuneOnly}
              checkedChildren="是"
              unCheckedChildren="否"
            />
          </Space>
          <Select
            value={hours}
            onChange={setHours}
            style={{ width: 120 }}
            options={HOURS_OPTIONS}
          />
          <Text type="secondary">
            {total} 条高价值点评
          </Text>
        </Space>
      </Card>

      {/* Comment List */}
      <List
        loading={loading}
        dataSource={items}
        locale={{
          emptyText: (
            <Empty description="暂无高价值点评，系统正在持续分析中..." />
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
                {/* Header tags */}
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                  <Space size={4} style={{ marginBottom: 6 }}>
                    <Tag color="orange">点评</Tag>
                    {item.is_new_fortune && (
                      <Tag color="gold" icon={<StarFilled />}>
                        新财富
                      </Tag>
                    )}
                    {sentiment && (
                      <Tag color={sentiment.color} icon={sentiment.icon}>
                        {sentiment.label}
                      </Tag>
                    )}
                  </Space>
                  <FavoriteButton
                    itemType="comment"
                    itemId={item.cmnt_hcode}
                    favoriteIds={favoriteIds}
                    onToggle={toggleFavorite}
                  />
                </div>

                {/* Title */}
                <div style={{ fontWeight: 600, fontSize: 15, marginBottom: 6 }}>
                  {item.title}
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
                  {item.psn_name && (
                    <span>
                      <UserOutlined style={{ marginRight: 3 }} />
                      {item.psn_name}
                    </span>
                  )}
                  {item.inst_cname && (
                    <span>
                      <BankOutlined style={{ marginRight: 3 }} />
                      {item.inst_cname}
                    </span>
                  )}
                  {item.cmnt_date && (
                    <span>
                      <ClockCircleOutlined style={{ marginRight: 3 }} />
                      {dayjs(item.cmnt_date).tz('Asia/Shanghai').fromNow()}
                    </span>
                  )}
                  {enr.relevance_score != null && enr.relevance_score > 0 && (
                    <Tooltip title={`AI评分: ${(enr.relevance_score * 100).toFixed(0)}%`}>
                      <Tag
                        color={enr.relevance_score >= 0.7 ? 'green' : 'default'}
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
