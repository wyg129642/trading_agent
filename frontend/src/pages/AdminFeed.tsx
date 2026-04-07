import { useEffect, useState, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Card,
  List,
  Tag,
  Select,
  Space,
  Input,
  Segmented,
  Typography,
  Tooltip,
  Progress,
} from 'antd'
import { SearchOutlined, ClockCircleOutlined, WarningOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import api from '../services/api'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'

dayjs.extend(relativeTime)

const { Text } = Typography

interface NewsItem {
  id: string
  source_name: string
  title: string
  url: string
  published_at: string | null
  fetched_at: string
  sentiment: string | null
  impact_magnitude: string | null
  surprise_factor: number | null
  affected_tickers: string[]
  affected_sectors: string[]
  summary: string | null
  category: string | null
  is_read: boolean
  time_type: string
  is_relevant: boolean | null
  has_analysis: boolean
}

const SENTIMENT_COLORS: Record<string, string> = {
  very_bullish: '#059669',
  bullish: '#10b981',
  neutral: '#94a3b8',
  bearish: '#f59e0b',
  very_bearish: '#ef4444',
}

export default function AdminFeed() {
  const { t } = useTranslation()
  const navigate = useNavigate()

  const [items, setItems] = useState<NewsItem[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const [hours, setHours] = useState<number>(24)
  const [searchQuery, setSearchQuery] = useState('')
  const [statusFilter, setStatusFilter] = useState<string | undefined>()

  const fetchNews = useCallback(async () => {
    setLoading(true)
    try {
      const params: Record<string, any> = {
        page,
        page_size: 30,
        hours,
        unfiltered: true,
      }

      let res
      if (searchQuery) {
        res = await api.get('/news/search', { params: { q: searchQuery, page, page_size: 30 } })
      } else {
        res = await api.get('/news', { params })
      }

      let filtered = res.data.items
      if (statusFilter === 'analyzed') {
        filtered = filtered.filter((i: NewsItem) => i.has_analysis)
      } else if (statusFilter === 'not_analyzed') {
        filtered = filtered.filter((i: NewsItem) => !i.has_analysis)
      } else if (statusFilter === 'irrelevant') {
        filtered = filtered.filter((i: NewsItem) => i.is_relevant === false)
      }

      setItems(filtered)
      setTotal(res.data.total)
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }, [page, hours, searchQuery, statusFilter])

  useEffect(() => {
    fetchNews()
  }, [fetchNews])

  const renderStatusTag = (item: NewsItem) => {
    if (!item.has_analysis) {
      return (
        <Tag icon={<WarningOutlined />} color="default">
          {t('news.unprocessed')}
        </Tag>
      )
    }
    if (item.is_relevant === false) {
      return <Tag color="default">{t('news.irrelevant')}</Tag>
    }
    return null
  }

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <Typography.Title level={4} style={{ margin: 0, marginBottom: 4 }}>
          {t('admin.masterFeed')}
        </Typography.Title>
        <Text type="secondary" style={{ fontSize: 13 }}>
          {t('admin.masterFeedDesc')}
        </Text>
      </div>

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
            placeholder={t('admin.processingStatus')}
            value={statusFilter}
            onChange={setStatusFilter}
            allowClear
            style={{ width: 140 }}
            options={[
              { value: 'analyzed', label: t('admin.analyzed') },
              { value: 'not_analyzed', label: t('admin.notAnalyzed') },
              { value: 'irrelevant', label: t('admin.irrelevant') },
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
          <Text type="secondary">
            {total} {t('news.results')}
          </Text>
        </Space>
      </Card>

      <List
        loading={loading}
        dataSource={items}
        locale={{ emptyText: t('news.noData') }}
        pagination={{
          current: page,
          total,
          pageSize: 30,
          onChange: setPage,
          showSizeChanger: false,
        }}
        renderItem={(item) => (
          <Card
            size="small"
            className={`news-card ${item.sentiment ? `sentiment-${item.sentiment}` : ''} ${!item.is_read ? 'unread' : ''}`}
            onClick={() => navigate(`/news/${item.id}`)}
            hoverable
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <div style={{ flex: 1 }}>
                <Space size={4} style={{ marginBottom: 4 }}>
                  {renderStatusTag(item)}
                  {item.sentiment && (
                    <Tag className={`tag-${item.sentiment}`}>
                      {t(`sentiment.${item.sentiment}`)}
                    </Tag>
                  )}
                  {item.impact_magnitude && (
                    <Tag className={`tag-impact-${item.impact_magnitude}`}>
                      {t(`impact.${item.impact_magnitude}`)}
                    </Tag>
                  )}
                  {item.affected_tickers?.slice(0, 3).map((ticker) => (
                    <Tag key={ticker} color="blue" style={{ fontSize: 11 }}>
                      {ticker}
                    </Tag>
                  ))}
                </Space>

                <div style={{ fontWeight: 600, fontSize: 14, marginBottom: 4 }}>
                  {item.title}
                </div>

                {item.summary && (
                  <Text type="secondary" style={{ fontSize: 12.5 }}>
                    {item.summary.substring(0, 200)}
                    {item.summary.length > 200 ? '...' : ''}
                  </Text>
                )}

                <div style={{ marginTop: 6, display: 'flex', gap: 12, fontSize: 12, color: '#94a3b8' }}>
                  <span style={{ fontWeight: 500 }}>{item.source_name}</span>
                  <span>
                    <ClockCircleOutlined style={{ marginRight: 3 }} />
                    <span style={{ color: '#64748b' }}>
                      {item.time_type === 'published' ? t('news.publishedTime') : t('news.crawledTime')}
                    </span>
                    ：{dayjs(item.published_at || item.fetched_at).tz('Asia/Shanghai').format('MM-DD HH:mm')}
                  </span>
                </div>
              </div>

              {item.surprise_factor != null && (
                <Tooltip title={`${t('news.surprise')}: ${item.surprise_factor.toFixed(2)}`}>
                  <Progress
                    type="circle"
                    percent={Math.round(item.surprise_factor * 100)}
                    size={44}
                    strokeColor={
                      item.surprise_factor > 0.7
                        ? '#ef4444'
                        : item.surprise_factor > 0.4
                          ? '#f59e0b'
                          : '#10b981'
                    }
                    format={(p) => `${(p! / 100).toFixed(1)}`}
                  />
                </Tooltip>
              )}
            </div>
          </Card>
        )}
      />
    </div>
  )
}
