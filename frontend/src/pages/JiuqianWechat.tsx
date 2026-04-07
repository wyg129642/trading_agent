/**
 * Jiuqian WeChat — public account articles with strict filtering (min_relevance=0.6)
 */
import { useEffect, useState, useCallback } from 'react'
import {
  Card, List, Tag, Select, Space, Input, Typography, Tooltip, Empty,
} from 'antd'
import {
  ClockCircleOutlined, LinkOutlined, RiseOutlined, FallOutlined, SearchOutlined,
} from '@ant-design/icons'
import api from '../services/api'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'

dayjs.extend(relativeTime)

const { Text } = Typography

interface WechatItem {
  id: string
  platform: string | null
  source: string | null
  district: string | null
  pub_time: string | null
  title: string
  summary: string | null
  post_url: string
  enrichment: {
    summary?: string
    relevance_score?: number
    tickers?: string[]
    sectors?: string[]
    tags?: string[]
    sentiment?: string
    skipped?: boolean
  }
  is_enriched: boolean
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

export default function JiuqianWechat() {
  const [items, setItems] = useState<WechatItem[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const [hours, setHours] = useState<number>(48)
  const [searchSource, setSearchSource] = useState('')

  const fetchData = useCallback(async () => {
    setLoading(true)
    try {
      const params: Record<string, any> = {
        page, page_size: 20, hours, min_relevance: 0.6,
      }
      if (searchSource) params.source = searchSource
      const res = await api.get('/jiuqian/wechat', { params })
      setItems(res.data.items)
      setTotal(res.data.total)
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }, [page, hours, searchSource])

  useEffect(() => { fetchData() }, [fetchData])
  useEffect(() => { setPage(1) }, [hours, searchSource])

  return (
    <div>
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap size="middle">
          <Input
            prefix={<SearchOutlined />}
            placeholder="搜索公众号来源..."
            value={searchSource}
            onChange={(e) => setSearchSource(e.target.value)}
            onPressEnter={() => fetchData()}
            allowClear
            style={{ width: 200 }}
          />
          <Select value={hours} onChange={setHours} style={{ width: 120 }} options={HOURS_OPTIONS} />
          <Text type="secondary">
            {total} 篇精选文章 (门槛: 60%+)
          </Text>
        </Space>
      </Card>

      <List
        loading={loading}
        dataSource={items}
        locale={{ emptyText: <Empty description="暂无高价值公众号文章" /> }}
        pagination={{
          current: page, total, pageSize: 20,
          onChange: setPage, showSizeChanger: false,
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
                borderLeft: sentiment ? `3px solid ${sentiment.color}` : '3px solid #e2e8f0',
              }}
              hoverable
            >
              <div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                  {sentiment && (
                    <Tag color={sentiment.color} icon={sentiment.icon} style={{ margin: 0 }}>
                      {sentiment.label}
                    </Tag>
                  )}
                  <span style={{ fontWeight: 600, fontSize: 15, flex: 1 }}>{item.title}</span>
                  {item.post_url && (
                    <a href={item.post_url} target="_blank" rel="noopener noreferrer"
                       onClick={(e) => e.stopPropagation()} style={{ flexShrink: 0 }}>
                      <Tag icon={<LinkOutlined />} color="blue">原文</Tag>
                    </a>
                  )}
                </div>

                {(enr.summary || item.summary) && (
                  <div style={{
                    background: '#f8fafc', borderRadius: 4, padding: '8px 12px',
                    marginBottom: 8, fontSize: 13, color: '#475569', lineHeight: 1.6,
                  }}>
                    {enr.summary || (item.summary && item.summary.length > 200 ? item.summary.slice(0, 200) + '...' : item.summary)}
                  </div>
                )}

                {(tickers.length > 0 || sectors.length > 0) && (
                  <div style={{ marginBottom: 6 }}>
                    {tickers.map((tk, i) => (
                      <Tag key={`t-${i}`} color="blue" style={{ fontSize: 12 }}>{tk}</Tag>
                    ))}
                    {sectors.map((s, i) => (
                      <Tag key={`s-${i}`} color="cyan" style={{ fontSize: 12 }}>{s}</Tag>
                    ))}
                    {tags.map((tag, i) => (
                      <Tag key={`tag-${i}`} style={{ fontSize: 11 }}>{tag}</Tag>
                    ))}
                  </div>
                )}

                <div style={{ display: 'flex', gap: 12, fontSize: 12, color: '#8c8c8c' }}>
                  {item.source && <span>{item.source}</span>}
                  {item.pub_time && (
                    <span>
                      <ClockCircleOutlined style={{ marginRight: 3 }} />
                      {dayjs(item.pub_time).tz('Asia/Shanghai').fromNow()}
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
