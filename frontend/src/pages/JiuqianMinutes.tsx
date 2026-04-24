/**
 * Jiuqian Minutes — research notes / roadshow transcripts (纪要)
 */
import { useEffect, useState, useCallback } from 'react'
import {
  Card, List, Tag, Select, Space, Input, Typography, Tooltip, Empty, Drawer, Divider,
} from 'antd'
import {
  ClockCircleOutlined, SearchOutlined, RiseOutlined, FallOutlined, FileTextOutlined,
} from '@ant-design/icons'
import api from '../services/api'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'

dayjs.extend(relativeTime)

import MarkdownRenderer from '../components/MarkdownRenderer'

const { Text, Paragraph } = Typography

interface MinutesItem {
  id: string
  platform: string | null
  source: string | null
  pub_time: string | null
  title: string
  summary: string | null
  author: string | null
  company: string[]
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

interface MinutesDetail extends MinutesItem {
  content: string
}

const SENTIMENT_MAP: Record<string, { color: string; label: string; icon: any }> = {
  bullish: { color: '#52c41a', label: '看多', icon: <RiseOutlined /> },
  bearish: { color: '#ff4d4f', label: '看空', icon: <FallOutlined /> },
  neutral: { color: '#d9d9d9', label: '中性', icon: null },
}

const HOURS_OPTIONS = [
  { value: 48, label: '48小时' },
  { value: 168, label: '7天' },
  { value: 720, label: '30天' },
]

export default function JiuqianMinutes() {
  const [items, setItems] = useState<MinutesItem[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const [hours, setHours] = useState<number>(168)
  const [searchSource, setSearchSource] = useState('')
  const [drawerItem, setDrawerItem] = useState<MinutesDetail | null>(null)
  const [drawerLoading, setDrawerLoading] = useState(false)

  const fetchData = useCallback(async () => {
    setLoading(true)
    try {
      const params: Record<string, any> = {
        page, page_size: 20, hours, min_relevance: 0.4,
      }
      if (searchSource) params.source = searchSource
      const res = await api.get('/jiuqian/minutes', { params })
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

  const openDetail = async (id: string) => {
    setDrawerLoading(true)
    setDrawerItem(null)
    try {
      const res = await api.get(`/jiuqian/minutes/${encodeURIComponent(id)}`)
      setDrawerItem(res.data)
    } catch (e) {
      console.error(e)
    } finally {
      setDrawerLoading(false)
    }
  }

  return (
    <div>
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap size="middle">
          <FileTextOutlined style={{ color: '#1890ff', fontSize: 16 }} />
          <Text strong>久谦纪要</Text>
          <Input
            prefix={<SearchOutlined />}
            placeholder="搜索来源..."
            value={searchSource}
            onChange={(e) => setSearchSource(e.target.value)}
            onPressEnter={() => fetchData()}
            allowClear
            style={{ width: 200 }}
          />
          <Select value={hours} onChange={setHours} style={{ width: 120 }} options={HOURS_OPTIONS} />
          <Text type="secondary">{total} 篇纪要</Text>
        </Space>
      </Card>

      <List
        loading={loading}
        dataSource={items}
        locale={{ emptyText: <Empty description="暂无纪要数据，系统正在持续处理中..." /> }}
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
                borderLeft: sentiment ? `3px solid ${sentiment.color}` : '3px solid #1890ff',
                cursor: 'pointer',
              }}
              hoverable
              onClick={() => openDetail(item.id)}
            >
              <div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                  <Tag color="blue" style={{ margin: 0 }}>纪要</Tag>
                  {sentiment && (
                    <Tag color={sentiment.color} icon={sentiment.icon} style={{ margin: 0 }}>
                      {sentiment.label}
                    </Tag>
                  )}
                  <span style={{ fontWeight: 600, fontSize: 14, flex: 1 }}>
                    {item.title.length > 80 ? item.title.slice(0, 80) + '...' : item.title}
                  </span>
                </div>

                {(enr.summary || item.summary) && (
                  <div style={{
                    background: '#f0f5ff', borderRadius: 4, padding: '8px 12px',
                    marginBottom: 8, fontSize: 13, color: '#1d39c4', lineHeight: 1.6,
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
                  {item.source && <Tag color="geekblue" style={{ fontSize: 11, margin: 0 }}>{item.source}</Tag>}
                  {item.company?.length > 0 && <span>{item.company.join(', ')}</span>}
                  {item.author && <span>{item.author}</span>}
                  {item.pub_time && (
                    <span>
                      <ClockCircleOutlined style={{ marginRight: 3 }} />
                      {dayjs(item.pub_time).format('MM-DD')}
                    </span>
                  )}
                  {enr.relevance_score != null && (
                    <Tag
                      color={enr.relevance_score >= 0.7 ? 'green' : enr.relevance_score >= 0.5 ? 'blue' : 'default'}
                      style={{ fontSize: 11, lineHeight: '18px', margin: 0 }}
                    >
                      {(enr.relevance_score * 100).toFixed(0)}%
                    </Tag>
                  )}
                </div>
              </div>
            </Card>
          )
        }}
      />

      <Drawer
        title="纪要详情"
        width={720}
        open={!!drawerItem}
        onClose={() => setDrawerItem(null)}
        loading={drawerLoading}
      >
        {drawerItem && (
          <div>
            <h3>{drawerItem.title}</h3>
            {drawerItem.source && <Tag color="geekblue">{drawerItem.source}</Tag>}
            {drawerItem.company?.length > 0 && drawerItem.company.map((c, i) => (
              <Tag key={i} color="orange">{c}</Tag>
            ))}
            <Divider />
            {drawerItem.enrichment?.summary && (
              <>
                <h4>AI 分析</h4>
                <Card size="small" style={{ background: '#f6ffed', marginBottom: 16 }}>
                  <p>{drawerItem.enrichment.summary}</p>
                </Card>
              </>
            )}
            {drawerItem.summary && (
              <>
                <h4>原文摘要</h4>
                <Paragraph>{drawerItem.summary}</Paragraph>
              </>
            )}
            <Divider />
            <h4>完整内容</h4>
            <div style={{ maxHeight: 600, overflow: 'auto', fontSize: 13, lineHeight: 1.8 }}>
              <MarkdownRenderer content={drawerItem.content} />
            </div>
          </div>
        )}
      </Drawer>
    </div>
  )
}
