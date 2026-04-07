/**
 * Jiuqian Forum — expert call transcripts (高价值专家调研)
 */
import { useEffect, useState, useCallback } from 'react'
import {
  Card, List, Tag, Select, Space, Input, Typography, Tooltip, Empty, Drawer, Divider,
} from 'antd'
import {
  ClockCircleOutlined, SearchOutlined, RiseOutlined, FallOutlined, BulbOutlined,
} from '@ant-design/icons'
import api from '../services/api'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'

dayjs.extend(relativeTime)

const { Text, Paragraph } = Typography

interface ForumItem {
  id: number
  industry: string | null
  related_targets: string | null
  title: string
  author: string | null
  expert_information: string | null
  summary: string | null
  meeting_time: string | null
  enrichment: {
    summary?: string
    relevance_score?: number
    tickers?: string[]
    sectors?: string[]
    tags?: string[]
    sentiment?: string
    key_points?: string[]
  }
  is_enriched: boolean
}

interface ForumDetail extends ForumItem {
  topic: string | null
  content: string
  insight: string | null
}

const SENTIMENT_MAP: Record<string, { color: string; label: string; icon: any }> = {
  bullish: { color: '#52c41a', label: '看多', icon: <RiseOutlined /> },
  bearish: { color: '#ff4d4f', label: '看空', icon: <FallOutlined /> },
  neutral: { color: '#d9d9d9', label: '中性', icon: null },
}

export default function JiuqianForum() {
  const [items, setItems] = useState<ForumItem[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const [searchIndustry, setSearchIndustry] = useState('')
  const [drawerItem, setDrawerItem] = useState<ForumDetail | null>(null)
  const [drawerLoading, setDrawerLoading] = useState(false)

  const fetchData = useCallback(async () => {
    setLoading(true)
    try {
      const params: Record<string, any> = {
        page, page_size: 20, min_relevance: 0.3,
      }
      if (searchIndustry) params.industry = searchIndustry
      const res = await api.get('/jiuqian/forum', { params })
      setItems(res.data.items)
      setTotal(res.data.total)
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }, [page, searchIndustry])

  useEffect(() => { fetchData() }, [fetchData])
  useEffect(() => { setPage(1) }, [searchIndustry])

  const openDetail = async (id: number) => {
    setDrawerLoading(true)
    setDrawerItem(null)
    try {
      const res = await api.get(`/jiuqian/forum/${id}`)
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
          <BulbOutlined style={{ color: '#faad14', fontSize: 16 }} />
          <Text strong>久谦专家调研</Text>
          <Input
            prefix={<SearchOutlined />}
            placeholder="搜索行业..."
            value={searchIndustry}
            onChange={(e) => setSearchIndustry(e.target.value)}
            onPressEnter={() => fetchData()}
            allowClear
            style={{ width: 200 }}
          />
          <Text type="secondary">{total} 篇专家调研</Text>
        </Space>
      </Card>

      <List
        loading={loading}
        dataSource={items}
        locale={{ emptyText: <Empty description="暂无专家调研数据" /> }}
        pagination={{
          current: page, total, pageSize: 20,
          onChange: setPage, showSizeChanger: false,
        }}
        renderItem={(item) => {
          const enr = item.enrichment || {}
          const sentiment = SENTIMENT_MAP[enr.sentiment || '']
          const tickers = enr.tickers || []
          const sectors = enr.sectors || []
          const keyPoints = enr.key_points || []

          return (
            <Card
              size="small"
              style={{
                marginBottom: 10,
                borderLeft: sentiment ? `3px solid ${sentiment.color}` : '3px solid #faad14',
                cursor: 'pointer',
              }}
              hoverable
              onClick={() => openDetail(item.id)}
            >
              <div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                  <Tag color="gold" style={{ margin: 0 }}>专家</Tag>
                  {sentiment && (
                    <Tag color={sentiment.color} icon={sentiment.icon} style={{ margin: 0 }}>
                      {sentiment.label}
                    </Tag>
                  )}
                  {item.industry && <Tag color="purple">{item.industry}</Tag>}
                  <span style={{ fontWeight: 600, fontSize: 14, flex: 1 }}>
                    {item.title.length > 80 ? item.title.slice(0, 80) + '...' : item.title}
                  </span>
                </div>

                {(enr.summary || item.summary) && (
                  <div style={{
                    background: '#fffbe6', borderRadius: 4, padding: '8px 12px',
                    marginBottom: 8, fontSize: 13, color: '#614700', lineHeight: 1.6,
                  }}>
                    {enr.summary || item.summary}
                  </div>
                )}

                {keyPoints.length > 0 && (
                  <div style={{ marginBottom: 6, fontSize: 12, color: '#595959' }}>
                    {keyPoints.slice(0, 3).map((pt, i) => (
                      <div key={i} style={{ marginBottom: 2 }}>• {pt}</div>
                    ))}
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
                  </div>
                )}

                <div style={{ display: 'flex', gap: 12, fontSize: 12, color: '#8c8c8c' }}>
                  {item.expert_information && <span>{item.expert_information}</span>}
                  {item.author && <span>by {item.author}</span>}
                  {item.meeting_time && (
                    <span>
                      <ClockCircleOutlined style={{ marginRight: 3 }} />
                      {dayjs(item.meeting_time).format('MM-DD HH:mm')}
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
        title="专家调研详情"
        width={720}
        open={!!drawerItem}
        onClose={() => setDrawerItem(null)}
        loading={drawerLoading}
      >
        {drawerItem && (
          <div>
            <h3>{drawerItem.title}</h3>
            {drawerItem.expert_information && (
              <Tag color="gold">{drawerItem.expert_information}</Tag>
            )}
            {drawerItem.industry && <Tag color="purple">{drawerItem.industry}</Tag>}
            <Divider />
            {drawerItem.enrichment?.summary && (
              <>
                <h4>AI 分析</h4>
                <Card size="small" style={{ background: '#f6ffed', marginBottom: 16 }}>
                  <p>{drawerItem.enrichment.summary}</p>
                  {drawerItem.enrichment.key_points?.map((pt, i) => (
                    <div key={i} style={{ marginBottom: 4 }}>• {pt}</div>
                  ))}
                </Card>
              </>
            )}
            {drawerItem.topic && (
              <>
                <h4>讨论议题</h4>
                <Paragraph style={{ whiteSpace: 'pre-wrap' }}>{drawerItem.topic}</Paragraph>
              </>
            )}
            {drawerItem.insight && (
              <>
                <h4>核心洞察</h4>
                <div dangerouslySetInnerHTML={{ __html: drawerItem.insight }} />
              </>
            )}
            <Divider />
            <h4>完整内容</h4>
            <div
              style={{ maxHeight: 600, overflow: 'auto' }}
              dangerouslySetInnerHTML={{ __html: drawerItem.content }}
            />
          </div>
        )}
      </Drawer>
    </div>
  )
}
