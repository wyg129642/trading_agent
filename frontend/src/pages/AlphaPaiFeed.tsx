/**
 * AlphaPai · 微信公众号 (Wechat Articles)
 *
 * 基于 MongoDB (`alphapai.wechat_articles`) 的视图。
 * 每条为券商 / 自媒体 公众号文章。
 */
import { useCallback, useEffect, useState } from 'react'
import {
  Alert,
  Avatar,
  Card,
  Drawer,
  Empty,
  Input,
  List,
  Select,
  Space,
  Spin,
  Statistic,
  Tag,
  Typography,
} from 'antd'
import {
  ReloadOutlined,
  LinkOutlined,
  ClockCircleOutlined,
  WechatOutlined,
  TagOutlined,
  FileTextOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'
import MarkdownRenderer from '../components/MarkdownRenderer'

dayjs.extend(relativeTime)

const { Text, Title, Paragraph } = Typography

interface Item {
  id: string
  category: string
  title: string
  publish_time: string | null
  web_url: string | null
  institution: string | null
  stocks: { code: string | null; name: string | null }[]
  industries: string[]
  analysts: string[]
  content_preview: string
  content_length: number
  has_pdf: boolean
  account_name: string | null
  source_url: string | null
  crawled_at: string | null
}

interface ListResponse {
  items: Item[]
  total: number
  page: number
  page_size: number
  has_next: boolean
}

interface StatsResponse {
  total: number
  per_category: Record<string, number>
  today: Record<string, number>
  last_7_days: {
    date: string
    roadshow: number
    report: number
    comment: number
    wechat: number
  }[]
  recent_publishers: Record<string, { name: string; count: number }[]>
  latest_per_category: Record<string, string | null>
}

interface DetailResponse extends Item {
  content: string
  raw_id: string | null
}

// Soft deterministic avatar color from account name
function avatarColor(name: string): string {
  const palette = [
    '#16a34a',
    '#0284c7',
    '#8b5cf6',
    '#f59e0b',
    '#ec4899',
    '#10b981',
    '#2563eb',
    '#d946ef',
  ]
  let hash = 0
  for (let i = 0; i < name.length; i++) hash = (hash * 31 + name.charCodeAt(i)) >>> 0
  return palette[hash % palette.length]
}

export default function AlphaPaiFeed() {
  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [statsLoading, setStatsLoading] = useState(true)
  const [statsError, setStatsError] = useState<string | null>(null)

  const [items, setItems] = useState<Item[]>([])
  const [itemsLoading, setItemsLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [total, setTotal] = useState(0)

  const [query, setQuery] = useState('')
  const [accountFilter, setAccountFilter] = useState<string | undefined>()

  const [detailOpen, setDetailOpen] = useState(false)
  const [detail, setDetail] = useState<DetailResponse | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)

  const loadStats = useCallback(async () => {
    setStatsLoading(true)
    setStatsError(null)
    try {
      const res = await api.get<StatsResponse>('/alphapai-db/stats')
      setStats(res.data)
    } catch (err: any) {
      setStatsError(err?.response?.data?.detail || err?.message || '加载失败')
    } finally {
      setStatsLoading(false)
    }
  }, [])

  const loadItems = useCallback(async () => {
    setItemsLoading(true)
    try {
      const res = await api.get<ListResponse>('/alphapai-db/items', {
        params: {
          category: 'wechat',
          page,
          page_size: 20,
          q: query || undefined,
          institution: accountFilter || undefined,
        },
      })
      setItems(res.data.items)
      setTotal(res.data.total)
    } catch {
      setItems([])
      setTotal(0)
    } finally {
      setItemsLoading(false)
    }
  }, [page, query, accountFilter])

  useEffect(() => {
    loadStats()
  }, [loadStats])

  useEffect(() => {
    loadItems()
  }, [loadItems])

  const openDetail = useCallback(async (item: Item) => {
    setDetailOpen(true)
    setDetailLoading(true)
    setDetail(null)
    try {
      const res = await api.get<DetailResponse>(
        `/alphapai-db/items/wechat/${encodeURIComponent(item.id)}`,
      )
      setDetail(res.data)
    } catch {
      setDetail(null)
    } finally {
      setDetailLoading(false)
    }
  }, [])

  return (
    <div style={{ padding: 20 }}>
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          marginBottom: 16,
        }}
      >
        <div>
          <Title level={3} style={{ margin: 0 }}>
            <WechatOutlined /> AlphaPai · 微信公众号
          </Title>
          <Text type="secondary">
            券商 / 行业自媒体 公众号文章聚合 · 来自 crawl/alphapai_crawl
          </Text>
        </div>
        <a onClick={loadStats} style={{ fontSize: 13 }}>
          <ReloadOutlined /> 刷新
        </a>
      </div>

      {statsError && (
        <Alert
          type="warning"
          showIcon
          message="无法从 MongoDB 加载数据"
          description={statsError}
          style={{ marginBottom: 16 }}
        />
      )}

      <Spin spinning={statsLoading}>
        <Card size="small" bodyStyle={{ padding: 14 }} style={{ marginBottom: 16 }}>
          <Space size={18} align="center">
            <Statistic
              title={
                <span style={{ color: '#10b981' }}>
                  <ClockCircleOutlined /> 今日新增文章
                </span>
              }
              value={stats?.today.wechat ?? 0}
              valueStyle={{ color: '#10b981', fontSize: 28 }}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              {dayjs().format('YYYY-MM-DD')}
              {stats?.latest_per_category?.wechat && (
                <> · 最近发布 {stats.latest_per_category.wechat}</>
              )}
            </Text>
          </Space>
        </Card>
      </Spin>

      <Card size="small">
        <Space wrap style={{ marginBottom: 12 }}>
          <Input.Search
            placeholder="搜索标题 / 内容"
            allowClear
            style={{ width: 260 }}
            onSearch={(v) => {
              setQuery(v)
              setPage(1)
            }}
          />
          <Select
            placeholder="公众号账号"
            allowClear
            value={accountFilter}
            onChange={(v) => {
              setAccountFilter(v)
              setPage(1)
            }}
            style={{ width: 220 }}
            options={(stats?.recent_publishers?.wechat || []).map((p) => ({
              value: p.name,
              label: `${p.name} (${p.count})`,
            }))}
          />
          <Text type="secondary" style={{ fontSize: 12 }}>
            共 {total} 条
          </Text>
        </Space>

        <List
          loading={itemsLoading}
          dataSource={items}
          locale={{ emptyText: <Empty description="暂无数据" /> }}
          pagination={{
            current: page,
            pageSize: 20,
            total,
            showSizeChanger: false,
            onChange: (p) => setPage(p),
          }}
          renderItem={(item) => {
            const accountName = item.account_name || item.institution || '未知'
            const initial = accountName.slice(0, 2)
            return (
              <List.Item
                key={item.id}
                style={{ cursor: 'pointer' }}
                onClick={() => openDetail(item)}
              >
                <List.Item.Meta
                  avatar={
                    <Avatar
                      style={{
                        backgroundColor: avatarColor(accountName),
                        fontSize: 12,
                        fontWeight: 600,
                      }}
                      size={36}
                    >
                      {initial}
                    </Avatar>
                  }
                  title={
                    <Space size={6} wrap>
                      <Text strong>{item.title}</Text>
                    </Space>
                  }
                  description={
                    <Space direction="vertical" size={2} style={{ width: '100%' }}>
                      <Space size={10} wrap style={{ fontSize: 12 }}>
                        <Tag color="green" icon={<WechatOutlined />}>
                          {accountName}
                        </Tag>
                        <Text type="secondary">
                          <ClockCircleOutlined /> {item.publish_time || '—'}
                        </Text>
                        {item.industries.slice(0, 4).map((ind) => (
                          <Tag key={ind} style={{ fontSize: 11 }}>
                            {ind}
                          </Tag>
                        ))}
                        {item.stocks.slice(0, 3).map((s, idx) => (
                          <Tag
                            key={`${s.code}-${idx}`}
                            color="cyan"
                            style={{ fontSize: 11 }}
                          >
                            {s.name} {s.code}
                          </Tag>
                        ))}
                      </Space>
                      <Text
                        style={{ fontSize: 12, color: '#64748b' }}
                        ellipsis={{ tooltip: item.content_preview } as any}
                      >
                        {item.content_preview.replace(/\n+/g, ' ')}
                      </Text>
                      <Text type="secondary" style={{ fontSize: 11 }}>
                        <FileTextOutlined /> {item.content_length} 字
                      </Text>
                    </Space>
                  }
                />
              </List.Item>
            )
          }}
        />
      </Card>

      <Drawer
        title={detail?.title || '详情'}
        open={detailOpen}
        onClose={() => setDetailOpen(false)}
        width={800}
        extra={
          <Space>
            {detail?.source_url && (
              <a href={detail.source_url} target="_blank" rel="noreferrer">
                <WechatOutlined /> 原文
              </a>
            )}
            {detail?.web_url && (
              <a href={detail.web_url} target="_blank" rel="noreferrer">
                <LinkOutlined /> AlphaPai 原页
              </a>
            )}
          </Space>
        }
      >
        <Spin spinning={detailLoading}>
          {detail ? (
            <div>
              <Space wrap size={6} style={{ marginBottom: 10 }}>
                <Tag color="green" icon={<WechatOutlined />}>
                  {detail.account_name || detail.institution || '未知账号'}
                </Tag>
                {detail.publish_time && (
                  <Tag icon={<ClockCircleOutlined />}>{detail.publish_time}</Tag>
                )}
              </Space>
              {detail.industries.length > 0 && (
                <div style={{ marginBottom: 6 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    <TagOutlined /> 行业:
                  </Text>
                  {detail.industries.map((i) => (
                    <Tag key={i}>{i}</Tag>
                  ))}
                </div>
              )}
              {detail.stocks.length > 0 && (
                <div style={{ marginBottom: 10 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    个股:
                  </Text>
                  {detail.stocks.map((s, idx) => (
                    <Tag key={`${s.code}-${idx}`} color="cyan">
                      {s.name} {s.code}
                    </Tag>
                  ))}
                </div>
              )}
              <Card
                size="small"
                title="文章内容"
                style={{ marginTop: 8 }}
                bodyStyle={{
                  maxHeight: '58vh',
                  overflowY: 'auto',
                  fontSize: 13,
                  lineHeight: 1.8,
                  background: '#f8fafc',
                }}
              >
                {detail.content ? (
                  <MarkdownRenderer content={detail.content} />
                ) : (
                  <Empty description="无内容" />
                )}
              </Card>
              <Paragraph
                type="secondary"
                style={{ fontSize: 11, marginTop: 12 }}
              >
                ID: {detail.id}
                {detail.crawled_at &&
                  ` · 抓取于 ${dayjs(detail.crawled_at).format('YYYY-MM-DD HH:mm')}`}
              </Paragraph>
            </div>
          ) : (
            <Empty />
          )}
        </Spin>
      </Drawer>
    </div>
  )
}
