/**
 * AlphaPai · 点评速递 (Comments)
 *
 * 基于 MongoDB (`alphapai.comments`) 的视图。
 * 每条为券商分析师简短点评 (通常 70-200 字)。
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
  Segmented,
  Select,
  Space,
  Spin,
  Statistic,
  Tag,
  Typography,
} from 'antd'
import {
  ReloadOutlined,
  MessageOutlined,
  LinkOutlined,
  ClockCircleOutlined,
  BankOutlined,
  UserOutlined,
  StockOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'
import TickerTagsTabs, { TickerTags } from '../components/TickerTagsTabs'

dayjs.extend(relativeTime)

import MarkdownRenderer from '../components/MarkdownRenderer'

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
  ticker_tags?: TickerTags
}

export default function AlphaPaiComments() {
  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [statsLoading, setStatsLoading] = useState(true)
  const [statsError, setStatsError] = useState<string | null>(null)

  const [items, setItems] = useState<Item[]>([])
  const [itemsLoading, setItemsLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [total, setTotal] = useState(0)

  const [query, setQuery] = useState('')
  const [institutionFilter, setInstitutionFilter] = useState<string | undefined>()
  // 子分类 (对齐 AlphaPai SPA 点评页左侧 tab): selected=干货点评 · regular=日报周报
  const [subcategory, setSubcategory] = useState<string | undefined>()

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
          category: 'comment',
          page,
          page_size: 20,
          q: query || undefined,
          institution: institutionFilter || undefined,
          subcategory: subcategory || undefined,
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
  }, [page, query, institutionFilter, subcategory])

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
        `/alphapai-db/items/comment/${encodeURIComponent(item.id)}`,
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
            <MessageOutlined /> AlphaPai · 点评速递
          </Title>
          <Text type="secondary">
            券商分析师短评 / 个股观点 / 行业快评 · 来自 crawl/alphapai_crawl
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
                  <ClockCircleOutlined /> 今日新增点评
                </span>
              }
              value={stats?.today.comment ?? 0}
              valueStyle={{ color: '#10b981', fontSize: 28 }}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              {dayjs().format('YYYY-MM-DD')}
              {stats?.latest_per_category?.comment && (
                <> · 最近发布 {stats.latest_per_category.comment}</>
              )}
            </Text>
          </Space>
        </Card>
      </Spin>

      <Card size="small">
        <Segmented
          style={{ marginBottom: 12 }}
          value={subcategory ?? 'all'}
          onChange={(v) => {
            setSubcategory(v === 'all' ? undefined : String(v))
            setPage(1)
          }}
          options={[
            { label: '全部', value: 'all' },
            { label: '干货点评', value: 'selected' },
            { label: '日报周报', value: 'regular' },
          ]}
        />
        <Space wrap style={{ marginBottom: 12 }}>
          <Input.Search
            placeholder="搜索标题 / 内容 / 分析师"
            allowClear
            style={{ width: 300 }}
            onSearch={(v) => {
              setQuery(v)
              setPage(1)
            }}
          />
          <Select
            placeholder="点评机构"
            allowClear
            value={institutionFilter}
            onChange={(v) => {
              setInstitutionFilter(v)
              setPage(1)
            }}
            style={{ width: 200 }}
            options={(stats?.recent_publishers?.comment || []).map((p) => ({
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
            // comment is very short — show content inline as the title-ish body
            const initial = (item.institution || 'A').slice(0, 1)
            return (
              <List.Item
                key={item.id}
                style={{ cursor: 'pointer', padding: '12px 0' }}
                onClick={() => openDetail(item)}
              >
                <List.Item.Meta
                  avatar={
                    <Avatar
                      style={{
                        backgroundColor: '#8b5cf6',
                        fontSize: 14,
                        fontWeight: 600,
                      }}
                    >
                      {initial}
                    </Avatar>
                  }
                  title={
                    <Space size={6} wrap>
                      {item.institution && (
                        <Tag color="purple" icon={<BankOutlined />}>
                          {item.institution}
                        </Tag>
                      )}
                      {item.analysts.slice(0, 2).map((a) => (
                        <Tag key={a} color="geekblue" icon={<UserOutlined />}>
                          {a}
                        </Tag>
                      ))}
                      <Text strong style={{ fontSize: 13 }}>
                        {item.title}
                      </Text>
                    </Space>
                  }
                  description={
                    <Space direction="vertical" size={2} style={{ width: '100%' }}>
                      <Text
                        style={{
                          fontSize: 13,
                          color: '#334155',
                          lineHeight: 1.6,
                          display: 'block',
                          marginTop: 4,
                        }}
                      >
                        {item.content_preview.replace(/\n+/g, ' ')}
                      </Text>
                      <Space size={10} wrap style={{ fontSize: 11, marginTop: 4 }}>
                        <Text type="secondary">
                          <ClockCircleOutlined /> {item.publish_time || '—'}
                        </Text>
                        {item.stocks.map((s, idx) => (
                          <Tag
                            key={`${s.code}-${idx}`}
                            color="cyan"
                            style={{ fontSize: 11 }}
                          >
                            <StockOutlined /> {s.name} {s.code}
                          </Tag>
                        ))}
                        {item.industries.map((ind) => (
                          <Tag key={ind} style={{ fontSize: 11 }}>
                            {ind}
                          </Tag>
                        ))}
                        <Text type="secondary" style={{ fontSize: 11 }}>
                          · {item.content_length} 字
                        </Text>
                      </Space>
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
        width={720}
        extra={
          detail?.web_url ? (
            <a href={detail.web_url} target="_blank" rel="noreferrer">
              <LinkOutlined /> AlphaPai 原页
            </a>
          ) : null
        }
      >
        <Spin spinning={detailLoading}>
          {detail ? (
            <div>
              <Space wrap size={6} style={{ marginBottom: 10 }}>
                {detail.institution && (
                  <Tag color="purple" icon={<BankOutlined />}>
                    {detail.institution}
                  </Tag>
                )}
                {detail.publish_time && (
                  <Tag icon={<ClockCircleOutlined />}>{detail.publish_time}</Tag>
                )}
                {detail.analysts.map((a) => (
                  <Tag key={a} color="geekblue" icon={<UserOutlined />}>
                    {a}
                  </Tag>
                ))}
              </Space>
              {detail.stocks.length > 0 && (
                <div style={{ marginBottom: 6 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    <StockOutlined /> 个股:
                  </Text>
                  {detail.stocks.map((s, idx) => (
                    <Tag key={`${s.code}-${idx}`} color="cyan">
                      {s.name} {s.code}
                    </Tag>
                  ))}
                </div>
              )}
              <TickerTagsTabs tags={detail.ticker_tags} />
              {detail.industries.length > 0 && (
                <div style={{ marginBottom: 10 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    行业:
                  </Text>
                  {detail.industries.map((i) => (
                    <Tag key={i}>{i}</Tag>
                  ))}
                </div>
              )}
              <Card
                size="small"
                title="点评内容"
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
