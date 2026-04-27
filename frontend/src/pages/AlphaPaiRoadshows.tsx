/**
 * AlphaPai · 会议纪要 (Roadshows)
 *
 * 基于 MongoDB (`alphapai.roadshows`) 的视图。每条为券商会议纪要，含：
 *   - 发布机构 (publishInstitution)
 *   - 关联个股 (stock[])
 *   - 行业 (industry[])
 *   - 分析师 (analyst)
 *   - 会议内容 (content)
 */
import { useCallback, useEffect, useState } from 'react'
import {
  Alert,
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
  ReadOutlined,
  LinkOutlined,
  ClockCircleOutlined,
  BankOutlined,
  StockOutlined,
  UserOutlined,
  TagOutlined,
} from '@ant-design/icons'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'
import TickerTagsTabs, { TickerTags } from '../components/TickerTagsTabs'

dayjs.extend(relativeTime)

const { Text, Title, Paragraph } = Typography

function stripMd(md: string): string {
  return md
    .replace(/```[\s\S]*?```/g, ' ')
    .replace(/`([^`]+)`/g, '$1')
    .replace(/!\[[^\]]*\]\([^)]*\)/g, '')
    .replace(/\[([^\]]+)\]\([^)]+\)/g, '$1')
    .replace(/^#{1,6}\s+/gm, '')
    .replace(/(\*\*|__)(.*?)\1/g, '$2')
    .replace(/(\*|_)(.*?)\1/g, '$2')
    .replace(/~~([^~]+)~~/g, '$1')
    .replace(/^\s*>\s?/gm, '')
    .replace(/^\s*[-*+]\s+/gm, '')
    .replace(/^\s*\d+\.\s+/gm, '')
    .replace(/^[-*_]{3,}\s*$/gm, '')
    .replace(/\|/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
}

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
  pdf_local_path: string | null
  pdf_size: number | null
  raw_id: string | null
  ticker_tags?: TickerTags
}

export default function AlphaPaiRoadshows() {
  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [statsLoading, setStatsLoading] = useState(true)
  const [statsError, setStatsError] = useState<string | null>(null)

  const [items, setItems] = useState<Item[]>([])
  const [itemsLoading, setItemsLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [total, setTotal] = useState(0)

  const [query, setQuery] = useState('')
  const [institutionFilter, setInstitutionFilter] = useState<string | undefined>()
  const [tickerFilter, setTickerFilter] = useState('')
  // 6 个子分类 (对齐 AlphaPai 会议 SPA 页面的 tab):
  //   ashare=A股会议 (marketTypeV2=10) · hk=港股会议 (50) · us=美股会议 (20)
  //   web=网络资源 (30) · ir=投资者关系 (60) · hot=热门会议 (70, 24h 窗)
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
          category: 'roadshow',
          page,
          page_size: 20,
          q: query || undefined,
          institution: institutionFilter || undefined,
          ticker: tickerFilter || undefined,
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
  }, [page, query, institutionFilter, tickerFilter, subcategory])

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
        `/alphapai-db/items/roadshow/${encodeURIComponent(item.id)}`,
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
            <ReadOutlined /> AlphaPai · 会议纪要
          </Title>
          <Text type="secondary">
            券商路演 / 业绩交流会 AI 纪要 · 来自 crawl/alphapai_crawl
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
                  <ClockCircleOutlined /> 今日新增纪要
                </span>
              }
              value={stats?.today.roadshow ?? 0}
              valueStyle={{ color: '#10b981', fontSize: 28 }}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              {dayjs().format('YYYY-MM-DD')}
              {stats?.latest_per_category?.roadshow && (
                <> · 最近发布 {stats.latest_per_category.roadshow}</>
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
            { label: 'A股会议', value: 'ashare' },
            { label: '港股会议', value: 'hk' },
            { label: '美股会议', value: 'us' },
            { label: '网络资源', value: 'web' },
            { label: '投资者关系', value: 'ir' },
            { label: '热门会议', value: 'hot' },
          ]}
        />
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
            placeholder="发布机构"
            allowClear
            value={institutionFilter}
            onChange={(v) => {
              setInstitutionFilter(v)
              setPage(1)
            }}
            style={{ width: 200 }}
            options={(stats?.recent_publishers?.roadshow || []).map((p) => ({
              value: p.name,
              label: `${p.name} (${p.count})`,
            }))}
          />
          <Input
            placeholder="个股代码/名称"
            allowClear
            style={{ width: 180 }}
            prefix={<StockOutlined />}
            onPressEnter={(e) => {
              setTickerFilter((e.target as HTMLInputElement).value)
              setPage(1)
            }}
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
          renderItem={(item) => (
            <List.Item
              key={item.id}
              style={{ cursor: 'pointer' }}
              onClick={() => openDetail(item)}
            >
              <List.Item.Meta
                title={
                  <Space size={6} wrap>
                    {item.institution && (
                      <Tag color="blue" icon={<BankOutlined />}>
                        {item.institution}
                      </Tag>
                    )}
                    <Text strong>{item.title}</Text>
                  </Space>
                }
                description={
                  <Space direction="vertical" size={2} style={{ width: '100%' }}>
                    <Space size={10} wrap style={{ fontSize: 12 }}>
                      <Text type="secondary">
                        <ClockCircleOutlined /> {item.publish_time || '—'}
                      </Text>
                      {item.analysts.length > 0 && (
                        <Text type="secondary">
                          <UserOutlined /> {item.analysts.join(' / ')}
                        </Text>
                      )}
                      {item.stocks.map((s, idx) => (
                        <Tag
                          key={`${s.code}-${idx}`}
                          color="cyan"
                          style={{ fontSize: 11 }}
                        >
                          {s.name} {s.code}
                        </Tag>
                      ))}
                      {item.industries.map((ind) => (
                        <Tag key={ind} style={{ fontSize: 11 }}>
                          {ind}
                        </Tag>
                      ))}
                    </Space>
                    <Text
                      style={{ fontSize: 12, color: '#64748b' }}
                      ellipsis={{ tooltip: stripMd(item.content_preview) } as any}
                    >
                      {stripMd(item.content_preview)}
                    </Text>
                    <Text type="secondary" style={{ fontSize: 11 }}>
                      {item.content_length} 字
                    </Text>
                  </Space>
                }
              />
            </List.Item>
          )}
        />
      </Card>

      <Drawer
        title={detail?.title || '详情'}
        open={detailOpen}
        onClose={() => setDetailOpen(false)}
        width={800}
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
                  <Tag color="blue" icon={<BankOutlined />}>
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
                    <TagOutlined /> 个股:
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
                title="纪要内容"
                style={{ marginTop: 8 }}
                bodyStyle={{
                  maxHeight: '62vh',
                  overflowY: 'auto',
                  fontSize: 13,
                  lineHeight: 1.8,
                  background: '#f8fafc',
                }}
              >
                {detail.content ? (
                  <div className="alphapai-md">
                    <ReactMarkdown remarkPlugins={[remarkGfm]}>
                      {detail.content}
                    </ReactMarkdown>
                  </div>
                ) : (
                  <Empty description="无内容" />
                )}
              </Card>
              <style>{`
                .alphapai-md h1, .alphapai-md h2, .alphapai-md h3, .alphapai-md h4 {
                  color: #0f172a;
                  margin-top: 18px;
                  margin-bottom: 10px;
                  font-weight: 600;
                  line-height: 1.45;
                }
                .alphapai-md h1 { font-size: 18px; border-bottom: 2px solid #e2e8f0; padding-bottom: 6px; }
                .alphapai-md h2 { font-size: 16px; color: #1e40af; }
                .alphapai-md h3 { font-size: 14px; color: #475569; }
                .alphapai-md h4 { font-size: 13.5px; color: #64748b; }
                .alphapai-md p  { margin: 8px 0; line-height: 1.85; }
                .alphapai-md ul, .alphapai-md ol { padding-left: 22px; margin: 8px 0; }
                .alphapai-md li { margin: 4px 0; line-height: 1.75; }
                .alphapai-md strong { color: #0f172a; font-weight: 600; }
                .alphapai-md em { color: #64748b; font-style: normal; font-size: 12px; }
                .alphapai-md hr {
                  margin: 18px 0;
                  border: none;
                  border-top: 1px dashed #cbd5e1;
                }
                .alphapai-md code {
                  background: #eef2ff;
                  padding: 1px 5px;
                  border-radius: 3px;
                  font-size: 12px;
                }
                .alphapai-md table {
                  border-collapse: collapse;
                  margin: 10px 0;
                  font-size: 12px;
                }
                .alphapai-md th, .alphapai-md td {
                  border: 1px solid #e2e8f0;
                  padding: 6px 10px;
                }
                .alphapai-md th { background: #f1f5f9; font-weight: 600; }
              `}</style>
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
