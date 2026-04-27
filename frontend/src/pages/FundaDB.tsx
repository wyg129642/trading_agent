/**
 * Funda 专区 · 基于 MongoDB (`funda.*`) 的视图。
 *
 * 3 个数据类型：
 *   post                 → 研究文章
 *   earnings_report      → 财报 (8-K)
 *   earnings_transcript  → 业绩会逐字稿
 */
import { useCallback, useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import {
  Alert,
  Card,
  Drawer,
  Empty,
  Input,
  List,
  Segmented,
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
  FileTextOutlined,
  AuditOutlined,
  MessageOutlined,
  StockOutlined,
  EyeOutlined,
  FundProjectionScreenOutlined,
} from '@ant-design/icons'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'
import TickerTagsTabs, { TickerTags } from '../components/TickerTagsTabs'

dayjs.extend(relativeTime)

const { Text, Title } = Typography

type CategoryKey = 'post' | 'earnings_report' | 'earnings_transcript'

const CATEGORY_META: Record<
  CategoryKey,
  { label: string; color: string; icon: React.ReactNode }
> = {
  post: { label: '研究文章', color: '#2563eb', icon: <FileTextOutlined /> },
  earnings_report: { label: '财报 (8-K)', color: '#ef4444', icon: <AuditOutlined /> },
  earnings_transcript: {
    label: '业绩会逐字稿',
    color: '#10b981',
    icon: <MessageOutlined />,
  },
}

interface Item {
  id: string
  category: CategoryKey
  category_label: string
  title: string
  release_time: string | null
  web_url: string | null
  source_url: string
  tickers: string[]
  industry: string
  year: number | null
  period: string
  access_level: string
  type: string
  tags: string[]
  views: number
  preview: string
  stats: { chars: number; html_chars: number }
  has_html: boolean
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
  per_category: Record<CategoryKey, number>
  today: Record<CategoryKey, number>
  latest_per_category: Record<CategoryKey, string | null>
  crawler_state: {
    category: string
    label: string
    in_progress: boolean
    last_processed_at: string | null
    last_run_end_at: string | null
    last_run_stats: { added?: number; updated?: number; skipped?: number; failed?: number }
  }[]
  daily_platform_stats: Record<
    CategoryKey,
    { platform_count: number; in_db: number; missing: number }
  > | null
}

interface DetailResponse extends Item {
  content_md: string
  content_html: string
  preview_body: string
  ticker_tags?: TickerTags
}

// URL slug → backend category key
const SLUG_TO_CATEGORY: Record<string, CategoryKey> = {
  'posts': 'post',
  'earnings-reports': 'earnings_report',
  'earnings-transcripts': 'earnings_transcript',
}

export default function FundaDB() {
  // URL-param 决定初始分类 (/funda/posts, /funda/earnings-reports, /funda/earnings-transcripts)
  // 不匹配时 fallback 到 "post"
  const { slug } = useParams<{ slug?: string }>()
  const initialCategory: CategoryKey = (slug && SLUG_TO_CATEGORY[slug]) || 'post'

  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [statsLoading, setStatsLoading] = useState(true)
  const [statsError, setStatsError] = useState<string | null>(null)

  const [category, setCategory] = useState<CategoryKey>(initialCategory)
  const [items, setItems] = useState<Item[]>([])
  const [itemsLoading, setItemsLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [total, setTotal] = useState(0)

  const [query, setQuery] = useState('')
  const [tickerFilter, setTickerFilter] = useState('')
  const [industryFilter, setIndustryFilter] = useState('')

  const [detailOpen, setDetailOpen] = useState(false)
  const [detail, setDetail] = useState<DetailResponse | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)

  // 侧栏切换时 URL 变 → 同步内部 category state + 重置页码
  useEffect(() => {
    const fromUrl = (slug && SLUG_TO_CATEGORY[slug]) || 'post'
    setCategory(fromUrl)
    setPage(1)
  }, [slug])

  const loadStats = useCallback(async () => {
    setStatsLoading(true)
    setStatsError(null)
    try {
      const res = await api.get<StatsResponse>('/funda-db/stats')
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
      const res = await api.get<ListResponse>('/funda-db/items', {
        params: {
          category,
          page,
          page_size: 20,
          q: query || undefined,
          ticker: tickerFilter || undefined,
          industry: industryFilter || undefined,
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
  }, [category, page, query, tickerFilter, industryFilter])

  useEffect(() => {
    loadStats()
  }, [loadStats])

  useEffect(() => {
    loadItems()
  }, [loadItems])

  const openDetail = useCallback(
    async (item: Item) => {
      setDetailOpen(true)
      setDetailLoading(true)
      setDetail(null)
      try {
        const res = await api.get<DetailResponse>(
          `/funda-db/items/${category}/${encodeURIComponent(item.id)}`,
        )
        setDetail(res.data)
      } catch {
        setDetail(null)
      } finally {
        setDetailLoading(false)
      }
    },
    [category],
  )

  const todayCount =
    stats?.today[category] ??
    (stats ? 0 : null) // null = still loading
  const totalForCategory = stats?.per_category[category] ?? 0
  const latest = stats?.latest_per_category?.[category]
  const platformDaily = stats?.daily_platform_stats?.[category]

  // Segmented options showing today increments for each category
  const segOptions = (['post', 'earnings_report', 'earnings_transcript'] as CategoryKey[]).map(
    (k) => {
      const meta = CATEGORY_META[k]
      const today = stats?.today[k] ?? 0
      return {
        label: (
          <span>
            {meta.icon} {meta.label}
            {today > 0 ? (
              <Tag color="green" style={{ marginLeft: 6, fontSize: 10 }}>
                今日 +{today}
              </Tag>
            ) : null}
          </span>
        ),
        value: k,
      }
    },
  )

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
            <FundProjectionScreenOutlined /> Funda 专区
          </Title>
          <Text type="secondary">
            funda.ai · 研究文章 + 8-K 财报 + 业绩会逐字稿
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
          message="无法从 MongoDB 加载 Funda 数据"
          description={statsError}
          style={{ marginBottom: 16 }}
        />
      )}

      <Spin spinning={statsLoading}>
        <Card size="small" bodyStyle={{ padding: 14 }} style={{ marginBottom: 16 }}>
          <Space size={18} align="center">
            <Statistic
              title={
                <span style={{ color: CATEGORY_META[category].color }}>
                  {CATEGORY_META[category].icon}{' '}
                  今日新增 · {CATEGORY_META[category].label}
                </span>
              }
              value={todayCount ?? 0}
              valueStyle={{ color: CATEGORY_META[category].color, fontSize: 28 }}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              {dayjs().format('YYYY-MM-DD')}
              {latest && <> · 最近发布 {latest}</>}
              {totalForCategory > 0 && <> · 该分类累计 {totalForCategory}</>}
            </Text>
          </Space>
        </Card>
      </Spin>

      <Card size="small">
        <Space
          direction="vertical"
          size={10}
          style={{ width: '100%', marginBottom: 12 }}
        >
          <Segmented
            value={category}
            onChange={(v) => {
              setCategory(v as CategoryKey)
              setPage(1)
            }}
            options={segOptions as any}
          />
          <Space wrap>
            <Input.Search
              placeholder="搜索标题 / 摘要 / 正文"
              allowClear
              style={{ width: 300 }}
              onSearch={(v) => {
                setQuery(v)
                setPage(1)
              }}
            />
            <Input
              placeholder="Ticker (如 INTC)"
              allowClear
              prefix={<StockOutlined />}
              style={{ width: 160 }}
              onPressEnter={(e) => {
                setTickerFilter((e.target as HTMLInputElement).value)
                setPage(1)
              }}
            />
            <Input
              placeholder="行业"
              allowClear
              style={{ width: 200 }}
              onPressEnter={(e) => {
                setIndustryFilter((e.target as HTMLInputElement).value)
                setPage(1)
              }}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              共 {total} 条
            </Text>
          </Space>
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
            const meta = CATEGORY_META[item.category]
            return (
              <List.Item
                key={item.id}
                style={{ cursor: 'pointer' }}
                onClick={() => openDetail(item)}
              >
                <List.Item.Meta
                  title={
                    <Space size={6} wrap>
                      <Tag color={meta.color} style={{ color: '#fff', border: 'none' }}>
                        {meta.icon} {item.category_label}
                      </Tag>
                      {item.type && item.type !== 'TRANSCRIPT' && item.type !== 'EIGHT_K' && (
                        <Tag style={{ fontSize: 11 }}>{item.type}</Tag>
                      )}
                      {item.access_level === 'ENTERPRISE' && (
                        <Tag color="gold" style={{ fontSize: 11 }}>
                          ENTERPRISE
                        </Tag>
                      )}
                      {item.tickers.map((tk) => (
                        <Tag key={tk} color="cyan" style={{ fontSize: 11 }}>
                          <StockOutlined /> {tk}
                        </Tag>
                      ))}
                      <Text strong>{item.title}</Text>
                    </Space>
                  }
                  description={
                    <Space direction="vertical" size={2} style={{ width: '100%' }}>
                      <Space size={10} wrap style={{ fontSize: 12 }}>
                        <Text type="secondary">
                          <ClockCircleOutlined /> {item.release_time || '—'}
                        </Text>
                        {item.industry && (
                          <Tag style={{ fontSize: 11 }}>{item.industry}</Tag>
                        )}
                        {item.period && item.year && (
                          <Tag color="blue" style={{ fontSize: 11 }}>
                            {item.year} {item.period}
                          </Tag>
                        )}
                        {item.tags.map((t) => (
                          <Tag key={t} style={{ fontSize: 11 }}>
                            {t}
                          </Tag>
                        ))}
                      </Space>
                      {item.preview && (
                        <Text
                          style={{ fontSize: 12, color: '#64748b' }}
                          ellipsis={{ tooltip: item.preview } as any}
                        >
                          {item.preview.replace(/\n+/g, ' ')}
                        </Text>
                      )}
                      <Space size={10} style={{ fontSize: 11, color: '#94a3b8' }}>
                        <span>
                          <FileTextOutlined /> {item.stats.chars.toLocaleString()} 字
                        </span>
                        {item.views > 0 && (
                          <span>
                            <EyeOutlined /> {item.views.toLocaleString()}
                          </span>
                        )}
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
        width={880}
        extra={
          <Space>
            {detail?.source_url && (
              <a href={detail.source_url} target="_blank" rel="noreferrer">
                <LinkOutlined /> 原文
              </a>
            )}
            {detail?.web_url && (
              <a href={detail.web_url} target="_blank" rel="noreferrer">
                <LinkOutlined /> Funda 原页
              </a>
            )}
          </Space>
        }
      >
        <Spin spinning={detailLoading}>
          {detail ? (
            <div>
              <Space wrap size={6} style={{ marginBottom: 10 }}>
                <Tag
                  color={CATEGORY_META[detail.category].color}
                  style={{ color: '#fff', border: 'none' }}
                >
                  {CATEGORY_META[detail.category].icon} {detail.category_label}
                </Tag>
                {detail.release_time && (
                  <Tag icon={<ClockCircleOutlined />}>{detail.release_time}</Tag>
                )}
                {detail.access_level === 'ENTERPRISE' && (
                  <Tag color="gold">ENTERPRISE</Tag>
                )}
                {detail.tickers.map((tk) => (
                  <Tag key={tk} color="cyan">
                    <StockOutlined /> {tk}
                  </Tag>
                ))}
                {detail.industry && <Tag>{detail.industry}</Tag>}
                {detail.period && detail.year && (
                  <Tag color="blue">
                    {detail.year} {detail.period}
                  </Tag>
                )}
              </Space>

              <TickerTagsTabs tags={detail.ticker_tags} />

              {detail.tags.length > 0 && (
                <div style={{ marginBottom: 8 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    标签:
                  </Text>
                  {detail.tags.map((t) => (
                    <Tag key={t}>{t}</Tag>
                  ))}
                </div>
              )}

              <Card
                size="small"
                title={
                  detail.category === 'earnings_report'
                    ? '8-K 正文'
                    : detail.category === 'earnings_transcript'
                    ? '业绩会逐字稿'
                    : '文章正文'
                }
                style={{ marginTop: 8 }}
                bodyStyle={{
                  maxHeight: '62vh',
                  overflowY: 'auto',
                  fontSize: 13,
                  lineHeight: 1.75,
                  background: '#f8fafc',
                }}
              >
                {detail.content_md ? (
                  detail.category === 'post' ? (
                    <div className="funda-md">
                      <ReactMarkdown remarkPlugins={[remarkGfm]}>
                        {detail.content_md}
                      </ReactMarkdown>
                    </div>
                  ) : (
                    <pre
                      style={{
                        whiteSpace: 'pre-wrap',
                        fontFamily: 'inherit',
                        margin: 0,
                      }}
                    >
                      {detail.content_md}
                    </pre>
                  )
                ) : (
                  <Empty description="无正文" />
                )}
              </Card>

              <Text
                type="secondary"
                style={{ fontSize: 11, display: 'block', marginTop: 16 }}
              >
                ID: {detail.id}
                {detail.crawled_at &&
                  ` · 抓取于 ${dayjs(detail.crawled_at).format('YYYY-MM-DD HH:mm')}`}
              </Text>
            </div>
          ) : (
            <Empty />
          )}
        </Spin>
      </Drawer>

      <style>{`
        .funda-md {
          font-size: 13.5px;
          line-height: 1.8;
        }
        .funda-md h1, .funda-md h2, .funda-md h3 {
          color: #0f172a;
          margin-top: 14px;
        }
        .funda-md table {
          border-collapse: collapse;
          margin: 10px 0;
          font-size: 12px;
        }
        .funda-md th, .funda-md td {
          border: 1px solid #e2e8f0;
          padding: 5px 9px;
        }
        .funda-md th { background: #f8fafc; font-weight: 600; }
        .funda-md a { color: #2563eb; text-decoration: none; }
        .funda-md a:hover { text-decoration: underline; }
      `}</style>
    </div>
  )
}
