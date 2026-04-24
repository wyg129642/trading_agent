/**
 * SemiAnalysis 专区 · 基于 MongoDB (`funda.semianalysis_posts`) 的视图.
 *
 * 单一内容类型: Substack newsletter posts (research articles).
 * 匿名/付费差异由 audience 字段 + is_paid 标签呈现.
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
  StockOutlined,
  ExperimentOutlined,
  LockOutlined,
  UnlockOutlined,
  HeartOutlined,
} from '@ant-design/icons'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'

dayjs.extend(relativeTime)

const { Text, Title } = Typography

type AudienceKey = 'all' | 'everyone' | 'only_paid'

interface Item {
  id: string
  post_id: number
  slug: string
  title: string
  subtitle: string
  release_time: string | null
  release_time_ms: number | null
  post_date: string | null
  audience: 'everyone' | 'only_paid' | 'founding' | null
  is_paid: boolean
  content_truncated: boolean
  section_name: string | null
  canonical_url: string | null
  cover_image: string
  podcast_url: string
  organization: string
  authors: string[]
  preview: string
  content_length: number
  word_count: number
  reaction_count: number
  canonical_tickers: string[]
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
  today: number
  paid_count: number
  free_count: number
  last_7_days: {
    date: string
    total: number
    paid: number
    free: number
  }[]
  top_authors: { name: string; count: number }[]
  latest_release_time: string | null
  crawler_state: {
    top_id: number | null
    in_progress: boolean
    last_run_end_at: string | null
    last_run_stats: { added?: number; updated?: number; skipped?: number; failed?: number }
    updated_at: string | null
  } | null
  daily_platform_stats: {
    platform_count: number
    in_db: number
    missing: number
    scanned_at: string | null
  } | null
}

interface DetailResponse extends Item {
  content_md: string
  content_html: string
  truncated_body_text: string
  description: string
  detail_result: Record<string, unknown>
}

export default function SemiAnalysisDB() {
  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [statsLoading, setStatsLoading] = useState(true)
  const [statsError, setStatsError] = useState<string | null>(null)

  const [items, setItems] = useState<Item[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [pageSize] = useState(20)
  const [itemsLoading, setItemsLoading] = useState(false)

  const [audience, setAudience] = useState<AudienceKey>('all')
  const [query, setQuery] = useState('')
  const [tickerFilter, setTickerFilter] = useState('')
  const [authorFilter, setAuthorFilter] = useState('')

  const [detailOpen, setDetailOpen] = useState(false)
  const [detail, setDetail] = useState<DetailResponse | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)

  const loadStats = useCallback(async () => {
    setStatsLoading(true)
    setStatsError(null)
    try {
      const res = await api.get<StatsResponse>('/semianalysis-db/stats')
      setStats(res.data)
    } catch (e: any) {
      setStatsError(e?.response?.data?.detail || String(e))
    } finally {
      setStatsLoading(false)
    }
  }, [])

  const loadItems = useCallback(async () => {
    setItemsLoading(true)
    try {
      const params: Record<string, string | number> = {
        page,
        page_size: pageSize,
      }
      if (query) params.q = query
      if (tickerFilter) params.ticker = tickerFilter
      if (authorFilter) params.author = authorFilter
      if (audience !== 'all') params.audience = audience
      const res = await api.get<ListResponse>('/semianalysis-db/posts', { params })
      setItems(res.data.items)
      setTotal(res.data.total)
    } catch {
      setItems([])
      setTotal(0)
    } finally {
      setItemsLoading(false)
    }
  }, [page, pageSize, query, tickerFilter, authorFilter, audience])

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
        `/semianalysis-db/posts/${encodeURIComponent(item.id)}`,
      )
      setDetail(res.data)
    } catch {
      setDetail(null)
    } finally {
      setDetailLoading(false)
    }
  }, [])

  const segOptions = [
    { label: '全部', value: 'all' as AudienceKey },
    {
      label: (
        <span>
          <UnlockOutlined /> 免费
          {stats && stats.free_count > 0 ? (
            <Tag color="green" style={{ marginLeft: 6, fontSize: 10 }}>
              {stats.free_count}
            </Tag>
          ) : null}
        </span>
      ),
      value: 'everyone' as AudienceKey,
    },
    {
      label: (
        <span>
          <LockOutlined /> 付费
          {stats && stats.paid_count > 0 ? (
            <Tag color="orange" style={{ marginLeft: 6, fontSize: 10 }}>
              {stats.paid_count}
            </Tag>
          ) : null}
        </span>
      ),
      value: 'only_paid' as AudienceKey,
    },
  ]

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
            <ExperimentOutlined /> SemiAnalysis 专区
          </Title>
          <Text type="secondary">
            newsletter.semianalysis.com · 半导体 / AI 基础设施研究 · Substack
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
          message="无法从 MongoDB 加载 SemiAnalysis 数据"
          description={statsError}
          style={{ marginBottom: 16 }}
        />
      )}

      <Spin spinning={statsLoading}>
        <Card size="small" bodyStyle={{ padding: 14 }} style={{ marginBottom: 16 }}>
          <Space size={24} align="center" wrap>
            <Statistic
              title={<span style={{ color: '#2563eb' }}><FileTextOutlined /> 今日新增</span>}
              value={stats?.today ?? 0}
              valueStyle={{ color: '#2563eb', fontSize: 28 }}
            />
            <Statistic
              title="累计 Posts"
              value={stats?.total ?? 0}
              valueStyle={{ fontSize: 22 }}
            />
            <Statistic
              title={<span><UnlockOutlined /> 免费 / <LockOutlined /> 付费</span>}
              value={stats ? `${stats.free_count} / ${stats.paid_count}` : '-'}
              valueStyle={{ fontSize: 18 }}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              {stats?.latest_release_time && <>最近发布 {stats.latest_release_time}</>}
              {stats?.crawler_state?.last_run_end_at && (
                <>
                  {' · '}爬虫末次运行{' '}
                  {dayjs(stats.crawler_state.last_run_end_at).fromNow()}
                </>
              )}
            </Text>
          </Space>
          {stats?.daily_platform_stats && (
            <div style={{ marginTop: 10, fontSize: 12 }}>
              <Text type="secondary">
                今日对齐: 平台 {stats.daily_platform_stats.platform_count} 条 · 入库{' '}
                {stats.daily_platform_stats.in_db} 条
                {stats.daily_platform_stats.missing > 0 && (
                  <>
                    {' '}·{' '}
                    <Tag color="red" style={{ fontSize: 11 }}>
                      漏 {stats.daily_platform_stats.missing}
                    </Tag>
                  </>
                )}
              </Text>
            </div>
          )}
        </Card>
      </Spin>

      <Card size="small">
        <Space
          direction="vertical"
          size={10}
          style={{ width: '100%', marginBottom: 12 }}
        >
          <Segmented
            value={audience}
            onChange={(v) => {
              setAudience(v as AudienceKey)
              setPage(1)
            }}
            options={segOptions as any}
          />
          <Space wrap>
            <Input.Search
              placeholder="搜索标题 / 副标题 / 正文 / 作者"
              allowClear
              style={{ width: 320 }}
              onSearch={(v) => {
                setQuery(v)
                setPage(1)
              }}
            />
            <Input
              placeholder="Ticker (如 NVDA)"
              allowClear
              prefix={<StockOutlined />}
              style={{ width: 160 }}
              onPressEnter={(e) => {
                setTickerFilter((e.target as HTMLInputElement).value)
                setPage(1)
              }}
            />
            <Input
              placeholder="作者"
              allowClear
              style={{ width: 180 }}
              onPressEnter={(e) => {
                setAuthorFilter((e.target as HTMLInputElement).value)
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
            pageSize,
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
                    {item.is_paid ? (
                      <Tag color="orange" style={{ fontSize: 11 }}>
                        <LockOutlined /> PAID
                      </Tag>
                    ) : (
                      <Tag color="green" style={{ fontSize: 11 }}>
                        <UnlockOutlined /> FREE
                      </Tag>
                    )}
                    {item.section_name && (
                      <Tag style={{ fontSize: 11 }}>{item.section_name}</Tag>
                    )}
                    {item.canonical_tickers.map((tk) => (
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
                      {item.authors.length > 0 && (
                        <Text type="secondary">
                          {item.authors.slice(0, 3).join(', ')}
                          {item.authors.length > 3 && ` 等${item.authors.length}人`}
                        </Text>
                      )}
                      {item.word_count > 0 && (
                        <Text type="secondary" style={{ fontSize: 11 }}>
                          <FileTextOutlined /> {item.word_count.toLocaleString()} words
                        </Text>
                      )}
                      {item.reaction_count > 0 && (
                        <Text type="secondary" style={{ fontSize: 11 }}>
                          <HeartOutlined /> {item.reaction_count}
                        </Text>
                      )}
                    </Space>
                    {item.subtitle && (
                      <Text strong style={{ fontSize: 12.5, color: '#475569' }}>
                        {item.subtitle}
                      </Text>
                    )}
                    {item.preview && (
                      <Text
                        style={{ fontSize: 12, color: '#64748b' }}
                        ellipsis={{ tooltip: item.preview } as any}
                      >
                        {item.preview.replace(/\n+/g, ' ')}
                      </Text>
                    )}
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
        width={920}
        extra={
          detail?.canonical_url && (
            <a href={detail.canonical_url} target="_blank" rel="noreferrer">
              <LinkOutlined /> 原文 (SemiAnalysis)
            </a>
          )
        }
      >
        <Spin spinning={detailLoading}>
          {detail ? (
            <div>
              <Space wrap size={6} style={{ marginBottom: 10 }}>
                {detail.is_paid ? (
                  <Tag color="orange">
                    <LockOutlined /> PAID · {detail.audience}
                  </Tag>
                ) : (
                  <Tag color="green">
                    <UnlockOutlined /> FREE
                  </Tag>
                )}
                {detail.release_time && (
                  <Tag icon={<ClockCircleOutlined />}>{detail.release_time}</Tag>
                )}
                {detail.section_name && <Tag>{detail.section_name}</Tag>}
                {detail.canonical_tickers.map((tk) => (
                  <Tag key={tk} color="cyan">
                    <StockOutlined /> {tk}
                  </Tag>
                ))}
                {detail.word_count > 0 && (
                  <Tag color="blue">
                    {detail.word_count.toLocaleString()} words
                  </Tag>
                )}
                {detail.reaction_count > 0 && (
                  <Tag color="red">
                    <HeartOutlined /> {detail.reaction_count}
                  </Tag>
                )}
              </Space>

              {detail.subtitle && (
                <div
                  style={{
                    padding: '10px 14px',
                    background: '#f8fafc',
                    borderLeft: '3px solid #2563eb',
                    borderRadius: 4,
                    marginBottom: 12,
                    fontSize: 14,
                    fontStyle: 'italic',
                    color: '#475569',
                  }}
                >
                  {detail.subtitle}
                </div>
              )}

              {detail.authors.length > 0 && (
                <div style={{ marginBottom: 10, fontSize: 12 }}>
                  <Text type="secondary">作者: </Text>
                  {detail.authors.map((a) => (
                    <Tag key={a} style={{ fontSize: 11 }}>{a}</Tag>
                  ))}
                </div>
              )}

              {detail.content_truncated && (
                <Alert
                  type="info"
                  showIcon
                  message="付费内容预览"
                  description="此 post 为 Substack 付费订阅内容. 已匿名抓取 SemiAnalysis 提供的免费预览; 完整正文需要 paid subscriber cookie (详见 /data-sources 凭证管理)."
                  style={{ marginBottom: 12 }}
                />
              )}

              <Card
                size="small"
                title="正文"
                bodyStyle={{
                  maxHeight: '62vh',
                  overflowY: 'auto',
                  fontSize: 13,
                  lineHeight: 1.75,
                  background: '#f8fafc',
                }}
              >
                {detail.content_md ? (
                  <div className="semi-md">
                    <ReactMarkdown remarkPlugins={[remarkGfm]}>
                      {detail.content_md}
                    </ReactMarkdown>
                  </div>
                ) : (
                  <Empty description="无正文" />
                )}
              </Card>

              <Text
                type="secondary"
                style={{ fontSize: 11, display: 'block', marginTop: 16 }}
              >
                ID: {detail.id} · post_id={detail.post_id}
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
        .semi-md { font-size: 13.5px; line-height: 1.8; }
        .semi-md h1, .semi-md h2, .semi-md h3 {
          color: #0f172a;
          margin-top: 14px;
        }
        .semi-md table {
          border-collapse: collapse;
          margin: 10px 0;
          font-size: 12px;
        }
        .semi-md th, .semi-md td {
          border: 1px solid #e2e8f0;
          padding: 5px 9px;
        }
        .semi-md th { background: #f8fafc; font-weight: 600; }
        .semi-md a { color: #2563eb; text-decoration: none; }
        .semi-md a:hover { text-decoration: underline; }
        .semi-md blockquote {
          border-left: 3px solid #cbd5e1;
          margin: 10px 0;
          padding: 4px 12px;
          color: #475569;
          background: #f1f5f9;
        }
      `}</style>
    </div>
  )
}
