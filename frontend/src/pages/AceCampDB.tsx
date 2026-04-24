/**
 * AceCamp · 基于 MongoDB (`acecamp.*`) 的视图。
 *
 * 4 个 UI 类别 (对照平台 SPA bundle 真实分类字典, 2026-04-23 起 路演/event 已移除):
 *   minutes  → 纪要  (articles.subtype=minutes, 业绩会/公司纪要)
 *   research → 调研  (articles.subtype=research, 专家调研/产业访谈)
 *   article  → 文章  (articles.subtype=article, 原创研报/白皮书)
 *   opinion  → 观点  (opinions collection, 短观点 + expected_trend)
 */
import { useCallback, useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import {
  Alert,
  Button,
  Card,
  Drawer,
  Empty,
  Input,
  List,
  Segmented,
  Space,
  Spin,
  Statistic,
  Tabs,
  Tag,
  Typography,
} from 'antd'
import {
  ReloadOutlined,
  LinkOutlined,
  ClockCircleOutlined,
  FileTextOutlined,
  AudioOutlined,
  StockOutlined,
  FundProjectionScreenOutlined,
  BankOutlined,
  DownloadOutlined,
  PlayCircleOutlined,
  EyeOutlined,
  LikeOutlined,
  StarOutlined,
  MessageOutlined,
  BulbOutlined,
  ExperimentOutlined,
  RiseOutlined,
  FallOutlined,
} from '@ant-design/icons'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'

dayjs.extend(relativeTime)

const { Text, Title } = Typography

type CategoryKey = 'minutes' | 'research' | 'article' | 'opinion'

const CATEGORY_META: Record<
  CategoryKey,
  { label: string; color: string; icon: React.ReactNode }
> = {
  minutes:  { label: '纪要', color: '#2563eb', icon: <FileTextOutlined /> },
  research: { label: '调研', color: '#a855f7', icon: <ExperimentOutlined /> },
  article:  { label: '文章', color: '#10b981', icon: <MessageOutlined /> },
  opinion:  { label: '观点', color: '#f59e0b', icon: <BulbOutlined /> },
}

const CATEGORY_ORDER: CategoryKey[] = ['minutes', 'research', 'article', 'opinion']

interface Corporation {
  id?: number | string | null
  code: string
  name: string
  exchange?: string
}

interface Item {
  id: string
  raw_id: number | string | null
  category: CategoryKey
  category_label: string
  subtype: string
  title: string
  original_title: string
  release_time: string | null
  release_time_ms: number | null
  organization: string
  organization_id: number | string | null
  corporations: Corporation[]
  hashtags: string[]
  industry_ids: (number | string)[]
  views: number
  likes: number
  favorites: number
  comment_count: number
  has_vip: boolean
  free: boolean
  need_to_pay: boolean
  has_paid: boolean
  can_download: boolean
  living: boolean
  playback: boolean
  state: string | null
  expected_trend: 'bullish' | 'bearish' | 'neutral' | null
  identity: string | null
  cover_image: string | null
  web_url: string | null
  preview: string
  content_length: number
  brief_length: number
  transcribe_length: number
  has_pdf: boolean
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
  last_7_days: { date: string; minutes?: number; research?: number; article?: number; opinion?: number }[]
  crawler_state: {
    category: string
    in_progress: boolean
    last_processed_at: string | null
    last_run_end_at: string | null
    last_run_stats: { added?: number; updated?: number; skipped?: number; failed?: number }
  }[]
  top_organizations: Record<CategoryKey, { name: string; count: number }[]>
  daily_platform_stats: Record<
    CategoryKey,
    { platform_count: number; in_db: number; missing: number }
  > | null
}

interface DetailResponse extends Item {
  content_md: string
  summary_md: string
  transcribe_md: string
  brief_md: string
  description_md: string
  source_url: string | null
  download_url: string | null
  addresses: string[]
  co_host_organizations: any[]
  expert_public_resume: string | null
  meeting_ids: (number | string)[]
}

// URL slug → backend category key
const SLUG_TO_CATEGORY: Record<string, CategoryKey> = {
  minutes: 'minutes',
  research: 'research',
  article: 'article',
  opinion: 'opinion',
  // 向后兼容: /acecamp/viewpoint 老链接 → 文章 (平台 type=original 重命名前叫 viewpoint)
  viewpoint: 'article',
  // 路演 (event) 2026-04-23 彻底移除 → 老链接 /acecamp/event 兜底跳纪要
  event: 'minutes',
}

export default function AceCampDB() {
  // URL-param 决定初始分类 (/acecamp/minutes, /acecamp/viewpoint, /acecamp/event)
  const { category: slug } = useParams<{ category?: string }>()
  const initialCategory: CategoryKey = (slug && SLUG_TO_CATEGORY[slug]) || 'minutes'

  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [statsLoading, setStatsLoading] = useState(true)
  const [statsError, setStatsError] = useState<string | null>(null)

  const [category, setCategory] = useState<CategoryKey>(initialCategory)
  const [items, setItems] = useState<Item[]>([])
  const [itemsLoading, setItemsLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [total, setTotal] = useState(0)

  const [query, setQuery] = useState('')
  const [organizationFilter, setOrganizationFilter] = useState('')
  const [tickerFilter, setTickerFilter] = useState('')
  const [hashtagFilter, setHashtagFilter] = useState('')

  const [detailOpen, setDetailOpen] = useState(false)
  const [detail, setDetail] = useState<DetailResponse | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailBodyTab, setDetailBodyTab] = useState<'content' | 'transcribe' | 'summary'>(
    'content',
  )

  // opinion trend (event 板块已移除)
  const [trend, setTrend] = useState<'bullish' | 'bearish' | 'neutral' | 'all'>('all')

  // 侧栏切换时 URL 变 → 同步 state + 重置页码
  useEffect(() => {
    const fromUrl = (slug && SLUG_TO_CATEGORY[slug]) || 'minutes'
    setCategory(fromUrl)
    setPage(1)
  }, [slug])

  const loadStats = useCallback(async () => {
    setStatsLoading(true)
    setStatsError(null)
    try {
      const res = await api.get<StatsResponse>('/acecamp-db/stats')
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
      const params: Record<string, string | number | undefined> = {
        category,
        page,
        page_size: 20,
        q: query || undefined,
        organization: organizationFilter || undefined,
        ticker: tickerFilter || undefined,
        hashtag: hashtagFilter || undefined,
      }
      if (category === 'opinion' && trend !== 'all') params.expected_trend = trend
      const res = await api.get<ListResponse>('/acecamp-db/items', { params })
      setItems(res.data.items)
      setTotal(res.data.total)
    } catch {
      setItems([])
      setTotal(0)
    } finally {
      setItemsLoading(false)
    }
  }, [category, page, query, organizationFilter, tickerFilter, hashtagFilter, trend])

  useEffect(() => {
    loadStats()
  }, [loadStats])

  useEffect(() => {
    loadItems()
  }, [loadItems])

  useEffect(() => {
    setTrend('all')
  }, [category])

  const openDetail = useCallback(
    async (item: Item) => {
      setDetailOpen(true)
      setDetailLoading(true)
      setDetail(null)
      setDetailBodyTab('content')
      try {
        const res = await api.get<DetailResponse>(
          `/acecamp-db/items/${category}/${encodeURIComponent(item.id)}`,
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

  const todayCount = stats?.today[category] ?? (stats ? 0 : null)
  const totalForCategory = stats?.per_category[category] ?? 0
  const latest = stats?.latest_per_category?.[category]
  const platformDaily = stats?.daily_platform_stats?.[category]

  const segOptions = CATEGORY_ORDER.map((k) => {
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
  })

  const trendMeta: Record<'bullish' | 'bearish' | 'neutral', { label: string; color: string; icon: React.ReactNode }> = {
    bullish: { label: '看多', color: 'green', icon: <RiseOutlined /> },
    bearish: { label: '看空', color: 'red', icon: <FallOutlined /> },
    neutral: { label: '中性', color: 'default', icon: <MessageOutlined /> },
  }

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
            <FundProjectionScreenOutlined /> AceCamp
          </Title>
          <Text type="secondary">
            acecamptech.com · 纪要 + 观点 + 调研 (Cookie 认证 · 无 PDF · 全文本)
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
          message="无法从 MongoDB 加载 AceCamp 数据"
          description={statsError}
          style={{ marginBottom: 16 }}
        />
      )}

      <Spin spinning={statsLoading}>
        <Card size="small" bodyStyle={{ padding: 14 }} style={{ marginBottom: 16 }}>
          <Space size={18} align="center" wrap>
            <Statistic
              title={
                <span style={{ color: CATEGORY_META[category].color }}>
                  {CATEGORY_META[category].icon} 今日新增 · {CATEGORY_META[category].label}
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
            {platformDaily && (
              <Text type="secondary" style={{ fontSize: 12 }}>
                平台 {platformDaily.platform_count} · 已入库 {platformDaily.in_db} ·{' '}
                {platformDaily.missing > 0 ? (
                  <span style={{ color: '#f59e0b' }}>缺失 {platformDaily.missing}</span>
                ) : (
                  <span style={{ color: '#10b981' }}>无缺失</span>
                )}
              </Text>
            )}
          </Space>
        </Card>
      </Spin>

      <Card size="small">
        <Space direction="vertical" size={10} style={{ width: '100%', marginBottom: 12 }}>
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
              placeholder="搜索标题 / 摘要 / 正文 / 转写"
              allowClear
              style={{ width: 320 }}
              onSearch={(v) => {
                setQuery(v)
                setPage(1)
              }}
            />
            <Input
              placeholder="机构"
              allowClear
              prefix={<BankOutlined />}
              style={{ width: 160 }}
              onPressEnter={(e) => {
                setOrganizationFilter((e.target as HTMLInputElement).value)
                setPage(1)
              }}
            />
            <Input
              placeholder="公司 Ticker / 名称"
              allowClear
              prefix={<StockOutlined />}
              style={{ width: 180 }}
              onPressEnter={(e) => {
                setTickerFilter((e.target as HTMLInputElement).value)
                setPage(1)
              }}
            />
            <Input
              placeholder="标签 Hashtag"
              allowClear
              style={{ width: 160 }}
              onPressEnter={(e) => {
                setHashtagFilter((e.target as HTMLInputElement).value)
                setPage(1)
              }}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              共 {total} 条
            </Text>
          </Space>
          {category === 'opinion' && (
            <Space wrap size={6}>
              <Text type="secondary" style={{ fontSize: 12 }}>观点方向:</Text>
              <Segmented
                size="small"
                value={trend}
                onChange={(v) => {
                  setTrend(v as 'bullish' | 'bearish' | 'neutral' | 'all')
                  setPage(1)
                }}
                options={[
                  { label: '全部', value: 'all' },
                  { label: <><RiseOutlined /> 看多</>, value: 'bullish' },
                  { label: <><FallOutlined /> 看空</>, value: 'bearish' },
                  { label: '中性', value: 'neutral' },
                ]}
              />
            </Space>
          )}
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
            const meta = CATEGORY_META[item.category] || CATEGORY_META.minutes
            return (
              <List.Item
                key={item.id}
                style={{ cursor: 'pointer' }}
                onClick={() => openDetail(item)}
              >
                <List.Item.Meta
                  avatar={
                    item.cover_image ? (
                      <img
                        src={item.cover_image}
                        alt=""
                        style={{
                          width: 60,
                          height: 60,
                          objectFit: 'cover',
                          borderRadius: 6,
                        }}
                      />
                    ) : null
                  }
                  title={
                    <Space size={6} wrap>
                      <Tag color={meta.color} style={{ color: '#fff', border: 'none' }}>
                        {meta.icon} {item.category_label}
                      </Tag>
                      {item.expected_trend && trendMeta[item.expected_trend] && (
                        <Tag color={trendMeta[item.expected_trend].color} style={{ fontSize: 11 }}>
                          {trendMeta[item.expected_trend].icon} {trendMeta[item.expected_trend].label}
                        </Tag>
                      )}
                      {item.living && (
                        <Tag color="red" style={{ fontSize: 11 }}>
                          <PlayCircleOutlined /> 直播中
                        </Tag>
                      )}
                      {item.playback && (
                        <Tag color="purple" style={{ fontSize: 11 }}>
                          <AudioOutlined /> 回放
                        </Tag>
                      )}
                      {item.has_vip && (
                        <Tag color="gold" style={{ fontSize: 11 }}>
                          VIP
                        </Tag>
                      )}
                      {item.free && (
                        <Tag color="green" style={{ fontSize: 11 }}>
                          免费
                        </Tag>
                      )}
                      {item.need_to_pay && !item.has_paid && (
                        <Tag color="orange" style={{ fontSize: 11 }}>
                          付费
                        </Tag>
                      )}
                      {item.corporations.slice(0, 3).map((c) => (
                        <Tag
                          key={c.code || c.name || String(c.id)}
                          color="cyan"
                          style={{ fontSize: 11 }}
                        >
                          <StockOutlined /> {c.name || c.code}
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
                        {item.organization && (
                          <Text type="secondary">
                            <BankOutlined /> {item.organization}
                          </Text>
                        )}
                        {item.hashtags.slice(0, 5).map((h) => (
                          <Tag key={h} style={{ fontSize: 11 }}>
                            #{h}
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
                        {item.content_length > 0 && (
                          <span>
                            <FileTextOutlined /> {item.content_length.toLocaleString()} 字
                          </span>
                        )}
                        {item.transcribe_length > 0 && (
                          <span>
                            <AudioOutlined /> 转写 {item.transcribe_length.toLocaleString()} 字
                          </span>
                        )}
                        {item.views > 0 && (
                          <span>
                            <EyeOutlined /> {item.views}
                          </span>
                        )}
                        {item.likes > 0 && (
                          <span>
                            <LikeOutlined /> {item.likes}
                          </span>
                        )}
                        {item.favorites > 0 && (
                          <span>
                            <StarOutlined /> {item.favorites}
                          </span>
                        )}
                        {item.comment_count > 0 && (
                          <span>
                            <MessageOutlined /> {item.comment_count}
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
            {detail?.download_url && (
              <Button
                size="small"
                type="primary"
                icon={<DownloadOutlined />}
                href={detail.download_url}
                target="_blank"
              >
                下载
              </Button>
            )}
            {detail?.web_url && (
              <a href={detail.web_url} target="_blank" rel="noreferrer">
                <LinkOutlined /> 原文
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
                  color={CATEGORY_META[detail.category]?.color || '#2563eb'}
                  style={{ color: '#fff', border: 'none' }}
                >
                  {CATEGORY_META[detail.category]?.icon} {detail.category_label}
                </Tag>
                {detail.release_time && (
                  <Tag icon={<ClockCircleOutlined />}>{detail.release_time}</Tag>
                )}
                {detail.organization && (
                  <Tag icon={<BankOutlined />}>{detail.organization}</Tag>
                )}
                {detail.state && <Tag>{detail.state}</Tag>}
                {detail.living && <Tag color="red">直播中</Tag>}
                {detail.playback && <Tag color="purple">回放</Tag>}
                {detail.has_vip && <Tag color="gold">VIP</Tag>}
                {detail.need_to_pay && !detail.has_paid && <Tag color="orange">付费</Tag>}
                {detail.corporations.map((c) => (
                  <Tag key={c.code || c.name || String(c.id)} color="cyan">
                    <StockOutlined /> {c.name || c.code}
                  </Tag>
                ))}
                {detail.hashtags.map((h) => (
                  <Tag key={h}>#{h}</Tag>
                ))}
              </Space>

              {(detail.addresses?.length > 0 || detail.expert_public_resume) && (
                <div style={{ marginBottom: 8, fontSize: 12, color: '#64748b' }}>
                  {detail.addresses?.length > 0 && (
                    <span style={{ marginRight: 12 }}>
                      地点: {detail.addresses.join(' / ')}
                    </span>
                  )}
                  {detail.expert_public_resume && (
                    <span>专家简介: {detail.expert_public_resume}</span>
                  )}
                </div>
              )}

              {detail.original_title && detail.original_title !== detail.title && (
                <div style={{ marginBottom: 8, fontSize: 13 }}>
                  <Text type="secondary">原标题: </Text>
                  <Text>{detail.original_title}</Text>
                </div>
              )}

              {detail.summary_md && (
                <Card
                  size="small"
                  title="核心摘要"
                  style={{ marginTop: 8, marginBottom: 8 }}
                  bodyStyle={{
                    fontSize: 13,
                    lineHeight: 1.75,
                    background: '#fffbeb',
                  }}
                >
                  <div className="acecamp-md">
                    <ReactMarkdown remarkPlugins={[remarkGfm]}>
                      {detail.summary_md}
                    </ReactMarkdown>
                  </div>
                </Card>
              )}

              {(() => {
                const tabItems: {
                  key: 'content' | 'transcribe' | 'summary'
                  label: React.ReactNode
                  body: string
                }[] = []
                if (detail.content_md) {
                  tabItems.push({
                    key: 'content',
                    label: `正文 (${detail.content_md.length.toLocaleString()} 字)`,
                    body: detail.content_md,
                  })
                }
                if (detail.transcribe_md) {
                  tabItems.push({
                    key: 'transcribe',
                    label: `转写 (${detail.transcribe_md.length.toLocaleString()} 字)`,
                    body: detail.transcribe_md,
                  })
                }
                if (detail.description_md && !detail.content_md) {
                  tabItems.push({
                    key: 'summary',
                    label: `说明 (${detail.description_md.length.toLocaleString()} 字)`,
                    body: detail.description_md,
                  })
                }
                if (tabItems.length === 0) {
                  return <Empty description="无正文" />
                }
                const active = tabItems.find((t) => t.key === detailBodyTab) || tabItems[0]
                return (
                  <Card size="small" style={{ marginTop: 8 }} bodyStyle={{ padding: 0 }}>
                    <Tabs
                      activeKey={active.key}
                      size="small"
                      onChange={(k) => setDetailBodyTab(k as any)}
                      items={tabItems.map((t) => ({
                        key: t.key,
                        label: t.label,
                        children: (
                          <div
                            style={{
                              maxHeight: '60vh',
                              overflowY: 'auto',
                              padding: '12px 16px',
                              background: '#f8fafc',
                              fontSize: 13,
                              lineHeight: 1.75,
                            }}
                          >
                            <div className="acecamp-md">
                              <ReactMarkdown remarkPlugins={[remarkGfm]}>
                                {t.body}
                              </ReactMarkdown>
                            </div>
                          </div>
                        ),
                      }))}
                    />
                  </Card>
                )
              })()}

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
        .acecamp-md {
          font-size: 13.5px;
          line-height: 1.8;
        }
        .acecamp-md h1, .acecamp-md h2, .acecamp-md h3 {
          color: #0f172a;
          margin-top: 14px;
        }
        .acecamp-md table {
          border-collapse: collapse;
          margin: 10px 0;
          font-size: 12px;
        }
        .acecamp-md th, .acecamp-md td {
          border: 1px solid #e2e8f0;
          padding: 5px 9px;
        }
        .acecamp-md th { background: #f8fafc; font-weight: 600; }
        .acecamp-md a { color: #2563eb; text-decoration: none; }
        .acecamp-md a:hover { text-decoration: underline; }
      `}</style>
    </div>
  )
}
