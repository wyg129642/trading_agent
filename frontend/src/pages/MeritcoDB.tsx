/**
 * 久谦中台 · Meritco 数据库视图
 *
 * 直接读取 crawl/meritco_crawl/scraper.py 写入的 MongoDB 数据 (`meritco.forum`)。
 * 3 种论坛类型 (forum_type):
 *   1 = 活动 / 活动预告
 *   2 = 专业内容 (纪要 + 研报)
 *   3 = 久谦自研 (含 调研周报 / 医药周报 等)
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import { useParams } from 'react-router-dom'
import {
  Alert,
  Card,
  Col,
  Drawer,
  Empty,
  Input,
  List,
  Row,
  Select,
  Space,
  Spin,
  Statistic,
  Tabs,
  Tag,
  Typography,
  Segmented,
  Badge,
  Tooltip,
} from 'antd'
import {
  ReloadOutlined,
  LinkOutlined,
  ClockCircleOutlined,
  BookOutlined,
  FileTextOutlined,
  TagOutlined,
  TeamOutlined,
  CrownOutlined,
  RocketOutlined,
  ReadOutlined,
  FilePdfOutlined,
  AimOutlined,
  ProfileOutlined,
  UserOutlined,
  EyeOutlined,
  DownloadOutlined,
  FireOutlined,
  CalendarOutlined,
  ThunderboltOutlined,
  ScheduleOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import api from '../services/api'
import TickerTagsTabs, { TickerTags } from '../components/TickerTagsTabs'

dayjs.extend(relativeTime)

const { Text, Title, Paragraph } = Typography

// Map forum_type → label / color / icon
const FORUM_TYPE_META: Record<
  number,
  { label: string; color: string; bg: string; icon: React.ReactNode }
> = {
  1: { label: '活动', color: '#f59e0b', bg: '#fffbeb', icon: <RocketOutlined /> },
  2: { label: '专业内容', color: '#2563eb', bg: '#eff6ff', icon: <ReadOutlined /> },
  3: { label: '久谦自研', color: '#8b5cf6', bg: '#f5f3ff', icon: <CrownOutlined /> },
}

interface StatsResponse {
  total: number
  today: number
  last_7_days: { date: string; count: number }[]
  per_forum_type: { forum_type: number; label: string; count: number }[]
  top_authors: { name: string; count: number }[]
  top_industries: { name: string; count: number }[]
  top_targets: { name: string; count: number }[]
  top_keywords: { name: string; count: number }[]
  latest_release_time: string | null
  crawler_state: {
    forum_type: number | null
    label: string
    in_progress: boolean
    last_processed_at: string | null
    last_run_end_at: string | null
    last_run_stats: { added?: number; skipped?: number; failed?: number }
    top_id: any
  }[]
  daily_platform_stats: {
    for_type: number
    total_on_platform: number
    in_db: number
    not_in_db: number
    by_author_top10: [string, number][]
    by_industry_top10: [string, number][]
  } | null
  content_coverage: {
    summary: number
    insight: number
    content: number
    expert_content: number
    pdf: number
  }
}

interface PdfFile {
  name: string
  size_bytes: number
  size_display: string
  preview_url: string
  has_local_pdf: boolean
  local_pdf_url: string
  local_pdf_size: number
}

interface ForumBrief {
  id: string
  forum_id: string
  forum_type: number
  forum_type_label: string
  title: string
  release_time: string | null
  meeting_time: string | null
  web_url: string | null
  industry: string
  author: string
  authors: string[]
  experts: string[]
  expert_type_name: string
  report_type_name: string
  related_targets: string[]
  keyword_arr: string[]
  hot_flag: boolean
  is_top: number
  language: number | null
  pdf_files: PdfFile[]
  meeting_link: string
  preview: string
  stats: {
    content_chars: number
    insight_chars: number
    summary_chars: number
    experts: number
    related_targets: number
  }
  has_summary: boolean
  has_insight: boolean
  has_content: boolean
  has_expert_content: boolean
  is_weekly_report?: boolean
  crawled_at: string | null
}

interface ForumDetail extends ForumBrief {
  summary_md: string
  insight_md: string
  content_md: string
  topic_md: string
  background_md: string
  expert_content_md: string
  pdf_text_md: string
  ticker_tags?: TickerTags
}

interface ListResponse {
  items: ForumBrief[]
  total: number
  page: number
  page_size: number
  has_next: boolean
}

// URL slug → forum_type. Sidebar routes /meritco/minutes and /meritco/research
// pre-select the forum_type before the page renders.
const SLUG_TO_TYPE: Record<string, number> = {
  minutes: 2,
  research: 3,
  events: 1,
  forum: 0,
  weekly: -1, // virtual: forum_type=3 + 周报
}

export default function MeritcoDB() {
  const { slug } = useParams<{ slug?: string }>()
  const initialType: number = slug && slug in SLUG_TO_TYPE ? SLUG_TO_TYPE[slug] : 0

  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [statsLoading, setStatsLoading] = useState(true)
  const [statsError, setStatsError] = useState<string | null>(null)

  const [items, setItems] = useState<ForumBrief[]>([])
  const [itemsLoading, setItemsLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [total, setTotal] = useState(0)

  // 0=all, 1/2/3=forum_type, -1=weekly报 virtual view
  const [activeType, setActiveType] = useState<number>(initialType)

  // sidebar route change → sync
  useEffect(() => {
    const fromUrl: number = slug && slug in SLUG_TO_TYPE ? SLUG_TO_TYPE[slug] : 0
    setActiveType(fromUrl)
    setPage(1)
  }, [slug])
  const [query, setQuery] = useState('')
  const [industryFilter, setIndustryFilter] = useState<string | undefined>()
  const [authorFilter, setAuthorFilter] = useState<string | undefined>()
  const [targetFilter, setTargetFilter] = useState<string | undefined>()

  const [detailOpen, setDetailOpen] = useState(false)
  const [detail, setDetail] = useState<ForumDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [activeTab, setActiveTab] = useState('insight')

  const loadStats = useCallback(async () => {
    setStatsLoading(true)
    setStatsError(null)
    try {
      const res = await api.get<StatsResponse>('/meritco-db/stats')
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
      // Weekly虚拟视图: forum_type=3 + q 含 周报
      const isWeekly = activeType === -1
      const res = await api.get<ListResponse>('/meritco-db/forum', {
        params: {
          page,
          page_size: 20,
          forum_type: isWeekly ? 3 : activeType || undefined,
          q: isWeekly ? (query ? `${query} 周报` : '周报') : query || undefined,
          industry: industryFilter || undefined,
          author: authorFilter || undefined,
          target: targetFilter || undefined,
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
  }, [page, activeType, query, industryFilter, authorFilter, targetFilter])

  useEffect(() => {
    loadStats()
  }, [loadStats])

  useEffect(() => {
    loadItems()
  }, [loadItems])

  const openDetail = useCallback(async (item: ForumBrief) => {
    setDetailOpen(true)
    setDetailLoading(true)
    setDetail(null)
    // 默认打开第一个有内容的 tab: 速览→摘要→正文
    const firstTab = item.has_insight
      ? 'insight'
      : item.has_summary
      ? 'summary'
      : item.has_content
      ? 'content'
      : 'insight'
    setActiveTab(firstTab)
    try {
      const res = await api.get<ForumDetail>(
        `/meritco-db/forum/${encodeURIComponent(item.id)}`,
      )
      setDetail(res.data)
    } catch {
      setDetail(null)
    } finally {
      setDetailLoading(false)
    }
  }, [])

  const openLocalPdf = useCallback(async (file: PdfFile, filename: string, download: boolean) => {
    try {
      const raw = file.local_pdf_url || ''
      if (!raw) return
      const path = raw.startsWith('/api') ? raw.slice(4) : raw
      const res = await api.get(path, {
        params: download ? { download: 1 } : {},
        responseType: 'blob',
      })
      const blob = new Blob([res.data], { type: 'application/pdf' })
      const url = URL.createObjectURL(blob)
      if (download) {
        const a = document.createElement('a')
        a.href = url
        a.download = filename || file.name || 'meritco.pdf'
        document.body.appendChild(a)
        a.click()
        document.body.removeChild(a)
      } else {
        window.open(url, '_blank', 'noopener')
      }
      setTimeout(() => URL.revokeObjectURL(url), 60_000)
    } catch (err: any) {
      const msg = err?.response?.data?.detail || err?.message || 'PDF 加载失败'
      // eslint-disable-next-line no-alert
      alert(msg)
    }
  }, [])

  const typeSegmentOptions = useMemo(() => {
    const all = { label: `全部 (${stats?.total ?? 0})`, value: 0 }
    const weekly = {
      label: (
        <span>
          <ScheduleOutlined style={{ color: '#ec4899' }} /> 周报
        </span>
      ),
      value: -1,
    }
    const each = [1, 2, 3].map((ft) => {
      const found = stats?.per_forum_type.find((p) => p.forum_type === ft)
      const meta = FORUM_TYPE_META[ft]
      return {
        label: (
          <span style={{ color: meta.color }}>
            {meta.icon} {meta.label} ({found?.count ?? 0})
          </span>
        ),
        value: ft,
      }
    })
    return [all, ...each, weekly]
  }, [stats])

  // 7-day peak for mini histogram
  const sevenDayMax = useMemo(
    () => Math.max(1, ...(stats?.last_7_days?.map((d) => d.count) || [1])),
    [stats],
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
            <CrownOutlined /> 久谦中台 · Meritco
          </Title>
          <Text type="secondary">
            research.meritco-group.com · 活动 / 专业内容 / 久谦自研 / 调研周报
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
          message="无法从 MongoDB 加载 Meritco 数据"
          description={statsError}
          style={{ marginBottom: 16 }}
        />
      )}

      <Spin spinning={statsLoading}>
        <Card size="small" bodyStyle={{ padding: 14 }} style={{ marginBottom: 16 }}>
          <Space size={18} align="center" wrap>
            <Statistic
              title={
                <span style={{ color: '#8b5cf6' }}>
                  <ClockCircleOutlined /> 今日新增
                </span>
              }
              value={stats?.today ?? 0}
              valueStyle={{ color: '#8b5cf6', fontSize: 28 }}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              {dayjs().format('YYYY-MM-DD')}
              {stats?.latest_release_time && (
                <> · 最近发布 {dayjs(stats.latest_release_time).format('MM-DD HH:mm')}</>
              )}
              {typeof stats?.total === 'number' && <> · 总条目 {stats.total}</>}
            </Text>
          </Space>
        </Card>

        <Row gutter={12} style={{ marginBottom: 16 }}>
          <Col xs={24} sm={12} md={8}>
            <Card
              size="small"
              bodyStyle={{ padding: 14 }}
            >
              <Space align="center" style={{ marginBottom: 6 }}>
                <CalendarOutlined style={{ color: '#8b5cf6' }} />
                <Text type="secondary" style={{ fontSize: 12 }}>
                  近 7 日发布趋势
                </Text>
              </Space>
              <div
                style={{
                  display: 'flex',
                  alignItems: 'flex-end',
                  gap: 4,
                  height: 48,
                  marginTop: 8,
                }}
              >
                {(stats?.last_7_days || []).map((d) => (
                  <Tooltip key={d.date} title={`${d.date} · ${d.count} 条`}>
                    <div
                      style={{
                        flex: 1,
                        background: '#8b5cf6',
                        height: `${(d.count / sevenDayMax) * 100}%`,
                        minHeight: 4,
                        borderRadius: '3px 3px 0 0',
                        cursor: 'pointer',
                      }}
                    />
                  </Tooltip>
                ))}
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 2 }}>
                {(stats?.last_7_days || []).map((d) => (
                  <span
                    key={d.date}
                    style={{ fontSize: 10, color: '#94a3b8', flex: 1, textAlign: 'center' }}
                  >
                    {d.date.slice(5)}
                  </span>
                ))}
              </div>
            </Card>
          </Col>
          <Col xs={24} sm={12} md={8}>
            <Card
              size="small"
              bodyStyle={{ padding: 14 }}
            >
              <Space align="center" style={{ marginBottom: 6 }}>
                <AimOutlined style={{ color: '#2563eb' }} />
                <Text type="secondary" style={{ fontSize: 12 }}>
                  Top 关联标的
                </Text>
              </Space>
              <div style={{ marginTop: 6 }}>
                {(stats?.top_targets || []).slice(0, 8).map((t) => (
                  <Tag
                    key={t.name}
                    color="cyan"
                    style={{
                      fontSize: 11,
                      cursor: 'pointer',
                      marginBottom: 4,
                      borderRadius: 10,
                    }}
                    onClick={() => {
                      setTargetFilter(t.name)
                      setPage(1)
                    }}
                  >
                    {t.name} · {t.count}
                  </Tag>
                ))}
                {(stats?.top_targets?.length ?? 0) === 0 && (
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    暂无
                  </Text>
                )}
              </div>
            </Card>
          </Col>
          <Col xs={24} sm={24} md={8}>
            <Card
              size="small"
              bodyStyle={{ padding: 14 }}
            >
              <Space align="center" style={{ marginBottom: 6 }}>
                <FireOutlined style={{ color: '#f59e0b' }} />
                <Text type="secondary" style={{ fontSize: 12 }}>
                  Top 行业 / 作者
                </Text>
              </Space>
              <div style={{ marginTop: 6 }}>
                {(stats?.top_industries || []).slice(0, 5).map((i) => (
                  <Tag
                    key={`ind-${i.name}`}
                    style={{ fontSize: 11, cursor: 'pointer', marginBottom: 4 }}
                    onClick={() => {
                      setIndustryFilter(i.name)
                      setPage(1)
                    }}
                  >
                    {i.name} · {i.count}
                  </Tag>
                ))}
                {(stats?.top_authors || []).slice(0, 3).map((a) => (
                  <Tag
                    key={`au-${a.name}`}
                    color="geekblue"
                    style={{ fontSize: 11, cursor: 'pointer', marginBottom: 4 }}
                    onClick={() => {
                      setAuthorFilter(a.name)
                      setPage(1)
                    }}
                  >
                    <UserOutlined /> {a.name} · {a.count}
                  </Tag>
                ))}
              </div>
            </Card>
          </Col>
        </Row>
      </Spin>

      {/* --- type selector + filter bar --- */}
      <Card size="small">
        <Space direction="vertical" size={10} style={{ width: '100%', marginBottom: 12 }}>
          <Segmented
            value={activeType}
            onChange={(v) => {
              setActiveType(Number(v))
              setPage(1)
            }}
            options={typeSegmentOptions as any}
            size="middle"
          />
          <Space wrap>
            <Input.Search
              placeholder="标题 / 摘要 / 速览 搜索"
              allowClear
              style={{ width: 280 }}
              onSearch={(v) => {
                setQuery(v)
                setPage(1)
              }}
            />
            <Select
              placeholder="行业"
              allowClear
              value={industryFilter}
              onChange={(v) => {
                setIndustryFilter(v)
                setPage(1)
              }}
              style={{ width: 160 }}
              options={(stats?.top_industries || []).map((i) => ({
                value: i.name,
                label: `${i.name} (${i.count})`,
              }))}
            />
            <Select
              placeholder="作者"
              allowClear
              value={authorFilter}
              onChange={(v) => {
                setAuthorFilter(v)
                setPage(1)
              }}
              style={{ width: 160 }}
              options={(stats?.top_authors || []).map((a) => ({
                value: a.name,
                label: `${a.name} (${a.count})`,
              }))}
            />
            <Select
              placeholder="关联标的"
              allowClear
              value={targetFilter}
              onChange={(v) => {
                setTargetFilter(v)
                setPage(1)
              }}
              style={{ width: 180 }}
              options={(stats?.top_targets || []).map((t) => ({
                value: t.name,
                label: `${t.name} (${t.count})`,
              }))}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              共 <Text strong>{total}</Text> 条
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
            const meta = FORUM_TYPE_META[item.forum_type] || FORUM_TYPE_META[2]
            const cleanPreview = (item.preview || '').replace(/[#*`_]/g, '').replace(/\n+/g, ' ')
            return (
              <List.Item
                key={item.id}
                style={{
                  cursor: 'pointer',
                  borderLeft: `3px solid ${meta.color}`,
                  paddingLeft: 12,
                }}
                onClick={() => openDetail(item)}
              >
                <List.Item.Meta
                  title={
                    <Space size={6} wrap>
                      <Tag
                        color={meta.color}
                        style={{
                          color: '#fff',
                          border: 'none',
                          borderRadius: 10,
                          fontSize: 11,
                          padding: '0 8px',
                        }}
                      >
                        {meta.icon} {item.forum_type_label}
                      </Tag>
                      {item.is_weekly_report && (
                        <Tag
                          color="#ec4899"
                          style={{
                            color: '#fff',
                            border: 'none',
                            borderRadius: 10,
                            fontSize: 11,
                            padding: '0 8px',
                          }}
                        >
                          <ScheduleOutlined /> 周报
                        </Tag>
                      )}
                      {item.hot_flag && (
                        <Tag
                          color="#dc2626"
                          style={{
                            color: '#fff',
                            border: 'none',
                            borderRadius: 10,
                            fontSize: 11,
                          }}
                        >
                          <FireOutlined /> 热门
                        </Tag>
                      )}
                      {item.is_top > 0 && (
                        <Tag color="gold" style={{ fontSize: 11, borderRadius: 10 }}>
                          置顶
                        </Tag>
                      )}
                      <Text
                        strong
                        style={{
                          fontSize: 14,
                          color: '#0f172a',
                          lineHeight: 1.5,
                        }}
                      >
                        {item.title}
                      </Text>
                    </Space>
                  }
                  description={
                    <Space direction="vertical" size={6} style={{ width: '100%' }}>
                      <Space size={10} wrap style={{ fontSize: 12 }}>
                        <Text type="secondary" style={{ fontSize: 12 }}>
                          <ClockCircleOutlined /> {item.release_time || '—'}
                        </Text>
                        {item.author && (
                          <Tag
                            color="geekblue"
                            style={{ fontSize: 11, borderRadius: 10 }}
                            icon={<UserOutlined />}
                          >
                            {item.author}
                          </Tag>
                        )}
                        {item.industry && (
                          <Tag style={{ fontSize: 11, borderRadius: 10 }}>{item.industry}</Tag>
                        )}
                        {item.expert_type_name && (
                          <Tag color="volcano" style={{ fontSize: 11, borderRadius: 10 }}>
                            {item.expert_type_name}
                          </Tag>
                        )}
                        {item.related_targets.slice(0, 5).map((t) => (
                          <Tag key={t} color="cyan" style={{ fontSize: 11, borderRadius: 10 }}>
                            {t}
                          </Tag>
                        ))}
                        {item.related_targets.length > 5 && (
                          <Text type="secondary" style={{ fontSize: 11 }}>
                            +{item.related_targets.length - 5}
                          </Text>
                        )}
                        {item.pdf_files.length > 0 && (
                          <Tag
                            color="#fef2f2"
                            style={{
                              fontSize: 11,
                              color: '#dc2626',
                              border: '1px solid #fecaca',
                              borderRadius: 10,
                            }}
                          >
                            <FilePdfOutlined /> PDF
                            {item.pdf_files.length > 1 && `×${item.pdf_files.length}`}
                          </Tag>
                        )}
                      </Space>
                      {cleanPreview && (
                        <Paragraph
                          style={{
                            fontSize: 13,
                            color: '#475569',
                            margin: 0,
                            lineHeight: 1.7,
                          }}
                          ellipsis={{ rows: 2, tooltip: cleanPreview }}
                        >
                          {cleanPreview}
                        </Paragraph>
                      )}
                      <Space size={10} style={{ fontSize: 11, color: '#94a3b8' }}>
                        {item.stats.insight_chars > 0 && (
                          <span>
                            <BookOutlined /> 速览 {item.stats.insight_chars}
                          </span>
                        )}
                        {item.stats.summary_chars > 0 && (
                          <span>
                            <ThunderboltOutlined /> 摘要 {item.stats.summary_chars}
                          </span>
                        )}
                        {item.stats.content_chars > 0 && (
                          <span>
                            <FileTextOutlined /> 正文 {item.stats.content_chars}
                          </span>
                        )}
                        {item.stats.related_targets > 0 && (
                          <span>
                            <AimOutlined /> 标的 {item.stats.related_targets}
                          </span>
                        )}
                        {item.stats.experts > 0 && (
                          <span>
                            <TeamOutlined /> 专家 {item.stats.experts}
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

      {/* --- detail drawer --- */}
      <Drawer
        title={
          <Space>
            {detail && (
              <Tag
                color={FORUM_TYPE_META[detail.forum_type]?.color}
                style={{ color: '#fff', border: 'none', borderRadius: 10 }}
              >
                {FORUM_TYPE_META[detail.forum_type]?.icon}{' '}
                {detail.forum_type_label}
              </Tag>
            )}
            {detail?.is_weekly_report && (
              <Tag color="#ec4899" style={{ color: '#fff', border: 'none', borderRadius: 10 }}>
                <ScheduleOutlined /> 周报
              </Tag>
            )}
            <span style={{ fontSize: 15 }}>{detail?.title || '详情'}</span>
          </Space>
        }
        open={detailOpen}
        onClose={() => setDetailOpen(false)}
        width={920}
        extra={
          <Space>
            {detail?.meeting_link && (
              <a href={detail.meeting_link} target="_blank" rel="noreferrer">
                <LinkOutlined /> 原页
              </a>
            )}
          </Space>
        }
      >
        <Spin spinning={detailLoading}>
          {detail ? (
            <div>
              {/* meta bar */}
              <div
                style={{
                  background: FORUM_TYPE_META[detail.forum_type]?.bg || '#f8fafc',
                  padding: '10px 14px',
                  borderRadius: 8,
                  marginBottom: 14,
                  display: 'flex',
                  flexWrap: 'wrap',
                  gap: 8,
                  fontSize: 12,
                  color: '#475569',
                }}
              >
                {detail.release_time && (
                  <span>
                    <ClockCircleOutlined /> {detail.release_time}
                  </span>
                )}
                {detail.author && (
                  <span>
                    <UserOutlined /> {detail.author}
                  </span>
                )}
                {detail.industry && <span>🏭 {detail.industry}</span>}
                {detail.expert_type_name && (
                  <span style={{ color: '#dc2626' }}>👤 {detail.expert_type_name}</span>
                )}
                {detail.hot_flag && <span style={{ color: '#dc2626' }}>🔥 热门</span>}
              </div>

              {detail.related_targets.length > 0 && (
                <div style={{ marginBottom: 8 }}>
                  <Text type="secondary" style={{ marginRight: 8, fontSize: 12 }}>
                    关联标的:
                  </Text>
                  {detail.related_targets.map((t) => (
                    <Tag key={t} color="cyan" style={{ borderRadius: 10, marginBottom: 3 }}>
                      {t}
                    </Tag>
                  ))}
                </div>
              )}
              <TickerTagsTabs tags={detail.ticker_tags} />
              {detail.experts.length > 0 && (
                <div style={{ marginBottom: 8 }}>
                  <Text type="secondary" style={{ marginRight: 8, fontSize: 12 }}>
                    专家:
                  </Text>
                  {detail.experts.map((e) => (
                    <Tag key={e} color="purple" style={{ borderRadius: 10, marginBottom: 3 }}>
                      {e}
                    </Tag>
                  ))}
                </div>
              )}
              {detail.keyword_arr.length > 0 && (
                <div style={{ marginBottom: 12 }}>
                  <Text type="secondary" style={{ marginRight: 8, fontSize: 12 }}>
                    关键词:
                  </Text>
                  {detail.keyword_arr.slice(0, 12).map((k) => (
                    <Tag key={k} style={{ borderRadius: 10, marginBottom: 3 }}>
                      {k}
                    </Tag>
                  ))}
                </div>
              )}

              {detail.pdf_files.length > 0 && (
                <Card
                  size="small"
                  title={
                    <Space>
                      <FilePdfOutlined style={{ color: '#dc2626' }} />
                      <span>PDF 附件 ({detail.pdf_files.length})</span>
                    </Space>
                  }
                  style={{ marginBottom: 14, borderLeft: '3px solid #dc2626', borderRadius: 8 }}
                  bodyStyle={{ padding: '12px 14px' }}
                  extra={
                    <Text type="secondary" style={{ fontSize: 11 }}>
                      {detail.pdf_files.some((f) => f.has_local_pdf)
                        ? '本地已下载 · 无需再登录久谦'
                        : '需先登录 research.meritco-group.com'}
                    </Text>
                  }
                >
                  <Space direction="vertical" size={10} style={{ width: '100%' }}>
                    {detail.pdf_files.map((f, idx) => (
                      <div
                        key={`${f.name}-${idx}`}
                        style={{
                          display: 'flex',
                          alignItems: 'center',
                          gap: 12,
                          padding: '10px 12px',
                          background: 'linear-gradient(to right, #fef2f2, #fff 80%)',
                          border: '1px solid #fecaca',
                          borderRadius: 6,
                        }}
                      >
                        <div
                          style={{
                            width: 44,
                            height: 54,
                            background: '#dc2626',
                            color: '#fff',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            borderRadius: 4,
                            fontSize: 11,
                            fontWeight: 700,
                            flexShrink: 0,
                          }}
                        >
                          PDF
                        </div>
                        <div style={{ flex: 1, minWidth: 0 }}>
                          <div
                            style={{
                              fontSize: 13,
                              fontWeight: 500,
                              color: '#0f172a',
                              wordBreak: 'break-all',
                              lineHeight: 1.5,
                            }}
                          >
                            {f.name}
                          </div>
                          <div style={{ fontSize: 11, color: '#94a3b8', marginTop: 2 }}>
                            {f.size_display || '大小未知'}
                          </div>
                        </div>
                        <Space size={6}>
                          {f.has_local_pdf ? (
                            <>
                              <button
                                onClick={(e) => {
                                  e.stopPropagation()
                                  openLocalPdf(f, f.name, false)
                                }}
                                title="在浏览器内打开 PDF"
                                style={{
                                  display: 'inline-flex',
                                  alignItems: 'center',
                                  gap: 4,
                                  padding: '6px 14px',
                                  background: '#dc2626',
                                  color: '#fff',
                                  border: 'none',
                                  borderRadius: 4,
                                  fontSize: 12,
                                  fontWeight: 500,
                                  cursor: 'pointer',
                                  whiteSpace: 'nowrap',
                                }}
                              >
                                <EyeOutlined /> 预览
                              </button>
                              <button
                                onClick={(e) => {
                                  e.stopPropagation()
                                  openLocalPdf(f, f.name, true)
                                }}
                                title="下载到本地"
                                style={{
                                  display: 'inline-flex',
                                  alignItems: 'center',
                                  gap: 4,
                                  padding: '6px 12px',
                                  background: '#fff',
                                  color: '#dc2626',
                                  border: '1px solid #dc2626',
                                  borderRadius: 4,
                                  fontSize: 12,
                                  cursor: 'pointer',
                                  whiteSpace: 'nowrap',
                                }}
                              >
                                <DownloadOutlined /> 下载
                              </button>
                              {f.preview_url && (
                                <a
                                  href={f.preview_url}
                                  target="_blank"
                                  rel="noreferrer"
                                  onClick={(e) => e.stopPropagation()}
                                  title="久谦原站预览页"
                                  style={{
                                    display: 'inline-flex',
                                    alignItems: 'center',
                                    gap: 4,
                                    padding: '6px 10px',
                                    background: 'transparent',
                                    color: '#64748b',
                                    border: '1px solid #cbd5e1',
                                    borderRadius: 4,
                                    fontSize: 12,
                                    textDecoration: 'none',
                                    whiteSpace: 'nowrap',
                                  }}
                                >
                                  <LinkOutlined /> 原站
                                </a>
                              )}
                            </>
                          ) : f.preview_url ? (
                            <>
                              <a
                                href={f.preview_url}
                                target="_blank"
                                rel="noreferrer"
                                onClick={(e) => e.stopPropagation()}
                                title="在新标签页打开久谦预览"
                                style={{
                                  display: 'inline-flex',
                                  alignItems: 'center',
                                  gap: 4,
                                  padding: '6px 14px',
                                  background: '#dc2626',
                                  color: '#fff',
                                  borderRadius: 4,
                                  fontSize: 12,
                                  fontWeight: 500,
                                  textDecoration: 'none',
                                  whiteSpace: 'nowrap',
                                }}
                              >
                                <EyeOutlined /> 预览
                              </a>
                            </>
                          ) : null}
                        </Space>
                      </div>
                    ))}
                  </Space>
                </Card>
              )}

              <Tabs
                activeKey={activeTab}
                onChange={setActiveTab}
                items={[
                  {
                    key: 'insight',
                    disabled: !detail.insight_md,
                    label: (
                      <span>
                        <EyeOutlined /> 速览
                        {detail.stats.insight_chars > 0 && (
                          <span style={{ color: '#94a3b8', marginLeft: 4 }}>
                            ({detail.stats.insight_chars})
                          </span>
                        )}
                      </span>
                    ),
                    children: <MarkdownContent md={detail.insight_md} empty="无速览" />,
                  },
                  {
                    key: 'summary',
                    disabled: !detail.summary_md,
                    label: (
                      <span>
                        <BookOutlined /> 摘要
                        {detail.stats.summary_chars > 0 && (
                          <span style={{ color: '#94a3b8', marginLeft: 4 }}>
                            ({detail.stats.summary_chars})
                          </span>
                        )}
                      </span>
                    ),
                    children: <MarkdownContent md={detail.summary_md} empty="无摘要" />,
                  },
                  {
                    key: 'content',
                    disabled: !detail.content_md,
                    label: (
                      <span
                        style={{
                          color: detail.is_weekly_report ? '#ec4899' : undefined,
                          fontWeight: detail.is_weekly_report ? 600 : undefined,
                        }}
                      >
                        <FileTextOutlined /> 正文
                        {detail.stats.content_chars > 0 && (
                          <span style={{ color: '#94a3b8', marginLeft: 4 }}>
                            ({detail.stats.content_chars})
                          </span>
                        )}
                      </span>
                    ),
                    children: <MarkdownContent md={detail.content_md} empty="无正文" />,
                  },
                  {
                    key: 'topic',
                    disabled: !detail.topic_md,
                    label: (
                      <span>
                        <TagOutlined /> 主题
                      </span>
                    ),
                    children: <MarkdownContent md={detail.topic_md} empty="无主题" />,
                  },
                  {
                    key: 'background',
                    disabled: !detail.background_md,
                    label: (
                      <span>
                        <ProfileOutlined /> 背景
                      </span>
                    ),
                    children: <MarkdownContent md={detail.background_md} empty="无背景" />,
                  },
                  {
                    key: 'expert',
                    disabled: !detail.expert_content_md,
                    label: (
                      <span>
                        <TeamOutlined /> 专家
                      </span>
                    ),
                    children: (
                      <MarkdownContent md={detail.expert_content_md} empty="无专家内容" />
                    ),
                  },
                  {
                    key: 'pdf_text',
                    disabled: !detail.pdf_text_md,
                    label: (
                      <span>
                        <FileTextOutlined /> PDF 全文
                        {detail.pdf_text_md && (
                          <span style={{ color: '#94a3b8', marginLeft: 4 }}>
                            ({detail.pdf_text_md.length.toLocaleString()})
                          </span>
                        )}
                      </span>
                    ),
                    children: <MarkdownContent md={detail.pdf_text_md} empty="无 PDF 全文" />,
                  },
                ]}
              />

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

      {/* --- Markdown typography styles (applied to .meritco-md) --- */}
      <style>{`
        .meritco-md {
          font-size: 14px;
          line-height: 1.9;
          color: #1e293b;
        }
        .meritco-md h1 {
          font-size: 20px;
          margin: 18px 0 10px;
          color: #0f172a;
          border-bottom: 2px solid #e2e8f0;
          padding-bottom: 6px;
        }
        .meritco-md h1:first-child,
        .meritco-md h2:first-child,
        .meritco-md h3:first-child {
          margin-top: 0;
        }
        .meritco-md h2 {
          font-size: 17px;
          margin: 22px 0 10px;
          color: #1e293b;
          border-bottom: 1px solid #e2e8f0;
          padding-bottom: 4px;
        }
        .meritco-md h3 {
          font-size: 15px;
          margin: 14px 0 6px;
          color: #1e293b;
          font-weight: 600;
        }
        .meritco-md h4 {
          font-size: 14px;
          margin: 10px 0 4px;
          color: #334155;
          font-weight: 600;
        }
        .meritco-md p {
          margin: 10px 0;
          text-align: justify;
        }
        .meritco-md ul,
        .meritco-md ol {
          margin: 10px 0 12px;
          padding-left: 24px;
        }
        .meritco-md li {
          margin-bottom: 6px;
        }
        .meritco-md li > p {
          margin: 2px 0;
        }
        .meritco-md strong {
          color: #0f172a;
          font-weight: 600;
          background: linear-gradient(transparent 65%, #fef08a 65%);
          padding: 0 1px;
        }
        .meritco-md em {
          color: #475569;
        }
        .meritco-md table {
          border-collapse: collapse;
          margin: 14px 0;
          font-size: 13px;
          width: 100%;
          box-shadow: 0 1px 2px rgba(0,0,0,0.03);
        }
        .meritco-md th,
        .meritco-md td {
          border: 1px solid #e2e8f0;
          padding: 8px 12px;
          text-align: left;
          vertical-align: top;
        }
        .meritco-md th {
          background: linear-gradient(#f1f5f9, #f8fafc);
          font-weight: 600;
          color: #0f172a;
        }
        .meritco-md tr:nth-child(even) td {
          background: #fafbfc;
        }
        .meritco-md code {
          background: #f1f5f9;
          padding: 1px 5px;
          border-radius: 3px;
          font-size: 12.5px;
          font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
          color: #be185d;
        }
        .meritco-md pre {
          background: #0f172a;
          color: #e2e8f0;
          padding: 12px 14px;
          border-radius: 6px;
          overflow-x: auto;
          font-size: 12.5px;
          margin: 12px 0;
        }
        .meritco-md pre code {
          background: transparent;
          color: inherit;
          padding: 0;
        }
        .meritco-md blockquote {
          border-left: 3px solid #8b5cf6;
          padding: 4px 12px;
          margin: 12px 0;
          color: #475569;
          background: #faf7ff;
          border-radius: 0 6px 6px 0;
        }
        .meritco-md blockquote p {
          margin: 4px 0;
        }
        .meritco-md hr {
          border: none;
          border-top: 1px dashed #cbd5e1;
          margin: 18px 0;
        }
        .meritco-md a {
          color: #2563eb;
          text-decoration: none;
          border-bottom: 1px dotted #2563eb;
        }
        .meritco-md a:hover {
          color: #1d4ed8;
          border-bottom-style: solid;
        }
      `}</style>
    </div>
  )
}

function MarkdownContent({
  md,
  empty,
  mono,
}: {
  md: string
  empty: string
  mono?: boolean
}) {
  if (!md) return <Empty description={empty} style={{ margin: '20px 0' }} />
  if (mono) {
    return (
      <div
        style={{
          background: '#f8fafc',
          padding: 16,
          borderRadius: 6,
          maxHeight: '62vh',
          overflowY: 'auto',
          fontSize: 13,
          lineHeight: 1.7,
          whiteSpace: 'pre-wrap',
          fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
          border: '1px solid #e2e8f0',
        }}
      >
        {md}
      </div>
    )
  }
  return (
    <div
      className="meritco-md"
      style={{
        background: '#ffffff',
        padding: '20px 26px',
        borderRadius: 8,
        maxHeight: '62vh',
        overflowY: 'auto',
        border: '1px solid #e2e8f0',
      }}
    >
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{md}</ReactMarkdown>
    </div>
  )
}
