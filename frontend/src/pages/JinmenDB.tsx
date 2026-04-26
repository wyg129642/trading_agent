/**
 * 进门专区 · 会议纪要
 *
 * 直接读取 crawl/jinmen/scraper.py 写入的 MongoDB 数据。
 * 简单可视化：
 *  - 顶部统计卡：总纪要 / 今日 / 最新发布 / 内容覆盖率
 *  - 近 7 天发布量折线 + Top 机构柱状图 + Top 行业标签
 *  - 纪要列表（标题 / 机构 / 标的 / 主题 / 速览字数 / 对话条数）
 *  - 详情抽屉：Tab 切换 速览 / 章节概要 / 指标 / 对话
 */
import { useCallback, useEffect, useRef, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import {
  Alert,
  Card,
  Drawer,
  Empty,
  Input,
  List,
  Select,
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
  DatabaseOutlined,
  ClockCircleOutlined,
  BookOutlined,
  NumberOutlined,
  MessageOutlined,
  FileTextOutlined,
  TeamOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import api from '../services/api'

dayjs.extend(relativeTime)

const { Text, Title } = Typography

interface StatsResponse {
  total: number
  today: number
  last_7_days: { date: string; count: number }[]
  top_organizations: { name: string; count: number }[]
  top_industries: { name: string; count: number }[]
  top_themes: { name: string; count: number }[]
  latest_release_time: string | null
  crawler_state: {
    in_progress: boolean
    last_processed_at: string | null
    last_run_end_at: string | null
    last_run_stats: { added?: number; skipped?: number; failed?: number }
  } | null
  daily_platform_stats: {
    total_on_platform: number
    in_db: number
    not_in_db: number
    by_organization_top10: [string, number][]
    by_industry_top10: [string, number][]
    by_tag: Record<string, number>
  } | null
  content_coverage: {
    points: number
    chapters: number
    indicators: number
    transcript: number
  }
}

interface MeetingBrief {
  id: string
  roadshow_id: string
  title: string
  release_time: string | null
  web_url: string | null
  organization: string | null
  industries: string[]
  themes: string[]
  stocks: { name: string; code: string; market: string }[]
  creators: string[]
  guests: string[]
  featured_tag?: string | null
  auth_tag?: string | null
  speaker_tag?: string | null
  content_types: string[]
  preview: string
  stats: {
    points_chars: number
    chapters: number
    indicators: number
    transcript_items: number
  }
  has_transcript: boolean
  has_chapters: boolean
  has_indicators: boolean
  crawled_at: string | null
}

interface MeetingDetail extends MeetingBrief {
  points_md: string
  chapter_summary_md: string
  indicators_md: string
  transcript_md: string
  present_url: string | null
}

interface ListResponse {
  items: MeetingBrief[]
  total: number
  page: number
  page_size: number
  has_next: boolean
}

export default function JinmenDB() {
  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [statsLoading, setStatsLoading] = useState(true)
  const [statsError, setStatsError] = useState<string | null>(null)

  const [items, setItems] = useState<MeetingBrief[]>([])
  const [itemsLoading, setItemsLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [total, setTotal] = useState(0)

  const [query, setQuery] = useState('')
  const [orgFilter, setOrgFilter] = useState<string | undefined>()
  const [industryFilter, setIndustryFilter] = useState<string | undefined>()

  const [detailOpen, setDetailOpen] = useState(false)
  const [detail, setDetail] = useState<MeetingDetail | null>(null)
  const [detailError, setDetailError] = useState<string | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [activeTab, setActiveTab] = useState('points')

  const loadStats = useCallback(async () => {
    setStatsLoading(true)
    setStatsError(null)
    try {
      const res = await api.get<StatsResponse>('/jinmen-db/stats')
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
      const res = await api.get<ListResponse>('/jinmen-db/meetings', {
        params: {
          page,
          page_size: 20,
          q: query || undefined,
          organization: orgFilter || undefined,
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
  }, [page, query, orgFilter, industryFilter])

  useEffect(() => {
    loadStats()
  }, [loadStats])

  useEffect(() => {
    loadItems()
  }, [loadItems])

  const openDetail = useCallback(async (item: MeetingBrief) => {
    setDetailOpen(true)
    setDetailLoading(true)
    setDetail(null)
    setDetailError(null)
    setActiveTab('points')
    try {
      const res = await api.get<MeetingDetail>(
        `/jinmen-db/meetings/${encodeURIComponent(item.id)}`,
      )
      setDetail(res.data)
    } catch (e: any) {
      setDetail(null)
      const status = e?.response?.status
      if (status === 404) {
        setDetailError(`该条目 (id=${item.id}) 尚未同步到本地数据库 — 爬虫下一轮会拉取`)
      } else {
        setDetailError(
          e?.response?.data?.detail || e?.message || '加载详情失败',
        )
      }
    } finally {
      setDetailLoading(false)
    }
  }, [])

  // Deep-link: ?open=<summaryId> opens the drawer directly — used by the
  // JinmenPlatformInfo feed rows to jump straight into a DB detail view.
  const [searchParams] = useSearchParams()
  const openParam = searchParams.get('open')
  const lastOpenedRef = useRef<string | null>(null)
  useEffect(() => {
    if (openParam && openParam !== lastOpenedRef.current) {
      lastOpenedRef.current = openParam
      openDetail({ id: openParam } as MeetingBrief)
    }
  }, [openParam, openDetail])

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
            <DatabaseOutlined /> 进门专区 · 会议纪要
          </Title>
          <Text type="secondary">
            brm.comein.cn · AI 会议纪要 · 来自 crawl/jinmen/scraper.py
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
          message="无法从 MongoDB 加载进门数据"
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
              value={stats?.today ?? 0}
              valueStyle={{ color: '#10b981', fontSize: 28 }}
              suffix={
                stats?.daily_platform_stats ? (
                  <Tag color="green" style={{ fontSize: 11, marginLeft: 8 }}>
                    平台 {stats.daily_platform_stats.total_on_platform}
                  </Tag>
                ) : null
              }
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              {dayjs().format('YYYY-MM-DD')}
              {stats?.latest_release_time && (
                <> · 最近发布 {stats.latest_release_time}</>
              )}
            </Text>
          </Space>
        </Card>
      </Spin>

      {/* --- meeting list --- */}
      <Card size="small">
        <Space style={{ marginBottom: 12, flexWrap: 'wrap' }}>
          <Input.Search
            placeholder="标题 / 速览内容搜索"
            allowClear
            style={{ width: 280 }}
            onSearch={(v) => {
              setQuery(v)
              setPage(1)
            }}
          />
          <Select
            placeholder="研究机构"
            allowClear
            value={orgFilter}
            onChange={(v) => {
              setOrgFilter(v)
              setPage(1)
            }}
            style={{ width: 180 }}
            options={(stats?.top_organizations || []).map((o) => ({
              value: o.name,
              label: `${o.name} (${o.count})`,
            }))}
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
                    <Text strong>{item.title}</Text>
                    {item.organization && <Tag color="blue">{item.organization}</Tag>}
                    {item.featured_tag && <Tag color="gold">{item.featured_tag}</Tag>}
                    {item.auth_tag && <Tag>{item.auth_tag}</Tag>}
                  </Space>
                }
                description={
                  <Space direction="vertical" size={2} style={{ width: '100%' }}>
                    <Space size={10} wrap style={{ fontSize: 12 }}>
                      <Text type="secondary">
                        <ClockCircleOutlined /> {item.release_time || '—'}
                      </Text>
                      {item.creators.length > 0 && (
                        <Text type="secondary">
                          <TeamOutlined /> {item.creators.slice(0, 3).join(' / ')}
                        </Text>
                      )}
                      {item.stocks.slice(0, 3).map((s) => (
                        <Tag key={s.code + s.market} color="cyan" style={{ fontSize: 11 }}>
                          {s.name} {s.code}
                        </Tag>
                      ))}
                      {item.industries.slice(0, 3).map((ind) => (
                        <Tag key={ind} style={{ fontSize: 11 }}>
                          {ind}
                        </Tag>
                      ))}
                    </Space>
                    <Text
                      style={{ fontSize: 12, color: '#64748b' }}
                      ellipsis={{ tooltip: item.preview } as any}
                    >
                      {item.preview.replace(/[#*]/g, '').replace(/\n+/g, ' ')}
                    </Text>
                    <Space size={6} style={{ fontSize: 11, color: '#94a3b8' }}>
                      <span>
                        <BookOutlined /> 速览 {item.stats.points_chars} 字
                      </span>
                      <span>
                        <FileTextOutlined /> 章节 {item.stats.chapters}
                      </span>
                      <span>
                        <NumberOutlined /> 指标 {item.stats.indicators}
                      </span>
                      <span>
                        <MessageOutlined /> 对话 {item.stats.transcript_items}
                      </span>
                    </Space>
                  </Space>
                }
              />
            </List.Item>
          )}
        />
      </Card>

      {/* --- detail drawer --- */}
      <Drawer
        title={detail?.title || '详情'}
        open={detailOpen}
        onClose={() => setDetailOpen(false)}
        width={800}
        extra={
          detail?.present_url ? (
            <a href={detail.present_url} target="_blank" rel="noreferrer">
              <LinkOutlined /> 原页
            </a>
          ) : null
        }
      >
        <Spin spinning={detailLoading}>
          {detail ? (
            <div>
              <Space wrap size={6} style={{ marginBottom: 10 }}>
                {detail.organization && <Tag color="blue">{detail.organization}</Tag>}
                {detail.release_time && (
                  <Tag icon={<ClockCircleOutlined />}>{detail.release_time}</Tag>
                )}
                {detail.featured_tag && <Tag color="gold">{detail.featured_tag}</Tag>}
                {detail.auth_tag && <Tag>{detail.auth_tag}</Tag>}
                {detail.speaker_tag && <Tag>{detail.speaker_tag}</Tag>}
              </Space>
              {detail.stocks.length > 0 && (
                <div style={{ marginBottom: 6 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    标的:
                  </Text>
                  {detail.stocks.map((s) => (
                    <Tag key={s.code + s.market} color="cyan">
                      {s.name} {s.code} · {s.market.toUpperCase()}
                    </Tag>
                  ))}
                </div>
              )}
              {detail.industries.length > 0 && (
                <div style={{ marginBottom: 6 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    行业:
                  </Text>
                  {detail.industries.map((i) => (
                    <Tag key={i}>{i}</Tag>
                  ))}
                </div>
              )}
              {detail.themes.length > 0 && (
                <div style={{ marginBottom: 6 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    主题:
                  </Text>
                  {detail.themes.map((t) => (
                    <Tag key={t} color="purple">
                      {t}
                    </Tag>
                  ))}
                </div>
              )}
              {detail.creators.length > 0 && (
                <div style={{ marginBottom: 10 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    作者/发言人:
                  </Text>
                  {detail.creators.map((c) => (
                    <Tag key={c} color="geekblue">
                      {c}
                    </Tag>
                  ))}
                </div>
              )}

              <Tabs
                activeKey={activeTab}
                onChange={setActiveTab}
                items={[
                  {
                    key: 'points',
                    label: (
                      <span>
                        <BookOutlined /> 速览 ({detail.stats.points_chars} 字)
                      </span>
                    ),
                    children: (
                      <MarkdownContent
                        md={detail.points_md}
                        empty="无速览内容"
                      />
                    ),
                  },
                  {
                    key: 'chapters',
                    label: (
                      <span>
                        <FileTextOutlined /> 章节 ({detail.stats.chapters})
                      </span>
                    ),
                    children: (
                      <MarkdownContent
                        md={detail.chapter_summary_md}
                        empty="无章节概要"
                      />
                    ),
                  },
                  {
                    key: 'indicators',
                    label: (
                      <span>
                        <NumberOutlined /> 指标 ({detail.stats.indicators})
                      </span>
                    ),
                    children: (
                      <MarkdownContent
                        md={detail.indicators_md}
                        empty="无指标数据"
                        mono
                      />
                    ),
                  },
                  {
                    key: 'transcript',
                    label: (
                      <span>
                        <MessageOutlined /> 对话 ({detail.stats.transcript_items})
                      </span>
                    ),
                    children: (
                      <MarkdownContent
                        md={detail.transcript_md}
                        empty="无对话稿"
                        mono
                      />
                    ),
                  },
                ]}
              />

              <Text type="secondary" style={{ fontSize: 11, display: 'block', marginTop: 16 }}>
                ID: {detail.id}
                {detail.crawled_at &&
                  ` · 抓取于 ${dayjs(detail.crawled_at).format('YYYY-MM-DD HH:mm')}`}
              </Text>
            </div>
          ) : detailError ? (
            <Empty description={detailError} />
          ) : detailLoading ? null : (
            <Empty />
          )}
        </Spin>
      </Drawer>

      <style>{`
        .jinmen-md h1, .jinmen-md h2, .jinmen-md h3, .jinmen-md h4 {
          color: #0f172a;
          margin: 16px 0 8px;
          font-weight: 600;
          line-height: 1.4;
        }
        .jinmen-md h2 { font-size: 16px; }
        .jinmen-md h3 { font-size: 15px; }
        .jinmen-md h4 { font-size: 13.5px; }
        .jinmen-md ul, .jinmen-md ol {
          padding-left: 1.6em;
          margin: 6px 0;
        }
        .jinmen-md li { margin: 3px 0; }
        .jinmen-md li > ul, .jinmen-md li > ol { margin: 3px 0; }
        .jinmen-md p { margin: 6px 0; }
        .jinmen-md strong { font-weight: 600; color: #0f172a; }
        .jinmen-md em { font-style: italic; }
        .jinmen-md table {
          border-collapse: collapse;
          margin: 10px 0;
          font-size: 12px;
        }
        .jinmen-md th, .jinmen-md td {
          border: 1px solid #e2e8f0;
          padding: 5px 9px;
        }
        .jinmen-md th { background: #f8fafc; font-weight: 600; }
        .jinmen-md a { color: #2563eb; text-decoration: none; }
        .jinmen-md a:hover { text-decoration: underline; }
        .jinmen-md code {
          background: #eef2f7;
          padding: 1px 5px;
          border-radius: 3px;
          font-size: 12px;
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
  return (
    <div
      className={mono ? undefined : 'jinmen-md'}
      style={{
        background: '#f8fafc',
        padding: 14,
        borderRadius: 4,
        maxHeight: '62vh',
        overflowY: 'auto',
        fontSize: 13,
        lineHeight: 1.7,
        whiteSpace: mono ? 'pre-wrap' : undefined,
        fontFamily: mono
          ? 'ui-monospace, SFMono-Regular, Menlo, monospace'
          : undefined,
      }}
    >
      {mono ? (
        md
      ) : (
        <ReactMarkdown remarkPlugins={[remarkGfm]}>{md}</ReactMarkdown>
      )}
    </div>
  )
}
