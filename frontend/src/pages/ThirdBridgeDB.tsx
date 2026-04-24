/**
 * 高临专区 · Third Bridge 专家访谈
 *
 * 基于 MongoDB (`thirdbridge.interviews`) 的视图。
 * 每条为一次专家访谈：议程 + 专家 + 目标/相关公司 + 逐字稿 (若有权限/已发生)。
 */
import { useCallback, useEffect, useState } from 'react'
import {
  Alert,
  Card,
  Checkbox,
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
  ClockCircleOutlined,
  BankOutlined,
  UserOutlined,
  CommentOutlined,
  TeamOutlined,
  GlobalOutlined,
  ReadOutlined,
  FileTextOutlined,
  AudioOutlined,
} from '@ant-design/icons'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'

dayjs.extend(relativeTime)

const { Text, Title } = Typography

interface Company {
  label: string
  ticker: string
  country: string
  sector: string
  public: boolean
}

interface Specialist {
  name: string
  title: string
  types: string[]
}

interface Interview {
  id: string
  uuid: string
  title: string
  release_time: string | null
  web_url: string | null
  status: string
  language: string
  content_type: string
  researcher_email: string
  target_companies: Company[]
  relevant_companies: Company[]
  specialists: Specialist[]
  moderators: any[]
  themes: string[]
  sectors: string[]
  geographies: string[]
  transcripts_available: string[]
  pdf_available: string[]
  audio: boolean
  has_commentary: boolean
  preview: string
  stats: {
    transcript_segments: number
    transcript_chars: number
    agenda_items: number
    specialists: number
    target_companies: number
    relevant_companies: number
  }
  has_transcript: boolean
  crawled_at: string | null
}

interface ListResponse {
  items: Interview[]
  total: number
  page: number
  page_size: number
  has_next: boolean
}

interface StatsResponse {
  total: number
  today: number
  with_transcript: number
  latest_release_time: string | null
  crawler_state: {
    in_progress: boolean
    last_processed_at: string | null
    last_run_end_at: string | null
    last_run_stats: { added?: number; skipped?: number; failed?: number }
    top_uuid?: string
  } | null
  daily_platform_stats: {
    total_on_platform: number
    in_db: number
    not_in_db: number
    by_content_type: Record<string, number>
    by_sector_top10: [string, number][]
  } | null
}

interface Detail extends Interview {
  agenda_md: string
  specialists_md: string
  introduction_md: string
  transcript_md: string
  commentary_md: string
}

const STATUS_COLOR: Record<string, string> = {
  READY: 'green',
  SCHEDULED: 'gold',
  COMPLETED: 'blue',
}

export default function ThirdBridgeDB() {
  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [statsLoading, setStatsLoading] = useState(true)
  const [statsError, setStatsError] = useState<string | null>(null)

  const [items, setItems] = useState<Interview[]>([])
  const [itemsLoading, setItemsLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [total, setTotal] = useState(0)

  const [query, setQuery] = useState('')
  const [companyFilter, setCompanyFilter] = useState('')
  const [onlyTranscript, setOnlyTranscript] = useState(false)

  const [detailOpen, setDetailOpen] = useState(false)
  const [detail, setDetail] = useState<Detail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [activeTab, setActiveTab] = useState('transcript')

  const loadStats = useCallback(async () => {
    setStatsLoading(true)
    setStatsError(null)
    try {
      const res = await api.get<StatsResponse>('/thirdbridge-db/stats')
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
      const res = await api.get<ListResponse>('/thirdbridge-db/interviews', {
        params: {
          page,
          page_size: 20,
          q: query || undefined,
          company: companyFilter || undefined,
          only_with_transcript: onlyTranscript || undefined,
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
  }, [page, query, companyFilter, onlyTranscript])

  useEffect(() => {
    loadStats()
  }, [loadStats])

  useEffect(() => {
    loadItems()
  }, [loadItems])

  const openDetail = useCallback(async (item: Interview) => {
    setDetailOpen(true)
    setDetailLoading(true)
    setDetail(null)
    setActiveTab(item.has_transcript ? 'transcript' : 'agenda')
    try {
      const res = await api.get<Detail>(
        `/thirdbridge-db/interviews/${encodeURIComponent(item.id)}`,
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
            <GlobalOutlined /> 高临专区 · Third Bridge 专家访谈
          </Title>
          <Text type="secondary">
            forum.thirdbridge.com · 全球前高管 / 行业专家访谈 + 逐字稿
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
          message="无法从 MongoDB 加载 Third Bridge 数据"
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
                  <ClockCircleOutlined /> 今日新增访谈
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
                <> · 最近访谈 {stats.latest_release_time}</>
              )}
              {typeof stats?.with_transcript === 'number' && (
                <> · 已有逐字稿 {stats.with_transcript} 条</>
              )}
            </Text>
          </Space>
        </Card>
      </Spin>

      <Card size="small">
        <Space wrap style={{ marginBottom: 12 }}>
          <Input.Search
            placeholder="搜索标题 / 议程 / 逐字稿"
            allowClear
            style={{ width: 300 }}
            onSearch={(v) => {
              setQuery(v)
              setPage(1)
            }}
          />
          <Input
            placeholder="公司名 / ticker (例: Deutsche Bank 或 DBK)"
            allowClear
            prefix={<BankOutlined />}
            style={{ width: 280 }}
            onPressEnter={(e) => {
              setCompanyFilter((e.target as HTMLInputElement).value)
              setPage(1)
            }}
          />
          <Checkbox
            checked={onlyTranscript}
            onChange={(e) => {
              setOnlyTranscript(e.target.checked)
              setPage(1)
            }}
          >
            仅显示有逐字稿
          </Checkbox>
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
            const mainTarget = item.target_companies[0]
            const statusColor = STATUS_COLOR[item.status] || 'default'
            return (
              <List.Item
                key={item.id}
                style={{ cursor: 'pointer' }}
                onClick={() => openDetail(item)}
              >
                <List.Item.Meta
                  title={
                    <Space size={6} wrap>
                      <Tag color={statusColor} style={{ fontSize: 11 }}>
                        {item.status}
                      </Tag>
                      {item.language && (
                        <Tag style={{ fontSize: 11 }}>{item.language}</Tag>
                      )}
                      {item.content_type && (
                        <Tag color="blue" style={{ fontSize: 11 }}>
                          {item.content_type}
                        </Tag>
                      )}
                      {item.has_transcript && (
                        <Tag color="green" icon={<FileTextOutlined />}>
                          有逐字稿 {item.stats.transcript_chars} 字
                        </Tag>
                      )}
                      {item.audio && (
                        <Tag color="purple" icon={<AudioOutlined />}>
                          音频
                        </Tag>
                      )}
                      <Text strong>{item.title}</Text>
                    </Space>
                  }
                  description={
                    <Space direction="vertical" size={2} style={{ width: '100%' }}>
                      <Space size={10} wrap style={{ fontSize: 12 }}>
                        <Text type="secondary">
                          <ClockCircleOutlined /> {item.release_time || '—'}
                        </Text>
                        {mainTarget && (
                          <Tag color="cyan" icon={<BankOutlined />}>
                            {mainTarget.label}
                            {mainTarget.ticker && ` · ${mainTarget.ticker}`}
                          </Tag>
                        )}
                        {item.specialists.slice(0, 2).map((s, i) => (
                          <Tag
                            key={`${s.name}-${i}`}
                            color="geekblue"
                            icon={<UserOutlined />}
                          >
                            {s.name}
                            {s.types.length > 0 && ` · ${s.types[0]}`}
                          </Tag>
                        ))}
                        {item.specialists.length > 2 && (
                          <Text type="secondary" style={{ fontSize: 11 }}>
                            +{item.specialists.length - 2} 位专家
                          </Text>
                        )}
                      </Space>
                      <Text
                        style={{ fontSize: 12, color: '#64748b' }}
                        ellipsis={{ tooltip: item.preview } as any}
                      >
                        {item.preview.replace(/[#*`]/g, '').replace(/\n+/g, ' ')}
                      </Text>
                      <Space size={10} style={{ fontSize: 11, color: '#94a3b8' }}>
                        <span>
                          <ReadOutlined /> 议程 {item.stats.agenda_items}
                        </span>
                        <span>
                          <TeamOutlined /> 专家 {item.stats.specialists}
                        </span>
                        <span>
                          <BankOutlined /> 目标公司 {item.stats.target_companies} · 相关{' '}
                          {item.stats.relevant_companies}
                        </span>
                        {item.transcripts_available.length > 0 && (
                          <span>
                            · 语种 {item.transcripts_available.join(' / ')}
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
          detail?.uuid ? (
            <a
              href={`https://forum.thirdbridge.com/zh/interview/${detail.uuid}`}
              target="_blank"
              rel="noreferrer"
            >
              <LinkOutlined /> Third Bridge 原页
            </a>
          ) : null
        }
      >
        <Spin spinning={detailLoading}>
          {detail ? (
            <div>
              <Space wrap size={6} style={{ marginBottom: 10 }}>
                <Tag color={STATUS_COLOR[detail.status] || 'default'}>
                  {detail.status}
                </Tag>
                {detail.release_time && (
                  <Tag icon={<ClockCircleOutlined />}>{detail.release_time}</Tag>
                )}
                {detail.language && <Tag>{detail.language}</Tag>}
                {detail.content_type && <Tag color="blue">{detail.content_type}</Tag>}
                {detail.has_transcript && (
                  <Tag color="green" icon={<FileTextOutlined />}>
                    有逐字稿
                  </Tag>
                )}
                {detail.audio && (
                  <Tag color="purple" icon={<AudioOutlined />}>
                    音频
                  </Tag>
                )}
              </Space>

              {detail.target_companies.length > 0 && (
                <div style={{ marginBottom: 6 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    <BankOutlined /> 目标公司:
                  </Text>
                  {detail.target_companies.map((c, idx) => (
                    <Tag key={`${c.label}-${idx}`} color="cyan">
                      {c.label}
                      {c.ticker && ` · ${c.ticker}`}
                      {c.country && ` · ${c.country}`}
                    </Tag>
                  ))}
                </div>
              )}
              {detail.relevant_companies.length > 0 && (
                <div style={{ marginBottom: 6 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    相关公司:
                  </Text>
                  {detail.relevant_companies.slice(0, 8).map((c, idx) => (
                    <Tag key={`${c.label}-${idx}`}>
                      {c.label}
                      {c.ticker && ` · ${c.ticker}`}
                    </Tag>
                  ))}
                  {detail.relevant_companies.length > 8 && (
                    <Text type="secondary" style={{ fontSize: 11 }}>
                      +{detail.relevant_companies.length - 8}
                    </Text>
                  )}
                </div>
              )}
              {detail.specialists.length > 0 && (
                <div style={{ marginBottom: 10 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    <TeamOutlined /> 专家:
                  </Text>
                  {detail.specialists.map((s, idx) => (
                    <Tag key={`${s.name}-${idx}`} color="geekblue">
                      <UserOutlined /> {s.name}
                      {s.types.length > 0 && ` · ${s.types.join(' / ')}`}
                    </Tag>
                  ))}
                </div>
              )}

              <Tabs
                activeKey={activeTab}
                onChange={setActiveTab}
                items={[
                  {
                    key: 'transcript',
                    label: (
                      <span>
                        <FileTextOutlined /> 逐字稿 ({detail.stats.transcript_chars} 字)
                      </span>
                    ),
                    children: (
                      <MarkdownContent
                        md={detail.transcript_md}
                        empty={
                          detail.status !== 'READY' || detail.stats.transcript_chars === 0
                            ? '访谈尚未完成或当前账号无权限读取逐字稿'
                            : '无逐字稿'
                        }
                      />
                    ),
                  },
                  {
                    key: 'agenda',
                    label: (
                      <span>
                        <ReadOutlined /> 议程 ({detail.stats.agenda_items} 项)
                      </span>
                    ),
                    children: <MarkdownContent md={detail.agenda_md} empty="无议程" />,
                  },
                  {
                    key: 'specialists',
                    label: (
                      <span>
                        <TeamOutlined /> 专家简介
                      </span>
                    ),
                    children: (
                      <MarkdownContent md={detail.specialists_md} empty="无专家信息" />
                    ),
                  },
                  {
                    key: 'intro',
                    label: (
                      <span>
                        <CommentOutlined /> 开场白
                      </span>
                    ),
                    children: (
                      <MarkdownContent
                        md={detail.introduction_md}
                        empty="无开场白"
                      />
                    ),
                  },
                  {
                    key: 'commentary',
                    label: (
                      <span>
                        <CommentOutlined /> 专家点评
                      </span>
                    ),
                    children: (
                      <MarkdownContent
                        md={detail.commentary_md}
                        empty="无专家点评（仅在 hasCommentary=true 时可用）"
                      />
                    ),
                  },
                ]}
              />

              <Text
                type="secondary"
                style={{ fontSize: 11, display: 'block', marginTop: 16 }}
              >
                UUID: {detail.uuid}
                {detail.crawled_at &&
                  ` · 抓取于 ${dayjs(detail.crawled_at).format('YYYY-MM-DD HH:mm')}`}
              </Text>
            </div>
          ) : (
            <Empty />
          )}
        </Spin>
      </Drawer>
    </div>
  )
}

function MarkdownContent({ md, empty }: { md: string; empty: string }) {
  if (!md) return <Empty description={empty} style={{ margin: '20px 0' }} />
  return (
    <div
      style={{
        background: '#f8fafc',
        padding: 14,
        borderRadius: 4,
        maxHeight: '62vh',
        overflowY: 'auto',
        fontSize: 13,
        lineHeight: 1.75,
      }}
    >
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{md}</ReactMarkdown>
    </div>
  )
}
