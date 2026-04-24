/**
 * AlphaPai 数据库视图 — 直接展示 MongoDB 中爬取的四大类投研内容
 * (会议路演 / 券商研报 / 券商点评 / 社媒公众号).
 *
 * 简单可视化:
 *   - 顶部 4 张统计卡片 (总量 + 今日新增)
 *   - 近 7 天堆叠柱状图
 *   - 各分类 Top 发布机构
 *   - 爬虫状态面板
 *   - 分类 Tab + 详情抽屉
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Card,
  Col,
  Drawer,
  Empty,
  Input,
  List,
  Row,
  Space,
  Spin,
  Statistic,
  Tabs,
  Tag,
  Tooltip,
  Typography,
} from 'antd'
import {
  ReloadOutlined,
  LinkOutlined,
  DatabaseOutlined,
  ClockCircleOutlined,
  FileSearchOutlined,
  AudioOutlined,
  FileTextOutlined,
  MessageOutlined,
  WechatOutlined,
} from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'
import MarkdownRenderer from '../components/MarkdownRenderer'

dayjs.extend(relativeTime)

const { Text, Paragraph, Title } = Typography

// ----- Types ----------------------------------------------------- //
type Category = 'roadshow' | 'report' | 'comment' | 'wechat'

const CATEGORY_META: Record<
  Category,
  { label: string; color: string; icon: any }
> = {
  roadshow: { label: '会议路演', color: '#2563eb', icon: <AudioOutlined /> },
  report: { label: '券商研报', color: '#a855f7', icon: <FileTextOutlined /> },
  comment: { label: '券商点评', color: '#10b981', icon: <MessageOutlined /> },
  wechat: { label: '社媒公众号', color: '#f59e0b', icon: <WechatOutlined /> },
}
const CATEGORY_KEYS: Category[] = ['roadshow', 'report', 'comment', 'wechat']

interface StatsResponse {
  total: number
  per_category: Record<Category, number>
  today: Record<Category, number>
  last_7_days: { date: string; roadshow: number; report: number; comment: number; wechat: number }[]
  crawler_state: {
    category: string
    last_processed_at: string | null
    last_run_end_at: string | null
    last_run_stats: { added?: number; skipped?: number; failed?: number }
    in_progress: boolean
  }[]
  daily_platform_stats:
    | Record<Category, { platform_count: number; in_db: number; missing: number }>
    | null
  recent_publishers: Record<Category, { name: string; count: number }[]>
  latest_per_category: Record<Category, string | null>
}

interface ItemBrief {
  id: string
  category: Category
  title: string
  publish_time: string | null
  web_url: string | null
  institution: string | null
  stocks: { code: string; name: string }[]
  industries: string[]
  analysts: string[]
  content_preview: string
  content_length: number
  has_pdf: boolean
  account_name: string | null
  source_url: string | null
  crawled_at: string | null
}

interface ItemDetail extends ItemBrief {
  content: string
  pdf_local_path: string | null
  pdf_size: number | null
  raw_id: string | null
}

interface ListResponse {
  items: ItemBrief[]
  total: number
  page: number
  page_size: number
  has_next: boolean
}

// ----- Page ------------------------------------------------------ //
export default function AlphaPaiDB() {
  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [statsLoading, setStatsLoading] = useState(true)
  const [statsError, setStatsError] = useState<string | null>(null)

  const [activeCategory, setActiveCategory] = useState<Category>('roadshow')
  const [query, setQuery] = useState('')
  const [items, setItems] = useState<ItemBrief[]>([])
  const [itemsLoading, setItemsLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [total, setTotal] = useState(0)

  const [detailOpen, setDetailOpen] = useState(false)
  const [detail, setDetail] = useState<ItemDetail | null>(null)
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

  const loadItems = useCallback(
    async (category: Category, p: number, q: string) => {
      setItemsLoading(true)
      try {
        const res = await api.get<ListResponse>('/alphapai-db/items', {
          params: { category, page: p, page_size: 20, q: q || undefined },
        })
        setItems(res.data.items)
        setTotal(res.data.total)
      } catch {
        setItems([])
        setTotal(0)
      } finally {
        setItemsLoading(false)
      }
    },
    [],
  )

  useEffect(() => {
    loadStats()
  }, [loadStats])

  useEffect(() => {
    loadItems(activeCategory, page, query)
  }, [activeCategory, page, query, loadItems])

  const openDetail = useCallback(async (item: ItemBrief) => {
    setDetailOpen(true)
    setDetailLoading(true)
    setDetail(null)
    try {
      const res = await api.get<ItemDetail>(
        `/alphapai-db/items/${item.category}/${encodeURIComponent(item.id)}`,
      )
      setDetail(res.data)
    } catch {
      setDetail(null)
    } finally {
      setDetailLoading(false)
    }
  }, [])

  // ----- Charts ----- //
  const last7DaysOption = useMemo(() => {
    if (!stats || stats.last_7_days.length === 0) return null
    const dates = stats.last_7_days.map((d) => d.date.slice(5))
    return {
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      legend: {
        data: CATEGORY_KEYS.map((k) => CATEGORY_META[k].label),
        bottom: 0,
      },
      grid: { top: 20, left: 40, right: 20, bottom: 42, containLabel: true },
      xAxis: { type: 'category', data: dates },
      yAxis: { type: 'value' },
      series: CATEGORY_KEYS.map((cat) => ({
        name: CATEGORY_META[cat].label,
        type: 'bar',
        stack: 'all',
        emphasis: { focus: 'series' },
        itemStyle: { color: CATEGORY_META[cat].color },
        data: stats.last_7_days.map((d) => d[cat]),
      })),
    }
  }, [stats])

  const distributionOption = useMemo(() => {
    if (!stats) return null
    return {
      tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
      legend: { bottom: 0 },
      series: [
        {
          type: 'pie',
          radius: ['48%', '72%'],
          center: ['50%', '45%'],
          avoidLabelOverlap: true,
          label: { show: true, formatter: '{b}\n{c}', fontSize: 11 },
          data: CATEGORY_KEYS.map((cat) => ({
            name: CATEGORY_META[cat].label,
            value: stats.per_category[cat],
            itemStyle: { color: CATEGORY_META[cat].color },
          })),
        },
      ],
    }
  }, [stats])

  // ----- Render ----- //
  return (
    <div style={{ padding: 20 }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
        <div>
          <Title level={3} style={{ margin: 0 }}>
            <DatabaseOutlined /> AlphaPai 数据库
          </Title>
          <Text type="secondary">MongoDB 爬取数据实时视图 · 来自 crawl/alphapai_crawl/scraper.py</Text>
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
        {/* --- 4 stat cards --- */}
        <Row gutter={12} style={{ marginBottom: 16 }}>
          {CATEGORY_KEYS.map((cat) => {
            const meta = CATEGORY_META[cat]
            const perCat = stats?.per_category[cat] ?? 0
            const todayCat = stats?.today[cat] ?? 0
            const latest = stats?.latest_per_category?.[cat]
            return (
              <Col span={6} key={cat}>
                <Card
                  size="small"
                  hoverable
                  onClick={() => {
                    setActiveCategory(cat)
                    setPage(1)
                  }}
                  style={{
                    borderTop: `3px solid ${meta.color}`,
                    cursor: 'pointer',
                  }}
                  bodyStyle={{ padding: 14 }}
                >
                  <Statistic
                    title={
                      <span style={{ color: meta.color }}>
                        {meta.icon} {meta.label}
                      </span>
                    }
                    value={perCat}
                    suffix={
                      <Tag color={meta.color} style={{ fontSize: 11, marginLeft: 6 }}>
                        今日 +{todayCat}
                      </Tag>
                    }
                  />
                  <Text type="secondary" style={{ fontSize: 11 }}>
                    <ClockCircleOutlined /> 最新: {latest || '—'}
                  </Text>
                </Card>
              </Col>
            )
          })}
        </Row>

        {/* --- Charts row --- */}
        <Row gutter={12} style={{ marginBottom: 16 }}>
          <Col span={14}>
            <Card size="small" title="近 7 天入库量（按分类堆叠）">
              {last7DaysOption ? (
                <ReactECharts option={last7DaysOption} style={{ height: 240 }} />
              ) : (
                <Empty description="暂无时间序列数据" />
              )}
            </Card>
          </Col>
          <Col span={10}>
            <Card size="small" title="分类分布">
              {distributionOption ? (
                <ReactECharts option={distributionOption} style={{ height: 240 }} />
              ) : (
                <Empty />
              )}
            </Card>
          </Col>
        </Row>

        {/* --- Crawler state + today's coverage --- */}
        <Row gutter={12} style={{ marginBottom: 16 }}>
          <Col span={14}>
            <Card size="small" title="爬虫 Checkpoint 状态">
              {stats?.crawler_state?.length ? (
                <Row gutter={[12, 12]}>
                  {stats.crawler_state.map((s) => {
                    const meta = CATEGORY_META[s.category as Category]
                    const stats_ = s.last_run_stats || {}
                    return (
                      <Col span={12} key={s.category}>
                        <div
                          style={{
                            padding: 10,
                            border: '1px solid #e2e8f0',
                            borderLeft: `3px solid ${meta?.color || '#94a3b8'}`,
                            borderRadius: 4,
                          }}
                        >
                          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                            <Text strong style={{ color: meta?.color }}>
                              {meta?.icon} {meta?.label || s.category}
                            </Text>
                            <Tag color={s.in_progress ? 'processing' : 'default'}>
                              {s.in_progress ? '运行中' : '空闲'}
                            </Tag>
                          </div>
                          <Text type="secondary" style={{ fontSize: 12 }}>
                            末次结束: {s.last_run_end_at ? dayjs(s.last_run_end_at).fromNow() : '—'}
                          </Text>
                          <div style={{ marginTop: 4, fontSize: 12 }}>
                            <Tag color="green">新增 {stats_.added ?? 0}</Tag>
                            <Tag>跳过 {stats_.skipped ?? 0}</Tag>
                            <Tag color={stats_.failed ? 'red' : 'default'}>
                              失败 {stats_.failed ?? 0}
                            </Tag>
                          </div>
                        </div>
                      </Col>
                    )
                  })}
                </Row>
              ) : (
                <Empty description="尚无爬虫运行记录" />
              )}
            </Card>
          </Col>
          <Col span={10}>
            <Card size="small" title={`今日平台覆盖度 (${dayjs().format('YYYY-MM-DD')})`}>
              {stats?.daily_platform_stats ? (
                <List
                  size="small"
                  dataSource={CATEGORY_KEYS}
                  renderItem={(cat) => {
                    const d = stats.daily_platform_stats![cat]
                    const meta = CATEGORY_META[cat]
                    const pct =
                      d.platform_count > 0 ? (d.in_db / d.platform_count) * 100 : 0
                    return (
                      <List.Item style={{ padding: '6px 0' }}>
                        <div style={{ width: '100%' }}>
                          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                            <Text style={{ color: meta.color }}>{meta.label}</Text>
                            <Text type="secondary" style={{ fontSize: 12 }}>
                              {d.in_db}/{d.platform_count}
                            </Text>
                          </div>
                          <div
                            style={{
                              height: 6,
                              background: '#f1f5f9',
                              borderRadius: 3,
                              marginTop: 3,
                            }}
                          >
                            <div
                              style={{
                                width: `${Math.min(pct, 100)}%`,
                                height: '100%',
                                background: meta.color,
                                borderRadius: 3,
                              }}
                            />
                          </div>
                        </div>
                      </List.Item>
                    )
                  }}
                />
              ) : (
                <Empty description="运行 scraper.py --today 后显示" />
              )}
            </Card>
          </Col>
        </Row>

        {/* --- Top publishers per category --- */}
        <Card size="small" title="各分类 Top 发布机构" style={{ marginBottom: 16 }}>
          <Row gutter={12}>
            {CATEGORY_KEYS.map((cat) => {
              const list = stats?.recent_publishers?.[cat] || []
              const meta = CATEGORY_META[cat]
              return (
                <Col span={6} key={cat}>
                  <div style={{ marginBottom: 6 }}>
                    <Text strong style={{ color: meta.color, fontSize: 13 }}>
                      {meta.icon} {meta.label}
                    </Text>
                  </div>
                  {list.length === 0 ? (
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      —
                    </Text>
                  ) : (
                    list.map((p) => (
                      <div key={p.name} style={{ marginBottom: 4, fontSize: 12 }}>
                        <Text ellipsis style={{ maxWidth: 140 }}>
                          {p.name}
                        </Text>
                        <Tag style={{ marginLeft: 6, fontSize: 10 }} color={meta.color}>
                          {p.count}
                        </Tag>
                      </div>
                    ))
                  )}
                </Col>
              )
            })}
          </Row>
        </Card>
      </Spin>

      {/* --- Item list tabs --- */}
      <Card size="small">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
          <Tabs
            activeKey={activeCategory}
            onChange={(k) => {
              setActiveCategory(k as Category)
              setPage(1)
            }}
            items={CATEGORY_KEYS.map((cat) => ({
              key: cat,
              label: (
                <span style={{ color: CATEGORY_META[cat].color }}>
                  {CATEGORY_META[cat].icon} {CATEGORY_META[cat].label}
                  <Tag style={{ marginLeft: 6 }}>{stats?.per_category[cat] ?? 0}</Tag>
                </span>
              ),
            }))}
          />
          <Input.Search
            placeholder="标题/内容搜索"
            allowClear
            style={{ width: 260 }}
            onSearch={(v) => {
              setQuery(v)
              setPage(1)
            }}
          />
        </div>

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
                    {item.institution && (
                      <Tag color={CATEGORY_META[item.category].color}>{item.institution}</Tag>
                    )}
                    {item.has_pdf && <Tag color="purple">PDF</Tag>}
                  </Space>
                }
                description={
                  <Space direction="vertical" size={2} style={{ width: '100%' }}>
                    <Space size={10} style={{ fontSize: 12 }}>
                      <Text type="secondary">
                        <ClockCircleOutlined /> {item.publish_time || '—'}
                      </Text>
                      {item.analysts.length > 0 && (
                        <Text type="secondary">分析师 {item.analysts.join(', ')}</Text>
                      )}
                      {item.stocks.length > 0 && (
                        <Space size={4}>
                          {item.stocks.slice(0, 3).map((s) => (
                            <Tag key={s.code} color="blue" style={{ fontSize: 11 }}>
                              {s.name} {s.code}
                            </Tag>
                          ))}
                        </Space>
                      )}
                      {item.industries.slice(0, 3).map((ind) => (
                        <Tag key={ind} style={{ fontSize: 11 }}>
                          {ind}
                        </Tag>
                      ))}
                    </Space>
                    <Paragraph
                      ellipsis={{ rows: 2 }}
                      style={{ margin: 0, fontSize: 12, color: '#64748b' }}
                    >
                      {item.content_preview}
                    </Paragraph>
                  </Space>
                }
              />
            </List.Item>
          )}
        />
      </Card>

      {/* --- Detail drawer --- */}
      <Drawer
        title={detail?.title || '详情'}
        open={detailOpen}
        onClose={() => setDetailOpen(false)}
        width={720}
        extra={
          detail?.web_url ? (
            <a href={detail.web_url} target="_blank" rel="noreferrer">
              <LinkOutlined /> 打开原文
            </a>
          ) : null
        }
      >
        <Spin spinning={detailLoading}>
          {detail ? (
            <div>
              <Space wrap size={6} style={{ marginBottom: 10 }}>
                <Tag color={CATEGORY_META[detail.category].color}>
                  {CATEGORY_META[detail.category].label}
                </Tag>
                {detail.institution && <Tag>{detail.institution}</Tag>}
                {detail.publish_time && (
                  <Tag icon={<ClockCircleOutlined />}>{detail.publish_time}</Tag>
                )}
                {detail.has_pdf && (
                  <Tag color="purple">
                    PDF {detail.pdf_size ? `${Math.round(detail.pdf_size / 1024)} KB` : ''}
                  </Tag>
                )}
              </Space>
              {detail.stocks.length > 0 && (
                <div style={{ marginBottom: 8 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    相关标的:
                  </Text>
                  {detail.stocks.map((s) => (
                    <Tag key={s.code} color="blue">
                      {s.name} {s.code}
                    </Tag>
                  ))}
                </div>
              )}
              {detail.industries.length > 0 && (
                <div style={{ marginBottom: 8 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    行业:
                  </Text>
                  {detail.industries.map((i) => (
                    <Tag key={i}>{i}</Tag>
                  ))}
                </div>
              )}
              {detail.analysts.length > 0 && (
                <div style={{ marginBottom: 8 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    分析师:
                  </Text>
                  {detail.analysts.map((a) => (
                    <Tag key={a} color="geekblue">
                      {a}
                    </Tag>
                  ))}
                </div>
              )}
              <div
                style={{
                  background: '#f8fafc',
                  padding: 12,
                  borderRadius: 4,
                  marginTop: 10,
                  fontSize: 13,
                  lineHeight: 1.8,
                }}
              >
                {detail.content ? (
                  <MarkdownRenderer content={detail.content} />
                ) : (
                  <Text type="secondary">（无正文）</Text>
                )}
              </div>
              <Text type="secondary" style={{ fontSize: 11 }}>
                <FileSearchOutlined /> ID: {detail.id}
                {detail.crawled_at && ` · 抓取于 ${dayjs(detail.crawled_at).format('YYYY-MM-DD HH:mm')}`}
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
