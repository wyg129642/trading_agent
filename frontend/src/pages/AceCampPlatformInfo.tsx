/**
 * AceCamp 本营 · 平台信息首页
 *
 * 对标 AlphaPai 的 PlatformInfo, 聚合 api.acecamptech.com 的 5 个主页 widget:
 *   · 今日 / 本周发布统计   (/platform-info/acecamp/today-counts)
 *   · 热搜关键词            (/platform-info/acecamp/hot-keywords)
 *   · 热门 feed             (/platform-info/acecamp/hot-feeds)
 *   · 观点广场 (最新)       (/platform-info/acecamp/opinions-index)
 *   · 热门公司              (/platform-info/acecamp/popular-corporations)
 */
import React, { useEffect, useState, useMemo, Component } from 'react'
import {
  Card, Row, Col, Tag, Statistic, Spin, Alert, Typography, Space, Button,
  Empty, List, Avatar, Divider,
} from 'antd'
import {
  FireOutlined, RiseOutlined, FallOutlined, BulbOutlined,
  FileTextOutlined, BankOutlined, StockOutlined, ReloadOutlined,
  ExperimentOutlined, MessageOutlined, ClockCircleOutlined,
  TeamOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'

dayjs.extend(relativeTime)
const { Text, Title, Paragraph } = Typography

// ──────────── Types ────────────
interface TodayCounts {
  today: Record<string, number>
  week: Record<string, number>
  updated_at?: string
}
interface HotKeyword { id: number; keyword: string; topping?: boolean }
interface Corporation {
  id?: number | string
  name?: string
  short_name?: string
  code?: string
  exchange?: string
}
interface OpinionItem {
  item_type: string
  item_id: number
  created_at?: number
  data: {
    id: number
    title?: string
    content?: string
    expected_trend?: 'bullish' | 'bearish' | 'neutral' | 'none' | null
    user?: { name?: string; nickname?: string; organization_name?: string }
    view_count?: number
    like_count?: number
    comment_count?: number
    release_time?: number
    corporations?: Corporation[]
  }
}
interface HotFeedItem {
  id?: number | string
  title?: string
  item_type?: string
  type?: string
  release_time?: number
  organization?: { name?: string } | string
  corporations?: Corporation[]
  summary?: string
  views?: number
  likes?: number
}
interface PopularCorp {
  id: number
  name: string
  short_name?: string
  code?: string
  exchange?: string
  market_cap?: number
  updated_at?: number
  industries?: { name?: string }[]
  stock_updates?: { trend?: string; change_pct?: number }
}

interface SummaryPayload {
  counts: TodayCounts | { error: string }
  hot_keywords: { list: HotKeyword[] } | { error: string }
  hot_feeds: { feeds: HotFeedItem[]; real_spotlights: any[] } | { error: string }
  opinions: { list: OpinionItem[] } | { error: string }
  corporations: { corporations: PopularCorp[]; updated_at?: string } | { error: string }
  generated_at: string
}

// ──────────── Helpers ────────────
function hasErr<T extends object>(x: T | { error: string }): x is { error: string } {
  return typeof (x as any)?.error === 'string'
}

function trendTag(t: string | null | undefined) {
  if (t === 'bullish') {
    return <Tag color="green" style={{ margin: 0 }}><RiseOutlined /> 看多</Tag>
  }
  if (t === 'bearish') {
    return <Tag color="red" style={{ margin: 0 }}><FallOutlined /> 看空</Tag>
  }
  return null
}

function fmtTs(sec?: number) {
  if (!sec) return '—'
  return dayjs.unix(sec).format('MM-DD HH:mm')
}

function stripHtml(s: string, n = 140) {
  const t = (s || '').replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim()
  return t.length > n ? t.slice(0, n) + '…' : t
}

// ──────────── Error Boundary ────────────
// 防白屏: 任一子组件抛异常都在此拦住, 展示错误文本 + 重试提示,
// 而不是让整个 React tree 卸载导致 root 空 div.
class BoundaryImpl extends Component<{ children: React.ReactNode }, { err: Error | null }> {
  state = { err: null as Error | null }
  static getDerivedStateFromError(err: Error) { return { err } }
  componentDidCatch(err: Error, info: any) {
    // eslint-disable-next-line no-console
    console.error('[AceCampPlatformInfo] crash:', err, info)
  }
  render() {
    if (this.state.err) {
      return (
        <div style={{ padding: 20 }}>
          <Alert
            type="error" showIcon
            message="AceCamp 平台信息页渲染失败"
            description={
              <>
                <div style={{ fontFamily: 'monospace', fontSize: 12, marginBottom: 8 }}>{String(this.state.err)}</div>
                <div>尝试 <b>Ctrl+Shift+R</b> 硬刷新 (清前端缓存), 或检查浏览器控制台。</div>
              </>
            }
          />
        </div>
      )
    }
    return <>{this.props.children}</>
  }
}

export default function AceCampPlatformInfo() {
  return (
    <BoundaryImpl>
      <AceCampPlatformInfoInner />
    </BoundaryImpl>
  )
}

function AceCampPlatformInfoInner() {
  const [data, setData] = useState<SummaryPayload | null>(null)
  const [loading, setLoading] = useState(true)
  const [err, setErr] = useState<string | null>(null)

  const load = async () => {
    setLoading(true)
    setErr(null)
    try {
      const res = await api.get<SummaryPayload>('/platform-info/acecamp/summary')
      setData(res.data)
    } catch (e: any) {
      setErr(e?.response?.data?.detail || e?.message || 'load failed')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load()
    const tid = setInterval(load, 60_000)  // 60s 自动刷
    return () => clearInterval(tid)
  }, [])

  const counts = data && !hasErr(data.counts) ? data.counts : null
  const kws = data && !hasErr(data.hot_keywords) ? data.hot_keywords.list : []
  const feeds = data && !hasErr(data.hot_feeds) ? data.hot_feeds.feeds : []
  const opinions = data && !hasErr(data.opinions) ? data.opinions.list : []
  const corps = data && !hasErr(data.corporations) ? data.corporations.corporations : []

  const todayTotal = useMemo(() => {
    if (!counts) return 0
    return Object.entries(counts.today || {})
      .filter(([k]) => !HIDE_TYPES.has(k))
      .reduce((a: number, [, v]: [string, any]) => a + (Number(v) || 0), 0)
  }, [counts])
  const weekTotal = useMemo(() => {
    if (!counts) return 0
    return Object.entries(counts.week || {})
      .filter(([k]) => !HIDE_TYPES.has(k))
      .reduce((a: number, [, v]: [string, any]) => a + (Number(v) || 0), 0)
  }, [counts])

  // 路演 (Event) 已从 UI 移除 — 首页统计接口仍可能返 Event 键, 通过
  // filterOut 忽略. Article/Minute/Spotlight 保留 (平台原命名).
  const TYPE_LABEL: Record<string, string> = {
    Article: '文章',
    Minute: '纪要',
    Spotlight: '专题',
  }
  const TYPE_COLOR: Record<string, string> = {
    Article: '#10b981',
    Minute: '#2563eb',
    Spotlight: '#a855f7',
  }
  const HIDE_TYPES = new Set(['Event'])

  return (
    <div style={{ padding: 20 }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
        <div>
          <Title level={3} style={{ margin: 0 }}>
            <FireOutlined /> AceCamp 平台信息
          </Title>
          <Text type="secondary">
            acecamptech.com · 首页聚合 (热搜 / 热门 feed / 观点广场 / 热门公司) · 每 60s 刷新
          </Text>
        </div>
        <Button icon={<ReloadOutlined />} onClick={load} loading={loading}>刷新</Button>
      </div>

      {err && <Alert type="error" showIcon message="加载失败" description={err} style={{ marginBottom: 16 }} />}

      {/* ── 顶部统计条 ── */}
      <Spin spinning={loading && !data}>
        <Card size="small" style={{ marginBottom: 16 }}>
          <Row gutter={16}>
            <Col xs={12} sm={4}>
              <Statistic title="今日发布" value={todayTotal} valueStyle={{ color: '#f59e0b' }} prefix={<FireOutlined />} />
            </Col>
            <Col xs={12} sm={4}>
              <Statistic title="本周发布" value={weekTotal} valueStyle={{ color: '#2563eb' }} prefix={<RiseOutlined />} />
            </Col>
            {counts && Object.entries(counts.today || {})
              .filter(([k]) => !HIDE_TYPES.has(k))
              .map(([k, v]) => (
                <Col xs={12} sm={4} key={k}>
                  <Statistic
                    title={<span><Tag color={TYPE_COLOR[k] || 'default'} style={{ margin: 0 }}>{TYPE_LABEL[k] || k}</Tag> 今日</span>}
                    value={Number(v) || 0}
                  />
                </Col>
              ))}
          </Row>
          {counts && Object.keys(counts.week || {}).length > 0 && (
            <>
              <Divider style={{ margin: '12px 0' }} />
              <Space size={16} wrap>
                <Text type="secondary" style={{ fontSize: 12 }}>本周分布:</Text>
                {Object.entries(counts.week || {})
                  .filter(([k]) => !HIDE_TYPES.has(k))
                  .map(([k, v]) => (
                    <Tag key={k} color={TYPE_COLOR[k] || 'default'}>
                      {TYPE_LABEL[k] || k} {Number(v) || 0}
                    </Tag>
                  ))}
                {counts.updated_at && (
                  <Text type="secondary" style={{ fontSize: 11 }}>
                    更新于 {dayjs(counts.updated_at).format('HH:mm:ss')}
                  </Text>
                )}
              </Space>
            </>
          )}
        </Card>
      </Spin>

      <Row gutter={[16, 16]}>
        {/* ── 热搜关键词 ── */}
        <Col xs={24} md={8}>
          <Card
            size="small"
            title={<span><FireOutlined style={{ color: '#f59e0b' }} /> 热搜关键词</span>}
            bodyStyle={{ padding: '8px 12px', minHeight: 320 }}
          >
            {kws.length === 0 && !loading ? (
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无数据" />
            ) : (
              <List
                dataSource={kws}
                size="small"
                renderItem={(item, idx) => (
                  <List.Item style={{ padding: '6px 0' }}>
                    <Space>
                      <Tag
                        color={idx < 3 ? 'red' : idx < 6 ? 'orange' : 'default'}
                        style={{ margin: 0, minWidth: 28, textAlign: 'center' }}
                      >
                        {idx + 1}
                      </Tag>
                      <Text strong={idx < 3}>{item.keyword}</Text>
                      {item.topping && <Tag color="red" style={{ fontSize: 10 }}>置顶</Tag>}
                    </Space>
                  </List.Item>
                )}
              />
            )}
          </Card>
        </Col>

        {/* ── 热门公司 ── */}
        <Col xs={24} md={8}>
          <Card
            size="small"
            title={<span><StockOutlined style={{ color: '#2563eb' }} /> 热门公司</span>}
            bodyStyle={{ padding: '8px 12px', minHeight: 320 }}
          >
            {corps.length === 0 && !loading ? (
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无数据" />
            ) : (
              <List
                dataSource={corps.slice(0, 12)}
                size="small"
                renderItem={(c) => {
                  const trend = c.stock_updates?.trend
                  const pct = c.stock_updates?.change_pct
                  const trendColor = trend === 'up' ? '#ef4444' : trend === 'down' ? '#10b981' : '#64748b'
                  return (
                    <List.Item style={{ padding: '6px 0' }}>
                      <Space size={6}>
                        <Tag color="cyan" style={{ margin: 0 }}>
                          {c.code || c.short_name || '—'}{c.exchange ? `.${c.exchange}` : ''}
                        </Tag>
                        <Text strong>{c.name || c.short_name}</Text>
                        {typeof pct === 'number' && (
                          <Text style={{ color: trendColor, fontSize: 12 }}>
                            {pct > 0 ? '+' : ''}{pct.toFixed(2)}%
                          </Text>
                        )}
                        {c.industries?.[0]?.name && (
                          <Tag color="default" style={{ fontSize: 10, margin: 0 }}>
                            {c.industries[0].name}
                          </Tag>
                        )}
                      </Space>
                    </List.Item>
                  )
                }}
              />
            )}
          </Card>
        </Col>

        {/* ── 观点广场 ── */}
        <Col xs={24} md={8}>
          <Card
            size="small"
            title={<span><BulbOutlined style={{ color: '#f59e0b' }} /> 观点广场 · 最新</span>}
            bodyStyle={{ padding: '8px 12px', maxHeight: 520, overflow: 'auto', minHeight: 320 }}
          >
            {opinions.length === 0 && !loading ? (
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无数据" />
            ) : (
              <List
                dataSource={opinions.slice(0, 12)}
                size="small"
                renderItem={(item) => {
                  const d = item.data || ({} as any)
                  const title = d.title || stripHtml(d.content || '', 60)
                  const author = d.user?.nickname || d.user?.name || '—'
                  return (
                    <List.Item style={{ padding: '8px 0', display: 'block' }}>
                      <Space size={6} wrap style={{ marginBottom: 4 }}>
                        {trendTag(d.expected_trend)}
                        <Text strong style={{ fontSize: 13 }}>{title || '(无标题)'}</Text>
                      </Space>
                      <div style={{ fontSize: 11, color: '#64748b' }}>
                        <TeamOutlined /> {author}
                        {d.release_time && <> · <ClockCircleOutlined /> {fmtTs(d.release_time)}</>}
                        {typeof d.view_count === 'number' && d.view_count > 0 && <> · 👁 {d.view_count}</>}
                        {typeof d.like_count === 'number' && d.like_count > 0 && <> · 👍 {d.like_count}</>}
                      </div>
                    </List.Item>
                  )
                }}
              />
            )}
          </Card>
        </Col>
      </Row>

      {/* ── 热门 feed ── */}
      <Card
        size="small"
        title={<span><FileTextOutlined style={{ color: '#10b981' }} /> 热门内容 feed</span>}
        style={{ marginTop: 16 }}
        bodyStyle={{ padding: '8px 12px' }}
      >
        {feeds.length === 0 && !loading ? (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无数据" />
        ) : (
          <List
            dataSource={feeds.slice(0, 20)}
            size="small"
            renderItem={(f: any) => {
              const d = f.data || f
              const typ = f.item_type || d.type || f.type || ''
              const typLabel = TYPE_LABEL[typ] || typ
              const typColor = TYPE_COLOR[typ] || 'default'
              const org =
                typeof d.organization === 'string' ? d.organization :
                d.organization?.name || ''
              const corps = Array.isArray(d.corporations) ? d.corporations.slice(0, 3) : []
              return (
                <List.Item style={{ padding: '10px 0' }}>
                  <List.Item.Meta
                    avatar={<Avatar size={32} style={{ background: typColor }}>{typLabel?.slice(0, 1) || '·'}</Avatar>}
                    title={
                      <Space size={6} wrap>
                        {typLabel && <Tag color={typColor} style={{ color: '#fff', border: 'none', margin: 0 }}>{typLabel}</Tag>}
                        {corps.map((c: any) => (
                          <Tag key={c.code || c.id} color="cyan" style={{ margin: 0 }}>
                            <StockOutlined /> {c.name || c.short_name || c.code}
                          </Tag>
                        ))}
                        <Text strong>{d.title || '(无标题)'}</Text>
                      </Space>
                    }
                    description={
                      <Space size={10} wrap style={{ fontSize: 12, color: '#64748b' }}>
                        {d.release_time && <span><ClockCircleOutlined /> {fmtTs(d.release_time)}</span>}
                        {org && <span><BankOutlined /> {org}</span>}
                        {typeof d.views === 'number' && d.views > 0 && <span>👁 {d.views}</span>}
                        {typeof d.likes === 'number' && d.likes > 0 && <span>👍 {d.likes}</span>}
                      </Space>
                    }
                  />
                </List.Item>
              )
            }}
          />
        )}
      </Card>

      {data?.generated_at && (
        <Paragraph type="secondary" style={{ fontSize: 11, marginTop: 12, textAlign: 'right' }}>
          <ExperimentOutlined /> generated at {dayjs(data.generated_at).format('YYYY-MM-DD HH:mm:ss')}
        </Paragraph>
      )}
    </div>
  )
}
