import { useEffect, useState, useRef, useMemo } from 'react'
import {
  Card, Row, Col, Tag, Statistic, Table, Spin, Alert, Typography, Space,
  Tooltip, Switch, Badge, Divider, Segmented,
} from 'antd'
import {
  ThunderboltOutlined, FireOutlined, BulbOutlined, BarChartOutlined,
  FileTextOutlined, RiseOutlined, ReloadOutlined, PauseCircleOutlined,
  PlayCircleOutlined, CheckCircleFilled, ArrowUpOutlined, ArrowDownOutlined,
} from '@ant-design/icons'
import api from '../services/api'
import MarkdownRenderer from '../components/MarkdownRenderer'

const { Title, Paragraph, Text } = Typography

interface HotWord { word: string; score: number }
interface HotStock { code: string; name: string; changePct: number; num: number }
interface PublicStock {
  stockCode: string; stockName: string;
  feature: string[] | null; feature2: string[] | null;
  dod: number; institutionType: string; rank: number;
}
interface Topic {
  id: string; topicName: string; summary: string;
  newFlag?: boolean; upFlag?: boolean; continueFlag?: boolean;
  stock?: Array<{ code?: string; name?: string }>;
  concept?: Array<{ name?: string }>;
  industry?: Array<{ name?: string }>;
}
interface DailyChild {
  summary?: string;
  stocks?: Array<{ code?: string; name?: string }>;
  title?: string;
}
interface DailyContentSection {
  title: string;
  children: DailyChild[];
}
interface DailySummaryItem {
  id: string; title: string;
  contentJson?: DailyContentSection[];
  content?: string | null;
}

interface Counts {
  today: string
  count_today: {
    roadshowSummaryNum?: number
    investigationNum?: number
    industryWechatArticleNum?: number
    institutionWechatArticleNum?: number
    commentNum?: number
  }
  report_today_num?: number
  report_yesterday_num?: number
  report_week_num?: number
  report_calendar_last_7d?: Record<string, number>
}

interface Summary {
  counts: Counts
  hot_words: { list: HotWord[] }
  hot_stocks: { list: HotStock[] }
  public_fund_stocks: { publicList?: PublicStock[]; privateList?: PublicStock[] }
  hot_topics: { list: Topic[]; updated_at?: string }
  daily_summary: { list: DailySummaryItem[] }
  generated_at: string
}

// Gradient backgrounds for section headers
const SEC_HEADER: React.CSSProperties = {
  background: 'linear-gradient(135deg, #f5f7fa 0%, #e8ecf5 100%)',
  padding: '4px 12px',
  borderRadius: 6,
  margin: '-12px -12px 12px -12px',
  borderBottom: '1px solid #e2e8f0',
}

// AlphaPai mixes markdown bold (**x**) with HTML <br> tags. Normalize to
// pure markdown so remarkGfm can render both.
function normalizeMd(s: string): string {
  if (!s) return ''
  return s
    .replace(/<br\s*\/?\s*>/gi, '\n\n')     // <br> → paragraph break
    .replace(/&nbsp;/g, ' ')                 // common HTML entities
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
}

// Format topic summary: replace **bold** markers + line breaks
function renderTopicSummary(s: string) {
  return <MarkdownRenderer content={normalizeMd(s)} />
}

export default function PlatformInfo() {
  const [data, setData] = useState<Summary | null>(null)
  const [loading, setLoading] = useState(true)
  const [err, setErr] = useState<string | null>(null)
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [refreshSec, setRefreshSec] = useState<number>(15)
  const [lastFetchMs, setLastFetchMs] = useState<number | null>(null)
  const [nextCountdown, setNextCountdown] = useState<number>(0)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const countdownRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const load = async (silent = false) => {
    try {
      if (!silent) setLoading(true)
      const res = await api.get('/platform-info/summary')
      setData(res.data)
      setLastFetchMs(Date.now())
      setErr(null)
    } catch (e: any) {
      setErr(e?.response?.data?.detail || String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load()
  }, [])

  // Auto-refresh loop
  useEffect(() => {
    if (timerRef.current) clearInterval(timerRef.current)
    if (autoRefresh) {
      timerRef.current = setInterval(() => load(true), refreshSec * 1000)
    }
    return () => {
      if (timerRef.current) clearInterval(timerRef.current)
    }
  }, [autoRefresh, refreshSec])

  // Countdown timer for next refresh
  useEffect(() => {
    if (countdownRef.current) clearInterval(countdownRef.current)
    if (!autoRefresh || !lastFetchMs) {
      setNextCountdown(0)
      return
    }
    countdownRef.current = setInterval(() => {
      const elapsed = (Date.now() - lastFetchMs) / 1000
      setNextCountdown(Math.max(0, Math.ceil(refreshSec - elapsed)))
    }, 500)
    return () => {
      if (countdownRef.current) clearInterval(countdownRef.current)
    }
  }, [autoRefresh, refreshSec, lastFetchMs])

  const counts = data?.counts.count_today || {}
  const calList = data?.counts.report_calendar_last_7d || {}
  const dates = useMemo(
    () => Object.keys(calList).sort().reverse().slice(0, 7),
    [calList],
  )

  if (loading && !data)
    return <Spin size="large" style={{ display: 'block', margin: '100px auto' }} />
  if (err && !data)
    return <Alert message="加载失败" description={err} type="error" showIcon />
  if (!data) return null

  return (
    <div style={{ padding: 16, background: '#f8fafc', minHeight: '100vh' }}>
      {/* Header */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: 20,
          padding: '14px 20px',
          background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
          borderRadius: 10,
          color: '#fff',
          boxShadow: '0 4px 12px rgba(102, 126, 234, 0.3)',
        }}
      >
        <div>
          <Title level={3} style={{ color: '#fff', margin: 0, fontSize: 20 }}>
            <ThunderboltOutlined /> 平台信息 · Alpha派首页实时聚合
          </Title>
          <Text style={{ color: 'rgba(255,255,255,0.9)', fontSize: 12 }}>
            热搜词 · 热度个股 · 热议话题 · 每日简报 · 研报日历
          </Text>
        </div>
        <Space size={12}>
          <Badge
            status={autoRefresh ? 'processing' : 'default'}
            text={
              <span style={{ color: '#fff', fontSize: 12 }}>
                {autoRefresh
                  ? `${nextCountdown}s 后刷新`
                  : '已暂停'}
              </span>
            }
          />
          <Segmented
            size="small"
            value={refreshSec}
            onChange={(v) => setRefreshSec(Number(v))}
            options={[
              { value: 10, label: '10s' },
              { value: 15, label: '15s' },
              { value: 30, label: '30s' },
              { value: 60, label: '60s' },
            ]}
          />
          <Switch
            checkedChildren={<PlayCircleOutlined />}
            unCheckedChildren={<PauseCircleOutlined />}
            checked={autoRefresh}
            onChange={setAutoRefresh}
          />
          <Tooltip title="立即刷新">
            <ReloadOutlined
              style={{ fontSize: 18, color: '#fff', cursor: 'pointer' }}
              onClick={() => load(true)}
            />
          </Tooltip>
        </Space>
      </div>

      {/* Metric cards */}
      <Row gutter={[12, 12]}>
        <Col xs={12} sm={8} md={4}>
          <Card
            size="small"
            bordered={false}
            style={{
              background: 'linear-gradient(135deg, #eef2ff 0%, #e0e7ff 100%)',
              borderLeft: '4px solid #6366f1',
            }}
          >
            <Statistic
              title={<Text style={{ fontSize: 12, color: '#64748b' }}>今日研报</Text>}
              value={data.counts.report_today_num || 0}
              valueStyle={{ color: '#4f46e5', fontSize: 24, fontWeight: 700 }}
              suffix={<Text type="secondary" style={{ fontSize: 12 }}>篇</Text>}
            />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Card
            size="small"
            bordered={false}
            style={{
              background: 'linear-gradient(135deg, #f3f4f6 0%, #e5e7eb 100%)',
              borderLeft: '4px solid #6b7280',
            }}
          >
            <Statistic
              title={<Text style={{ fontSize: 12, color: '#64748b' }}>昨日研报</Text>}
              value={data.counts.report_yesterday_num || 0}
              valueStyle={{ color: '#4b5563', fontSize: 24, fontWeight: 700 }}
              suffix={<Text type="secondary" style={{ fontSize: 12 }}>篇</Text>}
            />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Card
            size="small"
            bordered={false}
            style={{
              background: 'linear-gradient(135deg, #dcfce7 0%, #bbf7d0 100%)',
              borderLeft: '4px solid #16a34a',
            }}
          >
            <Statistic
              title={<Text style={{ fontSize: 12, color: '#64748b' }}>本周研报</Text>}
              value={data.counts.report_week_num || 0}
              valueStyle={{ color: '#15803d', fontSize: 24, fontWeight: 700 }}
              suffix={<Text type="secondary" style={{ fontSize: 12 }}>篇</Text>}
            />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Card
            size="small"
            bordered={false}
            style={{
              background: 'linear-gradient(135deg, #fef3c7 0%, #fde68a 100%)',
              borderLeft: '4px solid #d97706',
            }}
          >
            <Statistic
              title={<Text style={{ fontSize: 12, color: '#64748b' }}>会议纪要</Text>}
              value={counts.roadshowSummaryNum || 0}
              valueStyle={{ color: '#b45309', fontSize: 24, fontWeight: 700 }}
              suffix={<Text type="secondary" style={{ fontSize: 12 }}>条</Text>}
            />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Card
            size="small"
            bordered={false}
            style={{
              background: 'linear-gradient(135deg, #e0f2fe 0%, #bae6fd 100%)',
              borderLeft: '4px solid #0284c7',
            }}
          >
            <Statistic
              title={<Text style={{ fontSize: 12, color: '#64748b' }}>券商点评</Text>}
              value={counts.commentNum || 0}
              valueStyle={{ color: '#0369a1', fontSize: 24, fontWeight: 700 }}
              suffix={<Text type="secondary" style={{ fontSize: 12 }}>条</Text>}
            />
          </Card>
        </Col>
      </Row>

      {/* Hot words + Hot stocks */}
      <Row gutter={[12, 12]} style={{ marginTop: 14 }}>
        <Col xs={24} md={8}>
          <Card
            size="small"
            bordered={false}
            style={{ borderRadius: 10, height: '100%' }}
            styles={{ body: { padding: 14 } }}
            title={
              <span style={{ fontSize: 14 }}>
                <FireOutlined style={{ color: '#ef4444', marginRight: 6 }} />
                热搜词 TOP 30
              </span>
            }
            headStyle={{
              background: 'linear-gradient(90deg, #fee2e2 0%, #ffffff 100%)',
              borderBottom: '2px solid #fecaca',
              borderRadius: '10px 10px 0 0',
            }}
          >
            <Space size={[6, 8]} wrap>
              {(data.hot_words.list || []).slice(0, 30).map((w, i) => {
                const rank = i + 1
                const color = rank <= 3 ? '#dc2626' : rank <= 10 ? '#ea580c' : '#2563eb'
                const bg = rank <= 3 ? '#fef2f2' : rank <= 10 ? '#fff7ed' : '#eff6ff'
                return (
                  <Tooltip key={w.word} title={`排名 #${rank}`}>
                    <div
                      style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        padding: '3px 10px',
                        borderRadius: 14,
                        background: bg,
                        color,
                        fontSize: 13,
                        fontWeight: rank <= 10 ? 600 : 500,
                        border: `1px solid ${color}33`,
                      }}
                    >
                      <span
                        style={{
                          fontSize: 11,
                          color: '#94a3b8',
                          marginRight: 4,
                          fontWeight: 400,
                        }}
                      >
                        #{rank}
                      </span>
                      {w.word}
                    </div>
                  </Tooltip>
                )
              })}
            </Space>
          </Card>
        </Col>

        <Col xs={24} md={16}>
          <Card
            size="small"
            bordered={false}
            style={{ borderRadius: 10 }}
            styles={{ body: { padding: 0 } }}
            title={
              <span style={{ fontSize: 14 }}>
                <RiseOutlined style={{ color: '#dc2626', marginRight: 6 }} />
                热度个股 · 研报覆盖量
              </span>
            }
            headStyle={{
              background: 'linear-gradient(90deg, #fef2f2 0%, #ffffff 100%)',
              borderBottom: '2px solid #fecaca',
              borderRadius: '10px 10px 0 0',
            }}
          >
            <Table
              size="small"
              rowKey="code"
              pagination={false}
              dataSource={(data.hot_stocks.list || []).slice(0, 15)}
              columns={[
                {
                  title: '#',
                  width: 40,
                  render: (_, __, i) => (
                    <span
                      style={{
                        display: 'inline-block',
                        width: 22,
                        height: 22,
                        lineHeight: '22px',
                        textAlign: 'center',
                        borderRadius: 11,
                        fontSize: 11,
                        fontWeight: 600,
                        background:
                          i < 3 ? '#dc2626' : i < 10 ? '#fb923c' : '#e2e8f0',
                        color: i < 10 ? '#fff' : '#64748b',
                      }}
                    >
                      {i + 1}
                    </span>
                  ),
                },
                { title: '代码', dataIndex: 'code', width: 100 },
                {
                  title: '名称',
                  dataIndex: 'name',
                  render: (v: string) => <Text strong>{v}</Text>,
                },
                {
                  title: '研报数',
                  dataIndex: 'num',
                  width: 100,
                  sorter: (a, b) => (b.num || 0) - (a.num || 0),
                  render: (v: number) => (
                    <Tag color="blue" style={{ fontWeight: 600 }}>
                      {v}
                    </Tag>
                  ),
                },
                {
                  title: '涨跌幅',
                  dataIndex: 'changePct',
                  width: 110,
                  render: (v: number) => {
                    const up = v > 0
                    const flat = Math.abs(v) < 0.005
                    return (
                      <span
                        style={{
                          color: flat ? '#64748b' : up ? '#dc2626' : '#059669',
                          fontWeight: 600,
                        }}
                      >
                        {up && !flat && <ArrowUpOutlined />}
                        {!up && !flat && <ArrowDownOutlined />}{' '}
                        {up ? '+' : ''}
                        {(v || 0).toFixed(2)}%
                      </span>
                    )
                  },
                },
              ]}
            />
          </Card>
        </Col>
      </Row>

      {/* Hot Topics */}
      <Card
        size="small"
        bordered={false}
        style={{ marginTop: 14, borderRadius: 10 }}
        styles={{ body: { padding: 14 } }}
        title={
          <span style={{ fontSize: 15 }}>
            <BulbOutlined style={{ color: '#f59e0b', marginRight: 6 }} />
            热议话题 · 机构观点批量速递
            <Text type="secondary" style={{ fontSize: 12, marginLeft: 10 }}>
              {data.hot_topics.updated_at ? `更新于 ${data.hot_topics.updated_at}` : ''}
            </Text>
          </span>
        }
        headStyle={{
          background: 'linear-gradient(90deg, #fef3c7 0%, #ffffff 100%)',
          borderBottom: '2px solid #fde68a',
          borderRadius: '10px 10px 0 0',
        }}
      >
        <Row gutter={[12, 12]}>
          {(data.hot_topics.list || []).map((t) => (
            <Col key={t.id} xs={24} md={12}>
              <div
                style={{
                  padding: 12,
                  border: '1px solid #e2e8f0',
                  borderRadius: 8,
                  background: '#fff',
                  height: '100%',
                  transition: 'all 0.2s',
                  cursor: 'default',
                }}
                onMouseEnter={(e) => {
                  ;(e.currentTarget as HTMLDivElement).style.borderColor = '#f59e0b'
                  ;(e.currentTarget as HTMLDivElement).style.boxShadow =
                    '0 2px 8px rgba(245, 158, 11, 0.15)'
                }}
                onMouseLeave={(e) => {
                  ;(e.currentTarget as HTMLDivElement).style.borderColor = '#e2e8f0'
                  ;(e.currentTarget as HTMLDivElement).style.boxShadow = 'none'
                }}
              >
                <div style={{ marginBottom: 6 }}>
                  {t.newFlag && (
                    <Tag color="red" style={{ margin: 0, marginRight: 4 }}>
                      NEW
                    </Tag>
                  )}
                  {t.upFlag && (
                    <Tag color="orange" style={{ margin: 0, marginRight: 4 }}>
                      UP
                    </Tag>
                  )}
                  {t.continueFlag && (
                    <Tag color="blue" style={{ margin: 0, marginRight: 4 }}>
                      续
                    </Tag>
                  )}
                  <Text strong style={{ fontSize: 14, color: '#1e293b' }}>
                    {t.topicName}
                  </Text>
                </div>
                <div
                  style={{
                    fontSize: 13,
                    color: '#475569',
                    maxHeight: 160,
                    overflow: 'auto',
                    lineHeight: 1.6,
                  }}
                >
                  {renderTopicSummary(t.summary)}
                </div>
                {t.stock && t.stock.length > 0 && (
                  <>
                    <Divider style={{ margin: '8px 0 6px' }} />
                    <Space size={[4, 4]} wrap>
                      {t.stock.slice(0, 10).map((s, i) => (
                        <Tag key={i} color="geekblue" style={{ fontSize: 11 }}>
                          {s.name} {s.code}
                        </Tag>
                      ))}
                    </Space>
                  </>
                )}
                {(t.industry?.length || t.concept?.length) && (
                  <div style={{ marginTop: 4 }}>
                    <Space size={[3, 3]} wrap>
                      {(t.industry || []).map((i, idx) => (
                        <Tag key={`i${idx}`} style={{ fontSize: 10, margin: 0 }}>
                          {i.name}
                        </Tag>
                      ))}
                      {(t.concept || []).map((c, idx) => (
                        <Tag
                          key={`c${idx}`}
                          color="purple"
                          style={{ fontSize: 10, margin: 0 }}
                        >
                          {c.name}
                        </Tag>
                      ))}
                    </Space>
                  </div>
                )}
              </div>
            </Col>
          ))}
        </Row>
      </Card>

      {/* Public / Private fund stocks */}
      <Row gutter={[12, 12]} style={{ marginTop: 14 }}>
        <Col xs={24} md={12}>
          <Card
            size="small"
            bordered={false}
            style={{ borderRadius: 10 }}
            styles={{ body: { padding: 0 } }}
            title={
              <span style={{ fontSize: 14 }}>
                <BarChartOutlined style={{ color: '#8b5cf6', marginRight: 6 }} />
                公募基金重仓
              </span>
            }
            headStyle={{
              background: 'linear-gradient(90deg, #f3e8ff 0%, #ffffff 100%)',
              borderBottom: '2px solid #e9d5ff',
              borderRadius: '10px 10px 0 0',
            }}
          >
            <Table
              size="small"
              rowKey="stockCode"
              pagination={false}
              dataSource={(data.public_fund_stocks.publicList || []).slice(0, 10)}
              columns={[
                {
                  title: '#',
                  dataIndex: 'rank',
                  width: 40,
                  render: (v: number) => (
                    <span
                      style={{
                        display: 'inline-block',
                        width: 22,
                        height: 22,
                        lineHeight: '22px',
                        textAlign: 'center',
                        borderRadius: 11,
                        fontSize: 11,
                        fontWeight: 600,
                        background: v <= 3 ? '#8b5cf6' : '#e2e8f0',
                        color: v <= 3 ? '#fff' : '#64748b',
                      }}
                    >
                      {v}
                    </span>
                  ),
                },
                { title: '代码', dataIndex: 'stockCode', width: 100 },
                {
                  title: '名称',
                  dataIndex: 'stockName',
                  render: (v: string) => <Text strong>{v}</Text>,
                },
                {
                  title: '标签',
                  render: (_, r) => (
                    <Space size={[2, 2]} wrap>
                      {(r.feature || []).map((f) => (
                        <Tag key={f} color="purple" style={{ fontSize: 10 }}>
                          {f}
                        </Tag>
                      ))}
                      {(r.feature2 || []).map((f) => (
                        <Tag key={f} color="cyan" style={{ fontSize: 10 }}>
                          {f}
                        </Tag>
                      ))}
                    </Space>
                  ),
                },
                {
                  title: '变化',
                  dataIndex: 'dod',
                  width: 80,
                  render: (v: number) => (
                    <span
                      style={{ color: v > 0 ? '#dc2626' : '#059669', fontWeight: 600 }}
                    >
                      {v > 0 ? <ArrowUpOutlined /> : <ArrowDownOutlined />}{' '}
                      {Math.abs(v || 0).toFixed(1)}
                    </span>
                  ),
                },
              ]}
            />
          </Card>
        </Col>

        <Col xs={24} md={12}>
          <Card
            size="small"
            bordered={false}
            style={{ borderRadius: 10 }}
            styles={{ body: { padding: 0 } }}
            title={
              <span style={{ fontSize: 14 }}>
                <BarChartOutlined style={{ color: '#0891b2', marginRight: 6 }} />
                私募基金重仓
              </span>
            }
            headStyle={{
              background: 'linear-gradient(90deg, #cffafe 0%, #ffffff 100%)',
              borderBottom: '2px solid #a5f3fc',
              borderRadius: '10px 10px 0 0',
            }}
          >
            <Table
              size="small"
              rowKey="stockCode"
              pagination={false}
              dataSource={(data.public_fund_stocks.privateList || []).slice(0, 10)}
              columns={[
                {
                  title: '#',
                  dataIndex: 'rank',
                  width: 40,
                  render: (v: number) => (
                    <span
                      style={{
                        display: 'inline-block',
                        width: 22,
                        height: 22,
                        lineHeight: '22px',
                        textAlign: 'center',
                        borderRadius: 11,
                        fontSize: 11,
                        fontWeight: 600,
                        background: v <= 3 ? '#0891b2' : '#e2e8f0',
                        color: v <= 3 ? '#fff' : '#64748b',
                      }}
                    >
                      {v}
                    </span>
                  ),
                },
                { title: '代码', dataIndex: 'stockCode', width: 100 },
                {
                  title: '名称',
                  dataIndex: 'stockName',
                  render: (v: string) => <Text strong>{v}</Text>,
                },
                {
                  title: '标签',
                  render: (_, r) => (
                    <Space size={[2, 2]} wrap>
                      {(r.feature || []).map((f) => (
                        <Tag key={f} color="purple" style={{ fontSize: 10 }}>
                          {f}
                        </Tag>
                      ))}
                      {(r.feature2 || []).map((f) => (
                        <Tag key={f} color="cyan" style={{ fontSize: 10 }}>
                          {f}
                        </Tag>
                      ))}
                    </Space>
                  ),
                },
                {
                  title: '变化',
                  dataIndex: 'dod',
                  width: 80,
                  render: (v: number) => (
                    <span
                      style={{ color: v > 0 ? '#dc2626' : '#059669', fontWeight: 600 }}
                    >
                      {v > 0 ? <ArrowUpOutlined /> : <ArrowDownOutlined />}{' '}
                      {Math.abs(v || 0).toFixed(1)}
                    </span>
                  ),
                },
              ]}
            />
          </Card>
        </Col>
      </Row>

      {/* Research Calendar */}
      <Card
        size="small"
        bordered={false}
        style={{ marginTop: 14, borderRadius: 10 }}
        styles={{ body: { padding: 14 } }}
        title={
          <span style={{ fontSize: 14 }}>
            <CheckCircleFilled style={{ color: '#059669', marginRight: 6 }} />
            近 7 日研报日历
          </span>
        }
        headStyle={{
          background: 'linear-gradient(90deg, #d1fae5 0%, #ffffff 100%)',
          borderBottom: '2px solid #a7f3d0',
          borderRadius: '10px 10px 0 0',
        }}
      >
        <Row gutter={[10, 10]}>
          {dates.map((d) => {
            const n = calList[d]
            const color =
              n > 2000 ? '#dc2626' : n > 1500 ? '#ea580c' : n > 800 ? '#2563eb' : '#64748b'
            const bg =
              n > 2000 ? '#fef2f2' : n > 1500 ? '#fff7ed' : n > 800 ? '#eff6ff' : '#f8fafc'
            return (
              <Col key={d} xs={12} sm={6} md={3}>
                <div
                  style={{
                    padding: '10px 12px',
                    background: bg,
                    borderRadius: 8,
                    border: `1px solid ${color}33`,
                    textAlign: 'center',
                  }}
                >
                  <div style={{ fontSize: 11, color: '#64748b', marginBottom: 2 }}>
                    {d.slice(5)}
                  </div>
                  <div style={{ fontSize: 20, fontWeight: 700, color }}>
                    {n}
                  </div>
                  <div style={{ fontSize: 10, color: '#94a3b8' }}>篇研报</div>
                </div>
              </Col>
            )
          })}
        </Row>
      </Card>

      {/* Daily Summary (Markdown) — moved to bottom */}
      {data.daily_summary.list && data.daily_summary.list.length > 0 && (
        <Card
          size="small"
          style={{ marginTop: 14, borderRadius: 10 }}
          styles={{ body: { padding: 14 } }}
          bordered={false}
          title={
            <span style={{ fontSize: 15 }}>
              <FileTextOutlined style={{ color: '#d97706', marginRight: 6 }} />
              每日简报
              <Text type="secondary" style={{ fontSize: 12, marginLeft: 8 }}>
                早/午/晚版 · AI 摘要
              </Text>
            </span>
          }
          headStyle={{
            background: 'linear-gradient(90deg, #fef3c7 0%, #fdf4ff 100%)',
            borderBottom: '2px solid #fde68a',
            borderRadius: '10px 10px 0 0',
          }}
        >
          {data.daily_summary.list.map((d, idx) => (
            <div
              key={d.id}
              style={{
                marginBottom: idx < data.daily_summary.list.length - 1 ? 20 : 0,
              }}
            >
              <div
                style={{
                  fontSize: 15,
                  fontWeight: 700,
                  color: '#92400e',
                  background: 'linear-gradient(90deg, #fffbeb 0%, transparent 100%)',
                  padding: '6px 10px',
                  borderLeft: '4px solid #d97706',
                  marginBottom: 10,
                  borderRadius: '0 4px 4px 0',
                }}
              >
                {d.title}
              </div>
              {(d.contentJson || []).map((section, sidx) => (
                <div key={sidx} style={{ marginBottom: 12 }}>
                  <Text strong style={{ color: '#1e293b', fontSize: 13 }}>
                    ▸ {section.title}
                  </Text>
                  <div style={{ paddingLeft: 14, marginTop: 4 }}>
                    {(section.children || []).map((ch, cidx) => (
                      <div
                        key={cidx}
                        style={{
                          padding: '6px 10px',
                          marginBottom: 6,
                          background: '#fafbfd',
                          borderLeft: '2px solid #e2e8f0',
                          borderRadius: 4,
                        }}
                      >
                        {ch.title && (
                          <Text strong style={{ fontSize: 12, color: '#475569' }}>
                            {ch.title}
                          </Text>
                        )}
                        <div style={{ fontSize: 13, color: '#334155' }}>
                          <MarkdownRenderer content={normalizeMd(ch.summary || '')} />
                        </div>
                        {ch.stocks && ch.stocks.length > 0 && (
                          <Space size={[4, 4]} wrap style={{ marginTop: 4 }}>
                            {ch.stocks.map((st, stidx) => (
                              <Tag key={stidx} color="geekblue" style={{ fontSize: 11 }}>
                                {st.name} {st.code}
                              </Tag>
                            ))}
                          </Space>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          ))}
        </Card>
      )}
    </div>
  )
}
