/**
 * Funda · 情绪因子 (Sentiment Changes Dashboard)
 *
 * 镜像 funda.ai 的 Sentiment Changes Dashboard:
 *   - 顶部日期区间 + Refresh
 *   - Tabs: Sentiment Trend / Daily Rankings / Charts
 *   - 表格: Ticker | Sector | Trend | per-day score cells
 *   - 分数着色: 绿 ≥7, 黄 4-7, 红 <4, 灰 Low Data
 *   - Trend: HEATING / WARMING / STABLE / COOLING / FALLING
 *
 * 数据: GET /api/funda-db/sentiment/dashboard?from=&to=&ticker=&industry=&trend=
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Alert, Badge, Button, Card, DatePicker, Empty, Input, Select, Space, Spin,
  Table, Tabs, Tag, Typography,
} from 'antd'
import type { ColumnsType } from 'antd/es/table'
import { FundOutlined, ReloadOutlined } from '@ant-design/icons'
import dayjs, { Dayjs } from 'dayjs'
import api from '../services/api'

const { Text, Title } = Typography

type TrendLabel =
  | 'HEATING' | 'WARMING' | 'STABLE' | 'COOLING' | 'FALLING' | 'LOW_DATA'

interface Row {
  ticker: string
  company: string
  sector: string
  industry: string
  scores: Record<string, number>      // date → 0-10 score
  counts: Record<string, number>
  trend_label: TrendLabel
  trend_delta: number
  latest_score: number | null
  latest_date: string | null
  scored_days: number
  low_data: boolean
  // 持仓专属
  stock_name?: string
  stock_market?: string
  funda_ticker?: string | null
}

interface DashboardResp {
  date_range: { from: string; to: string; days: number; dates: string[] }
  total_tickers: number
  total_scored: number
  rows: Row[]
  sectors: string[]
  industries: string[]
  trend_labels: TrendLabel[]
  // 持仓 endpoint 额外返回
  portfolio_total?: number
  portfolio_covered?: number
  portfolio_missing?: { ticker: string; stock_name: string; stock_market: string }[]
  note?: string
}

// 市场标签颜色 — 对齐 Dashboard 持仓概览
const MARKET_TAG_COLORS: Record<string, string> = {
  '美股': 'blue',
  '港股': 'purple',
  '主板': 'red',
  '创业板': 'orange',
  '科创板': 'geekblue',
  '北交所': 'cyan',
  '韩股': 'magenta',
  '日股': 'gold',
}

/* ============ helpers ============ */

function scoreBg(score: number | null | undefined): string {
  if (score === null || score === undefined) return '#f1f5f9'      // gray
  if (score >= 7) return '#16a34a'                                   // green
  if (score >= 5) return '#f59e0b'                                   // amber
  if (score >= 3) return '#f97316'                                   // orange
  return '#ef4444'                                                   // red
}

function scoreText(score: number | null | undefined): string {
  if (score === null || score === undefined) return '#64748b'
  return '#ffffff'
}

function TrendPill({ label, delta }: { label: TrendLabel; delta: number }) {
  const style: Record<TrendLabel, { bg: string; fg: string; text: string }> = {
    HEATING:  { bg: '#fecdd3', fg: '#9f1239', text: 'HEATING' },
    WARMING:  { bg: '#fed7aa', fg: '#9a3412', text: 'WARMING' },
    STABLE:   { bg: '#e5e7eb', fg: '#374151', text: 'STABLE' },
    COOLING:  { bg: '#fce7f3', fg: '#9d174d', text: 'COOLING' },
    FALLING:  { bg: '#dbeafe', fg: '#1e3a8a', text: 'FALLING' },
    LOW_DATA: { bg: '#f1f5f9', fg: '#64748b', text: 'LOW DATA' },
  }
  const s = style[label] || style.STABLE
  return (
    <div style={{ display: 'inline-flex', flexDirection: 'column', alignItems: 'center', gap: 2 }}>
      <span style={{
        background: s.bg, color: s.fg,
        padding: '2px 10px', borderRadius: 10, fontSize: 11, fontWeight: 600,
        letterSpacing: 0.4,
      }}>{s.text}</span>
      {label !== 'LOW_DATA' && (
        <span style={{ fontSize: 11, color: delta >= 0 ? '#16a34a' : '#ef4444' }}>
          {delta >= 0 ? '+' : ''}{delta.toFixed(1)}
        </span>
      )}
    </div>
  )
}

function ScoreCell({ score, count }: { score: number | null | undefined; count?: number }) {
  if (score === null || score === undefined) {
    return (
      <span style={{
        display: 'inline-block', padding: '3px 10px', borderRadius: 12,
        background: '#f1f5f9', color: '#94a3b8', fontSize: 11,
      }}>Low Data</span>
    )
  }
  return (
    <span
      title={count !== undefined ? `${count} 条` : undefined}
      style={{
        display: 'inline-block', minWidth: 42, textAlign: 'center',
        padding: '3px 10px', borderRadius: 14,
        background: scoreBg(score), color: scoreText(score),
        fontWeight: 600, fontSize: 12, fontVariantNumeric: 'tabular-nums',
      }}
    >
      {score.toFixed(1)}
    </span>
  )
}

function formatHeaderDate(dstr: string, todayStr: string): { main: string; sub: string | null } {
  const d = dayjs(dstr)
  const weekday = d.day()
  const main = d.format('MMM D')
  if (dstr === todayStr) return { main, sub: 'Running' }
  if (weekday === 0) return { main, sub: 'Sun' }
  if (weekday === 6) return { main, sub: 'Sat' }
  return { main, sub: null }
}

/* ============ Sentiment Trend Tab ============ */

function SentimentTrendTab({
  data, loading, hideLowData = true, showPortfolioMeta = false,
}: {
  data: DashboardResp | null
  loading: boolean
  hideLowData?: boolean
  showPortfolioMeta?: boolean
}) {
  const [query, setQuery] = useState('')
  const [trendFilter, setTrendFilter] = useState<string>('ALL')
  const [industryFilter, setIndustryFilter] = useState<string>('ALL')
  const [pageSize, setPageSize] = useState(50)

  const todayStr = dayjs().format('YYYY-MM-DD')

  const filtered = useMemo(() => {
    if (!data) return []
    const q = query.trim().toUpperCase()
    return data.rows.filter(r => {
      if (hideLowData && r.low_data) return false
      if (q && !(r.ticker.toUpperCase().includes(q)
          || (r.company || '').toUpperCase().includes(q)
          || (r.stock_name || '').toUpperCase().includes(q))) return false
      if (trendFilter !== 'ALL' && r.trend_label !== trendFilter) return false
      if (industryFilter !== 'ALL' && r.industry !== industryFilter) return false
      return true
    })
  }, [data, query, trendFilter, industryFilter, hideLowData])

  const hidden = data ? data.rows.length - filtered.length : 0

  const columns: ColumnsType<Row> = useMemo(() => {
    if (!data) return []
    const base: ColumnsType<Row> = [
      {
        title: showPortfolioMeta ? '股票' : 'Ticker',
        dataIndex: 'ticker',
        key: 'ticker',
        width: showPortfolioMeta ? 180 : 140,
        fixed: 'left',
        sorter: (a, b) => a.ticker.localeCompare(b.ticker),
        render: (t: string, rec: Row) => (
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
              <span style={{ fontWeight: 700, fontSize: 13, color: '#0f172a' }}>
                {showPortfolioMeta && rec.stock_name ? rec.stock_name : t}
              </span>
              {showPortfolioMeta && rec.stock_market && (
                <Tag color={MARKET_TAG_COLORS[rec.stock_market] || 'default'}
                     style={{ margin: 0, fontSize: 10, padding: '0 5px', lineHeight: '16px' }}>
                  {rec.stock_market}
                </Tag>
              )}
            </div>
            <div style={{ fontSize: 11, color: '#64748b', lineHeight: 1.3 }}>
              {showPortfolioMeta ? (
                <>
                  {t}
                  {rec.funda_ticker && rec.funda_ticker !== t && (
                    <span style={{ marginLeft: 4, color: '#cbd5e1' }}>({rec.funda_ticker})</span>
                  )}
                </>
              ) : (
                (rec.company || '').length > 22 ? (rec.company || '').slice(0, 20) + '…' : rec.company
              )}
            </div>
          </div>
        ),
      },
      {
        title: 'Sector',
        dataIndex: 'sector',
        key: 'sector',
        width: 130,
        sorter: (a, b) => (a.sector || '').localeCompare(b.sector || ''),
        render: (s: string) => (
          <Text style={{ fontSize: 12, color: '#334155' }}>
            {s && s.length > 14 ? s.slice(0, 12) + '…' : (s || '—')}
          </Text>
        ),
      },
      {
        title: 'Trend',
        dataIndex: 'trend_label',
        key: 'trend',
        width: 110,
        sorter: (a, b) => (a.trend_delta || 0) - (b.trend_delta || 0),
        render: (_: string, rec: Row) => <TrendPill label={rec.trend_label} delta={rec.trend_delta} />,
      },
    ]
    // 每个日期一列 (从新到旧, 与截图一致: Apr 20, 19, 18, ...)
    const dateCols: ColumnsType<Row> = data.date_range.dates.map((d) => {
      const h = formatHeaderDate(d, todayStr)
      return {
        title: (
          <div style={{ lineHeight: 1.2, textAlign: 'center' }}>
            <div style={{ fontSize: 12, fontWeight: 600 }}>{h.main}</div>
            {h.sub && (
              <div style={{ fontSize: 10, color: h.sub === 'Running' ? '#2563eb' : '#94a3b8' }}>
                {h.sub === 'Running' ? (
                  <><Badge status="processing" /> {h.sub}</>
                ) : `(${h.sub})`}
              </div>
            )}
          </div>
        ),
        dataIndex: ['scores', d],
        key: `d_${d}`,
        width: 88,
        align: 'center' as const,
        sorter: (a: Row, b: Row) => (a.scores[d] ?? -1) - (b.scores[d] ?? -1),
        render: (_v: unknown, rec: Row) => <ScoreCell score={rec.scores[d]} count={rec.counts[d]} />,
      }
    })
    return [...base, ...dateCols]
  }, [data, todayStr])

  if (loading && !data) return <Spin style={{ padding: 40 }} />
  if (!data) return <Empty description="暂无数据" style={{ padding: 40 }} />

  return (
    <>
      {/* 筛选条: Ticker / Trend / Industry / PageSize */}
      <Space wrap style={{ marginBottom: 12 }} size={12}>
        <Input
          placeholder="Ticker"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          style={{ width: 200 }}
          allowClear
        />
        <Select
          value={trendFilter}
          onChange={setTrendFilter}
          style={{ width: 180 }}
          options={[
            { value: 'ALL', label: 'All Trends' },
            { value: 'HEATING', label: 'HEATING' },
            { value: 'WARMING', label: 'WARMING' },
            { value: 'STABLE', label: 'STABLE' },
            { value: 'COOLING', label: 'COOLING' },
            { value: 'FALLING', label: 'FALLING' },
          ]}
        />
        <Select
          value={industryFilter}
          onChange={setIndustryFilter}
          style={{ width: 260 }}
          showSearch
          options={[
            { value: 'ALL', label: 'All Industries' },
            ...data.industries.map(i => ({ value: i, label: i })),
          ]}
        />
        <Select
          value={pageSize}
          onChange={setPageSize}
          style={{ width: 120 }}
          options={[
            { value: 25, label: '25 per page' },
            { value: 50, label: '50 per page' },
            { value: 100, label: '100 per page' },
            { value: 200, label: '200 per page' },
          ]}
        />
        <Text type="secondary" style={{ fontSize: 12 }}>
          {filtered.length} tickers ({hidden} hidden — insufficient data)
        </Text>
      </Space>

      <Text type="secondary" style={{ fontSize: 11, display: 'block', marginBottom: 6 }}>
        Click column headers to sort. Tickers with insufficient data are hidden.
      </Text>

      <Table<Row>
        size="small"
        rowKey="ticker"
        dataSource={filtered}
        columns={columns}
        pagination={{ pageSize, showSizeChanger: false }}
        scroll={{ x: 'max-content' }}
        sticky
      />
    </>
  )
}

/* ============ My Portfolio Tab ============ */

function MyPortfolioTab({ from, to }: { from: Dayjs; to: Dayjs }) {
  const [data, setData] = useState<DashboardResp | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await api.get<DashboardResp>('/funda-db/sentiment/dashboard/my-portfolio', {
        params: { from: from.format('YYYY-MM-DD'), to: to.format('YYYY-MM-DD') },
      })
      setData(res.data)
    } catch (e: any) {
      setError(e?.response?.data?.detail || e?.message || '加载失败')
    } finally {
      setLoading(false)
    }
  }, [from, to])

  useEffect(() => { load() }, [load])

  if (loading && !data) return <Spin style={{ padding: 40 }} />
  if (error) return <Alert type="error" showIcon message="加载失败" description={error} />
  if (!data) return <Empty description="暂无数据" />

  const covered = data.portfolio_covered ?? 0
  const total = data.portfolio_total ?? 0
  const missing = data.portfolio_missing ?? []

  return (
    <>
      {/* 覆盖率摘要条 */}
      <div style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 8,
        padding: '10px 14px', marginBottom: 12,
      }}>
        <Space size={16} wrap>
          <Text strong style={{ fontSize: 13 }}>
            持仓 {total} 只 · 有数据 <span style={{ color: '#16a34a' }}>{covered}</span>
            {missing.length > 0 && (
              <span style={{ color: '#64748b' }}> · 暂无数据 {missing.length}</span>
            )}
          </Text>
          <Text type="secondary" style={{ fontSize: 11 }}>
            数据源 config/portfolio_sources.yaml
          </Text>
        </Space>
        {data.note && <Text type="warning" style={{ fontSize: 12 }}>{data.note}</Text>}
      </div>

      <SentimentTrendTab data={data} loading={false} hideLowData={false} showPortfolioMeta />

      {/* 未覆盖股票 */}
      {missing.length > 0 && (
        <div style={{ marginTop: 16, padding: 12,
             background: '#fefce8', border: '1px solid #fde68a', borderRadius: 8 }}>
          <Text strong style={{ fontSize: 12 }}>暂无情绪数据的持仓股 ({missing.length}):</Text>
          <Space wrap size={6} style={{ marginTop: 6 }}>
            {missing.map(m => (
              <Tag key={m.ticker}
                   color={MARKET_TAG_COLORS[m.stock_market] || 'default'}
                   style={{ margin: 0, fontSize: 11 }}>
                {m.stock_name || m.ticker}
                <span style={{ marginLeft: 4, color: '#94a3b8', fontSize: 10 }}>
                  {m.ticker}
                </span>
              </Tag>
            ))}
          </Space>
        </div>
      )}
    </>
  )
}


/* ============ Daily Rankings Tab ============ */

function DailyRankingsTab({ data }: { data: DashboardResp | null }) {
  const [pickedDate, setPickedDate] = useState<string>('')

  const dates = data?.date_range.dates || []
  const activeDate = pickedDate || dates[0] || ''

  const ranked = useMemo(() => {
    if (!data || !activeDate) return []
    return data.rows
      .filter(r => typeof r.scores[activeDate] === 'number')
      .map(r => ({ ...r, score_today: r.scores[activeDate], count_today: r.counts[activeDate] }))
      .sort((a, b) => (b.score_today ?? 0) - (a.score_today ?? 0))
  }, [data, activeDate])

  if (!data) return <Empty description="暂无数据" style={{ padding: 40 }} />

  const columns: ColumnsType<Row & { score_today?: number; count_today?: number }> = [
    { title: '#', key: 'rank', width: 56, render: (_: unknown, __: unknown, i: number) => <Text strong>{i + 1}</Text> },
    { title: 'Ticker', dataIndex: 'ticker', key: 'ticker', width: 110,
      render: (t: string, rec: Row) => (
        <div><div style={{ fontWeight: 700 }}>{t}</div>
          <div style={{ fontSize: 11, color: '#64748b' }}>{rec.company}</div></div>
      ) },
    { title: 'Sector', dataIndex: 'sector', key: 'sector', width: 160 },
    { title: 'Industry', dataIndex: 'industry', key: 'industry', width: 180 },
    { title: 'Score', dataIndex: 'score_today', key: 'score', width: 100, align: 'center',
      render: (s: number) => <ScoreCell score={s} /> },
    { title: 'Posts', dataIndex: 'count_today', key: 'count', width: 80, align: 'center',
      render: (c: number) => <Text type="secondary">{c ?? 0}</Text> },
    { title: 'Trend', dataIndex: 'trend_label', key: 'trend', width: 110,
      render: (_: string, r: Row) => <TrendPill label={r.trend_label} delta={r.trend_delta} /> },
  ]

  return (
    <>
      <Space style={{ marginBottom: 12 }}>
        <Text>Date:</Text>
        <Select
          style={{ width: 160 }}
          value={activeDate}
          onChange={setPickedDate}
          options={dates.map(d => ({ value: d, label: dayjs(d).format('MMM D (ddd)') }))}
        />
        <Text type="secondary">{ranked.length} tickers ranked</Text>
      </Space>
      <Table<Row & { score_today?: number; count_today?: number }>
        size="small"
        rowKey="ticker"
        dataSource={ranked}
        columns={columns}
        pagination={{ pageSize: 50, showSizeChanger: false }}
        scroll={{ x: 'max-content' }}
      />
    </>
  )
}

/* ============ Charts Tab (sparkline summary) ============ */

function ChartsTab({ data }: { data: DashboardResp | null }) {
  if (!data) return <Empty description="暂无数据" style={{ padding: 40 }} />

  // 分布统计
  const buckets = { HEATING: 0, WARMING: 0, STABLE: 0, COOLING: 0, FALLING: 0 }
  data.rows.filter(r => !r.low_data).forEach(r => {
    if (r.trend_label in buckets) buckets[r.trend_label as keyof typeof buckets]++
  })
  const total = Object.values(buckets).reduce((a, b) => a + b, 0) || 1

  // 最新日各档
  const latestDate = data.date_range.dates[0]
  const latestScores = data.rows
    .map(r => r.scores[latestDate])
    .filter((s): s is number => typeof s === 'number')
  const bands = {
    '≥8 Very Bullish': latestScores.filter(s => s >= 8).length,
    '7–8 Bullish':    latestScores.filter(s => s >= 7 && s < 8).length,
    '5–7 Neutral':    latestScores.filter(s => s >= 5 && s < 7).length,
    '3–5 Bearish':    latestScores.filter(s => s >= 3 && s < 5).length,
    '<3 Very Bearish': latestScores.filter(s => s < 3).length,
  }
  const avgLatest = latestScores.length > 0
    ? (latestScores.reduce((a, b) => a + b, 0) / latestScores.length).toFixed(2)
    : '—'

  const bar = (n: number, tot: number, color: string) => (
    <div style={{ background: '#f1f5f9', borderRadius: 4, height: 14, width: 240, overflow: 'hidden' }}>
      <div style={{ background: color, height: '100%', width: `${(n / tot) * 100}%` }} />
    </div>
  )

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24 }}>
      <Card size="small" title="Trend Distribution">
        {(['HEATING', 'WARMING', 'STABLE', 'COOLING', 'FALLING'] as const).map(k => {
          const colors: Record<string, string> = {
            HEATING: '#dc2626', WARMING: '#f97316', STABLE: '#64748b',
            COOLING: '#ec4899', FALLING: '#2563eb',
          }
          return (
            <Space key={k} style={{ display: 'flex', marginBottom: 8 }}>
              <div style={{ width: 90 }}>
                <Tag color={colors[k]} style={{ margin: 0 }}>{k}</Tag>
              </div>
              {bar(buckets[k], total, colors[k])}
              <Text style={{ fontVariantNumeric: 'tabular-nums' }}>
                {buckets[k]} ({((buckets[k] / total) * 100).toFixed(0)}%)
              </Text>
            </Space>
          )
        })}
      </Card>
      <Card size="small" title={`Score Distribution · ${dayjs(latestDate).format('MMM D')} · avg ${avgLatest}`}>
        {Object.entries(bands).map(([label, n]) => {
          const color = label.startsWith('≥8') ? '#16a34a'
            : label.startsWith('7') ? '#22c55e'
            : label.startsWith('5') ? '#f59e0b'
            : label.startsWith('3') ? '#f97316'
            : '#ef4444'
          const tot = latestScores.length || 1
          return (
            <Space key={label} style={{ display: 'flex', marginBottom: 8 }}>
              <div style={{ width: 130, fontSize: 12 }}>{label}</div>
              {bar(n, tot, color)}
              <Text style={{ fontVariantNumeric: 'tabular-nums' }}>
                {n} ({((n / tot) * 100).toFixed(0)}%)
              </Text>
            </Space>
          )
        })}
      </Card>
    </div>
  )
}

/* ============ Page ============ */

export default function FundaSentiment() {
  const today = dayjs()
  const defaultFrom = today.subtract(7, 'day')
  const [from, setFrom] = useState<Dayjs>(defaultFrom)
  const [to, setTo] = useState<Dayjs>(today)
  const [data, setData] = useState<DashboardResp | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await api.get<DashboardResp>('/funda-db/sentiment/dashboard', {
        params: { from: from.format('YYYY-MM-DD'), to: to.format('YYYY-MM-DD') },
      })
      setData(res.data)
    } catch (e: any) {
      setError(e?.response?.data?.detail || e?.message || '加载失败')
    } finally {
      setLoading(false)
    }
  }, [from, to])

  useEffect(() => { load() }, [load])

  return (
    <div style={{ padding: 24 }}>
      {/* 顶部: 标题 + 日期 + Refresh */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 8 }}>
        <div>
          <Title level={3} style={{ margin: 0 }}>
            <FundOutlined style={{ marginRight: 6, color: '#2563eb' }} />
            Sentiment Changes Dashboard
          </Title>
          <Text type="secondary" style={{ fontSize: 12 }}>
            funda.ai Twitter / Reddit 零售投资者情绪 · 数据源 <code>api.funda.ai/v1/sentiment-scores</code>
          </Text>
        </div>
        <div style={{ textAlign: 'right' }}>
          <Space size={6}>
            <Text type="secondary">From</Text>
            <DatePicker value={from} onChange={(d) => d && setFrom(d)} allowClear={false} />
            <Text type="secondary">To</Text>
            <DatePicker value={to} onChange={(d) => d && setTo(d)} allowClear={false} />
            <Button type="primary" icon={<ReloadOutlined />} onClick={load} loading={loading}>
              Refresh
            </Button>
          </Space>
          {data && (
            <div style={{ marginTop: 6, fontSize: 11, color: '#64748b' }}>
              ticker_sentiment_trend {data.date_range.from} to {data.date_range.to}
              {' | '}{data.total_scored} tickers scored
            </div>
          )}
        </div>
      </div>

      {error && <Alert type="error" showIcon message="加载失败" description={error} style={{ marginTop: 12 }} />}

      <Tabs
        defaultActiveKey="portfolio"
        style={{ marginTop: 8 }}
        items={[
          { key: 'portfolio', label: '我的持仓情绪',     children: <MyPortfolioTab from={from} to={to} /> },
          { key: 'trend',     label: 'Sentiment Trend', children: <SentimentTrendTab data={data} loading={loading} /> },
          { key: 'rankings',  label: 'Daily Rankings',  children: <DailyRankingsTab data={data} /> },
          { key: 'charts',    label: 'Charts',          children: <ChartsTab data={data} /> },
        ]}
      />
    </div>
  )
}
