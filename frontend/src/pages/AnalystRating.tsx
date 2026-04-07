import { useEffect, useState, useMemo } from 'react'
import {
  Card,
  Table,
  Tag,
  Select,
  Space,
  Typography,
  Statistic,
  Row,
  Col,
  Tooltip,
  Progress,
  Segmented,
  InputNumber,
  Tabs,
  Modal,
  Empty,
  Spin,
  Badge,
} from 'antd'
import {
  TrophyOutlined,
  RiseOutlined,
  UserOutlined,
  BankOutlined,
  FileTextOutlined,
  TeamOutlined,
  ExperimentOutlined,
  CalendarOutlined,
  StarFilled,
} from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import { useTranslation } from 'react-i18next'

const { Text, Title } = Typography

// ─── Types ──────────────────────────────────────────────────────

interface Analyst {
  rank: number; analyst_id: string; name: string; org: string
  total: number; hits: number; hit_rate: number; excess: number
  conf: number; bull: number; bear: number
}
interface Brokerage {
  rank: number; org: string; total: number; hits: number
  hit_rate: number; excess: number; analysts: number
}
interface Report {
  id: number; ticker: string; title: string; authors: string
  org: string; date: string; signal: string; conf: number
  reason: string; er5: number | null; er20: number | null; er60: number | null
}
interface RawData {
  analysts: Record<string, Analyst[]>
  brokerages: Record<string, Brokerage[]>
  reports: Report[]
  meta: {
    total_reports: number
    date_range: [string, string]
    orgs: string[]
    signal_dist: Record<string, number>
    dist_5d?: Record<string, number>
    dist_20d?: Record<string, number>
    dist_60d?: Record<string, number>
  }
}

// ─── Constants ──────────────────────────────────────────────────

const SIGNAL_CFG: Record<string, { color: string; zh: string; en: string }> = {
  bullish: { color: '#10b981', zh: '看多', en: 'Bullish' },
  neutral: { color: '#94a3b8', zh: '中性', en: 'Neutral' },
  bearish: { color: '#ef4444', zh: '看空', en: 'Bearish' },
}

const TIER_CFG = [
  { tier: 'S', color: '#faad14', bg: 'linear-gradient(135deg,#fffbe6 0%,#fff1b8 100%)', border: '#ffe58f', min: 0.75 },
  { tier: 'A', color: '#52c41a', bg: 'linear-gradient(135deg,#f6ffed 0%,#d9f7be 100%)', border: '#b7eb8f', min: 0.65 },
  { tier: 'B', color: '#1677ff', bg: 'linear-gradient(135deg,#e6f4ff 0%,#bae0ff 100%)', border: '#91caff', min: 0.55 },
  { tier: 'C', color: '#8c8c8c', bg: '#fafafa', border: '#d9d9d9', min: 0 },
]

function getTier(hitRate: number) {
  return TIER_CFG.find(t => hitRate >= t.min) || TIER_CFG[3]
}

// ─── Shared cell renderers ──────────────────────────────────────

function HitRateBar({ value }: { value: number }) {
  const pct = Math.round(value * 100)
  const color = pct >= 70 ? '#52c41a' : pct >= 60 ? '#1677ff' : pct >= 50 ? '#faad14' : '#ff4d4f'
  return (
    <Tooltip title={`${(value * 100).toFixed(1)}%`}>
      <Progress percent={pct} size="small" strokeColor={color}
        format={() => `${pct}%`} style={{ width: 90 }} />
    </Tooltip>
  )
}

function ExcessCell({ value }: { value: number | null | undefined }) {
  if (value == null) return <Text type="secondary">-</Text>
  const pct = (value * 100).toFixed(2)
  const pos = value > 0
  return (
    <Text style={{ color: pos ? '#10b981' : '#ef4444', fontWeight: 600, fontFamily: 'monospace' }}>
      {pos ? '+' : ''}{pct}%
    </Text>
  )
}

function RankBadge({ rank }: { rank: number }) {
  if (rank === 1) return <TrophyOutlined style={{ color: '#faad14', fontSize: 18 }} />
  if (rank === 2) return <TrophyOutlined style={{ color: '#bfbfbf', fontSize: 16 }} />
  if (rank === 3) return <TrophyOutlined style={{ color: '#d48806', fontSize: 14 }} />
  return <Text type="secondary">{rank}</Text>
}

// ─── Top Analyst Card ───────────────────────────────────────────

function TopAnalystCard({ a, rank, lang, onClick }: { a: Analyst; rank: number; lang: string; onClick: () => void }) {
  const tier = getTier(a.hit_rate)
  const medals = ['', '🥇', '🥈', '🥉']
  return (
    <div onClick={onClick} style={{
      background: tier.bg, border: `1px solid ${tier.border}`,
      borderRadius: 10, padding: '14px 16px', cursor: 'pointer',
      transition: 'box-shadow .2s', position: 'relative',
    }}
    onMouseEnter={e => (e.currentTarget.style.boxShadow = '0 4px 16px rgba(0,0,0,.1)')}
    onMouseLeave={e => (e.currentTarget.style.boxShadow = 'none')}>
      {/* Tier badge */}
      <div style={{
        position: 'absolute', top: -8, right: 12,
        background: tier.color, color: '#fff', fontSize: 11, fontWeight: 800,
        padding: '1px 8px', borderRadius: 8, letterSpacing: 1,
      }}>
        {tier.tier}
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
        <span style={{ fontSize: 20 }}>{medals[rank] || ''}</span>
        <div>
          <div style={{ fontWeight: 700, fontSize: 15 }}>{a.name}</div>
          <div style={{ fontSize: 12, color: '#666' }}>{a.org}</div>
        </div>
      </div>
      <Row gutter={8}>
        <Col span={8}>
          <div style={{ fontSize: 11, color: '#888' }}>{lang === 'zh' ? '命中率' : 'Hit Rate'}</div>
          <div style={{ fontSize: 18, fontWeight: 800, color: tier.color }}>
            {(a.hit_rate * 100).toFixed(0)}%
          </div>
        </Col>
        <Col span={8}>
          <div style={{ fontSize: 11, color: '#888' }}>{lang === 'zh' ? '超额收益' : 'Excess'}</div>
          <div style={{ fontSize: 14, fontWeight: 700, color: a.excess > 0 ? '#10b981' : '#ef4444', fontFamily: 'monospace' }}>
            {a.excess > 0 ? '+' : ''}{(a.excess * 100).toFixed(1)}%
          </div>
        </Col>
        <Col span={8}>
          <div style={{ fontSize: 11, color: '#888' }}>{lang === 'zh' ? '研报数' : 'Reports'}</div>
          <div style={{ fontSize: 14, fontWeight: 600 }}>
            {a.hits}/{a.total}
          </div>
        </Col>
      </Row>
    </div>
  )
}

// ─── Main Component ─────────────────────────────────────────────

export default function AnalystRating() {
  const { i18n } = useTranslation()
  const lang = i18n.language === 'zh' ? 'zh' : 'en'

  const [raw, setRaw] = useState<RawData | null>(null)
  const [loading, setLoading] = useState(true)
  const [win, setWin] = useState<string>('20d')
  const [minCalls, setMinCalls] = useState(3)
  const [orgFilter, setOrgFilter] = useState('')
  const [activeTab, setActiveTab] = useState('analysts')

  // Detail modal
  const [modalOpen, setModalOpen] = useState(false)
  const [modalAnalyst, setModalAnalyst] = useState<Analyst | null>(null)

  // Load static JSON
  useEffect(() => {
    setLoading(true)
    fetch('/data/analyst_rating.json')
      .then(r => r.json())
      .then((d: RawData) => setRaw(d))
      .catch(e => console.error('Failed to load analyst data:', e))
      .finally(() => setLoading(false))
  }, [])

  // Filtered data
  const analysts = useMemo(() => {
    if (!raw) return []
    let list = raw.analysts[win] || []
    if (minCalls > 1) list = list.filter(a => a.total >= minCalls)
    if (orgFilter) list = list.filter(a => a.org === orgFilter)
    return list.map((a, i) => ({ ...a, rank: i + 1 }))
  }, [raw, win, minCalls, orgFilter])

  const brokerages = useMemo(() => {
    if (!raw) return []
    let list = raw.brokerages[win] || []
    if (minCalls > 1) list = list.filter(b => b.total >= minCalls)
    if (orgFilter) list = list.filter(b => b.org === orgFilter)
    return list.map((b, i) => ({ ...b, rank: i + 1 }))
  }, [raw, win, minCalls, orgFilter])

  const reports = useMemo(() => {
    if (!raw) return []
    let list = raw.reports
    if (orgFilter) list = list.filter(r => r.org === orgFilter)
    return list
  }, [raw, orgFilter])

  // Analyst's reports for modal
  const modalReports = useMemo(() => {
    if (!modalAnalyst || !raw) return []
    return raw.reports.filter(r =>
      r.authors.includes(modalAnalyst.name) &&
      (!modalAnalyst.org || r.org === modalAnalyst.org)
    ).sort((a, b) => b.date.localeCompare(a.date))
  }, [modalAnalyst, raw])

  // Top 6 analysts (min 5 reports for credibility)
  const topAnalysts = useMemo(() => {
    if (!raw) return []
    const list = (raw.analysts[win] || []).filter(a => a.total >= Math.max(minCalls, 5))
    return list.slice(0, 6)
  }, [raw, win, minCalls])

  // ─── Charts ───────────────────────────────────────────────────

  // Horizontal bar: top 15 brokerages
  const brokerBarOption = useMemo(() => {
    const top = brokerages.filter(b => b.total >= 10).slice(0, 15).reverse()
    return {
      tooltip: {
        trigger: 'axis' as const,
        formatter: (ps: any) => {
          const p = ps[0]
          const b = top[p.dataIndex]
          return `<b>${b.org}</b><br/>` +
            `${lang === 'zh' ? '命中率' : 'Hit Rate'}: ${(b.hit_rate * 100).toFixed(1)}%<br/>` +
            `${lang === 'zh' ? '超额收益' : 'Excess'}: ${(b.excess * 100).toFixed(2)}%<br/>` +
            `${lang === 'zh' ? '研报数' : 'Reports'}: ${b.total} | ` +
            `${lang === 'zh' ? '分析师' : 'Analysts'}: ${b.analysts}`
        },
      },
      grid: { top: 10, right: 80, bottom: 10, left: 100 },
      xAxis: {
        type: 'value' as const, max: 1,
        axisLabel: { formatter: (v: number) => `${(v * 100).toFixed(0)}%` },
        splitLine: { lineStyle: { type: 'dashed' as const, opacity: 0.3 } },
      },
      yAxis: {
        type: 'category' as const,
        data: top.map(b => b.org),
        axisLabel: { fontSize: 11 },
      },
      series: [
        {
          type: 'bar', barWidth: 16,
          data: top.map(b => ({
            value: b.hit_rate,
            itemStyle: {
              color: b.hit_rate >= 0.7 ? '#52c41a' : b.hit_rate >= 0.6 ? '#1677ff' : b.hit_rate >= 0.5 ? '#faad14' : '#ff4d4f',
              borderRadius: [0, 4, 4, 0],
            },
          })),
          label: {
            show: true, position: 'right' as const, fontSize: 11,
            formatter: (p: any) => `${(p.value * 100).toFixed(1)}%`,
          },
        },
      ],
    }
  }, [brokerages, lang])

  // Hit-rate distribution pie
  const distKey = `dist_${win}`
  const distOption = useMemo(() => {
    const dist = (raw?.meta as any)?.[distKey] as Record<string, number> | undefined
    if (!dist) return null
    const colors = ['#ff4d4f', '#faad14', '#1677ff', '#52c41a', '#faad14']
    const labels = Object.keys(dist)
    return {
      tooltip: { trigger: 'item' as const, formatter: '{b}: {c} ({d}%)' },
      legend: { bottom: 0, textStyle: { fontSize: 11 } },
      series: [{
        type: 'pie', radius: ['35%', '65%'],
        center: ['50%', '45%'],
        label: { formatter: '{b}\n{d}%', fontSize: 11 },
        data: labels.map((k, i) => ({
          name: k, value: dist[k],
          itemStyle: { color: colors[i] },
        })),
      }],
    }
  }, [raw, distKey])

  // ─── Table columns ────────────────────────────────────────────

  const analystCols: any[] = [
    { title: '#', dataIndex: 'rank', key: 'rank', width: 50, render: (v: number) => <RankBadge rank={v} /> },
    {
      title: lang === 'zh' ? '分析师' : 'Analyst', key: 'name', width: 130,
      render: (_: any, r: Analyst) => {
        const tier = getTier(r.hit_rate)
        return (
          <a onClick={() => { setModalAnalyst(r); setModalOpen(true) }} style={{ fontWeight: 600 }}>
            {r.name} <Badge count={tier.tier} style={{ backgroundColor: tier.color, fontSize: 10, marginLeft: 4 }} />
          </a>
        )
      },
    },
    { title: lang === 'zh' ? '券商' : 'Brokerage', dataIndex: 'org', key: 'org', width: 130, render: (v: string) => <Tag color="blue">{v}</Tag> },
    { title: lang === 'zh' ? '研报数' : 'Reports', dataIndex: 'total', key: 'total', width: 75, sorter: (a: Analyst, b: Analyst) => a.total - b.total },
    {
      title: lang === 'zh' ? '命中率' : 'Hit Rate', dataIndex: 'hit_rate', key: 'hit_rate', width: 130,
      defaultSortOrder: 'descend' as const,
      sorter: (a: Analyst, b: Analyst) => a.hit_rate - b.hit_rate,
      render: (v: number) => <HitRateBar value={v} />,
    },
    {
      title: lang === 'zh' ? '超额收益' : 'Excess Return', dataIndex: 'excess', key: 'excess', width: 120,
      sorter: (a: Analyst, b: Analyst) => a.excess - b.excess,
      render: (v: number) => <ExcessCell value={v} />,
    },
    {
      title: lang === 'zh' ? '命中' : 'Hits', key: 'hits', width: 70,
      render: (_: any, r: Analyst) => <Text style={{ fontWeight: 500 }}>{r.hits}/{r.total}</Text>,
    },
    {
      title: lang === 'zh' ? '多/空' : 'Bull/Bear', key: 'signals', width: 80,
      render: (_: any, r: Analyst) => (
        <Space size={2}><Tag color="green" style={{ margin: 0 }}>{r.bull}</Tag><Tag color="red" style={{ margin: 0 }}>{r.bear}</Tag></Space>
      ),
    },
  ]

  const brokerageCols: any[] = [
    { title: '#', dataIndex: 'rank', key: 'rank', width: 50, render: (v: number) => <RankBadge rank={v} /> },
    { title: lang === 'zh' ? '券商' : 'Brokerage', dataIndex: 'org', key: 'org', width: 160, render: (v: string) => <Text strong>{v}</Text> },
    { title: lang === 'zh' ? '分析师数' : 'Analysts', dataIndex: 'analysts', key: 'analysts', width: 90, sorter: (a: Brokerage, b: Brokerage) => a.analysts - b.analysts, render: (v: number) => <Space><TeamOutlined />{v}</Space> },
    { title: lang === 'zh' ? '研报数' : 'Reports', dataIndex: 'total', key: 'total', width: 80, sorter: (a: Brokerage, b: Brokerage) => a.total - b.total },
    { title: lang === 'zh' ? '命中率' : 'Hit Rate', dataIndex: 'hit_rate', key: 'hit_rate', width: 130, defaultSortOrder: 'descend' as const, sorter: (a: Brokerage, b: Brokerage) => a.hit_rate - b.hit_rate, render: (v: number) => <HitRateBar value={v} /> },
    { title: lang === 'zh' ? '超额收益' : 'Excess', dataIndex: 'excess', key: 'excess', width: 130, sorter: (a: Brokerage, b: Brokerage) => a.excess - b.excess, render: (v: number) => <ExcessCell value={v} /> },
    { title: lang === 'zh' ? '命中' : 'Hits', key: 'hits', width: 80, render: (_: any, r: Brokerage) => <Text style={{ fontWeight: 500 }}>{r.hits}/{r.total}</Text> },
  ]

  const reportCols: any[] = [
    { title: lang === 'zh' ? '日期' : 'Date', dataIndex: 'date', key: 'date', width: 100, sorter: (a: Report, b: Report) => a.date.localeCompare(b.date) },
    { title: lang === 'zh' ? '标题' : 'Title', dataIndex: 'title', key: 'title', width: 280, ellipsis: true, render: (v: string) => <Tooltip title={v}><Text style={{ fontSize: 13 }}>{v}</Text></Tooltip> },
    { title: lang === 'zh' ? '股票' : 'Ticker', dataIndex: 'ticker', key: 'ticker', width: 80, render: (v: string) => <Tag>{v}</Tag> },
    { title: lang === 'zh' ? '券商' : 'Brokerage', dataIndex: 'org', key: 'org', width: 100 },
    {
      title: lang === 'zh' ? '信号' : 'Signal', dataIndex: 'signal', key: 'signal', width: 75,
      filters: Object.entries(SIGNAL_CFG).map(([k, v]) => ({ text: v[lang], value: k })),
      onFilter: (v: any, r: Report) => r.signal === v,
      render: (v: string) => { const c = SIGNAL_CFG[v] || SIGNAL_CFG.neutral; return <Tag color={c.color}>{c[lang]}</Tag> },
    },
    { title: '5D', dataIndex: 'er5', key: 'er5', width: 90, sorter: (a: Report, b: Report) => (a.er5 || 0) - (b.er5 || 0), render: (v: number | null) => <ExcessCell value={v} /> },
    { title: '20D', dataIndex: 'er20', key: 'er20', width: 90, sorter: (a: Report, b: Report) => (a.er20 || 0) - (b.er20 || 0), render: (v: number | null) => <ExcessCell value={v} /> },
    { title: '60D', dataIndex: 'er60', key: 'er60', width: 90, sorter: (a: Report, b: Report) => (a.er60 || 0) - (b.er60 || 0), render: (v: number | null) => <ExcessCell value={v} /> },
  ]

  // Modal report columns
  const modalCols: any[] = [
    { title: lang === 'zh' ? '日期' : 'Date', dataIndex: 'date', key: 'date', width: 100 },
    { title: lang === 'zh' ? '标题' : 'Title', dataIndex: 'title', key: 'title', ellipsis: true },
    { title: lang === 'zh' ? '股票' : 'Ticker', dataIndex: 'ticker', key: 'ticker', width: 80, render: (v: string) => <Tag>{v}</Tag> },
    { title: lang === 'zh' ? '信号' : 'Signal', dataIndex: 'signal', key: 'signal', width: 70, render: (v: string) => { const c = SIGNAL_CFG[v] || SIGNAL_CFG.neutral; return <Tag color={c.color}>{c[lang]}</Tag> } },
    { title: '5D', dataIndex: 'er5', key: 'er5', width: 85, render: (v: number | null) => <ExcessCell value={v} /> },
    { title: '20D', dataIndex: 'er20', key: 'er20', width: 85, render: (v: number | null) => <ExcessCell value={v} /> },
    { title: '60D', dataIndex: 'er60', key: 'er60', width: 85, render: (v: number | null) => <ExcessCell value={v} /> },
  ]

  // ─── Org options ──────────────────────────────────────────────

  const orgOptions = useMemo(() => [
    { value: '', label: lang === 'zh' ? '全部券商' : 'All Brokerages' },
    ...(raw?.meta?.orgs || []).map(o => ({ value: o, label: o })),
  ], [raw, lang])

  const winLabel = win === '5d' ? (lang === 'zh' ? '5个交易日' : '5 trading days')
    : win === '20d' ? (lang === 'zh' ? '20个交易日' : '20 trading days')
    : (lang === 'zh' ? '60个交易日' : '60 trading days')

  if (loading) {
    return <div style={{ textAlign: 'center', padding: 100 }}><Spin size="large" /></div>
  }

  return (
    <div>
      {/* ──── Header Stats ──── */}
      <Row gutter={[12, 12]} style={{ marginBottom: 12 }}>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title={lang === 'zh' ? '上榜分析师' : 'Ranked Analysts'} value={analysts.length} prefix={<UserOutlined />} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title={lang === 'zh' ? '上榜券商' : 'Ranked Brokerages'} value={brokerages.length} prefix={<BankOutlined />} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title={lang === 'zh' ? '评估研报数' : 'Reports Evaluated'} value={raw?.meta?.total_reports || 0} prefix={<FileTextOutlined />} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic
              title={lang === 'zh' ? '评估时段' : 'Evaluation Period'}
              value={raw?.meta?.date_range ? `${raw.meta.date_range[0].slice(5)} ~ ${raw.meta.date_range[1].slice(5)}` : '-'}
              prefix={<CalendarOutlined />}
              valueStyle={{ fontSize: 16 }}
            />
          </Card>
        </Col>
      </Row>

      {/* ──── Filters Bar ──── */}
      <Card size="small" style={{ marginBottom: 12 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8, alignItems: 'center' }}>
          <Space wrap size="middle">
            <Space size={4}>
              <Text type="secondary" style={{ fontSize: 12 }}>{lang === 'zh' ? '评估窗口:' : 'Window:'}</Text>
              <Segmented
                options={[
                  { value: '5d', label: lang === 'zh' ? '5日' : '5D' },
                  { value: '20d', label: lang === 'zh' ? '20日' : '20D' },
                  { value: '60d', label: lang === 'zh' ? '60日' : '60D' },
                ]}
                value={win} onChange={v => setWin(v as string)} size="small"
              />
            </Space>
            <Select value={orgFilter} options={orgOptions} onChange={setOrgFilter}
              style={{ width: 170 }} size="small" showSearch
              filterOption={(input, opt) => (opt?.label as string)?.includes(input)} />
            <Space size={4}>
              <Text type="secondary" style={{ fontSize: 12 }}>{lang === 'zh' ? '最少研报:' : 'Min:'}</Text>
              <InputNumber value={minCalls} onChange={v => setMinCalls(v || 3)} min={1} max={50} size="small" style={{ width: 55 }} />
            </Space>
          </Space>
          <Tooltip title={
            lang === 'zh'
              ? `评分方法：通过LLM将研报分类为看多/看空/中性信号，对比${winLabel}后股价相对沪深300的超额收益进行回测验证`
              : `Methodology: LLM classifies reports into bullish/bearish/neutral signals, backtested against stock excess returns over CSI 300 after ${winLabel}`
          }>
            <Tag icon={<ExperimentOutlined />} color="purple" style={{ cursor: 'help', fontSize: 12 }}>
              {lang === 'zh' ? '回测评分方法说明' : 'Backtested Scoring'}
            </Tag>
          </Tooltip>
        </div>
      </Card>

      {/* ──── Top Performers Spotlight ──── */}
      {topAnalysts.length > 0 && (
        <Card size="small" style={{ marginBottom: 12 }}
          title={<Space><StarFilled style={{ color: '#faad14' }} />{lang === 'zh' ? `明星分析师 (${winLabel})` : `Star Analysts (${winLabel})`}</Space>}>
          <Row gutter={[12, 12]}>
            {topAnalysts.map((a, i) => (
              <Col xs={12} sm={8} md={4} key={a.analyst_id}>
                <TopAnalystCard a={a} rank={i + 1} lang={lang}
                  onClick={() => { setModalAnalyst(a); setModalOpen(true) }} />
              </Col>
            ))}
          </Row>
        </Card>
      )}

      {/* ──── Charts Row ──── */}
      <Row gutter={[12, 12]} style={{ marginBottom: 12 }}>
        {/* Horizontal bar: brokerage comparison */}
        <Col xs={24}>
          <Card size="small" title={
            <Space>
              <BankOutlined style={{ color: '#1677ff' }} />
              {lang === 'zh' ? '券商命中率排名' : 'Brokerage Hit Rate Ranking'}
              <Text type="secondary" style={{ fontSize: 11, fontWeight: 400 }}>
                {lang === 'zh' ? '(≥10篇研报)' : '(≥10 reports)'}
              </Text>
            </Space>
          }>
            <ReactECharts option={brokerBarOption} style={{ height: 340 }} />
          </Card>
        </Col>
      </Row>

      {/* ──── Distribution Chart ──── */}
      {distOption && (
        <Row gutter={[12, 12]} style={{ marginBottom: 12 }}>
          <Col xs={24} md={8}>
            <Card size="small" title={
              <Space><RiseOutlined style={{ color: '#52c41a' }} />{lang === 'zh' ? '命中率分布' : 'Hit Rate Distribution'}</Space>
            }>
              <ReactECharts option={distOption} style={{ height: 240 }} />
            </Card>
          </Col>
          <Col xs={24} md={16}>
            <Card size="small" title={
              <Space><ExperimentOutlined />{lang === 'zh' ? '评分等级说明' : 'Tier System'}</Space>
            }>
              <div style={{ padding: '12px 0' }}>
                {TIER_CFG.map(t => (
                  <div key={t.tier} style={{
                    display: 'flex', alignItems: 'center', gap: 12, marginBottom: 12,
                    padding: '10px 16px', background: t.bg, borderRadius: 8, border: `1px solid ${t.border}`,
                  }}>
                    <div style={{
                      width: 36, height: 36, borderRadius: 8, background: t.color,
                      display: 'flex', alignItems: 'center', justifyContent: 'center',
                      color: '#fff', fontWeight: 900, fontSize: 18,
                    }}>{t.tier}</div>
                    <div>
                      <div style={{ fontWeight: 600, fontSize: 14 }}>
                        {t.tier === 'S' ? (lang === 'zh' ? '顶级分析师' : 'Top Tier')
                          : t.tier === 'A' ? (lang === 'zh' ? '优秀分析师' : 'Excellent')
                          : t.tier === 'B' ? (lang === 'zh' ? '良好分析师' : 'Good')
                          : (lang === 'zh' ? '一般分析师' : 'Average')}
                      </div>
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        {t.tier === 'S' ? (lang === 'zh' ? `命中率 ≥ 75%，具有显著的市场预判能力，建议重点关注其研报` : 'Hit rate ≥ 75%. Significant market prediction ability. Highly recommended.')
                          : t.tier === 'A' ? (lang === 'zh' ? `命中率 65%-75%，预判能力较强，值得参考其观点` : 'Hit rate 65%-75%. Strong prediction. Worth following.')
                          : t.tier === 'B' ? (lang === 'zh' ? `命中率 55%-65%，略高于随机水平，选择性参考` : 'Hit rate 55%-65%. Slightly above random. Selective reference.')
                          : (lang === 'zh' ? `命中率 < 55%，预测准确度待提升` : 'Hit rate < 55%. Prediction accuracy needs improvement.')}
                      </Text>
                    </div>
                  </div>
                ))}
              </div>
            </Card>
          </Col>
        </Row>
      )}

      {/* ──── Detailed Tables ──── */}
      <Card size="small">
        <Tabs activeKey={activeTab} onChange={setActiveTab} items={[
          {
            key: 'analysts',
            label: <span><UserOutlined /> {lang === 'zh' ? '分析师排行' : 'Analyst Rankings'} ({analysts.length})</span>,
            children: (
              <Table dataSource={analysts} columns={analystCols} rowKey="analyst_id" size="small"
                pagination={{ pageSize: 20, showSizeChanger: true, showTotal: t => `${t} ${lang === 'zh' ? '位分析师' : 'analysts'}` }}
                scroll={{ x: 900 }} />
            ),
          },
          {
            key: 'brokerages',
            label: <span><BankOutlined /> {lang === 'zh' ? '券商排行' : 'Brokerage Rankings'} ({brokerages.length})</span>,
            children: (
              <Table dataSource={brokerages} columns={brokerageCols} rowKey="org" size="small"
                pagination={{ pageSize: 20, showSizeChanger: true, showTotal: t => `${t} ${lang === 'zh' ? '家券商' : 'brokerages'}` }}
                scroll={{ x: 800 }} />
            ),
          },
          {
            key: 'reports',
            label: <span><FileTextOutlined /> {lang === 'zh' ? '研报明细' : 'Reports'} ({reports.length})</span>,
            children: (
              <Table dataSource={reports} columns={reportCols} rowKey="id" size="small"
                pagination={{ pageSize: 20, showSizeChanger: true, showTotal: t => `${t} ${lang === 'zh' ? '篇研报' : 'reports'}` }}
                scroll={{ x: 1000 }}
                expandable={{
                  expandedRowRender: (r: Report) => (
                    <div style={{ padding: '4px 0' }}>
                      <Text type="secondary" style={{ fontSize: 12 }}>{lang === 'zh' ? 'AI判断理由: ' : 'AI Reasoning: '}</Text>
                      <Text style={{ fontSize: 13 }}>{r.reason}</Text>
                    </div>
                  ),
                }} />
            ),
          },
        ]} />
      </Card>

      {/* ──── Analyst Detail Modal ──── */}
      <Modal open={modalOpen} onCancel={() => setModalOpen(false)} footer={null} width={950}
        title={modalAnalyst ? (
          <Space>
            <UserOutlined />
            <span style={{ fontWeight: 700 }}>{modalAnalyst.name}</span>
            <Tag color="blue">{modalAnalyst.org}</Tag>
            <Badge count={getTier(modalAnalyst.hit_rate).tier}
              style={{ backgroundColor: getTier(modalAnalyst.hit_rate).color }} />
          </Space>
        ) : null}>
        {modalAnalyst && (
          <>
            <Row gutter={16} style={{ marginBottom: 16 }}>
              {[
                { label: lang === 'zh' ? '命中率' : 'Hit Rate', value: `${(modalAnalyst.hit_rate * 100).toFixed(1)}%`, color: getTier(modalAnalyst.hit_rate).color },
                { label: lang === 'zh' ? '超额收益' : 'Excess Return', value: `${modalAnalyst.excess > 0 ? '+' : ''}${(modalAnalyst.excess * 100).toFixed(2)}%`, color: modalAnalyst.excess > 0 ? '#10b981' : '#ef4444' },
                { label: lang === 'zh' ? '命中/总数' : 'Hits/Total', value: `${modalAnalyst.hits}/${modalAnalyst.total}`, color: '#1677ff' },
                { label: lang === 'zh' ? '置信度' : 'Confidence', value: `${(modalAnalyst.conf * 100).toFixed(0)}%`, color: '#722ed1' },
              ].map((item, i) => (
                <Col span={6} key={i}>
                  <div style={{ textAlign: 'center', padding: 12, background: '#fafafa', borderRadius: 8 }}>
                    <div style={{ fontSize: 11, color: '#888', marginBottom: 4 }}>{item.label}</div>
                    <div style={{ fontSize: 22, fontWeight: 800, color: item.color, fontFamily: 'monospace' }}>{item.value}</div>
                  </div>
                </Col>
              ))}
            </Row>
            <Title level={5} style={{ marginBottom: 8 }}>
              {lang === 'zh' ? `历史研报 (${modalReports.length}篇)` : `Report History (${modalReports.length})`}
            </Title>
            <Table dataSource={modalReports} columns={modalCols} rowKey="id" size="small"
              pagination={{ pageSize: 8 }}
              locale={{ emptyText: <Empty description={lang === 'zh' ? '暂无研报记录' : 'No reports'} /> }}
              expandable={{
                expandedRowRender: (r: Report) => <Text style={{ fontSize: 13 }}>{r.reason}</Text>,
              }} />
          </>
        )}
      </Modal>
    </div>
  )
}
