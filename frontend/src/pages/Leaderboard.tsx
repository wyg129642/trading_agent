import { useEffect, useState, useCallback } from 'react'
import {
  Alert,
  Card,
  Table,
  Tag,
  Select,
  Space,
  Typography,
  Statistic,
  Row,
  Col,
  Button,
  Tooltip,
  Progress,
  Segmented,
  InputNumber,
  message,
} from 'antd'
import {
  TrophyOutlined,
  ReloadOutlined,
  RiseOutlined,
  FallOutlined,
  FireOutlined,
  ClockCircleOutlined,
  ExclamationCircleOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import api from '../services/api'

const { Text } = Typography

const CATEGORY_LABELS: Record<string, { en: string; zh: string }> = {
  analyst_research:   { en: 'Analyst Research',     zh: '券商研报' },
  roadshow:           { en: 'Roadshow',             zh: '路演纪要' },
  ai_technology:      { en: 'AI & Technology',      zh: 'AI与科技' },
  semiconductors:     { en: 'Semiconductors',       zh: '半导体' },
  financial_news:     { en: 'Financial News',       zh: '财经新闻' },
  central_banks:      { en: 'Central Banks',        zh: '央行政策' },
  macro_economics:    { en: 'Macro Economics',      zh: '宏观经济' },
  commodities_energy: { en: 'Commodities & Energy', zh: '大宗商品与能源' },
  regulatory:         { en: 'Regulatory & Policy',  zh: '监管与政策' },
  pharma_healthcare:  { en: 'Pharma & Healthcare',  zh: '医药与健康' },
  china_news:         { en: 'China Hot News',       zh: '中国热点' },
  exchanges:          { en: 'Exchanges & Futures',  zh: '交易所与期货' },
  geopolitics:        { en: 'Geopolitics & Trade',  zh: '地缘政治与贸易' },
  portfolio:          { en: 'Portfolio Holdings',   zh: '持仓股监控' },
}

const CATEGORY_COLORS: Record<string, string> = {
  analyst_research: '#1890ff', roadshow: '#722ed1',
  ai_technology: '#9254de', semiconductors: '#1677ff', financial_news: '#fa8c16',
  central_banks: '#eb2f96', macro_economics: '#13c2c2', commodities_energy: '#faad14',
  regulatory: '#f5222d', pharma_healthcare: '#52c41a', china_news: '#ff4d4f',
  exchanges: '#2f54eb', geopolitics: '#a0d911', portfolio: '#597ef7',
}

interface LeaderboardEntry {
  rank: number
  source_name: string
  category: string
  total_signals: number
  accuracy_t0: number | null
  accuracy_t1: number | null
  accuracy_t5: number | null
  accuracy_t20: number | null
  avg_return_bullish: number | null
  avg_return_bearish: number | null
  timeliness_score: number | null
  composite_score: number
  ic_t1: number | null
  ic_t5: number | null
  ic_t20: number | null
  icir: number | null
  avg_confidence: number | null
}

interface LeaderboardData {
  entries: LeaderboardEntry[]
  total_sources: number
  total_signals: number
  last_evaluated: string | null
  period_start: string | null
  period_end: string | null
}

function AccuracyCell({ value }: { value: number | null }) {
  if (value === null || value === undefined) return <Text type="secondary">-</Text>
  const pct = Math.round(value * 100)
  const color = pct >= 60 ? '#52c41a' : pct >= 50 ? '#faad14' : '#ff4d4f'
  return (
    <Tooltip title={`${(value * 100).toFixed(1)}%`}>
      <Progress
        percent={pct}
        size="small"
        strokeColor={color}
        format={() => `${pct}%`}
        style={{ width: 80 }}
      />
    </Tooltip>
  )
}

function ICCell({ value }: { value: number | null }) {
  if (value === null || value === undefined) return <Text type="secondary">-</Text>
  const color = value > 0.05 ? '#52c41a' : value >= 0 ? '#faad14' : '#ff4d4f'
  return <Text style={{ color, fontWeight: 500, fontFamily: 'monospace' }}>{value > 0 ? '+' : ''}{value.toFixed(3)}</Text>
}

function ReturnCell({ value }: { value: number | null }) {
  if (value === null || value === undefined) return <Text type="secondary">-</Text>
  const pct = (value * 100).toFixed(2)
  const isPositive = value > 0
  return (
    <Text style={{ color: isPositive ? '#52c41a' : '#ff4d4f', fontWeight: 500 }}>
      {isPositive ? '+' : ''}{pct}%
    </Text>
  )
}

// Treat null as "less than any number" so null rows sort to the bottom in
// descending order — we care about the BEST sources, not the incomplete ones.
const cmpNullable = (a: number | null, b: number | null) => {
  if (a === null && b === null) return 0
  if (a === null) return -1
  if (b === null) return 1
  return a - b
}

interface EvaluationStats {
  total_evaluations: number
  sources_evaluated: number
  last_run: string | null
  last_signal_time: string | null
  hours_since_last_run: number | null
  period_days: number
  is_stale: boolean
}

export default function Leaderboard() {
  const { i18n } = useTranslation()
  const lang = i18n.language === 'zh' ? 'zh' : 'en'
  const [data, setData] = useState<LeaderboardData | null>(null)
  const [stats, setStats] = useState<EvaluationStats | null>(null)
  const [loading, setLoading] = useState(true)
  const [evaluating, setEvaluating] = useState(false)
  const [errorMsg, setErrorMsg] = useState<string | null>(null)
  // Default to 30 days — short windows can be empty (T+5 not yet resolved)
  // and 7-day data becomes fully stale after two weeks without a refresh.
  const [days, setDays] = useState(30)
  const [minSignals, setMinSignals] = useState(3)
  const [categoryFilter, setCategoryFilter] = useState('')
  const [marketFilter, setMarketFilter] = useState('')
  const [minConfidence, setMinConfidence] = useState(0)
  const [minScore, setMinScore] = useState(0)

  const getCategoryLabel = (cat: string) => {
    const entry = CATEGORY_LABELS[cat]
    return entry ? entry[lang] : cat || '-'
  }

  const fetchLeaderboard = useCallback(async () => {
    setLoading(true)
    setErrorMsg(null)
    try {
      const params: any = { days, min_signals: Math.max(1, minSignals) }
      if (categoryFilter) params.category = categoryFilter
      if (marketFilter) params.market = marketFilter
      if (minConfidence > 0) params.min_confidence = minConfidence
      if (minScore > 0) params.min_score = minScore

      // Use /quick endpoint for short periods (≤7 days), /leaderboard for longer
      const endpoint = days <= 7 ? '/leaderboard/quick' : '/leaderboard'
      const res = await api.get(endpoint, { params })
      setData(res.data)
    } catch (e: any) {
      console.error(e)
      const detail = e?.response?.data?.detail || e?.message || (lang === 'zh' ? '加载失败' : 'Failed to load')
      setErrorMsg(String(detail))
    } finally {
      setLoading(false)
    }
  }, [days, minSignals, categoryFilter, marketFilter, minConfidence, minScore, lang])

  const fetchStats = useCallback(async () => {
    try {
      const res = await api.get<EvaluationStats>('/leaderboard/stats')
      setStats(res.data)
    } catch (e) {
      // stats are advisory — don't block the page
      console.error(e)
    }
  }, [])

  const triggerEvaluation = useCallback(async () => {
    if (evaluating) return
    setEvaluating(true)
    try {
      const res = await api.post('/leaderboard/evaluate', null, {
        params: { days: Math.max(7, days) },
        timeout: 300000, // 5 min — evaluation can be slow (price fetches)
      })
      const { evaluated, updated, skipped, errors, total_signals_found } = res.data || {}
      message.success(
        lang === 'zh'
          ? `评估完成：新增 ${evaluated} / 更新 ${updated} / 跳过 ${skipped} / 错误 ${errors}（共 ${total_signals_found}）`
          : `Done: +${evaluated} / updated ${updated} / skipped ${skipped} / errors ${errors} (of ${total_signals_found})`,
        6,
      )
      await Promise.all([fetchLeaderboard(), fetchStats()])
    } catch (e: any) {
      const detail = e?.response?.data?.detail || e?.message || 'Failed'
      if (e?.response?.status === 403) {
        message.error(lang === 'zh' ? '需要管理员权限才能触发评估' : 'Admin role required to trigger evaluation')
      } else {
        message.error(String(detail))
      }
    } finally {
      setEvaluating(false)
    }
  }, [days, evaluating, fetchLeaderboard, fetchStats, lang])

  useEffect(() => {
    fetchLeaderboard()
  }, [fetchLeaderboard])

  useEffect(() => {
    fetchStats()
  }, [fetchStats])

  const columns = [
    {
      title: '#',
      dataIndex: 'rank',
      key: 'rank',
      width: 50,
      render: (v: number) => {
        if (v === 1) return <TrophyOutlined style={{ color: '#faad14', fontSize: 18 }} />
        if (v === 2) return <TrophyOutlined style={{ color: '#bfbfbf', fontSize: 16 }} />
        if (v === 3) return <TrophyOutlined style={{ color: '#d48806', fontSize: 14 }} />
        return <Text type="secondary">{v}</Text>
      },
    },
    {
      title: lang === 'zh' ? '数据源' : 'Source',
      dataIndex: 'source_name',
      key: 'source_name',
      width: 220,
      render: (v: string) => <Text strong>{v}</Text>,
    },
    {
      title: lang === 'zh' ? '分类' : 'Category',
      dataIndex: 'category',
      key: 'category',
      width: 140,
      render: (v: string) => v ? (
        <Tag color={CATEGORY_COLORS[v] || '#8c8c8c'}>{getCategoryLabel(v)}</Tag>
      ) : '-',
    },
    {
      title: lang === 'zh' ? '信号数' : 'Signals',
      dataIndex: 'total_signals',
      key: 'total_signals',
      width: 70,
      sorter: (a: LeaderboardEntry, b: LeaderboardEntry) => a.total_signals - b.total_signals,
    },
    {
      title: lang === 'zh' ? '当日准确率' : 'T+0',
      dataIndex: 'accuracy_t0',
      key: 'accuracy_t0',
      width: 110,
      render: (v: number | null) => <AccuracyCell value={v} />,
      sorter: (a: LeaderboardEntry, b: LeaderboardEntry) => cmpNullable(a.accuracy_t0, b.accuracy_t0),
    },
    {
      title: lang === 'zh' ? '次日准确率' : 'T+1',
      dataIndex: 'accuracy_t1',
      key: 'accuracy_t1',
      width: 110,
      render: (v: number | null) => <AccuracyCell value={v} />,
      sorter: (a: LeaderboardEntry, b: LeaderboardEntry) => cmpNullable(a.accuracy_t1, b.accuracy_t1),
    },
    {
      title: lang === 'zh' ? '5日准确率' : 'T+5',
      dataIndex: 'accuracy_t5',
      key: 'accuracy_t5',
      width: 110,
      render: (v: number | null) => <AccuracyCell value={v} />,
      sorter: (a: LeaderboardEntry, b: LeaderboardEntry) => cmpNullable(a.accuracy_t5, b.accuracy_t5),
    },
    {
      title: lang === 'zh' ? '月度准确率' : 'T+20',
      dataIndex: 'accuracy_t20',
      key: 'accuracy_t20',
      width: 110,
      render: (v: number | null) => <AccuracyCell value={v} />,
      sorter: (a: LeaderboardEntry, b: LeaderboardEntry) => cmpNullable(a.accuracy_t20, b.accuracy_t20),
    },
    {
      title: 'IC(T+1)',
      dataIndex: 'ic_t1',
      key: 'ic_t1',
      width: 75,
      render: (v: number | null) => <ICCell value={v} />,
      sorter: (a: LeaderboardEntry, b: LeaderboardEntry) => cmpNullable(a.ic_t1, b.ic_t1),
    },
    {
      title: 'IC(T+5)',
      dataIndex: 'ic_t5',
      key: 'ic_t5',
      width: 75,
      render: (v: number | null) => <ICCell value={v} />,
      sorter: (a: LeaderboardEntry, b: LeaderboardEntry) => cmpNullable(a.ic_t5, b.ic_t5),
    },
    {
      title: 'ICIR',
      dataIndex: 'icir',
      key: 'icir',
      width: 70,
      render: (v: number | null) => <ICCell value={v} />,
      sorter: (a: LeaderboardEntry, b: LeaderboardEntry) => cmpNullable(a.icir, b.icir),
    },
    {
      title: <span><RiseOutlined /> {lang === 'zh' ? '看多收益' : 'Bull Ret.'}</span>,
      dataIndex: 'avg_return_bullish',
      key: 'avg_return_bullish',
      width: 90,
      render: (v: number | null) => <ReturnCell value={v} />,
      sorter: (a: LeaderboardEntry, b: LeaderboardEntry) => cmpNullable(a.avg_return_bullish, b.avg_return_bullish),
    },
    {
      title: <span><FallOutlined /> {lang === 'zh' ? '看空收益' : 'Bear Ret.'}</span>,
      dataIndex: 'avg_return_bearish',
      key: 'avg_return_bearish',
      width: 90,
      render: (v: number | null) => <ReturnCell value={v} />,
      // Bear-signal ranking: lower (more negative) return = more accurate, so ascending sort
      sorter: (a: LeaderboardEntry, b: LeaderboardEntry) => cmpNullable(a.avg_return_bearish, b.avg_return_bearish),
    },
    {
      title: <span><FireOutlined /> {lang === 'zh' ? '综合得分' : 'Score'}</span>,
      dataIndex: 'composite_score',
      key: 'composite_score',
      width: 90,
      defaultSortOrder: 'descend' as const,
      sorter: (a: LeaderboardEntry, b: LeaderboardEntry) => a.composite_score - b.composite_score,
      render: (v: number) => (
        <Text strong style={{ color: v >= 0.6 ? '#52c41a' : v >= 0.5 ? '#faad14' : '#ff4d4f' }}>
          {(v * 100).toFixed(1)}%
        </Text>
      ),
    },
  ]

  const categoryOptions = [
    { value: '', label: lang === 'zh' ? '全部分类' : 'All Categories' },
    ...Object.entries(CATEGORY_LABELS).map(([key, labels]) => ({
      value: key, label: labels[lang],
    })),
  ]

  const marketOptions = [
    { value: '', label: lang === 'zh' ? '全部市场' : 'All Markets' },
    { value: 'china', label: lang === 'zh' ? 'A 股' : 'China' },
    { value: 'us', label: lang === 'zh' ? '美股' : 'US' },
    { value: 'hk', label: lang === 'zh' ? '港股' : 'HK' },
    { value: 'global', label: lang === 'zh' ? '全球' : 'Global' },
  ]

  const lastEvalDate = data?.last_evaluated ? new Date(data.last_evaluated) : null
  const lastEvalLabel = lastEvalDate ? lastEvalDate.toLocaleString() : '-'
  const isStale = stats?.is_stale ?? false
  const hoursSince = stats?.hours_since_last_run ?? null
  const staleMessage = (() => {
    if (!isStale) return null
    const hoursStr = hoursSince !== null ? hoursSince.toFixed(0) : '?'
    if (lang === 'zh') {
      return `数据已陈旧：上次评估在 ${hoursStr} 小时前。排行榜只覆盖到 ${lastEvalLabel}。点击"立即评估"更新最新数据。`
    }
    return `Data is stale — last evaluation was ${hoursStr}h ago (up to ${lastEvalLabel}). Click "Evaluate now" to refresh.`
  })()
  const emptyResults = !loading && !errorMsg && (data?.entries?.length ?? 0) === 0

  return (
    <div>
      {errorMsg && (
        <Alert
          type="error"
          showIcon
          style={{ marginBottom: 12 }}
          message={lang === 'zh' ? '加载失败' : 'Failed to load'}
          description={errorMsg}
          closable
          onClose={() => setErrorMsg(null)}
        />
      )}
      {staleMessage && (
        <Alert
          type="warning"
          showIcon
          icon={<ExclamationCircleOutlined />}
          style={{ marginBottom: 12 }}
          message={staleMessage}
          action={
            <Button
              size="small"
              type="primary"
              loading={evaluating}
              onClick={triggerEvaluation}
            >
              {lang === 'zh' ? '立即评估' : 'Evaluate now'}
            </Button>
          }
        />
      )}
      {emptyResults && !isStale && (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          message={
            lang === 'zh'
              ? '当前筛选没有匹配的数据源。尝试增大回溯天数或降低最少信号数。'
              : 'No sources match these filters. Try increasing the lookback window or lowering the minimum signal count.'
          }
        />
      )}
      <Card style={{ marginBottom: 16 }}>
        <Row gutter={[16, 16]}>
          <Col xs={12} sm={6}>
            <Statistic
              title={lang === 'zh' ? '上榜数据源' : 'Ranked Sources'}
              value={data?.total_sources || 0}
              prefix={<TrophyOutlined />}
            />
          </Col>
          <Col xs={12} sm={6}>
            <Statistic
              title={lang === 'zh' ? '总评估信号' : 'Total Signals'}
              value={data?.total_signals || 0}
              prefix={<FireOutlined />}
            />
          </Col>
          <Col xs={12} sm={6}>
            <Statistic
              title={lang === 'zh' ? '最佳综合得分' : 'Top Composite'}
              value={data?.entries?.[0]?.composite_score ? `${(data.entries[0].composite_score * 100).toFixed(1)}%` : '-'}
              prefix={<RiseOutlined />}
            />
          </Col>
          <Col xs={12} sm={6}>
            <Statistic
              title={lang === 'zh' ? '最后评估' : 'Last Evaluated'}
              value={lastEvalDate ? lastEvalDate.toLocaleDateString() : '-'}
              prefix={<ClockCircleOutlined />}
              valueStyle={isStale ? { color: '#faad14' } : undefined}
            />
          </Col>
        </Row>
      </Card>

      <Card>
        <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8 }}>
          <Space wrap>
            <Select
              value={categoryFilter}
              options={categoryOptions}
              onChange={setCategoryFilter}
              style={{ width: 180 }}
              size="small"
            />
            <Select
              value={marketFilter}
              options={marketOptions}
              onChange={setMarketFilter}
              style={{ width: 140 }}
              size="small"
            />
            <Space size={4}>
              <Text type="secondary" style={{ fontSize: 12 }}>
                {lang === 'zh' ? '回溯天数:' : 'Days:'}
              </Text>
              <Segmented
                options={[
                  { value: 1, label: '1d' },
                  { value: 3, label: '3d' },
                  { value: 5, label: '5d' },
                  { value: 7, label: '7d' },
                  { value: 30, label: '30d' },
                  { value: 90, label: '90d' },
                  { value: 180, label: '180d' },
                  { value: 365, label: '1y' },
                ]}
                value={days}
                onChange={(v) => setDays(v as number)}
                size="small"
              />
            </Space>
            <Space size={4}>
              <Text type="secondary" style={{ fontSize: 12 }}>
                {lang === 'zh' ? '最少信号:' : 'Min:'}
              </Text>
              <InputNumber
                value={minSignals}
                onChange={(v) => setMinSignals(v || 3)}
                min={1}
                max={50}
                size="small"
                style={{ width: 60 }}
              />
            </Space>
            <Space size={4}>
              <Text type="secondary" style={{ fontSize: 12 }}>
                {lang === 'zh' ? '最低置信度:' : 'Min Conf:'}
              </Text>
              <InputNumber
                value={minConfidence}
                onChange={(v) => setMinConfidence(v || 0)}
                min={0}
                max={1}
                step={0.1}
                size="small"
                style={{ width: 65 }}
              />
            </Space>
            <Space size={4}>
              <Text type="secondary" style={{ fontSize: 12 }}>
                {lang === 'zh' ? '最低信号强度:' : 'Min Score:'}
              </Text>
              <InputNumber
                value={minScore}
                onChange={(v) => setMinScore(v || 0)}
                min={0}
                max={1}
                step={0.1}
                size="small"
                style={{ width: 65 }}
              />
            </Space>
          </Space>
          <Space>
            <Button
              icon={<ReloadOutlined />}
              onClick={fetchLeaderboard}
              size="small"
            >
              {lang === 'zh' ? '刷新' : 'Refresh'}
            </Button>
            <Tooltip
              title={
                lang === 'zh'
                  ? '对最近的新闻与券商信号重新评估（管理员权限；最长 5 分钟）'
                  : 'Re-score recent signals against actual price moves (admin; up to ~5 min)'
              }
            >
              <Button
                size="small"
                type="default"
                loading={evaluating}
                onClick={triggerEvaluation}
              >
                {lang === 'zh' ? '立即评估' : 'Evaluate now'}
              </Button>
            </Tooltip>
            <Text type="secondary" style={{ fontSize: 11 }}>
              {lang === 'zh'
                ? `每日 16:00 CST 自动评估${hoursSince !== null ? `（已 ${hoursSince.toFixed(0)}h 未刷新）` : ''}`
                : `Auto-evaluates daily 16:00 CST${hoursSince !== null ? ` (last ${hoursSince.toFixed(0)}h ago)` : ''}`}
            </Text>
          </Space>
        </div>

        <Table
          dataSource={data?.entries || []}
          columns={columns}
          rowKey="source_name"
          size="small"
          loading={loading}
          pagination={false}
          scroll={{ x: 1200 }}
        />
      </Card>
    </div>
  )
}
