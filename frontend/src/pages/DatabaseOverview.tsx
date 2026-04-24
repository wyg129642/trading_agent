import { useEffect, useRef, useState, useCallback } from 'react'
import {
  Card,
  Row,
  Col,
  Statistic,
  Table,
  Button,
  Space,
  Typography,
  Spin,
  Alert,
  Tag,
  Tooltip,
  Divider,
  Badge,
  Segmented,
  Switch,
} from 'antd'
import {
  DatabaseOutlined,
  ReloadOutlined,
  CloudServerOutlined,
  FileSearchOutlined,
  ThunderboltOutlined,
  ClockCircleOutlined,
  WarningOutlined,
  RocketOutlined,
  CheckCircleFilled,
  CloseCircleFilled,
  PauseCircleOutlined,
  PlayCircleOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import api from '../services/api'
import DailyIngestionChart, { PLATFORM_COLORS } from '../components/DailyIngestionChart'

const { Title, Text } = Typography

interface TableItem {
  table: string
  label: string
  count: number | null
  approximate: boolean
  error: string | null
}

interface PostgresGroup {
  group: string
  items: TableItem[]
}

interface MongoItem {
  collection: string
  label: string
  count: number | null
  error: string | null
  oldest_ms?: number | null
  newest_ms?: number | null
  coverage_pct?: number | null
  max_gap_days?: number | null
  max_gap_from?: string | null
  max_gap_to?: string | null
  docs_per_active_day?: number | null
}

interface MongoPlatform {
  platform: string
  database: string
  items: MongoItem[]
  error: string | null
  oldest_ms?: number | null
  newest_ms?: number | null
  span_days?: number | null
  coverage_pct?: number | null
  max_gap_days?: number | null
}

interface RedisInfo {
  available: boolean
  keys: number | null
  error: string | null
}

interface ActiveScraper {
  pid: number
  date: string | null
  mode: 'streaming' | 'date_sweep' | 'bypass'
}

interface BackfillTarget {
  platform: string
  category: string
  db: string
  coll: string
  mode: 'streaming' | 'date_sweep'
  filter?: Record<string, unknown>
  count: number | null
  oldest_ms: number | null
  gap_days_to_cutoff?: number | null
  covered_6m?: boolean
  days_covered?: number | null
  continuous_days?: number | null
  continuous_oldest_ms?: number | null
  active_scrapers?: ActiveScraper[]
  // 人工标记的"回填完毕" (源自 <scraper_db>._state.backfill_complete:<category>)
  backfill_complete?: boolean
  backfill_completed_at?: string | null
  backfill_method?: string | null
  error?: string | null
}

interface IngestRateBucket {
  last_60s: number
  last_5m: number
  last_1h: number
  today_cst: number
}

interface IngestRatePerPlatform extends IngestRateBucket {
  platform: string
  realtime?: IngestRateBucket
  backfill?: IngestRateBucket
  error?: string | null
}

interface IngestRates {
  totals: IngestRateBucket
  totals_realtime?: IngestRateBucket
  totals_backfill?: IngestRateBucket
  per_platform: IngestRatePerPlatform[]
  generated_at: string
  error?: string
}

interface BackfillInfo {
  cutoff_date: string
  cutoff_ms: number
  processes: {
    streaming_orchestrator: boolean
    by_date_orchestrator: boolean
    oversea_summary_refill: boolean
    alphaengine_scheduled: boolean
  }
  children: {
    streaming_scrapers: number
    date_sweep_scrapers: number
    bypass_scrapers?: number
    total_scrapers?: number
  }
  targets: BackfillTarget[]
  active_scrapers_flat?: {
    pid: number
    platform: string | null
    category: string | null
    date: string | null
    mode: 'streaming' | 'date_sweep' | 'bypass'
  }[]
  ingest_rates?: IngestRates
  by_date: {
    total_days: number
    done: number
    skipped_coverage: number
    error: number
    pending: number
  }
  oversea_summary_refill: {
    last_processed_id?: number | null
    filled?: number | null
    still_empty?: number | null
    invalid?: number | null
    error?: number | null
    started_at?: string | null
    updated_at?: string | null
  }
  error?: string
}

interface Overview {
  generated_at: string
  elapsed_ms: number
  backfill?: BackfillInfo
  postgres: { total: number; approximate: boolean; groups: PostgresGroup[] }
  mongodb: { total: number; platforms: MongoPlatform[] }
  redis: RedisInfo
}

const fmt = (n: number | null | undefined): string => {
  if (n === null || n === undefined) return '—'
  return n.toLocaleString('en-US')
}

// Format ms epoch → "YYYY-MM-DD" in CST (Asia/Shanghai, crawler 时区).
// release_time_ms 是 platform 人类发布时间的毫秒戳, 按 CST 展示比 UTC 更直观.
const fmtDateCST = (ms: number | null | undefined): string => {
  if (!ms || !isFinite(ms)) return '—'
  const d = new Date(ms + 8 * 3600 * 1000)
  return d.toISOString().slice(0, 10)
}

// Map PG group name → accent color, keeping product areas visually distinct.
const PG_GROUP_COLORS: Record<string, string> = {
  '用户与权限': '#2563eb',
  '自选与提醒': '#0ea5e9',
  '新闻分析流水线': '#10b981',
  'AI 助手': '#8b5cf6',
  '荐股评分': '#ec4899',
  'AlphaPai 镜像 (PG)': '#6366f1',
  '久谦镜像 (PG)': '#f59e0b',
  '系统运营': '#64748b',
}

// Derive the platform key (lowercase token) from the backend-provided label
// so we can color-match against PLATFORM_COLORS.
function platformKeyOf(label: string): string {
  const lower = label.toLowerCase()
  if (lower.startsWith('alphapai')) return 'alphapai'
  if (lower.startsWith('jinmen')) return 'jinmen'
  if (lower.startsWith('meritco')) return 'meritco'
  if (lower.startsWith('third bridge')) return 'thirdbridge'
  if (lower.startsWith('funda')) return 'funda'
  if (lower.startsWith('gangtise')) return 'gangtise'
  if (lower.startsWith('acecamp')) return 'acecamp'
  if (lower.startsWith('alphaengine')) return 'alphaengine'
  if (lower.startsWith('sentimentrader')) return 'sentimentrader'
  return ''
}

// ── Count cell with approximate-tag + error fallback ─────────────────────
function renderCount(count: number | null, approximate: boolean, error: string | null, approxTip: string) {
  if (error) {
    return (
      <Tooltip title={error}>
        <Tag color="error" icon={<WarningOutlined />} style={{ margin: 0 }}>
          error
        </Tag>
      </Tooltip>
    )
  }
  const prefix = approximate ? '~' : ''
  return (
    <Space size={4} style={{ justifyContent: 'flex-end', width: '100%' }}>
      <Text strong style={{ fontVariantNumeric: 'tabular-nums' }}>
        {prefix}
        {fmt(count)}
      </Text>
      {approximate && (
        <Tooltip title={approxTip}>
          <Tag color="blue" style={{ margin: 0, fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>
            est.
          </Tag>
        </Tooltip>
      )}
    </Space>
  )
}

// Sliding-window history of last-60s ingest rate for a tiny sparkline.
const SPARK_LEN = 30

function Sparkline({ points, color = '#10b981', width = 140, height = 36 }:
    { points: number[]; color?: string; width?: number; height?: number }) {
  if (points.length < 2) {
    return <svg width={width} height={height} />
  }
  const max = Math.max(...points, 1)
  const step = width / (SPARK_LEN - 1)
  const path = points.slice(-SPARK_LEN).map((v, i) => {
    const x = i * step
    const y = height - (v / max) * (height - 4) - 2
    return `${i === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)}`
  }).join(' ')
  const last = points[points.length - 1]
  const lastX = (Math.min(points.length, SPARK_LEN) - 1) * step
  const lastY = height - (last / max) * (height - 4) - 2
  return (
    <svg width={width} height={height} style={{ display: 'block' }}>
      <defs>
        <linearGradient id="sparkgrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity={0.35} />
          <stop offset="100%" stopColor={color} stopOpacity={0} />
        </linearGradient>
      </defs>
      <path d={`${path} L ${lastX} ${height} L 0 ${height} Z`} fill="url(#sparkgrad)" />
      <path d={path} stroke={color} strokeWidth={1.5} fill="none" />
      <circle cx={lastX} cy={lastY} r={2.5} fill={color} />
    </svg>
  )
}

// Group targets by platform so UI reads like "platform → list of categories".
function groupTargetsByPlatform(targets: BackfillTarget[]): Record<string, BackfillTarget[]> {
  const g: Record<string, BackfillTarget[]> = {}
  for (const t of targets) {
    if (!g[t.platform]) g[t.platform] = []
    g[t.platform].push(t)
  }
  return g
}

const PLATFORM_LABELS: Record<string, string> = {
  alphapai: 'AlphaPai',
  jinmen: 'Jinmen',
  meritco: 'Meritco',
  gangtise: 'Gangtise',
  alphaengine: 'AlphaEngine',
  acecamp: 'AceCamp',
  funda: 'Funda',
}

// AceCamp 的 category slug 对齐侧边栏 (中文友好). 其它平台保留英文 slug
// (历史上就是英文, 改成中文会和之前的习惯冲突).
const ACECAMP_CATEGORY_LABELS: Record<string, string> = {
  minutes: '纪要',
  research: '调研',
  article: '文章',
  opinion: '观点',
}

function CategoryLabel(row: BackfillTarget): string {
  // AceCamp 的 subtype filter 已经编码在 category slug 里了 (前端按侧边栏四类
  // 展示), 再把 filter 贴出来会重复 — 所以 AceCamp 不附加 filter tag.
  if (row.platform === 'acecamp') {
    return ACECAMP_CATEGORY_LABELS[row.category] || row.category
  }
  const filterTag = row.filter ? ` ${Object.entries(row.filter).map(([k, v]) => `${k}=${v}`).join(',')}` : ''
  return `${row.category}${filterTag}`
}

export default function DatabaseOverview() {
  const { t } = useTranslation()
  const [data, setData] = useState<Overview | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [sparkHistory, setSparkHistory] = useState<number[]>([])

  // Auto-refresh controls — same pattern as AlphaPai/Jinmen PlatformInfo
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [refreshSec, setRefreshSec] = useState<number>(10)
  const [lastFetchMs, setLastFetchMs] = useState<number | null>(null)
  const [nextCountdown, setNextCountdown] = useState<number>(0)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const countdownRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const fetchData = useCallback(async () => {
    // Background refresh: don't flash the spinner once we have data.
    if (!data) setLoading(true)
    setError(null)
    try {
      const res = await api.get<Overview>('/database-overview')
      setData(res.data)
      const last60 = res.data.backfill?.ingest_rates?.totals?.last_60s ?? 0
      setSparkHistory(prev => [...prev, last60].slice(-SPARK_LEN))
      setLastFetchMs(Date.now())
    } catch (e: any) {
      setError(e.response?.data?.detail || e.message || 'Failed to load')
    } finally {
      setLoading(false)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // initial fetch
  useEffect(() => { fetchData() }, [fetchData])

  // auto-refresh loop
  useEffect(() => {
    if (timerRef.current) clearInterval(timerRef.current)
    if (autoRefresh) {
      timerRef.current = setInterval(fetchData, refreshSec * 1000)
    }
    return () => { if (timerRef.current) clearInterval(timerRef.current) }
  }, [autoRefresh, refreshSec, fetchData])

  // countdown display (updates every 500ms)
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
    return () => { if (countdownRef.current) clearInterval(countdownRef.current) }
  }, [autoRefresh, refreshSec, lastFetchMs])

  if (loading && !data) {
    return (
      <div style={{ textAlign: 'center', padding: 80 }}>
        <Spin size="large" tip={t('dbOverview.loading')} />
      </div>
    )
  }

  if (error && !data) {
    return (
      <Alert
        type="error"
        showIcon
        message={t('dbOverview.fetchFailed')}
        description={error}
        action={
          <Button size="small" onClick={fetchData} icon={<ReloadOutlined />}>
            {t('common.retry')}
          </Button>
        }
      />
    )
  }

  if (!data) return null

  const approxTip = t('dbOverview.approximateTip')

  const pgColumns = [
    {
      title: t('dbOverview.table'),
      dataIndex: 'label',
      key: 'label',
      render: (text: string, row: TableItem) => (
        <Space direction="vertical" size={0}>
          <Text strong>{text}</Text>
          <Text type="secondary" style={{ fontSize: 11, fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace' }}>
            {row.table}
          </Text>
        </Space>
      ),
    },
    {
      title: t('dbOverview.rows'),
      dataIndex: 'count',
      key: 'count',
      align: 'right' as const,
      width: 140,
      render: (_: any, row: TableItem) => renderCount(row.count, row.approximate, row.error, approxTip),
    },
  ]

  const mongoColumns = [
    {
      title: t('dbOverview.collection'),
      dataIndex: 'label',
      key: 'label',
      render: (text: string, row: MongoItem) => {
        const covColor = (row.coverage_pct ?? 100) >= 95 ? '#10b981'
                      : (row.coverage_pct ?? 100) >= 80 ? '#f59e0b' : '#ef4444'
        const gap = row.max_gap_days ?? 0
        return (
          <Space direction="vertical" size={0}>
            <Text strong>{text}</Text>
            <Text type="secondary" style={{ fontSize: 11, fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace' }}>
              {row.collection}
            </Text>
            {row.oldest_ms && row.newest_ms ? (
              <Text type="secondary" style={{ fontSize: 10, fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace' }}>
                {fmtDateCST(row.oldest_ms)} → {fmtDateCST(row.newest_ms)}
                {' · '}
                <span style={{ color: covColor, fontWeight: 600 }}>
                  cov {row.coverage_pct ?? 100}%
                </span>
                {gap >= 7 ? (
                  <span
                    style={{ color: '#ef4444', fontWeight: 600, marginLeft: 4 }}
                    title={row.max_gap_from && row.max_gap_to ? `${row.max_gap_from} → ${row.max_gap_to}` : undefined}
                  >
                    · gap {gap}d
                  </span>
                ) : null}
                {row.docs_per_active_day ? (
                  <span style={{ marginLeft: 6, color: '#6b7280' }} title="平均每个 active day 的入库条数">
                    · {row.docs_per_active_day}/d
                  </span>
                ) : null}
              </Text>
            ) : null}
          </Space>
        )
      },
    },
    {
      title: t('dbOverview.docs'),
      dataIndex: 'count',
      key: 'count',
      align: 'right' as const,
      width: 140,
      render: (_: any, row: MongoItem) => renderCount(row.count, false, row.error, approxTip),
    },
  ]

  // ── Header ─────────────────────────────────────────────────────────────
  return (
    <div>
      {/* Page header */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: 20,
          flexWrap: 'wrap',
          gap: 12,
        }}
      >
        <Space size={14}>
          <div
            style={{
              width: 44,
              height: 44,
              borderRadius: 10,
              background: 'linear-gradient(135deg, #2563eb 0%, #7c3aed 100%)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              boxShadow: '0 4px 12px rgba(37, 99, 235, 0.25)',
            }}
          >
            <DatabaseOutlined style={{ fontSize: 22, color: '#fff' }} />
          </div>
          <div>
            <Title level={4} style={{ margin: 0, lineHeight: 1.2 }}>
              {t('dbOverview.title')}
            </Title>
            <Text type="secondary" style={{ fontSize: 12 }}>
              {t('dbOverview.subtitle')}
            </Text>
          </div>
        </Space>
        <Space size={12} wrap>
          <Text type="secondary" style={{ fontSize: 12 }}>
            <ClockCircleOutlined /> {new Date(data.generated_at).toLocaleTimeString()} ·{' '}
            {data.elapsed_ms.toFixed(0)} ms
          </Text>
          <Badge
            status={autoRefresh ? 'processing' : 'default'}
            text={
              <span style={{ fontSize: 12, color: '#64748b' }}>
                {autoRefresh ? `${nextCountdown}s 后刷新` : '已暂停'}
              </span>
            }
          />
          <Segmented
            size="small"
            value={refreshSec}
            onChange={(v) => setRefreshSec(Number(v))}
            options={[
              { value: 5,  label: '5s' },
              { value: 10, label: '10s' },
              { value: 30, label: '30s' },
              { value: 60, label: '1m' },
            ]}
          />
          <Switch
            checkedChildren={<PlayCircleOutlined />}
            unCheckedChildren={<PauseCircleOutlined />}
            checked={autoRefresh}
            onChange={setAutoRefresh}
          />
          <Button
            type="primary"
            icon={<ReloadOutlined />}
            onClick={fetchData}
            loading={loading}
          >
            {t('dbOverview.refresh')}
          </Button>
        </Space>
      </div>

      {/* Summary stats */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} sm={8}>
          <Card
            style={{
              background: 'linear-gradient(135deg, #eff6ff 0%, #ffffff 60%)',
              border: '1px solid #dbeafe',
            }}
            bodyStyle={{ padding: 20 }}
          >
            <Statistic
              title={
                <Space>
                  <CloudServerOutlined style={{ color: '#2563eb' }} />
                  <span style={{ color: '#1e293b', fontWeight: 500 }}>
                    {t('dbOverview.pgTotal')}
                  </span>
                </Space>
              }
              value={data.postgres.total}
              valueStyle={{ color: '#2563eb', fontSize: 30, fontWeight: 700 }}
              suffix={
                data.postgres.approximate ? (
                  <Tooltip title={approxTip}>
                    <Tag color="blue" style={{ marginLeft: 8, fontSize: 10 }}>
                      部分估算
                    </Tag>
                  </Tooltip>
                ) : null
              }
            />
            <Text type="secondary" style={{ fontSize: 11 }}>
              {data.postgres.groups.length} {t('dbOverview.groups')} · {' '}
              {data.postgres.groups.reduce((acc, g) => acc + g.items.length, 0)}{' '}
              {t('dbOverview.table')}
            </Text>
          </Card>
        </Col>
        <Col xs={24} sm={8}>
          <Card
            style={{
              background: 'linear-gradient(135deg, #ecfdf5 0%, #ffffff 60%)',
              border: '1px solid #d1fae5',
            }}
            bodyStyle={{ padding: 20 }}
          >
            <Statistic
              title={
                <Space>
                  <FileSearchOutlined style={{ color: '#10b981' }} />
                  <span style={{ color: '#1e293b', fontWeight: 500 }}>
                    {t('dbOverview.mongoTotal')}
                  </span>
                </Space>
              }
              value={data.mongodb.total}
              valueStyle={{ color: '#10b981', fontSize: 30, fontWeight: 700 }}
            />
            <Text type="secondary" style={{ fontSize: 11 }}>
              {data.mongodb.platforms.length} {t('dbOverview.platforms')} · {' '}
              {data.mongodb.platforms.reduce((acc, p) => acc + p.items.length, 0)} {t('dbOverview.collection')}
            </Text>
          </Card>
        </Col>
        <Col xs={24} sm={8}>
          <Card
            style={{
              background: 'linear-gradient(135deg, #fffbeb 0%, #ffffff 60%)',
              border: '1px solid #fde68a',
            }}
            bodyStyle={{ padding: 20 }}
          >
            <Statistic
              title={
                <Space>
                  <ThunderboltOutlined style={{ color: '#f59e0b' }} />
                  <span style={{ color: '#1e293b', fontWeight: 500 }}>
                    {t('dbOverview.redisKeys')}
                  </span>
                </Space>
              }
              value={data.redis.keys ?? 0}
              valueStyle={{ color: '#f59e0b', fontSize: 30, fontWeight: 700 }}
              suffix={
                !data.redis.available ? (
                  <Tag color="error" style={{ marginLeft: 8, fontSize: 10 }}>
                    offline
                  </Tag>
                ) : null
              }
            />
            <Text type="secondary" style={{ fontSize: 11 }}>
              {data.redis.available ? '实时速率限制 + 缓存' : data.redis.error || '—'}
            </Text>
          </Card>
        </Col>
      </Row>

      {/* Daily ingestion charts — split into realtime vs backfill so the
          user can tell how much of today's volume came from live watchers
          vs historical catchup. Split signal: crawled_at − release_time_ms
          (< 24h → realtime, else backfill). */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} xl={12}>
          <DailyIngestionChart mode="realtime" />
        </Col>
        <Col xs={24} xl={12}>
          <DailyIngestionChart mode="backfill" />
        </Col>
      </Row>

      {/* Backfill progress */}
      {data.backfill && !data.backfill.error && (() => {
        const bf = data.backfill
        const rates = bf.ingest_rates
        const grouped = groupTargetsByPlatform(bf.targets)
        // Choose platform accent color for the per-platform card headers.
        const platformAccent = (p: string) =>
          (PLATFORM_COLORS as Record<string, string>)[p] || '#6366f1'
        return (
          <>
            <Divider orientation="left" style={{ marginTop: 24, marginBottom: 16 }}>
              <Space>
                <div style={{
                  width: 28, height: 28, borderRadius: 8,
                  background: 'linear-gradient(135deg, #f59e0b 0%, #ec4899 100%)',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  boxShadow: '0 2px 6px rgba(245,158,11,.35)',
                }}>
                  <RocketOutlined style={{ color: '#fff', fontSize: 16 }} />
                </div>
                <span style={{ fontWeight: 600, fontSize: 15 }}>回填进度</span>
                <Tag color="default" style={{ margin: 0, fontFamily: 'ui-monospace, Menlo, monospace', fontSize: 11 }}>
                  cutoff {bf.cutoff_date}
                </Tag>
              </Space>
            </Divider>

            {/* ── Live ingest stats banner — split RT vs backfill ─── */}
            {(() => {
              const rtTotals = rates?.totals_realtime
              const bfTotals = rates?.totals_backfill
              // Two-column layout: realtime (cyan) on the left, backfill (amber) on the right.
              // Each column shows 60s / 5m / 1h / today. Backward-compatible with old
              // backend that only emits `totals` (shown once under a single label).
              const hasSplit = !!(rtTotals && bfTotals)
              const columns = hasSplit
                ? [
                    { key: 'realtime', title: '实时入库', accent: '#22d3ee', totals: rtTotals! },
                    { key: 'backfill', title: '回填入库', accent: '#f59e0b', totals: bfTotals! },
                  ]
                : [{ key: 'all', title: '入库总量', accent: '#22d3ee', totals: rates?.totals ?? { last_60s: 0, last_5m: 0, last_1h: 0, today_cst: 0 } }]
              return (
                <Card
                  size="small"
                  style={{
                    marginBottom: 16,
                    background: 'linear-gradient(135deg, #0f172a 0%, #1e293b 100%)',
                    border: '1px solid rgba(148, 163, 184, 0.2)',
                  }}
                  styles={{ body: { padding: 16 } }}
                >
                  <Row gutter={[20, 16]} align="top">
                    {columns.map(col => (
                      <Col xs={24} lg={hasSplit ? 10 : 14} key={col.key}>
                        <div style={{
                          display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10,
                        }}>
                          <span className="ta-pulse-dot" style={{
                            width: 8, height: 8, borderRadius: '50%',
                            background: col.accent, display: 'inline-block',
                            boxShadow: `0 0 0 0 ${col.accent}`,
                          }} />
                          <Text style={{ color: '#e2e8f0', fontSize: 13, fontWeight: 600 }}>
                            {col.title}
                          </Text>
                        </div>
                        <Space size={20} wrap style={{ width: '100%' }}>
                          {[
                            { label: '过去 60s', value: col.totals.last_60s ?? 0, emphasis: true },
                            { label: '过去 5m',  value: col.totals.last_5m  ?? 0 },
                            { label: '过去 1h',  value: col.totals.last_1h  ?? 0 },
                            { label: '今日',     value: col.totals.today_cst ?? 0 },
                          ].map(s => (
                            <div key={s.label}>
                              <Text style={{ color: '#94a3b8', fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.5 }}>
                                {s.label}
                              </Text>
                              <div style={{
                                color: col.accent,
                                fontSize: s.emphasis ? 22 : 18,
                                fontWeight: 700,
                                fontVariantNumeric: 'tabular-nums',
                                lineHeight: 1.2,
                              }}>
                                {fmt(s.value)}
                              </div>
                            </div>
                          ))}
                        </Space>
                      </Col>
                    ))}
                    <Col xs={24} lg={4}>
                      <Space direction="vertical" size={2} style={{ width: '100%' }}>
                        <Text style={{ color: '#94a3b8', fontSize: 11 }}>总入库（每 10 秒采样）</Text>
                        <Sparkline points={sparkHistory} color="#22d3ee" width={220} height={44} />
                      </Space>
                    </Col>
                  </Row>

                  {/* Per-platform breakdown with RT vs BF mini-rows */}
                  {rates?.per_platform && rates.per_platform.some(p => (p.last_5m ?? 0) > 0) && (
                    <div style={{ marginTop: 14, paddingTop: 12, borderTop: '1px solid rgba(148,163,184,0.15)' }}>
                      <Row gutter={[12, 10]}>
                        {rates.per_platform
                          .filter(p => (p.last_1h ?? 0) > 0 || (p.today_cst ?? 0) > 0)
                          .sort((a, b) => (b.last_5m ?? 0) - (a.last_5m ?? 0))
                          .map(p => {
                            const pColor = platformAccent(p.platform)
                            const max5m = Math.max(1, ...rates.per_platform.map(x => x.last_5m ?? 0))
                            const rt5 = p.realtime?.last_5m ?? 0
                            const bf5 = p.backfill?.last_5m ?? 0
                            const rtPct = (rt5 / max5m) * 100
                            const bfPct = (bf5 / max5m) * 100
                            return (
                              <Col xs={24} sm={12} md={8} lg={6} key={p.platform}>
                                <div style={{ fontSize: 11 }}>
                                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                                    <span style={{ color: '#e2e8f0', fontWeight: 600 }}>
                                      {PLATFORM_LABELS[p.platform] || p.platform}
                                    </span>
                                    <span style={{ color: '#94a3b8', fontFamily: 'ui-monospace, Menlo, monospace' }}>
                                      5m: <span style={{ color: pColor }}>{fmt(rt5)}</span>
                                      {' / '}
                                      <span style={{ color: '#f59e0b' }}>{fmt(bf5)}</span>
                                    </span>
                                  </div>
                                  {/* Stacked bar: RT (solid) above, BF (striped-style tint) below */}
                                  <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                                    <div style={{ height: 3, background: 'rgba(148,163,184,0.12)', borderRadius: 2, overflow: 'hidden' }}>
                                      <div style={{
                                        height: '100%', width: `${rtPct}%`,
                                        background: pColor, transition: 'width 0.6s ease',
                                      }} />
                                    </div>
                                    <div style={{ height: 3, background: 'rgba(148,163,184,0.12)', borderRadius: 2, overflow: 'hidden' }}>
                                      <div style={{
                                        height: '100%', width: `${bfPct}%`,
                                        background: 'repeating-linear-gradient(45deg, #f59e0b, #f59e0b 3px, #d9770688 3px, #d9770688 6px)',
                                        transition: 'width 0.6s ease',
                                      }} />
                                    </div>
                                  </div>
                                </div>
                              </Col>
                            )
                          })}
                      </Row>
                      <div style={{ marginTop: 8, color: '#64748b', fontSize: 10 }}>
                        每平台 5 min 入库: 上条 <span style={{ color: '#22d3ee' }}>实时</span>（实时 watcher 抓到的新发布）/
                        下条 <span style={{ color: '#f59e0b' }}>回填</span>（crawled_at − release_time_ms &gt; 24h）
                      </div>
                    </div>
                  )}
                </Card>
              )
            })()}

            {/* ── Total scrapers running banner ──────────────────── */}
            <Card size="small" style={{ marginBottom: 12, background: '#f0f9ff', borderColor: '#bae6fd' }}>
              <Space size={24} wrap>
                <Space>
                  <span className="ta-pulse-dot" style={{
                    width: 10, height: 10, borderRadius: '50%',
                    background: '#0ea5e9', color: '#0ea5e9',
                    display: 'inline-block',
                  }} />
                  <Text strong style={{ fontSize: 15, color: '#0c4a6e' }}>
                    共 {bf.children.total_scrapers ?? (bf.children.streaming_scrapers + bf.children.date_sweep_scrapers)} 个 scraper 正在往回爬
                  </Text>
                </Space>
                <Text type="secondary" style={{ fontSize: 12 }}>
                  流式 {bf.children.streaming_scrapers} · 日扫 {bf.children.date_sweep_scrapers}
                </Text>
                {(bf.active_scrapers_flat && bf.active_scrapers_flat.filter(s => s.date).length > 0) && (
                  <Space size={4} wrap>
                    <Text style={{ fontSize: 12, color: '#475569' }}>今正在抓的日期:</Text>
                    {[...new Set(bf.active_scrapers_flat.filter(s => s.date).map(s => s.date))].sort().reverse().slice(0, 10).map(d => (
                      <Tag key={d} color="gold" style={{ margin: 0, fontFamily: 'ui-monospace, Menlo, monospace', fontSize: 11 }}>{d}</Tag>
                    ))}
                  </Space>
                )}
              </Space>
            </Card>

            {/* ── Orchestrator status row ─────────────────────────── */}
            <Row gutter={[12, 12]} style={{ marginBottom: 12 }}>
              {[
                { k: 'streaming_orchestrator', label: '流式编排', note: '8 平台 streaming 总控', children: bf.children.streaming_scrapers },
                { k: 'by_date_orchestrator',   label: '日扫编排', note: 'alphapai report 日循环',  children: bf.children.date_sweep_scrapers },
                { k: 'oversea_summary_refill', label: 'oversea summary 补抓', note: `filled ${fmt(bf.oversea_summary_refill.filled ?? 0)} / empty ${fmt(bf.oversea_summary_refill.still_empty ?? 0)}` },
                { k: 'alphaengine_scheduled',  label: 'AlphaEngine 定时', note: 'CST 00:10 配额重置后自动' },
              ].map(row => {
                const alive = (bf.processes as Record<string, boolean>)[row.k]
                return (
                  <Col xs={24} sm={12} md={6} key={row.k}>
                    <Card
                      size="small"
                      style={{
                        borderLeft: `3px solid ${alive ? '#10b981' : '#94a3b8'}`,
                        height: '100%',
                      }}
                    >
                      <Space direction="vertical" size={4} style={{ width: '100%' }}>
                        <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                          <Space size={6}>
                            {alive ? (
                              <span className="ta-pulse-dot" style={{
                                width: 8, height: 8, borderRadius: '50%',
                                background: '#10b981', display: 'inline-block',
                                boxShadow: '0 0 0 0 #10b981',
                              }} />
                            ) : (
                              <CloseCircleFilled style={{ color: '#94a3b8', fontSize: 12 }} />
                            )}
                            <Text strong style={{ fontSize: 13 }}>{row.label}</Text>
                          </Space>
                          {row.children !== undefined && (
                            <Tag color={alive ? 'green' : 'default'} style={{ margin: 0, fontSize: 11 }}>
                              {row.children} 子进程
                            </Tag>
                          )}
                        </Space>
                        <Text type="secondary" style={{ fontSize: 11 }}>{row.note}</Text>
                      </Space>
                    </Card>
                  </Col>
                )
              })}
            </Row>

            {/* ── Per-platform coverage cards ─────────────────────── */}
            <Row gutter={[12, 12]} style={{ marginBottom: 16 }}>
              {Object.entries(grouped).map(([platform, tgts]) => {
                const accent = platformAccent(platform)
                const total = tgts.reduce((acc, r) => acc + (r.count ?? 0), 0)
                const coveredCount = tgts.filter(r => r.covered_6m).length
                return (
                  <Col xs={24} md={12} xl={8} key={platform}>
                    <Card
                      size="small"
                      style={{ borderLeft: `3px solid ${accent}`, height: '100%' }}
                      title={
                        <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                          <Text strong style={{ fontSize: 13 }}>
                            {PLATFORM_LABELS[platform] || platform}
                          </Text>
                          <Space size={6}>
                            <Tag
                              style={{
                                margin: 0,
                                background: `${accent}14`,
                                color: accent,
                                border: `1px solid ${accent}40`,
                                fontVariantNumeric: 'tabular-nums',
                              }}
                            >
                              {fmt(total)}
                            </Tag>
                            <Tag
                              color={coveredCount === tgts.length ? 'success' : 'default'}
                              style={{ margin: 0, fontSize: 11 }}
                            >
                              {coveredCount}/{tgts.length} 达标
                            </Tag>
                          </Space>
                        </Space>
                      }
                    >
                      <Space direction="vertical" size={10} style={{ width: '100%' }}>
                        {tgts.map(row => {
                          // Use CONTINUOUS days (从今天往回连续覆盖) as the
                          // real progress number. days_covered (absolute oldest)
                          // is kept for gap detection.
                          const contDays = row.continuous_days ?? 0
                          const totalDays = row.days_covered ?? 0
                          const gapDays = Math.max(0, totalDays - contDays)
                          // Scale bar against the 183-day (6mo) target.
                          const pct = Math.max(2, Math.min(100, (contDays / 183) * 100))
                          const barColor = contDays >= 180 ? '#10b981'
                            : contDays >= 90 ? '#f59e0b'
                            : contDays >= 30 ? '#fb923c'
                            : '#ef4444'
                          return (
                            <div key={`${row.category}_${JSON.stringify(row.filter ?? {})}`}>
                              <div style={{
                                display: 'flex',
                                justifyContent: 'space-between',
                                alignItems: 'center',
                                marginBottom: 4,
                                gap: 8,
                              }}>
                                <Space size={6}>
                                  <Text strong style={{ fontSize: 12 }}>{CategoryLabel(row)}</Text>
                                  {row.mode === 'date_sweep'
                                    ? <Tag color="gold" style={{ margin: 0, fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>日扫</Tag>
                                    : <Tag color="geekblue" style={{ margin: 0, fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>流式</Tag>}
                                  {row.backfill_complete && (
                                    <Tooltip title={`回填完毕 @ ${row.backfill_completed_at ?? '?'}${row.backfill_method ? ` · ${row.backfill_method}` : ''}`}>
                                      <Tag color="success" style={{ margin: 0, fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>
                                        ✓ 回填完毕
                                      </Tag>
                                    </Tooltip>
                                  )}
                                </Space>
                                <Space size={6}>
                                  <Text style={{ fontSize: 11, fontVariantNumeric: 'tabular-nums', color: '#475569' }}>
                                    {fmt(row.count)}
                                  </Text>
                                  {!row.backfill_complete && (
                                    <Tooltip title="从今天开始往回,不间断覆盖的数据天数(遇到第一个没有任何数据的日期即停)">
                                      <Tag style={{
                                        margin: 0, fontSize: 10, lineHeight: '16px', padding: '0 4px',
                                        background: `${barColor}20`, color: barColor, border: `1px solid ${barColor}40`,
                                      }}>
                                        连续 {contDays} 天
                                      </Tag>
                                    </Tooltip>
                                  )}
                                </Space>
                              </div>
                              {/* 回填完毕后不再展示进度条 + "已回填至 X" — 直接结案, 避免误导
                                  (6mo cutoff 对已全量的源不再有意义). 只保留错误徽标. */}
                              {!row.backfill_complete && (
                                <>
                                  <div style={{
                                    height: 6,
                                    background: 'rgba(148,163,184,0.2)',
                                    borderRadius: 3,
                                    overflow: 'hidden',
                                    position: 'relative',
                                  }}>
                                    <div style={{
                                      height: '100%',
                                      width: `${pct}%`,
                                      background: `linear-gradient(90deg, ${barColor}, ${barColor}cc)`,
                                      borderRadius: 3,
                                      transition: 'width 0.8s ease',
                                    }} />
                                  </div>
                                  <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 2 }}>
                                    <Text type="secondary" style={{ fontSize: 10, fontFamily: 'ui-monospace, Menlo, monospace' }}>
                                      已回填至 {fmtDateCST(row.continuous_oldest_ms ?? row.oldest_ms)}
                                    </Text>
                                    {row.error && (
                                      <Tooltip title={row.error}>
                                        <Tag color="error" style={{ margin: 0, fontSize: 10, lineHeight: '14px', padding: '0 4px' }}>err</Tag>
                                      </Tooltip>
                                    )}
                                  </div>
                                </>
                              )}
                              {row.backfill_complete && row.error && (
                                <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: 2 }}>
                                  <Tooltip title={row.error}>
                                    <Tag color="error" style={{ margin: 0, fontSize: 10, lineHeight: '14px', padding: '0 4px' }}>err</Tag>
                                  </Tooltip>
                                </div>
                              )}
                              {/* 正在爬的那几天 / 或流式状态 */}
                              {row.active_scrapers && row.active_scrapers.length > 0 && (
                                <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 3, flexWrap: 'wrap' }}>
                                  <span className="ta-pulse-dot" style={{
                                    width: 6, height: 6, borderRadius: '50%',
                                    background: '#22d3ee', color: '#22d3ee',
                                    display: 'inline-block',
                                  }} />
                                  <Text style={{ fontSize: 10, color: '#22d3ee', fontWeight: 600 }}>
                                    {row.active_scrapers.length} 正在爬:
                                  </Text>
                                  {row.active_scrapers.map(s => {
                                    const bg = s.mode === 'date_sweep' ? '#f59e0b20'
                                      : s.mode === 'bypass' ? '#ec489920'
                                      : '#60a5fa20'
                                    const fg = s.mode === 'date_sweep' ? '#d97706'
                                      : s.mode === 'bypass' ? '#be185d'
                                      : '#2563eb'
                                    const border = s.mode === 'date_sweep' ? '#f59e0b40'
                                      : s.mode === 'bypass' ? '#ec489940'
                                      : '#60a5fa40'
                                    const label = s.mode === 'bypass' ? 'bypass' : (s.date ?? 'stream')
                                    return (
                                      <Tag key={s.pid} style={{
                                        margin: 0, padding: '0 4px', fontSize: 9,
                                        lineHeight: '14px',
                                        fontFamily: 'ui-monospace, Menlo, monospace',
                                        background: bg, color: fg,
                                        border: `1px solid ${border}`,
                                      }}>{label}</Tag>
                                    )
                                  })}
                                </div>
                              )}
                            </div>
                          )
                        })}
                      </Space>
                    </Card>
                  </Col>
                )
              })}
            </Row>
          </>
        )
      })()}

      {/* PostgreSQL section */}
      <Divider orientation="left" style={{ marginTop: 24, marginBottom: 16 }}>
        <Space>
          <CloudServerOutlined style={{ color: '#2563eb' }} />
          <span style={{ fontWeight: 600 }}>{t('dbOverview.pgSection')}</span>
          <Tag color="blue" style={{ margin: 0 }}>
            {fmt(data.postgres.total)} {t('dbOverview.rows')}
          </Tag>
        </Space>
      </Divider>

      <Row gutter={[16, 16]}>
        {data.postgres.groups.map((g) => {
          const groupTotal = g.items.reduce((acc, it) => acc + (it.count ?? 0), 0)
          const accent = PG_GROUP_COLORS[g.group] || '#64748b'
          return (
            <Col xs={24} md={12} xl={8} key={g.group}>
              <Card
                size="small"
                style={{
                  borderLeft: `3px solid ${accent}`,
                  height: '100%',
                }}
                title={
                  <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                    <Text strong style={{ fontSize: 13 }}>{g.group}</Text>
                    <Tag
                      style={{
                        margin: 0,
                        background: `${accent}14`,
                        color: accent,
                        border: `1px solid ${accent}40`,
                        fontVariantNumeric: 'tabular-nums',
                      }}
                    >
                      {fmt(groupTotal)}
                    </Tag>
                  </Space>
                }
              >
                <Table
                  size="small"
                  rowKey="table"
                  dataSource={g.items}
                  columns={pgColumns}
                  pagination={false}
                  showHeader={false}
                />
              </Card>
            </Col>
          )
        })}
      </Row>

      {/* MongoDB section */}
      <Divider orientation="left" style={{ marginTop: 28, marginBottom: 16 }}>
        <Space>
          <FileSearchOutlined style={{ color: '#10b981' }} />
          <span style={{ fontWeight: 600 }}>{t('dbOverview.mongoSection')}</span>
          <Tag color="green" style={{ margin: 0 }}>
            {fmt(data.mongodb.total)} {t('dbOverview.docs')}
          </Tag>
        </Space>
      </Divider>

      <Row gutter={[16, 16]}>
        {data.mongodb.platforms.map((p) => {
          const platformTotal = p.items.reduce((acc, it) => acc + (it.count ?? 0), 0)
          const pkey = platformKeyOf(p.platform)
          const accent = PLATFORM_COLORS[pkey] || '#64748b'
          return (
            <Col xs={24} md={12} xl={8} key={p.platform}>
              <Card
                size="small"
                style={{
                  borderLeft: `3px solid ${accent}`,
                  height: '100%',
                }}
                title={
                  <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                    <Space direction="vertical" size={0}>
                      <Text strong style={{ fontSize: 13 }}>{p.platform}</Text>
                      <Text type="secondary" style={{ fontSize: 10, fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace' }}>
                        {p.database}
                      </Text>
                      {p.oldest_ms && p.newest_ms ? (
                        <>
                          <Text type="secondary" style={{ fontSize: 10, fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace' }}>
                            {fmtDateCST(p.oldest_ms)} → {fmtDateCST(p.newest_ms)}
                            {' '}
                            <span
                              style={{
                                color: (p.span_days ?? 0) >= 180 ? '#10b981'
                                     : (p.span_days ?? 0) >= 30  ? '#f59e0b'
                                                                  : '#ef4444',
                                fontWeight: 600,
                              }}
                            >
                              · {p.span_days ?? 0}d
                            </span>
                          </Text>
                          <Text type="secondary" style={{ fontSize: 10, fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace' }}>
                            <span
                              style={{
                                color: (p.coverage_pct ?? 100) >= 95 ? '#10b981'
                                     : (p.coverage_pct ?? 100) >= 80 ? '#f59e0b'
                                                                      : '#ef4444',
                                fontWeight: 600,
                              }}
                              title="日级覆盖率: 首尾跨度内至少有 1 条数据的天数 / 总天数"
                            >
                              cov {p.coverage_pct ?? 100}%
                            </span>
                            {p.max_gap_days && p.max_gap_days >= 7 ? (
                              <span
                                style={{ color: '#ef4444', fontWeight: 600, marginLeft: 6 }}
                                title="最大连续无数据天数 (某 collection 里)"
                              >
                                · gap {p.max_gap_days}d
                              </span>
                            ) : null}
                          </Text>
                        </>
                      ) : null}
                    </Space>
                    <Tag
                      style={{
                        margin: 0,
                        background: `${accent}14`,
                        color: accent,
                        border: `1px solid ${accent}40`,
                        fontVariantNumeric: 'tabular-nums',
                      }}
                    >
                      {fmt(platformTotal)}
                    </Tag>
                  </Space>
                }
              >
                {p.error ? (
                  <Alert
                    type="error"
                    showIcon
                    message={t('dbOverview.platformUnreachable')}
                    description={p.error}
                  />
                ) : p.items.length === 0 ? (
                  <Text type="secondary">{t('dbOverview.noCollections')}</Text>
                ) : (
                  <Table
                    size="small"
                    rowKey="collection"
                    dataSource={p.items}
                    columns={mongoColumns}
                    pagination={false}
                    showHeader={false}
                  />
                )}
              </Card>
            </Col>
          )
        })}
      </Row>
    </div>
  )
}
