import { useEffect, useMemo, useState } from 'react'
import { Card, Row, Col, Tag, Typography, Spin, Tooltip, Space, Image } from 'antd'
import { LineChartOutlined, LinkOutlined, ClockCircleOutlined, InfoCircleOutlined, CheckCircleFilled, WarningFilled } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import api from '../services/api'
import { useAuthStore } from '../store/auth'

const { Text } = Typography

// ── Types ─────────────────────────────────────────────────────────────

interface SparklinePoint { t: string | null; v: number }

interface Indicator {
  slug: string
  name: string
  indicator_name?: string
  chart_title?: string
  latest_value: number
  latest_date: string | null
  benchmark: { name?: string; value?: number | null }
  image_url: string | null
  sparkline: SparklinePoint[]
  benchmark_sparkline: SparklinePoint[]
  secondary?: {
    name: string
    latest_value: number
    sparkline: SparklinePoint[]
  } | null
  source_url: string
  updated_at: string | null
}

interface IndicatorsResponse {
  indicators: Indicator[]
  source: string
  source_url: string
  updated_at: string | null
}

// ── Zone interpretation per indicator ────────────────────────────────

type Zone = { label: string; labelZh: string; color: string; tagColor: string }

const NEUTRAL: Zone = { label: 'Neutral', labelZh: '中性', color: '#8c8c8c', tagColor: 'default' }

// Fear & Greed + QQQ Optix both use a 0-100 "sentiment" scale where high = Greed/Optimism (contrarian bearish)
function zoneForFearGreed(v: number): Zone {
  if (v <= 25) return { label: 'Extreme Fear', labelZh: '极度恐慌', color: '#389e0d', tagColor: 'green' }
  if (v <= 45) return { label: 'Fear',         labelZh: '恐慌',     color: '#52c41a', tagColor: 'lime' }
  if (v <= 55) return NEUTRAL
  if (v <= 75) return { label: 'Greed',        labelZh: '贪婪',     color: '#fa8c16', tagColor: 'orange' }
  return               { label: 'Extreme Greed',labelZh: '极度贪婪', color: '#cf1322', tagColor: 'red' }
}

// QQQ Optix — sentimentrader's own published thresholds: <30 pessimism, >70 optimism.
// We surface a finer 30/50/70 split for visual clarity; the "extreme" labels
// below/above 15 and 85 match the site's extreme-reading annotations.
function zoneForOptix(v: number): Zone {
  if (v <  15) return { label: 'Extreme Pessimism', labelZh: '极度悲观', color: '#389e0d', tagColor: 'green' }
  if (v <= 30) return { label: 'Pessimism',         labelZh: '悲观',     color: '#52c41a', tagColor: 'lime' }
  if (v <  70) return NEUTRAL
  if (v <  85) return { label: 'Optimism',          labelZh: '乐观',     color: '#fa8c16', tagColor: 'orange' }
  return               { label: 'Extreme Optimism', labelZh: '极度乐观', color: '#cf1322', tagColor: 'red' }
}

// Smart/Dumb spread roughly -1 .. +1. sentimentrader doesn't publish fixed
// buckets — these are a coarse heuristic: positive spread ≈ smart money more
// bullish than retail (contrarian bullish), negative ≈ retail more bullish
// than smart money (contrarian bearish). Treat the labels as a visual cue,
// not a signal.
function zoneForSmartDumb(v: number): Zone {
  if (v >= 0.3)  return { label: 'Smart Money Bullish', labelZh: '聪明钱偏多', color: '#389e0d', tagColor: 'green' }
  if (v >= 0.1)  return { label: 'Mildly Bullish',      labelZh: '温和偏多',   color: '#52c41a', tagColor: 'lime' }
  if (v >  -0.1) return NEUTRAL
  if (v >  -0.3) return { label: 'Mildly Bearish',      labelZh: '温和偏空',   color: '#fa8c16', tagColor: 'orange' }
  return                { label: 'Dumb Money Bullish',  labelZh: '散户偏多',   color: '#cf1322', tagColor: 'red' }
}

function zoneFor(slug: string, value: number): Zone {
  if (slug === 'cnn_fear_greed') return zoneForFearGreed(value)
  if (slug === 'etf_qqq')        return zoneForOptix(value)
  if (slug === 'smart_dumb_spread') return zoneForSmartDumb(value)
  // smart_dumb has two components; no single-value zone label on its own card.
  return NEUTRAL
}

// Short human-readable title for the indicator card header
function headlineFor(slug: string, zh: boolean): string {
  if (slug === 'smart_dumb_spread') return zh ? '聪明钱 vs 散户（信心差）' : 'Smart vs Dumb Money Spread'
  if (slug === 'smart_dumb')        return zh ? '聪明钱 & 散户（信心值）' : 'Smart & Dumb Money Confidence'
  if (slug === 'cnn_fear_greed')    return zh ? '恐慌 & 贪婪模型' : 'Fear & Greed Model'
  if (slug === 'etf_qqq')           return zh ? 'QQQ 乐观指数' : 'QQQ Optix'
  return slug
}

// Per-indicator hover note explaining where the zone thresholds come from so
// users know which labels are authoritative vs heuristic.
function thresholdSourceFor(slug: string, zh: boolean): string {
  if (slug === 'cnn_fear_greed') {
    return zh
      ? '区间 0/25/45/55/75/100 为 CNN Fear & Greed 官方分档'
      : 'Zones 0/25/45/55/75/100 match the CNN Fear & Greed official bands'
  }
  if (slug === 'etf_qqq') {
    return zh
      ? '区间 15/30/70/85 基于 sentimentrader 发布的极值线（红虚线 ~70 以上 = 乐观，绿虚线 ~30 以下 = 悲观）'
      : 'Zones at 15/30/70/85 follow sentimentrader\'s published extreme lines'
  }
  if (slug === 'smart_dumb_spread') {
    return zh
      ? 'sentimentrader 未公布固定档位；±0.1 / ±0.3 为主观近似，仅作视觉提示'
      : 'sentimentrader publishes no fixed bucket — labels are a rough heuristic'
  }
  if (slug === 'smart_dumb') {
    return zh
      ? '两条原始信心值（0–1）。官方以 0.3 / 0.7 为极值线（图中红黑虚线），下卡片展示两者之差（更可操作的信号）'
      : 'Two raw confidence values (0–1). The 0.3 / 0.7 lines on the chart are sentimentrader\'s extreme markers'
  }
  return ''
}

// ── Crawl freshness ───────────────────────────────────────────────────
// Scraper runs daily. `updated_at` is written by the scraper ONLY on a
// successful scrape (crawl/sentimentrader/scraper.py:_upsert_indicator), so
// we can read it as "time since last success". >28h since that success means
// today's run never landed — flag red. This lets users spot a broken scrape
// at a glance without tailing backend logs.
function crawlFreshness(iso: string | null, langZh: boolean): {
  label: string            // primary text, e.g. "6 小时前成功爬取"
  tooltip: string          // hover, absolute timestamp
  stale: boolean           // true ⇒ needs attention (> 28h since last success)
  never: boolean           // true ⇒ no successful scrape has ever been recorded
} {
  if (!iso) {
    return {
      label: langZh ? '尚无成功爬取' : 'no successful crawl yet',
      tooltip: langZh ? '从未成功爬取过该指标' : 'never successfully scraped',
      stale: true,
      never: true,
    }
  }
  const ts = new Date(iso).getTime()
  if (!Number.isFinite(ts)) {
    return { label: iso, tooltip: iso, stale: false, never: false }
  }
  const diffMs = Math.max(0, Date.now() - ts)
  const mins = Math.floor(diffMs / 60000)
  const hrs = Math.floor(mins / 60)
  const days = Math.floor(hrs / 24)
  let rel: string
  if (mins < 1) rel = langZh ? '刚刚' : 'just now'
  else if (mins < 60) rel = langZh ? `${mins} 分钟前` : `${mins}m ago`
  else if (hrs < 24) rel = langZh ? `${hrs} 小时前` : `${hrs}h ago`
  else rel = langZh ? `${days} 天前` : `${days}d ago`
  const stale = hrs >= 28
  const label = stale
    ? (langZh ? `最近一次成功 ${rel} · 疑似失败` : `last success ${rel} · likely failing`)
    : (langZh ? `${rel}成功爬取` : `crawled successfully ${rel}`)
  const absolute = new Date(ts).toLocaleString(langZh ? 'zh-CN' : 'en-US', {
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit',
  })
  const tooltip = langZh
    ? `上一次成功爬取：${absolute}`
    : `Last successful crawl: ${absolute}`
  return { label, tooltip, stale, never: false }
}

// ── Sparkline (SVG) ───────────────────────────────────────────────────

function Sparkline({ points, color }: { points: SparklinePoint[]; color: string }) {
  const W = 180
  const H = 42
  const PAD = 2

  const path = useMemo(() => {
    if (!points.length) return ''
    const vals = points.map(p => p.v).filter(v => Number.isFinite(v))
    if (!vals.length) return ''
    const min = Math.min(...vals)
    const max = Math.max(...vals)
    const span = max - min || 1
    const step = (W - PAD * 2) / Math.max(points.length - 1, 1)
    const ys = vals.map(v => H - PAD - ((v - min) / span) * (H - PAD * 2))
    const xs = vals.map((_, i) => PAD + i * step)
    return xs.map((x, i) => `${i === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${ys[i].toFixed(1)}`).join(' ')
  }, [points])

  const last = points[points.length - 1]
  const first = points[0]
  const trendUp = last && first && last.v >= first.v

  return (
    <svg width={W} height={H} style={{ display: 'block' }}>
      <path d={path} fill="none" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
      {last && (
        <circle
          cx={W - PAD}
          cy={(() => {
            const vals = points.map(p => p.v)
            const min = Math.min(...vals), max = Math.max(...vals), span = max - min || 1
            return H - PAD - ((last.v - min) / span) * (H - PAD * 2)
          })()}
          r={2.5}
          fill={color}
        />
      )}
      {/* subtle baseline */}
      <line x1={0} y1={H - 1} x2={W} y2={H - 1} stroke="#f0f0f0" strokeWidth="1" />
    </svg>
  )
}

// Module-level blob URL cache, keyed by image_url. Cards in this dashboard
// remount on every navigation back to the workbench; without this every
// remount fired a fresh authenticated XHR for each chart even when the
// backend's HTTP Cache-Control would have happily returned 304. axios with
// responseType:'blob' + Bearer Authorization tends to bypass the disk HTTP
// cache in Chromium. Stash the object URL once per session.
const _imageBlobCache = new Map<string, string>()
const _imageInflight = new Map<string, Promise<string>>()

async function fetchChartBlobUrl(imageUrl: string): Promise<string> {
  const cached = _imageBlobCache.get(imageUrl)
  if (cached) return cached
  const existing = _imageInflight.get(imageUrl)
  if (existing) return existing
  const p = api.get(imageUrl, { responseType: 'blob' })
    .then((r) => {
      const url = URL.createObjectURL(r.data)
      _imageBlobCache.set(imageUrl, url)
      return url
    })
    .finally(() => { _imageInflight.delete(imageUrl) })
  _imageInflight.set(imageUrl, p)
  return p
}

// ── One card ──────────────────────────────────────────────────────────

function IndicatorCard({ ind, langZh, token }: { ind: Indicator; langZh: boolean; token: string | null }) {
  const isDual = !!ind.secondary
  const zone = isDual ? NEUTRAL : zoneFor(ind.slug, ind.latest_value)
  const headline = headlineFor(ind.slug, langZh)
  const thresholdNote = thresholdSourceFor(ind.slug, langZh)
  const freshness = crawlFreshness(ind.updated_at, langZh)

  const decimals = (ind.slug === 'smart_dumb_spread' || ind.slug === 'smart_dumb') ? 2 : 1
  const fmt = (v: number | null | undefined) => (
    v != null && Number.isFinite(v) ? Number(v).toFixed(decimals) : '—'
  )
  const valueDisplay = fmt(ind.latest_value)
  const secondaryDisplay = ind.secondary ? fmt(ind.secondary.latest_value) : null
  const primaryLabelShort = ind.slug === 'smart_dumb' ? (langZh ? '聪明钱' : 'Smart') : null
  const secondaryLabelShort = ind.slug === 'smart_dumb' ? (langZh ? '散户' : 'Dumb') : null

  const bench = ind.benchmark
  const benchLabel = bench?.name && bench?.value != null
    ? `${bench.name} ${Number(bench.value).toLocaleString(undefined, { maximumFractionDigits: 2 })}`
    : null

  // The chart endpoint is auth-gated; we fetch as a blob with the bearer token
  // and render via an object URL. Falls back to the lightweight sparkline if
  // the image hasn't been captured yet (older doc, or scraper pre-screenshot).
  // Module-level cache (see fetchChartBlobUrl) keeps the blob URL alive across
  // remounts so the workbench's 4 cards don't refetch on every navigation.
  const [imageBlobUrl, setImageBlobUrl] = useState<string | null>(
    ind.image_url ? _imageBlobCache.get(ind.image_url) ?? null : null,
  )
  const [imageErr, setImageErr] = useState(false)
  useEffect(() => {
    if (!ind.image_url || !token) return
    if (_imageBlobCache.has(ind.image_url)) {
      setImageBlobUrl(_imageBlobCache.get(ind.image_url)!)
      return
    }
    let cancelled = false
    fetchChartBlobUrl(ind.image_url)
      .then((url) => { if (!cancelled) setImageBlobUrl(url) })
      .catch(() => { if (!cancelled) setImageErr(true) })
    return () => { cancelled = true }
    // Intentionally do NOT revoke the object URL on unmount — it's shared via
    // the module-level cache and other mounts in this session reuse it.
  }, [ind.image_url, token])

  return (
    <Card
      size="small"
      bodyStyle={{ padding: '12px 14px' }}
      hoverable
      style={{ height: '100%' }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 8 }}>
        <Space size={6} align="center">
          <LineChartOutlined style={{ color: zone.color, fontSize: 14 }} />
          <Text strong style={{ fontSize: 13 }}>{headline}</Text>
          {thresholdNote && (
            <Tooltip title={thresholdNote}>
              <InfoCircleOutlined style={{ color: '#bfbfbf', fontSize: 12, cursor: 'help' }} />
            </Tooltip>
          )}
        </Space>
        <Tooltip title={langZh ? '点击打开官网原图（需已登录 sentimentrader）' : 'Open the live chart on sentimentrader.com'}>
          <Tag
            color="default"
            style={{ fontSize: 10, margin: 0, cursor: 'pointer' }}
            onClick={(e) => { e.stopPropagation(); window.open(ind.source_url, '_blank', 'noopener,noreferrer') }}
          >
            SentimenTrader <LinkOutlined style={{ fontSize: 10 }} />
          </Tag>
        </Tooltip>
      </div>

      <div style={{ display: 'flex', alignItems: 'baseline', gap: 10, marginTop: 6, flexWrap: 'wrap' }}>
        {isDual ? (
          // Dual-line chart — show both values side by side, no zone tag.
          <>
            <Space size={4} align="baseline">
              {primaryLabelShort && <Text type="secondary" style={{ fontSize: 11 }}>{primaryLabelShort}</Text>}
              <Text style={{ fontSize: 22, fontWeight: 600, color: '#1677ff', lineHeight: 1 }}>
                {valueDisplay}
              </Text>
            </Space>
            <Space size={4} align="baseline">
              {secondaryLabelShort && <Text type="secondary" style={{ fontSize: 11 }}>{secondaryLabelShort}</Text>}
              <Text style={{ fontSize: 22, fontWeight: 600, color: '#cf1322', lineHeight: 1 }}>
                {secondaryDisplay}
              </Text>
            </Space>
          </>
        ) : (
          <>
            <Text style={{ fontSize: 26, fontWeight: 600, color: zone.color, lineHeight: 1 }}>
              {valueDisplay}
            </Text>
            <Tag color={zone.tagColor} style={{ margin: 0 }}>
              {langZh ? zone.labelZh : zone.label}
            </Tag>
          </>
        )}
      </div>

      <div style={{ marginTop: 8, minHeight: 120, display: 'flex', alignItems: 'center', justifyContent: 'center', background: '#fafafa', borderRadius: 4 }}>
        {ind.image_url && !imageErr ? (
          imageBlobUrl ? (
            <Image
              src={imageBlobUrl}
              alt={headline}
              preview={{ mask: langZh ? '点击放大' : 'Click to enlarge' }}
              style={{ maxWidth: '100%', display: 'block' }}
            />
          ) : (
            <Spin size="small" />
          )
        ) : (
          // Fallback when screenshot isn't available (first-ever run, image deleted, etc.)
          <Sparkline points={ind.sparkline} color={zone.color} />
        )}
      </div>

      <div style={{ marginTop: 6, color: '#8c8c8c', fontSize: 11 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 6 }}>
          <Tooltip title={langZh ? '数据最新日期（来源站上的最后一个数据点）' : 'Latest data point date on source'}>
            <Space size={4} style={{ cursor: 'help' }}>
              <ClockCircleOutlined />
              <span>{langZh ? '数据' : 'Data'} {ind.latest_date || '—'}</span>
            </Space>
          </Tooltip>
          {benchLabel && <span>{benchLabel}</span>}
        </div>
        <Tooltip title={freshness.tooltip}>
          <Space
            size={4}
            style={{
              marginTop: 2,
              cursor: 'help',
              color: freshness.stale ? '#cf1322' : '#389e0d',
              fontWeight: freshness.stale ? 500 : undefined,
            }}
          >
            {freshness.stale
              ? <WarningFilled style={{ color: '#cf1322' }} />
              : <CheckCircleFilled style={{ color: '#52c41a' }} />}
            <span>{freshness.label}</span>
          </Space>
        </Tooltip>
      </div>
    </Card>
  )
}

// ── Strip ─────────────────────────────────────────────────────────────

// Module-level snapshot of the last-loaded indicators payload so navigating
// away from the workbench and back doesn't re-show a spinner for fresh data
// the user just saw. Backend caches its response 60s; we treat anything within
// 5 min as fresh-enough to skip the loading state. Sentimentrader updates once
// daily so this is conservative.
const _INDICATORS_FRESH_MS = 5 * 60_000
let _indicatorsSnapshot: { ts: number; data: IndicatorsResponse } | null = null

export default function SentimentTraderCards() {
  const seeded = _indicatorsSnapshot && (Date.now() - _indicatorsSnapshot.ts < _INDICATORS_FRESH_MS)
    ? _indicatorsSnapshot.data
    : null
  const [data, setData] = useState<IndicatorsResponse | null>(seeded)
  const [loading, setLoading] = useState(seeded == null)
  const [err, setErr] = useState<string | null>(null)
  const { i18n } = useTranslation()
  const langZh = (i18n.language || 'zh').startsWith('zh')
  const token = useAuthStore((s) => s.token)

  useEffect(() => {
    let cancelled = false
    api.get<IndicatorsResponse>('/sentimentrader/indicators')
      .then(r => {
        if (cancelled) return
        _indicatorsSnapshot = { ts: Date.now(), data: r.data }
        setData(r.data)
      })
      .catch(e => { if (!cancelled && !seeded) setErr(e?.response?.data?.detail || e?.message || 'error') })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  if (loading) {
    return (
      <div style={{ marginBottom: 16, textAlign: 'center', padding: 20 }}>
        <Spin size="small" />
      </div>
    )
  }

  // Silent fail — if sentimentrader data isn't there yet (scraper hasn't run),
  // don't render a broken strip; just hide. Surfaced in backend logs.
  if (err || !data || !data.indicators.length) {
    return null
  }

  return (
    <div style={{ marginBottom: 20 }}>
      <Row gutter={[12, 12]}>
        {data.indicators.map((ind) => (
          // 4 cards: full width on mobile, 2×2 on tablet+desktop, 4-across on xl.
          <Col xs={24} sm={12} lg={12} xl={6} key={ind.slug}>
            <IndicatorCard ind={ind} langZh={langZh} token={token} />
          </Col>
        ))}
      </Row>
    </div>
  )
}
