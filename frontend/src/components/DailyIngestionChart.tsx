import { useEffect, useState } from 'react'
import { Button, Card, Empty, Space, Spin, Typography } from 'antd'
import ReactECharts from 'echarts-for-react'
import api from '../services/api'

interface DailyIngestion {
  tz: string
  dates: string[]
  series: Record<string, number[]>
  series_realtime?: Record<string, number[]>
  series_backfill?: Record<string, number[]>
  totals_today: Record<string, number>
  totals_today_realtime?: Record<string, number>
  totals_today_backfill?: Record<string, number>
}

// Three display modes for the stacked chart:
//   'all'      — total (realtime + backfill), current default behavior
//   'realtime' — only docs crawled within 24h of platform publish time
//   'backfill' — only docs older than 24h (catchup / stream-backfill)
export type IngestionMode = 'all' | 'realtime' | 'backfill'

interface PlatformName {
  key: string
  display_name: string
}

// Platform color palette — mirrors the one in DataSources.tsx so the same
// platform always shows the same accent color across the app.
export const PLATFORM_COLORS: Record<string, string> = {
  alphapai: '#6366f1',
  jinmen: '#10b981',
  meritco: '#f59e0b',
  thirdbridge: '#ef4444',
  funda: '#06b6d4',
  gangtise: '#ec4899',
  acecamp: '#8b5cf6',
  alphaengine: '#0ea5e9',
  sentimentrader: '#64748b',
}

const PLATFORM_DISPLAY_FALLBACK: Record<string, string> = {
  alphapai: 'AlphaPai (派派)',
  jinmen: 'Jinmen (进门财经)',
  meritco: 'Meritco (久谦)',
  thirdbridge: 'Third Bridge (高临)',
  funda: 'Funda',
  gangtise: 'Gangtise (岗底斯)',
  acecamp: 'AceCamp',
  alphaengine: 'AlphaEngine',
  sentimentrader: 'SentimenTrader',
}

interface Props {
  /** Optional pre-fetched platform name map; component fetches its own if omitted. */
  platforms?: PlatformName[]
  /** Override the default [7, 14, 30] choices. */
  choices?: number[]
  /** Override the initial selected day count. */
  defaultDays?: number
  /** Which slice of docs to chart. Defaults to 'all' (realtime + backfill). */
  mode?: IngestionMode
  /** Custom card title override. */
  title?: string
  /** Hide the day-range selector (useful when caller renders 2 charts side-by-side sharing days). */
  hideDaysSelector?: boolean
}

export default function DailyIngestionChart({
  platforms: platformsProp,
  choices = [7, 14, 30],
  defaultDays = 14,
  mode = 'all',
  title,
  hideDaysSelector = false,
}: Props) {
  const [data, setData] = useState<DailyIngestion | null>(null)
  const [days, setDays] = useState<number>(defaultDays)
  const [loading, setLoading] = useState(false)
  const [platforms, setPlatforms] = useState<PlatformName[]>(platformsProp || [])

  // Self-fetch platform names if caller didn't pass them in. Keeps the
  // component drop-in-usable on any page.
  useEffect(() => {
    if (platformsProp && platformsProp.length > 0) {
      setPlatforms(platformsProp)
      return
    }
    let cancelled = false
    api
      .get<PlatformName[]>('/data-sources')
      .then((res) => {
        if (!cancelled) setPlatforms(res.data)
      })
      .catch(() => {
        /* fall back to static labels via PLATFORM_DISPLAY_FALLBACK */
      })
    return () => {
      cancelled = true
    }
  }, [platformsProp])

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    api
      .get<DailyIngestion>(`/data-sources/ingestion-daily?days=${days}`)
      .then((res) => {
        if (!cancelled) setData(res.data)
      })
      .catch(() => {
        if (!cancelled) setData(null)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [days])

  const nameByKey = new Map(platforms.map((p) => [p.key, p.display_name]))
  const labelFor = (k: string) => nameByKey.get(k) || PLATFORM_DISPLAY_FALLBACK[k] || k

  // Select which series set to render based on mode. Fall back to total
  // (backward-compat for servers that don't yet emit the split fields).
  const selectedSeries: Record<string, number[]> =
    mode === 'realtime'
      ? data?.series_realtime ?? data?.series ?? {}
      : mode === 'backfill'
      ? data?.series_backfill ?? {}
      : data?.series ?? {}
  const selectedTotalsToday: Record<string, number> =
    mode === 'realtime'
      ? data?.totals_today_realtime ?? data?.totals_today ?? {}
      : mode === 'backfill'
      ? data?.totals_today_backfill ?? {}
      : data?.totals_today ?? {}

  const option = (() => {
    if (!data) return {}
    const activeKeys = Object.keys(selectedSeries).filter(
      (k) => (selectedSeries[k] || []).some((v) => v > 0),
    )
    const ordered = activeKeys.sort((a, b) => {
      const sa = (selectedSeries[a] || []).reduce((x, y) => x + y, 0)
      const sb = (selectedSeries[b] || []).reduce((x, y) => x + y, 0)
      return sb - sa
    })
    return {
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow' },
        valueFormatter: (v: any) => (typeof v === 'number' ? v.toLocaleString() : v),
      },
      legend: {
        data: ordered.map(labelFor),
        top: 0,
        textStyle: { fontSize: 12 },
      },
      grid: { left: 40, right: 20, top: 40, bottom: 40 },
      xAxis: {
        type: 'category',
        data: data.dates.map((d) => d.slice(5)),
        axisLabel: { fontSize: 11 },
      },
      yAxis: {
        type: 'value',
        axisLabel: {
          formatter: (v: number) => (v >= 1000 ? `${v / 1000}k` : String(v)),
        },
      },
      series: ordered.map((k) => ({
        name: labelFor(k),
        type: 'bar',
        stack: 'total',
        itemStyle: {
          color: PLATFORM_COLORS[k] || '#64748b',
          borderRadius: [2, 2, 0, 0],
          // Backfill mode draws with a diagonal pattern to visually distinguish
          // from realtime when the two charts sit side-by-side.
          ...(mode === 'backfill' ? {
            decal: { symbol: 'rect', symbolSize: 0.7, dashArrayX: [1, 0], dashArrayY: [3, 3], rotation: -Math.PI / 4 },
            opacity: 0.88,
          } : {}),
        },
        emphasis: { focus: 'series' },
        data: selectedSeries[k],
      })),
    }
  })()

  const todayTotal = Object.values(selectedTotalsToday).reduce((a, b) => a + b, 0)
  const resolvedTitle =
    title ??
    (mode === 'realtime' ? '实时入库量' : mode === 'backfill' ? '回填入库量' : '每日入库量')
  const totalBadgeColor = mode === 'backfill' ? '#f59e0b' : '#10b981'

  return (
    <Card
      size="small"
      title={
        <Space>
          <span style={{ fontSize: 14, fontWeight: 600 }}>{resolvedTitle}</span>
          <Typography.Text type="secondary" style={{ fontSize: 12, fontWeight: 'normal' }}>
            CST · 最近 {days} 天
          </Typography.Text>
        </Space>
      }
      extra={
        <Space>
          {data && (
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              今日合计{' '}
              <strong style={{ color: totalBadgeColor }}>{todayTotal.toLocaleString()}</strong> 条
            </Typography.Text>
          )}
          {!hideDaysSelector && (
            <Button.Group size="small">
              {choices.map((d) => (
                <Button
                  key={d}
                  type={days === d ? 'primary' : 'default'}
                  onClick={() => setDays(d)}
                >
                  {d}天
                </Button>
              ))}
            </Button.Group>
          )}
        </Space>
      }
    >
      {loading && !data ? (
        <div
          style={{
            height: 260,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          <Spin />
        </div>
      ) : data ? (
        <ReactECharts option={option} style={{ height: 260 }} notMerge lazyUpdate />
      ) : (
        <Empty description="暂无数据" style={{ padding: 40 }} />
      )}
    </Card>
  )
}
