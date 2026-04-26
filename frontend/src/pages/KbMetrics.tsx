/**
 * Admin observability dashboard for KB retrieval (kb_search / user_kb_search /
 * kb_fetch_document).
 *
 * Surfaces four reads from /api/admin/kb-metrics:
 *   1. Per-tool roll-up (calls / empty rate / p95 / p99)
 *   2. Time-series of calls + p95 latency for trend spotting
 *   3. Recent zero-hit queries (recall improvement input)
 *   4. Recent slowest queries (perf regression input)
 */
import { useEffect, useState } from 'react'
import {
  Card, Col, Row, Select, Statistic, Table, Tag, Space, Typography, Empty,
  Spin, Tabs, message,
} from 'antd'
import ReactECharts from 'echarts-for-react'
import dayjs from 'dayjs'
import api from '../services/api'

const { Title, Text } = Typography

interface ToolSummary {
  tool_name: string
  calls: number
  empty_result_rate: number
  error_rate: number
  avg_results: number
  p50_ms: number
  p95_ms: number
  p99_ms: number
}

interface TimeSeriesPoint {
  bucket: string
  calls: number
  p95_ms: number
}

interface EmptyRow {
  ts: string
  trace_id: string
  tool_name: string
  query: string
  ticker_count: number
  has_date_filter: boolean
  total_ms: number
}

interface SlowRow {
  ts: string
  trace_id: string
  tool_name: string
  query: string
  total_ms: number
  embed_ms: number
  milvus_ms: number
  mongo_ms: number
  result_count: number
  mode: string
}

const TOOL_COLORS: Record<string, string> = {
  kb_search: '#3b82f6',
  user_kb_search: '#10b981',
  kb_fetch_document: '#f59e0b',
}

export default function KbMetrics() {
  const [days, setDays] = useState(7)
  const [interval, setInterval] = useState<'15m' | '1h' | '6h' | '1d'>('1h')
  const [summary, setSummary] = useState<ToolSummary[]>([])
  const [series, setSeries] = useState<Record<string, TimeSeriesPoint[]>>({})
  const [emptyRows, setEmptyRows] = useState<EmptyRow[]>([])
  const [slowRows, setSlowRows] = useState<SlowRow[]>([])
  const [loading, setLoading] = useState(false)

  const load = async () => {
    setLoading(true)
    try {
      const [s, ts, eq, sq] = await Promise.all([
        api.get('/admin/kb-metrics/summary', { params: { days } }),
        api.get('/admin/kb-metrics/timeseries', { params: { days, interval } }),
        api.get('/admin/kb-metrics/empty-queries', { params: { days, limit: 50 } }),
        api.get('/admin/kb-metrics/slow-queries', { params: { days, limit: 50 } }),
      ])
      setSummary(s.data?.tools ?? [])
      setSeries(ts.data?.series ?? {})
      setEmptyRows(eq.data?.rows ?? [])
      setSlowRows(sq.data?.rows ?? [])
    } catch (e: any) {
      message.error(e?.response?.data?.detail || 'Failed to load KB metrics')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [days, interval])

  const totalCalls = summary.reduce((acc, t) => acc + t.calls, 0)
  const overallEmptyRate = totalCalls
    ? summary.reduce((acc, t) => acc + t.empty_result_rate * t.calls, 0) / totalCalls
    : 0
  const overallP95 = summary.length
    ? Math.round(
        summary.reduce((acc, t) => acc + t.p95_ms * t.calls, 0) /
          Math.max(totalCalls, 1),
      )
    : 0

  // Build echarts options from the per-tool series.
  const bucketsUnion = Array.from(
    new Set(
      Object.values(series).flatMap((arr) => arr.map((p) => p.bucket)),
    ),
  ).sort()
  const tools = Object.keys(series)

  const callsOption = {
    tooltip: { trigger: 'axis' as const },
    legend: { data: tools },
    grid: { left: 50, right: 20, top: 30, bottom: 50 },
    xAxis: {
      type: 'category' as const,
      data: bucketsUnion.map((b) => dayjs(b).format('MM-DD HH:mm')),
    },
    yAxis: { type: 'value' as const },
    series: tools.map((tool) => {
      const byBucket: Record<string, number> = {}
      for (const p of series[tool] || []) byBucket[p.bucket] = p.calls
      return {
        name: tool,
        type: 'line' as const,
        stack: 'total',
        areaStyle: {},
        emphasis: { focus: 'series' as const },
        data: bucketsUnion.map((b) => byBucket[b] ?? 0),
        color: TOOL_COLORS[tool] || '#888',
        smooth: true,
      }
    }),
  }

  const latencyOption = {
    tooltip: { trigger: 'axis' as const },
    legend: { data: tools },
    grid: { left: 50, right: 20, top: 30, bottom: 50 },
    xAxis: {
      type: 'category' as const,
      data: bucketsUnion.map((b) => dayjs(b).format('MM-DD HH:mm')),
    },
    yAxis: { type: 'value' as const, name: 'p95 ms' },
    series: tools.map((tool) => {
      const byBucket: Record<string, number> = {}
      for (const p of series[tool] || []) byBucket[p.bucket] = p.p95_ms
      return {
        name: tool,
        type: 'line' as const,
        data: bucketsUnion.map((b) => byBucket[b] ?? null),
        color: TOOL_COLORS[tool] || '#888',
        smooth: true,
      }
    }),
  }

  return (
    <div style={{ padding: 16 }}>
      <Space style={{ marginBottom: 16 }} wrap>
        <Title level={4} style={{ margin: 0 }}>
          知识库检索监控
        </Title>
        <Select
          value={days}
          onChange={setDays}
          style={{ width: 120 }}
          options={[
            { label: '最近 1 天', value: 1 },
            { label: '最近 7 天', value: 7 },
            { label: '最近 14 天', value: 14 },
            { label: '最近 30 天', value: 30 },
          ]}
        />
        <Select
          value={interval}
          onChange={(v) => setInterval(v as any)}
          style={{ width: 120 }}
          options={[
            { label: '15 分钟', value: '15m' },
            { label: '1 小时', value: '1h' },
            { label: '6 小时', value: '6h' },
            { label: '1 天', value: '1d' },
          ]}
        />
      </Space>

      <Spin spinning={loading}>
        <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
          <Col span={6}>
            <Card>
              <Statistic title="总调用次数" value={totalCalls} />
              <Text type="secondary">window: {days} day{days > 1 ? 's' : ''}</Text>
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic
                title="0 命中率（加权）"
                value={(overallEmptyRate * 100).toFixed(2)}
                suffix="%"
                valueStyle={{
                  color:
                    overallEmptyRate > 0.15 ? '#cf1322'
                    : overallEmptyRate > 0.05 ? '#d4b106' : '#3f8600',
                }}
              />
              <Text type="secondary">超 15% 表示召回需改进</Text>
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic title="P95 延迟（加权）" value={overallP95} suffix="ms" />
              <Text type="secondary">慢查询见下方表格</Text>
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic title="工具种类" value={summary.length} />
              <Text type="secondary">kb / user_kb / fetch</Text>
            </Card>
          </Col>
        </Row>

        <Card title="按工具拆分" style={{ marginBottom: 16 }}>
          <Table
            rowKey="tool_name"
            dataSource={summary}
            pagination={false}
            size="small"
            columns={[
              {
                title: '工具',
                dataIndex: 'tool_name',
                render: (v: string) => <Tag color={TOOL_COLORS[v] || 'default'}>{v}</Tag>,
              },
              { title: '调用数', dataIndex: 'calls', align: 'right' as const },
              {
                title: '0 命中率',
                dataIndex: 'empty_result_rate',
                align: 'right' as const,
                render: (v: number) => `${(v * 100).toFixed(2)}%`,
              },
              {
                title: '错误率',
                dataIndex: 'error_rate',
                align: 'right' as const,
                render: (v: number) => `${(v * 100).toFixed(2)}%`,
              },
              { title: '平均结果数', dataIndex: 'avg_results', align: 'right' as const },
              { title: 'p50 ms', dataIndex: 'p50_ms', align: 'right' as const },
              { title: 'p95 ms', dataIndex: 'p95_ms', align: 'right' as const },
              { title: 'p99 ms', dataIndex: 'p99_ms', align: 'right' as const },
            ]}
          />
        </Card>

        <Card title="调用量 / P95 延迟趋势" style={{ marginBottom: 16 }}>
          {bucketsUnion.length === 0 ? (
            <Empty description="window 内无数据" />
          ) : (
            <Row gutter={16}>
              <Col span={12}>
                <Title level={5}>调用数（堆叠）</Title>
                <ReactECharts option={callsOption} style={{ height: 240 }} />
              </Col>
              <Col span={12}>
                <Title level={5}>P95 延迟（ms）</Title>
                <ReactECharts option={latencyOption} style={{ height: 240 }} />
              </Col>
            </Row>
          )}
        </Card>

        <Tabs
          items={[
            {
              key: 'empty',
              label: `0 命中查询 (${emptyRows.length})`,
              children: (
                <Table
                  rowKey={(r) => `${r.ts}-${r.trace_id}`}
                  dataSource={emptyRows}
                  size="small"
                  pagination={{ pageSize: 20 }}
                  columns={[
                    {
                      title: '时间',
                      dataIndex: 'ts',
                      width: 160,
                      render: (v: string) => dayjs(v).format('MM-DD HH:mm:ss'),
                    },
                    {
                      title: '工具',
                      dataIndex: 'tool_name',
                      width: 140,
                      render: (v: string) => <Tag color={TOOL_COLORS[v] || 'default'}>{v}</Tag>,
                    },
                    { title: 'Query', dataIndex: 'query', ellipsis: true },
                    {
                      title: 'Ticker',
                      dataIndex: 'ticker_count',
                      width: 80,
                      align: 'right' as const,
                    },
                    {
                      title: '日期过滤',
                      dataIndex: 'has_date_filter',
                      width: 100,
                      render: (v: boolean) => (v ? <Tag color="blue">yes</Tag> : <Tag>no</Tag>),
                    },
                    { title: 'ms', dataIndex: 'total_ms', width: 80, align: 'right' as const },
                    {
                      title: 'trace',
                      dataIndex: 'trace_id',
                      width: 120,
                      render: (v: string) => (
                        <Text code style={{ fontSize: 11 }}>
                          {v ? v.slice(0, 12) : '-'}
                        </Text>
                      ),
                    },
                  ]}
                />
              ),
            },
            {
              key: 'slow',
              label: `慢查询 (${slowRows.length})`,
              children: (
                <Table
                  rowKey={(r) => `${r.ts}-${r.trace_id}`}
                  dataSource={slowRows}
                  size="small"
                  pagination={{ pageSize: 20 }}
                  columns={[
                    {
                      title: '时间',
                      dataIndex: 'ts',
                      width: 160,
                      render: (v: string) => dayjs(v).format('MM-DD HH:mm:ss'),
                    },
                    {
                      title: '工具',
                      dataIndex: 'tool_name',
                      width: 140,
                      render: (v: string) => <Tag color={TOOL_COLORS[v] || 'default'}>{v}</Tag>,
                    },
                    { title: 'Query', dataIndex: 'query', ellipsis: true },
                    {
                      title: '结果',
                      dataIndex: 'result_count',
                      width: 70,
                      align: 'right' as const,
                    },
                    { title: 'mode', dataIndex: 'mode', width: 100 },
                    {
                      title: 'embed ms',
                      dataIndex: 'embed_ms',
                      width: 90,
                      align: 'right' as const,
                    },
                    {
                      title: 'milvus ms',
                      dataIndex: 'milvus_ms',
                      width: 90,
                      align: 'right' as const,
                    },
                    {
                      title: 'mongo ms',
                      dataIndex: 'mongo_ms',
                      width: 90,
                      align: 'right' as const,
                    },
                    {
                      title: '总 ms',
                      dataIndex: 'total_ms',
                      width: 80,
                      align: 'right' as const,
                      sorter: (a: SlowRow, b: SlowRow) => a.total_ms - b.total_ms,
                    },
                  ]}
                />
              ),
            },
          ]}
        />
      </Spin>
    </div>
  )
}
