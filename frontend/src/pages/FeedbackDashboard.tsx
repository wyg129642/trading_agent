/**
 * Feedback Dashboard — lesson-learning loop observability.
 *
 * Top-trading-firm principle: a self-improving agent is only as good as the
 * feedback it ingests. This page shows:
 *   - Hallucination-rate trend over the last 8 weeks
 *   - Cell review-status distribution (pending/approved/flagged)
 *   - Feedback event counts by type (cell_edit / lesson_applied / ...)
 *   - Lesson pipeline: pending / approved / rejected / archived
 *   - Recent lesson impact (how many cells auto-apply touched)
 *   - A "run weekly review now" button for admins
 */
import { useEffect, useMemo, useState } from 'react'
import {
  Alert, Button, Card, Col, Row, Select, Space, Statistic, Table, Tag,
  Typography, message,
} from 'antd'
import { ReloadOutlined, PlayCircleOutlined } from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import { governanceApi } from '../services/modeling'
import { useAuthStore } from '../store/auth'

const { Title, Paragraph } = Typography

export default function FeedbackDashboard() {
  const user = useAuthStore(s => s.user)
  const isAdmin = user?.role === 'admin' || user?.role === 'boss'

  const [industry, setIndustry] = useState<string | null>(null)
  const [sinceDays, setSinceDays] = useState(30)
  const [data, setData] = useState<Awaited<ReturnType<typeof governanceApi.feedbackDashboard>> | null>(null)
  const [loading, setLoading] = useState(false)
  const [runningReview, setRunningReview] = useState(false)

  const reload = async () => {
    setLoading(true)
    try {
      const d = await governanceApi.feedbackDashboard({
        since_days: sinceDays,
        industry: industry || undefined,
      })
      setData(d)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { reload() }, [sinceDays, industry])

  const runWeeklyReview = async () => {
    setRunningReview(true)
    try {
      const result = await governanceApi.runReviewNow(7, true)
      const rate = (result as any)?.summary?.hallucination_rate ?? 0
      const paused = ((result as any)?.paused_models || []).length
      message.success(`Review 完成: 幻觉率 ${(rate * 100).toFixed(1)}% · 自动暂停 ${paused} 个模型`)
      await reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setRunningReview(false)
    }
  }

  const trendChart = useMemo(() => {
    const t = data?.hallucination_trend_weekly || []
    return {
      grid: { left: 50, right: 30, top: 10, bottom: 30 },
      tooltip: { trigger: 'axis' },
      xAxis: { type: 'category', data: t.map(r => r.week_start) },
      yAxis: { type: 'value', axisLabel: { formatter: '{value}%' } },
      series: [
        {
          name: '幻觉率',
          type: 'line',
          smooth: true,
          data: t.map(r => +(r.hallucination_rate * 100).toFixed(2)),
          itemStyle: { color: '#ef4444' },
          markLine: {
            symbol: 'none',
            lineStyle: { type: 'dashed', color: '#dc2626' },
            data: [{ yAxis: 15, label: { formatter: '红线 15%' } }],
          },
        },
        {
          name: '采样数',
          type: 'bar',
          yAxisIndex: 0,
          data: t.map(() => 0), // hidden; kept for hover alignment
          itemStyle: { color: 'rgba(100,116,139,0.2)' },
          showBackground: false,
        },
      ],
    }
  }, [data])

  const eventsChart = useMemo(() => {
    const entries = Object.entries(data?.events_by_type || {}).sort((a, b) => b[1] - a[1])
    return {
      grid: { left: 160, right: 30, top: 10, bottom: 30 },
      xAxis: { type: 'value' },
      yAxis: { type: 'category', data: entries.map(([k]) => k).reverse() },
      tooltip: { trigger: 'axis' },
      series: [{
        type: 'bar',
        data: entries.map(([, v]) => v).reverse(),
        itemStyle: { color: '#2563eb' },
      }],
    }
  }, [data])

  const latestRate = data?.hallucination_trend_weekly?.[data.hallucination_trend_weekly.length - 1]?.hallucination_rate ?? 0
  const prevRate = data?.hallucination_trend_weekly?.[data.hallucination_trend_weekly.length - 2]?.hallucination_rate ?? 0

  return (
    <div style={{ padding: 24, maxWidth: 1400, margin: '0 auto' }}>
      <div style={{ marginBottom: 16 }}>
        <Title level={3} style={{ marginBottom: 4 }}>🔄 反馈闭环仪表盘</Title>
        <Paragraph style={{ color: '#64748b', marginBottom: 0 }}>
          幻觉率趋势、反馈事件、lesson 管线、lesson 影响 — 回答 "agent 到底在变得更好吗？"
        </Paragraph>
      </div>

      {/* Controls */}
      <Card style={{ marginBottom: 16 }} size="small">
        <Space wrap size="middle">
          <Select
            value={sinceDays}
            onChange={setSinceDays}
            style={{ width: 140 }}
            options={[
              { value: 7, label: '最近 7 天' },
              { value: 30, label: '最近 30 天' },
              { value: 90, label: '最近 90 天' },
              { value: 180, label: '最近 180 天' },
            ]}
          />
          <Select
            value={industry}
            onChange={setIndustry}
            placeholder="所有行业"
            allowClear
            style={{ width: 200 }}
            options={[
              { value: 'optical_modules', label: '光模块' },
              { value: 'storage', label: '存储' },
              { value: 'semiconductor', label: '半导体' },
            ]}
          />
          <Button icon={<ReloadOutlined />} onClick={reload} loading={loading}>刷新</Button>
          {isAdmin && (
            <Button
              icon={<PlayCircleOutlined />}
              type="primary"
              ghost
              onClick={runWeeklyReview}
              loading={runningReview}
            >
              立即运行周度 Review
            </Button>
          )}
        </Space>
      </Card>

      {/* Summary cards */}
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Card size="small">
            <Statistic
              title="最新周幻觉率"
              value={latestRate * 100}
              suffix="%"
              precision={2}
              valueStyle={{ color: latestRate >= 0.15 ? '#dc2626' : latestRate >= 0.08 ? '#f59e0b' : '#16a34a' }}
            />
            <div style={{ fontSize: 11, color: '#64748b', marginTop: 4 }}>
              {prevRate > 0
                ? `周环比 ${((latestRate - prevRate) * 100).toFixed(2)} 个百分点`
                : '无上一周数据'}
            </div>
          </Card>
        </Col>
        <Col span={6}>
          <Card size="small">
            <Statistic title={`反馈事件 (${sinceDays}d)`} value={data?.total_events ?? 0} />
          </Card>
        </Col>
        <Col span={6}>
          <Card size="small">
            <Statistic title={`Lessons 产生 (${sinceDays}d)`} value={data?.total_lessons ?? 0} />
            <div style={{ fontSize: 11, color: '#64748b', marginTop: 4 }}>
              approved: {data?.lessons_by_status?.approved ?? 0} / pending: {data?.lessons_by_status?.pending ?? 0}
            </div>
          </Card>
        </Col>
        <Col span={6}>
          <Card size="small">
            <Statistic
              title="Flagged 单元格"
              value={data?.cells_by_review_status?.flagged ?? 0}
              valueStyle={{ color: (data?.cells_by_review_status?.flagged ?? 0) > 0 ? '#f59e0b' : undefined }}
            />
            <div style={{ fontSize: 11, color: '#64748b', marginTop: 4 }}>
              approved: {data?.cells_by_review_status?.approved ?? 0} / pending: {data?.cells_by_review_status?.pending ?? 0}
            </div>
          </Card>
        </Col>
      </Row>

      {/* Hallucination trend */}
      <Card size="small" title="幻觉率周趋势 (过去 8 周)" style={{ marginBottom: 16 }}>
        {data && data.hallucination_trend_weekly.every(r => r.total_sampled === 0) ? (
          <Alert
            type="info"
            message="暂无 citation-audit 采样数据"
            description="周度 citation audit 在每周一 08:00 自动触发, 或由管理员用上方按钮手动触发."
            showIcon
          />
        ) : (
          <ReactECharts option={trendChart} style={{ height: 300 }} />
        )}
      </Card>

      {/* Events + lesson impact */}
      <Row gutter={16}>
        <Col span={14}>
          <Card size="small" title="反馈事件按类型" style={{ marginBottom: 16 }}>
            {Object.keys(data?.events_by_type || {}).length === 0 ? (
              <Alert type="info" message="窗口内无反馈事件" />
            ) : (
              <ReactECharts option={eventsChart} style={{ height: 340 }} />
            )}
          </Card>
        </Col>
        <Col span={10}>
          <Card size="small" title="近期已批准 Lesson 影响" style={{ marginBottom: 16 }}>
            {(data?.recent_lesson_impact || []).length === 0 ? (
              <Alert type="info" message="窗口内无已批准 lesson" />
            ) : (
              <Table
                dataSource={data!.recent_lesson_impact}
                rowKey="lesson_id"
                size="small"
                pagination={false}
                columns={[
                  { title: 'Lesson', dataIndex: 'title', ellipsis: true },
                  {
                    title: '影响 cells',
                    dataIndex: 'cells_touched_by_auto_apply',
                    align: 'right' as const,
                    width: 110,
                    render: (v: number) => <Tag color={v > 0 ? 'green' : 'default'}>{v}</Tag>,
                  },
                ]}
              />
            )}
          </Card>
        </Col>
      </Row>

      {/* Lesson pipeline status */}
      <Card size="small" title="Lesson 管线状态">
        <Space wrap size="middle">
          {Object.entries(data?.lessons_by_status || {}).map(([status, n]) => (
            <Tag
              key={status}
              color={
                status === 'approved' ? 'green'
                  : status === 'pending' ? 'orange'
                    : status === 'rejected' ? 'red'
                      : 'default'
              }
              style={{ fontSize: 14, padding: '4px 10px' }}
            >
              {status}: {n}
            </Tag>
          ))}
          {Object.keys(data?.lessons_by_status || {}).length === 0 && (
            <span style={{ color: '#64748b' }}>窗口内无 lesson 数据</span>
          )}
        </Space>
      </Card>
    </div>
  )
}
