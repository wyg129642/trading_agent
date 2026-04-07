import { useEffect, useState } from 'react'
import { Card, Col, Row, Table, Statistic, Select, Typography, Spin } from 'antd'
import { DollarOutlined, ApiOutlined, ThunderboltOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import ReactECharts from 'echarts-for-react'
import api from '../services/api'

const { Text } = Typography

interface TokenStats {
  total_calls: number
  total_prompt_tokens: number
  total_completion_tokens: number
  total_tokens: number
  total_cost_cny: number
  by_stage: Record<string, any>
  by_model: Record<string, any>
  daily_trend: Array<{ date: string; calls: number; cost_cny: number }>
}

interface PipelineStats {
  total_processed: number
  pass_rate_phase1: number
}

export default function Analytics() {
  const { t } = useTranslation()
  const [tokenStats, setTokenStats] = useState<TokenStats | null>(null)
  const [pipelineStats, setPipelineStats] = useState<PipelineStats | null>(null)
  const [days, setDays] = useState(7)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    Promise.all([
      api.get(`/analytics/token-usage?days=${days}`),
      api.get('/analytics/pipeline'),
    ])
      .then(([tokenRes, pipelineRes]) => {
        setTokenStats(tokenRes.data)
        setPipelineStats(pipelineRes.data)
      })
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [days])

  if (loading) {
    return (
      <div style={{ textAlign: 'center', padding: 100 }}>
        <Spin size="large" />
      </div>
    )
  }

  // Daily cost chart
  const costChartOption = {
    tooltip: { trigger: 'axis' as const },
    xAxis: {
      type: 'category' as const,
      data: tokenStats?.daily_trend.map((d) => d.date.substring(0, 10)) || [],
    },
    yAxis: [
      { type: 'value' as const, name: 'Cost (CNY)' },
      { type: 'value' as const, name: 'Calls' },
    ],
    series: [
      {
        name: 'Cost (CNY)',
        type: 'bar',
        data: tokenStats?.daily_trend.map((d) => d.cost_cny) || [],
        itemStyle: { color: '#1677ff' },
      },
      {
        name: 'API Calls',
        type: 'line',
        yAxisIndex: 1,
        data: tokenStats?.daily_trend.map((d) => d.calls) || [],
        itemStyle: { color: '#52c41a' },
      },
    ],
  }

  // By-stage table data
  const stageData = Object.entries(tokenStats?.by_stage || {}).map(([stage, data]) => ({
    key: stage,
    stage,
    ...data,
  }))

  const modelData = Object.entries(tokenStats?.by_model || {}).map(([model, data]) => ({
    key: model,
    model,
    ...data,
  }))

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
        <Typography.Title level={4} style={{ margin: 0 }}>
          {t('nav.analytics')}
        </Typography.Title>
        <Select
          value={days}
          onChange={setDays}
          options={[
            { value: 1, label: 'Today' },
            { value: 7, label: '7 Days' },
            { value: 30, label: '30 Days' },
          ]}
          style={{ width: 120 }}
        />
      </div>

      {/* Summary Cards */}
      <Row gutter={[16, 16]}>
        <Col xs={24} sm={8}>
          <Card>
            <Statistic
              title="Total Cost"
              value={tokenStats?.total_cost_cny || 0}
              prefix={<DollarOutlined />}
              suffix="CNY"
              precision={4}
            />
          </Card>
        </Col>
        <Col xs={24} sm={8}>
          <Card>
            <Statistic
              title="API Calls"
              value={tokenStats?.total_calls || 0}
              prefix={<ApiOutlined />}
            />
          </Card>
        </Col>
        <Col xs={24} sm={8}>
          <Card>
            <Statistic
              title="Total Tokens"
              value={tokenStats?.total_tokens || 0}
              prefix={<ThunderboltOutlined />}
              formatter={(v) => Number(v).toLocaleString()}
            />
          </Card>
        </Col>
      </Row>

      {/* Pipeline Stats */}
      <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
        <Col xs={24} sm={12}>
          <Card>
            <Statistic title="Processed (24h)" value={pipelineStats?.total_processed || 0} />
          </Card>
        </Col>
        <Col xs={24} sm={12}>
          <Card>
            <Statistic
              title="Phase 1 Pass Rate"
              value={pipelineStats?.pass_rate_phase1 || 0}
              suffix="%"
              precision={1}
            />
          </Card>
        </Col>
      </Row>

      {/* Daily Cost Chart */}
      <Card title="Daily Token Cost & Calls" style={{ marginTop: 16 }}>
        <ReactECharts option={costChartOption} style={{ height: 300 }} />
      </Card>

      {/* By Stage */}
      <Card title="Usage by Pipeline Stage" size="small" style={{ marginTop: 16 }}>
        <Table
          dataSource={stageData}
          size="small"
          pagination={false}
          columns={[
            { title: 'Stage', dataIndex: 'stage', key: 'stage' },
            { title: 'Calls', dataIndex: 'calls', key: 'calls' },
            {
              title: 'Prompt Tokens',
              dataIndex: 'prompt_tokens',
              key: 'pt',
              render: (v: number) => v?.toLocaleString(),
            },
            {
              title: 'Completion Tokens',
              dataIndex: 'completion_tokens',
              key: 'ct',
              render: (v: number) => v?.toLocaleString(),
            },
            {
              title: 'Cost (CNY)',
              dataIndex: 'cost_cny',
              key: 'cost',
              render: (v: number) => `¥${v?.toFixed(4)}`,
            },
          ]}
        />
      </Card>

      {/* By Model */}
      <Card title="Usage by LLM Model" size="small" style={{ marginTop: 16 }}>
        <Table
          dataSource={modelData}
          size="small"
          pagination={false}
          columns={[
            { title: 'Model', dataIndex: 'model', key: 'model' },
            { title: 'Calls', dataIndex: 'calls', key: 'calls' },
            {
              title: 'Total Tokens',
              key: 'total',
              render: (_: any, r: any) =>
                ((r.prompt_tokens || 0) + (r.completion_tokens || 0)).toLocaleString(),
            },
            {
              title: 'Cost (CNY)',
              dataIndex: 'cost_cny',
              key: 'cost',
              render: (v: number) => `¥${v?.toFixed(4)}`,
            },
          ]}
        />
      </Card>
    </div>
  )
}
