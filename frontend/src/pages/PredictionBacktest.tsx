import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Card, Table, Tag, Select, Space, Button, Typography, Statistic,
  Row, Col, Progress, message, Modal, Tooltip, Empty,
} from 'antd'
import {
  TrophyOutlined, ArrowUpOutlined, ArrowDownOutlined,
  ExperimentOutlined, ReloadOutlined, UserOutlined,
  CheckCircleOutlined, CloseCircleOutlined,
  CrownOutlined, FireOutlined, AimOutlined,
  BarChartOutlined, RiseOutlined,
} from '@ant-design/icons'
import type { ColumnsType } from 'antd/es/table'
import ReactECharts from 'echarts-for-react'
import { useAuthStore } from '../store/auth'
import api from '../services/api'

const { Title, Text } = Typography
const { Option } = Select

interface RankingItem {
  rank: number
  user_id: string
  username: string
  display_name: string | null
  total_predictions: number
  evaluated_predictions: number
  accuracy_rate: number
  avg_return_pct: number
  avg_score: number
  composite_score: number
}

interface AnalystStats {
  user_id: string
  username: string
  display_name: string | null
  total_predictions: number
  evaluated_predictions: number
  correct_predictions: number
  accuracy_rate: number
  avg_return_pct: number
  avg_score: number
  horizon_stats: Record<string, { label: string; total: number; correct: number; accuracy: number; avg_return: number }>
  direction_stats: Record<string, { total: number; correct: number; accuracy: number; avg_return: number }>
  confidence_calibration: Record<string, { total: number; correct: number; accuracy: number }>
  recent_accuracy: number | null
}

const RANK_ICONS = [
  <CrownOutlined style={{ color: '#f59e0b', fontSize: 20 }} />,
  <CrownOutlined style={{ color: '#94a3b8', fontSize: 18 }} />,
  <CrownOutlined style={{ color: '#cd7f32', fontSize: 16 }} />,
]

export default function PredictionBacktest() {
  const navigate = useNavigate()
  const user = useAuthStore((s) => s.user)
  const isBossOrAdmin = user?.role === 'admin' || user?.role === 'boss'

  const [rankings, setRankings] = useState<RankingItem[]>([])
  const [loading, setLoading] = useState(false)
  const [backtestLoading, setBacktestLoading] = useState(false)
  const [filterMarket, setFilterMarket] = useState<string | undefined>()
  const [filterHorizon, setFilterHorizon] = useState<string | undefined>()
  const [analystDetail, setAnalystDetail] = useState<AnalystStats | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)

  const fetchRankings = async () => {
    setLoading(true)
    try {
      const params: any = { min_predictions: 1 }
      if (filterMarket) params.market = filterMarket
      if (filterHorizon) params.time_horizon = filterHorizon
      const res = await api.get('/predictions/rankings/list', { params })
      setRankings(res.data.rankings)
    } catch {
      message.error('加载排名失败')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchRankings()
  }, [filterMarket, filterHorizon])

  const runBacktest = async () => {
    setBacktestLoading(true)
    try {
      const res = await api.post('/predictions/backtest', { force: false })
      const { successful, failed, errors } = res.data
      if (successful > 0) {
        message.success(`回测完成：成功 ${successful} 条`)
      }
      if (failed > 0) {
        message.warning(`${failed} 条回测失败`)
      }
      if (errors.length > 0) {
        Modal.info({
          title: '回测详情',
          content: (
            <ul>
              {errors.map((e: string, i: number) => <li key={i}>{e}</li>)}
            </ul>
          ),
        })
      }
      fetchRankings()
    } catch (e: any) {
      message.error(e.response?.data?.detail || '回测执行失败')
    } finally {
      setBacktestLoading(false)
    }
  }

  const showAnalystDetail = async (userId: string) => {
    setDetailLoading(true)
    try {
      const res = await api.get(`/predictions/analyst/${userId}`)
      setAnalystDetail(res.data)
    } catch {
      message.error('加载分析师详情失败')
    } finally {
      setDetailLoading(false)
    }
  }

  // Summary stats
  const totalAnalysts = rankings.length
  const avgAccuracy = rankings.length > 0
    ? rankings.reduce((sum, r) => sum + r.accuracy_rate, 0) / rankings.length
    : 0
  const avgReturn = rankings.length > 0
    ? rankings.reduce((sum, r) => sum + r.avg_return_pct, 0) / rankings.length
    : 0
  const topPerformer = rankings[0]

  // Chart: accuracy distribution
  const accuracyChartOption = {
    tooltip: { trigger: 'axis' as const },
    xAxis: {
      type: 'category' as const,
      data: rankings.map((r) => r.display_name || r.username),
      axisLabel: { rotate: 45, fontSize: 11 },
    },
    yAxis: [
      { type: 'value' as const, name: '准确率 %', max: 100 },
      { type: 'value' as const, name: '综合分' },
    ],
    series: [
      {
        name: '准确率',
        type: 'bar',
        data: rankings.map((r) => (r.accuracy_rate * 100).toFixed(1)),
        itemStyle: { color: '#2563eb' },
      },
      {
        name: '综合分',
        type: 'line',
        yAxisIndex: 1,
        data: rankings.map((r) => r.composite_score.toFixed(1)),
        itemStyle: { color: '#f59e0b' },
      },
    ],
    grid: { bottom: 80 },
  }

  const columns: ColumnsType<RankingItem> = [
    {
      title: '排名',
      dataIndex: 'rank',
      width: 70,
      render: (rank) => (
        <Space>
          {rank <= 3 ? RANK_ICONS[rank - 1] : <Text type="secondary">{rank}</Text>}
        </Space>
      ),
    },
    {
      title: '分析师',
      key: 'analyst',
      width: 140,
      render: (_, r) => (
        <Button type="link" style={{ padding: 0 }} onClick={() => showAnalystDetail(r.user_id)}>
          <UserOutlined style={{ marginRight: 4 }} />
          {r.display_name || r.username}
        </Button>
      ),
    },
    {
      title: '预测数',
      key: 'counts',
      width: 100,
      render: (_, r) => (
        <Tooltip title={`总计 ${r.total_predictions}，已评估 ${r.evaluated_predictions}`}>
          <Text>{r.evaluated_predictions}</Text>
          <Text type="secondary"> / {r.total_predictions}</Text>
        </Tooltip>
      ),
    },
    {
      title: '准确率',
      dataIndex: 'accuracy_rate',
      width: 150,
      render: (v) => (
        <Space>
          <Progress
            percent={Number((v * 100).toFixed(1))}
            size="small"
            style={{ width: 80 }}
            strokeColor={v >= 0.6 ? '#10b981' : v >= 0.4 ? '#f59e0b' : '#ef4444'}
          />
          <Text strong>{(v * 100).toFixed(1)}%</Text>
        </Space>
      ),
      sorter: (a, b) => a.accuracy_rate - b.accuracy_rate,
    },
    {
      title: '平均收益',
      dataIndex: 'avg_return_pct',
      width: 110,
      render: (v) => (
        <Text style={{ color: v >= 0 ? '#ef4444' : '#22c55e', fontWeight: 600 }}>
          {v >= 0 ? '+' : ''}{v.toFixed(2)}%
        </Text>
      ),
      sorter: (a, b) => a.avg_return_pct - b.avg_return_pct,
    },
    {
      title: '平均得分',
      dataIndex: 'avg_score',
      width: 100,
      render: (v) => <Text>{v.toFixed(1)}</Text>,
      sorter: (a, b) => a.avg_score - b.avg_score,
    },
    {
      title: '综合分',
      dataIndex: 'composite_score',
      width: 100,
      render: (v) => (
        <Text strong style={{ fontSize: 16, color: '#2563eb' }}>
          {v.toFixed(1)}
        </Text>
      ),
      sorter: (a, b) => a.composite_score - b.composite_score,
      defaultSortOrder: 'descend',
    },
    {
      title: '操作',
      key: 'action',
      width: 100,
      render: (_, r) => (
        <Space>
          <Button size="small" onClick={() => showAnalystDetail(r.user_id)}>
            详情
          </Button>
          <Button
            size="small"
            type="link"
            onClick={() => navigate(`/predictions?user_id=${r.user_id}`)}
          >
            记录
          </Button>
        </Space>
      ),
    },
  ]

  // Analyst detail chart: confidence calibration
  const getCalibrationChart = (stats: AnalystStats) => ({
    tooltip: { trigger: 'axis' as const },
    xAxis: {
      type: 'category' as const,
      data: ['1星', '2星', '3星', '4星', '5星'],
      name: '置信度',
    },
    yAxis: { type: 'value' as const, name: '准确率 %', max: 100 },
    series: [
      {
        name: '准确率',
        type: 'bar',
        data: [1, 2, 3, 4, 5].map((c) => {
          const s = stats.confidence_calibration[String(c)]
          return s ? (s.accuracy * 100).toFixed(1) : 0
        }),
        itemStyle: {
          color: (params: any) => {
            const colors = ['#94a3b8', '#60a5fa', '#2563eb', '#f59e0b', '#ef4444']
            return colors[params.dataIndex] || '#2563eb'
          },
        },
      },
      {
        name: '预测数',
        type: 'line',
        data: [1, 2, 3, 4, 5].map((c) => {
          const s = stats.confidence_calibration[String(c)]
          return s ? s.total : 0
        }),
        itemStyle: { color: '#94a3b8' },
      },
    ],
  })

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <Title level={3} style={{ margin: 0 }}>
          <TrophyOutlined style={{ marginRight: 8, color: '#f59e0b' }} />
          荐股回测 & 分析师排行
        </Title>
        <Space>
          {isBossOrAdmin && (
            <Button
              type="primary"
              icon={<ExperimentOutlined />}
              loading={backtestLoading}
              onClick={runBacktest}
            >
              执行回测
            </Button>
          )}
          <Button icon={<ReloadOutlined />} onClick={fetchRankings}>
            刷新
          </Button>
        </Space>
      </div>

      {/* Summary stats */}
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Card size="small">
            <Statistic
              title="参与分析师"
              value={totalAnalysts}
              prefix={<UserOutlined />}
              suffix="人"
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card size="small">
            <Statistic
              title="平均准确率"
              value={(avgAccuracy * 100).toFixed(1)}
              prefix={<AimOutlined />}
              suffix="%"
              valueStyle={{ color: avgAccuracy >= 0.5 ? '#10b981' : '#ef4444' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card size="small">
            <Statistic
              title="平均收益率"
              value={avgReturn.toFixed(2)}
              prefix={<RiseOutlined />}
              suffix="%"
              valueStyle={{ color: avgReturn >= 0 ? '#ef4444' : '#22c55e' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card size="small">
            <Statistic
              title="最佳分析师"
              value={topPerformer?.display_name || topPerformer?.username || '-'}
              prefix={<FireOutlined style={{ color: '#f59e0b' }} />}
            />
          </Card>
        </Col>
      </Row>

      {/* Filters */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space>
          <Text type="secondary">筛选：</Text>
          <Select
            placeholder="市场"
            allowClear
            style={{ width: 100 }}
            onChange={(v) => setFilterMarket(v)}
          >
            <Option value="A股">A股</Option>
            <Option value="港股">港股</Option>
            <Option value="美股">美股</Option>
          </Select>
          <Select
            placeholder="周期"
            allowClear
            style={{ width: 110 }}
            onChange={(v) => setFilterHorizon(v)}
          >
            <Option value="1w">1周</Option>
            <Option value="2w">2周</Option>
            <Option value="1m">1个月</Option>
            <Option value="3m">3个月</Option>
            <Option value="6m">6个月</Option>
          </Select>
        </Space>
      </Card>

      {/* Chart */}
      {rankings.length > 0 && (
        <Card size="small" title="分析师对比" style={{ marginBottom: 16 }}>
          <ReactECharts option={accuracyChartOption} style={{ height: 300 }} />
        </Card>
      )}

      {/* Rankings table */}
      <Card title="排行榜">
        <Table
          columns={columns}
          dataSource={rankings}
          rowKey="user_id"
          loading={loading}
          pagination={false}
          scroll={{ x: 900 }}
          size="small"
          locale={{ emptyText: <Empty description="暂无回测数据，请先提交预测并执行回测" /> }}
        />
      </Card>

      {/* Analyst detail modal */}
      <Modal
        open={!!analystDetail}
        onCancel={() => setAnalystDetail(null)}
        footer={null}
        title={
          <Space>
            <UserOutlined />
            {analystDetail?.display_name || analystDetail?.username} - 详细统计
          </Space>
        }
        width={800}
        loading={detailLoading}
      >
        {analystDetail && (
          <div>
            {/* Overview stats */}
            <Row gutter={16} style={{ marginBottom: 16 }}>
              <Col span={6}>
                <Statistic title="总预测数" value={analystDetail.total_predictions} />
              </Col>
              <Col span={6}>
                <Statistic title="已评估" value={analystDetail.evaluated_predictions} />
              </Col>
              <Col span={6}>
                <Statistic
                  title="准确率"
                  value={(analystDetail.accuracy_rate * 100).toFixed(1)}
                  suffix="%"
                  valueStyle={{ color: analystDetail.accuracy_rate >= 0.5 ? '#10b981' : '#ef4444' }}
                />
              </Col>
              <Col span={6}>
                <Statistic
                  title="平均得分"
                  value={analystDetail.avg_score.toFixed(1)}
                  suffix="/ 100"
                />
              </Col>
            </Row>

            {/* Recent trend */}
            {analystDetail.recent_accuracy != null && (
              <Card size="small" style={{ marginBottom: 16 }}>
                <Space>
                  <Text type="secondary">近10次预测准确率：</Text>
                  <Progress
                    percent={Number((analystDetail.recent_accuracy * 100).toFixed(0))}
                    style={{ width: 200 }}
                    strokeColor={analystDetail.recent_accuracy >= 0.6 ? '#10b981' : '#f59e0b'}
                  />
                </Space>
              </Card>
            )}

            {/* Horizon breakdown */}
            {Object.keys(analystDetail.horizon_stats).length > 0 && (
              <Card size="small" title="按周期统计" style={{ marginBottom: 16 }}>
                <Row gutter={8}>
                  {Object.entries(analystDetail.horizon_stats).map(([key, s]) => (
                    <Col span={4} key={key}>
                      <Card size="small" style={{ textAlign: 'center' }}>
                        <Text strong>{s.label}</Text>
                        <div style={{ margin: '8px 0' }}>
                          <Progress
                            type="circle"
                            percent={Number((s.accuracy * 100).toFixed(0))}
                            size={60}
                            strokeColor={s.accuracy >= 0.6 ? '#10b981' : s.accuracy >= 0.4 ? '#f59e0b' : '#ef4444'}
                          />
                        </div>
                        <Text type="secondary" style={{ fontSize: 12 }}>
                          {s.correct}/{s.total}
                        </Text>
                      </Card>
                    </Col>
                  ))}
                </Row>
              </Card>
            )}

            {/* Direction breakdown */}
            {Object.keys(analystDetail.direction_stats).length > 0 && (
              <Card size="small" title="按方向统计" style={{ marginBottom: 16 }}>
                <Row gutter={16}>
                  {analystDetail.direction_stats.bullish && (
                    <Col span={12}>
                      <Card size="small">
                        <Space>
                          <ArrowUpOutlined style={{ color: '#ef4444', fontSize: 20 }} />
                          <div>
                            <Text strong>看涨</Text>
                            <br />
                            <Text>准确率: {(analystDetail.direction_stats.bullish.accuracy * 100).toFixed(1)}%</Text>
                            <br />
                            <Text type="secondary">
                              {analystDetail.direction_stats.bullish.correct}/{analystDetail.direction_stats.bullish.total}，
                              平均收益 {analystDetail.direction_stats.bullish.avg_return.toFixed(2)}%
                            </Text>
                          </div>
                        </Space>
                      </Card>
                    </Col>
                  )}
                  {analystDetail.direction_stats.bearish && (
                    <Col span={12}>
                      <Card size="small">
                        <Space>
                          <ArrowDownOutlined style={{ color: '#22c55e', fontSize: 20 }} />
                          <div>
                            <Text strong>看跌</Text>
                            <br />
                            <Text>准确率: {(analystDetail.direction_stats.bearish.accuracy * 100).toFixed(1)}%</Text>
                            <br />
                            <Text type="secondary">
                              {analystDetail.direction_stats.bearish.correct}/{analystDetail.direction_stats.bearish.total}，
                              平均收益 {analystDetail.direction_stats.bearish.avg_return.toFixed(2)}%
                            </Text>
                          </div>
                        </Space>
                      </Card>
                    </Col>
                  )}
                </Row>
              </Card>
            )}

            {/* Confidence calibration chart */}
            {Object.keys(analystDetail.confidence_calibration).length > 0 && (
              <Card size="small" title="置信度校准（置信度越高是否越准？）">
                <ReactECharts option={getCalibrationChart(analystDetail)} style={{ height: 250 }} />
              </Card>
            )}
          </div>
        )}
      </Modal>
    </div>
  )
}
