import { useState, useEffect } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import {
  Card, Table, Tag, Select, Space, Button, Input, Typography, Tooltip,
  Rate, Badge, Modal, Descriptions, Timeline, message,
} from 'antd'
import {
  ArrowUpOutlined, ArrowDownOutlined, SearchOutlined,
  PlusOutlined, EditOutlined, DeleteOutlined,
  CheckCircleOutlined, CloseCircleOutlined,
  ClockCircleOutlined, ExperimentOutlined,
  HistoryOutlined, EyeOutlined,
} from '@ant-design/icons'
import type { ColumnsType } from 'antd/es/table'
import { useAuthStore } from '../store/auth'
import api from '../services/api'

const { Title, Text } = Typography
const { Option } = Select

const HORIZON_LABELS: Record<string, string> = {
  '1w': '1周', '2w': '2周', '1m': '1个月', '3m': '3个月', '6m': '6个月',
}

const STATUS_MAP: Record<string, { color: string; text: string; icon: React.ReactNode }> = {
  active: { color: 'processing', text: '进行中', icon: <ClockCircleOutlined /> },
  expired: { color: 'warning', text: '待回测', icon: <ExperimentOutlined /> },
  evaluated: { color: 'success', text: '已评估', icon: <CheckCircleOutlined /> },
}

interface PredictionItem {
  id: string
  user: { id: string; username: string; display_name: string | null } | null
  submitted_by: { id: string; username: string; display_name: string | null } | null
  stock_code: string
  stock_name: string
  market: string
  direction: string
  time_horizon: string
  reason: string | null
  confidence: number
  price_at_submit: number | null
  target_price: number | null
  status: string
  expires_at: string | null
  created_at: string
  updated_at: string
  evaluation: {
    price_at_end: number | null
    return_pct: number | null
    is_direction_correct: boolean | null
    score: number | null
    max_favorable_pct: number | null
    max_adverse_pct: number | null
  } | null
  edit_logs: {
    field_changed: string
    old_value: string | null
    new_value: string | null
    edited_at: string
    edited_by: { username: string; display_name: string | null } | null
  }[]
}

export default function PredictionList() {
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const user = useAuthStore((s) => s.user)
  const isBossOrAdmin = user?.role === 'admin' || user?.role === 'boss'

  const [data, setData] = useState<PredictionItem[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(20)
  const [detailModal, setDetailModal] = useState<PredictionItem | null>(null)

  // Filters
  const [filterUserId, setFilterUserId] = useState<string | undefined>(searchParams.get('user_id') || undefined)
  const [filterMarket, setFilterMarket] = useState<string | undefined>()
  const [filterDirection, setFilterDirection] = useState<string | undefined>()
  const [filterStatus, setFilterStatus] = useState<string | undefined>()
  const [filterHorizon, setFilterHorizon] = useState<string | undefined>()
  const [filterStock, setFilterStock] = useState<string | undefined>()

  const fetchData = async () => {
    setLoading(true)
    try {
      const params: any = { page, page_size: pageSize }
      if (filterUserId) params.user_id = filterUserId
      if (filterMarket) params.market = filterMarket
      if (filterDirection) params.direction = filterDirection
      if (filterStatus) params.status = filterStatus
      if (filterHorizon) params.time_horizon = filterHorizon
      if (filterStock) params.stock_code = filterStock

      const res = await api.get('/predictions/', { params })
      setData(res.data.items)
      setTotal(res.data.total)
    } catch {
      message.error('加载失败')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchData()
  }, [page, pageSize, filterUserId, filterMarket, filterDirection, filterStatus, filterHorizon, filterStock])

  const handleDelete = async (id: string) => {
    Modal.confirm({
      title: '确认删除',
      content: '删除后不可恢复，确定要删除这条预测吗？',
      onOk: async () => {
        try {
          await api.delete(`/predictions/${id}`)
          message.success('已删除')
          fetchData()
        } catch {
          message.error('删除失败')
        }
      },
    })
  }

  const columns: ColumnsType<PredictionItem> = [
    {
      title: '分析师',
      dataIndex: 'user',
      width: 100,
      render: (u) => u?.display_name || u?.username || '-',
    },
    {
      title: '股票',
      key: 'stock',
      width: 150,
      render: (_, r) => (
        <div>
          <Text strong>{r.stock_name}</Text>
          <br />
          <Text type="secondary" style={{ fontSize: 12 }}>{r.stock_code}</Text>
          <Tag
            color={r.market === 'A股' ? 'red' : r.market === '美股' ? 'blue' : 'purple'}
            style={{ marginLeft: 6, fontSize: 10 }}
          >
            {r.market}
          </Tag>
        </div>
      ),
    },
    {
      title: '方向',
      dataIndex: 'direction',
      width: 80,
      render: (d) => (
        d === 'bullish'
          ? <Tag color="red" icon={<ArrowUpOutlined />}>看涨</Tag>
          : <Tag color="green" icon={<ArrowDownOutlined />}>看跌</Tag>
      ),
    },
    {
      title: '周期',
      dataIndex: 'time_horizon',
      width: 70,
      render: (h) => HORIZON_LABELS[h] || h,
    },
    {
      title: '置信度',
      dataIndex: 'confidence',
      width: 130,
      render: (c) => <Rate disabled value={c} style={{ fontSize: 14 }} />,
    },
    {
      title: '提交价',
      dataIndex: 'price_at_submit',
      width: 90,
      render: (p) => p != null ? p.toFixed(2) : '-',
    },
    {
      title: '状态',
      dataIndex: 'status',
      width: 90,
      render: (s) => {
        const info = STATUS_MAP[s] || { color: 'default', text: s, icon: null }
        return <Badge status={info.color as any} text={info.text} />
      },
    },
    {
      title: '评估结果',
      key: 'evaluation',
      width: 160,
      render: (_, r) => {
        if (!r.evaluation) return <Text type="secondary">-</Text>
        const { return_pct, is_direction_correct, score } = r.evaluation
        return (
          <Space direction="vertical" size={0}>
            <Space size={4}>
              {is_direction_correct
                ? <CheckCircleOutlined style={{ color: '#10b981' }} />
                : <CloseCircleOutlined style={{ color: '#ef4444' }} />
              }
              <Text style={{ color: return_pct != null && return_pct >= 0 ? '#ef4444' : '#22c55e' }}>
                {return_pct != null ? `${return_pct >= 0 ? '+' : ''}${return_pct.toFixed(2)}%` : '-'}
              </Text>
            </Space>
            <Text type="secondary" style={{ fontSize: 12 }}>
              得分: {score != null ? score.toFixed(1) : '-'}
            </Text>
          </Space>
        )
      },
    },
    {
      title: '提交时间',
      dataIndex: 'created_at',
      width: 110,
      render: (t) => new Date(t).toLocaleDateString('zh-CN'),
    },
    {
      title: '操作',
      key: 'action',
      width: 120,
      render: (_, r) => (
        <Space size={4}>
          <Tooltip title="详情">
            <Button
              type="text"
              size="small"
              icon={<EyeOutlined />}
              onClick={() => setDetailModal(r)}
            />
          </Tooltip>
          {r.edit_logs.length > 0 && (
            <Tooltip title={`${r.edit_logs.length}次修改`}>
              <Badge count={r.edit_logs.length} size="small" offset={[-4, 0]}>
                <Button type="text" size="small" icon={<HistoryOutlined />} onClick={() => setDetailModal(r)} />
              </Badge>
            </Tooltip>
          )}
          {isBossOrAdmin && (
            <Tooltip title="删除">
              <Button
                type="text"
                size="small"
                danger
                icon={<DeleteOutlined />}
                onClick={() => handleDelete(r.id)}
              />
            </Tooltip>
          )}
        </Space>
      ),
    },
  ]

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <Title level={3} style={{ margin: 0 }}>荐股预测记录</Title>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          onClick={() => navigate('/predictions/submit')}
        >
          提交新预测
        </Button>
      </div>

      {/* Filters */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap>
          <Input
            placeholder="股票代码"
            prefix={<SearchOutlined />}
            allowClear
            style={{ width: 140 }}
            onChange={(e) => { setFilterStock(e.target.value || undefined); setPage(1) }}
          />
          <Select
            placeholder="市场"
            allowClear
            style={{ width: 100 }}
            onChange={(v) => { setFilterMarket(v); setPage(1) }}
          >
            <Option value="A股">A股</Option>
            <Option value="港股">港股</Option>
            <Option value="美股">美股</Option>
          </Select>
          <Select
            placeholder="方向"
            allowClear
            style={{ width: 100 }}
            onChange={(v) => { setFilterDirection(v); setPage(1) }}
          >
            <Option value="bullish">看涨</Option>
            <Option value="bearish">看跌</Option>
          </Select>
          <Select
            placeholder="状态"
            allowClear
            style={{ width: 110 }}
            onChange={(v) => { setFilterStatus(v); setPage(1) }}
          >
            <Option value="active">进行中</Option>
            <Option value="expired">待回测</Option>
            <Option value="evaluated">已评估</Option>
          </Select>
          <Select
            placeholder="周期"
            allowClear
            style={{ width: 110 }}
            onChange={(v) => { setFilterHorizon(v); setPage(1) }}
          >
            <Option value="1w">1周</Option>
            <Option value="2w">2周</Option>
            <Option value="1m">1个月</Option>
            <Option value="3m">3个月</Option>
            <Option value="6m">6个月</Option>
          </Select>
        </Space>
      </Card>

      <Card>
        <Table
          columns={columns}
          dataSource={data}
          rowKey="id"
          loading={loading}
          pagination={{
            current: page,
            pageSize,
            total,
            showSizeChanger: true,
            showTotal: (t) => `共 ${t} 条`,
            onChange: (p, s) => { setPage(p); setPageSize(s) },
          }}
          scroll={{ x: 1100 }}
          size="small"
        />
      </Card>

      {/* Detail modal */}
      <Modal
        open={!!detailModal}
        onCancel={() => setDetailModal(null)}
        footer={null}
        title="预测详情"
        width={700}
      >
        {detailModal && (
          <div>
            <Descriptions bordered size="small" column={2}>
              <Descriptions.Item label="分析师">
                {detailModal.user?.display_name || detailModal.user?.username}
              </Descriptions.Item>
              <Descriptions.Item label="提交者">
                {detailModal.submitted_by?.display_name || detailModal.submitted_by?.username}
                {detailModal.submitted_by?.id !== detailModal.user?.id && (
                  <Tag color="orange" style={{ marginLeft: 6, fontSize: 10 }}>代提交</Tag>
                )}
              </Descriptions.Item>
              <Descriptions.Item label="股票">
                {detailModal.stock_name} ({detailModal.stock_code})
              </Descriptions.Item>
              <Descriptions.Item label="市场">
                <Tag>{detailModal.market}</Tag>
              </Descriptions.Item>
              <Descriptions.Item label="方向">
                {detailModal.direction === 'bullish'
                  ? <Tag color="red" icon={<ArrowUpOutlined />}>看涨</Tag>
                  : <Tag color="green" icon={<ArrowDownOutlined />}>看跌</Tag>
                }
              </Descriptions.Item>
              <Descriptions.Item label="周期">
                {HORIZON_LABELS[detailModal.time_horizon]}
              </Descriptions.Item>
              <Descriptions.Item label="置信度">
                <Rate disabled value={detailModal.confidence} style={{ fontSize: 14 }} />
              </Descriptions.Item>
              <Descriptions.Item label="提交价">
                {detailModal.price_at_submit?.toFixed(2) || '-'}
              </Descriptions.Item>
              {detailModal.target_price && (
                <Descriptions.Item label="目标价" span={2}>
                  {detailModal.target_price.toFixed(2)}
                </Descriptions.Item>
              )}
              <Descriptions.Item label="提交时间">
                {new Date(detailModal.created_at).toLocaleString('zh-CN')}
              </Descriptions.Item>
              <Descriptions.Item label="到期时间">
                {detailModal.expires_at ? new Date(detailModal.expires_at).toLocaleString('zh-CN') : '-'}
              </Descriptions.Item>
            </Descriptions>

            {detailModal.reason && (
              <Card size="small" style={{ marginTop: 16 }} title="推荐理由">
                <Text>{detailModal.reason}</Text>
              </Card>
            )}

            {detailModal.evaluation && (
              <Card size="small" style={{ marginTop: 16 }} title="回测结果">
                <Descriptions size="small" column={2}>
                  <Descriptions.Item label="结束价">
                    {detailModal.evaluation.price_at_end?.toFixed(2)}
                  </Descriptions.Item>
                  <Descriptions.Item label="实际收益">
                    <Text style={{
                      color: (detailModal.evaluation.return_pct ?? 0) >= 0 ? '#ef4444' : '#22c55e',
                      fontWeight: 600,
                    }}>
                      {detailModal.evaluation.return_pct != null
                        ? `${detailModal.evaluation.return_pct >= 0 ? '+' : ''}${detailModal.evaluation.return_pct.toFixed(2)}%`
                        : '-'}
                    </Text>
                  </Descriptions.Item>
                  <Descriptions.Item label="方向正确">
                    {detailModal.evaluation.is_direction_correct
                      ? <Tag color="success" icon={<CheckCircleOutlined />}>正确</Tag>
                      : <Tag color="error" icon={<CloseCircleOutlined />}>错误</Tag>
                    }
                  </Descriptions.Item>
                  <Descriptions.Item label="评分">
                    <Text strong style={{ fontSize: 18 }}>
                      {detailModal.evaluation.score?.toFixed(1)}
                    </Text>
                    <Text type="secondary"> / 100</Text>
                  </Descriptions.Item>
                  <Descriptions.Item label="最大有利">
                    <Text style={{ color: '#10b981' }}>
                      +{detailModal.evaluation.max_favorable_pct?.toFixed(2)}%
                    </Text>
                  </Descriptions.Item>
                  <Descriptions.Item label="最大不利">
                    <Text style={{ color: '#ef4444' }}>
                      {detailModal.evaluation.max_adverse_pct?.toFixed(2)}%
                    </Text>
                  </Descriptions.Item>
                </Descriptions>
              </Card>
            )}

            {detailModal.edit_logs.length > 0 && (
              <Card size="small" style={{ marginTop: 16 }} title="修改记录">
                <Timeline
                  items={detailModal.edit_logs.map((log) => ({
                    color: 'blue',
                    children: (
                      <div>
                        <Text strong>{log.edited_by?.display_name || log.edited_by?.username}</Text>
                        <Text type="secondary"> 修改了 </Text>
                        <Tag>{log.field_changed}</Tag>
                        <br />
                        <Text delete type="secondary">{log.old_value}</Text>
                        <Text> → </Text>
                        <Text>{log.new_value}</Text>
                        <br />
                        <Text type="secondary" style={{ fontSize: 12 }}>
                          {new Date(log.edited_at).toLocaleString('zh-CN')}
                        </Text>
                      </div>
                    ),
                  }))}
                />
              </Card>
            )}
          </div>
        )}
      </Modal>
    </div>
  )
}
