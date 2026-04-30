import { useEffect, useMemo, useState } from 'react'
import {
  Card, Table, Tag, Select, Space, Button, Typography, Tooltip,
  Segmented, Statistic, Row, Col, message, Modal, Input, InputNumber,
  Switch, Empty, Alert,
} from 'antd'
import {
  ReloadOutlined, ThunderboltOutlined, FileTextOutlined,
  RiseOutlined, RadarChartOutlined, FilterOutlined,
} from '@ant-design/icons'
import type { ColumnsType } from 'antd/es/table'
import { useNavigate } from 'react-router-dom'
import { useAuthStore } from '../store/auth'
import api from '../services/api'

const { Title, Text } = Typography

interface AsofEntry {
  asof: string
  has_full: boolean
  has_top: boolean
  has_primed: boolean
  has_stage2: boolean
  has_meta: boolean
  has_html: boolean
  rows_screener: number | null
  rows_primed: number | null
  rows_stage2: number | null
  regime: string | null
  wrote_at: number | null
}

interface ScreenerRow {
  ts_code: string
  name?: string
  board?: string
  industry?: string
  sw1_name?: string
  close?: number
  stage1_score?: number
  stage2_trigger?: boolean
  rs_rating?: number
  composite?: number
  quality_pass?: boolean | string
  veto_reasons?: string
  regime?: string
  drawdown?: number
  base_length?: number
  pivot?: number
  vol_surge?: number
  trend_template_ok?: boolean
  fresh_breakout?: boolean
  hv60?: number
  atr_pct?: number
  [key: string]: any
}

const REGIME_COLOR: Record<string, string> = {
  risk_on: 'green',
  caution: 'orange',
  risk_off: 'red',
  unknown: 'default',
}

type Kind = 'top' | 'primed' | 'stage2' | 'full'

export default function Screener() {
  const navigate = useNavigate()
  const user = useAuthStore((s) => s.user)
  const isAdmin = user?.role === 'admin'

  const [asofList, setAsofList] = useState<AsofEntry[]>([])
  const [asof, setAsof] = useState<string | undefined>()
  const [kind, setKind] = useState<Kind>('top')
  const [rows, setRows] = useState<ScreenerRow[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)
  const [meta, setMeta] = useState<any>(null)
  const [searchText, setSearchText] = useState('')
  const [onlyPassed, setOnlyPassed] = useState(false)
  const [minStage1, setMinStage1] = useState<number | null>(null)
  const [triggerOpen, setTriggerOpen] = useState(false)
  const [triggerForm, setTriggerForm] = useState<{
    asof?: string; top: number; limit?: number; years?: number
  }>({ top: 50 })

  const loadAsofList = async () => {
    try {
      const { data } = await api.get<AsofEntry[]>('/screener/asof-list')
      setAsofList(data)
      if (!asof && data.length) setAsof(data[0].asof)
    } catch (e: any) {
      const msg = e?.response?.data?.detail || e?.message || '加载失败'
      message.error(`as-of 列表加载失败: ${msg}`)
    }
  }

  const loadMeta = async (a: string) => {
    try {
      const { data } = await api.get('/screener/meta', { params: { asof: a } })
      setMeta(data)
    } catch {
      setMeta(null)
    }
  }

  const loadRows = async (a: string, k: Kind) => {
    setLoading(true)
    try {
      let resp
      if (k === 'full') {
        resp = await api.get('/screener/full', {
          params: {
            asof: a,
            limit: 500,
            only_passed: onlyPassed,
            min_stage1: minStage1 ?? undefined,
          },
        })
      } else {
        resp = await api.get('/screener/watchlist', {
          params: { asof: a, kind: k },
        })
      }
      const { rows: rs, total: t } = resp.data
      setRows(rs || [])
      setTotal(t || 0)
    } catch (e: any) {
      const msg = e?.response?.data?.detail || e?.message || 'unknown'
      message.error(`筛选结果加载失败: ${msg}`)
      setRows([]); setTotal(0)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { loadAsofList() }, [])
  useEffect(() => {
    if (asof) { loadMeta(asof); loadRows(asof, kind) }
  }, [asof, kind, onlyPassed, minStage1])

  const filtered = useMemo(() => {
    if (!searchText) return rows
    const q = searchText.toLowerCase()
    return rows.filter(r =>
      (r.ts_code || '').toLowerCase().includes(q) ||
      (r.name || '').toLowerCase().includes(q) ||
      (r.industry || '').toLowerCase().includes(q) ||
      (r.sw1_name || '').toLowerCase().includes(q),
    )
  }, [rows, searchText])

  const columns: ColumnsType<ScreenerRow> = [
    {
      title: 'Code', dataIndex: 'ts_code', key: 'ts_code', width: 110, fixed: 'left',
      render: (v) => (
        <a onClick={() => navigate(`/stock/${v}`)} style={{ fontFamily: 'monospace' }}>
          {v}
        </a>
      ),
    },
    {
      title: '名称', dataIndex: 'name', key: 'name', width: 100, fixed: 'left',
      render: (v) => v || <Text type="secondary">—</Text>,
    },
    {
      title: '板块', dataIndex: 'board', key: 'board', width: 90,
      filters: [
        { text: 'Main', value: 'Main' },
        { text: 'ChiNext', value: 'ChiNext' },
        { text: 'STAR', value: 'STAR' },
      ],
      onFilter: (val, r) => r.board === val,
      render: (v) => v ? <Tag>{v}</Tag> : '—',
    },
    {
      title: '行业', dataIndex: 'industry', key: 'industry', width: 110,
      render: (v) => v || '—',
    },
    {
      title: '申万一级', dataIndex: 'sw1_name', key: 'sw1_name', width: 110,
      render: (v) => v ? <Tag color="geekblue">{v}</Tag> : '—',
    },
    {
      title: '价格', dataIndex: 'close', key: 'close', width: 80,
      align: 'right',
      render: (v) => v != null ? Number(v).toFixed(2) : '—',
      sorter: (a, b) => (a.close || 0) - (b.close || 0),
    },
    {
      title: 'Stage-1',
      dataIndex: 'stage1_score', key: 'stage1_score', width: 90, align: 'right',
      render: (v) => {
        const n = Number(v ?? 0)
        const color = n >= 4 ? 'success' : n >= 3 ? 'processing' : 'default'
        return <Tag color={color}>{n.toFixed(0)}/5</Tag>
      },
      sorter: (a, b) => (a.stage1_score || 0) - (b.stage1_score || 0),
      defaultSortOrder: 'descend',
    },
    {
      title: 'Stage-2', dataIndex: 'stage2_trigger', key: 'stage2_trigger', width: 90,
      render: (v) => v === true || v === 'True' || v === 1
        ? <Tag color="success" icon={<ThunderboltOutlined />}>触发</Tag>
        : <Text type="secondary">—</Text>,
    },
    {
      title: 'RS', dataIndex: 'rs_rating', key: 'rs_rating', width: 80, align: 'right',
      render: (v) => v != null ? Number(v).toFixed(1) : '—',
      sorter: (a, b) => (a.rs_rating || 0) - (b.rs_rating || 0),
    },
    {
      title: '复合分', dataIndex: 'composite', key: 'composite', width: 95, align: 'right',
      render: (v) => {
        const n = Number(v ?? 0)
        const color = n >= 70 ? '#10b981' : n >= 40 ? '#2563eb' : '#94a3b8'
        return <Text strong style={{ color }}>{n.toFixed(1)}</Text>
      },
      sorter: (a, b) => (a.composite || 0) - (b.composite || 0),
    },
    {
      title: '基本面',
      dataIndex: 'quality_pass', key: 'quality_pass', width: 95,
      render: (v) => {
        const pass = v === true || v === 'True' || v === 1
        return pass
          ? <Tag color="success">通过</Tag>
          : <Tag>未达</Tag>
      },
    },
    {
      title: '回撤',
      dataIndex: 'drawdown', key: 'drawdown', width: 80, align: 'right',
      render: (v) => v != null ? `${(Number(v) * 100).toFixed(1)}%` : '—',
    },
    {
      title: '基础天数',
      dataIndex: 'base_length', key: 'base_length', width: 90, align: 'right',
      render: (v) => v ?? '—',
    },
    {
      title: 'Pivot',
      dataIndex: 'pivot', key: 'pivot', width: 80, align: 'right',
      render: (v) => v != null ? Number(v).toFixed(2) : '—',
    },
    {
      title: 'Vol×',
      dataIndex: 'vol_surge', key: 'vol_surge', width: 75, align: 'right',
      render: (v) => v != null ? `${Number(v).toFixed(2)}×` : '—',
    },
    {
      title: '否决原因',
      dataIndex: 'veto_reasons', key: 'veto_reasons', width: 200,
      render: (v) => {
        if (!v) return <Text type="secondary">—</Text>
        return v.split(',').filter(Boolean).map((r: string) =>
          <Tag color="warning" key={r} style={{ marginBottom: 2 }}>{r}</Tag>,
        )
      },
    },
  ]

  const handleTrigger = async () => {
    try {
      const { data } = await api.post('/screener/trigger', triggerForm)
      message.success(`已启动筛选 (PID ${data.pid})。完成约 5–25 分钟，刷新 as-of 列表查看。`)
      setTriggerOpen(false)
    } catch (e: any) {
      const msg = e?.response?.data?.detail || e?.message
      message.error(`启动失败: ${msg}`)
    }
  }

  const regime = meta?.regime as string | undefined

  return (
    <div style={{ padding: '0 4px' }}>
      <Title level={3} style={{ marginBottom: 16 }}>
        <RadarChartOutlined /> Multi-Bagger Hunt 筛选器
      </Title>

      <Alert
        type="info" showIcon style={{ marginBottom: 16 }}
        message="STRATEGY_PLAN.md v2 — 右侧量化价量初筛 + 行业深入验证"
        description="先用 Stage-1 五族打分(≥4 入主备库)，等 Stage-1 → Stage-2 突破再触发买点。RS 排名横截面计算，回归门用 CSI 300。具体定义见 /docs/STRATEGY_PLAN.md。"
      />

      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Card size="small">
            <Statistic
              title="As-of"
              value={asof || '—'}
              valueStyle={{ fontFamily: 'monospace', fontSize: 18 }}
            />
            <Text type="secondary" style={{ fontSize: 11 }}>
              共 {asofList.length} 个历史结果
            </Text>
          </Card>
        </Col>
        <Col span={6}>
          <Card size="small">
            <Statistic
              title="市场环境 (Regime)"
              value={regime ? regime.replace('_', '-') : '—'}
              valueStyle={{ color: REGIME_COLOR[regime || 'unknown'] || '#1e293b' }}
            />
            <Text type="secondary" style={{ fontSize: 11 }}>
              CSI 300 + breadth + 实现波动率
            </Text>
          </Card>
        </Col>
        <Col span={6}>
          <Card size="small">
            <Statistic
              title="Universe survivors"
              value={meta?.rows_screener ?? '—'}
            />
            <Text type="secondary" style={{ fontSize: 11 }}>
              通过 §1 + base anchor
            </Text>
          </Card>
        </Col>
        <Col span={6}>
          <Card size="small">
            <Statistic
              title="Stage-2 触发 / 主备库"
              value={`${meta?.rows_stage2 ?? 0} / ${meta?.rows_primed ?? 0}`}
              valueStyle={{ color: '#2563eb' }}
            />
            <Text type="secondary" style={{ fontSize: 11 }}>
              今日突破 / Stage-1≥4
            </Text>
          </Card>
        </Col>
      </Row>

      <Card style={{ marginBottom: 16 }}>
        <Space wrap size="middle">
          <Space>
            <Text strong>As-of：</Text>
            <Select
              style={{ width: 160 }}
              value={asof}
              onChange={setAsof}
              options={asofList.map(a => ({
                value: a.asof,
                label: `${a.asof} (${a.rows_screener ?? 0})`,
              }))}
              placeholder="选择日期"
            />
          </Space>
          <Segmented
            value={kind}
            onChange={(v) => setKind(v as Kind)}
            options={[
              { label: 'Top 复合分', value: 'top', icon: <RiseOutlined /> },
              { label: 'Stage-2 突破', value: 'stage2', icon: <ThunderboltOutlined /> },
              { label: '主备库 (Stage-1≥4)', value: 'primed', icon: <FilterOutlined /> },
              { label: '完整结果', value: 'full', icon: <FileTextOutlined /> },
            ]}
          />
          <Input.Search
            placeholder="代码 / 名称 / 行业..."
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            style={{ width: 220 }}
            allowClear
          />
          {kind === 'full' && (
            <>
              <Space>
                <Text>仅基本面通过：</Text>
                <Switch checked={onlyPassed} onChange={setOnlyPassed} />
              </Space>
              <Space>
                <Text>Stage-1≥</Text>
                <InputNumber
                  min={0} max={5} step={0.5} style={{ width: 80 }}
                  value={minStage1 ?? undefined}
                  onChange={(v) => setMinStage1(v == null ? null : Number(v))}
                  placeholder="任意"
                />
              </Space>
            </>
          )}
          <Button icon={<ReloadOutlined />} onClick={() => {
            if (asof) { loadAsofList(); loadRows(asof, kind) }
          }}>刷新</Button>
          {isAdmin && (
            <Button type="primary" icon={<ThunderboltOutlined />}
              onClick={() => setTriggerOpen(true)}>
              启动新一轮筛选
            </Button>
          )}
          {meta?.has_html !== false && asof && (
            <Button icon={<FileTextOutlined />}
              onClick={() => window.open(`/api/screener/report-html?asof=${asof}`, '_blank')}>
              查看 HTML 报告
            </Button>
          )}
        </Space>
      </Card>

      <Card>
        {asofList.length === 0 ? (
          <Empty
            description={
              <Space direction="vertical">
                <Text>还没有筛选结果。</Text>
                <Text type="secondary">
                  在主机上运行 <Text code>stockfilter screen --top 50</Text>{' '}
                  或点击右上角"启动新一轮筛选"（管理员）。
                </Text>
              </Space>
            }
          />
        ) : (
          <Table<ScreenerRow>
            rowKey="ts_code"
            dataSource={filtered}
            columns={columns}
            loading={loading}
            scroll={{ x: 1700 }}
            pagination={{
              pageSize: 25, showSizeChanger: true,
              showTotal: (n) => `共 ${n} 条 / 服务端总数 ${total}`,
            }}
            size="small"
          />
        )}
      </Card>

      <Modal
        title="启动新一轮筛选"
        open={triggerOpen}
        onCancel={() => setTriggerOpen(false)}
        onOk={handleTrigger}
        okText="启动"
        width={520}
      >
        <Alert
          type="warning" style={{ marginBottom: 16 }} showIcon
          message="筛选会作为后台进程运行，完整 universe 约 15–25 分钟，--limit 测试模式 1–3 分钟。"
        />
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
          <Space>
            <Text style={{ width: 100, display: 'inline-block' }}>As-of：</Text>
            <Input
              placeholder="YYYYMMDD（留空 = 最近交易日）"
              value={triggerForm.asof || ''}
              onChange={(e) => setTriggerForm({ ...triggerForm, asof: e.target.value || undefined })}
              style={{ width: 200 }}
            />
          </Space>
          <Space>
            <Text style={{ width: 100, display: 'inline-block' }}>Top N：</Text>
            <InputNumber
              min={1} max={500} value={triggerForm.top}
              onChange={(v) => setTriggerForm({ ...triggerForm, top: Number(v) || 50 })}
              style={{ width: 120 }}
            />
          </Space>
          <Space>
            <Text style={{ width: 100, display: 'inline-block' }}>限制 (limit)：</Text>
            <InputNumber
              min={10} max={5000} value={triggerForm.limit}
              onChange={(v) => setTriggerForm({ ...triggerForm, limit: v == null ? undefined : Number(v) })}
              style={{ width: 120 }}
              placeholder="测试用"
            />
            <Text type="secondary">留空 = 完整 4000+ 票</Text>
          </Space>
          <Space>
            <Text style={{ width: 100, display: 'inline-block' }}>历史年限：</Text>
            <InputNumber
              min={1} max={10} value={triggerForm.years}
              onChange={(v) => setTriggerForm({ ...triggerForm, years: v == null ? undefined : Number(v) })}
              style={{ width: 120 }}
              placeholder="3"
            />
          </Space>
        </Space>
      </Modal>
    </div>
  )
}
