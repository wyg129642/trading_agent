import { useCallback, useEffect, useState } from 'react'
import {
  Alert, Button, Card, Col, Descriptions, Drawer, Empty, InputNumber, List,
  message, Row, Slider, Space, Statistic, Table, Tag, Tooltip, Typography, Upload,
} from 'antd'
import type { UploadProps } from 'antd'
import {
  CloudUploadOutlined, FileSearchOutlined, FilterOutlined,
  InfoCircleOutlined, LinkOutlined, ReloadOutlined, WarningOutlined,
} from '@ant-design/icons'
import api from '../services/api'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
dayjs.extend(relativeTime)

const { Text, Paragraph, Title } = Typography

const TIER_COLOR: Record<string, string> = {
  HARD: '#ef4444', HIGH: '#f97316', MEDIUM: '#eab308',
  LOW: '#3b82f6', WATCH: '#94a3b8',
}
const TIER_LABEL: Record<string, string> = {
  HARD: '强烈', HIGH: '高', MEDIUM: '中', LOW: '低', WATCH: '观察',
}
const TYPE_LABEL: Record<string, string> = {
  DELIST: '退市', ST_RISK: 'ST/*ST', SUSPEND: '长期停牌',
  INVESTIGATE: '立案调查', FRAUD: '财务造假', AUDIT_BAD: '审计非标',
  EARNINGS_BAD: '业绩预亏', GOODWILL: '商誉减值', RESTRUCT_FAIL: '重组失败',
  RELATED_OCC: '关联占用', SUBSIDIARY_LOSS: '子公司失控',
  CONTROL_CHANGE: '控股股东变更', REDUCE: '股东减持', INQUIRY: '问询函',
  PLEDGE_RISK: '质押风险', EXEC_LEAVE: '高管离职', LITIGATION: '重大诉讼',
  RATING_DOWN: '评级下调',
}

interface RiskDetail {
  type: string
  type_name: string
  tier: string
  score: number
  raw_score?: number
  confidence: number
  evidence: string
  source: string
  source_url: string | null
  observed_at: string
  age_days: number
  classifier: string
}

interface Risk {
  stock_code: string
  stock_name?: string
  composite_score: number
  tier: string
  hard_blocks: number
  active_types: string[]
  details: RiskDetail[]
  updated_at: string
}

interface NewsItem {
  source: string
  title: string
  url: string | null
  category: string | null
  published_at: string | null
  fetched_at: string | null
  body_preview: string | null
}

interface ScanItem {
  code: string
  name: string | null
  trade_date: string | null
  buy_price: string | null
  actClosePrice: string | null
  types: string[]
  blocked: boolean
  risk: Risk | null
  news: NewsItem[]
  input_rows: any[]
}

interface ScanResult {
  summary: {
    total_rows: number
    unique_codes: number
    risky: number
    kept: number
    by_tier: Record<string, number>
    block_settings: { min_block_score: number; block_tiers: string[] }
  }
  items: ScanItem[]
}

interface Health {
  ok: boolean
  db_path?: string
  sources?: number
  signals_active?: number
  risk_list_size?: number
  by_tier?: Record<string, number>
  last_crawl?: Record<string, string>
  error?: string
}

export default function RiskDetection() {
  const [scanResult, setScanResult] = useState<ScanResult | null>(null)
  const [scanning, setScanning] = useState(false)
  const [minScore, setMinScore] = useState(70)
  const [blockTiers, setBlockTiers] = useState<string[]>(['HARD', 'HIGH'])
  const [lookbackDays, setLookbackDays] = useState(14)
  const [drawer, setDrawer] = useState<ScanItem | null>(null)
  const [health, setHealth] = useState<Health | null>(null)
  const [showOnlyBlocked, setShowOnlyBlocked] = useState(true)
  const [lastFile, setLastFile] = useState<File | null>(null)

  const fetchHealth = useCallback(async () => {
    try {
      const r = await api.get<Health>('/risk-detection/health')
      setHealth(r.data)
    } catch (e: any) {
      setHealth({ ok: false, error: e?.response?.data?.detail || String(e) })
    }
  }, [])

  useEffect(() => { fetchHealth() }, [fetchHealth])

  const runScan = useCallback(async (file: File) => {
    setLastFile(file)
    setScanning(true)
    try {
      const fd = new FormData()
      fd.append('file', file)
      fd.append('min_block_score', String(minScore))
      fd.append('block_tiers', blockTiers.join(','))
      fd.append('lookback_days', String(lookbackDays))
      fd.append('news_per_stock', '15')
      const res = await api.post<ScanResult>('/risk-detection/scan-csv', fd, {
        headers: { 'Content-Type': 'multipart/form-data' },
        timeout: 120000,
      })
      setScanResult(res.data)
      message.success(
        `扫描完成: ${res.data.summary.unique_codes} 只候选股, 命中 ${res.data.summary.risky} 只风险股`,
      )
    } catch (e: any) {
      message.error(e?.response?.data?.detail || '扫描失败')
    } finally {
      setScanning(false)
    }
  }, [minScore, blockTiers, lookbackDays])

  const uploadProps: UploadProps = {
    accept: '.csv',
    showUploadList: false,
    beforeUpload: (file) => { runScan(file); return false },
  }

  const rerunWithCurrentSettings = () => { if (lastFile) runScan(lastFile) }

  const itemsToShow = scanResult
    ? (showOnlyBlocked ? scanResult.items.filter(x => x.blocked) : scanResult.items)
    : []

  const columns = [
    {
      title: '代码', dataIndex: 'code', key: 'code', width: 90, fixed: 'left' as const,
      render: (c: string) => <Text code>{c}</Text>,
    },
    {
      title: '名称', dataIndex: 'name', key: 'name', width: 130,
      render: (n: string | null) => n || <Text type="secondary">-</Text>,
    },
    {
      title: '风险等级', key: 'tier', width: 100,
      render: (_: any, r: ScanItem) => r.risk
        ? <Tag color={TIER_COLOR[r.risk.tier]}>{TIER_LABEL[r.risk.tier] || r.risk.tier}</Tag>
        : <Tag color="default">无</Tag>,
    },
    {
      title: '风险系数', key: 'score', width: 130,
      sorter: (a: ScanItem, b: ScanItem) => (b.risk?.composite_score || 0) - (a.risk?.composite_score || 0),
      render: (_: any, r: ScanItem) => r.risk
        ? (
          <Space>
            <Text strong style={{ color: TIER_COLOR[r.risk.tier] }}>
              {r.risk.composite_score.toFixed(1)}
            </Text>
            <Text type="secondary" style={{ fontSize: 11 }}>/100</Text>
          </Space>
        )
        : <Text type="secondary">-</Text>,
    },
    {
      title: '风险类型', key: 'types',
      render: (_: any, r: ScanItem) => r.risk
        ? (
          <Space size={4} wrap>
            {r.risk.active_types.slice(0, 5).map(t => (
              <Tag key={t} color={TIER_COLOR[r.risk!.details.find(d => d.type === t)?.tier || 'WATCH']} style={{ marginRight: 0 }}>
                {TYPE_LABEL[t] || t}
              </Tag>
            ))}
            {r.risk.active_types.length > 5 && (
              <Text type="secondary" style={{ fontSize: 11 }}>+{r.risk.active_types.length - 5}</Text>
            )}
          </Space>
        )
        : <Text type="secondary">-</Text>,
    },
    {
      title: '主要证据', key: 'evidence',
      render: (_: any, r: ScanItem) => r.risk?.details?.[0]
        ? (
          <Tooltip title={r.risk.details[0].evidence}>
            <Text style={{ fontSize: 12, maxWidth: 280, display: 'inline-block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {r.risk.details[0].evidence}
            </Text>
          </Tooltip>
        )
        : <Text type="secondary">-</Text>,
    },
    {
      title: '近期新闻', key: 'news_count', width: 90, align: 'center' as const,
      render: (_: any, r: ScanItem) => r.news.length > 0
        ? <Tag color="blue">{r.news.length} 条</Tag>
        : <Text type="secondary" style={{ fontSize: 11 }}>无</Text>,
    },
    {
      title: '操作', key: 'action', width: 80, fixed: 'right' as const,
      render: (_: any, r: ScanItem) => (
        <Button type="link" size="small" onClick={() => setDrawer(r)}>
          详情
        </Button>
      ),
    },
  ]

  return (
    <div style={{ padding: 16 }}>
      <Title level={3} style={{ marginTop: 0 }}>
        <WarningOutlined style={{ color: '#f97316', marginRight: 8 }} />
        股票风险检测
      </Title>
      <Paragraph type="secondary" style={{ marginBottom: 16 }}>
        上传量化策略输出的候选股 CSV，系统从持续爬取的负面公告/新闻库中匹配每只股票的风险类型与系数。
        支持 18 类基本面负面信号: 退市/ST/立案/财务造假/审计非标/业绩预亏/减持/重组失败/商誉减值/...
      </Paragraph>

      {/* 系统状态卡 */}
      <Card size="small" style={{ marginBottom: 16 }} title={
        <Space>
          <InfoCircleOutlined />
          系统状态
          <Button size="small" icon={<ReloadOutlined />} onClick={fetchHealth}>刷新</Button>
        </Space>
      }>
        {health?.ok ? (
          <Row gutter={16}>
            <Col span={4}><Statistic title="风险股票" value={health.risk_list_size} /></Col>
            <Col span={4}><Statistic title="活跃信号" value={health.signals_active} /></Col>
            <Col span={4}><Statistic title="原始数据" value={health.sources} /></Col>
            <Col span={12}>
              <Text type="secondary" style={{ fontSize: 12 }}>分级: </Text>
              {health.by_tier && Object.entries(health.by_tier).map(([t, n]) => (
                <Tag key={t} color={TIER_COLOR[t]}>{TIER_LABEL[t] || t} {n}</Tag>
              ))}
            </Col>
          </Row>
        ) : (
          <Alert type="error" showIcon message={`detect 后端未就绪: ${health?.error || ''}`} />
        )}
      </Card>

      {/* 上传 + 参数卡 */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Row gutter={[16, 16]} align="middle">
          <Col span={8}>
            <Upload {...uploadProps}>
              <Button type="primary" size="large" icon={<CloudUploadOutlined />} loading={scanning}>
                上传候选股 CSV
              </Button>
            </Upload>
            <div style={{ fontSize: 11, color: '#94a3b8', marginTop: 4 }}>
              支持 secID / 代码 列；同名股可重复出现
            </div>
          </Col>
          <Col span={6}>
            <Text type="secondary">剔除阈值 (分数 ≥)</Text>
            <Slider min={20} max={100} value={minScore} onChange={setMinScore}
              marks={{ 45: 'M', 65: 'H', 85: '!' }} />
          </Col>
          <Col span={6}>
            <Text type="secondary">回溯新闻 (天)</Text>
            <InputNumber min={1} max={60} value={lookbackDays} onChange={(v) => setLookbackDays(v || 14)} style={{ width: '100%' }} />
          </Col>
          <Col span={4}>
            <Button block icon={<FilterOutlined />} disabled={!lastFile} onClick={rerunWithCurrentSettings}>
              重跑当前文件
            </Button>
          </Col>
        </Row>
      </Card>

      {/* 结果汇总 */}
      {scanResult && (
        <Card size="small" style={{ marginBottom: 16 }}>
          <Row gutter={16}>
            <Col span={4}><Statistic title="候选总数" value={scanResult.summary.unique_codes} /></Col>
            <Col span={4}><Statistic title="✅ 保留" value={scanResult.summary.kept} valueStyle={{ color: '#10b981' }} /></Col>
            <Col span={4}><Statistic title="❌ 剔除" value={scanResult.summary.risky} valueStyle={{ color: '#ef4444' }} /></Col>
            <Col span={12}>
              <Text type="secondary" style={{ fontSize: 12 }}>剔除分布: </Text>
              {Object.entries(scanResult.summary.by_tier).map(([t, n]) => (
                <Tag key={t} color={TIER_COLOR[t]}>{TIER_LABEL[t] || t} {n}</Tag>
              ))}
              <div style={{ marginTop: 8, fontSize: 11, color: '#64748b' }}>
                阈值: 系数 ≥ {scanResult.summary.block_settings.min_block_score} 或 等级 ∈ [{scanResult.summary.block_settings.block_tiers.join(', ')}]
              </div>
            </Col>
          </Row>
        </Card>
      )}

      {/* 表格 */}
      {scanResult && (
        <Card
          size="small"
          title={
            <Space>
              <FileSearchOutlined />
              检测结果 ({itemsToShow.length} 只)
            </Space>
          }
          extra={
            <Button size="small" type={showOnlyBlocked ? 'primary' : 'default'} onClick={() => setShowOnlyBlocked(s => !s)}>
              {showOnlyBlocked ? '只看风险股' : '显示全部'}
            </Button>
          }
        >
          {itemsToShow.length === 0 ? <Empty description="无数据" /> : (
            <Table
              size="small"
              rowKey="code"
              dataSource={itemsToShow}
              columns={columns as any}
              pagination={{ pageSize: 30, showSizeChanger: true, showTotal: (t) => `共 ${t} 只` }}
              scroll={{ x: 1200 }}
              rowClassName={(r) => r.blocked ? '' : ''}
            />
          )}
        </Card>
      )}

      {/* 详情 Drawer */}
      <Drawer
        title={drawer ? `${drawer.code}  ${drawer.name || ''}` : ''}
        open={!!drawer}
        onClose={() => setDrawer(null)}
        width={680}
        destroyOnClose
      >
        {drawer && (
          <>
            <Descriptions size="small" column={2} bordered style={{ marginBottom: 12 }}>
              <Descriptions.Item label="风险系数">
                {drawer.risk
                  ? <Text strong style={{ color: TIER_COLOR[drawer.risk.tier], fontSize: 18 }}>
                      {drawer.risk.composite_score.toFixed(1)}
                    </Text>
                  : '-'}
              </Descriptions.Item>
              <Descriptions.Item label="风险等级">
                {drawer.risk
                  ? <Tag color={TIER_COLOR[drawer.risk.tier]}>{TIER_LABEL[drawer.risk.tier]}</Tag>
                  : <Tag>无</Tag>}
              </Descriptions.Item>
              <Descriptions.Item label="买入档位">{drawer.types.join(', ') || '-'}</Descriptions.Item>
              <Descriptions.Item label="收盘价">{drawer.actClosePrice || '-'}</Descriptions.Item>
            </Descriptions>

            {drawer.risk?.details?.length ? (
              <>
                <Title level={5} style={{ marginTop: 12 }}>风险类型明细</Title>
                <List
                  size="small"
                  dataSource={drawer.risk.details}
                  renderItem={(d) => (
                    <List.Item style={{ alignItems: 'flex-start' }}>
                      <div style={{ width: '100%' }}>
                        <Space>
                          <Tag color={TIER_COLOR[d.tier]}>{d.score.toFixed(1)}</Tag>
                          <Text strong>{d.type_name}</Text>
                          <Text type="secondary" style={{ fontSize: 11 }}>
                            {dayjs(d.observed_at).fromNow()} · 置信 {(d.confidence * 100).toFixed(0)}%
                          </Text>
                        </Space>
                        <div style={{ marginTop: 4, fontSize: 12 }}>{d.evidence}</div>
                        {d.source_url && (
                          <a href={d.source_url} target="_blank" rel="noreferrer" style={{ fontSize: 11 }}>
                            <LinkOutlined /> 原始公告 ({d.source})
                          </a>
                        )}
                      </div>
                    </List.Item>
                  )}
                />
              </>
            ) : <Alert type="success" showIcon message="未检测到任何风险信号" style={{ marginTop: 8 }} />}

            {drawer.news.length > 0 && (
              <>
                <Title level={5} style={{ marginTop: 16 }}>近期相关公告/新闻</Title>
                <List
                  size="small"
                  dataSource={drawer.news}
                  renderItem={(n) => (
                    <List.Item>
                      <div style={{ width: '100%' }}>
                        <Space wrap size={4}>
                          <Tag>{n.source}</Tag>
                          <Text type="secondary" style={{ fontSize: 11 }}>
                            {n.published_at ? dayjs(n.published_at).format('MM-DD HH:mm') : '-'}
                          </Text>
                          {n.category && <Tag color="cyan">{n.category}</Tag>}
                        </Space>
                        <div style={{ marginTop: 2, fontSize: 12 }}>
                          {n.url
                            ? <a href={n.url} target="_blank" rel="noreferrer">{n.title}</a>
                            : n.title}
                        </div>
                        {n.body_preview && (
                          <div style={{ fontSize: 11, color: '#94a3b8', marginTop: 2 }}>{n.body_preview}</div>
                        )}
                      </div>
                    </List.Item>
                  )}
                />
              </>
            )}
          </>
        )}
      </Drawer>
    </div>
  )
}
