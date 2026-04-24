/**
 * Main revenue-modeling workspace — the spreadsheet + inspector view.
 *
 * Layout: left = cell grid grouped by path prefix; right = CellInspector
 * drawer. Top bar shows model metadata, Run/Stop button, sanity summary.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import {
  Alert, Badge, Button, Card, Drawer, Input, Modal, Progress, Select, Space,
  Spin, Table, Tabs, Tag, Timeline, Tooltip, message,
} from 'antd'
import {
  ArrowLeftOutlined, CheckCircleOutlined, EditOutlined, LockOutlined,
  PlayCircleOutlined, WarningOutlined,
} from '@ant-design/icons'
import {
  modelingApi, subscribeRun, type Confidence, type ModelCell, type RecipeRun,
  type RevenueModelDetail, type SanityIssue, type SourceType,
} from '../services/modeling'
import CellInspector from '../components/modeling/CellInspector'
import SanityPanel from '../components/modeling/SanityPanel'
import RunTimeline from '../components/modeling/RunTimeline'

const SOURCE_COLORS: Record<SourceType, string> = {
  historical: '#10b981',   // green
  guidance: '#3b82f6',     // blue
  expert: '#8b5cf6',       // purple
  inferred: '#94a3b8',     // gray
  assumption: '#f59e0b',   // yellow/amber
  derived: '#64748b',      // slate
}

const CONFIDENCE_COLORS: Record<Confidence, string> = {
  HIGH: '#10b981',
  MEDIUM: '#f59e0b',
  LOW: '#ef4444',
}

function fmtValue(c: ModelCell): string {
  if (c.value_text && c.value_type === 'text') return c.value_text
  if (c.value == null) return c.formula ? '(computing)' : '—'
  if (c.value_type === 'percent') return (c.value * 100).toFixed(1) + '%'
  if (c.value_type === 'currency') return c.value.toLocaleString('en-US', {
    maximumFractionDigits: 2,
  })
  if (c.value_type === 'count') return c.value.toLocaleString()
  return c.value.toLocaleString('en-US', { maximumFractionDigits: 4 })
}

export default function RevenueModel() {
  const { id = '' } = useParams()
  const nav = useNavigate()
  const [model, setModel] = useState<RevenueModelDetail | null>(null)
  const [cells, setCells] = useState<ModelCell[]>([])
  const [sanity, setSanity] = useState<SanityIssue[]>([])
  const [runs, setRuns] = useState<RecipeRun[]>([])
  const [activeRun, setActiveRun] = useState<RecipeRun | null>(null)
  const [selectedCell, setSelectedCell] = useState<ModelCell | null>(null)
  const [loading, setLoading] = useState(true)
  const [runEvents, setRunEvents] = useState<any[]>([])
  const esRef = useRef<EventSource | null>(null)

  const reload = useCallback(async () => {
    setLoading(true)
    try {
      const detail = await modelingApi.getModel(id)
      setModel(detail)
      setCells(detail.cells)
      const [s, rs] = await Promise.all([
        modelingApi.listSanity(id).catch(() => []),
        modelingApi.listRuns(id).catch(() => []),
      ])
      setSanity(s)
      setRuns(rs)
      if (rs.length > 0 && rs[0].status === 'running') {
        setActiveRun(rs[0])
      }
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setLoading(false)
    }
  }, [id])

  useEffect(() => { reload() }, [reload])

  // Subscribe to active run events
  useEffect(() => {
    if (!activeRun) return
    if (esRef.current) esRef.current.close()
    const es = subscribeRun(activeRun.id, (type, data) => {
      setRunEvents(prev => [...prev, { type, data, ts: new Date().toISOString() }])
      if (type === 'run_completed' || type === 'step_failed') {
        setActiveRun(null)
        reload()
      }
      // Light polling on cell writes during long-running steps
      if (type === 'step_completed') {
        reload()
      }
    })
    esRef.current = es
    return () => es.close()
  }, [activeRun?.id, reload])

  const startRun = async (dry_run: boolean) => {
    try {
      const run = await modelingApi.startRun(id, { settings: { dry_run } })
      setActiveRun(run)
      setRunEvents([])
      message.success(`Run ${run.id.substring(0, 8)} 已启动${dry_run ? ' (dry-run)' : ''}`)
    } catch (e: any) {
      const detail = e?.response?.data?.detail
      if (e?.response?.status === 402 && detail?.error === 'monthly_quota_exceeded') {
        Modal.error({
          title: '月度 LLM 预算已耗尽',
          content: (
            <div>
              <p>本月剩余预算不足以启动该 Recipe:</p>
              <ul>
                <li>预估成本: <strong>${detail.estimated_cost_usd?.toFixed(2)}</strong></li>
                <li>本月已用: <strong>${detail.spent_this_month_usd?.toFixed(2)}</strong></li>
                <li>月度预算: <strong>${detail.monthly_budget_usd?.toFixed(2)}</strong></li>
              </ul>
              <p>请联系管理员提升您的 LLM 预算，或等待下月预算重置。</p>
            </div>
          ),
        })
      } else {
        message.error(typeof detail === 'string' ? detail : JSON.stringify(detail) || String(e))
      }
    }
  }

  const previewRunCost = async () => {
    try {
      const est = await modelingApi.estimateCost(id, {})
      Modal.confirm({
        title: '运行前成本预估',
        width: 620,
        content: (
          <div>
            <div style={{ marginBottom: 12 }}>
              <span style={{ fontSize: 22, fontWeight: 600 }}>
                预估 ${est.total_usd.toFixed(2)}
              </span>
              <span style={{ marginLeft: 12, color: '#64748b' }}>
                {est.step_count} 步 · {est.total_input_tokens.toLocaleString()} in / {est.total_output_tokens.toLocaleString()} out tokens
              </span>
              {est.recommendation === 'warn' && (
                <Tag color="orange" style={{ marginLeft: 8 }}>接近预算警戒线</Tag>
              )}
              {est.recommendation === 'blocked' && (
                <Tag color="red" style={{ marginLeft: 8 }}>将超出预算</Tag>
              )}
            </div>
            <div style={{ fontSize: 12, color: '#64748b', marginBottom: 8 }}>
              本月已用 ${est.quota.spent_this_month_usd.toFixed(2)} / ${est.quota.monthly_budget_usd.toFixed(2)}
              &nbsp; · 剩余 ${est.quota.remaining_usd.toFixed(2)}
            </div>
            <div style={{ maxHeight: 240, overflow: 'auto', borderTop: '1px solid #e2e8f0', paddingTop: 8 }}>
              {Object.entries(est.per_step_usd).sort((a, b) => b[1] - a[1]).map(([step, cost]) => (
                <div key={step} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, padding: '2px 0' }}>
                  <code>{step}</code>
                  <span>${cost.toFixed(2)}</span>
                </div>
              ))}
            </div>
            <div style={{ marginTop: 10, fontSize: 11, color: '#94a3b8' }}>
              {est.assumptions.map((a, i) => <div key={i}>• {a}</div>)}
            </div>
          </div>
        ),
        okText: est.recommendation === 'blocked' ? '我知道会被拒(取消)' : '确认运行',
        okButtonProps: { disabled: est.recommendation === 'blocked' },
        onOk: () => startRun(false),
      })
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const onCellClick = (c: ModelCell) => setSelectedCell(c)

  const handleCellUpdate = async (updated: Partial<ModelCell> & { edit_reason?: string }) => {
    if (!selectedCell) return
    try {
      const patched = await modelingApi.updateCell(id, selectedCell.id, updated as any)
      setCells(prev => prev.map(c => (c.id === patched.id ? patched : c)))
      setSelectedCell(patched)
      message.success('已保存')
      // Kick a silent sanity refresh
      modelingApi.listSanity(id).then(setSanity).catch(() => {})
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  // Group cells by leading path segment for the Tabs view
  const groups = useMemo(() => {
    const g: Record<string, ModelCell[]> = {}
    for (const c of cells) {
      const head = c.path.split('.')[0]
      g[head] = g[head] || []
      g[head].push(c)
    }
    // Sort each group by path
    Object.values(g).forEach(arr => arr.sort((a, b) => a.path.localeCompare(b.path)))
    return g
  }, [cells])

  if (loading || !model) {
    return <div style={{ padding: 40, textAlign: 'center' }}><Spin size="large" /></div>
  }

  return (
    <div style={{ padding: '16px 24px' }}>
      {/* Hallucination-guard pause banner — shown above header when tripped */}
      {model.paused_by_guard && (
        <Alert
          type="error"
          showIcon
          style={{ marginBottom: 12 }}
          message={<strong>🚨 模型已被幻觉守卫暂停</strong>}
          description={
            <div>
              <div>原因: <code>{model.paused_reason || 'hallucination_rate_exceeded'}</code></div>
              <div style={{ marginTop: 4 }}>
                周度 citation audit 检测到本模型的 mismatch 数量超阈值,已自动从 ready 状态降级。
                请修复被 flag 的 cell 后由管理员在 /admin/citation-audit 手动恢复。
              </div>
            </div>
          }
        />
      )}
      {/* Header */}
      <Card size="small" style={{ marginBottom: 12 }}>
        <Space size="large" align="center">
          <Button icon={<ArrowLeftOutlined />} onClick={() => nav('/modeling')}>返回</Button>
          <span style={{ fontSize: 18 }}>
            <strong>{model.company_name}</strong>{' '}
            <Tag color="blue">{model.ticker}</Tag>
            <Tag>{model.industry}</Tag>
            <Tag color={model.status === 'ready' ? 'green' : model.status === 'running' ? 'processing' : 'default'}>
              {model.status}
            </Tag>
          </span>
          <span style={{ color: '#64748b' }}>
            {model.fiscal_periods.join(' · ')} · {model.cell_count} cells
            {model.flagged_count > 0 && <> · ⚠️ {model.flagged_count} flagged</>}
          </span>
          <Space style={{ marginLeft: 'auto' }}>
            {activeRun ? (
              <Button loading>运行中 ({activeRun.current_step_id || '...'})</Button>
            ) : (
              <>
                <Button onClick={() => startRun(true)}>🧪 Dry-run (no LLM)</Button>
                <Button onClick={previewRunCost}>💰 预估成本</Button>
                <Button type="primary" icon={<PlayCircleOutlined />} onClick={() => startRun(false)}>
                  运行 Recipe
                </Button>
              </>
            )}
            <Button onClick={() => modelingApi.evaluate(id).then(() => reload())}>重算公式</Button>
            <Button onClick={() => {
              const token = (localStorage.getItem('auth-storage') &&
                JSON.parse(localStorage.getItem('auth-storage') || '{}').state?.token) || ''
              // Use fetch to download with auth header
              fetch(`/api/models/${id}/export.xlsx`, {
                headers: { Authorization: `Bearer ${token}` },
              }).then(async (r) => {
                if (!r.ok) throw new Error(`HTTP ${r.status}`)
                const blob = await r.blob()
                const url = URL.createObjectURL(blob)
                const a = document.createElement('a')
                a.href = url
                a.download = `${model.ticker.replace('.', '_')}.xlsx`
                a.click()
                URL.revokeObjectURL(url)
              }).catch(e => message.error(String(e)))
            }}>📥 导出 Excel</Button>
          </Space>
        </Space>
      </Card>

      {/* Sanity summary */}
      {sanity.length > 0 && (
        <Alert
          type={sanity.some(x => x.severity === 'error') ? 'error' : 'warning'}
          message={`${sanity.length} 个健全性问题 (${sanity.filter(x => x.severity === 'error').length} 严重 · ${sanity.filter(x => x.severity === 'warn').length} 警告)`}
          style={{ marginBottom: 12 }}
          closable
        />
      )}

      {/* Active run events */}
      {activeRun && runEvents.length > 0 && (
        <Card size="small" title="📡 实时执行日志" style={{ marginBottom: 12 }}>
          <RunTimeline events={runEvents} />
        </Card>
      )}

      {/* Main: grouped cell tables */}
      <Card size="small" bodyStyle={{ padding: 8 }}>
        <Tabs
          type="card"
          items={[
            ...Object.entries(groups).map(([head, arr]) => ({
              key: head,
              label: `${head} (${arr.length})`,
              children: (
                <Table
                  size="small"
                  rowKey="id"
                  dataSource={arr}
                  pagination={false}
                  onRow={(r) => ({
                    onClick: () => onCellClick(r),
                    style: { cursor: 'pointer' },
                  })}
                  columns={[
                    {
                      title: '路径', dataIndex: 'path', key: 'path', width: 380,
                      render: (p: string, r: ModelCell) => (
                        <Space>
                          <Badge
                            status="success"
                            color={CONFIDENCE_COLORS[r.confidence]}
                          />
                          <code style={{ fontSize: 12, color: r.extra?.dry_run ? '#94a3b8' : undefined }}>{p}</code>
                          {r.extra?.dry_run && (
                            <Tooltip title="Dry-run (未调用真实 LLM/工具) — 数据仅供结构演示，不要用于实际决策">
                              <Tag color="default" style={{ margin: 0 }}>🧪 DRY</Tag>
                            </Tooltip>
                          )}
                          {r.locked_by_human && <LockOutlined style={{ color: '#64748b' }} />}
                          {r.human_override && <EditOutlined style={{ color: '#3b82f6' }} />}
                          {r.review_status === 'flagged' && <WarningOutlined style={{ color: '#f59e0b' }} />}
                        </Space>
                      ),
                    },
                    { title: '标签', dataIndex: 'label', key: 'label', width: 180 },
                    { title: '期间', dataIndex: 'period', key: 'period', width: 80 },
                    { title: '单位', dataIndex: 'unit', key: 'unit', width: 80 },
                    {
                      title: '值', dataIndex: 'value', key: 'value', width: 160,
                      render: (_: any, r: ModelCell) => {
                        const noCite = !r.formula && r.citations.length === 0 && r.value != null
                        return (
                          <Space size={4}>
                            <strong style={{
                              color: r.extra?.dry_run ? '#94a3b8' : (r.formula ? '#64748b' : '#0f172a'),
                              textDecoration: r.extra?.dry_run ? 'line-through' : undefined,
                            }}>
                              {fmtValue(r)}
                            </strong>
                            {noCite && (
                              <Tooltip title="此数字未挂接任何工具抓取到的引用 — 请人工校验">
                                <Tag color="red" style={{ margin: 0, fontSize: 10 }}>无引用</Tag>
                              </Tooltip>
                            )}
                          </Space>
                        )
                      },
                    },
                    {
                      title: '公式', dataIndex: 'formula', key: 'formula',
                      ellipsis: true,
                      render: (f: string | null) => f ? <code>{f}</code> : '',
                    },
                    {
                      title: '来源', dataIndex: 'source_type', key: 'source_type', width: 120,
                      render: (t: SourceType) => (
                        <Tag color={SOURCE_COLORS[t]}>{t}</Tag>
                      ),
                    },
                    {
                      title: '置信度', dataIndex: 'confidence', key: 'confidence', width: 90,
                      render: (c: Confidence) => (
                        <Tag color={
                          c === 'HIGH' ? 'green' : c === 'MEDIUM' ? 'orange' : 'red'
                        }>{c}</Tag>
                      ),
                    },
                    {
                      title: '引用', key: 'citations', width: 80,
                      render: (_: any, r: ModelCell) => (
                        r.citations.length > 0
                          ? <Tooltip title={r.citations.map(c => c.title).join(' · ')}>
                              <Tag>{r.citations.length}</Tag>
                            </Tooltip>
                          : <span style={{ color: '#cbd5e1' }}>—</span>
                      ),
                    },
                  ]}
                />
              ),
            })),
            {
              key: '_sanity',
              label: sanity.length > 0 ? `⚠️ 健全性 (${sanity.length})` : '✅ 健全性',
              children: <SanityPanel issues={sanity} onSelectCell={(path) => {
                const found = cells.find(c => c.path === path)
                if (found) onCellClick(found)
              }} />,
            },
            {
              key: '_runs',
              label: `运行记录 (${runs.length})`,
              children: (
                <Table
                  size="small"
                  rowKey="id"
                  dataSource={runs}
                  columns={[
                    { title: 'Run', dataIndex: 'id', key: 'id', render: (v: string) => v.substring(0, 8) },
                    { title: '状态', dataIndex: 'status', key: 'status', render: (s: string) => (
                      <Tag color={s === 'completed' ? 'green' : s === 'failed' ? 'red' : s === 'running' ? 'processing' : 'default'}>
                        {s}
                      </Tag>
                    )},
                    { title: '当前步骤', dataIndex: 'current_step_id', key: 'step' },
                    { title: 'tokens', dataIndex: 'total_tokens', key: 'tokens' },
                    { title: '开始', dataIndex: 'created_at', key: 'created', render: (v: string) => new Date(v).toLocaleString('zh-CN') },
                    { title: '完成', dataIndex: 'completed_at', key: 'completed', render: (v: string | null) => v ? new Date(v).toLocaleString('zh-CN') : '—' },
                  ]}
                />
              ),
            },
          ]}
        />
      </Card>

      <CellInspector
        cell={selectedCell}
        modelId={id}
        industry={model?.industry}
        onClose={() => setSelectedCell(null)}
        onUpdate={handleCellUpdate}
      />
    </div>
  )
}
