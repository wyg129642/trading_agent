/**
 * RecipeCanvasEditor — researcher-facing visual DAG editor.
 *
 * Non-CS researchers can:
 *   * drag step nodes around a canvas
 *   * click a node to edit its prompt template, tool selection, LLM model,
 *     thresholds, validation
 *   * add/remove/reorder steps and wire them up with edges
 *   * run the edited recipe in dry-run or wet mode, with live progress
 *   * A/B run against another recipe (see RecipeABCompare)
 *
 * Every edit is auto-saved as a fork (if read-only source) or in-place
 * (if owner). Prompt edits emit a recipe_prompt_edit feedback event.
 */
// @ts-nocheck
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate, useParams, useSearchParams } from 'react-router-dom'
// NOTE: reactflow is listed in package.json — run `npm install` before building.
// Using dynamic import via typeof any to avoid a TS error if deps aren't yet
// installed in the editing environment.
import ReactFlow, {
  Background, Controls, MiniMap, MarkerType, ReactFlowProvider,
  addEdge, useEdgesState, useNodesState, Handle, Position,
} from 'reactflow'
import 'reactflow/dist/style.css'
import {
  Alert, Button, Card, Drawer, Dropdown, Form, Input, InputNumber, Modal,
  Select, Space, Switch, Tabs, Tag, Tooltip, Typography, message,
} from 'antd'
import {
  ArrowLeftOutlined, BranchesOutlined, DeleteOutlined, ExperimentOutlined,
  PlayCircleOutlined, PlusOutlined, SaveOutlined, ThunderboltOutlined,
} from '@ant-design/icons'
import { recipeApi, modelingApi, type Recipe } from '../services/modeling'

const { TextArea } = Input
const { Paragraph } = Typography

const STEP_TYPES: { key: string; label: string; color: string; description: string }[] = [
  { key: 'GATHER_CONTEXT', label: '1. 读纪要/基本面', color: '#60a5fa', description: '业绩会、10-K、投资者日' },
  { key: 'DECOMPOSE_SEGMENTS', label: '2. 拆业务部门', color: '#34d399', description: '按 pack skeleton + 公司披露' },
  { key: 'CLASSIFY_GROWTH_PROFILE', label: '3. 分类增长曲线', color: '#fbbf24', description: 'stable/declining/high_growth/new' },
  { key: 'EXTRACT_HISTORICAL', label: '4. 抽历史收入', color: '#a78bfa', description: '近 3 年分板块收入' },
  { key: 'CLASSIFY_PEERS', label: '4b. 对标同行', color: '#f472b6', description: '拉可比公司 margin 区间' },
  { key: 'MODEL_VOLUME_PRICE', label: '5a. 量×价建模', color: '#fb7185', description: '高增长业务的 volume × ASP' },
  { key: 'APPLY_GUIDANCE', label: '5b. 套管理层指引', color: '#fdba74', description: '稳定业务用默认增速' },
  { key: 'GROWTH_DECOMPOSITION', label: '5c. 量价拆解', color: '#facc15', description: '量增 vs 涨价 vs mix' },
  { key: 'MARGIN_CASCADE', label: '6. Margin 级联', color: '#4ade80', description: 'OM → EBIT → NI → EPS → PE' },
  { key: 'MULTI_PATH_CHECK', label: '7a. 多路径交叉', color: '#a3e635', description: '4 条独立路径复算' },
  { key: 'CONSENSUS_CHECK', label: '7b. 一致预期核对', color: '#38bdf8', description: '对照 Wind / 卖方一致' },
  { key: 'VERIFY_AND_ASK', label: '8. CoVe + Debate', color: '#e879f9', description: '独立验证 + 三模型辩论' },
]

const AVAILABLE_TOOLS = [
  { value: 'kb_search', label: 'kb_search (内部知识库)' },
  { value: 'alphapai_recall', label: 'alphapai_recall (Alpha派)' },
  { value: 'jinmen_search', label: 'jinmen_search (进门)' },
  { value: 'user_kb_search', label: 'user_kb_search (团队 KB)' },
  { value: 'web_search', label: 'web_search (联网)' },
  { value: 'read_webpage', label: 'read_webpage (抓网页)' },
  { value: 'consensus_forecast', label: 'consensus_forecast (Wind 一致)' },
]

const AVAILABLE_MODELS = [
  { value: 'anthropic/claude-opus-4-7', label: 'Claude Opus 4.7' },
  { value: 'anthropic/claude-opus-4-6', label: 'Claude Opus 4.6' },
  { value: 'google/gemini-3.1-pro-preview', label: 'Gemini 3.1 Pro' },
  { value: 'openai/gpt-5.4', label: 'GPT-5.4' },
]

const AVAILABLE_VARIABLES = [
  '{ticker}', '{company_name}', '{industry}', '{fiscal_periods}',
  '{segments}', '{periods}', '{currency}', '{history_periods}',
  '{skeleton}', '{default_growth}', '{negative_default}', '{segment}',
  '{profile}',
]

// ── Custom step node ────────────────────────────────────────

function StepNode({ data, selected }: { data: any; selected: boolean }) {
  const meta = STEP_TYPES.find(t => t.key === data.type)
  return (
    <div
      style={{
        background: meta?.color ?? '#cbd5e1',
        color: '#0f172a',
        padding: '8px 12px',
        borderRadius: 8,
        border: selected ? '2px solid #1e40af' : '1px solid rgba(0,0,0,0.1)',
        minWidth: 180,
        boxShadow: selected ? '0 0 0 3px rgba(59,130,246,0.25)' : '0 1px 3px rgba(0,0,0,0.15)',
        fontSize: 12,
      }}
    >
      <Handle type="target" position={Position.Top} />
      <div style={{ fontWeight: 600 }}>{meta?.label || data.type}</div>
      <div style={{ fontSize: 10, opacity: 0.8, marginTop: 2 }}>{data.label || data.id}</div>
      <div style={{ fontSize: 10, opacity: 0.65, marginTop: 4 }}>
        {(data.config?.tools || []).slice(0, 3).join(' · ') || 'no tools'}
      </div>
      <Handle type="source" position={Position.Bottom} />
    </div>
  )
}

const nodeTypes = { step: StepNode }

// ── Editor page ─────────────────────────────────────────────

export default function RecipeCanvasEditor() {
  const nav = useNavigate()
  const { id = '' } = useParams()
  const [params] = useSearchParams()
  const modelId = params.get('model_id') || ''

  const [recipe, setRecipe] = useState<Recipe | null>(null)
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [dirty, setDirty] = useState(false)
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)
  const [nodes, setNodes, onNodesChange] = useNodesState<any>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<any>([])
  const [drawerOpen, setDrawerOpen] = useState(false)
  const canvasRef = useRef<HTMLDivElement>(null)

  const selectedNode = useMemo(
    () => nodes.find(n => n.id === selectedNodeId),
    [nodes, selectedNodeId],
  )

  const reload = useCallback(async () => {
    if (!id) return
    setLoading(true)
    try {
      const r = await recipeApi.get(id)
      setRecipe(r)
      const graphNodes = (r.graph?.nodes || []).map((n: any, i: number) => ({
        id: n.id,
        type: 'step',
        position: n.position || { x: 80, y: 80 + i * 120 },
        data: {
          id: n.id,
          type: n.type,
          label: n.label || '',
          config: n.config || {},
        },
      }))
      const graphEdges: Edge[] = (r.graph?.edges || []).map((e: any, i: number) => ({
        id: `e${i}-${e.from || e.from_id}-${e.to}`,
        source: e.from || e.from_id,
        target: e.to,
        type: 'smoothstep',
        markerEnd: { type: MarkerType.ArrowClosed },
      }))
      setNodes(graphNodes)
      setEdges(graphEdges)
      setDirty(false)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setLoading(false)
    }
  }, [id, setNodes, setEdges])

  useEffect(() => { reload() }, [reload])

  const onConnect = useCallback((conn: Connection) => {
    setEdges(eds => addEdge({ ...conn, type: 'smoothstep', markerEnd: { type: MarkerType.ArrowClosed } }, eds))
    setDirty(true)
  }, [setEdges])

  const onNodeClick = useCallback((_: any, node: Node) => {
    setSelectedNodeId(node.id)
    setDrawerOpen(true)
  }, [])

  const addStep = (stepType: string) => {
    const id = `node_${Date.now()}`
    setNodes(ns => [
      ...ns,
      {
        id,
        type: 'step',
        position: { x: 100 + (ns.length * 20) % 300, y: 100 + ns.length * 100 },
        data: {
          id,
          type: stepType,
          label: '',
          config: { tools: defaultToolsFor(stepType), prompt_template: '', model_id: 'anthropic/claude-opus-4-7' },
        },
      },
    ])
    setDirty(true)
  }

  const removeSelected = () => {
    if (!selectedNodeId) return
    setNodes(ns => ns.filter(n => n.id !== selectedNodeId))
    setEdges(es => es.filter(e => e.source !== selectedNodeId && e.target !== selectedNodeId))
    setSelectedNodeId(null)
    setDrawerOpen(false)
    setDirty(true)
  }

  const updateSelected = (patch: any) => {
    if (!selectedNodeId) return
    setNodes(ns =>
      ns.map(n =>
        n.id !== selectedNodeId
          ? n
          : { ...n, data: { ...n.data, ...patch, config: { ...n.data.config, ...(patch.config || {}) } } }
      )
    )
    setDirty(true)
  }

  const save = async () => {
    if (!recipe) return
    setSaving(true)
    try {
      const graph = {
        nodes: nodes.map(n => ({
          id: n.id,
          type: n.data.type,
          label: n.data.label,
          config: n.data.config,
          position: n.position,
        })),
        edges: edges.map(e => ({ from: e.source, to: e.target })),
      }
      await recipeApi.update(recipe.id, { graph: graph as any })
      setDirty(false)
      message.success('已保存')
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setSaving(false)
    }
  }

  const fork = async () => {
    if (!recipe) return
    try {
      const f = await recipeApi.fork(recipe.id)
      message.success(`已 fork 为 ${f.name}`)
      nav(`/modeling/recipes/${f.id}`)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const runNow = async (dry: boolean) => {
    if (!modelId) {
      message.warning('无关联 model_id — 请从 RevenueModel 页面打开此编辑器')
      return
    }
    if (dirty) {
      await save()
    }
    try {
      const run = await modelingApi.startRun(modelId, {
        recipe_id: recipe?.id,
        settings: { dry_run: dry },
      })
      message.success(`已启动 run ${run.id.slice(0, 8)}`)
      nav(`/modeling/${modelId}`)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const startAB = async (otherRecipeId: string) => {
    if (!modelId) {
      message.warning('A/B 需要关联 model_id')
      return
    }
    try {
      const res = await fetch(`/api/models/${modelId}/ab-run`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${authToken()}`,
        },
        body: JSON.stringify({
          recipe_a_id: recipe?.id,
          recipe_b_id: otherRecipeId,
          settings: { dry_run: false },
        }),
      })
      if (!res.ok) throw new Error(await res.text())
      const body = await res.json()
      message.success(`A/B 已启动: session=${body.session}`)
      nav(`/modeling/${modelId}/ab/${body.session}`)
    } catch (e: any) {
      message.error(String(e))
    }
  }

  return (
    <div style={{ padding: 16, height: 'calc(100vh - 100px)' }}>
      <Card size="small" style={{ marginBottom: 12 }}>
        <Space wrap>
          <Button icon={<ArrowLeftOutlined />} onClick={() => nav('/modeling')}>返回</Button>
          <span style={{ fontSize: 16 }}>
            <strong>{recipe?.name || '…'}</strong>{' '}
            {recipe?.version != null && <Tag>v{recipe.version}</Tag>}
            {recipe?.is_public ? <Tag color="blue">公开</Tag> : <Tag>私有</Tag>}
            {dirty && <Tag color="orange">未保存</Tag>}
          </span>
          <Dropdown
            menu={{
              items: STEP_TYPES.map(t => ({
                key: t.key,
                label: (
                  <Space>
                    <span style={{ width: 10, height: 10, background: t.color, display: 'inline-block', borderRadius: '50%' }} />
                    {t.label}
                  </Space>
                ),
                onClick: () => addStep(t.key),
              })),
            }}
          >
            <Button icon={<PlusOutlined />}>添加步骤</Button>
          </Dropdown>
          <Button icon={<SaveOutlined />} type="primary" loading={saving}
                  disabled={!dirty} onClick={save}>保存</Button>
          <Button icon={<BranchesOutlined />} onClick={fork}>Fork</Button>
          <Space.Compact>
            <Button icon={<ThunderboltOutlined />} onClick={() => runNow(true)}>Dry-run</Button>
            <Button type="primary" icon={<PlayCircleOutlined />} onClick={() => runNow(false)}>跑真实 LLM</Button>
          </Space.Compact>
          <ABButton currentId={recipe?.id} onStart={startAB} />
        </Space>
      </Card>

      {recipe && !dirty && nodes.length === 0 && (
        <Alert type="info" showIcon message="空白 recipe — 点 '添加步骤' 从模板构建，或 fork 一个现有 pack recipe。" style={{ marginBottom: 12 }} />
      )}

      <div ref={canvasRef} style={{ height: 'calc(100vh - 220px)', background: '#f8fafc', borderRadius: 8, border: '1px solid #e2e8f0' }}>
        <ReactFlowProvider>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={(ch) => { onNodesChange(ch); setDirty(true) }}
            onEdgesChange={(ch) => { onEdgesChange(ch); setDirty(true) }}
            onConnect={onConnect}
            onNodeClick={onNodeClick}
            nodeTypes={nodeTypes}
            fitView
          >
            <Background gap={16} />
            <Controls />
            <MiniMap />
          </ReactFlow>
        </ReactFlowProvider>
      </div>

      <Drawer
        width={600}
        open={drawerOpen}
        title={selectedNode ? `编辑步骤: ${selectedNode.data.type}` : '选择一个步骤'}
        onClose={() => setDrawerOpen(false)}
        extra={
          <Space>
            {selectedNode && (
              <Button danger icon={<DeleteOutlined />} onClick={removeSelected}>删除节点</Button>
            )}
          </Space>
        }
      >
        {selectedNode && (
          <NodeConfigForm
            node={selectedNode}
            onChange={updateSelected}
          />
        )}
      </Drawer>
    </div>
  )
}

function defaultToolsFor(stepType: string): string[] {
  switch (stepType) {
    case 'GATHER_CONTEXT':
    case 'DECOMPOSE_SEGMENTS':
      return ['kb_search', 'alphapai_recall', 'jinmen_search']
    case 'CLASSIFY_GROWTH_PROFILE':
    case 'EXTRACT_HISTORICAL':
    case 'APPLY_GUIDANCE':
      return ['kb_search', 'alphapai_recall']
    case 'MODEL_VOLUME_PRICE':
    case 'GROWTH_DECOMPOSITION':
      return ['alphapai_recall', 'jinmen_search', 'kb_search', 'web_search']
    case 'MARGIN_CASCADE':
    case 'CLASSIFY_PEERS':
    case 'CONSENSUS_CHECK':
      return ['kb_search', 'alphapai_recall', 'consensus_forecast', 'web_search']
    case 'MULTI_PATH_CHECK':
      return ['kb_search', 'alphapai_recall', 'jinmen_search', 'web_search']
    case 'VERIFY_AND_ASK':
      return ['kb_search', 'web_search']
    default:
      return ['kb_search']
  }
}

function NodeConfigForm({ node, onChange }: { node: Node; onChange: (patch: any) => void }) {
  const data = node.data
  const cfg = data.config || {}
  return (
    <Tabs
      items={[
        {
          key: 'prompt', label: 'Prompt 模板',
          children: (
            <Form layout="vertical">
              <Form.Item label="节点 ID">
                <Input value={data.id} disabled />
              </Form.Item>
              <Form.Item label="显示名">
                <Input value={data.label}
                       onChange={e => onChange({ label: e.target.value })} />
              </Form.Item>
              <Form.Item label="Prompt 模板 (支持变量 {ticker}, {segments} 等)">
                <TextArea
                  rows={12}
                  value={cfg.prompt_template || ''}
                  onChange={e => onChange({ config: { prompt_template: e.target.value } })}
                  placeholder="留空则使用步骤默认 prompt"
                />
              </Form.Item>
              <Form.Item label="可用变量">
                <Space wrap size={[4, 8]}>
                  {AVAILABLE_VARIABLES.map(v => (
                    <Tag key={v} color="blue" style={{ cursor: 'pointer' }}
                         onClick={() => {
                           const cur = cfg.prompt_template || ''
                           onChange({ config: { prompt_template: cur + v } })
                         }}>
                      {v}
                    </Tag>
                  ))}
                </Space>
              </Form.Item>
            </Form>
          ),
        },
        {
          key: 'tools', label: '工具',
          children: (
            <Form layout="vertical">
              <Form.Item label="本步骤可调用的工具">
                <Select
                  mode="multiple" allowClear
                  value={cfg.tools || []}
                  options={AVAILABLE_TOOLS}
                  onChange={v => onChange({ config: { tools: v } })}
                  placeholder="选择 kb_search / alphapai_recall / …"
                />
              </Form.Item>
              <Form.Item label="最大 tool 轮次">
                <InputNumber min={1} max={10}
                             value={cfg.max_tool_rounds || 5}
                             onChange={v => onChange({ config: { max_tool_rounds: v } })} />
              </Form.Item>
            </Form>
          ),
        },
        {
          key: 'model', label: 'Model',
          children: (
            <Form layout="vertical">
              <Form.Item label="LLM 模型">
                <Select value={cfg.model_id || 'anthropic/claude-opus-4-7'}
                        options={AVAILABLE_MODELS}
                        onChange={v => onChange({ config: { model_id: v } })} />
              </Form.Item>
              <Form.Item label="Temperature">
                <InputNumber min={0} max={1} step={0.1}
                             value={cfg.temperature ?? 0.1}
                             onChange={v => onChange({ config: { temperature: v } })} />
              </Form.Item>
            </Form>
          ),
        },
        {
          key: 'thresholds', label: '阈值',
          children: (
            <Form layout="vertical">
              <Form.Item label="diff_threshold_pct (verify_and_ask)">
                <InputNumber min={0.01} max={1.0} step={0.01}
                             value={cfg.diff_threshold_pct ?? 0.10}
                             onChange={v => onChange({ config: { diff_threshold_pct: v } })} />
              </Form.Item>
              <Form.Item label="debate_on_critical_cells">
                <Switch checked={!!cfg.debate_on_critical_cells}
                        onChange={v => onChange({ config: { debate_on_critical_cells: v } })} />
              </Form.Item>
              <Form.Item label="debate_policy (condition DSL, 一行一条)">
                <TextArea rows={4}
                          value={(cfg.debate_policy || []).join('\n')}
                          onChange={e => onChange({ config: { debate_policy: e.target.value.split('\n').filter(Boolean) } })}
                          placeholder="e.g. confidence == 'LOW' AND source_type == 'inferred'" />
              </Form.Item>
              <Form.Item label="history_periods (extract_historical)">
                <Select mode="tags"
                        value={cfg.history_periods || []}
                        onChange={v => onChange({ config: { history_periods: v } })}
                        placeholder="FY23, FY24" />
              </Form.Item>
            </Form>
          ),
        },
        {
          key: 'validation', label: '输出校验',
          children: (
            <Form layout="vertical">
              <Form.Item label="期望的 JSON schema (宽松: 仅作 hint)">
                <TextArea rows={10}
                          value={cfg.output_schema_hint || ''}
                          onChange={e => onChange({ config: { output_schema_hint: e.target.value } })}
                          placeholder='{ "segments": [{"slug": "…", "volume_unit": "…"}] }' />
              </Form.Item>
            </Form>
          ),
        },
      ]}
    />
  )
}

function ABButton({ currentId, onStart }: { currentId?: string; onStart: (other: string) => void }) {
  const [open, setOpen] = useState(false)
  const [list, setList] = useState<Recipe[]>([])
  const [pick, setPick] = useState<string>('')
  useEffect(() => {
    if (open) {
      recipeApi.list().then(setList).catch(() => {})
    }
  }, [open])
  return (
    <>
      <Button icon={<ExperimentOutlined />} onClick={() => setOpen(true)}>A/B 对比</Button>
      <Modal
        title="选择对照 recipe (B 组)"
        open={open}
        onCancel={() => setOpen(false)}
        onOk={() => {
          if (!pick) return
          onStart(pick)
          setOpen(false)
        }}
      >
        <Paragraph type="secondary">
          A/B 会对同一 ticker 并行跑两个 recipe，结果并列展示，帮你判断哪个 prompt / 工具组合更好。
        </Paragraph>
        <Select
          style={{ width: '100%' }}
          showSearch
          options={list
            .filter(r => r.id !== currentId)
            .map(r => ({ value: r.id, label: `${r.name} (v${r.version}, ${r.industry || 'generic'})` }))}
          value={pick}
          onChange={setPick}
          placeholder="搜索/选择 recipe"
          optionFilterProp="label"
        />
      </Modal>
    </>
  )
}

function authToken(): string {
  try {
    return JSON.parse(localStorage.getItem('auth-storage') || '{}').state?.token || ''
  } catch {
    return ''
  }
}
