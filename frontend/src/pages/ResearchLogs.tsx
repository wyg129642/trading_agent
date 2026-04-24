/**
 * Admin-only: visualize the AI research assistant's full research lifecycle.
 *
 * For each session we show the user query, per-model timeline (rounds,
 * reasoning, tool calls with complete arguments, tool result previews,
 * webpage reads, citations), aggregate summary, and the final reply — all
 * drawn from MongoDB (collection `research_sessions` in DB
 * `research-agent-interaction-process-all-accounts`).
 */
import { useEffect, useMemo, useState } from 'react'
import {
  Card, Table, Tag, Drawer, Typography, Input, Select, Space, Button,
  message, Tabs, Collapse, Descriptions, Tooltip, Statistic, Row, Col,
  Empty, Alert, Timeline,
} from 'antd'
import {
  ReloadOutlined, SearchOutlined, ClockCircleOutlined,
  ThunderboltOutlined, CheckCircleOutlined, CloseCircleOutlined,
  DatabaseOutlined, RobotOutlined, GlobalOutlined, ReadOutlined,
  BranchesOutlined, EyeOutlined,
} from '@ant-design/icons'
import api from '../services/api'
import dayjs from 'dayjs'

const { Text, Paragraph } = Typography

// ─── Types ─────────────────────────────────────────────────

interface Summary {
  rounds_used?: number
  tool_calls_total?: number
  tool_call_names?: string[]
  search_queries?: string[]
  urls_found?: string[]
  urls_read?: string[]
  citations?: any[]
  final_content_len?: number
  total_tokens?: number
}

interface SessionListItem {
  trace_id: string
  user_id: string
  username: string
  conversation_id: string
  query: string
  models_requested: string[]
  mode: string
  web_search?: string
  alphapai_enabled?: boolean
  jinmen_enabled?: boolean
  kb_enabled?: boolean
  tools_enabled?: string[]
  status: string
  total_elapsed_ms?: number
  summary?: Summary
  created_at: string
  updated_at: string
}

interface ToolCall {
  tool_name: string
  round: number
  arguments: Record<string, any>
  result_preview?: string
  result_len?: number
  elapsed_ms?: number
  error?: string | null
  ts?: string
}

interface Round {
  round: number
  mode: string
  tool_names?: string[]
  messages_preview?: any[]
  reasoning?: string
  tool_calls?: ToolCall[]
  tool_calls_declared?: any[]
  started_at?: string
  latency_ms?: number | null
  finish_reason?: string | null
  response_preview?: string
}

interface ModelLog {
  model_id: string
  model_name?: string
  status?: string
  rounds?: Round[]
  tool_results?: ToolCall[]
  search_calls?: any[]
  webpage_reads?: any[]
  events?: any[]
  final_content?: string
  final_content_len?: number
  total_tokens?: number
  latency_ms?: number
  error?: string | null
  citations?: any[]
  started_at?: string
  ended_at?: string
}

interface SessionDetail extends SessionListItem {
  system_prompt_preview?: string
  system_prompt_len?: number
  history_len?: number
  initial_messages?: any[]
  models: Record<string, ModelLog>
  attachments?: any[]
}

interface Stats {
  window_days: number
  total_requests: number
  running_requests: number
  per_user: {
    username: string
    requests: number
    tool_calls_total: number
    total_tokens: number
    total_elapsed_ms: number
  }[]
  tool_usage: { tool: string; count: number }[]
  model_usage: { model: string; count: number }[]
}

// ─── Helpers ───────────────────────────────────────────────

const MODEL_COLORS: Record<string, string> = {
  'anthropic/claude-opus-4-6': '#d97706',
  'google/gemini-3.1-pro-preview': '#2563eb',
  'openai/gpt-5.4': '#10b981',
}

const TOOL_ICON: Record<string, any> = {
  web_search: <GlobalOutlined />,
  read_webpage: <ReadOutlined />,
  alphapai_recall: <DatabaseOutlined />,
  jinmen_search: <DatabaseOutlined />,
  jinmen_analyst_comments: <DatabaseOutlined />,
  jinmen_roadshow: <DatabaseOutlined />,
  jinmen_announcements: <DatabaseOutlined />,
  jinmen_foreign_reports: <DatabaseOutlined />,
  jinmen_business_segments: <DatabaseOutlined />,
  kb_search: <DatabaseOutlined />,
}

const fmtDuration = (ms?: number) => {
  if (!ms && ms !== 0) return '—'
  if (ms < 1000) return `${ms}ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`
  return `${Math.floor(ms / 60_000)}m${Math.round((ms % 60_000) / 1000)}s`
}

const parseArgs = (args: any): Record<string, any> => {
  if (typeof args === 'object' && args !== null) return args
  if (typeof args === 'string') {
    try {
      return JSON.parse(args)
    } catch {
      return { raw: args }
    }
  }
  return {}
}

// ─── Subcomponents ────────────────────────────────────────

function ArgsRender({ args }: { args: any }) {
  const parsed = parseArgs(args)
  return (
    <pre style={{
      background: '#f8fafc', borderRadius: 4, padding: 10, fontSize: 12,
      whiteSpace: 'pre-wrap', wordBreak: 'break-word', margin: 0,
      maxHeight: 300, overflow: 'auto',
    }}>
      {JSON.stringify(parsed, null, 2)}
    </pre>
  )
}

function ToolCallCard({ call }: { call: ToolCall }) {
  const args = parseArgs(call.arguments)
  const icon = TOOL_ICON[call.tool_name] ?? <ThunderboltOutlined />
  const isWebRead = call.tool_name === 'read_webpage'
  const isSearch = call.tool_name === 'web_search'
  const title = (
    <Space>
      {icon}
      <Text strong>{call.tool_name}</Text>
      {isSearch && args.query_cn && <Tag color="geekblue">CN: {String(args.query_cn).slice(0, 50)}</Tag>}
      {isSearch && args.query_en && <Tag color="blue">EN: {String(args.query_en).slice(0, 50)}</Tag>}
      {isWebRead && args.url && (
        <Tooltip title={args.url}>
          <Tag color="purple"><EyeOutlined /> {new URL(args.url).hostname}</Tag>
        </Tooltip>
      )}
      <Tag>{fmtDuration(call.elapsed_ms)}</Tag>
      <Tag>{call.result_len ?? 0}ch</Tag>
      {call.error && <Tag color="error"><CloseCircleOutlined /> error</Tag>}
    </Space>
  )

  return (
    <Collapse ghost size="small" style={{ marginBottom: 6 }}>
      <Collapse.Panel key="1" header={title}>
        <div style={{ fontSize: 12, color: '#64748b', marginBottom: 4 }}>Arguments</div>
        <ArgsRender args={args} />
        <div style={{ fontSize: 12, color: '#64748b', margin: '8px 0 4px' }}>Result {(call.result_len ?? 0).toLocaleString()} chars</div>
        <pre style={{
          background: '#0f172a', color: '#e2e8f0', borderRadius: 4, padding: 10,
          fontSize: 12, whiteSpace: 'pre-wrap', wordBreak: 'break-word',
          maxHeight: 420, overflow: 'auto',
        }}>
          {call.result_preview || '(empty)'}
        </pre>
        {call.error && (
          <Alert type="error" message={`Error: ${call.error}`} style={{ marginTop: 6 }} />
        )}
      </Collapse.Panel>
    </Collapse>
  )
}

function RoundPanel({ round, modelId }: { round: Round; modelId: string }) {
  const toolCalls = round.tool_calls || []
  return (
    <Card
      size="small"
      title={
        <Space>
          <BranchesOutlined style={{ color: MODEL_COLORS[modelId] }} />
          <Text strong>Round {round.round}</Text>
          <Tag>{round.mode}</Tag>
          {round.tool_names && round.tool_names.length > 0 && (
            <Tag color="blue">{round.tool_names.length} tools available</Tag>
          )}
          {round.finish_reason && <Tag>finish: {round.finish_reason}</Tag>}
          {toolCalls.length > 0 && <Tag color="green">{toolCalls.length} tool calls</Tag>}
        </Space>
      }
      style={{ marginBottom: 10, borderColor: '#e2e8f0' }}
    >
      {round.reasoning && (
        <>
          <div style={{ fontSize: 12, color: '#64748b', marginBottom: 4 }}>Model Reasoning</div>
          <Paragraph style={{
            background: '#fff7ed', padding: 8, borderRadius: 4,
            fontSize: 13, whiteSpace: 'pre-wrap', marginBottom: 12,
          }}>
            {round.reasoning}
          </Paragraph>
        </>
      )}
      {round.messages_preview && round.messages_preview.length > 0 && (
        <Collapse ghost size="small" style={{ marginBottom: 10 }}>
          <Collapse.Panel
            key="msgs"
            header={<Text type="secondary" style={{ fontSize: 12 }}>
              Messages sent to LLM this round ({round.messages_preview.length})
            </Text>}
          >
            <pre style={{
              background: '#f8fafc', borderRadius: 4, padding: 10, fontSize: 11,
              whiteSpace: 'pre-wrap', maxHeight: 360, overflow: 'auto', margin: 0,
            }}>
              {JSON.stringify(round.messages_preview, null, 2)}
            </pre>
          </Collapse.Panel>
        </Collapse>
      )}
      {toolCalls.length === 0 && !round.reasoning && (
        <Text type="secondary" style={{ fontSize: 12 }}>
          (No tool calls in this round — model produced the final answer directly.)
        </Text>
      )}
      {toolCalls.map((tc, i) => (
        <ToolCallCard key={i} call={tc} />
      ))}
    </Card>
  )
}

function ModelTimeline({ modelId, log }: { modelId: string; log: ModelLog }) {
  const rounds = log.rounds || []
  const statusColor = log.status === 'done' ? 'success' : log.status === 'error' ? 'error' : 'processing'
  return (
    <div>
      <Descriptions
        size="small"
        column={3}
        bordered
        items={[
          { key: 'status', label: 'Status', children: (
            <Tag color={statusColor}>
              {log.status === 'done' && <CheckCircleOutlined />}{' '}
              {log.status || 'pending'}
            </Tag>
          ) },
          { key: 'rounds', label: 'Rounds', children: rounds.length },
          { key: 'latency', label: 'Latency', children: fmtDuration(log.latency_ms) },
          { key: 'tokens', label: 'Tokens', children: log.total_tokens ?? 0 },
          { key: 'content', label: 'Final content', children: `${log.final_content_len ?? 0} chars` },
          { key: 'citations', label: 'Citations', children: log.citations?.length ?? 0 },
        ]}
        style={{ marginBottom: 12 }}
      />

      {log.error && (
        <Alert type="error" message={`Error: ${log.error}`} style={{ marginBottom: 10 }} />
      )}

      {rounds.map((r) => (
        <RoundPanel key={r.round} round={r} modelId={modelId} />
      ))}

      {log.final_content && (
        <Card size="small" title="Final Response" style={{ marginTop: 10 }}>
          <pre style={{
            background: '#f8fafc', borderRadius: 4, padding: 10, fontSize: 12,
            whiteSpace: 'pre-wrap', wordBreak: 'break-word', margin: 0,
            maxHeight: 500, overflow: 'auto',
          }}>
            {log.final_content}
          </pre>
        </Card>
      )}

      {log.citations && log.citations.length > 0 && (
        <Card size="small" title={`Citations (${log.citations.length})`} style={{ marginTop: 10 }}>
          {log.citations.map((c: any, i: number) => (
            <div key={i} style={{ marginBottom: 6, fontSize: 12 }}>
              <Tag color="blue">[{c.citation_index ?? i + 1}]</Tag>
              <Text>{c.title}</Text>
              {c.url && (
                <>
                  {' — '}
                  <a href={c.url} target="_blank" rel="noopener noreferrer">
                    {c.website || c.url}
                  </a>
                </>
              )}
              {c.date && <Text type="secondary"> · {c.date}</Text>}
            </div>
          ))}
        </Card>
      )}
    </div>
  )
}

// ─── Main page ────────────────────────────────────────────

export default function ResearchLogs() {
  const [items, setItems] = useState<SessionListItem[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(30)
  const [search, setSearch] = useState('')
  const [userFilter, setUserFilter] = useState('')
  const [modelFilter, setModelFilter] = useState<string | undefined>(undefined)
  const [statusFilter, setStatusFilter] = useState<string | undefined>(undefined)
  const [loading, setLoading] = useState(false)
  const [detail, setDetail] = useState<SessionDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [stats, setStats] = useState<Stats | null>(null)
  const [health, setHealth] = useState<any>(null)

  const fetchList = async () => {
    setLoading(true)
    try {
      const res = await api.get('/research-logs/sessions', {
        params: {
          search: search || undefined,
          user: userFilter || undefined,
          model: modelFilter,
          status: statusFilter,
          page,
          page_size: pageSize,
        },
      })
      setItems(res.data.items || [])
      setTotal(res.data.total || 0)
    } catch (e: any) {
      const msg = e.response?.data?.detail || e.message
      message.error(`Failed to load sessions: ${msg}`)
    } finally {
      setLoading(false)
    }
  }

  const fetchStats = async () => {
    try {
      const res = await api.get('/research-logs/stats', { params: { days: 7 } })
      setStats(res.data)
    } catch {
      /* ignore — stats is optional */
    }
  }

  const fetchHealth = async () => {
    try {
      const res = await api.get('/research-logs/health')
      setHealth(res.data)
    } catch {
      setHealth(null)
    }
  }

  const openDetail = async (trace_id: string) => {
    setDrawerOpen(true)
    setDetail(null)
    setDetailLoading(true)
    try {
      const res = await api.get(`/research-logs/sessions/${trace_id}`)
      setDetail(res.data)
    } catch (e: any) {
      message.error(`Failed to load detail: ${e.response?.data?.detail || e.message}`)
    } finally {
      setDetailLoading(false)
    }
  }

  useEffect(() => {
    fetchHealth()
    fetchStats()
  }, [])

  useEffect(() => {
    fetchList()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [page, pageSize, modelFilter, statusFilter])

  const modelOptions = useMemo(() => [
    { value: 'openai/gpt-5.4', label: 'GPT-5.4' },
    { value: 'anthropic/claude-opus-4-6', label: 'Claude Opus 4.6' },
    { value: 'google/gemini-3.1-pro-preview', label: 'Gemini 3.1 Pro' },
  ], [])

  const columns = [
    {
      title: 'Time',
      dataIndex: 'created_at',
      width: 170,
      render: (v: string) => (
        <Tooltip title={dayjs(v).format('YYYY-MM-DD HH:mm:ss')}>
          <span style={{ fontSize: 12 }}>{dayjs(v).format('MM-DD HH:mm:ss')}</span>
        </Tooltip>
      ),
    },
    {
      title: 'User',
      dataIndex: 'username',
      width: 120,
      render: (v: string, r: SessionListItem) => v || r.user_id || '(anon)',
    },
    {
      title: 'Query',
      dataIndex: 'query',
      ellipsis: true,
      render: (v: string) => <Text style={{ fontSize: 13 }}>{v}</Text>,
    },
    {
      title: 'Models',
      dataIndex: 'models_requested',
      width: 230,
      render: (v: string[] = []) => (
        <Space size={[4, 4]} wrap>
          {v.map((m) => (
            <Tag color={MODEL_COLORS[m] || 'default'} key={m} style={{ margin: 0 }}>
              <RobotOutlined /> {m.split('/').pop()}
            </Tag>
          ))}
        </Space>
      ),
    },
    {
      title: 'Tools',
      dataIndex: 'summary',
      width: 110,
      render: (s: Summary = {}) => (
        <Tooltip title={(s.tool_call_names || []).join(', ')}>
          <Tag color="processing">{s.tool_calls_total ?? 0} calls</Tag>
        </Tooltip>
      ),
    },
    {
      title: 'Elapsed',
      dataIndex: 'total_elapsed_ms',
      width: 90,
      render: (v?: number) => <Text style={{ fontSize: 12 }}><ClockCircleOutlined /> {fmtDuration(v)}</Text>,
    },
    {
      title: 'Status',
      dataIndex: 'status',
      width: 90,
      render: (v: string) => {
        const color = v === 'done' ? 'success' : v === 'running' ? 'processing' : 'default'
        return <Tag color={color}>{v}</Tag>
      },
    },
    {
      title: '',
      key: 'actions',
      width: 80,
      render: (_: any, r: SessionListItem) => (
        <Button size="small" type="link" onClick={() => openDetail(r.trace_id)}>
          View
        </Button>
      ),
    },
  ]

  return (
    <div>
      <Typography.Title level={4} style={{ marginBottom: 16 }}>
        AI 研究助手 · 调用日志（管理员）
      </Typography.Title>
      <Typography.Paragraph type="secondary" style={{ marginTop: -8 }}>
        可视化所有账户近期的 AI 研究助手使用记录。每一次对话都记录了三个 LLM 接到用户 query 之后进行的
        所有搜索、工具调用（含完整参数）、网页抓取及最终生成的回复，便于 Claude Code
        在迭代 AI 研究助手模块时查阅和复盘。
      </Typography.Paragraph>

      {health && !health.enabled && (
        <Alert
          type="warning"
          showIcon
          message="Research log recorder is NOT connected"
          description={
            <div>
              <div><b>Target:</b> {health.uri}</div>
              <div><b>DB:</b> {health.db}</div>
              <div><b>Reason:</b> {health.disabled_reason}</div>
              <div style={{ marginTop: 6 }}>
                Set <code>RESEARCH_LOG_MONGO_URI</code> in <code>.env</code> and restart the backend to point at a different Mongo.
              </div>
            </div>
          }
          style={{ marginBottom: 16 }}
        />
      )}

      {stats && (
        <Row gutter={12} style={{ marginBottom: 16 }}>
          <Col span={6}>
            <Card size="small">
              <Statistic title="近 7 天请求" value={stats.total_requests} />
            </Card>
          </Col>
          <Col span={6}>
            <Card size="small">
              <Statistic title="进行中" value={stats.running_requests} valueStyle={{ color: '#2563eb' }} />
            </Card>
          </Col>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title="活跃用户 (Top)"
                value={stats.per_user[0]?.username || '—'}
                suffix={stats.per_user[0] ? ` · ${stats.per_user[0].requests} req` : ''}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card size="small">
              <Statistic
                title="使用最多工具"
                value={stats.tool_usage[0]?.tool || '—'}
                suffix={stats.tool_usage[0] ? ` · ${stats.tool_usage[0].count}` : ''}
              />
            </Card>
          </Col>
        </Row>
      )}

      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap>
          <Input
            placeholder="搜索 query 文本..."
            prefix={<SearchOutlined />}
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            onPressEnter={() => { setPage(1); fetchList() }}
            allowClear
            style={{ width: 240 }}
          />
          <Input
            placeholder="按用户名..."
            value={userFilter}
            onChange={(e) => setUserFilter(e.target.value)}
            onPressEnter={() => { setPage(1); fetchList() }}
            allowClear
            style={{ width: 160 }}
          />
          <Select
            placeholder="模型"
            allowClear
            options={modelOptions}
            value={modelFilter}
            onChange={setModelFilter}
            style={{ width: 180 }}
          />
          <Select
            placeholder="状态"
            allowClear
            options={[
              { value: 'done', label: 'done' },
              { value: 'running', label: 'running' },
            ]}
            value={statusFilter}
            onChange={setStatusFilter}
            style={{ width: 120 }}
          />
          <Button type="primary" onClick={() => { setPage(1); fetchList() }}>查询</Button>
          <Button icon={<ReloadOutlined />} onClick={() => { fetchList(); fetchStats() }}>刷新</Button>
        </Space>
      </Card>

      <Card size="small">
        <Table
          size="small"
          loading={loading}
          rowKey="trace_id"
          dataSource={items}
          columns={columns as any}
          pagination={{
            current: page, pageSize, total,
            showSizeChanger: true,
            showTotal: (t) => `共 ${t} 条`,
            onChange: (p, ps) => { setPage(p); setPageSize(ps) },
          }}
        />
      </Card>

      <Drawer
        title={
          <Space>
            <BranchesOutlined />
            <Text strong>{detail?.query?.slice(0, 80) || 'Loading...'}</Text>
          </Space>
        }
        width="80vw"
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        destroyOnClose
      >
        {detailLoading && <Text>Loading…</Text>}
        {!detailLoading && !detail && <Empty />}
        {!detailLoading && detail && (
          <div>
            <Descriptions bordered size="small" column={2} style={{ marginBottom: 16 }}
              items={[
                { key: 'trace', label: 'Trace ID', children: <Text code>{detail.trace_id}</Text> },
                { key: 'user', label: 'User', children: `${detail.username} (${detail.user_id})` },
                { key: 'conv', label: 'Conversation', children: <Text code>{detail.conversation_id}</Text> },
                { key: 'mode', label: 'Mode', children: <Tag>{detail.mode}</Tag> },
                { key: 'web', label: 'Web search', children: <Tag>{detail.web_search}</Tag> },
                { key: 'tools',
                  label: 'Tools enabled',
                  children: (
                    <Space wrap>
                      {detail.alphapai_enabled && <Tag color="blue">AlphaPai</Tag>}
                      {detail.jinmen_enabled && <Tag color="geekblue">Jinmen</Tag>}
                      {detail.kb_enabled && <Tag color="purple">KB</Tag>}
                      {(detail.tools_enabled || []).map((t) => <Tag key={t}>{t}</Tag>)}
                    </Space>
                  ) },
                { key: 'ts', label: 'Started', children: dayjs(detail.created_at).format('YYYY-MM-DD HH:mm:ss') },
                { key: 'elapsed', label: 'Total elapsed', children: fmtDuration(detail.total_elapsed_ms) },
                { key: 'status', label: 'Status',
                  children: <Tag color={detail.status === 'done' ? 'success' : 'processing'}>{detail.status}</Tag> },
                { key: 'models', label: 'Models',
                  children: <Space wrap>
                    {(detail.models_requested || []).map((m) => (
                      <Tag key={m} color={MODEL_COLORS[m] || 'default'}>{m}</Tag>
                    ))}
                  </Space> },
              ]}
            />

            <Card size="small" title="用户 Query" style={{ marginBottom: 12 }}>
              <Paragraph style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{detail.query}</Paragraph>
            </Card>

            {detail.summary && (
              <Card size="small" title="Aggregate Summary" style={{ marginBottom: 12 }}>
                <Row gutter={16}>
                  <Col span={6}><Statistic title="Rounds" value={detail.summary.rounds_used || 0} /></Col>
                  <Col span={6}><Statistic title="Tool calls" value={detail.summary.tool_calls_total || 0} /></Col>
                  <Col span={6}><Statistic title="URLs read" value={(detail.summary.urls_read || []).length} /></Col>
                  <Col span={6}><Statistic title="Citations" value={(detail.summary.citations || []).length} /></Col>
                </Row>
                {detail.summary.search_queries && detail.summary.search_queries.length > 0 && (
                  <>
                    <Text type="secondary" style={{ fontSize: 12 }}>Search queries</Text>
                    <div style={{ marginTop: 4 }}>
                      {detail.summary.search_queries.map((q, i) => (
                        <Tag key={i} style={{ marginBottom: 4 }}>{q}</Tag>
                      ))}
                    </div>
                  </>
                )}
                {detail.summary.urls_read && detail.summary.urls_read.length > 0 && (
                  <>
                    <Text type="secondary" style={{ fontSize: 12, display: 'block', marginTop: 8 }}>URLs read</Text>
                    <ul style={{ margin: '4px 0 0', paddingLeft: 20, fontSize: 12 }}>
                      {detail.summary.urls_read.slice(0, 20).map((u, i) => (
                        <li key={i}>
                          <a href={u} target="_blank" rel="noopener noreferrer">{u}</a>
                        </li>
                      ))}
                    </ul>
                  </>
                )}
              </Card>
            )}

            {detail.system_prompt_preview && (
              <Collapse size="small" style={{ marginBottom: 12 }}>
                <Collapse.Panel key="sys" header={`System prompt (${detail.system_prompt_len ?? 0} chars)`}>
                  <pre style={{
                    background: '#f8fafc', borderRadius: 4, padding: 10, fontSize: 12,
                    whiteSpace: 'pre-wrap', maxHeight: 360, overflow: 'auto', margin: 0,
                  }}>
                    {detail.system_prompt_preview}
                  </pre>
                </Collapse.Panel>
              </Collapse>
            )}

            {detail.initial_messages && detail.initial_messages.length > 0 && (
              <Collapse size="small" style={{ marginBottom: 12 }}>
                <Collapse.Panel key="msgs" header={`Initial messages payload (${detail.initial_messages.length})`}>
                  <pre style={{
                    background: '#f8fafc', borderRadius: 4, padding: 10, fontSize: 11,
                    whiteSpace: 'pre-wrap', maxHeight: 360, overflow: 'auto', margin: 0,
                  }}>
                    {JSON.stringify(detail.initial_messages, null, 2)}
                  </pre>
                </Collapse.Panel>
              </Collapse>
            )}

            <Tabs
              items={Object.keys(detail.models || {}).map((mid) => {
                const log = detail.models[mid]
                return {
                  key: mid,
                  label: (
                    <Space>
                      <RobotOutlined style={{ color: MODEL_COLORS[mid] }} />
                      <Text>{log.model_name || mid}</Text>
                      <Tag color={log.status === 'done' ? 'success' : log.status === 'error' ? 'error' : 'processing'}>
                        {log.status || 'pending'}
                      </Tag>
                    </Space>
                  ),
                  children: <ModelTimeline modelId={mid} log={log} />,
                }
              })}
            />
          </div>
        )}
      </Drawer>
    </div>
  )
}
