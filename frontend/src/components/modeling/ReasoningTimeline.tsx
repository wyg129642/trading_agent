/**
 * ReasoningTimeline — renders a ProvenanceTrace as a human-readable timeline.
 *
 * Top-trading-firm best practice: analysts won't trust a number they can't
 * trace. This component converts the raw step log into a researcher-friendly
 * narrative — "first we searched X, got 5 results, read the top one,
 * extracted 12% from paragraph 3, then the LLM reasoned: ...".
 *
 * The props are kept simple so this can be reused in any drawer/page that has
 * access to a provenance trace.
 */
import { Alert, Empty, Space, Tag, Timeline, Tooltip, Typography } from 'antd'
import {
  ApiOutlined, BulbOutlined, DatabaseOutlined, FileSearchOutlined,
  GlobalOutlined, LinkOutlined, SearchOutlined, ThunderboltOutlined,
} from '@ant-design/icons'
import type { ProvenanceTrace } from '../../services/modeling'

const { Text } = Typography

interface Props {
  trace: ProvenanceTrace | null
  /** Cell citations — used to link `[N]` mentions back to sources below. */
  citations?: Array<{
    index?: number
    title?: string
    url?: string
    snippet?: string
  }>
}

/** Map a step_type / tool name to an icon + colour for the timeline dot. */
function stepGlyph(step: any): { icon: React.ReactNode; color: string; label: string } {
  const type = (step?.step_type || '').toLowerCase()
  const tool = (step?.tool || '').toLowerCase()
  if (tool.includes('web_search') || tool.includes('search')) {
    return { icon: <GlobalOutlined />, color: 'blue', label: 'web search' }
  }
  if (tool.includes('read_webpage') || tool.includes('fetch')) {
    return { icon: <LinkOutlined />, color: 'geekblue', label: 'read webpage' }
  }
  if (tool.includes('kb_search') || tool.includes('user_kb')) {
    return { icon: <DatabaseOutlined />, color: 'purple', label: 'knowledge base' }
  }
  if (tool.includes('alphapai') || tool.includes('jinmen') || tool.includes('consensus')) {
    return { icon: <ApiOutlined />, color: 'magenta', label: tool }
  }
  if (type === 'llm_call' || type === 'llm') {
    return { icon: <BulbOutlined />, color: 'orange', label: 'LLM reasoning' }
  }
  if (type === 'tool_status') {
    return {
      icon: <ThunderboltOutlined />,
      color: step.status === 'completed' ? 'green' : 'gray',
      label: `tool ${step.status || 'status'}`,
    }
  }
  if (type === 'search_status') {
    return { icon: <SearchOutlined />, color: 'blue', label: 'search' }
  }
  if (type === 'read_status') {
    return { icon: <FileSearchOutlined />, color: 'geekblue', label: 'reading URL' }
  }
  return { icon: <BulbOutlined />, color: 'blue', label: type || 'step' }
}

function truncate(s: any, n = 360): string {
  if (!s) return ''
  const str = typeof s === 'string' ? s : JSON.stringify(s)
  return str.length > n ? `${str.slice(0, n)}…` : str
}

export default function ReasoningTimeline({ trace, citations }: Props) {
  if (!trace) {
    return (
      <Empty
        image={Empty.PRESENTED_IMAGE_SIMPLE}
        description="本单元格没有 provenance 记录 — 可能是手动录入或从公式计算得出。"
      />
    )
  }
  const steps = trace.steps || []
  if (steps.length === 0) {
    return (
      <Empty
        image={Empty.PRESENTED_IMAGE_SIMPLE}
        description="Trace 为空(未记录任何步骤)。"
      />
    )
  }

  // Build a flat list of unique search queries + URLs the model touched,
  // so researchers can see the full "investigation trail" at a glance.
  const searchQueries: string[] = []
  const urlsRead: string[] = []
  for (const s of steps) {
    const q = (s as any)?.query_preview || (s as any)?.query
    if (q && !searchQueries.includes(q)) searchQueries.push(q)
    const ulist = (s as any)?.urls_read || []
    for (const u of ulist) if (u && !urlsRead.includes(u)) urlsRead.push(u)
    const u2 = (s as any)?.url
    if (u2 && !urlsRead.includes(u2)) urlsRead.push(u2)
  }

  return (
    <div>
      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 12 }}
        message={(
          <Space size="middle" wrap>
            <span><strong>{steps.length}</strong> 推理步骤</span>
            <span><strong>{trace.total_tokens.toLocaleString()}</strong> tokens</span>
            <span><strong>{(trace.total_latency_ms / 1000).toFixed(1)}s</strong></span>
            {searchQueries.length > 0 && (
              <span>搜索 <strong>{searchQueries.length}</strong> 次</span>
            )}
            {urlsRead.length > 0 && (
              <span>读取 <strong>{urlsRead.length}</strong> 个来源</span>
            )}
          </Space>
        )}
      />

      <Timeline style={{ marginTop: 8 }}>
        {steps.map((raw: any, i: number) => {
          const g = stepGlyph(raw)
          const stepModel = raw.model || raw.model_id
          const toolCalls = raw.tool_calls as Array<{ tool: string; status?: string }> | undefined
          const searchQueriesInStep = (raw.search_queries || []) as string[]
          const urlsInStep = (raw.urls_read || []) as string[]
          return (
            <Timeline.Item
              key={i}
              color={g.color}
              dot={g.icon as any}
            >
              <Space direction="vertical" size={4} style={{ width: '100%' }}>
                <Space wrap>
                  <strong>{g.label}</strong>
                  {stepModel && <Tag color="cyan">{stepModel}</Tag>}
                  {raw.finish_reason && (
                    <Tag color={raw.finish_reason === 'stop' ? 'green' : 'orange'}>
                      {raw.finish_reason}
                    </Tag>
                  )}
                  {raw.citation_count != null && (
                    <Tag color="gold">📚 {raw.citation_count}</Tag>
                  )}
                </Space>

                {/* Query / URL / snippet preview */}
                {raw.query_preview && (
                  <div style={{ color: '#475569', fontSize: 12 }}>
                    <SearchOutlined style={{ marginRight: 4 }} />
                    <Text code style={{ fontSize: 12 }}>{truncate(raw.query_preview, 280)}</Text>
                  </div>
                )}
                {raw.query && raw.query !== raw.query_preview && (
                  <div style={{ color: '#475569', fontSize: 12 }}>
                    <SearchOutlined style={{ marginRight: 4 }} />{truncate(raw.query, 180)}
                  </div>
                )}
                {raw.url && (
                  <div style={{ fontSize: 12 }}>
                    <LinkOutlined style={{ marginRight: 4 }} />
                    <a href={raw.url} target="_blank" rel="noreferrer">{truncate(raw.url, 110)}</a>
                  </div>
                )}

                {/* Per-tool calls within a single LLM round */}
                {toolCalls && toolCalls.length > 0 && (
                  <Space size={4} wrap style={{ marginTop: 2 }}>
                    {toolCalls.slice(0, 10).map((tc, idx) => (
                      <Tooltip key={idx} title={tc.status}>
                        <Tag
                          color={tc.status === 'completed' || tc.status === 'success'
                            ? 'green' : tc.status === 'error' ? 'red' : 'default'}
                          style={{ marginRight: 0 }}
                        >
                          {tc.tool}
                        </Tag>
                      </Tooltip>
                    ))}
                    {toolCalls.length > 10 && <span style={{ fontSize: 11 }}>+{toolCalls.length - 10} more</span>}
                  </Space>
                )}

                {/* In-step search queries + URLs */}
                {searchQueriesInStep.length > 0 && (
                  <div style={{ fontSize: 12, color: '#475569' }}>
                    <strong>本轮搜索:</strong> {searchQueriesInStep.slice(0, 3).map((q, idx) => (
                      <Tag key={idx} style={{ marginLeft: 4 }}>{truncate(q, 40)}</Tag>
                    ))}
                    {searchQueriesInStep.length > 3 && (
                      <span>+{searchQueriesInStep.length - 3}</span>
                    )}
                  </div>
                )}
                {urlsInStep.length > 0 && (
                  <div style={{ fontSize: 12, color: '#475569' }}>
                    <strong>本轮读取:</strong> {urlsInStep.slice(0, 3).map((u, idx) => (
                      <a
                        key={idx}
                        href={u}
                        target="_blank"
                        rel="noreferrer"
                        style={{ marginLeft: 6, fontSize: 11 }}
                      >
                        {truncate(u.replace(/^https?:\/\//, ''), 32)}
                      </a>
                    ))}
                  </div>
                )}

                {/* Response / reasoning preview */}
                {raw.response_preview && (
                  <div style={{
                    background: '#f8fafc', borderLeft: '3px solid #60a5fa',
                    padding: '6px 10px', borderRadius: 2, fontSize: 12,
                    color: '#334155', whiteSpace: 'pre-wrap', maxHeight: 140,
                    overflow: 'auto',
                  }}>
                    {truncate(raw.response_preview, 600)}
                  </div>
                )}
                {raw.result_preview && !raw.response_preview && (
                  <div style={{ fontSize: 12, color: '#334155' }}>
                    {truncate(raw.result_preview, 280)}
                  </div>
                )}
                {raw.parse_error && (
                  <div style={{ fontSize: 12, color: '#dc2626' }}>
                    ⚠ 解析错误: {truncate(raw.parse_error, 120)}
                  </div>
                )}

                {/* Token + latency footer */}
                <div style={{ fontSize: 11, color: '#94a3b8' }}>
                  {raw.tokens ? `${raw.tokens.toLocaleString()} tokens` : ''}
                  {raw.tokens && raw.latency ? ' · ' : ''}
                  {raw.latency ? `${raw.latency}ms` : ''}
                  {raw.dry_run && <Tag color="default" style={{ marginLeft: 6 }}>DRY</Tag>}
                </div>
              </Space>
            </Timeline.Item>
          )
        })}
      </Timeline>

      {/* Citation roll-up — the "sources used" footer */}
      {citations && citations.length > 0 && (
        <div style={{ marginTop: 12, padding: 10, background: '#f8fafc', borderRadius: 4 }}>
          <div style={{ fontSize: 12, color: '#64748b', marginBottom: 6 }}>
            <strong>调用证据来源:</strong>
          </div>
          <Space direction="vertical" size={4} style={{ width: '100%' }}>
            {citations.slice(0, 12).map((c, i) => {
              const docId = (c as any).source_id || (c as any).doc_id
              const viewer = docId
                ? `/modeling/kb-viewer?doc_id=${encodeURIComponent(docId)}${c.snippet ? `&snippet=${encodeURIComponent((c.snippet || '').slice(0, 300))}` : ''}`
                : null
              return (
                <div key={i} style={{ fontSize: 12 }}>
                  <Tag color="gold">[{c.index ?? i + 1}]</Tag>
                  {viewer
                    ? <a href={viewer} target="_blank" rel="noreferrer">📖 {c.title || docId}</a>
                    : c.url
                      ? <a href={c.url} target="_blank" rel="noreferrer">{c.title || c.url}</a>
                      : <span>{c.title}</span>}
                </div>
              )
            })}
          </Space>
        </div>
      )}
    </div>
  )
}
