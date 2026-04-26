import { Tag, Typography, Space, Tooltip } from 'antd'
import {
  RobotOutlined,
  ToolOutlined,
  SearchOutlined,
  GlobalOutlined,
  DatabaseOutlined,
  FileTextOutlined,
  WarningOutlined,
  RightOutlined,
  LinkOutlined,
  BulbOutlined,
} from '@ant-design/icons'
import { AuditEvent } from '../../services/chatAudit'

const TYPE_META: Record<string, { color: string; icon: any; label: string }> = {
  REQUEST_START: { color: '#64748b', icon: <RobotOutlined />, label: '请求开始' },
  REQUEST_END: { color: '#64748b', icon: <RobotOutlined />, label: '请求结束' },
  REQUEST_SUMMARY: { color: '#64748b', icon: <RobotOutlined />, label: '请求摘要' },
  MESSAGES_PAYLOAD: { color: '#475569', icon: <FileTextOutlined />, label: '消息载荷' },
  LLM_REQUEST: { color: '#2563eb', icon: <RobotOutlined />, label: 'LLM 请求' },
  LLM_DONE: { color: '#0ea5e9', icon: <RobotOutlined />, label: 'LLM 完成' },
  LLM_RESPONSE_CONTENT: { color: '#0ea5e9', icon: <FileTextOutlined />, label: 'LLM 输出' },
  LLM_FULL_RESPONSE: { color: '#0ea5e9', icon: <FileTextOutlined />, label: 'LLM 最终回复' },
  LLM_ERROR: { color: '#ef4444', icon: <WarningOutlined />, label: 'LLM 错误' },
  LLM_RETRY: { color: '#f59e0b', icon: <WarningOutlined />, label: 'LLM 重试' },
  MODEL_REASONING: { color: '#8b5cf6', icon: <BulbOutlined />, label: '模型思考' },
  TOOL_CALLS_DETECTED: { color: '#10b981', icon: <ToolOutlined />, label: '工具调用决定' },
  TOOL_EXEC_START: { color: '#10b981', icon: <ToolOutlined />, label: '工具开始' },
  TOOL_EXEC_DONE: { color: '#059669', icon: <ToolOutlined />, label: '工具完成' },
  TOOL_TIMEOUT: { color: '#ef4444', icon: <WarningOutlined />, label: '工具超时' },
  SEARCH_KEYWORDS: { color: '#f59e0b', icon: <SearchOutlined />, label: '搜索关键词' },
  SEARCH_ENGINE_CALL: { color: '#f59e0b', icon: <SearchOutlined />, label: '搜索 API 调用' },
  SEARCH_URLS_RETURNED: { color: '#f59e0b', icon: <LinkOutlined />, label: '搜索返回 URL' },
  SEARCH_TOP_RESULTS: { color: '#d97706', icon: <SearchOutlined />, label: '搜索 Top 结果' },
  SEARCH_CACHE_HIT: { color: '#a3a3a3', icon: <SearchOutlined />, label: '搜索缓存命中' },
  WEBPAGE_READ: { color: '#7c3aed', icon: <GlobalOutlined />, label: '网页抓取' },
  KB_REQUEST: { color: '#0891b2', icon: <DatabaseOutlined />, label: '知识库查询' },
  KB_RESULTS: { color: '#0e7490', icon: <DatabaseOutlined />, label: '知识库结果' },
  USER_KB_REQUEST: { color: '#0891b2', icon: <DatabaseOutlined />, label: '个人库查询' },
  USER_KB_RESULTS: { color: '#0e7490', icon: <DatabaseOutlined />, label: '个人库结果' },
  GEMINI_FUNC_CALLS: { color: '#10b981', icon: <ToolOutlined />, label: 'Gemini 函数调用' },
  GEMINI_GROUNDING: { color: '#0ea5e9', icon: <SearchOutlined />, label: 'Gemini Grounding' },
  ALPHAPAI_RESULTS: { color: '#0e7490', icon: <DatabaseOutlined />, label: 'AlphaPai 结果' },
  JINMEN_RESULTS: { color: '#0e7490', icon: <DatabaseOutlined />, label: 'Jinmen 结果' },
}

function truncate(s: string, max = 200) {
  if (!s) return ''
  return s.length > max ? s.slice(0, max) + '…' : s
}

export interface EventCardProps {
  event: AuditEvent
  onClick: () => void
}

/** One-line summary derived from event_type + payload. */
function eventSummary(e: AuditEvent): string {
  const p = e.payload || {}
  switch (e.event_type) {
    case 'LLM_REQUEST':
      return `round=${e.round_num} · ${(p.tools_offered || []).length} tools · ${p.messages_count} msgs`
    case 'LLM_DONE':
      return `tokens=${p.tokens || 0} · len=${p.content_len || 0}${p.error ? ' · ERROR' : ''}`
    case 'LLM_FULL_RESPONSE':
      return truncate(p.content || '', 160)
    case 'LLM_ERROR':
      return truncate(p.error || '', 160)
    case 'TOOL_CALLS_DETECTED': {
      const calls = (p.calls || [])
        .map((c: any) => c.name)
        .join(', ')
      return `${p.count} call(s): ${calls}`
    }
    case 'TOOL_EXEC_START':
      return `${p.tool_name} · ${truncate(JSON.stringify(p.arguments || {}), 200)}`
    case 'TOOL_EXEC_DONE':
      return `${p.tool_name} · result_len=${p.result_len}${p.error ? ' · ERROR' : ''}`
    case 'TOOL_TIMEOUT':
      return `${p.tool_name} · ${p.timeout_s}s`
    case 'SEARCH_KEYWORDS':
      return `cn="${truncate(p.query_cn || '', 80)}" · en="${truncate(p.query_en || '', 80)}"`
    case 'SEARCH_ENGINE_CALL':
      return `${p.engine} · status=${p.status} · ${p.result_count} results`
    case 'SEARCH_URLS_RETURNED':
      return `${p.engine} · ${p.count} URLs`
    case 'SEARCH_TOP_RESULTS':
      return `top ${p.count}`
    case 'WEBPAGE_READ':
      return `${truncate(p.url || '', 120)} · status=${p.status} · ${p.content_len || 0} chars`
    case 'KB_REQUEST':
      return `"${truncate(p.query || '', 100)}" · tickers=${(p.tickers || []).join(',')} · top_k=${p.top_k}`
    case 'KB_RESULTS':
      return `${p.result_count} hits`
    case 'USER_KB_REQUEST':
      return `"${truncate(p.query || '', 100)}" · top_k=${p.top_k}`
    case 'USER_KB_RESULTS':
      return `${p.result_count} hits`
    case 'MODEL_REASONING':
      return truncate(p.text || '', 200)
    case 'REQUEST_SUMMARY':
      return `rounds=${p.rounds_used} · tools=${p.tool_calls_total} · cit=${p.citations_count}`
    case 'MESSAGES_PAYLOAD':
      return `${p.count} messages`
    default:
      return ''
  }
}

export default function EventCard({ event, onClick }: EventCardProps) {
  const meta = TYPE_META[event.event_type] || {
    color: '#64748b',
    icon: <ToolOutlined />,
    label: event.event_type,
  }
  const summary = eventSummary(event)

  return (
    <div
      onClick={onClick}
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: 8,
        padding: '6px 10px',
        borderLeft: `3px solid ${meta.color}`,
        background: '#fafbfc',
        marginBottom: 4,
        borderRadius: 4,
        cursor: 'pointer',
        transition: 'background 0.1s',
      }}
      onMouseEnter={(e) => {
        ;(e.currentTarget as HTMLDivElement).style.background = '#f1f5f9'
      }}
      onMouseLeave={(e) => {
        ;(e.currentTarget as HTMLDivElement).style.background = '#fafbfc'
      }}
    >
      <span style={{ color: meta.color, fontSize: 14 }}>{meta.icon}</span>
      <Typography.Text style={{ fontWeight: 500, fontSize: 12, minWidth: 110 }}>
        {meta.label}
      </Typography.Text>
      {event.tool_name && (
        <Tag color="green" style={{ fontSize: 11 }}>
          {event.tool_name}
        </Tag>
      )}
      {event.latency_ms != null && (
        <Tag color="default" style={{ fontSize: 11 }}>
          {event.latency_ms < 1000
            ? `${event.latency_ms}ms`
            : `${(event.latency_ms / 1000).toFixed(1)}s`}
        </Tag>
      )}
      <Typography.Text
        ellipsis
        style={{ flex: 1, fontSize: 12, color: '#475569' }}
      >
        {summary}
      </Typography.Text>
      <Tooltip title={`#${event.sequence}`}>
        <Typography.Text type="secondary" style={{ fontSize: 11 }}>
          #{event.sequence}
        </Typography.Text>
      </Tooltip>
      <RightOutlined style={{ fontSize: 11, color: '#94a3b8' }} />
    </div>
  )
}

export { TYPE_META }
