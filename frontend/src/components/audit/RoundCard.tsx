import { useState } from 'react'
import { Tag, Typography, Space, Button, Collapse } from 'antd'
import {
  RobotOutlined,
  BulbOutlined,
  WarningOutlined,
  RightOutlined,
  DownOutlined,
  MessageOutlined,
  EyeOutlined,
} from '@ant-design/icons'
import { RoundData } from './grouping'
import { AuditEvent } from '../../services/chatAudit'
import ToolCallPair from './ToolCallPair'
import MarkdownRenderer from '../MarkdownRenderer'

interface Props {
  round: RoundData
  modelId: string
  onInspect?: (e: AuditEvent) => void
}

function formatLatency(ms: number | null | undefined): string {
  if (ms == null) return '-'
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

function summarizeRequest(request: AuditEvent | null): string {
  if (!request) return ''
  const p = request.payload || {}
  const tools = (p.tools_offered || []).length
  const msgs = p.messages_count
  const mode = p.mode
  const parts = []
  if (mode) parts.push(`mode=${mode}`)
  if (msgs != null) parts.push(`${msgs} msgs`)
  if (tools) parts.push(`${tools} tools`)
  return parts.join(' · ')
}

function MessagesPreview({ event }: { event: AuditEvent }) {
  const [open, setOpen] = useState(false)
  const summary = (event.payload?.messages_summary || []) as any[]
  const full = (event.payload?.messages_full || []) as any[]
  const count = full.length || summary.length
  return (
    <div style={{ marginBottom: 8 }}>
      <Button
        type="text"
        size="small"
        icon={open ? <DownOutlined /> : <RightOutlined />}
        onClick={() => setOpen(!open)}
        style={{ padding: '2px 4px', height: 'auto', fontSize: 12 }}
      >
        Prompt sent to model
        <Tag color="default" style={{ marginLeft: 6, fontSize: 10 }}>
          {count} msgs
        </Tag>
        {(event.payload?.tools_offered || []).length > 0 && (
          <Tag style={{ fontSize: 10 }}>
            {(event.payload?.tools_offered || []).length} tools offered
          </Tag>
        )}
      </Button>
      {open && (
        <div
          style={{
            background: '#fafbfc',
            border: '1px solid #e2e8f0',
            borderRadius: 4,
            padding: 8,
            marginTop: 4,
          }}
        >
          {summary.length > 0 && (
            <table
              style={{
                fontSize: 11,
                width: '100%',
                borderCollapse: 'collapse',
              }}
            >
              <thead>
                <tr style={{ borderBottom: '1px solid #e2e8f0' }}>
                  <th align="left" style={{ padding: 4, width: 24 }}>
                    #
                  </th>
                  <th align="left" style={{ padding: 4, width: 80 }}>
                    role
                  </th>
                  <th align="left" style={{ padding: 4 }}>
                    content_len
                  </th>
                  <th align="left" style={{ padding: 4 }}>
                    tool_calls
                  </th>
                  <th align="left" style={{ padding: 4 }}>
                    tool_call_id
                  </th>
                </tr>
              </thead>
              <tbody>
                {summary.map((m: any, i: number) => (
                  <tr
                    key={i}
                    style={{ borderBottom: '1px dashed #f1f5f9' }}
                  >
                    <td style={{ padding: 4 }}>{i + 1}</td>
                    <td style={{ padding: 4 }}>
                      <Tag
                        color={
                          m.role === 'system'
                            ? 'purple'
                            : m.role === 'user'
                            ? 'blue'
                            : m.role === 'assistant'
                            ? 'green'
                            : 'default'
                        }
                        style={{ fontSize: 10 }}
                      >
                        {m.role}
                      </Tag>
                    </td>
                    <td style={{ padding: 4 }}>{m.content_len}</td>
                    <td style={{ padding: 4 }}>
                      {m.has_tool_calls ? '✓' : ''}
                    </td>
                    <td
                      style={{
                        padding: 4,
                        fontFamily: 'monospace',
                        fontSize: 10,
                        color: '#64748b',
                      }}
                    >
                      {m.tool_call_id || ''}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {summary.length === 0 && (
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              (no messages summary in payload)
            </Typography.Text>
          )}
        </div>
      )}
    </div>
  )
}

function ReasoningPreview({ event }: { event: AuditEvent }) {
  const [open, setOpen] = useState(false)
  const text: string = event.payload?.text || ''
  return (
    <div style={{ marginBottom: 8 }}>
      <Button
        type="text"
        size="small"
        icon={open ? <DownOutlined /> : <RightOutlined />}
        onClick={() => setOpen(!open)}
        style={{ padding: '2px 4px', height: 'auto', fontSize: 12 }}
      >
        <BulbOutlined style={{ color: '#8b5cf6', marginRight: 4 }} />
        Model reasoning
        <Tag style={{ marginLeft: 6, fontSize: 10 }}>{text.length} chars</Tag>
      </Button>
      {open && (
        <div
          style={{
            background: '#faf5ff',
            border: '1px solid #e9d5ff',
            borderRadius: 4,
            padding: 8,
            marginTop: 4,
            fontSize: 12,
            maxHeight: 320,
            overflow: 'auto',
          }}
        >
          <MarkdownRenderer content={text} />
        </div>
      )}
    </div>
  )
}

function ResponsePreview({ event }: { event: AuditEvent }) {
  const [open, setOpen] = useState(true)
  const text: string = event.payload?.content || ''
  if (!text) return null
  return (
    <div>
      <Button
        type="text"
        size="small"
        icon={open ? <DownOutlined /> : <RightOutlined />}
        onClick={() => setOpen(!open)}
        style={{ padding: '2px 4px', height: 'auto', fontSize: 12 }}
      >
        <MessageOutlined style={{ color: '#0ea5e9', marginRight: 4 }} />
        Model output for this round
        <Tag style={{ marginLeft: 6, fontSize: 10 }}>{text.length} chars</Tag>
      </Button>
      {open && (
        <div
          style={{
            background: '#f0f9ff',
            border: '1px solid #bae6fd',
            borderRadius: 4,
            padding: 8,
            marginTop: 4,
            fontSize: 12,
            maxHeight: 360,
            overflow: 'auto',
          }}
        >
          <MarkdownRenderer content={text} />
        </div>
      )}
    </div>
  )
}

export default function RoundCard({ round, modelId, onInspect }: Props) {
  const [open, setOpen] = useState(true)

  const totalLatencyMs = round.toolPairs.reduce(
    (acc, p) => acc + (p.latencyMs || 0),
    0,
  )
  const llmLatency = round.done?.latency_ms ?? null

  const errorCount = round.errors.length
  const toolCount = round.toolPairs.length

  return (
    <div
      style={{
        border: `1px solid ${errorCount > 0 ? '#fecaca' : '#e2e8f0'}`,
        borderRadius: 6,
        background: '#fff',
        marginBottom: 10,
      }}
    >
      {/* Header */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          padding: '8px 12px',
          background: errorCount > 0 ? '#fef2f2' : '#f8fafc',
          borderBottom: open ? '1px solid #f1f5f9' : 'none',
          cursor: 'pointer',
          borderRadius: '6px 6px 0 0',
        }}
        onClick={() => setOpen(!open)}
      >
        {open ? (
          <DownOutlined style={{ fontSize: 11, color: '#94a3b8' }} />
        ) : (
          <RightOutlined style={{ fontSize: 11, color: '#94a3b8' }} />
        )}
        <Tag color="blue" style={{ fontSize: 11 }}>
          Round {round.roundNum}
        </Tag>
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          {summarizeRequest(round.request)}
        </Typography.Text>
        <span style={{ flex: 1 }} />
        {toolCount > 0 && (
          <Tag color="green" style={{ fontSize: 11 }}>
            {toolCount} tool call(s)
          </Tag>
        )}
        {errorCount > 0 && (
          <Tag color="red" style={{ fontSize: 11 }}>
            <WarningOutlined /> {errorCount} error
          </Tag>
        )}
        {llmLatency != null && (
          <Tag style={{ fontSize: 11 }}>LLM {formatLatency(llmLatency)}</Tag>
        )}
        {totalLatencyMs > 0 && (
          <Tag style={{ fontSize: 11 }}>
            tools {formatLatency(totalLatencyMs)}
          </Tag>
        )}
        {round.done?.payload?.tokens != null && (
          <Tag style={{ fontSize: 11 }}>
            {round.done.payload.tokens} tok
          </Tag>
        )}
      </div>

      {open && (
        <div style={{ padding: '10px 12px' }}>
          {/* 1. What was sent to the LLM (collapsed by default) */}
          {round.request && <MessagesPreview event={round.request} />}

          {/* 2. Reasoning (if any) */}
          {round.reasoning && <ReasoningPreview event={round.reasoning} />}

          {/* 3. Tool calls (the meat of the round) */}
          {round.toolPairs.length > 0 && (
            <div style={{ marginTop: 6 }}>
              <div
                style={{
                  fontSize: 11,
                  fontWeight: 600,
                  color: '#64748b',
                  letterSpacing: 0.4,
                  marginBottom: 4,
                }}
              >
                TOOL CALLS · {round.toolPairs.length}
              </div>
              {round.toolPairs.map((p, i) => (
                <ToolCallPair key={i} pair={p} onInspect={onInspect} />
              ))}
            </div>
          )}

          {/* 4. Model's text output for this round */}
          {round.responseContent && (
            <ResponsePreview event={round.responseContent} />
          )}

          {/* 5. Errors */}
          {round.errors.length > 0 && (
            <div
              style={{
                marginTop: 8,
                padding: 8,
                border: '1px solid #fecaca',
                borderRadius: 4,
                background: '#fef2f2',
              }}
            >
              <div
                style={{
                  fontSize: 11,
                  fontWeight: 600,
                  color: '#991b1b',
                  marginBottom: 4,
                }}
              >
                ERRORS
              </div>
              {round.errors.map((e) => (
                <div
                  key={e.id}
                  style={{
                    fontSize: 12,
                    cursor: 'pointer',
                    padding: '2px 0',
                  }}
                  onClick={() => onInspect?.(e)}
                >
                  <Tag color="red" style={{ fontSize: 10 }}>
                    {e.event_type}
                  </Tag>
                  <Typography.Text type="danger">
                    {e.payload?.error ||
                      e.payload?.tool_name ||
                      '(see raw event)'}
                  </Typography.Text>
                </div>
              ))}
            </div>
          )}

          {/* 6. Defensive escape hatch: any event in this round that didn't
             land in a known slot above. Keeps newly-added event types
             visible without a frontend update. */}
          <OtherEvents round={round} onInspect={onInspect} />
        </div>
      )}
    </div>
  )
}

function OtherEvents({
  round,
  onInspect,
}: {
  round: RoundData
  onInspect?: (e: AuditEvent) => void
}) {
  // Anything we already rendered in a structured slot is excluded.
  const claimed = new Set<string>()
  if (round.request) claimed.add(round.request.id)
  if (round.reasoning) claimed.add(round.reasoning.id)
  if (round.responseContent) claimed.add(round.responseContent.id)
  if (round.done) claimed.add(round.done.id)
  if (round.toolCallsDecision) claimed.add(round.toolCallsDecision.id)
  for (const e of round.errors) claimed.add(e.id)
  for (const p of round.toolPairs) {
    claimed.add(p.start.id)
    if (p.done) claimed.add(p.done.id)
    for (const s of p.subEvents) claimed.add(s.id)
  }
  // SSE meta-events and a couple of structural duplicates are noise.
  const SKIP = new Set(['LLM_FULL_RESPONSE'])
  const others = round.events.filter(
    (e) => !claimed.has(e.id) && !SKIP.has(e.event_type),
  )
  if (others.length === 0) return null
  return (
    <div style={{ marginTop: 8 }}>
      <div
        style={{
          fontSize: 11,
          fontWeight: 600,
          color: '#64748b',
          letterSpacing: 0.4,
          marginBottom: 4,
        }}
      >
        OTHER EVENTS · {others.length}
      </div>
      <div
        style={{
          background: '#fafbfc',
          border: '1px dashed #e2e8f0',
          borderRadius: 4,
          padding: 6,
        }}
      >
        {others.map((e) => (
          <div
            key={e.id}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 6,
              padding: '2px 0',
              cursor: onInspect ? 'pointer' : 'default',
              fontSize: 12,
            }}
            onClick={() => onInspect?.(e)}
          >
            <Tag style={{ fontSize: 10 }}>#{e.sequence}</Tag>
            <Tag color="geekblue" style={{ fontSize: 10 }}>
              {e.event_type}
            </Tag>
            {e.tool_name && (
              <Tag style={{ fontSize: 10 }}>{e.tool_name}</Tag>
            )}
            {e.latency_ms != null && (
              <Tag style={{ fontSize: 10 }}>{e.latency_ms}ms</Tag>
            )}
            <Typography.Text type="secondary" style={{ fontSize: 11 }}>
              {summarizePayload(e.payload)}
            </Typography.Text>
          </div>
        ))}
      </div>
    </div>
  )
}

function summarizePayload(p: any): string {
  if (!p || typeof p !== 'object') return ''
  // Pull the most useful field for a one-line preview.
  if (typeof p.query === 'string') return `query="${p.query.slice(0, 80)}"`
  if (typeof p.text === 'string') return `${p.text.slice(0, 80)}`
  if (typeof p.content === 'string') return `${p.content.slice(0, 80)}`
  if (typeof p.tool_name === 'string') return `tool=${p.tool_name}`
  if (typeof p.error === 'string') return p.error.slice(0, 100)
  return ''
}
