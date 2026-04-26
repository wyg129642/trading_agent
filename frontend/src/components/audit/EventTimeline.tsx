import { useMemo, useState } from 'react'
import { Collapse, Tag, Typography, Empty, Segmented, Space } from 'antd'
import {
  RobotOutlined,
  WarningOutlined,
  ToolOutlined,
  MessageOutlined,
} from '@ant-design/icons'
import { AuditEvent } from '../../services/chatAudit'
import EventCard from './EventCard'
import EventDrawer from './EventDrawer'
import RoundCard from './RoundCard'
import { groupEvents } from './grouping'

interface EventTimelineProps {
  events: AuditEvent[]
}

type ViewMode = 'rounds' | 'raw'
type Filter = 'all' | 'errors' | 'tools' | 'llm'

const ERROR_TYPES = new Set(['LLM_ERROR', 'TOOL_TIMEOUT'])
const TOOL_TYPES = new Set([
  'TOOL_CALLS_DETECTED',
  'TOOL_EXEC_START',
  'TOOL_EXEC_DONE',
  'TOOL_TIMEOUT',
  'GEMINI_FUNC_CALLS',
  'SEARCH_KEYWORDS',
  'SEARCH_ENGINE_CALL',
  'SEARCH_URLS_RETURNED',
  'SEARCH_TOP_RESULTS',
  'SEARCH_CACHE_HIT',
  'WEBPAGE_READ',
  'KB_REQUEST',
  'KB_RESULTS',
  'USER_KB_REQUEST',
  'USER_KB_RESULTS',
])
const LLM_TYPES = new Set([
  'LLM_REQUEST',
  'LLM_DONE',
  'LLM_RESPONSE_CONTENT',
  'LLM_FULL_RESPONSE',
  'LLM_ERROR',
  'LLM_RETRY',
  'MODEL_REASONING',
  'MESSAGES_PAYLOAD',
])

function applyFilter(events: AuditEvent[], filter: Filter): AuditEvent[] {
  if (filter === 'all') return events
  return events.filter((e) => {
    if (filter === 'errors')
      return ERROR_TYPES.has(e.event_type) || e.payload?.error
    if (filter === 'tools') return TOOL_TYPES.has(e.event_type)
    if (filter === 'llm') return LLM_TYPES.has(e.event_type)
    return true
  })
}

export default function EventTimeline({ events }: EventTimelineProps) {
  const [activeEvent, setActiveEvent] = useState<AuditEvent | null>(null)
  const [view, setView] = useState<ViewMode>('rounds')
  const [filter, setFilter] = useState<Filter>('all')

  const filtered = useMemo(() => applyFilter(events, filter), [events, filter])
  const grouped = useMemo(() => groupEvents(filtered), [filtered])

  if (!events.length) return <Empty description="No events" />

  const totalErrors = events.filter(
    (e) => ERROR_TYPES.has(e.event_type) || e.payload?.error,
  ).length
  const totalTools = events.filter((e) =>
    e.event_type.startsWith('TOOL_'),
  ).length

  return (
    <>
      {/* Toolbar */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 12,
          marginBottom: 12,
          padding: '6px 0',
          borderBottom: '1px solid #f1f5f9',
        }}
      >
        <Segmented
          size="small"
          value={view}
          onChange={(v) => setView(v as ViewMode)}
          options={[
            {
              label: (
                <span>
                  <RobotOutlined /> Rounds
                </span>
              ),
              value: 'rounds',
            },
            { label: 'Raw events', value: 'raw' },
          ]}
        />
        <Segmented
          size="small"
          value={filter}
          onChange={(v) => setFilter(v as Filter)}
          options={[
            { label: `All (${events.length})`, value: 'all' },
            {
              label: (
                <span>
                  <WarningOutlined /> Errors ({totalErrors})
                </span>
              ),
              value: 'errors',
              disabled: totalErrors === 0,
            },
            {
              label: (
                <span>
                  <ToolOutlined /> Tools ({totalTools})
                </span>
              ),
              value: 'tools',
            },
            {
              label: (
                <span>
                  <MessageOutlined /> LLM I/O
                </span>
              ),
              value: 'llm',
            },
          ]}
        />
      </div>

      {/* Request lifecycle (truly request-level events) */}
      {grouped.requestLifecycle.length > 0 && (
        <div style={{ marginBottom: 12 }}>
          <Typography.Text strong style={{ fontSize: 13 }}>
            Request lifecycle
          </Typography.Text>
          <Typography.Text
            type="secondary"
            style={{ fontSize: 11, marginLeft: 8 }}
          >
            (events emitted before fan-out to per-model streams)
          </Typography.Text>
          <div style={{ marginTop: 6 }}>
            {grouped.requestLifecycle.map((e) => (
              <EventCard
                key={e.id}
                event={e}
                onClick={() => setActiveEvent(e)}
              />
            ))}
          </div>
        </div>
      )}

      {/* Per-model rounds */}
      {view === 'rounds' && grouped.models.length > 0 && (
        <Collapse
          defaultActiveKey={grouped.models.map((m) => m.modelId)}
          items={grouped.models.map((m) => {
            const errCount = m.rounds.reduce(
              (acc, r) => acc + r.errors.length,
              0,
            )
            const toolCount = m.rounds.reduce(
              (acc, r) => acc + r.toolPairs.length,
              0,
            )
            return {
              key: m.modelId,
              label: (
                <Space size={6}>
                  <Tag color="blue" style={{ fontSize: 12 }}>
                    {m.modelId.split('/').pop()}
                  </Tag>
                  <Typography.Text
                    type="secondary"
                    style={{ fontSize: 12 }}
                  >
                    {m.rounds.length} round(s) · {toolCount} tool call(s)
                  </Typography.Text>
                  {errCount > 0 && (
                    <Tag color="red" style={{ fontSize: 11 }}>
                      <WarningOutlined /> {errCount} error
                    </Tag>
                  )}
                </Space>
              ),
              children: (
                <div>
                  {m.loose.length > 0 && (
                    <div style={{ marginBottom: 8 }}>
                      <Typography.Text
                        type="secondary"
                        style={{ fontSize: 11 }}
                      >
                        Loose events (before first round)
                      </Typography.Text>
                      {m.loose.map((e) => (
                        <EventCard
                          key={e.id}
                          event={e}
                          onClick={() => setActiveEvent(e)}
                        />
                      ))}
                    </div>
                  )}
                  {m.rounds.map((r) => (
                    <RoundCard
                      key={r.roundNum}
                      round={r}
                      modelId={m.modelId}
                      onInspect={(e) => setActiveEvent(e)}
                    />
                  ))}
                  {m.rounds.length === 0 && (
                    <Empty
                      description="No rounds for this model"
                      image={null}
                    />
                  )}
                </div>
              ),
            }
          })}
        />
      )}

      {/* Raw fallback view */}
      {view === 'raw' && (
        <Collapse
          defaultActiveKey={grouped.models.map((m) => m.modelId)}
          items={grouped.models.map((m) => ({
            key: m.modelId,
            label: (
              <Space size={6}>
                <Tag color="blue">{m.modelId.split('/').pop()}</Tag>
                <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                  {m.rounds.reduce(
                    (acc, r) => acc + r.events.length,
                    0,
                  ) + m.loose.length}{' '}
                  events
                </Typography.Text>
              </Space>
            ),
            children: (
              <div>
                {[...m.loose, ...m.rounds.flatMap((r) => r.events)].map(
                  (e) => (
                    <EventCard
                      key={e.id}
                      event={e}
                      onClick={() => setActiveEvent(e)}
                    />
                  ),
                )}
              </div>
            ),
          }))}
        />
      )}

      {grouped.models.length === 0 && grouped.requestLifecycle.length > 0 && (
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          (no per-model events match the current filter)
        </Typography.Text>
      )}

      <EventDrawer
        event={activeEvent}
        open={!!activeEvent}
        onClose={() => setActiveEvent(null)}
      />
    </>
  )
}
