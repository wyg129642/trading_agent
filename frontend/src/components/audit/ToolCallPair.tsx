import { useState } from 'react'
import { Tag, Typography, Space, Button, Tooltip } from 'antd'
import {
  ToolOutlined,
  CheckCircleFilled,
  CloseCircleFilled,
  LoadingOutlined,
  RightOutlined,
  DownOutlined,
  EyeOutlined,
} from '@ant-design/icons'
import { ToolPair } from './grouping'
import { AuditEvent } from '../../services/chatAudit'
import ToolResultRenderer from './ToolResultRenderer'

interface Props {
  pair: ToolPair
  /** Click any sub-event / start / done to open the side drawer with raw JSON. */
  onInspect?: (e: AuditEvent) => void
}

/** Render a single arg key=value pair compactly. */
function ArgItem({ k, v }: { k: string; v: any }) {
  let display: string
  if (v == null) display = String(v)
  else if (typeof v === 'string') display = v
  else if (Array.isArray(v))
    display = `[${v.length}] ${v.slice(0, 4).join(', ')}${v.length > 4 ? '…' : ''}`
  else if (typeof v === 'object') display = JSON.stringify(v)
  else display = String(v)
  if (display.length > 200) display = display.slice(0, 200) + '…'
  return (
    <div style={{ fontSize: 12, lineHeight: 1.6 }}>
      <Typography.Text type="secondary" style={{ marginRight: 6 }}>
        {k}
      </Typography.Text>
      <Typography.Text code style={{ fontSize: 11 }}>
        {display}
      </Typography.Text>
    </div>
  )
}

function formatLatency(ms: number | null): string {
  if (ms == null) return '-'
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

export default function ToolCallPair({ pair, onInspect }: Props) {
  const [open, setOpen] = useState(true)
  const inFlight = pair.done == null

  const statusColor = pair.error
    ? '#ef4444'
    : inFlight
    ? '#f59e0b'
    : '#10b981'
  const statusIcon = pair.error ? (
    <CloseCircleFilled style={{ color: statusColor }} />
  ) : inFlight ? (
    <LoadingOutlined style={{ color: statusColor }} />
  ) : (
    <CheckCircleFilled style={{ color: statusColor }} />
  )
  const statusLabel = pair.error ? 'ERROR' : inFlight ? 'IN FLIGHT' : 'OK'

  const argEntries = Object.entries(pair.args || {})

  return (
    <div
      style={{
        border: `1px solid ${pair.error ? '#fecaca' : '#e2e8f0'}`,
        borderLeft: `3px solid ${statusColor}`,
        borderRadius: 6,
        background: '#fff',
        marginBottom: 8,
      }}
    >
      {/* Header */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          padding: '6px 10px',
          borderBottom: open ? '1px solid #f1f5f9' : 'none',
          cursor: 'pointer',
          background: pair.error ? '#fef2f2' : '#f8fafc',
        }}
        onClick={() => setOpen(!open)}
      >
        {open ? (
          <DownOutlined style={{ fontSize: 10, color: '#94a3b8' }} />
        ) : (
          <RightOutlined style={{ fontSize: 10, color: '#94a3b8' }} />
        )}
        <ToolOutlined style={{ color: statusColor }} />
        <Typography.Text strong style={{ fontSize: 13 }}>
          {pair.name}
        </Typography.Text>
        <Tag
          color={pair.error ? 'red' : inFlight ? 'orange' : 'green'}
          style={{ fontSize: 10 }}
        >
          {statusIcon} {statusLabel}
        </Tag>
        <Typography.Text type="secondary" style={{ fontSize: 11 }}>
          {formatLatency(pair.latencyMs)}
        </Typography.Text>
        {pair.subEvents.length > 0 && (
          <Tag style={{ fontSize: 10 }}>{pair.subEvents.length} sub-events</Tag>
        )}
        {pair.result && (
          <Tag style={{ fontSize: 10 }}>
            {(pair.result.length / 1024).toFixed(1)}KB returned
          </Tag>
        )}
        {pair.done?.payload_truncated && (
          <Tooltip title="Stored payload exceeded the per-event size cap and was truncated. The 'raw event' view shows the truncation marker.">
            <Tag color="orange" style={{ fontSize: 10 }}>
              truncated
            </Tag>
          </Tooltip>
        )}
        <span style={{ flex: 1 }} />
        <Tooltip title={`#${pair.start.sequence} → #${pair.done?.sequence ?? '?'}`}>
          <Typography.Text type="secondary" style={{ fontSize: 10 }}>
            #{pair.start.sequence}
          </Typography.Text>
        </Tooltip>
      </div>

      {open && (
        <div style={{ padding: '8px 12px' }}>
          {/* Arguments */}
          <div style={{ marginBottom: 8 }}>
            <div
              style={{
                fontSize: 11,
                fontWeight: 600,
                color: '#64748b',
                marginBottom: 4,
                letterSpacing: 0.4,
              }}
            >
              ARGUMENTS
            </div>
            {argEntries.length === 0 ? (
              <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                (no arguments)
              </Typography.Text>
            ) : (
              <div
                style={{
                  background: '#fafbfc',
                  border: '1px solid #f1f5f9',
                  borderRadius: 4,
                  padding: '6px 10px',
                }}
              >
                {argEntries.map(([k, v]) => (
                  <ArgItem k={k} v={v} key={k} />
                ))}
              </div>
            )}
          </div>

          {/* Result / sub-events */}
          {(pair.done || pair.subEvents.length > 0) && (
            <div>
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  marginBottom: 4,
                }}
              >
                <span
                  style={{
                    fontSize: 11,
                    fontWeight: 600,
                    color: '#64748b',
                    letterSpacing: 0.4,
                  }}
                >
                  RESULT
                </span>
                <span style={{ flex: 1 }} />
                {pair.done && onInspect && (
                  <Button
                    size="small"
                    type="text"
                    icon={<EyeOutlined />}
                    onClick={() => onInspect(pair.done!)}
                    style={{ fontSize: 11, padding: '0 6px', height: 20 }}
                  >
                    raw event
                  </Button>
                )}
              </div>
              <ToolResultRenderer
                toolName={pair.name}
                result={pair.result}
                subEvents={pair.subEvents}
                onInspect={onInspect}
              />
              {pair.error && pair.result && (
                <div style={{ marginTop: 4 }}>
                  <Typography.Text type="danger" style={{ fontSize: 12 }}>
                    Tool reported error — see raw event for details.
                  </Typography.Text>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
