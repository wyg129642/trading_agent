import { Drawer, Tag, Typography, Descriptions, Table, Space } from 'antd'
import { LinkOutlined } from '@ant-design/icons'
import { AuditEvent } from '../../services/chatAudit'
import { TYPE_META } from './EventCard'
import MarkdownRenderer from '../MarkdownRenderer'

interface EventDrawerProps {
  event: AuditEvent | null
  open: boolean
  onClose: () => void
}

function isExternalLink(url: string) {
  return /^https?:\/\//i.test(url || '')
}

function JsonBlock({ value }: { value: any }) {
  return (
    <pre
      style={{
        background: '#f8fafc',
        border: '1px solid #e2e8f0',
        padding: 10,
        borderRadius: 6,
        fontSize: 12,
        maxHeight: 480,
        overflow: 'auto',
      }}
    >
      {JSON.stringify(value, null, 2)}
    </pre>
  )
}

/** Render a list of {title, url, website, date, score} items as a clickable table. */
function ResultsTable({
  items,
  showScore,
}: {
  items: any[]
  showScore?: boolean
}) {
  return (
    <Table
      size="small"
      pagination={false}
      dataSource={items.map((it, i) => ({ ...it, _i: i }))}
      rowKey="_i"
      columns={[
        {
          title: '#',
          key: 'idx',
          width: 40,
          render: (_: any, _r: any, i: number) => i + 1,
        },
        {
          title: 'Title',
          dataIndex: 'title',
          key: 'title',
          render: (v: string, r: any) => (
            <Space size={4}>
              {isExternalLink(r.url) && (
                <a href={r.url} target="_blank" rel="noreferrer">
                  <LinkOutlined />
                </a>
              )}
              <span>{v || '-'}</span>
            </Space>
          ),
        },
        {
          title: 'Source',
          key: 'src',
          width: 140,
          render: (_: any, r: any) =>
            r.website || r.source || r.institution || '-',
        },
        {
          title: 'Date',
          dataIndex: 'date',
          key: 'date',
          width: 110,
          render: (v: string) => v || '-',
        },
        ...(showScore
          ? [
              {
                title: 'Score',
                dataIndex: 'score',
                key: 'score',
                width: 80,
                render: (v: any) => (v != null ? String(v) : '-'),
              },
            ]
          : []),
      ]}
    />
  )
}

function renderTypeSpecific(e: AuditEvent) {
  const p = (e.payload || {}) as any
  switch (e.event_type) {
    case 'LLM_REQUEST': {
      const summary = p.messages_summary || []
      return (
        <>
          <Descriptions bordered size="small" column={2}>
            <Descriptions.Item label="round">{p.round_num}</Descriptions.Item>
            <Descriptions.Item label="mode">{p.mode}</Descriptions.Item>
            <Descriptions.Item label="messages_count">
              {p.messages_count}
            </Descriptions.Item>
            <Descriptions.Item label="tools_offered">
              <Space size={2} wrap>
                {(p.tools_offered || []).map((t: string) => (
                  <Tag key={t} color="green">
                    {t}
                  </Tag>
                ))}
              </Space>
            </Descriptions.Item>
          </Descriptions>
          <Typography.Title level={5} style={{ marginTop: 12 }}>
            Messages summary
          </Typography.Title>
          <Table
            size="small"
            pagination={false}
            dataSource={summary.map((s: any, i: number) => ({ ...s, _i: i }))}
            rowKey="_i"
            columns={[
              { title: '#', key: 'i', width: 40, render: (_: any, _r: any, i: number) => i + 1 },
              { title: 'role', dataIndex: 'role', key: 'role', width: 100 },
              {
                title: 'content_len',
                dataIndex: 'content_len',
                key: 'len',
                width: 110,
              },
              {
                title: 'has_tool_calls',
                dataIndex: 'has_tool_calls',
                key: 'tc',
                width: 130,
                render: (v: boolean) => (v ? '✓' : ''),
              },
              {
                title: 'tool_call_id',
                dataIndex: 'tool_call_id',
                key: 'tcid',
                ellipsis: true,
              },
            ]}
          />
          <Typography.Title level={5} style={{ marginTop: 12 }}>
            Full messages payload
          </Typography.Title>
          <JsonBlock value={p.messages_full || []} />
        </>
      )
    }
    case 'TOOL_CALLS_DETECTED':
      return (
        <>
          <Typography.Paragraph>
            <b>Count:</b> {p.count} · <b>round:</b> {p.round_num}
          </Typography.Paragraph>
          {(p.calls || []).map((c: any, i: number) => (
            <div key={i} style={{ marginBottom: 12 }}>
              <Tag color="blue">{c.name}</Tag>
              <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                id={c.id}
              </Typography.Text>
              <JsonBlock value={c.arguments} />
            </div>
          ))}
        </>
      )
    case 'TOOL_EXEC_START':
      return (
        <>
          <Descriptions bordered size="small" column={1}>
            <Descriptions.Item label="tool_name">
              <Tag color="green">{p.tool_name}</Tag>
            </Descriptions.Item>
          </Descriptions>
          <Typography.Title level={5} style={{ marginTop: 12 }}>
            Arguments
          </Typography.Title>
          <JsonBlock value={p.arguments || {}} />
        </>
      )
    case 'TOOL_EXEC_DONE':
      return (
        <>
          <Descriptions bordered size="small" column={2}>
            <Descriptions.Item label="tool_name">
              <Tag color="green">{p.tool_name}</Tag>
            </Descriptions.Item>
            <Descriptions.Item label="result_len">
              {p.result_len}
            </Descriptions.Item>
            <Descriptions.Item label="latency">
              {e.latency_ms != null ? `${e.latency_ms} ms` : '-'}
            </Descriptions.Item>
            <Descriptions.Item label="error">
              {p.error ? <Tag color="red">true</Tag> : '-'}
            </Descriptions.Item>
          </Descriptions>
          <Typography.Title level={5} style={{ marginTop: 12 }}>
            Result
          </Typography.Title>
          <pre
            style={{
              background: '#f8fafc',
              border: '1px solid #e2e8f0',
              padding: 10,
              borderRadius: 6,
              fontSize: 12,
              maxHeight: 600,
              overflow: 'auto',
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
            }}
          >
            {p.result || ''}
          </pre>
        </>
      )
    case 'SEARCH_KEYWORDS':
      return (
        <Descriptions bordered size="small" column={1}>
          <Descriptions.Item label="query_cn">
            {p.query_cn || '-'}
          </Descriptions.Item>
          <Descriptions.Item label="query_en">
            {p.query_en || '-'}
          </Descriptions.Item>
          <Descriptions.Item label="search_type">
            {p.search_type || '-'}
          </Descriptions.Item>
          <Descriptions.Item label="recency">{p.recency || '-'}</Descriptions.Item>
          <Descriptions.Item label="is_cn_stock">
            {p.is_cn_stock ? '✓' : '-'}
          </Descriptions.Item>
        </Descriptions>
      )
    case 'SEARCH_ENGINE_CALL':
      return (
        <Descriptions bordered size="small" column={2}>
          <Descriptions.Item label="engine">{p.engine}</Descriptions.Item>
          <Descriptions.Item label="status">{p.status}</Descriptions.Item>
          <Descriptions.Item label="latency_ms">
            {e.latency_ms ?? '-'}
          </Descriptions.Item>
          <Descriptions.Item label="result_count">
            {p.result_count}
          </Descriptions.Item>
          <Descriptions.Item label="api_url" span={2}>
            <Typography.Text copyable>{p.api_url}</Typography.Text>
          </Descriptions.Item>
          <Descriptions.Item label="query" span={2}>
            <Typography.Text copyable>{p.query}</Typography.Text>
          </Descriptions.Item>
          {p.error ? (
            <Descriptions.Item label="error" span={2}>
              <Typography.Text type="danger">{p.error}</Typography.Text>
            </Descriptions.Item>
          ) : null}
        </Descriptions>
      )
    case 'SEARCH_URLS_RETURNED':
      return (
        <>
          <Typography.Paragraph>
            <b>{p.engine}</b> returned {p.count} URLs for query{' '}
            <Typography.Text code>{p.query}</Typography.Text>
          </Typography.Paragraph>
          <ResultsTable items={p.items || []} showScore />
        </>
      )
    case 'SEARCH_TOP_RESULTS':
      return (
        <>
          <Typography.Paragraph>
            Top {p.count} merged results (round {p.round_num})
          </Typography.Paragraph>
          <ResultsTable items={p.results || []} showScore />
        </>
      )
    case 'WEBPAGE_READ':
      return (
        <>
          <Descriptions bordered size="small" column={2}>
            <Descriptions.Item label="url" span={2}>
              <a href={p.url} target="_blank" rel="noreferrer">
                {p.url}
              </a>
            </Descriptions.Item>
            <Descriptions.Item label="status">{p.status}</Descriptions.Item>
            <Descriptions.Item label="latency">
              {e.latency_ms != null ? `${e.latency_ms} ms` : '-'}
            </Descriptions.Item>
            <Descriptions.Item label="content_len">
              {p.content_len}
            </Descriptions.Item>
            {p.error ? (
              <Descriptions.Item label="error">
                <Typography.Text type="danger">{p.error}</Typography.Text>
              </Descriptions.Item>
            ) : null}
          </Descriptions>
          <Typography.Title level={5} style={{ marginTop: 12 }}>
            Content preview
          </Typography.Title>
          <pre
            style={{
              background: '#f8fafc',
              border: '1px solid #e2e8f0',
              padding: 10,
              borderRadius: 6,
              fontSize: 12,
              maxHeight: 600,
              overflow: 'auto',
              whiteSpace: 'pre-wrap',
            }}
          >
            {p.content_preview || ''}
          </pre>
        </>
      )
    case 'KB_REQUEST':
    case 'USER_KB_REQUEST':
      return (
        <Descriptions bordered size="small" column={1}>
          <Descriptions.Item label="query">
            <Typography.Text copyable>{p.query}</Typography.Text>
          </Descriptions.Item>
          <Descriptions.Item label="top_k">{p.top_k}</Descriptions.Item>
          {p.tickers ? (
            <Descriptions.Item label="tickers">
              <Space size={2} wrap>
                {(p.tickers || []).map((t: string) => (
                  <Tag key={t} color="blue">
                    {t}
                  </Tag>
                ))}
              </Space>
            </Descriptions.Item>
          ) : null}
          {p.doc_types ? (
            <Descriptions.Item label="doc_types">
              {(p.doc_types || []).join(', ') || '-'}
            </Descriptions.Item>
          ) : null}
          {p.sources ? (
            <Descriptions.Item label="sources">
              {(p.sources || []).join(', ') || '-'}
            </Descriptions.Item>
          ) : null}
          {p.date_range ? (
            <Descriptions.Item label="date_range">
              <JsonBlock value={p.date_range} />
            </Descriptions.Item>
          ) : null}
          {p.document_ids ? (
            <Descriptions.Item label="document_ids">
              {(p.document_ids || []).join(', ') || '-'}
            </Descriptions.Item>
          ) : null}
        </Descriptions>
      )
    case 'KB_RESULTS':
    case 'USER_KB_RESULTS':
      return (
        <>
          <Typography.Paragraph>
            <b>{p.result_count}</b> hits for{' '}
            <Typography.Text code>{p.query}</Typography.Text>
          </Typography.Paragraph>
          {p.src_distribution ? (
            <Typography.Paragraph>
              Source distribution:{' '}
              {Object.entries(p.src_distribution).map(([k, v]) => (
                <Tag key={k}>
                  {k}·{v as number}
                </Tag>
              ))}
            </Typography.Paragraph>
          ) : null}
          <Typography.Title level={5}>Top titles</Typography.Title>
          <ol style={{ paddingLeft: 20, fontSize: 13 }}>
            {(p.top_titles || []).map((t: string, i: number) => (
              <li key={i}>{t}</li>
            ))}
          </ol>
        </>
      )
    case 'MODEL_REASONING':
      return (
        <>
          <Typography.Paragraph type="secondary">
            round={p.round_num} · len={p.text?.length || 0}
          </Typography.Paragraph>
          <MarkdownRenderer content={p.text || ''} />
        </>
      )
    case 'LLM_FULL_RESPONSE':
    case 'LLM_RESPONSE_CONTENT':
      return (
        <>
          <Typography.Paragraph type="secondary">
            len={p.len ?? p.content?.length ?? 0}
          </Typography.Paragraph>
          <MarkdownRenderer content={p.content || ''} />
        </>
      )
    case 'LLM_DONE':
      return (
        <Descriptions bordered size="small" column={2}>
          <Descriptions.Item label="round_num">
            {e.round_num ?? '-'}
          </Descriptions.Item>
          <Descriptions.Item label="latency_ms">
            {e.latency_ms ?? '-'}
          </Descriptions.Item>
          <Descriptions.Item label="content_len">
            {p.content_len}
          </Descriptions.Item>
          <Descriptions.Item label="tokens">{p.tokens}</Descriptions.Item>
          <Descriptions.Item label="finish_reason">
            {p.finish_reason || '-'}
          </Descriptions.Item>
          <Descriptions.Item label="error">
            {p.error ? <Typography.Text type="danger">{p.error}</Typography.Text> : '-'}
          </Descriptions.Item>
        </Descriptions>
      )
    case 'MESSAGES_PAYLOAD':
      return (
        <>
          <Typography.Paragraph>
            {p.count} messages sent to the LLM (system + history + current).
          </Typography.Paragraph>
          <JsonBlock value={p.messages || []} />
        </>
      )
    default:
      return <JsonBlock value={e.payload || {}} />
  }
}

export default function EventDrawer({ event, open, onClose }: EventDrawerProps) {
  if (!event) return null
  const meta = TYPE_META[event.event_type] || { color: '#64748b', label: event.event_type }
  return (
    <Drawer
      title={
        <Space>
          <span style={{ color: meta.color }}>●</span>
          <span>{meta.label || event.event_type}</span>
          <Tag color="default">#{event.sequence}</Tag>
          {event.model_id && <Tag color="blue">{event.model_id.split('/').pop()}</Tag>}
          {event.round_num != null && <Tag>round {event.round_num}</Tag>}
          {event.tool_name && <Tag color="green">{event.tool_name}</Tag>}
          {event.payload_truncated && (
            <Tag color="orange">payload truncated</Tag>
          )}
        </Space>
      }
      placement="right"
      width={760}
      open={open}
      onClose={onClose}
      destroyOnClose
    >
      <Typography.Paragraph type="secondary" style={{ fontSize: 12 }}>
        event_type: <code>{event.event_type}</code> · created_at:{' '}
        {event.created_at}
      </Typography.Paragraph>
      {renderTypeSpecific(event)}
    </Drawer>
  )
}
