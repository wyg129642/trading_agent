import { useState } from 'react'
import { Tag, Typography, Table, Space, Empty, Button } from 'antd'
import { LinkOutlined, DownOutlined, RightOutlined } from '@ant-design/icons'
import { AuditEvent } from '../../services/chatAudit'
import MarkdownRenderer from '../MarkdownRenderer'

interface Props {
  toolName: string
  result: string
  subEvents: AuditEvent[]
  resultLen?: number
  /** Click-through to inspect a sub-event in the side drawer. */
  onInspect?: (e: AuditEvent) => void
}

function ResultPre({ text, max = 480 }: { text: string; max?: number }) {
  return (
    <pre
      style={{
        background: '#fafbfc',
        border: '1px solid #e2e8f0',
        padding: 8,
        borderRadius: 4,
        fontSize: 12,
        maxHeight: max,
        overflow: 'auto',
        whiteSpace: 'pre-wrap',
        wordBreak: 'break-word',
        margin: 0,
      }}
    >
      {text}
    </pre>
  )
}

function CollapsibleBlock({
  title,
  defaultOpen = false,
  children,
  count,
}: {
  title: string
  defaultOpen?: boolean
  children: React.ReactNode
  count?: number
}) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div style={{ marginTop: 8 }}>
      <Button
        type="text"
        size="small"
        onClick={() => setOpen(!open)}
        style={{ padding: '2px 4px', height: 'auto', fontSize: 12 }}
        icon={open ? <DownOutlined /> : <RightOutlined />}
      >
        <span style={{ fontWeight: 500 }}>{title}</span>
        {count != null && (
          <Tag color="default" style={{ marginLeft: 6, fontSize: 11 }}>
            {count}
          </Tag>
        )}
      </Button>
      {open && <div style={{ marginTop: 4, paddingLeft: 18 }}>{children}</div>}
    </div>
  )
}

function isExternalLink(url: string) {
  return /^https?:\/\//i.test(url || '')
}

function WebSearchResult({
  subEvents,
  result,
  onInspect,
}: {
  subEvents: AuditEvent[]
  result: string
  onInspect?: (e: AuditEvent) => void
}) {
  const keywords = subEvents.find((e) => e.event_type === 'SEARCH_KEYWORDS')
  const engineCalls = subEvents.filter(
    (e) => e.event_type === 'SEARCH_ENGINE_CALL',
  )
  const top = subEvents.find((e) => e.event_type === 'SEARCH_TOP_RESULTS')
  const cacheHit = subEvents.find((e) => e.event_type === 'SEARCH_CACHE_HIT')
  const topResults: any[] = top?.payload?.results || []

  return (
    <div>
      {keywords && (
        <div style={{ marginBottom: 6 }}>
          <Tag color="orange" style={{ marginRight: 4 }}>
            CN
          </Tag>
          <Typography.Text style={{ fontSize: 12 }}>
            {keywords.payload?.query_cn || '-'}
          </Typography.Text>
          {keywords.payload?.query_en && (
            <>
              <Tag color="orange" style={{ marginLeft: 12, marginRight: 4 }}>
                EN
              </Tag>
              <Typography.Text style={{ fontSize: 12 }}>
                {keywords.payload?.query_en}
              </Typography.Text>
            </>
          )}
          {keywords.payload?.recency && (
            <Tag style={{ marginLeft: 8, fontSize: 11 }}>
              recency={keywords.payload.recency}
            </Tag>
          )}
          {keywords.payload?.search_type && (
            <Tag style={{ fontSize: 11 }}>
              type={keywords.payload.search_type}
            </Tag>
          )}
        </div>
      )}

      {cacheHit && (
        <Tag color="default" style={{ fontSize: 11 }}>
          cache hit
        </Tag>
      )}

      {engineCalls.length > 0 && (
        <Space size={[4, 4]} wrap style={{ marginBottom: 6 }}>
          {engineCalls.map((e, i) => (
            <Tag
              key={i}
              color={
                e.payload?.status === 'OK' || e.payload?.status === 'CACHE'
                  ? 'green'
                  : 'red'
              }
              onClick={() => onInspect?.(e)}
              style={{ cursor: 'pointer', fontSize: 11 }}
            >
              {e.payload?.engine} · {e.payload?.status} ·{' '}
              {e.payload?.result_count ?? 0} ·{' '}
              {e.latency_ms != null ? `${e.latency_ms}ms` : '-'}
            </Tag>
          ))}
        </Space>
      )}

      {topResults.length > 0 ? (
        <CollapsibleBlock
          title="Top results"
          count={topResults.length}
          defaultOpen
        >
          <Table
            size="small"
            pagination={false}
            dataSource={topResults.map((r, i) => ({ ...r, _i: i }))}
            rowKey="_i"
            columns={[
              {
                title: '#',
                key: 'i',
                width: 32,
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
                    <span style={{ fontSize: 12 }}>{v || '-'}</span>
                  </Space>
                ),
              },
              {
                title: 'Source',
                key: 'src',
                width: 120,
                render: (_: any, r: any) =>
                  r.website || r.source || r.institution || '-',
              },
              {
                title: 'Date',
                dataIndex: 'date',
                key: 'date',
                width: 100,
                render: (v: string) => v || '-',
              },
            ]}
          />
        </CollapsibleBlock>
      ) : (
        result && (
          <CollapsibleBlock title="Raw result text">
            <ResultPre text={result} />
          </CollapsibleBlock>
        )
      )}
    </div>
  )
}

function ReadWebpageResult({
  subEvents,
  result,
}: {
  subEvents: AuditEvent[]
  result: string
}) {
  const ev = subEvents.find((e) => e.event_type === 'WEBPAGE_READ')
  const p = ev?.payload || {}
  return (
    <div>
      {p.url && (
        <div style={{ marginBottom: 6, fontSize: 12 }}>
          <a href={p.url} target="_blank" rel="noreferrer">
            <LinkOutlined /> {p.url}
          </a>
          {p.status && (
            <Tag
              color={p.status === 'OK' ? 'green' : 'red'}
              style={{ marginLeft: 8, fontSize: 11 }}
            >
              {p.status}
            </Tag>
          )}
          {ev?.latency_ms != null && (
            <Tag style={{ fontSize: 11 }}>{ev.latency_ms}ms</Tag>
          )}
          {p.content_len != null && (
            <Tag style={{ fontSize: 11 }}>{p.content_len} chars</Tag>
          )}
        </div>
      )}
      {p.content_preview && (
        <CollapsibleBlock title="Content preview">
          <ResultPre text={p.content_preview} />
        </CollapsibleBlock>
      )}
      {result && !p.content_preview && (
        <CollapsibleBlock title="Returned to LLM">
          <ResultPre text={result} />
        </CollapsibleBlock>
      )}
    </div>
  )
}

function KbSearchResult({
  subEvents,
  result,
}: {
  subEvents: AuditEvent[]
  result: string
}) {
  const req = subEvents.find(
    (e) => e.event_type === 'KB_REQUEST' || e.event_type === 'USER_KB_REQUEST',
  )
  const res = subEvents.find(
    (e) => e.event_type === 'KB_RESULTS' || e.event_type === 'USER_KB_RESULTS',
  )
  const p = req?.payload || {}
  const r = res?.payload || {}

  return (
    <div>
      {p.query && (
        <div style={{ marginBottom: 4 }}>
          <Typography.Text style={{ fontSize: 12 }}>
            <b>query:</b>{' '}
          </Typography.Text>
          <Typography.Text code style={{ fontSize: 12 }}>
            {p.query}
          </Typography.Text>
        </div>
      )}
      <Space size={[4, 4]} wrap style={{ marginBottom: 6 }}>
        {p.top_k != null && (
          <Tag style={{ fontSize: 11 }}>top_k={p.top_k}</Tag>
        )}
        {(p.tickers || []).length > 0 &&
          p.tickers.map((t: string) => (
            <Tag color="blue" key={t} style={{ fontSize: 11 }}>
              {t}
            </Tag>
          ))}
        {(p.doc_types || []).length > 0 && (
          <Tag style={{ fontSize: 11 }}>
            doc_types={p.doc_types.join(',')}
          </Tag>
        )}
        {(p.sources || []).length > 0 && (
          <Tag style={{ fontSize: 11 }}>sources={p.sources.join(',')}</Tag>
        )}
      </Space>
      {r.result_count != null && (
        <div style={{ fontSize: 12, marginBottom: 4 }}>
          <b>{r.result_count}</b> hit(s)
          {r.src_distribution && (
            <Space size={[4, 4]} wrap style={{ marginLeft: 8 }}>
              {Object.entries(r.src_distribution).map(([k, v]) => (
                <Tag key={k} style={{ fontSize: 11 }}>
                  {k}·{v as number}
                </Tag>
              ))}
            </Space>
          )}
        </div>
      )}
      {(r.top_titles || []).length > 0 && (
        <CollapsibleBlock title="Top titles" defaultOpen count={r.top_titles.length}>
          <ol style={{ paddingLeft: 18, fontSize: 12, margin: 0 }}>
            {(r.top_titles || []).map((t: string, i: number) => (
              <li key={i} style={{ marginBottom: 2 }}>
                {t}
              </li>
            ))}
          </ol>
        </CollapsibleBlock>
      )}
      {result && (
        <CollapsibleBlock title="Returned to LLM (formatted)">
          <ResultPre text={result} max={360} />
        </CollapsibleBlock>
      )}
    </div>
  )
}

function FetchDocumentResult({ result }: { result: string }) {
  return (
    <div>
      <CollapsibleBlock title="Document content returned to LLM" defaultOpen>
        <div
          style={{
            background: '#fafbfc',
            border: '1px solid #e2e8f0',
            padding: 10,
            borderRadius: 4,
            fontSize: 12,
            maxHeight: 480,
            overflow: 'auto',
          }}
        >
          <MarkdownRenderer content={result || '(empty)'} />
        </div>
      </CollapsibleBlock>
    </div>
  )
}

export default function ToolResultRenderer({
  toolName,
  result,
  subEvents,
  resultLen,
  onInspect,
}: Props) {
  if (toolName === 'web_search')
    return (
      <WebSearchResult
        subEvents={subEvents}
        result={result}
        onInspect={onInspect}
      />
    )
  if (toolName === 'read_webpage')
    return <ReadWebpageResult subEvents={subEvents} result={result} />
  if (toolName === 'kb_search' || toolName === 'user_kb_search')
    return <KbSearchResult subEvents={subEvents} result={result} />
  if (toolName === 'kb_fetch_document' || toolName === 'user_kb_fetch_document')
    return <FetchDocumentResult result={result} />

  // Default: show whatever the tool returned as text
  if (!result) return <Empty image={null} description="(no result)" />
  return (
    <CollapsibleBlock
      title="Result text"
      defaultOpen
      count={resultLen ?? result.length}
    >
      <ResultPre text={result} />
    </CollapsibleBlock>
  )
}
