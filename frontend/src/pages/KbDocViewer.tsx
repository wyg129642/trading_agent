/**
 * KB Document Viewer — full-text view with snippet highlight.
 *
 * Opened from citation chips in CellInspector or the provenance timeline.
 * Query params:
 *   ?doc_id=alphapai:reports:XXXX
 *   ?snippet=<urlencoded text to locate and highlight>
 */
import { useEffect, useMemo, useRef, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { Alert, Card, Space, Spin, Tag, Typography } from 'antd'
import api from '../services/api'

const { Title, Paragraph } = Typography

interface DocPayload {
  found: boolean
  doc_id: string
  source: string
  doc_type: string
  doc_type_cn: string
  title: string
  text: string
  full_text_len: number
  truncated: boolean
  snippet_start: number
  snippet_end: number
  date: string | null
  institution?: string
  tickers?: string[]
  url?: string
  has_pdf?: boolean
}

function splitByHighlight(text: string, start: number, end: number): Array<{ kind: 'normal' | 'mark'; text: string }> {
  if (start < 0 || end <= start) return [{ kind: 'normal', text }]
  const safeEnd = Math.min(end, text.length)
  return [
    { kind: 'normal', text: text.slice(0, start) },
    { kind: 'mark', text: text.slice(start, safeEnd) },
    { kind: 'normal', text: text.slice(safeEnd) },
  ]
}

export default function KbDocViewer() {
  const [params] = useSearchParams()
  const docId = params.get('doc_id') || ''
  const snippet = params.get('snippet') || ''
  const [doc, setDoc] = useState<DocPayload | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string>('')
  const markRef = useRef<HTMLSpanElement>(null)

  useEffect(() => {
    if (!docId) {
      setError('缺少 doc_id 参数')
      return
    }
    setLoading(true)
    api.get<DocPayload>('/api/kb-viewer/doc', {
      params: { doc_id: docId, snippet: snippet || undefined, max_chars: 30000 },
    })
      .then(r => setDoc(r.data))
      .catch(e => setError(e?.response?.data?.detail || String(e)))
      .finally(() => setLoading(false))
  }, [docId, snippet])

  // Auto-scroll to highlight once rendered
  useEffect(() => {
    if (!doc || doc.snippet_start < 0) return
    // Let DOM paint first
    const t = setTimeout(() => {
      markRef.current?.scrollIntoView({ block: 'center', behavior: 'smooth' })
    }, 120)
    return () => clearTimeout(t)
  }, [doc?.doc_id, doc?.snippet_start])

  const segments = useMemo(() => {
    if (!doc) return []
    return splitByHighlight(doc.text || '', doc.snippet_start, doc.snippet_end)
  }, [doc])

  if (loading) {
    return <div style={{ padding: 40, textAlign: 'center' }}><Spin size="large" /></div>
  }
  if (error) {
    return <div style={{ padding: 24 }}><Alert type="error" message={error} /></div>
  }
  if (!doc) return null

  return (
    <div style={{ padding: 24, maxWidth: 960, margin: '0 auto' }}>
      <Title level={3} style={{ marginBottom: 8 }}>{doc.title}</Title>
      <Space wrap size={6} style={{ marginBottom: 12 }}>
        <Tag>{doc.source}</Tag>
        <Tag color="blue">{doc.doc_type_cn || doc.doc_type}</Tag>
        {doc.date && <Tag>{doc.date}</Tag>}
        {doc.institution && <Tag color="purple">{doc.institution}</Tag>}
        {(doc.tickers || []).map(t => <Tag key={t} color="gold">{t}</Tag>)}
        {doc.url && <a href={doc.url} target="_blank" rel="noreferrer">原文链接 ↗</a>}
        {doc.has_pdf && <Tag color="orange">PDF</Tag>}
      </Space>
      <div style={{ marginBottom: 8, fontSize: 12, color: '#64748b' }}>
        {doc.full_text_len.toLocaleString()} chars
        {doc.truncated && <Tag style={{ marginLeft: 8 }} color="orange">已裁切</Tag>}
        {doc.snippet_start >= 0 && <Tag style={{ marginLeft: 8 }} color="gold">已定位引用片段</Tag>}
        {snippet && doc.snippet_start < 0 && <Tag style={{ marginLeft: 8 }} color="red">片段未在文中找到</Tag>}
      </div>
      <Card>
        <Paragraph style={{
          whiteSpace: 'pre-wrap', fontSize: 14, lineHeight: 1.7, fontFamily: 'inherit', margin: 0,
        }}>
          {segments.map((seg, i) => (
            seg.kind === 'mark'
              ? <span
                  key={i}
                  ref={markRef}
                  style={{
                    background: '#fef08a',
                    padding: '1px 2px',
                    borderRadius: 2,
                    boxShadow: '0 0 0 2px #facc15 inset',
                  }}
                >{seg.text}</span>
              : <span key={i}>{seg.text}</span>
          ))}
        </Paragraph>
      </Card>
    </div>
  )
}
