/**
 * Markdown editor for `.md` documents in the workspace.
 *
 * Split layout:
 *   - Left pane: raw source editor (`textarea`-backed, keeps bundle tiny
 *     vs pulling in Milkdown / Monaco).
 *   - Right pane: live preview rendered with the existing
 *     ``react-markdown`` + GFM setup used elsewhere in the app.
 *
 * The parent passes the document id + initial content; we debounce a
 * 1.2 s autosave to ``/user-kb/documents/{id}/markdown`` on each edit,
 * matching the spreadsheet autosave cadence. Dirty state and the last
 * error are surfaced via the ``onDirtyChange`` / ``onSavingChange``
 * callbacks so the tab bar can show an unsaved-dot.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Alert, Button, Space, Tooltip, Typography, Spin, message as antdMessage } from 'antd'
import {
  SaveOutlined, EyeOutlined, EditOutlined, SplitCellsOutlined,
} from '@ant-design/icons'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import api from '../../services/api'

const { Text } = Typography

export interface MarkdownCanvasProps {
  documentId: string
  readonly?: boolean
  onDirtyChange?: (dirty: boolean) => void
  onSavingChange?: (saving: boolean) => void
}

type ViewMode = 'split' | 'source' | 'preview'

export default function MarkdownCanvas({
  documentId, readonly = false, onDirtyChange, onSavingChange,
}: MarkdownCanvasProps) {
  const [loading, setLoading] = useState(true)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [saveError, setSaveError] = useState<string | null>(null)
  const [content, setContent] = useState('')
  const [title, setTitle] = useState('')
  const [dirty, setDirty] = useState(false)
  const [saving, setSaving] = useState(false)
  const [mode, setMode] = useState<ViewMode>('split')
  const saveTimer = useRef<number | null>(null)
  const lastSavedRef = useRef<string>('')

  // Push dirty/saving up so the DocumentTabBar can show a dot. Route via a
  // ref so the effect only re-fires when the state value changes — otherwise
  // an unstable parent callback (inline lambda) plus our effect would
  // infinite-loop through markDirty, crashing the canvas with React #185.
  const onDirtyChangeRef = useRef(onDirtyChange)
  const onSavingChangeRef = useRef(onSavingChange)
  useEffect(() => { onDirtyChangeRef.current = onDirtyChange })
  useEffect(() => { onSavingChangeRef.current = onSavingChange })
  useEffect(() => { onDirtyChangeRef.current?.(dirty) }, [dirty])
  useEffect(() => { onSavingChangeRef.current?.(saving) }, [saving])

  // Initial load.
  useEffect(() => {
    let cancelled = false
    setLoading(true); setLoadError(null)
    api.get<{ document_id: string; title: string; content_md: string }>(
      `/user-kb/documents/${documentId}/markdown`,
    )
      .then((res) => {
        if (cancelled) return
        setContent(res.data.content_md || '')
        setTitle(res.data.title || '')
        lastSavedRef.current = res.data.content_md || ''
        setDirty(false)
      })
      .catch((err) => {
        if (cancelled) return
        setLoadError(
          err?.response?.data?.detail || err.message || 'failed to load markdown',
        )
      })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [documentId])

  const doSave = useCallback(async (text: string) => {
    setSaving(true); setSaveError(null)
    try {
      await api.patch(
        `/user-kb/documents/${documentId}/markdown`,
        { content_md: text },
      )
      lastSavedRef.current = text
      setDirty(false)
    } catch (err: any) {
      const msg = err?.response?.data?.detail || err.message || 'save failed'
      setSaveError(msg)
      antdMessage.error(`保存 Markdown 失败: ${msg}`)
    } finally {
      setSaving(false)
    }
  }, [documentId])

  const onChange = useCallback((text: string) => {
    setContent(text)
    setDirty(text !== lastSavedRef.current)
    if (readonly) return
    if (saveTimer.current) window.clearTimeout(saveTimer.current)
    saveTimer.current = window.setTimeout(() => {
      doSave(text)
    }, 1200) as unknown as number
  }, [doSave, readonly])

  // Flush pending save on unmount.
  useEffect(() => {
    return () => {
      if (saveTimer.current) window.clearTimeout(saveTimer.current)
    }
  }, [])

  if (loading) {
    return <div style={{ padding: 40, textAlign: 'center' }}><Spin /></div>
  }
  if (loadError) {
    return (
      <Alert
        type="error" showIcon style={{ margin: 16 }}
        message="加载 Markdown 失败" description={loadError}
      />
    )
  }

  return (
    <div style={{
      display: 'flex', flexDirection: 'column',
      flex: 1, minHeight: 0, width: '100%', height: '100%',
    }}>
      {/* Toolbar */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 8,
        padding: '8px 12px',
        background: 'var(--ws-surface-alt, #f7f9fb)',
        borderBottom: '1px solid var(--ws-border, #e6e8eb)',
        flexShrink: 0,
      }}>
        <Text strong style={{ marginRight: 8 }}>{title || '未命名'}</Text>
        <Text type="secondary" style={{ fontSize: 12 }}>
          {saving
            ? '保存中…'
            : dirty
              ? '编辑中，1.2 秒后自动保存…'
              : `已加载 ${content.length} 字`}
        </Text>
        {saveError && (
          <Tooltip title={saveError}>
            <Text type="danger" style={{ fontSize: 12 }}>保存出错</Text>
          </Tooltip>
        )}
        <div style={{ flex: 1 }} />
        <Space size={4}>
          <Tooltip title="仅编辑">
            <Button
              size="small"
              icon={<EditOutlined />}
              type={mode === 'source' ? 'primary' : 'default'}
              onClick={() => setMode('source')}
            />
          </Tooltip>
          <Tooltip title="编辑 + 预览">
            <Button
              size="small"
              icon={<SplitCellsOutlined />}
              type={mode === 'split' ? 'primary' : 'default'}
              onClick={() => setMode('split')}
            />
          </Tooltip>
          <Tooltip title="仅预览">
            <Button
              size="small"
              icon={<EyeOutlined />}
              type={mode === 'preview' ? 'primary' : 'default'}
              onClick={() => setMode('preview')}
            />
          </Tooltip>
          {!readonly && (
            <Button
              size="small" type="primary" icon={<SaveOutlined />}
              loading={saving}
              onClick={() => doSave(content)}
            >
              保存
            </Button>
          )}
        </Space>
      </div>

      {/* Body — explicit min-heights so the flex children render even if
          the grandparent's intrinsic height hasn't resolved on first paint. */}
      <div style={{
        flex: 1, display: 'flex', overflow: 'hidden', minHeight: 320,
      }}>
        {mode !== 'preview' && (
          <textarea
            value={content}
            onChange={(e) => onChange(e.target.value)}
            readOnly={readonly}
            spellCheck={false}
            placeholder={content === '' ? '(此文档正文为空，可直接开始编辑)' : undefined}
            style={{
              flex: 1,
              minWidth: 0, minHeight: 300,
              border: 'none', outline: 'none',
              padding: 16,
              fontSize: 13, lineHeight: 1.6,
              fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
              background: '#fff',
              color: 'var(--ws-text-primary, #1a1d21)',
              resize: 'none',
              overflow: 'auto',
              borderRight: mode === 'split' ? '1px solid var(--ws-border, #e6e8eb)' : 'none',
            }}
          />
        )}
        {mode !== 'source' && (
          <div
            style={{
              flex: 1,
              minWidth: 0, minHeight: 300,
              padding: 16,
              overflow: 'auto',
              fontSize: 13, lineHeight: 1.6,
              background: 'var(--ws-surface-alt, #fafbfd)',
            }}
            className="ws-md-preview"
          >
            {content ? (
              <ReactMarkdown remarkPlugins={[remarkGfm]}>
                {content}
              </ReactMarkdown>
            ) : (
              <Text type="secondary" italic>(此文档正文为空)</Text>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
