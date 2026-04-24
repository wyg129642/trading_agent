/**
 * The editor canvas for the workspace. Renders whatever document the
 * active tab (from ``useWorkspaceTabs``) points to:
 *
 *   - workbook  → SpreadsheetEditor with debounced autosave
 *   - markdown  → MarkdownCanvas (split / source / preview)
 *   - file      → legacy detail-viewer (delegated to the parent via
 *                 ``onOpenFileDetail``; this canvas just shows a hint)
 *
 * The canvas also renders the document tab bar at the top.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Alert, Button, Empty, Space, Spin, Typography, message as antdMessage } from 'antd'
import { FileTextOutlined, ReloadOutlined } from '@ant-design/icons'
import DocumentTabBar from './DocumentTabBar'
import SpreadsheetEditor, { SpreadsheetData, Workbook } from '../SpreadsheetEditor'
import MarkdownCanvas from './MarkdownCanvas'
import { useWorkspaceTabs } from '../../store/workspaceTabs'
import api from '../../services/api'

const { Text } = Typography

export interface WorkspaceCanvasProps {
  readonly?: boolean
  onOpenFileDetail?: (documentId: string) => void
}

type SheetState = {
  data: SpreadsheetData
  dirty: boolean
  saving: boolean
  loading: boolean
  error?: string
  title: string
}

export default function WorkspaceCanvas({ readonly, onOpenFileDetail }: WorkspaceCanvasProps) {
  const { tabs, activeId, markDirty } = useWorkspaceTabs()
  const activeTab = useMemo(
    () => tabs.find((t) => t.id === activeId) || null,
    [tabs, activeId],
  )

  // Per-tab workbook state, keyed by tab id. Swapping tabs keeps edits in
  // memory so the user doesn't lose a dirty spreadsheet on a quick switch.
  const [sheetByTab, setSheetByTab] = useState<Record<string, SheetState>>({})
  const saveTimers = useRef<Record<string, number>>({})

  // ── Workbook load ──
  const fetchWorkbook = useCallback(async (docId: string) => {
    setSheetByTab((prev) => ({
      ...prev,
      [docId]: {
        ...(prev[docId] || { data: { active_sheet_id: 'sheet-1', sheets: [] }, dirty: false, saving: false, title: '' }),
        loading: true,
        error: undefined,
      },
    }))
    try {
      const res = await api.get<{
        document_id: string
        title: string
        doc_type: string
        spreadsheet_data: SpreadsheetData
      }>(`/user-kb/documents/${docId}/spreadsheet`)
      setSheetByTab((prev) => ({
        ...prev,
        [docId]: {
          data: res.data.spreadsheet_data,
          dirty: false, saving: false, loading: false,
          title: res.data.title,
        },
      }))
    } catch (err: any) {
      const msg = err?.response?.data?.detail || err.message
      setSheetByTab((prev) => ({
        ...prev,
        [docId]: {
          ...(prev[docId] || { data: { active_sheet_id: 'sheet-1', sheets: [] }, dirty: false, saving: false, title: '' }),
          loading: false,
          error: msg,
        },
      }))
    }
  }, [])

  // ── Workbook autosave ──
  const saveWorkbook = useCallback(async (docId: string) => {
    const st = sheetByTab[docId]
    if (!st || !st.dirty) return
    setSheetByTab((prev) => ({
      ...prev,
      [docId]: { ...prev[docId], saving: true, error: undefined },
    }))
    try {
      await api.patch(
        `/user-kb/documents/${docId}/spreadsheet`,
        { spreadsheet_data: st.data },
      )
      setSheetByTab((prev) => ({
        ...prev,
        [docId]: { ...prev[docId], dirty: false, saving: false, error: undefined },
      }))
      markDirty(docId, false)
    } catch (err: any) {
      const msg = err?.response?.data?.detail || err.message || 'save failed'
      setSheetByTab((prev) => ({
        ...prev,
        [docId]: { ...prev[docId], saving: false, error: msg },
      }))
      antdMessage.error(`保存失败: ${msg}`)
    }
  }, [sheetByTab, markDirty])

  const onWorkbookChange = useCallback((docId: string, next: Workbook) => {
    setSheetByTab((prev) => ({
      ...prev,
      [docId]: {
        ...(prev[docId] as any),
        data: next,
        dirty: true,
      },
    }))
    markDirty(docId, true)
    if (saveTimers.current[docId]) {
      window.clearTimeout(saveTimers.current[docId])
    }
    saveTimers.current[docId] = window.setTimeout(
      () => saveWorkbook(docId),
      1200,
    ) as unknown as number
  }, [markDirty, saveWorkbook])

  // Trigger lazy-load when a workbook tab becomes active. Fires only
  // when we have no cached state at all; once ``fetchWorkbook`` has run
  // (even if it errored), the cached row's presence stops the effect
  // from re-firing on every re-render.
  useEffect(() => {
    if (!activeTab) return
    if (activeTab.kind !== 'workbook') return
    if (sheetByTab[activeTab.id]) return
    fetchWorkbook(activeTab.id)
  }, [activeTab, fetchWorkbook, sheetByTab])

  // Cleanup save timers on unmount.
  useEffect(() => {
    return () => {
      Object.values(saveTimers.current).forEach((t) => window.clearTimeout(t))
    }
  }, [])

  // ── Render ──
  return (
    <div style={{
      display: 'flex', flexDirection: 'column',
      flex: 1, minHeight: 0,
      background: '#fff',
      borderLeft: '1px solid var(--ws-border, #e6e8eb)',
    }}>
      <DocumentTabBar />

      {/* Tab body — explicit minHeight so the child editor has room even if
          the grandparent's flex sizing races the first paint. */}
      <div style={{
        flex: 1, display: 'flex', flexDirection: 'column',
        minHeight: 400, overflow: 'hidden',
      }}>
        {!activeTab ? (
          <Empty
            style={{ marginTop: 60 }}
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description="双击左侧文件以打开"
          />
        ) : activeTab.kind === 'workbook' ? (
          <WorkbookPane
            docId={activeTab.id}
            state={sheetByTab[activeTab.id]}
            readonly={readonly}
            onRetry={() => fetchWorkbook(activeTab.id)}
            onChange={(next) => onWorkbookChange(activeTab.id, next)}
            onSave={() => saveWorkbook(activeTab.id)}
          />
        ) : activeTab.kind === 'markdown' ? (
          <MarkdownCanvas
            documentId={activeTab.id}
            readonly={readonly}
            onDirtyChange={(d) => markDirty(activeTab.id, d)}
          />
        ) : (
          <GenericFilePane
            docId={activeTab.id}
            title={activeTab.title}
            onOpenDetail={onOpenFileDetail}
          />
        )}
      </div>
    </div>
  )
}


function WorkbookPane({
  docId, state, readonly, onRetry, onChange, onSave,
}: {
  docId: string
  state: SheetState | undefined
  readonly?: boolean
  onRetry: () => void
  onChange: (next: Workbook) => void
  onSave: () => void
}) {
  if (!state || state.loading) {
    return <div style={{ padding: 40, textAlign: 'center' }}><Spin /></div>
  }
  if (state.error) {
    return (
      <Alert
        type="error" showIcon style={{ margin: 16 }}
        message="加载估值表失败" description={state.error}
        action={<Button size="small" icon={<ReloadOutlined />} onClick={onRetry}>重试</Button>}
      />
    )
  }
  return (
    <SpreadsheetEditor
      value={state.data}
      readonly={readonly}
      saving={state.saving}
      title={
        <Space size={6}>
          <Text strong>{state.title || '估值表'}</Text>
          <Text type="secondary" style={{ fontSize: 11 }}>
            {state.dirty
              ? '编辑中，1.2 秒后自动保存…'
              : state.saving ? '保存中…' : '已保存'}
            {state.error && ` · 保存失败: ${state.error}`}
          </Text>
        </Space>
      }
      onChange={onChange}
      onSave={onSave}
    />
  )
  // docId is used implicitly via the parent's callbacks; listed in deps
  // outside.
  void docId
}


function GenericFilePane({
  docId, title, onOpenDetail,
}: {
  docId: string
  title: string
  onOpenDetail?: (docId: string) => void
}) {
  return (
    <div style={{ padding: 24, textAlign: 'center' }}>
      <FileTextOutlined style={{ fontSize: 48, color: '#cbd5e1' }} />
      <div style={{ marginTop: 12 }}>
        <Text strong>{title}</Text>
      </div>
      <Text type="secondary" style={{ fontSize: 12, display: 'block', marginTop: 4 }}>
        此文件类型暂不支持直接在工作台中编辑。
      </Text>
      <Button
        type="primary" style={{ marginTop: 16 }}
        onClick={() => onOpenDetail?.(docId)}
      >
        在详情面板中打开
      </Button>
    </div>
  )
}
