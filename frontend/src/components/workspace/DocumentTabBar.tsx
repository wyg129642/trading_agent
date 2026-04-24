/**
 * Browser-style document tab bar for the workspace editor canvas.
 *
 * Renders :func:`useWorkspaceTabs` state. Supports:
 *   - Click to focus
 *   - Middle click / × button to close
 *   - Drag to reorder
 *   - Close confirmation on dirty tabs
 *   - Overflow horizontal scroll
 *   - Kind icons (workbook / markdown / file)
 */

import { useCallback, useRef, useState } from 'react'
import { Tag, Tooltip, Modal } from 'antd'
import {
  CloseOutlined, FileMarkdownOutlined, TableOutlined,
  FileTextOutlined,
} from '@ant-design/icons'
import { useWorkspaceTabs, OpenTab, DocKind } from '../../store/workspaceTabs'

function kindIcon(kind: DocKind) {
  if (kind === 'workbook') {
    return <TableOutlined style={{ color: 'var(--ws-accent, #2ec98a)' }} />
  }
  if (kind === 'markdown') {
    return <FileMarkdownOutlined style={{ color: '#0ea5e9' }} />
  }
  return <FileTextOutlined style={{ color: '#64748b' }} />
}

export default function DocumentTabBar() {
  const { tabs, activeId, activate, close, reorder } = useWorkspaceTabs()
  const dragSrc = useRef<string | null>(null)
  const [hoverTarget, setHoverTarget] = useState<string | null>(null)

  const onClose = useCallback((tab: OpenTab, e?: React.MouseEvent | React.KeyboardEvent) => {
    e?.stopPropagation()
    if (tab.dirty) {
      Modal.confirm({
        title: `"${tab.title}" 有未保存的更改`,
        content: '关闭将丢失这些更改。确定要关闭吗？',
        okText: '关闭', okButtonProps: { danger: true }, cancelText: '取消',
        onOk: () => close(tab.id),
      })
      return
    }
    close(tab.id)
  }, [close])

  if (tabs.length === 0) {
    return (
      <div style={{
        padding: '8px 16px',
        fontSize: 12,
        color: 'var(--ws-text-tertiary, #9ca3af)',
        background: 'var(--ws-surface-alt, #f7f9fb)',
        borderBottom: '1px solid var(--ws-border, #e6e8eb)',
        minHeight: 32,
      }}>
        在左侧双击文件打开
      </div>
    )
  }

  return (
    <div
      style={{
        display: 'flex', alignItems: 'flex-end',
        background: 'var(--ws-surface-alt, #f7f9fb)',
        borderBottom: '1px solid var(--ws-border, #e6e8eb)',
        padding: '4px 6px 0 6px',
        overflowX: 'auto', overflowY: 'hidden',
        minHeight: 34,
        gap: 2,
      }}
    >
      {tabs.map((t) => {
        const isActive = t.id === activeId
        const isDragHover = hoverTarget === t.id
        return (
          <div
            key={t.id}
            draggable
            onDragStart={() => { dragSrc.current = t.id }}
            onDragOver={(e) => { e.preventDefault(); setHoverTarget(t.id) }}
            onDragLeave={() => setHoverTarget(null)}
            onDrop={(e) => {
              e.preventDefault()
              if (dragSrc.current && dragSrc.current !== t.id) {
                reorder(dragSrc.current, t.id)
              }
              dragSrc.current = null
              setHoverTarget(null)
            }}
            onClick={() => activate(t.id)}
            onAuxClick={(e) => {
              if (e.button === 1) onClose(t, e)
            }}
            role="tab"
            aria-selected={isActive}
            title={t.title}
            style={{
              display: 'flex', alignItems: 'center', gap: 6,
              padding: '6px 8px 6px 10px',
              borderRadius: '6px 6px 0 0',
              background: isActive ? '#fff' : isDragHover ? 'var(--ws-accent-soft, #e8f8f0)' : 'transparent',
              borderTop: isActive ? '1px solid var(--ws-border, #e6e8eb)' : '1px solid transparent',
              borderLeft: isActive ? '1px solid var(--ws-border, #e6e8eb)' : '1px solid transparent',
              borderRight: isActive ? '1px solid var(--ws-border, #e6e8eb)' : '1px solid transparent',
              borderBottom: isActive ? '1px solid #fff' : '1px solid transparent',
              color: isActive ? 'var(--ws-text-primary, #1a1d21)' : 'var(--ws-text-secondary, #6b7280)',
              cursor: 'pointer',
              fontSize: 12,
              fontWeight: isActive ? 500 : 400,
              maxWidth: 240,
              whiteSpace: 'nowrap',
              userSelect: 'none',
              position: 'relative', top: 1,
            }}
          >
            {kindIcon(t.kind)}
            <span
              style={{
                overflow: 'hidden', textOverflow: 'ellipsis',
                maxWidth: 160,
              }}
            >
              {t.title}
              {t.dirty && <span style={{ color: 'var(--ws-accent, #2ec98a)' }}> ●</span>}
            </span>
            {t.stockTicker && (
              <Tag
                color="default"
                style={{ margin: 0, fontSize: 10, padding: '0 4px', lineHeight: '14px' }}
              >
                {t.stockTicker}
              </Tag>
            )}
            <Tooltip title="关闭 (Ctrl+W)">
              <CloseOutlined
                onClick={(e) => onClose(t, e)}
                style={{
                  fontSize: 10, padding: 2, borderRadius: 3,
                  opacity: isActive ? 0.8 : 0.5,
                }}
              />
            </Tooltip>
          </div>
        )
      })}
    </div>
  )
}
