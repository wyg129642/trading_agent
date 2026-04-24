/**
 * Tighter, VS Code-style folder + document tree for the workspace.
 *
 * - 24px rows, 8px indent (AntD default is 32 / 24, too roomy).
 * - Folders AND documents render as nodes so users can double-click a
 *   document to open it as a tab in the editor canvas.
 * - Right-click context menu: folder CRUD + "安装 Skill".
 * - Drag-drop is supported at the AntD level (HTML5 DnD).
 *
 * The tree is a controlled component — the parent manages:
 *   - selected folder key (for upload target, skills target, file list)
 *   - expanded keys (persisted in localStorage by the parent)
 */

import { useMemo } from 'react'
import { Badge, Dropdown, Space, Tag, Tree, Typography } from 'antd'
import type { DataNode } from 'antd/es/tree'
import type { MenuProps } from 'antd'
import {
  FolderOutlined, FolderOpenOutlined, LineChartOutlined,
  TagOutlined, TableOutlined, FileMarkdownOutlined, FileTextOutlined,
  FilePdfOutlined, FileWordOutlined, SoundOutlined, MoreOutlined,
  PlusOutlined, EditOutlined, DeleteOutlined, DatabaseOutlined,
  FileUnknownOutlined,
} from '@ant-design/icons'

const { Text } = Typography
const { DirectoryTree } = Tree

export interface FolderNode {
  id: string
  user_id: string | null
  scope: 'personal' | 'public'
  parent_id: string | null
  name: string
  folder_type: 'stock' | 'industry' | 'general'
  stock_ticker: string | null
  stock_market: string | null
  stock_name: string | null
  order_index: number
  created_at: string
  updated_at: string
  document_count: number
  children: FolderNode[]
}

export interface DocumentLite {
  id: string
  title: string
  original_filename: string
  file_extension: string
  folder_id: string | null
  doc_type: string
  parse_status: string
}

// Node key conventions (match MyKnowledgeBase usage):
export const UNFILED_KEY = '__unfiled__'
export const ALL_KEY = '__all__'
const DOC_KEY_PREFIX = 'doc:'

export function docKey(id: string): string {
  return `${DOC_KEY_PREFIX}${id}`
}
export function isDocKey(key: string): boolean {
  return key.startsWith(DOC_KEY_PREFIX)
}
export function docIdFromKey(key: string): string {
  return key.slice(DOC_KEY_PREFIX.length)
}

function folderIcon(t: FolderNode['folder_type']) {
  if (t === 'stock') return <LineChartOutlined style={{ color: 'var(--ws-accent, #2ec98a)' }} />
  if (t === 'industry') return <TagOutlined style={{ color: '#0ea5e9' }} />
  return <FolderOutlined style={{ color: 'var(--ws-text-secondary, #6b7280)' }} />
}

function docIcon(doc: DocumentLite): React.ReactNode {
  if (doc.doc_type === 'workbook' || doc.doc_type === 'spreadsheet') {
    return <TableOutlined style={{ color: 'var(--ws-accent, #2ec98a)' }} />
  }
  if (doc.doc_type === 'markdown') {
    return <FileMarkdownOutlined style={{ color: '#0ea5e9' }} />
  }
  const ext = (doc.file_extension || '').toLowerCase()
  if (ext === 'pdf') return <FilePdfOutlined style={{ color: '#ef4444' }} />
  if (ext === 'docx') return <FileWordOutlined style={{ color: '#2563eb' }} />
  if (ext === 'md' || ext === 'markdown') return <FileMarkdownOutlined style={{ color: '#0ea5e9' }} />
  if (['mp3', 'wav', 'm4a', 'flac', 'ogg', 'opus', 'webm', 'aac'].includes(ext)) {
    return <SoundOutlined style={{ color: '#7c3aed' }} />
  }
  if (['txt', 'csv', 'json', 'html', 'htm'].includes(ext)) {
    return <FileTextOutlined style={{ color: '#64748b' }} />
  }
  return <FileUnknownOutlined style={{ color: '#94a3b8' }} />
}


export interface WorkspaceTreeProps {
  tree: FolderNode[]
  unfiledCount: number
  scope: 'personal' | 'public'
  canWrite: boolean
  selectedKey: string
  expandedKeys: React.Key[]
  // Documents per folder, keyed by folder_id. Pass what you have; empty
  // folders simply show a (∅) state. Unfiled docs go under the UNFILED_KEY.
  docsByFolder: Record<string, DocumentLite[]>
  onSelect: (key: string) => void
  onExpand: (keys: React.Key[]) => void
  onOpenDoc: (doc: DocumentLite) => void
  onCreateFolder: (parent: FolderNode | null) => void
  onRenameFolder: (f: FolderNode) => void
  onDeleteFolder: (f: FolderNode) => void
  onInstallSkillTo?: (f: FolderNode) => void
  onCreateMarkdown?: (parent: FolderNode) => void
}

export default function WorkspaceTree({
  tree, unfiledCount, scope, canWrite,
  selectedKey, expandedKeys,
  docsByFolder,
  onSelect, onExpand, onOpenDoc,
  onCreateFolder, onRenameFolder, onDeleteFolder,
  onInstallSkillTo, onCreateMarkdown,
}: WorkspaceTreeProps) {
  const data: DataNode[] = useMemo(() => {
    const mapFolder = (n: FolderNode): DataNode => {
      const docs = docsByFolder[n.id] || []
      // Documents as leaf children (in addition to sub-folders).
      const docNodes: DataNode[] = docs.map((d) => ({
        key: docKey(d.id),
        title: (
          <div
            title={d.original_filename}
            onDoubleClick={(e) => { e.stopPropagation(); onOpenDoc(d) }}
            style={{
              display: 'flex', alignItems: 'center', gap: 6,
              fontSize: 12, lineHeight: '20px',
            }}
          >
            {docIcon(d)}
            <Text ellipsis style={{ maxWidth: 180 }}>{d.title || d.original_filename}</Text>
          </div>
        ),
        isLeaf: true,
      }))

      const titleEl = (
        <div
          onContextMenu={(e) => e.preventDefault()}
          style={{
            display: 'flex', alignItems: 'center',
            justifyContent: 'space-between', width: '100%', gap: 4,
          }}
        >
          <Space size={6} style={{ minWidth: 0 }}>
            {folderIcon(n.folder_type)}
            <Text ellipsis style={{ maxWidth: 160, fontSize: 13 }}>{n.name}</Text>
            {n.folder_type === 'stock' && n.stock_ticker && (
              <Tag
                color="default"
                style={{
                  margin: 0, fontSize: 10,
                  padding: '0 4px', lineHeight: '16px',
                }}
              >
                {n.stock_ticker}
              </Tag>
            )}
            {n.document_count > 0 && (
              <Badge
                count={n.document_count}
                style={{ backgroundColor: '#94a3b8' }}
                size="small"
              />
            )}
          </Space>
          {canWrite && (
            <FolderRowActions
              node={n}
              canInstallSkill={!!onInstallSkillTo}
              canCreateMarkdown={!!onCreateMarkdown}
              onAdd={() => onCreateFolder(n)}
              onRename={() => onRenameFolder(n)}
              onDelete={() => onDeleteFolder(n)}
              onInstallSkill={() => onInstallSkillTo?.(n)}
              onCreateMarkdown={() => onCreateMarkdown?.(n)}
            />
          )}
        </div>
      )
      return {
        key: n.id,
        title: titleEl,
        children: [
          ...(n.children || []).map(mapFolder),
          ...docNodes,
        ],
        isLeaf: (n.children?.length || 0) === 0 && docNodes.length === 0,
      }
    }

    const folderNodes = tree.map(mapFolder)

    // Unfiled bucket
    const unfiledDocs = docsByFolder[UNFILED_KEY] || []
    const unfiled: DataNode = {
      key: UNFILED_KEY,
      title: (
        <Space size={6}>
          <FolderOpenOutlined style={{ color: '#94a3b8' }} />
          <Text style={{ fontSize: 13 }}>未归档</Text>
          {unfiledCount > 0 && (
            <Badge count={unfiledCount} style={{ backgroundColor: '#94a3b8' }} size="small" />
          )}
        </Space>
      ),
      isLeaf: unfiledDocs.length === 0,
      children: unfiledDocs.map((d) => ({
        key: docKey(d.id),
        title: (
          <div
            onDoubleClick={(e) => { e.stopPropagation(); onOpenDoc(d) }}
            style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12 }}
          >
            {docIcon(d)}
            <Text ellipsis style={{ maxWidth: 180 }}>{d.title || d.original_filename}</Text>
          </div>
        ),
        isLeaf: true,
      })),
    }

    const root: DataNode = {
      key: ALL_KEY,
      title: (
        <Space size={6}>
          <DatabaseOutlined style={{ color: 'var(--ws-accent, #2ec98a)' }} />
          <Text strong style={{ fontSize: 13 }}>
            {scope === 'public' ? '公共工作区' : '我的工作台'}
          </Text>
        </Space>
      ),
      isLeaf: false,
      children: [...folderNodes, unfiled],
    }
    return [root]
  }, [tree, unfiledCount, scope, canWrite, docsByFolder,
      onOpenDoc, onCreateFolder, onRenameFolder, onDeleteFolder,
      onInstallSkillTo, onCreateMarkdown])

  return (
    <DirectoryTree
      treeData={data}
      selectedKeys={[selectedKey]}
      expandedKeys={expandedKeys}
      onExpand={(keys) => onExpand(keys)}
      onSelect={(keys, info) => {
        if (keys.length === 0) return
        const key = String(keys[0])
        onSelect(key)
        if (isDocKey(key)) {
          // Deep-lookup the doc object to open in a tab. We walk the flat
          // list rather than extracting from DataNode to keep types happy.
          const docId = docIdFromKey(key)
          for (const list of Object.values(docsByFolder)) {
            const found = list.find((d) => d.id === docId)
            if (found) {
              onOpenDoc(found)
              break
            }
          }
        }
        // Swallow unused `info`.
        void info
      }}
      blockNode
      showIcon={false}
    />
  )
}


function FolderRowActions({
  onAdd, onRename, onDelete, onInstallSkill, onCreateMarkdown,
  canInstallSkill, canCreateMarkdown,
}: {
  node: FolderNode
  onAdd: () => void
  onRename: () => void
  onDelete: () => void
  onInstallSkill: () => void
  onCreateMarkdown: () => void
  canInstallSkill: boolean
  canCreateMarkdown: boolean
}) {
  const items: MenuProps['items'] = [
    { key: 'add', label: '新建子目录', icon: <PlusOutlined /> },
    ...(canCreateMarkdown ? [{ key: 'md', label: '新建 Markdown 文档', icon: <FileMarkdownOutlined /> }] : []),
    ...(canInstallSkill ? [{ key: 'skill', label: '安装 Skill', icon: <TableOutlined /> }] : []),
    { key: 'rename', label: '重命名', icon: <EditOutlined /> },
    { key: 'delete', label: '删除', icon: <DeleteOutlined />, danger: true },
  ]
  return (
    <Dropdown
      trigger={['click']}
      menu={{
        items,
        onClick: ({ key, domEvent }) => {
          domEvent.stopPropagation()
          if (key === 'add') onAdd()
          else if (key === 'md') onCreateMarkdown()
          else if (key === 'skill') onInstallSkill()
          else if (key === 'rename') onRename()
          else if (key === 'delete') onDelete()
        },
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          padding: 2, cursor: 'pointer',
          color: 'var(--ws-text-tertiary, #9ca3af)',
          borderRadius: 3,
        }}
      >
        <MoreOutlined />
      </div>
    </Dropdown>
  )
}
