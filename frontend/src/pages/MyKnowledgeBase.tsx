/**
 * Personal Knowledge Base — PaiWork-style 3-pane workspace.
 *
 * Layout:
 *   ┌──────┬──────────────────────────┬──────────────────────────┐
 *   │ rail │ workspace panel          │ editor canvas            │
 *   │ 48px │  tabs: 工作区 | Skills   │  document tab bar        │
 *   │      │  scope: 个人 | 公共       │  active doc editor       │
 *   │      │  folder tree + docs      │  (workbook / markdown)   │
 *   └──────┴──────────────────────────┴──────────────────────────┘
 *
 * The shell wraps itself in ``ConfigProvider`` with the green workspace
 * theme tokens so the rest of the app keeps its indigo-blue brand.
 *
 * Folder CRUD, upload, audio transcript viewer and the detail drawer are
 * preserved from the previous implementation — only the layout around
 * them is new. Tabs live in a Zustand store so they persist across folder
 * navigation and survive page refreshes via sessionStorage.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Alert, Badge, Button, Card, ConfigProvider, Descriptions, Drawer, Empty,
  Form, Input, Modal, Popconfirm, Progress, Row, Select, Space, Spin,
  Tabs, Tag, Tooltip, Typography, Upload, message as antdMessage,
} from 'antd'
import type { UploadProps } from 'antd/es/upload/interface'
import {
  InboxOutlined, DeleteOutlined, EditOutlined, ReloadOutlined,
  FileTextOutlined,
  FilePdfOutlined, FileWordOutlined, FileUnknownOutlined,
  FileMarkdownOutlined, DownloadOutlined,
  SoundOutlined, FolderOutlined, FolderOpenOutlined,
  PlusOutlined, TagOutlined, LineChartOutlined, FolderAddOutlined,
  SearchOutlined, AppstoreOutlined, ThunderboltOutlined,
} from '@ant-design/icons'
import api from '../services/api'
import { useAuthStore } from '../store/auth'
import AudioTranscriptViewer, {
  AudioMeta, isAudioDoc,
} from '../components/AudioTranscriptViewer'
import WorkspaceRail from '../components/workspace/WorkspaceRail'
import WorkspaceTree, {
  ALL_KEY, UNFILED_KEY, docKey, isDocKey, docIdFromKey,
  FolderNode, DocumentLite,
} from '../components/workspace/WorkspaceTree'
import WorkspaceCanvas from '../components/workspace/WorkspaceCanvas'
import SkillsPanel from '../components/workspace/SkillsPanel'
import { useWorkspaceTabs, DocKind } from '../store/workspaceTabs'

const { Text } = Typography
const { Dragger } = Upload

// ── Types (kept in sync with backend/app/api/user_kb.py) ────────

interface DocumentRow extends DocumentLite {
  user_id: string
  description: string
  tags: string[]
  content_type: string
  file_size_bytes: number
  upload_status: 'uploading' | 'completed' | 'failed'
  upload_error: string | null
  parse_error: string | null
  parser_backend: string | null
  parse_warnings: string[]
  parse_progress_percent: number
  parse_phase: string
  extracted_char_count: number
  num_chunks: number
  scope: 'personal' | 'public'
  created_at: string
  updated_at: string
  parse_started_at: string | null
  parse_completed_at: string | null
  audio: AudioMeta | null
}

interface TreeResponse {
  scope: 'personal' | 'public'
  folders: FolderNode[]
  unfiled_count: number
  can_write: boolean
}

interface StockSuggestion {
  code: string
  name: string
  market: string
  label?: string
  rank?: number
}

// ── Constants ──────────────────────────────────────────────────

const ACCEPT =
  '.pdf,.md,.markdown,.txt,.text,.docx,.csv,.json,.html,.htm,' +
  '.mp3,.wav,.m4a,.flac,.ogg,.opus,.webm,.aac,' +
  'application/pdf,text/markdown,text/plain,audio/*'
const AUDIO_EXTS = new Set([
  'mp3', 'wav', 'm4a', 'flac', 'ogg', 'opus', 'webm', 'aac',
])
const MAX_FILE_MB = 50
const MAX_AUDIO_MB = 500

function isAudioFilename(name: string): boolean {
  const idx = name.lastIndexOf('.')
  if (idx < 0) return false
  return AUDIO_EXTS.has(name.slice(idx + 1).toLowerCase())
}

function humanSize(bytes: number): string {
  if (!bytes) return '0 B'
  const k = 1024
  const units = ['B', 'KB', 'MB', 'GB']
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(k)), units.length - 1)
  return `${(bytes / Math.pow(k, i)).toFixed(i === 0 ? 0 : 1)} ${units[i]}`
}

function fileIcon(ext: string) {
  const e = (ext || '').toLowerCase()
  if (e === 'pdf') return <FilePdfOutlined style={{ color: '#ef4444' }} />
  if (e === 'docx') return <FileWordOutlined style={{ color: '#2563eb' }} />
  if (e === 'md' || e === 'markdown') return <FileMarkdownOutlined style={{ color: '#0ea5e9' }} />
  if (AUDIO_EXTS.has(e)) return <SoundOutlined style={{ color: '#7c3aed' }} />
  if (['txt', 'csv', 'json', 'html', 'htm'].includes(e)) {
    return <FileTextOutlined style={{ color: '#64748b' }} />
  }
  return <FileUnknownOutlined style={{ color: '#94a3b8' }} />
}

interface UploadProgressRow {
  uid: string
  name: string
  percent: number
  status: 'uploading' | 'processing' | 'done' | 'error'
  error?: string
}

// ── Theme scoped to this page ───────────────────────────────────

const workspaceTheme = {
  token: {
    colorPrimary: '#2ec98a',
    colorLink: '#2ec98a',
    colorLinkHover: '#23b579',
    borderRadius: 6,
  },
}

// ── Main ────────────────────────────────────────────────────────

export default function MyKnowledgeBase() {
  const navigate = useNavigate()
  const currentUser = useAuthStore((s) => s.user)
  const isAdminOrBoss =
    currentUser?.role === 'admin' || currentUser?.role === 'boss'

  // ── State ────────────────────────────────────────────────────

  const [scope, setScope] = useState<'personal' | 'public'>('personal')
  const [panelTab, setPanelTab] = useState<'tree' | 'skills'>('tree')
  const [tree, setTree] = useState<FolderNode[]>([])
  const [unfiledCount, setUnfiledCount] = useState(0)
  const [canWrite, setCanWrite] = useState(true)
  const [treeLoading, setTreeLoading] = useState(false)
  const [selectedKey, setSelectedKey] = useState<string>(ALL_KEY)
  const [expandedKeys, setExpandedKeys] = useState<React.Key[]>([ALL_KEY])

  const [allDocs, setAllDocs] = useState<DocumentRow[]>([])
  const [docsLoading, setDocsLoading] = useState(false)
  const [pingOk, setPingOk] = useState<boolean | null>(null)
  const [pingMsg, setPingMsg] = useState('')
  const [asrOk, setAsrOk] = useState<boolean | null>(null)
  const [asrMsg, setAsrMsg] = useState('')
  const [treeSearch, setTreeSearch] = useState('')

  const [uploadRows, setUploadRows] = useState<UploadProgressRow[]>([])

  // Folder create/rename modal
  const [folderModalOpen, setFolderModalOpen] = useState(false)
  const [folderMode, setFolderMode] = useState<'create' | 'rename'>('create')
  const [folderParentId, setFolderParentId] = useState<string | null>(null)
  const [folderEditing, setFolderEditing] = useState<FolderNode | null>(null)
  const [folderForm] = Form.useForm()
  const [folderSaving, setFolderSaving] = useState(false)
  const folderTypeWatched = Form.useWatch('folder_type', folderForm)

  // New markdown modal
  const [mdModalOpen, setMdModalOpen] = useState(false)
  const [mdModalFolder, setMdModalFolder] = useState<FolderNode | null>(null)
  const [mdForm] = Form.useForm()
  const [mdSaving, setMdSaving] = useState(false)

  // Stock picker
  const [stockOptions, setStockOptions] = useState<StockSuggestion[]>([])
  const [stockSearching, setStockSearching] = useState(false)
  const stockTimerRef = useRef<number | null>(null)

  // Detail drawer — kept for files that can't be edited inline (audio, PDF)
  const [detailOpen, setDetailOpen] = useState(false)
  const [detailDoc, setDetailDoc] = useState<DocumentRow | null>(null)
  const [detailContent, setDetailContent] = useState('')
  const [detailLoading, setDetailLoading] = useState(false)

  // Poll bookkeeping
  const pollTimerRef = useRef<number | null>(null)
  const pollAttemptsRef = useRef(0)
  const progressSnapshotRef = useRef('')
  const MAX_POLL_ATTEMPTS = 120

  const tabsStore = useWorkspaceTabs()

  // ── Derived ─────────────────────────────────────────────────

  const folderById = useMemo(() => {
    const map: Record<string, FolderNode> = {}
    const walk = (nodes: FolderNode[]) => {
      for (const n of nodes) {
        map[n.id] = n
        if (n.children?.length) walk(n.children)
      }
    }
    walk(tree)
    return map
  }, [tree])

  const selectedFolder: FolderNode | null =
    selectedKey && selectedKey !== ALL_KEY && selectedKey !== UNFILED_KEY && !isDocKey(selectedKey)
      ? folderById[selectedKey] || null
      : null

  const createParent: FolderNode | null = folderParentId
    ? folderById[folderParentId] || null
    : null
  const parentIsStock = createParent?.folder_type === 'stock'

  // Docs grouped by folder — consumed by the WorkspaceTree.
  const docsByFolder = useMemo(() => {
    const q = treeSearch.trim().toLowerCase()
    const filter = (d: DocumentRow) => {
      if (!q) return true
      return (
        (d.title || '').toLowerCase().includes(q)
        || (d.original_filename || '').toLowerCase().includes(q)
      )
    }
    const groups: Record<string, DocumentLite[]> = {}
    for (const d of allDocs) {
      if (!filter(d)) continue
      const key = d.folder_id || UNFILED_KEY
      if (!groups[key]) groups[key] = []
      groups[key].push(d)
    }
    return groups
  }, [allDocs, treeSearch])

  // ── Data loading ────────────────────────────────────────────

  const fetchPing = useCallback(async () => {
    try {
      const res = await api.get<{ ok: boolean; message: string }>('/user-kb/ping')
      setPingOk(res.data.ok); setPingMsg(res.data.message)
    } catch (err: any) {
      setPingOk(false); setPingMsg(err?.message || 'ping failed')
    }
  }, [])

  const fetchAsrPing = useCallback(async () => {
    try {
      const res = await api.get<{ ok: boolean; message: string }>('/user-kb/asr/ping')
      setAsrOk(res.data.ok); setAsrMsg(res.data.message)
    } catch (err: any) {
      setAsrOk(false); setAsrMsg(err?.message || 'asr ping failed')
    }
  }, [])

  const fetchTree = useCallback(async () => {
    setTreeLoading(true)
    try {
      const res = await api.get<TreeResponse>(
        '/user-kb/tree', { params: { scope } },
      )
      setTree(res.data.folders)
      setUnfiledCount(res.data.unfiled_count)
      setCanWrite(res.data.can_write)
    } catch (err: any) {
      antdMessage.error(`加载目录失败: ${err?.response?.data?.detail || err.message}`)
    } finally {
      setTreeLoading(false)
    }
  }, [scope])

  const fetchAllDocs = useCallback(async () => {
    setDocsLoading(true)
    try {
      // Backend caps ``limit`` at 200; page through so a user with many
      // holdings (each stock folder seeds ~2-3 docs) still gets the full
      // picture without a 422. Stop early once we've drained ``total`` or
      // we hit a sane safety cap (10 pages = 2 000 docs).
      const PAGE = 200
      const MAX_PAGES = 10
      let offset = 0
      let collected: DocumentRow[] = []
      for (let i = 0; i < MAX_PAGES; i++) {
        const res = await api.get<{ items: DocumentRow[]; total: number }>(
          '/user-kb/documents',
          { params: { scope, limit: PAGE, offset } },
        )
        const items = res.data.items || []
        collected = collected.concat(items)
        if (items.length < PAGE) break
        offset += PAGE
        if (collected.length >= (res.data.total || 0)) break
      }
      setAllDocs(collected)
    } catch (err: any) {
      antdMessage.error(`加载文档失败: ${err?.response?.data?.detail || err.message}`)
    } finally {
      setDocsLoading(false)
    }
  }, [scope])

  useEffect(() => { fetchPing(); fetchAsrPing() }, [fetchPing, fetchAsrPing])

  // When scope flips, refresh everything and reset selection.
  useEffect(() => {
    setSelectedKey(ALL_KEY)
    setExpandedKeys([ALL_KEY])
    fetchTree()
    fetchAllDocs()
  }, [scope, fetchTree, fetchAllDocs])

  // Poll while any doc is still parsing.
  const hasInFlight = useMemo(
    () => allDocs.some((d) => d.parse_status === 'pending' || d.parse_status === 'parsing'),
    [allDocs],
  )
  useEffect(() => {
    if (!hasInFlight) {
      pollAttemptsRef.current = 0
      progressSnapshotRef.current = ''
    }
  }, [hasInFlight])
  useEffect(() => {
    const snapshot = allDocs
      .filter((d) => d.parse_status === 'pending' || d.parse_status === 'parsing')
      .map((d) => `${d.id}:${d.parse_status}:${d.parse_progress_percent}`)
      .sort().join('|')
    if (snapshot !== progressSnapshotRef.current) {
      progressSnapshotRef.current = snapshot
      pollAttemptsRef.current = 0
    }
  }, [allDocs])
  useEffect(() => {
    if (pollTimerRef.current) {
      window.clearTimeout(pollTimerRef.current); pollTimerRef.current = null
    }
    if (hasInFlight && pollAttemptsRef.current < MAX_POLL_ATTEMPTS) {
      pollTimerRef.current = window.setTimeout(() => {
        pollAttemptsRef.current += 1
        fetchAllDocs()
        fetchTree()
      }, 2500)
    }
    return () => {
      if (pollTimerRef.current) {
        window.clearTimeout(pollTimerRef.current); pollTimerRef.current = null
      }
    }
  }, [hasInFlight, fetchAllDocs, fetchTree, allDocs])

  // ── Document tab ops ─────────────────────────────────────────

  const resolveDocKind = useCallback((doc: DocumentLite): DocKind => {
    if (doc.doc_type === 'workbook' || doc.doc_type === 'spreadsheet') {
      return 'workbook'
    }
    if (doc.doc_type === 'markdown') return 'markdown'
    return 'file'
  }, [])

  const openDocAsTab = useCallback((doc: DocumentLite) => {
    const kind = resolveDocKind(doc)
    const folder = doc.folder_id ? folderById[doc.folder_id] : null
    const stockTicker = folder?.folder_type === 'stock' ? folder.stock_ticker : null
    if (kind === 'file') {
      // Audio files get a dedicated detail page (AlphaPai-style split layout).
      // Other generic files (PDF, docx, ...) stay in the legacy drawer.
      if (isAudioDoc(doc.file_extension)) {
        navigate(`/my-knowledge/audio/${doc.id}`)
        return
      }
      const full = allDocs.find((d) => d.id === doc.id)
      if (full) openDetail(full)
      return
    }
    tabsStore.open({
      id: doc.id,
      title: doc.title || doc.original_filename,
      kind,
      folderId: doc.folder_id,
      stockTicker,
    })
  }, [resolveDocKind, folderById, allDocs, tabsStore, navigate])

  // For stock folders we also expose the default 估值表 as a single-click
  // open — matches the old "估值表 tab" UX.
  const openStockValuation = useCallback(async (folder: FolderNode) => {
    if (folder.folder_type !== 'stock') return
    try {
      const res = await api.get<{
        document_id: string; title: string; doc_type: string
      }>(`/user-kb/folders/${folder.id}/default-spreadsheet`)
      tabsStore.open({
        id: res.data.document_id,
        title: res.data.title,
        kind: 'workbook',
        folderId: folder.id,
        stockTicker: folder.stock_ticker,
      })
    } catch (err: any) {
      antdMessage.error(`打开估值表失败: ${err?.response?.data?.detail || err.message}`)
    }
  }, [tabsStore])

  // ── Folder CRUD ─────────────────────────────────────────────

  const openCreateFolder = useCallback((parent: FolderNode | null) => {
    setFolderMode('create')
    setFolderEditing(null)
    setFolderParentId(parent ? parent.id : null)
    folderForm.resetFields()
    folderForm.setFieldsValue({ folder_type: 'general', name: '' })
    setFolderModalOpen(true)
  }, [folderForm])

  const openRenameFolder = useCallback((f: FolderNode) => {
    setFolderMode('rename')
    setFolderEditing(f)
    setFolderParentId(f.parent_id)
    folderForm.resetFields()
    folderForm.setFieldsValue({
      name: f.name,
      folder_type: f.folder_type,
      stock_value: f.stock_ticker
        ? {
            value: f.stock_ticker,
            label: `${f.stock_name || f.stock_ticker} (${f.stock_ticker})`,
          }
        : undefined,
    })
    setFolderModalOpen(true)
  }, [folderForm])

  const saveFolder = useCallback(async () => {
    const vals = await folderForm.validateFields()
    setFolderSaving(true)
    try {
      if (folderMode === 'create') {
        const payload: any = {
          scope, name: vals.name,
          folder_type: vals.folder_type, parent_id: folderParentId,
        }
        if (vals.folder_type === 'stock') {
          const sv: any = vals.stock_value
          if (!sv || !sv.value) {
            antdMessage.error('股票型目录需要绑定一只股票')
            setFolderSaving(false); return
          }
          const match = stockOptions.find((o) => o.code === sv.value)
          payload.stock_ticker = sv.value
          payload.stock_market = match?.market || ''
          payload.stock_name = match?.name || ''
          payload.name = match?.name || sv.value
        }
        await api.post('/user-kb/folders', payload)
        antdMessage.success('目录已创建')
      } else if (folderEditing) {
        await api.patch(`/user-kb/folders/${folderEditing.id}`, { name: vals.name })
        antdMessage.success('已重命名')
      }
      setFolderModalOpen(false)
      await Promise.all([fetchTree(), fetchAllDocs()])
    } catch (err: any) {
      antdMessage.error(`操作失败: ${err?.response?.data?.detail || err.message}`)
    } finally {
      setFolderSaving(false)
    }
  }, [folderMode, folderEditing, folderForm, folderParentId, scope,
      stockOptions, fetchTree, fetchAllDocs])

  const deleteFolder = useCallback(async (f: FolderNode) => {
    try {
      const res = await api.delete<{
        ok: boolean; deleted_folders: number; deleted_documents: number
      }>(`/user-kb/folders/${f.id}`)
      antdMessage.success(
        `已删除 ${res.data.deleted_folders} 个目录、${res.data.deleted_documents} 份文档`,
      )
      if (selectedKey === f.id) setSelectedKey(ALL_KEY)
      await Promise.all([fetchTree(), fetchAllDocs()])
    } catch (err: any) {
      antdMessage.error(`删除失败: ${err?.response?.data?.detail || err.message}`)
    }
  }, [selectedKey, fetchTree, fetchAllDocs])

  const confirmDeleteFolder = useCallback((f: FolderNode) => {
    Modal.confirm({
      title: `确认删除 "${f.name}" ？`,
      content: f.document_count > 0
        ? `将同时删除目录下 ${f.document_count} 份文档（不可恢复）`
        : '将删除此目录及其所有子目录。',
      okText: '删除',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: () => deleteFolder(f),
    })
  }, [deleteFolder])

  // ── Markdown create ──────────────────────────────────────────

  const openCreateMarkdown = useCallback((parent: FolderNode) => {
    setMdModalFolder(parent)
    mdForm.resetFields()
    mdForm.setFieldsValue({
      title: '新笔记',
      original_filename: 'notes.md',
      content_md: `# ${parent.name} 笔记\n\n`,
    })
    setMdModalOpen(true)
  }, [mdForm])

  const saveMarkdown = useCallback(async () => {
    if (!mdModalFolder) return
    const vals = await mdForm.validateFields()
    setMdSaving(true)
    try {
      const res = await api.post<{ id: string; title: string; folder_id: string }>(
        '/user-kb/documents/markdown',
        {
          title: vals.title,
          original_filename: vals.original_filename,
          folder_id: mdModalFolder.id,
          scope: mdModalFolder.scope,
          content_md: vals.content_md || '',
        },
      )
      antdMessage.success('已创建')
      setMdModalOpen(false)
      await Promise.all([fetchTree(), fetchAllDocs()])
      // Open the new doc as a tab.
      tabsStore.open({
        id: res.data.id,
        title: res.data.title,
        kind: 'markdown',
        folderId: mdModalFolder.id,
      })
    } catch (err: any) {
      antdMessage.error(`创建失败: ${err?.response?.data?.detail || err.message}`)
    } finally {
      setMdSaving(false)
    }
  }, [mdModalFolder, mdForm, fetchTree, fetchAllDocs, tabsStore])

  // ── Stock suggest ───────────────────────────────────────────

  const debouncedStockSearch = (q: string) => {
    if (stockTimerRef.current) {
      window.clearTimeout(stockTimerRef.current); stockTimerRef.current = null
    }
    if (!q || !q.trim()) { setStockOptions([]); return }
    stockTimerRef.current = window.setTimeout(async () => {
      setStockSearching(true)
      try {
        const res = await api.get<StockSuggestion[]>(
          '/stock/suggest', { params: { q: q.trim(), limit: 10 } },
        )
        setStockOptions(Array.isArray(res.data) ? res.data : [])
      } catch {
        setStockOptions([])
      } finally {
        setStockSearching(false)
      }
    }, 200) as unknown as number
  }

  // ── Upload ──────────────────────────────────────────────────

  const uploadTargetFolderId = useMemo(() => {
    if (selectedKey === ALL_KEY || selectedKey === UNFILED_KEY) return null
    if (isDocKey(selectedKey)) return null
    return selectedKey
  }, [selectedKey])

  const uploadProps: UploadProps = {
    name: 'file',
    multiple: true,
    accept: ACCEPT,
    showUploadList: false,
    disabled: !canWrite,
    beforeUpload: (file) => {
      const limitMb = isAudioFilename(file.name) ? MAX_AUDIO_MB : MAX_FILE_MB
      if (file.size > limitMb * 1024 * 1024) {
        antdMessage.error(`${file.name} 超过 ${limitMb} MB 上限`)
        return Upload.LIST_IGNORE
      }
      return true
    },
    customRequest: async (opts) => {
      const { file, onSuccess, onError } = opts
      const f = file as File
      const uid = `u-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
      setUploadRows((rows) => [
        ...rows, { uid, name: f.name, percent: 0, status: 'uploading' },
      ])
      const form = new FormData()
      form.append('file', f, f.name)
      if (uploadTargetFolderId) form.append('folder_id', uploadTargetFolderId)
      form.append('scope', scope)
      try {
        await api.post('/user-kb/documents', form, {
          headers: { 'Content-Type': 'multipart/form-data' },
          timeout: 0,
          onUploadProgress: (e) => {
            if (!e.total) return
            const pct = Math.round((e.loaded / e.total) * 100)
            setUploadRows((rows) =>
              rows.map((r) =>
                r.uid === uid
                  ? { ...r, percent: pct, status: pct >= 100 ? 'processing' : 'uploading' }
                  : r,
              ),
            )
          },
        })
        setUploadRows((rows) =>
          rows.map((r) => (r.uid === uid ? { ...r, percent: 100, status: 'done' } : r)),
        )
        onSuccess?.('ok')
        antdMessage.success(`${f.name} 已上传，后台解析中`)
        await Promise.all([fetchAllDocs(), fetchTree()])
      } catch (err: any) {
        const msg = err?.response?.data?.detail || err.message || 'upload failed'
        setUploadRows((rows) =>
          rows.map((r) => (r.uid === uid ? { ...r, status: 'error', error: msg } : r)),
        )
        antdMessage.error(`${f.name} 上传失败: ${msg}`)
        onError?.(err)
      }
    },
  }

  const clearUploadRows = () =>
    setUploadRows((rows) =>
      rows.filter((r) => r.status === 'uploading' || r.status === 'processing'),
    )

  // ── Detail drawer ───────────────────────────────────────────

  const openDetail = useCallback(async (doc: DocumentRow) => {
    setDetailDoc(doc)
    setDetailOpen(true)
    setDetailContent('')
    if (doc.parse_status !== 'completed') return
    const audio = isAudioDoc(doc.file_extension)
    setDetailLoading(true)
    try {
      if (audio) {
        const res = await api.get<DocumentRow>(`/user-kb/documents/${doc.id}`)
        let full = res.data
        if (!full.audio || !full.audio.segments?.length) {
          const content = await api.get<{ content: string }>(
            `/user-kb/documents/${doc.id}/content`,
            { params: { max_chars: 200000 } },
          )
          full = {
            ...full,
            audio: {
              duration_seconds: full.audio?.duration_seconds ?? null,
              language: full.audio?.language ?? null,
              summary: full.audio?.summary ?? null,
              summary_generated_at: full.audio?.summary_generated_at ?? null,
              segments: [{
                index: 0, start_ms: 0, end_ms: 0, text: content.data.content || '',
              }],
            },
          }
        }
        setDetailDoc(full)
      } else {
        const res = await api.get<{ content: string }>(
          `/user-kb/documents/${doc.id}/content`, { params: { max_chars: 60000 } },
        )
        setDetailContent(res.data.content)
      }
    } catch (err: any) {
      antdMessage.error(`读取正文失败: ${err?.response?.data?.detail || err.message}`)
    } finally {
      setDetailLoading(false)
    }
  }, [])

  const deleteDoc = useCallback(async (doc: DocumentRow) => {
    try {
      await api.delete(`/user-kb/documents/${doc.id}`)
      antdMessage.success('已删除')
      if (detailDoc?.id === doc.id) { setDetailOpen(false); setDetailDoc(null) }
      tabsStore.close(doc.id)
      await Promise.all([fetchAllDocs(), fetchTree()])
    } catch (err: any) {
      antdMessage.error(`删除失败: ${err?.response?.data?.detail || err.message}`)
    }
  }, [detailDoc, fetchAllDocs, fetchTree, tabsStore])

  const downloadOriginal = useCallback((doc: DocumentRow) => {
    (async () => {
      try {
        const res = await api.get(`/user-kb/documents/${doc.id}/file`, {
          responseType: 'blob',
        })
        const blob = new Blob([res.data], { type: doc.content_type })
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = doc.original_filename
        document.body.appendChild(a); a.click(); document.body.removeChild(a)
        URL.revokeObjectURL(url)
      } catch (err: any) {
        antdMessage.error(`下载失败: ${err?.response?.data?.detail || err.message}`)
      }
    })()
  }, [])

  const canEditDoc = useCallback((doc: DocumentRow) => {
    if (!currentUser) return false
    if (doc.user_id === String(currentUser.id)) return true
    if (doc.scope === 'public' && isAdminOrBoss) return true
    return false
  }, [currentUser, isAdminOrBoss])

  // ── Tree selection / double-click ────────────────────────────

  const onTreeSelect = useCallback((key: string) => {
    setSelectedKey(key)
    // Auto-expand the selected folder.
    if (!isDocKey(key) && key !== UNFILED_KEY && key !== ALL_KEY) {
      if (!expandedKeys.includes(key)) {
        setExpandedKeys([...expandedKeys, key])
      }
    }
  }, [expandedKeys])

  // ── Render ──────────────────────────────────────────────────

  return (
    <ConfigProvider theme={workspaceTheme}>
      <div
        className="ws-root"
        style={{
          display: 'flex',
          height: 'calc(100vh - 60px)',
          background: 'var(--ws-surface-alt)',
        }}
      >
        <WorkspaceRail />

        {/* Workspace panel */}
        <div
          style={{
            width: 300, flexShrink: 0,
            display: 'flex', flexDirection: 'column',
            borderRight: '1px solid var(--ws-border)',
            background: 'var(--ws-surface)',
          }}
        >
          {/* Banners */}
          {pingOk === false && (
            <Alert
              type="warning" showIcon banner
              message="知识库 MongoDB 连接失败"
              description={<Text type="secondary" style={{ fontSize: 11 }}>{pingMsg}</Text>}
            />
          )}
          {asrOk === false && (
            <Alert
              type="info" showIcon banner
              message="语音转写服务暂不可用"
              description={<Text type="secondary" style={{ fontSize: 11 }}>{asrMsg}</Text>}
            />
          )}

          {/* Panel tabs */}
          <Tabs
            activeKey={panelTab}
            onChange={(k) => setPanelTab(k as 'tree' | 'skills')}
            size="small"
            style={{ padding: '0 8px' }}
            items={[
              {
                key: 'tree',
                label: <Space size={4}><AppstoreOutlined />工作区</Space>,
              },
              {
                key: 'skills',
                label: <Space size={4}><ThunderboltOutlined />Skills</Space>,
              },
            ]}
          />

          {panelTab === 'tree' ? (
            <>
              {/* Scope toggle + search */}
              <div style={{ padding: '4px 8px 8px 8px' }}>
                <Space.Compact style={{ width: '100%', marginBottom: 6 }}>
                  <Button
                    size="small"
                    type={scope === 'personal' ? 'primary' : 'default'}
                    onClick={() => setScope('personal')}
                    style={{ flex: 1 }}
                  >
                    个人
                  </Button>
                  <Button
                    size="small"
                    type={scope === 'public' ? 'primary' : 'default'}
                    onClick={() => setScope('public')}
                    style={{ flex: 1 }}
                  >
                    公共
                  </Button>
                </Space.Compact>
                <Input
                  size="small"
                  placeholder="搜索文件名"
                  prefix={<SearchOutlined />}
                  value={treeSearch}
                  onChange={(e) => setTreeSearch(e.target.value)}
                  allowClear
                />
              </div>
              {/* Create root folder */}
              {canWrite && (
                <div style={{ padding: '0 8px 6px 8px' }}>
                  <Button
                    size="small" block
                    icon={<FolderAddOutlined />}
                    onClick={() => openCreateFolder(null)}
                  >
                    新建根目录
                  </Button>
                </div>
              )}
              {/* Tree */}
              <div style={{ flex: 1, overflow: 'auto', padding: '0 4px' }}>
                {treeLoading ? (
                  <div style={{ textAlign: 'center', padding: 20 }}><Spin size="small" /></div>
                ) : (
                  <WorkspaceTree
                    tree={tree}
                    unfiledCount={unfiledCount}
                    scope={scope}
                    canWrite={canWrite}
                    selectedKey={selectedKey}
                    expandedKeys={expandedKeys}
                    docsByFolder={docsByFolder}
                    onSelect={onTreeSelect}
                    onExpand={setExpandedKeys}
                    onOpenDoc={(d) => openDocAsTab(d)}
                    onCreateFolder={openCreateFolder}
                    onRenameFolder={openRenameFolder}
                    onDeleteFolder={confirmDeleteFolder}
                    onInstallSkillTo={(f) => { setSelectedKey(f.id); setPanelTab('skills') }}
                    onCreateMarkdown={(f) => openCreateMarkdown(f)}
                  />
                )}
              </div>
              {/* Upload affordance */}
              {canWrite && (
                <div style={{
                  padding: 6,
                  borderTop: '1px solid var(--ws-border)',
                  background: 'var(--ws-surface-alt)',
                }}>
                  <Dragger
                    {...uploadProps}
                    style={{ padding: '4px 0', fontSize: 11 }}
                  >
                    <p className="ant-upload-drag-icon" style={{ margin: '2px 0' }}>
                      <InboxOutlined />
                    </p>
                    <p className="ant-upload-text" style={{ fontSize: 12, margin: '0 0 2px 0' }}>
                      拖放文件或点击上传
                    </p>
                    <p className="ant-upload-hint" style={{ fontSize: 10, margin: 0 }}>
                      {selectedFolder ? `→ ${selectedFolder.name}` : '→ 未归档'}
                      {` · 文档 ≤${MAX_FILE_MB}MB · 音频 ≤${MAX_AUDIO_MB}MB`}
                    </p>
                  </Dragger>
                  {uploadRows.length > 0 && (
                    <div style={{ marginTop: 6 }}>
                      <Space size={4} style={{ marginBottom: 4 }}>
                        <Text strong style={{ fontSize: 11 }}>进度</Text>
                        <Button size="small" type="link" onClick={clearUploadRows}>清理</Button>
                      </Space>
                      {uploadRows.map((r) => (
                        <Row key={r.uid} gutter={4} align="middle" style={{ fontSize: 10, marginBottom: 2 }}>
                          <div style={{ flex: 1, minWidth: 0 }}>
                            <Text ellipsis style={{ width: '100%', fontSize: 11 }}>{r.name}</Text>
                            <Progress
                              percent={r.percent} size="small"
                              status={r.status === 'error' ? 'exception'
                                : r.status === 'done' ? 'success' : 'active'}
                            />
                          </div>
                        </Row>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </>
          ) : (
            <SkillsPanel
              targetFolderId={selectedFolder?.id || null}
              targetFolderType={selectedFolder?.folder_type || null}
              canWrite={canWrite}
              onInstalled={() => { fetchTree(); fetchAllDocs() }}
            />
          )}
        </div>

        {/* Editor canvas */}
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minWidth: 0 }}>
          {/* Breadcrumb header */}
          {selectedFolder && (
            <div style={{
              padding: '6px 14px', borderBottom: '1px solid var(--ws-border)',
              background: 'var(--ws-surface)', fontSize: 12,
              display: 'flex', alignItems: 'center', gap: 8,
            }}>
              <FolderOutlined style={{ color: 'var(--ws-text-secondary)' }} />
              <Text>{selectedFolder.name}</Text>
              {selectedFolder.stock_ticker && (
                <Tag color="default" style={{ margin: 0, fontSize: 10 }}>
                  {selectedFolder.stock_ticker}
                </Tag>
              )}
              {selectedFolder.folder_type === 'stock' && (
                <Button
                  size="small" type="link" icon={<LineChartOutlined />}
                  onClick={() => openStockValuation(selectedFolder)}
                >
                  打开估值表
                </Button>
              )}
              <div style={{ flex: 1 }} />
              {docsLoading ? <Spin size="small" /> : (
                <Button size="small" icon={<ReloadOutlined />}
                  onClick={() => { fetchAllDocs(); fetchTree() }}>刷新</Button>
              )}
            </div>
          )}

          <WorkspaceCanvas
            readonly={!canWrite}
            onOpenFileDetail={(docId) => {
              const full = allDocs.find((d) => d.id === docId)
              if (!full) return
              if (isAudioDoc(full.file_extension)) {
                navigate(`/my-knowledge/audio/${full.id}`)
                return
              }
              openDetail(full)
            }}
          />
        </div>

        {/* Folder create/rename modal */}
        <Modal
          open={folderModalOpen}
          title={folderMode === 'create' ? '新建目录' : '重命名目录'}
          onCancel={() => setFolderModalOpen(false)}
          onOk={saveFolder}
          okButtonProps={{ loading: folderSaving }}
          okText={folderMode === 'create' ? '创建' : '保存'}
          cancelText="取消"
          destroyOnClose
        >
          <Form form={folderForm} layout="vertical"
            initialValues={{ folder_type: parentIsStock ? 'general' : 'general' }}>
            {folderParentId && folderById[folderParentId] && folderMode === 'create' && (
              <Alert
                type="info" showIcon style={{ marginBottom: 12 }}
                message={
                  <span>
                    创建位置：<Text code>{folderById[folderParentId].name}</Text>
                    {parentIsStock && (
                      <Text type="secondary" style={{ marginLeft: 6 }}>
                        （股票目录下只能新建通用/行业子目录）
                      </Text>
                    )}
                  </span>
                }
              />
            )}
            {folderMode === 'create' && (
              <Form.Item name="folder_type" label="目录类型" rules={[{ required: true }]}
                help={folderTypeWatched === 'stock' ? '目录名称将自动使用股票名称' : undefined}>
                <Select
                  options={[
                    ...(parentIsStock ? [] : [{
                      value: 'stock',
                      label: (<Space><LineChartOutlined style={{ color: 'var(--ws-accent)' }} />股票型</Space>),
                    }]),
                    { value: 'industry', label: (<Space><TagOutlined style={{ color: '#0ea5e9' }} />行业型</Space>) },
                    { value: 'general', label: (<Space><FolderOutlined />通用型</Space>) },
                  ]}
                />
              </Form.Item>
            )}
            {folderMode === 'create' && folderTypeWatched === 'stock' && (
              <Form.Item name="stock_value" label="绑定股票"
                rules={[{ required: true, message: '请选择要绑定的股票' }]}
                extra="支持 A股 / 港股 / 美股；输入代码或名称即可搜索">
                <Select
                  showSearch
                  placeholder="输入代码或名称"
                  notFoundContent={stockSearching ? <Spin size="small" /> : <span style={{ color: '#94a3b8' }}>输入关键字…</span>}
                  labelInValue filterOption={false} onSearch={debouncedStockSearch}
                  optionLabelProp="title"
                  options={stockOptions.map((s) => ({
                    value: s.code,
                    title: `${s.name} (${s.code})`,
                    label: (
                      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8 }}>
                        <span><LineChartOutlined style={{ color: 'var(--ws-accent)', marginRight: 6 }} />{s.name}</span>
                        <Space size={4}>
                          <Tag color="blue" style={{ margin: 0 }}>{s.code}</Tag>
                          <Tag color="default" style={{ margin: 0 }}>{s.market}</Tag>
                        </Space>
                      </div>
                    ),
                  }))}
                />
              </Form.Item>
            )}
            {(folderMode === 'rename' || folderTypeWatched !== 'stock') && (
              <Form.Item name="name" label="目录名称" rules={[
                { required: true, message: '请填写目录名称' },
                { max: 255, message: '过长' },
                { pattern: /^[^/\\\n\r\t]+$/, message: '不能包含 / \\ 或换行字符' },
              ]}>
                <Input placeholder={folderTypeWatched === 'industry'
                  ? '例如：半导体 / 新能源车' : '例如：研究笔记 / Q1 数据'} />
              </Form.Item>
            )}
          </Form>
        </Modal>

        {/* Markdown create modal */}
        <Modal
          open={mdModalOpen}
          title={`在 "${mdModalFolder?.name || ''}" 新建 Markdown`}
          onCancel={() => setMdModalOpen(false)}
          onOk={saveMarkdown}
          okButtonProps={{ loading: mdSaving }}
          okText="创建"
          cancelText="取消"
          destroyOnClose
          width={600}
        >
          <Form form={mdForm} layout="vertical">
            <Form.Item name="title" label="标题" rules={[{ required: true, max: 200 }]}>
              <Input />
            </Form.Item>
            <Form.Item name="original_filename" label="文件名" rules={[
              { required: true, max: 200 },
              { pattern: /^[^/\\\n\r\t]+$/, message: '不能包含 / \\ 或换行字符' },
            ]} extra="建议以 .md 结尾">
              <Input />
            </Form.Item>
            <Form.Item name="content_md" label="初始内容">
              <Input.TextArea rows={8} />
            </Form.Item>
          </Form>
        </Modal>

        {/* Detail drawer for files that can't be edited inline */}
        <Drawer
          open={detailOpen}
          onClose={() => setDetailOpen(false)}
          width={detailDoc && isAudioDoc(detailDoc.file_extension) ? 1180 : 720}
          title={detailDoc ? (
            <Space>
              {fileIcon(detailDoc.file_extension)}
              <span>{detailDoc.title || detailDoc.original_filename}</span>
              {detailDoc.scope === 'public' && <Tag color="purple">公共</Tag>}
            </Space>
          ) : null}
          destroyOnClose
        >
          {detailDoc && (
            <Space direction="vertical" size={16} style={{ width: '100%' }}>
              <Descriptions column={1} size="small" bordered>
                <Descriptions.Item label="原始文件">
                  {detailDoc.original_filename} ({humanSize(detailDoc.file_size_bytes)})
                </Descriptions.Item>
                <Descriptions.Item label="所属目录">
                  {detailDoc.folder_id && folderById[detailDoc.folder_id]
                    ? folderById[detailDoc.folder_id].name
                    : '未归档'}
                </Descriptions.Item>
                <Descriptions.Item label="上传时间">
                  {new Date(detailDoc.created_at).toLocaleString('zh-CN')}
                </Descriptions.Item>
                <Descriptions.Item label="内容长度">
                  {detailDoc.extracted_char_count.toLocaleString()} 字 · {detailDoc.num_chunks} 片段
                </Descriptions.Item>
              </Descriptions>
              {canEditDoc(detailDoc) && (
                <Space>
                  <Button size="small" icon={<DownloadOutlined />}
                    onClick={() => downloadOriginal(detailDoc)}>下载</Button>
                  <Popconfirm
                    title="确认删除？" description="不可恢复"
                    onConfirm={() => deleteDoc(detailDoc)}
                    okText="删除" cancelText="取消" okButtonProps={{ danger: true }}
                  >
                    <Button size="small" danger icon={<DeleteOutlined />}>删除</Button>
                  </Popconfirm>
                </Space>
              )}
              {isAudioDoc(detailDoc.file_extension)
                && detailDoc.parse_status === 'completed'
                && detailDoc.audio ? (
                detailLoading ? (
                  <Card size="small"><Spin /></Card>
                ) : (
                  <AudioTranscriptViewer
                    documentId={detailDoc.id}
                    title={detailDoc.title || detailDoc.original_filename}
                    audio={detailDoc.audio}
                    filename={detailDoc.original_filename}
                    onDownload={() => downloadOriginal(detailDoc)}
                  />
                )
              ) : (
                <Card size="small" title={<Space><FileTextOutlined />正文预览</Space>}>
                  {detailLoading ? <Spin /> : detailContent ? (
                    <pre style={{
                      maxHeight: 520, overflowY: 'auto', margin: 0, padding: 8,
                      background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 4,
                      fontSize: 12, whiteSpace: 'pre-wrap', wordBreak: 'break-word',
                    }}>
                      {detailContent}
                    </pre>
                  ) : (
                    <Empty
                      image={Empty.PRESENTED_IMAGE_SIMPLE}
                      description={detailDoc.parse_status === 'completed'
                        ? '正文为空' : '等待解析完成后可查看正文'}
                    />
                  )}
                </Card>
              )}
            </Space>
          )}
        </Drawer>
      </div>
    </ConfigProvider>
  )
}
