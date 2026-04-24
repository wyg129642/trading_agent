import { useState, useEffect, useRef, useCallback } from 'react'
import {
  Layout, Input, Button, List, Card, Tag, Rate, Space, Typography,
  Select, Upload, Tooltip, Modal, Dropdown, Empty, Spin, Badge,
  Popconfirm, message, Drawer, Collapse, Divider, Avatar, Grid,
  Tabs, Tree,
} from 'antd'
import {
  SendOutlined, PlusOutlined, DeleteOutlined, PaperClipOutlined,
  RobotOutlined, UserOutlined, StarOutlined, ExportOutlined,
  PushpinOutlined, PushpinFilled, SearchOutlined, MenuFoldOutlined,
  MenuUnfoldOutlined, ThunderboltOutlined, FileTextOutlined,
  LinkOutlined, BulbOutlined, CopyOutlined, EditOutlined,
  FileImageOutlined, FilePdfOutlined, CloseOutlined,
  TrophyOutlined, BookOutlined, CompressOutlined,
  DownloadOutlined, HistoryOutlined, GlobalOutlined,
  ExpandAltOutlined, MessageOutlined, StopOutlined, ReloadOutlined,
  DatabaseOutlined, FolderOutlined, FolderOpenOutlined,
  TagOutlined, LineChartOutlined, ExperimentOutlined,
} from '@ant-design/icons'
import api from '../services/api'
import { useAuthStore } from '../store/auth'
import MarkdownRenderer from '../components/MarkdownRenderer'
import CitationRenderer from '../components/CitationRenderer'
import FeedbackModal from '../components/FeedbackModal'
import MemoriesPanel, { type MemoriesPanelHandle } from '../components/MemoriesPanel'

const { Sider, Content } = Layout
const { TextArea } = Input
const { Text, Title, Paragraph } = Typography
const { useBreakpoint } = Grid

// ── Types ────────────────────────────────────────────────────

interface ModelInfo {
  id: string
  name: string
  provider: string
  supports_vision: boolean
  supports_thinking: boolean
  description: string
}

interface ModelResponseData {
  id: string
  model_id: string
  model_name: string
  content: string
  tokens_used: number | null
  latency_ms: number | null
  rating: number | null
  rating_comment: string | null
  error: string | null
  sources: Array<{index: number, title: string, url: string, website: string, date: string, source_type?: string, doc_type?: string}> | null
  debate_round: number | null
  created_at: string
}

interface ChatMessageData {
  id: string
  role: string
  content: string
  attachments: any[]
  is_debate: boolean
  model_responses: ModelResponseData[]
  created_at: string
}

interface TrackingTopic {
  id: string
  topic: string
  keywords: string[]
  related_tickers: string[]
  related_sectors: string[]
  notify_channels: string[]
  is_active: boolean
  created_at: string
  last_checked_at: string | null
  last_triggered_at: string | null
  unread_count: number
}

interface DebateRound {
  round: number
  role: string
  model: string
  model_name: string
  content: string
  response_id: string
  tokens: number
  latency_ms: number
  error: string | null
}

interface Conversation {
  id: string
  title: string
  tags: string[]
  is_pinned: boolean
  created_at: string
  updated_at: string
  message_count: number
  last_message_preview: string
}

interface PromptTemplate {
  id: string
  name: string
  content: string
  category: string
  is_system: boolean
  usage_count: number
}

interface ModelRanking {
  model_id: string
  model_name: string
  avg_rating: number
  total_ratings: number
  total_uses: number
}

interface Attachment {
  filename: string
  file_type: string
  file_url: string
  file_path?: string  // server-side path for native file upload to LLM
}

// ── Personal knowledge base (drag-drop into chat) ─────────────

interface KbDocumentRef {
  id: string
  title: string
  filename: string
  file_extension: string
  scope: 'personal' | 'public'
  folder_id: string | null
}

interface KbFolderNode {
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
  document_count: number
  children: KbFolderNode[]
}

// Drag payload MIME type — opaque to the browser but picked up by our own
// drop handler on the chat input.
const KB_DRAG_MIME = 'application/x-trading-kb-doc+json'

// ── Constants ────────────────────────────────────────────────

const CATEGORY_LABELS: Record<string, string> = {
  general: '通用',
  fundamental: '基本面',
  technical: '技术面',
  news: '新闻分析',
  macro: '宏观经济',
  industry: '行业研究',
}

const USEFUL_LINKS = [
  { name: 'MiroMind AI', url: 'https://miromind.ai', desc: 'AI深度研究助手' },
  { name: '东方财富', url: 'https://www.eastmoney.com', desc: 'A股行情数据' },
  { name: '雪球', url: 'https://xueqiu.com', desc: '投资社区讨论' },
  { name: '同花顺iFind', url: 'https://www.51ifind.com', desc: '金融数据终端' },
  { name: 'Wind万得', url: 'https://www.wind.com.cn', desc: '专业金融数据' },
  { name: 'TradingView', url: 'https://www.tradingview.com', desc: '全球行情图表' },
  { name: 'Finviz', url: 'https://finviz.com', desc: '美股筛选器' },
  { name: 'SEC Edgar', url: 'https://www.sec.gov/edgar', desc: '美股公司公告' },
  { name: '巨潮资讯', url: 'http://www.cninfo.com.cn', desc: 'A股公司公告' },
]

const TOOL_LABELS: Record<string, string> = {
  alphapai_qa: 'Alpha派·投研问答',
  alphapai_agent: 'Alpha派·投研Agent',
  alphapai_report: 'Alpha派·公告查询',
  alphapai_watchlist: 'Alpha派·自选股',
  alphapai_image_search: 'Alpha派·图表搜索',
  alphapai_recall: 'Alpha派·知识检索',
  jinmen_research_reports: '进门·研报查询',
  jinmen_minutes: '进门·纪要查询',
  jinmen_comments: '进门·点评查询',
  web_search: '联网搜索',
  read_webpage: '阅读网页',
}

// ── Built-in system prompt templates ─────────────────────────

const SYSTEM_PROMPTS: PromptTemplate[] = [
  {
    id: '_sys_stock_analyst',
    name: '📊 股票分析师',
    content: '你是一位专业的股票分析师。请基于基本面、技术面和市场情绪进行分析，给出买入/持有/卖出建议，并说明风险因素。请用中文回答。',
    category: 'fundamental',
    is_system: true,
    usage_count: 0,
  },
  {
    id: '_sys_financial_report',
    name: '📑 财报解读',
    content: '你是财务报表分析专家。请从营收增长、利润率、现金流、负债率等维度分析公司财报，识别关键变化和潜在风险信号。请用中文回答。',
    category: 'fundamental',
    is_system: true,
    usage_count: 0,
  },
  {
    id: '_sys_technical',
    name: '📈 技术分析',
    content: '你是一位技术分析专家。请从K线形态、成交量、均线系统、MACD/RSI/KDJ等技术指标角度分析股票走势，给出关键支撑位和阻力位。请用中文回答。',
    category: 'technical',
    is_system: true,
    usage_count: 0,
  },
  {
    id: '_sys_news_analyst',
    name: '📰 新闻解读',
    content: '你是一位市场新闻分析师。请分析新闻对相关股票和行业的短期/中期影响，评估利好/利空程度，并与历史类似事件做对比。请用中文回答。',
    category: 'news',
    is_system: true,
    usage_count: 0,
  },
  {
    id: '_sys_macro',
    name: '🌍 宏观经济',
    content: '你是一位宏观经济分析师。请从GDP、通胀、利率、汇率、货币政策等角度分析宏观经济环境对市场的影响。请用中文回答。',
    category: 'macro',
    is_system: true,
    usage_count: 0,
  },
  {
    id: '_sys_industry',
    name: '🏭 行业研究',
    content: '你是一位行业研究专家。请分析行业竞争格局、产业链上下游、政策影响、技术趋势和市场空间，评估行业投资价值。请用中文回答。',
    category: 'industry',
    is_system: true,
    usage_count: 0,
  },
  {
    id: '_sys_risk',
    name: '⚠️ 风险评估',
    content: '你是一位风险管理专家。请从系统性风险、行业风险、公司特有风险等多个维度评估投资风险，给出风险控制建议和止损策略。请用中文回答。',
    category: 'general',
    is_system: true,
    usage_count: 0,
  },
  {
    id: '_sys_compare',
    name: '🔄 对比分析',
    content: '你是一位比较分析专家。请对给定的股票/公司/行业进行全方位对比，包括估值、成长性、盈利能力、市场地位等，用表格形式呈现结论。请用中文回答。',
    category: 'fundamental',
    is_system: true,
    usage_count: 0,
  },
]

// ── Time helpers ────────────────────────────────────────────

function timeAgo(isoStr: string): string {
  const diff = Date.now() - new Date(isoStr).getTime()
  const mins = Math.floor(diff / 60000)
  if (mins < 1) return '刚刚'
  if (mins < 60) return `${mins}分钟前`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}小时前`
  const days = Math.floor(hrs / 24)
  if (days < 7) return `${days}天前`
  const d = new Date(isoStr)
  return `${d.getMonth() + 1}/${d.getDate()}`
}

function dateGroup(isoStr: string): string {
  const d = new Date(isoStr)
  const now = new Date()
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate())
  const target = new Date(d.getFullYear(), d.getMonth(), d.getDate())
  const diff = today.getTime() - target.getTime()
  const days = Math.floor(diff / 86400000)
  if (days === 0) return '今天'
  if (days === 1) return '昨天'
  if (days < 7) return '最近7天'
  if (days < 30) return '最近30天'
  return `${d.getFullYear()}年${d.getMonth() + 1}月`
}

// ── Main Component ───────────────────────────────────────────

export default function AIChat() {
  const screens = useBreakpoint()
  const user = useAuthStore((s) => s.user)
  const chatEndRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<any>(null)

  // Models (persisted in localStorage)
  const [models, setModels] = useState<ModelInfo[]>([])
  const [selectedModels, setSelectedModels] = useState<string[]>(() => {
    try {
      const saved = localStorage.getItem('chat_selected_models')
      if (saved) {
        const parsed = JSON.parse(saved)
        if (Array.isArray(parsed) && parsed.length > 0) return parsed
      }
    } catch {}
    return []
  })

  // Conversations
  const [conversations, setConversations] = useState<Conversation[]>([])
  const [activeConvId, setActiveConvId] = useState<string | null>(null)
  const [messages, setMessages] = useState<ChatMessageData[]>([])
  const [convSearch, setConvSearch] = useState('')

  // Detailed feedback modal — null when closed, else {responseId, modelName}
  const [feedbackTarget, setFeedbackTarget] = useState<{
    responseId: string
    modelName: string
    initialRating: number | null
    initialText: string
  } | null>(null)

  // Input
  const [inputText, setInputText] = useState('')
  const [attachments, setAttachments] = useState<Attachment[]>([])
  // Personal-knowledge-base docs that the user dragged into the input.
  // The ids are posted as ``kb_document_ids`` on the stream request; the
  // backend hydrates them into a reference-documents prefix before the LLM
  // sees the user message.
  const [kbRefs, setKbRefs] = useState<KbDocumentRef[]>([])
  const [kbPanelOpen, setKbPanelOpen] = useState(false)
  const [kbTreePersonal, setKbTreePersonal] = useState<KbFolderNode[]>([])
  const [kbTreePublic, setKbTreePublic] = useState<KbFolderNode[]>([])
  const [kbTreeLoading, setKbTreeLoading] = useState(false)
  const [kbPanelScope, setKbPanelScope] = useState<'personal' | 'public'>('personal')
  // Lazy per-folder doc listings (folder_id -> docs). "" = unfiled in scope.
  const [kbFolderDocs, setKbFolderDocs] = useState<Record<string, KbDocumentRef[]>>({})
  const [kbFolderDocsLoading, setKbFolderDocsLoading] = useState<Record<string, boolean>>({})
  // Fetch error surfaced in the workspace panel when /user-kb/documents fails.
  const [kbDocsError, setKbDocsError] = useState<string | null>(null)
  const [kbDragging, setKbDragging] = useState(false)
  // Track drag-over on the input so we can highlight the drop zone. Use a
  // ref counter to avoid flicker from dragleave firing on nested children.
  const [kbDragOverInput, setKbDragOverInput] = useState(false)
  const kbDragOverDepthRef = useRef(0)
  const [systemPrompt, setSystemPrompt] = useState<string | null>(null)
  const [activePromptName, setActivePromptName] = useState<string>('')
  const [sendingConvIds, setSendingConvIds] = useState<Set<string>>(new Set())
  const [chatMode, setChatMode] = useState<'standard' | 'thinking' | 'fast'>(() => {
    const saved = localStorage.getItem('chat_mode')
    if (saved === 'standard' || saved === 'thinking' || saved === 'fast') return saved
    return 'standard'
  })

  // Web search mode: "off" | "auto" (LLM decides) | "on" (force search)
  const [webSearchMode, setWebSearchMode] = useState<'off' | 'auto' | 'on'>(() => {
    const saved = localStorage.getItem('web_search_mode')
    if (saved === 'off' || saved === 'auto' || saved === 'on') return saved
    return 'auto'
  })

  // Citation sources per model
  const [modelSources, setModelSources] = useState<Record<string, Array<{index: number, title: string, url: string, website: string, date: string}>>>({})

  // 内部知识库 toggle — covers 7 crawled platforms (alphapai + jinmen + meritco +
  // thirdbridge + funda + gangtise + acecamp) via the kb_search / kb_fetch_document
  // / kb_list_facets tools (default ON).
  const [kbEnabled, setKbEnabled] = useState(() => {
    const saved = localStorage.getItem('kb_enabled')
    return saved === null ? true : saved === 'true'
  })

  // 用户个人知识库 toggle — user-uploaded files via the user_kb_search /
  // user_kb_fetch_document tools (default OFF so the LLM doesn't reach for
  // it unless the user explicitly wants their own docs consulted).
  const [userKbEnabled, setUserKbEnabled] = useState(() => {
    const saved = localStorage.getItem('user_kb_enabled')
    return saved === null ? false : saved === 'true'
  })

  // Tool calling status per model (shared for both Alpha派 and 进门)
  const [toolCallingModels, setToolCallingModels] = useState<Record<string, string>>({})

  // Streaming state: model_id -> accumulated content
  const [streamingContents, setStreamingContents] = useState<Record<string, string>>({})
  const [streamingModels, setStreamingModels] = useState<string[]>([])

  // Per-conversation streaming support: refs to track state across conversation switches
  const activeConvIdRef = useRef<string | null>(null)
  const convStreamSnapshotsRef = useRef<Record<string, {
    streamingContents: Record<string, string>
    streamingModels: string[]
    toolCallingModels: Record<string, string>
    debateRounds: DebateRound[]
    activeDebateRound: number
    debateStreamContent: string
    debateStreaming: boolean
    debateSummary: any
    debateSummaryLoading: boolean
  }>>({})

  // Derived: is the currently active conversation sending?
  const sending = !!(activeConvId && sendingConvIds.has(activeConvId))

  // Templates & rankings
  const [templates, setTemplates] = useState<PromptTemplate[]>([])
  const [rankings, setRankings] = useState<ModelRanking[]>([])

  // Personalized quick-start questions (refreshed daily by the backend scheduler)
  const DEFAULT_QUICK_QUESTIONS = [
    '帮我分析贵州茅台(600519)的基本面，包括最近的财报表现',
    '当前A股市场的宏观环境如何？有哪些板块值得关注？',
    '比较宁德时代和比亚迪在新能源领域的竞争优势',
    '近期半导体板块大跌，分析一下原因和后续走势',
  ]
  const [recommendedQuestions, setRecommendedQuestions] = useState<string[]>(DEFAULT_QUICK_QUESTIONS)
  const [recommendedLoading, setRecommendedLoading] = useState(false)
  const [recommendedRefreshing, setRecommendedRefreshing] = useState(false)

  // UI
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const [rightDrawerOpen, setRightDrawerOpen] = useState(false)
  const [memoriesDrawerOpen, setMemoriesDrawerOpen] = useState(false)
  const [memoriesActiveCount, setMemoriesActiveCount] = useState<number>(0)
  // memory_ids used in the latest chat turn — highlighted in the drawer
  const [memoryIdsThisTurn, setMemoryIdsThisTurn] = useState<string[]>([])
  const memoriesPanelRef = useRef<MemoriesPanelHandle>(null)
  const [templateModalOpen, setTemplateModalOpen] = useState(false)
  const [newTplName, setNewTplName] = useState('')
  const [newTplContent, setNewTplContent] = useState('')
  const [newTplCategory, setNewTplCategory] = useState('general')

  // Debate mode
  const [debateMode, setDebateMode] = useState(false)
  const [debateFormat, setDebateFormat] = useState<string>(() => localStorage.getItem('chat_debate_format') || 'bull_bear')
  const [debateRounds, setDebateRounds] = useState<DebateRound[]>([])
  const [activeDebateRound, setActiveDebateRound] = useState(0)
  const [debateStreaming, setDebateStreaming] = useState(false)
  const [debateStreamContent, setDebateStreamContent] = useState('')
  const [debateSummary, setDebateSummary] = useState<any>(null)
  const [debateSummaryLoading, setDebateSummaryLoading] = useState(false)

  // Tracking
  const [trackingTopics, setTrackingTopics] = useState<TrackingTopic[]>([])
  const [trackingModalOpen, setTrackingModalOpen] = useState(false)
  const [newTrackingTopic, setNewTrackingTopic] = useState('')

  // Single-model expanded view
  const [expandedModel, setExpandedModel] = useState<string | null>(null)
  // Send target: 'all' or a specific model_id
  const [sendTarget, setSendTarget] = useState<'all' | string>('all')
  // Closed/hidden models in current conversation
  const [closedModels, setClosedModels] = useState<string[]>([])
  // Scroll tracking for smart auto-scroll
  const messagesContainerRef = useRef<HTMLDivElement>(null)
  const isNearBottomRef = useRef(true)

  // Cancel & regenerate refs
  const abortControllerRef = useRef<AbortController | null>(null)
  const currentMessageIdRef = useRef<string>('')
  const wasCancelledRef = useRef(false)

  // ── Persist preferences ─────────────────────────────────────

  useEffect(() => {
    if (selectedModels.length > 0) {
      localStorage.setItem('chat_selected_models', JSON.stringify(selectedModels))
    }
  }, [selectedModels])

  useEffect(() => {
    localStorage.setItem('chat_mode', chatMode)
  }, [chatMode])

  // Initial fetch of active memory count for the toolbar badge (drawer lazy-loads on open).
  useEffect(() => {
    api.get('/chat-memory/memories', { params: { include_inactive: false } })
      .then((res) => setMemoriesActiveCount(res.data?.total_active || 0))
      .catch(() => { /* non-fatal */ })
  }, [])

  // ── Save partial on page unload (refresh / close tab) ──────
  useEffect(() => {
    const handleBeforeUnload = () => {
      // Abort any active stream
      abortControllerRef.current?.abort()

      // Save partial content via fetch+keepalive (reliable during unload, supports headers)
      const msgId = currentMessageIdRef.current
      if (!msgId) return
      const token = useAuthStore.getState().token

      for (const [convId, snapshot] of Object.entries(convStreamSnapshotsRef.current)) {
        const partials = snapshot?.streamingContents || {}
        const nonEmpty = Object.fromEntries(
          Object.entries(partials).filter(([, v]) => v && v.length > 0)
        )
        if (Object.keys(nonEmpty).length === 0) continue
        try {
          fetch(`/api/chat/conversations/${convId}/messages/${msgId}/save-partial`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${token}`,
            },
            body: JSON.stringify({ partial_responses: nonEmpty }),
            keepalive: true,  // ensures request completes even during page unload
          })
        } catch { /* best effort */ }
      }
    }

    window.addEventListener('beforeunload', handleBeforeUnload)
    return () => window.removeEventListener('beforeunload', handleBeforeUnload)
  }, [])

  // ── Data fetching ──────────────────────────────────────────

  const fetchModels = useCallback(async () => {
    try {
      const res = await api.get('/chat/models')
      setModels(res.data)
      const availableIds = res.data.map((m: ModelInfo) => m.id)
      setSelectedModels((prev) => {
        const valid = prev.filter((id: string) => availableIds.includes(id))
        if (valid.length > 0) return valid
        return res.data.length > 0 ? [res.data[0].id] : []
      })
    } catch { /* ignore */ }
  }, [])

  const fetchConversations = useCallback(async () => {
    try {
      const res = await api.get('/chat/conversations', { params: { page_size: 100 } })
      setConversations(res.data.conversations)
    } catch { /* ignore */ }
  }, [])

  const fetchMessages = useCallback(async (convId: string, retries = 2): Promise<boolean> => {
    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        const res = await api.get(`/chat/conversations/${convId}`)
        // Guard against stale responses: only update if this conversation is still active
        if (activeConvIdRef.current !== convId) return true
        setMessages(res.data.messages)
        // Restore citation sources from persisted model responses
        const restored: Record<string, Array<{index: number, title: string, url: string, website: string, date: string}>> = {}
        for (const msg of res.data.messages) {
          if (msg.model_responses) {
            for (const resp of msg.model_responses) {
              if (resp.sources && resp.sources.length > 0) {
                restored[resp.model_id] = resp.sources
              }
            }
          }
        }
        setModelSources(restored)
        return true
      } catch {
        if (attempt === retries) {
          message.error('加载对话失败')
          return false
        }
        // Brief delay before retry
        await new Promise(r => setTimeout(r, 1000))
      }
    }
    return false
  }, [])

  const fetchTemplates = useCallback(async () => {
    try {
      const res = await api.get('/chat/templates')
      setTemplates(res.data)
    } catch { /* ignore */ }
  }, [])

  const fetchRankings = useCallback(async () => {
    try {
      const res = await api.get('/chat/model-rankings')
      setRankings(res.data.rankings)
    } catch { /* ignore */ }
  }, [])

  const fetchTrackingTopics = useCallback(async () => {
    try {
      const res = await api.get('/chat/tracking')
      setTrackingTopics(res.data)
    } catch { /* ignore */ }
  }, [])

  const fetchRecommendedQuestions = useCallback(async () => {
    setRecommendedLoading(true)
    try {
      const res = await api.get('/chat/recommended-questions')
      const qs = Array.isArray(res.data?.questions) ? res.data.questions : []
      if (qs.length > 0) setRecommendedQuestions(qs)
    } catch { /* ignore — keep defaults */ }
    finally {
      setRecommendedLoading(false)
    }
  }, [])

  const refreshRecommendedQuestions = useCallback(async () => {
    setRecommendedRefreshing(true)
    try {
      const res = await api.post('/chat/recommended-questions/refresh')
      const qs = Array.isArray(res.data?.questions) ? res.data.questions : []
      if (qs.length > 0) {
        setRecommendedQuestions(qs)
        message.success('已为你重新生成推荐问题')
      }
    } catch {
      message.error('刷新失败，请稍后再试')
    } finally {
      setRecommendedRefreshing(false)
    }
  }, [])

  useEffect(() => {
    fetchModels()
    fetchConversations()
    fetchTemplates()
    fetchRankings()
    fetchTrackingTopics()
    fetchRecommendedQuestions()
  }, [])

  useEffect(() => {
    activeConvIdRef.current = activeConvId

    if (activeConvId) {
      fetchMessages(activeConvId)

      // Restore streaming state if this conv has active streaming
      const snapshot = convStreamSnapshotsRef.current[activeConvId]
      if (snapshot) {
        setStreamingContents({ ...snapshot.streamingContents })
        setStreamingModels([...snapshot.streamingModels])
        setToolCallingModels({ ...snapshot.toolCallingModels })
        setDebateRounds([...snapshot.debateRounds])
        setActiveDebateRound(snapshot.activeDebateRound)
        setDebateStreamContent(snapshot.debateStreamContent)
        setDebateStreaming(snapshot.debateStreaming)
        setDebateSummary(snapshot.debateSummary)
        setDebateSummaryLoading(snapshot.debateSummaryLoading)
      } else {
        setStreamingContents({})
        setStreamingModels([])
        setToolCallingModels({})
        setDebateRounds([])
        setActiveDebateRound(0)
        setDebateStreamContent('')
        setDebateStreaming(false)
        setDebateSummary(null)
        setDebateSummaryLoading(false)
      }
    } else {
      setMessages([])
      setStreamingContents({})
      setStreamingModels([])
      setToolCallingModels({})
    }

    // Reset per-conversation UI state
    setClosedModels([])
    setSendTarget('all')
    setExpandedModel(null)
  }, [activeConvId])

  // Smart auto-scroll: only scroll to bottom if user is near the bottom
  const handleMessagesScroll = useCallback(() => {
    const container = messagesContainerRef.current
    if (!container) return
    const threshold = 150
    isNearBottomRef.current = container.scrollHeight - container.scrollTop - container.clientHeight < threshold
  }, [])

  useEffect(() => {
    if (isNearBottomRef.current) {
      chatEndRef.current?.scrollIntoView({ behavior: 'smooth' })
    }
  }, [messages, streamingContents])

  // ── Actions ────────────────────────────────────────────────

  const createConversation = async () => {
    try {
      const res = await api.post('/chat/conversations', { title: '新对话' })
      setConversations((prev) => [res.data, ...prev])
      setActiveConvId(res.data.id)
      setMessages([])
    } catch {
      message.error('创建对话失败')
    }
  }

  const deleteConversation = async (id: string) => {
    try {
      await api.delete(`/chat/conversations/${id}`)
      setConversations((prev) => prev.filter((c) => c.id !== id))
      if (activeConvId === id) {
        setActiveConvId(null)
        setMessages([])
      }
    } catch {
      message.error('删除失败')
    }
  }

  const togglePin = async (conv: Conversation) => {
    try {
      await api.patch(`/chat/conversations/${conv.id}`, { is_pinned: !conv.is_pinned })
      fetchConversations()
    } catch { /* ignore */ }
  }

  // ── Debate send handler ──────────────────────────────────────

  const handleDebateSend = async () => {
    if (!inputText.trim()) return
    if (selectedModels.length < 2 || selectedModels.length > 6) {
      message.warning('辩论模式需要选择2-6个模型')
      return
    }

    let convId = activeConvId
    if (!convId) {
      try {
        const res = await api.post('/chat/conversations', { title: '新对话' })
        convId = res.data.id
        setConversations((prev) => [res.data, ...prev])
        setActiveConvId(convId)
      } catch {
        message.error('创建对话失败')
        return
      }
    }

    // Block duplicate sends to the same conversation
    if (sendingConvIds.has(convId!)) return

    setSendingConvIds(prev => new Set(prev).add(convId!))
    setDebateStreaming(true)
    setDebateRounds([])
    setActiveDebateRound(0)
    setDebateStreamContent('')
    setDebateSummary(null)

    // Initialize per-conversation streaming snapshot
    convStreamSnapshotsRef.current[convId!] = {
      streamingContents: {},
      streamingModels: [],
      toolCallingModels: {},
      debateRounds: [],
      activeDebateRound: 0,
      debateStreamContent: '',
      debateStreaming: true,
      debateSummary: null,
      debateSummaryLoading: false,
    }

    const userText = inputText
    setInputText('')

    const tempUserMsg: ChatMessageData = {
      id: `temp_${Date.now()}`,
      role: 'user',
      content: userText,
      attachments: [],
      is_debate: true,
      model_responses: [],
      created_at: new Date().toISOString(),
    }
    setMessages((prev) => [...prev, tempUserMsg])

    const currentAttachments = attachments.map((a) => ({
      filename: a.filename, file_type: a.file_type, file_url: a.file_url, file_path: a.file_path,
    }))
    setAttachments([])

    try {
      const controller = new AbortController()
      abortControllerRef.current = controller
      const token = useAuthStore.getState().token
      const response = await fetch(`/api/chat/conversations/${convId}/messages/debate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        signal: controller.signal,
        body: JSON.stringify({
          content: userText,
          debate_models: selectedModels,
          attachments: currentAttachments,
          system_prompt: systemPrompt,
          web_search: webSearchMode !== 'off',  // debate uses boolean
          debate_format: debateFormat,
        }),
      })

      if (!response.ok) throw new Error(`HTTP ${response.status}`)

      const reader = response.body?.getReader()
      const decoder = new TextDecoder()
      let buffer = ''
      let currentRoundContent = ''
      let currentRoundRole = ''
      let debateMessageId = ''

      if (reader) {
        while (true) {
          const { done, value } = await reader.read()
          if (done) break
          buffer += decoder.decode(value, { stream: true })
          const lines = buffer.split('\n')
          buffer = lines.pop() || ''

          for (const line of lines) {
            if (!line.startsWith('data: ')) continue
            const data = line.slice(6).trim()
            if (data === '[DONE]') break

            try {
              const event = JSON.parse(data)
              const snapshot = convStreamSnapshotsRef.current[convId!]
              const isActive = activeConvIdRef.current === convId

              if (event.type === 'meta') {
                debateMessageId = event.message_id || ''
              } else if (event.type === 'web_search_start' || event.type === 'web_search_done') {
                // Web search status
              } else if (event.type === 'round_start') {
                currentRoundContent = ''
                currentRoundRole = event.role || ''
                if (snapshot) {
                  snapshot.activeDebateRound = event.round
                  snapshot.debateStreamContent = ''
                }
                if (isActive) {
                  setActiveDebateRound(event.round)
                  setDebateStreamContent('')
                }
              } else if (event.type === 'delta') {
                currentRoundContent += event.delta
                if (snapshot) {
                  snapshot.debateStreamContent = currentRoundContent
                }
                if (isActive) {
                  setDebateStreamContent(currentRoundContent)
                }
              } else if (event.type === 'done') {
                if (event.error) {
                  message.error(`辩论模型出错: ${event.error}`)
                }
                const newRound: DebateRound = {
                  round: event.debate_round,
                  role: currentRoundRole || (event.debate_round === 1 ? '看多方' : event.debate_round === 2 ? '质疑方' : '综合判断'),
                  model: event.model,
                  model_name: event.model_name || event.model,
                  content: currentRoundContent,
                  response_id: event.response_id || '',
                  tokens: event.tokens || 0,
                  latency_ms: event.latency_ms || 0,
                  error: event.error || null,
                }
                if (snapshot) {
                  snapshot.debateRounds = [...snapshot.debateRounds, newRound]
                  snapshot.debateStreamContent = ''
                }
                if (isActive) {
                  setDebateRounds((prev) => [...prev, newRound])
                  setDebateStreamContent('')
                }
                currentRoundContent = ''
                currentRoundRole = ''
              } else if (event.type === 'debate_aborted') {
                message.warning(`辩论中止: ${event.reason}`)
                if (snapshot) snapshot.activeDebateRound = 0
                if (isActive) setActiveDebateRound(0)
              } else if (event.type === 'all_done') {
                if (snapshot) snapshot.activeDebateRound = 0
                if (isActive) setActiveDebateRound(0)
              }
            } catch (parseErr) {
              console.warn('Debate SSE parse error:', parseErr)
            }
          }
        }
      }
      // Auto-fetch debate summary after completion
      if (convId && debateMessageId) {
        const snap = convStreamSnapshotsRef.current[convId!]
        if (snap) snap.debateSummaryLoading = true
        if (activeConvIdRef.current === convId) setDebateSummaryLoading(true)
        try {
          const summaryRes = await api.post(`/chat/conversations/${convId}/messages/${debateMessageId}/debate-summary`)
          if (snap) snap.debateSummary = summaryRes.data
          if (activeConvIdRef.current === convId) setDebateSummary(summaryRes.data)
        } catch (summaryErr) {
          console.warn('Failed to generate debate summary:', summaryErr)
        } finally {
          if (snap) snap.debateSummaryLoading = false
          if (activeConvIdRef.current === convId) setDebateSummaryLoading(false)
        }
      }
    } catch (err: any) {
      if (err.name === 'AbortError') {
        // User cancelled debate
      } else {
        message.error('辩论发送失败，请重试')
        console.error('Debate error:', err)
      }
    } finally {
      abortControllerRef.current = null
      wasCancelledRef.current = false

      setSendingConvIds(prev => {
        const next = new Set(prev)
        next.delete(convId!)
        return next
      })
      delete convStreamSnapshotsRef.current[convId!]

      if (activeConvIdRef.current === convId && convId) {
        const loaded = await fetchMessages(convId)
        if (loaded && activeConvIdRef.current === convId) {
          setDebateStreaming(false)
          setActiveDebateRound(0)
          setDebateStreamContent('')
        }
      }
      fetchConversations()
    }
  }

  // ── Cancel handler ──────────────────────────────────────────

  const handleCancel = useCallback(() => {
    wasCancelledRef.current = true
    abortControllerRef.current?.abort()
  }, [])

  // ── Normal send handler ─────────────────────────────────────

  // ── Personal-knowledge-base panel (drag-drop into the chat input) ─

  const fetchKbTreeFor = useCallback(async (scope: 'personal' | 'public') => {
    setKbTreeLoading(true)
    try {
      const res = await api.get<{ folders: KbFolderNode[] }>(
        '/user-kb/tree', { params: { scope } },
      )
      if (scope === 'personal') setKbTreePersonal(res.data.folders)
      else setKbTreePublic(res.data.folders)
    } catch (err: any) {
      // Silent — the KB panel may open without the user caring about errors.
      console.error('kb tree fetch failed', err)
    } finally {
      setKbTreeLoading(false)
    }
  }, [])

  // Fetch the selected scope's tree whenever the panel opens or scope changes.
  useEffect(() => {
    if (!kbPanelOpen) return
    fetchKbTreeFor(kbPanelScope)
  }, [kbPanelOpen, kbPanelScope, fetchKbTreeFor])

  // Load *all* documents in the current scope and bucket them by folder_id.
  // The backend caps ``limit`` at 200, so we page through until we've
  // collected everything (or hit a sane safety ceiling). Mirrors the
  // pagination loop in MyKnowledgeBase.tsx so the two views never diverge.
  const fetchKbFolderDocs = useCallback(
    async (scope: 'personal' | 'public') => {
      const cacheKey = `${scope}::ALL`
      setKbFolderDocsLoading((prev) => ({ ...prev, [cacheKey]: true }))
      setKbDocsError(null)
      try {
        const PAGE = 200       // backend max
        const MAX_PAGES = 10   // 2000 docs — well above realistic per-user volume
        const collected: any[] = []
        let offset = 0
        for (let i = 0; i < MAX_PAGES; i++) {
          const res = await api.get<{ items: any[]; total: number }>(
            '/user-kb/documents',
            { params: { scope, limit: PAGE, offset } },
          )
          const items = res.data.items || []
          collected.push(...items)
          if (items.length < PAGE) break
          offset += PAGE
          if (collected.length >= (res.data.total || 0)) break
        }
        const byFolder: Record<string, KbDocumentRef[]> = {}
        for (const d of collected) {
          const fid = d.folder_id || ''  // "" for unfiled
          const ref: KbDocumentRef = {
            id: d.id,
            title: d.title || d.original_filename,
            filename: d.original_filename,
            file_extension: d.file_extension,
            scope: d.scope || 'personal',
            folder_id: d.folder_id || null,
          }
          if (!byFolder[fid]) byFolder[fid] = []
          byFolder[fid].push(ref)
        }
        setKbFolderDocs((prev) => {
          const next = { ...prev }
          for (const k of Object.keys(next)) {
            if (k.startsWith(`${scope}::`)) delete next[k]
          }
          for (const [fid, refs] of Object.entries(byFolder)) {
            next[`${scope}::${fid}`] = refs
          }
          return next
        })
      } catch (err: any) {
        const detail = err?.response?.data?.detail || err?.message || '未知错误'
        console.error('kb docs fetch failed', err)
        setKbDocsError(String(detail))
      } finally {
        setKbFolderDocsLoading((prev) => ({ ...prev, [cacheKey]: false }))
      }
    },
    [],
  )

  // Fetch docs whenever the panel opens or the scope changes.
  useEffect(() => {
    if (!kbPanelOpen) return
    fetchKbFolderDocs(kbPanelScope)
  }, [kbPanelOpen, kbPanelScope, fetchKbFolderDocs])

  // Attach a doc to the chat input (dedupes by id).
  const attachKbDoc = (ref: KbDocumentRef) => {
    setKbRefs((prev) => {
      if (prev.some((r) => r.id === ref.id)) return prev
      if (prev.length >= 8) {
        message.warning('最多引用 8 份知识库文档')
        return prev
      }
      return [...prev, ref]
    })
  }
  const removeKbDoc = (id: string) =>
    setKbRefs((prev) => prev.filter((r) => r.id !== id))

  // HTML5 drag source for KB file items.
  const makeKbDragHandlers = (ref: KbDocumentRef) => ({
    draggable: true,
    onDragStart: (e: React.DragEvent) => {
      try {
        e.dataTransfer.setData(KB_DRAG_MIME, JSON.stringify(ref))
        // Include text/plain so native drop targets outside our app don't
        // error out — they'll just paste the title.
        e.dataTransfer.setData('text/plain', ref.title || ref.filename)
      } catch {
        /* some browsers restrict custom MIMEs — the text fallback still works */
      }
      e.dataTransfer.effectAllowed = 'copy'
      setKbDragging(true)
    },
    onDragEnd: () => setKbDragging(false),
  })

  // Drop target on the chat input textarea wrapper.
  //
  // We use a counter on dragenter/dragleave because with nested children
  // (the textarea + overlay) the naive ``currentTarget === target`` check
  // flickers every time the drag crosses a child boundary. Counting enter
  // vs leave gives us a stable "is the pointer still anywhere inside the
  // wrapper" signal.
  const hasKbPayload = (dt: DataTransfer | null | undefined): boolean => {
    if (!dt) return false
    // ``types`` is array-like in modern browsers; convert for safe iteration
    // and fall back to ``includes`` which exists on both real arrays and
    // DOMStringList polyfills.
    try {
      return Array.from(dt.types).includes(KB_DRAG_MIME)
    } catch {
      return (dt.types as any).includes?.(KB_DRAG_MIME) === true
    }
  }
  const inputDropHandlers = {
    onDragEnter: (e: React.DragEvent) => {
      if (!hasKbPayload(e.dataTransfer)) return
      e.preventDefault()
      kbDragOverDepthRef.current += 1
      setKbDragOverInput(true)
    },
    onDragOver: (e: React.DragEvent) => {
      // preventDefault on dragover is *required* to mark this element as a
      // valid drop target. Without it, the browser's default (e.g. the
      // textarea inserting the text/plain title) wins.
      if (!hasKbPayload(e.dataTransfer)) return
      e.preventDefault()
      e.dataTransfer.dropEffect = 'copy'
      if (!kbDragOverInput) setKbDragOverInput(true)
    },
    onDragLeave: (e: React.DragEvent) => {
      if (!hasKbPayload(e.dataTransfer)) return
      kbDragOverDepthRef.current = Math.max(0, kbDragOverDepthRef.current - 1)
      if (kbDragOverDepthRef.current === 0) setKbDragOverInput(false)
    },
    onDrop: (e: React.DragEvent) => {
      if (!hasKbPayload(e.dataTransfer)) return
      e.preventDefault()
      // The textarea would otherwise paste the text/plain fallback; prevent
      // default on both the drop and any pending dragover.
      e.stopPropagation()
      kbDragOverDepthRef.current = 0
      setKbDragOverInput(false)
      try {
        const raw = e.dataTransfer.getData(KB_DRAG_MIME)
        if (!raw) return
        const ref = JSON.parse(raw) as KbDocumentRef
        attachKbDoc(ref)
      } catch (err) {
        console.warn('invalid kb drop payload', err)
      }
    },
  }

  const handleSend = async () => {
    if (debateMode) return handleDebateSend()

    // Backend requires ``content`` to be non-empty (schema min_length=1).
    // KB refs alone aren't enough — the user still needs to type a
    // question or attach a file. The send button's disabled state is kept
    // in sync with this guard so the button never looks clickable when
    // clicking would silently no-op.
    if (!inputText.trim() && attachments.length === 0) {
      if (kbRefs.length > 0) {
        message.info('请输入一个问题，或点击"移除"清理引用的文档')
      }
      return
    }
    const activeModels = selectedModels.filter(m => !closedModels.includes(m))
    const modelsToSend = sendTarget === 'all' ? activeModels : [sendTarget]
    if (modelsToSend.length === 0) {
      message.warning('请至少选择一个活跃模型')
      return
    }

    let convId = activeConvId

    // Create conversation if needed
    if (!convId) {
      try {
        const res = await api.post('/chat/conversations', { title: '新对话' })
        convId = res.data.id
        setConversations((prev) => [res.data, ...prev])
        setActiveConvId(convId)
      } catch {
        message.error('创建对话失败')
        return
      }
    }

    // Block duplicate sends to the same conversation
    if (sendingConvIds.has(convId!)) return

    setSendingConvIds(prev => new Set(prev).add(convId!))
    const userText = inputText
    setInputText('')

    // Optimistically add user message
    const tempUserMsg: ChatMessageData = {
      id: `temp_${Date.now()}`,
      role: 'user',
      content: userText,
      attachments: attachments.map((a) => ({
        filename: a.filename,
        file_type: a.file_type,
        file_url: a.file_url,
      })),
      is_debate: false,
      model_responses: [],
      created_at: new Date().toISOString(),
    }
    setMessages((prev) => [...prev, tempUserMsg])
    setStreamingModels(modelsToSend)
    setStreamingContents({})
    setModelSources({})  // Clear stale citation sources
    // Reset send target after sending to specific model
    if (sendTarget !== 'all') setSendTarget('all')

    // Initialize per-conversation streaming snapshot
    convStreamSnapshotsRef.current[convId!] = {
      streamingContents: {},
      streamingModels: [...modelsToSend],
      toolCallingModels: {},
      debateRounds: [],
      activeDebateRound: 0,
      debateStreamContent: '',
      debateStreaming: false,
      debateSummary: null,
      debateSummaryLoading: false,
    }

    const currentAttachments = attachments.map((a) => ({
      filename: a.filename,
      file_type: a.file_type,
      file_url: a.file_url,
      file_path: a.file_path,  // server-side absolute path from upload response
    }))
    const currentKbDocIds = kbRefs.map((r) => r.id)
    setAttachments([])
    setKbRefs([])

    try {
      // Use SSE streaming
      const controller = new AbortController()
      abortControllerRef.current = controller
      const token = useAuthStore.getState().token
      const response = await fetch(`/api/chat/conversations/${convId}/messages/stream`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`,
        },
        signal: controller.signal,
        body: JSON.stringify({
          content: userText,
          models: modelsToSend,
          attachments: currentAttachments,
          system_prompt: systemPrompt,
          mode: chatMode,
          web_search: webSearchMode,
          kb_enabled: kbEnabled,
          user_kb_enabled: userKbEnabled,
          kb_document_ids: currentKbDocIds,
        }),
      })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`)
      }

      const reader = response.body?.getReader()
      const decoder = new TextDecoder()
      let buffer = ''
      // Streaming read timeout: if no data received for 120s, assume connection lost
      const STREAM_READ_TIMEOUT = 120_000

      if (reader) {
        while (true) {
          let readResult: ReadableStreamReadResult<Uint8Array>
          try {
            readResult = await Promise.race([
              reader.read(),
              new Promise<never>((_, reject) =>
                setTimeout(() => reject(new Error('stream_timeout')), STREAM_READ_TIMEOUT)
              ),
            ])
          } catch (e: any) {
            if (e?.message === 'stream_timeout') {
              console.warn('SSE stream read timeout — closing connection')
              reader.cancel()
            }
            break
          }
          const { done, value } = readResult
          if (done) break

          buffer += decoder.decode(value, { stream: true })
          const lines = buffer.split('\n')
          buffer = lines.pop() || ''

          for (const line of lines) {
            if (!line.startsWith('data: ')) continue
            const data = line.slice(6).trim()
            if (data === '[DONE]') break

            try {
              const event = JSON.parse(data)
              const snapshot = convStreamSnapshotsRef.current[convId!]
              const isActive = activeConvIdRef.current === convId

              if (event.type === 'meta') {
                currentMessageIdRef.current = event.message_id || ''
                if (Array.isArray(event.memory_ids)) {
                  setMemoryIdsThisTurn(event.memory_ids)
                }
              } else if (event.type === 'delta') {
                if (snapshot) {
                  snapshot.streamingContents[event.model] = (snapshot.streamingContents[event.model] || '') + event.delta
                }
                if (isActive) {
                  setStreamingContents((prev) => ({
                    ...prev,
                    [event.model]: (prev[event.model] || '') + event.delta,
                  }))
                }
              } else if (event.type === 'tool_status') {
                if (event.status === 'calling') {
                  if (snapshot) {
                    snapshot.toolCallingModels[event.model] = TOOL_LABELS[event.tool_name] || event.tool_name
                  }
                  if (isActive) {
                    setToolCallingModels((prev) => ({
                      ...prev,
                      [event.model]: TOOL_LABELS[event.tool_name] || event.tool_name,
                    }))
                  }
                } else {
                  if (snapshot) {
                    delete snapshot.toolCallingModels[event.model]
                  }
                  if (isActive) {
                    setToolCallingModels((prev) => {
                      const next = { ...prev }
                      delete next[event.model]
                      return next
                    })
                  }
                }
              } else if (event.type === 'search_status') {
                const label = event.status === 'searching'
                  ? `🔍 搜索: ${event.query || ''}`
                  : ''
                if (event.status === 'searching') {
                  if (snapshot) snapshot.toolCallingModels[event.model] = label
                  if (isActive) setToolCallingModels((prev) => ({ ...prev, [event.model]: label }))
                } else {
                  if (snapshot) delete snapshot.toolCallingModels[event.model]
                  if (isActive) setToolCallingModels((prev) => { const n = { ...prev }; delete n[event.model]; return n })
                }
              } else if (event.type === 'read_status') {
                if (event.status === 'reading') {
                  const host = (() => { try { return new URL(event.url).hostname } catch { return event.url?.slice(0, 30) } })()
                  const label = `📖 阅读: ${host}`
                  if (snapshot) snapshot.toolCallingModels[event.model] = label
                  if (isActive) setToolCallingModels((prev) => ({ ...prev, [event.model]: label }))
                } else {
                  if (snapshot) delete snapshot.toolCallingModels[event.model]
                  if (isActive) setToolCallingModels((prev) => { const n = { ...prev }; delete n[event.model]; return n })
                }
              } else if (event.type === 'sources') {
                if (isActive) {
                  setModelSources((prev) => ({
                    ...prev,
                    [event.model]: event.sources || [],
                  }))
                }
              } else if (event.type === 'done') {
                if (event.error) {
                  message.error(`${event.model_name || event.model}: ${(event.error || '').slice(0, 100)}`)
                }
                if (snapshot) {
                  snapshot.streamingModels = snapshot.streamingModels.filter((m) => m !== event.model)
                  delete snapshot.toolCallingModels[event.model]
                }
                if (isActive) {
                  setStreamingModels((prev) => prev.filter((m) => m !== event.model))
                  setToolCallingModels((prev) => {
                    const next = { ...prev }
                    delete next[event.model]
                    return next
                  })
                }
              } else if (event.type === 'error') {
                message.error(`流式响应出错: ${event.error || '未知错误'}`)
              }
            } catch (parseErr) {
              console.warn('SSE JSON parse error:', parseErr, 'raw data:', data)
            }
          }
        }
      }
    } catch (err: any) {
      if (err.name === 'AbortError') {
        // User cancelled or page unloading — partial content saved in finally
      } else {
        message.error('发送失败，请重试')
        console.error('Send error:', err)
      }
    } finally {
      wasCancelledRef.current = false
      abortControllerRef.current = null

      // Always try to save partial content for incomplete responses
      // (covers: user cancel, page navigation, network error, etc.)
      if (currentMessageIdRef.current && convId) {
        const partialContents = convStreamSnapshotsRef.current[convId!]?.streamingContents || {}
        const nonEmpty = Object.fromEntries(
          Object.entries(partialContents).filter(([, v]) => v && v.length > 0)
        )
        if (Object.keys(nonEmpty).length > 0) {
          try {
            await api.post(`/chat/conversations/${convId}/messages/${currentMessageIdRef.current}/save-partial`, {
              partial_responses: nonEmpty,
            })
          } catch { /* ignore — backend may have already saved via disconnect detection */ }
        }
      }
      currentMessageIdRef.current = ''

      setSendingConvIds(prev => {
        const next = new Set(prev)
        next.delete(convId!)
        return next
      })
      delete convStreamSnapshotsRef.current[convId!]

      if (activeConvIdRef.current === convId && convId) {
        // Load persisted responses from DB FIRST, then clear streaming state.
        // If fetchMessages fails, keep streaming content visible as fallback
        // so the user doesn't lose completed responses.
        const loaded = await fetchMessages(convId)
        // Re-check after async: user may have switched conversation during fetch
        if (loaded && activeConvIdRef.current === convId) {
          setStreamingModels([])
          setStreamingContents({})
          setToolCallingModels({})
        }
      }
      fetchConversations()
    }
  }

  // ── Regenerate handler ─────────────────────────────────────

  const handleRegenerate = async (msg: ChatMessageData) => {
    if (!activeConvId || sending) return

    const modelsToRegenerate = msg.model_responses.map(r => r.model_id)
    if (modelsToRegenerate.length === 0) return

    // Clear old responses from local state
    setMessages(prev => prev.map(m =>
      m.id === msg.id ? { ...m, model_responses: [] } : m
    ))

    const convId = activeConvId
    setSendingConvIds(prev => new Set(prev).add(convId!))
    setStreamingModels(modelsToRegenerate)
    setStreamingContents({})
    setModelSources({})

    convStreamSnapshotsRef.current[convId!] = {
      streamingContents: {},
      streamingModels: [...modelsToRegenerate],
      toolCallingModels: {},
      debateRounds: [],
      activeDebateRound: 0,
      debateStreamContent: '',
      debateStreaming: false,
      debateSummary: null,
      debateSummaryLoading: false,
    }

    try {
      const controller = new AbortController()
      abortControllerRef.current = controller
      const token = useAuthStore.getState().token
      const response = await fetch(`/api/chat/conversations/${convId}/messages/${msg.id}/regenerate`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`,
        },
        signal: controller.signal,
        body: JSON.stringify({
          models: modelsToRegenerate,
          system_prompt: systemPrompt,
          mode: chatMode,
          web_search: webSearchMode,
          kb_enabled: kbEnabled,
          user_kb_enabled: userKbEnabled,
          // Regenerating does not re-attach KB refs — the user can re-drag
          // files if they want the new retry to see them.
          kb_document_ids: [],
        }),
      })

      if (!response.ok) throw new Error(`HTTP ${response.status}`)

      const reader = response.body?.getReader()
      const decoder = new TextDecoder()
      let buffer = ''
      const STREAM_READ_TIMEOUT = 120_000

      if (reader) {
        while (true) {
          let readResult: ReadableStreamReadResult<Uint8Array>
          try {
            readResult = await Promise.race([
              reader.read(),
              new Promise<never>((_, reject) =>
                setTimeout(() => reject(new Error('stream_timeout')), STREAM_READ_TIMEOUT)
              ),
            ])
          } catch (e: any) {
            if (e?.message === 'stream_timeout') {
              console.warn('Regenerate SSE stream read timeout — closing connection')
              reader.cancel()
            }
            break
          }
          const { done, value } = readResult
          if (done) break
          buffer += decoder.decode(value, { stream: true })
          const lines = buffer.split('\n')
          buffer = lines.pop() || ''

          for (const line of lines) {
            if (!line.startsWith('data: ')) continue
            const data = line.slice(6).trim()
            if (data === '[DONE]') break

            try {
              const event = JSON.parse(data)
              const snapshot = convStreamSnapshotsRef.current[convId!]
              const isActive = activeConvIdRef.current === convId

              if (event.type === 'meta') {
                currentMessageIdRef.current = event.message_id || ''
              } else if (event.type === 'delta') {
                if (snapshot) {
                  snapshot.streamingContents[event.model] = (snapshot.streamingContents[event.model] || '') + event.delta
                }
                if (isActive) {
                  setStreamingContents(prev => ({
                    ...prev,
                    [event.model]: (prev[event.model] || '') + event.delta,
                  }))
                }
              } else if (event.type === 'tool_status') {
                if (event.status === 'calling') {
                  if (snapshot) snapshot.toolCallingModels[event.model] = TOOL_LABELS[event.tool_name] || event.tool_name
                  if (isActive) setToolCallingModels(prev => ({ ...prev, [event.model]: TOOL_LABELS[event.tool_name] || event.tool_name }))
                } else {
                  if (snapshot) delete snapshot.toolCallingModels[event.model]
                  if (isActive) setToolCallingModels(prev => { const n = { ...prev }; delete n[event.model]; return n })
                }
              } else if (event.type === 'search_status') {
                const label = event.status === 'searching' ? `🔍 搜索: ${event.query || ''}` : ''
                if (event.status === 'searching') {
                  if (snapshot) snapshot.toolCallingModels[event.model] = label
                  if (isActive) setToolCallingModels(prev => ({ ...prev, [event.model]: label }))
                } else {
                  if (snapshot) delete snapshot.toolCallingModels[event.model]
                  if (isActive) setToolCallingModels(prev => { const n = { ...prev }; delete n[event.model]; return n })
                }
              } else if (event.type === 'read_status') {
                if (event.status === 'reading') {
                  const host = (() => { try { return new URL(event.url).hostname } catch { return event.url?.slice(0, 30) } })()
                  const label = `📖 阅读: ${host}`
                  if (snapshot) snapshot.toolCallingModels[event.model] = label
                  if (isActive) setToolCallingModels(prev => ({ ...prev, [event.model]: label }))
                } else {
                  if (snapshot) delete snapshot.toolCallingModels[event.model]
                  if (isActive) setToolCallingModels(prev => { const n = { ...prev }; delete n[event.model]; return n })
                }
              } else if (event.type === 'sources') {
                if (isActive) setModelSources(prev => ({ ...prev, [event.model]: event.sources || [] }))
              } else if (event.type === 'done') {
                if (event.error) message.error(`${event.model_name || event.model}: ${(event.error || '').slice(0, 100)}`)
                if (snapshot) {
                  snapshot.streamingModels = snapshot.streamingModels.filter(m => m !== event.model)
                  delete snapshot.toolCallingModels[event.model]
                }
                if (isActive) {
                  setStreamingModels(prev => prev.filter(m => m !== event.model))
                  setToolCallingModels(prev => { const n = { ...prev }; delete n[event.model]; return n })
                }
              } else if (event.type === 'error') {
                message.error(`流式响应出错: ${event.error || '未知错误'}`)
              }
            } catch (parseErr) {
              console.warn('SSE JSON parse error:', parseErr, 'raw data:', data)
            }
          }
        }
      }
    } catch (err: any) {
      if (err.name === 'AbortError') {
        // User cancelled or page unloading
      } else {
        message.error('重新生成失败，请重试')
        console.error('Regenerate error:', err)
      }
    } finally {
      wasCancelledRef.current = false
      abortControllerRef.current = null

      // Always try to save partial content for incomplete responses
      if (currentMessageIdRef.current && convId) {
        const partialContents = convStreamSnapshotsRef.current[convId!]?.streamingContents || {}
        const nonEmpty = Object.fromEntries(
          Object.entries(partialContents).filter(([, v]) => v && v.length > 0)
        )
        if (Object.keys(nonEmpty).length > 0) {
          try {
            await api.post(`/chat/conversations/${convId}/messages/${currentMessageIdRef.current}/save-partial`, {
              partial_responses: nonEmpty,
            })
          } catch { /* ignore — backend may have already saved via disconnect detection */ }
        }
      }
      currentMessageIdRef.current = ''

      setSendingConvIds(prev => {
        const next = new Set(prev)
        next.delete(convId!)
        return next
      })
      delete convStreamSnapshotsRef.current[convId!]

      if (activeConvIdRef.current === convId && convId) {
        const loaded = await fetchMessages(convId)
        if (loaded && activeConvIdRef.current === convId) {
          setStreamingModels([])
          setStreamingContents({})
          setToolCallingModels({})
        }
      }
      fetchConversations()
    }
  }

  const handleRate = async (responseId: string, rating: number) => {
    try {
      await api.post(`/chat/rate/${responseId}`, { rating })
      // Update local state
      setMessages((prev) =>
        prev.map((m) => ({
          ...m,
          model_responses: m.model_responses.map((r) =>
            r.id === responseId ? { ...r, rating } : r
          ),
        }))
      )
      fetchRankings()
    } catch {
      message.error('评分失败')
    }
  }

  const handleUpload = async (file: File) => {
    const formData = new FormData()
    formData.append('file', file)
    try {
      const res = await api.post('/chat/upload', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
      })
      setAttachments((prev) => [...prev, res.data])
      message.success(`已上传: ${res.data.filename}`)
    } catch (err: any) {
      message.error(err.response?.data?.detail || '上传失败')
    }
    return false // Prevent default upload
  }

  const handleExport = async () => {
    if (!activeConvId) return
    try {
      const res = await api.get(`/chat/export/${activeConvId}`)
      const blob = new Blob([res.data.markdown], { type: 'text/markdown' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `${res.data.title}.md`
      a.click()
      URL.revokeObjectURL(url)
      message.success('导出成功')
    } catch {
      message.error('导出失败')
    }
  }

  const handleSummarize = async () => {
    if (!activeConvId) return
    try {
      const res = await api.post(`/chat/summarize/${activeConvId}`)
      Modal.info({
        title: '对话总结',
        content: <div style={{ whiteSpace: 'pre-wrap', maxHeight: 400, overflow: 'auto' }}>{res.data.summary}</div>,
        width: 640,
      })
    } catch {
      message.error('总结生成失败')
    }
  }

  const handleCloseModel = (modelId: string) => {
    setClosedModels(prev => [...prev, modelId])
    if (expandedModel === modelId) setExpandedModel(null)
    if (sendTarget === modelId) setSendTarget('all')
  }

  const handleReopenModel = (modelId: string) => {
    setClosedModels(prev => prev.filter(m => m !== modelId))
  }

  const handleCreateTemplate = async () => {
    if (!newTplName || !newTplContent) {
      message.warning('请填写名称和内容')
      return
    }
    try {
      await api.post('/chat/templates', {
        name: newTplName,
        content: newTplContent,
        category: newTplCategory,
      })
      setTemplateModalOpen(false)
      setNewTplName('')
      setNewTplContent('')
      fetchTemplates()
      message.success('提示词已保存')
    } catch {
      message.error('保存失败')
    }
  }

  const handleDeleteTemplate = async (id: string) => {
    try {
      await api.delete(`/chat/templates/${id}`)
      fetchTemplates()
    } catch {
      message.error('删除失败')
    }
  }

  const applyTemplate = (tpl: PromptTemplate) => {
    setSystemPrompt(tpl.content)
    setActivePromptName(tpl.name)
    if (!tpl.is_system && !tpl.id.startsWith('_sys_')) {
      api.post(`/chat/templates/${tpl.id}/use`).catch(() => {})
    }
    message.success(`已启用提示词: ${tpl.name}`)
  }

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text)
    message.success('已复制到剪贴板')
  }

  // ── Filtered conversations ─────────────────────────────────

  const filteredConvs = conversations.filter((c) =>
    !convSearch || c.title.toLowerCase().includes(convSearch.toLowerCase())
  )

  // ── All templates (system + user) ──────────────────────────

  const allTemplates = [...SYSTEM_PROMPTS, ...templates]

  // ── Render: Conversation Sidebar ───────────────────────────

  const renderSidebar = () => (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <div style={{ padding: '12px 12px 8px' }}>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          block
          onClick={createConversation}
          style={{ marginBottom: 8 }}
        >
          新建对话
        </Button>
        <Input
          prefix={<SearchOutlined style={{ color: '#94a3b8' }} />}
          placeholder="搜索对话..."
          size="small"
          value={convSearch}
          onChange={(e) => setConvSearch(e.target.value)}
          allowClear
        />
      </div>

      <div style={{ flex: 1, overflow: 'auto', padding: '0 4px' }}>
        {filteredConvs.length === 0 ? (
          <Empty description="暂无对话" image={Empty.PRESENTED_IMAGE_SIMPLE} style={{ marginTop: 40 }} />
        ) : (
          (() => {
            // Group conversations by date
            let lastGroup = ''
            return filteredConvs.map((conv) => {
              const group = dateGroup(conv.updated_at || conv.created_at)
              const showHeader = group !== lastGroup
              lastGroup = group
              return (
                <div key={conv.id}>
                  {showHeader && (
                    <div style={{ padding: '8px 12px 2px', marginTop: lastGroup === group ? 0 : 4 }}>
                      <Text type="secondary" style={{ fontSize: 11, fontWeight: 600, textTransform: 'uppercase', letterSpacing: 0.5 }}>
                        {group}
                      </Text>
                    </div>
                  )}
                  <div
                    onClick={() => setActiveConvId(conv.id)}
                    style={{
                      padding: '10px 12px',
                      margin: '2px 4px',
                      borderRadius: 8,
                      cursor: 'pointer',
                      backgroundColor: activeConvId === conv.id ? '#e8f0fe' : 'transparent',
                      borderLeft: activeConvId === conv.id ? '3px solid #2563eb' : '3px solid transparent',
                      transition: 'all 0.15s',
                    }}
                    onMouseEnter={(e) => {
                      if (activeConvId !== conv.id) e.currentTarget.style.backgroundColor = '#f1f5f9'
                    }}
                    onMouseLeave={(e) => {
                      if (activeConvId !== conv.id) e.currentTarget.style.backgroundColor = 'transparent'
                    }}
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                      <Text
                        ellipsis
                        style={{
                          fontSize: 13,
                          fontWeight: activeConvId === conv.id ? 600 : 400,
                          flex: 1,
                          color: '#1e293b',
                        }}
                      >
                        {conv.is_pinned && <PushpinFilled style={{ color: '#2563eb', marginRight: 4, fontSize: 11 }} />}
                        {conv.title}
                      </Text>
                      {sendingConvIds.has(conv.id) && (
                        <Spin size="small" style={{ marginLeft: 6, flexShrink: 0 }} />
                      )}
                      <Space size={2} style={{ marginLeft: 4, flexShrink: 0 }}>
                        <Tooltip title={conv.is_pinned ? '取消置顶' : '置顶'}>
                          <Button
                            type="text"
                            size="small"
                            icon={conv.is_pinned ? <PushpinFilled style={{ fontSize: 11 }} /> : <PushpinOutlined style={{ fontSize: 11 }} />}
                            onClick={(e) => { e.stopPropagation(); togglePin(conv) }}
                            style={{ width: 20, height: 20 }}
                          />
                        </Tooltip>
                        <Popconfirm title="确认删除？" onConfirm={() => deleteConversation(conv.id)} okText="删除" cancelText="取消">
                          <Button
                            type="text"
                            size="small"
                            danger
                            icon={<DeleteOutlined style={{ fontSize: 11 }} />}
                            onClick={(e) => e.stopPropagation()}
                            style={{ width: 20, height: 20 }}
                          />
                        </Popconfirm>
                      </Space>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 6 }}>
                      <Text type="secondary" style={{ fontSize: 11, flex: 1, minWidth: 0 }} ellipsis>
                        {conv.last_message_preview || '空对话'}
                      </Text>
                      <Text type="secondary" style={{ fontSize: 10, flexShrink: 0, color: '#94a3b8' }}>
                        {timeAgo(conv.updated_at || conv.created_at)}
                      </Text>
                    </div>
                  </div>
                </div>
              )
            })
          })()
        )}
      </div>

      {/* Quick links at bottom of sidebar */}
      <Divider style={{ margin: '4px 0' }} />
      <div style={{ padding: '4px 12px 12px' }}>
        <Text type="secondary" style={{ fontSize: 11, fontWeight: 600 }}>常用工具</Text>
        <div style={{ marginTop: 4, display: 'flex', flexWrap: 'wrap', gap: 4 }}>
          {USEFUL_LINKS.slice(0, 4).map((link) => (
            <Tooltip key={link.url} title={link.desc}>
              <Tag
                style={{ cursor: 'pointer', fontSize: 11, margin: 0 }}
                onClick={() => window.open(link.url, '_blank')}
              >
                <GlobalOutlined style={{ marginRight: 3 }} />
                {link.name}
              </Tag>
            </Tooltip>
          ))}
          <Tag
            style={{ cursor: 'pointer', fontSize: 11, margin: 0 }}
            onClick={() => setRightDrawerOpen(true)}
          >
            更多...
          </Tag>
        </div>
      </div>
    </div>
  )

  // ── Render: Debate message (vertical timeline) ─────────────

  const DEBATE_COLOR_PALETTE = [
    { bg: '#eff6ff', border: '#3b82f6', badge: '#2563eb' },
    { bg: '#fef2f2', border: '#ef4444', badge: '#dc2626' },
    { bg: '#fffbeb', border: '#f59e0b', badge: '#d97706' },
    { bg: '#f0fdf4', border: '#22c55e', badge: '#16a34a' },
    { bg: '#faf5ff', border: '#a855f7', badge: '#9333ea' },
    { bg: '#fff1f2', border: '#fb7185', badge: '#e11d48' },
  ]

  const getDebateColor = (round: number) => DEBATE_COLOR_PALETTE[(round - 1) % DEBATE_COLOR_PALETTE.length]

  const RATING_COLORS: Record<string, string> = {
    '强烈看多': '#16a34a', '看多': '#22c55e', '中性': '#f59e0b', '看空': '#ef4444', '强烈看空': '#dc2626',
  }

  const renderDebateRoundCard = (
    key: string, round: number, label: string, modelName: string,
    content: string, isStreaming: boolean, latencyMs?: number | null,
    tokens?: number | null, responseId?: string, error?: string | null,
    rating?: number | null,
  ) => {
    const style = getDebateColor(round)
    return (
      <Card
        key={key}
        size="small"
        style={{ borderLeft: `3px solid ${style.border}`, background: style.bg }}
        title={
          <Space size={8}>
            <Tag color={style.badge} style={{ fontWeight: 600 }}>Round {round} - {label}</Tag>
            <Text strong style={{ fontSize: 13 }}>{modelName}</Text>
            {isStreaming && <Spin size="small" />}
            {!isStreaming && latencyMs && (
              <Tag style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>
                {(latencyMs / 1000).toFixed(1)}s
              </Tag>
            )}
            {!isStreaming && tokens != null && tokens > 0 && (
              <Tag style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>
                {tokens} tokens
              </Tag>
            )}
          </Space>
        }
        extra={!isStreaming && (
          <Tooltip title="复制">
            <Button
              type="text" size="small" icon={<CopyOutlined />}
              onClick={() => { navigator.clipboard.writeText(content); message.success('已复制') }}
            />
          </Tooltip>
        )}
      >
        {error ? (
          <Text type="danger" style={{ fontSize: 13 }}>{error}</Text>
        ) : content ? (
          <div style={{ fontSize: 14, lineHeight: 1.8, wordBreak: 'break-word' }}>
            <MarkdownRenderer content={content} />
          </div>
        ) : (
          <div style={{ textAlign: 'center', padding: 20 }}>
            <Spin />
            <div style={{ marginTop: 8, color: '#94a3b8', fontSize: 12 }}>正在思考...</div>
          </div>
        )}
        {!isStreaming && responseId && (
          <div style={{ marginTop: 8 }}>
            <Rate
              value={rating || 0}
              onChange={(val) => handleRate(responseId, val)}
              style={{ fontSize: 14 }}
            />
          </div>
        )}
      </Card>
    )
  }

  const copyAllDebateRounds = (responses: ModelResponseData[]) => {
    const sorted = [...responses]
      .filter((r) => (r.debate_round || 0) > 0)
      .sort((a, b) => (a.debate_round || 0) - (b.debate_round || 0))
    const text = sorted.map((r) => `## Round ${r.debate_round} - ${r.model_name}\n\n${r.content}`).join('\n\n---\n\n')
    navigator.clipboard.writeText(text)
    message.success('已复制全部辩论内容')
  }

  const renderDebateSummaryCard = (summary: any) => {
    if (!summary) return null
    const ratingColor = RATING_COLORS[summary.rating] || '#6b7280'
    return (
      <Card
        size="small"
        style={{ borderLeft: '3px solid #6366f1', background: '#eef2ff', marginTop: 12 }}
        title={
          <Space size={8}>
            <Tag color="#6366f1" style={{ fontWeight: 600 }}>辩论总结</Tag>
            {summary.rating && <Tag color={ratingColor} style={{ fontWeight: 600 }}>{summary.rating}</Tag>}
            {summary.confidence > 0 && <Text type="secondary" style={{ fontSize: 12 }}>信心: {summary.confidence}/10</Text>}
            {summary.time_horizon && <Text type="secondary" style={{ fontSize: 12 }}>{summary.time_horizon}</Text>}
          </Space>
        }
      >
        {summary.conclusion && <Paragraph strong style={{ fontSize: 14, marginBottom: 12 }}>{summary.conclusion}</Paragraph>}
        <Collapse ghost size="small" items={[
          ...(summary.key_bull_arguments?.length ? [{
            key: 'bull', label: <Text style={{ color: '#16a34a', fontSize: 13 }}>看多论据 ({summary.key_bull_arguments.length})</Text>,
            children: <ul style={{ margin: 0, paddingLeft: 20 }}>{summary.key_bull_arguments.map((a: string, i: number) => <li key={i} style={{ fontSize: 13 }}>{a}</li>)}</ul>,
          }] : []),
          ...(summary.key_bear_arguments?.length ? [{
            key: 'bear', label: <Text style={{ color: '#dc2626', fontSize: 13 }}>看空论据 ({summary.key_bear_arguments.length})</Text>,
            children: <ul style={{ margin: 0, paddingLeft: 20 }}>{summary.key_bear_arguments.map((a: string, i: number) => <li key={i} style={{ fontSize: 13 }}>{a}</li>)}</ul>,
          }] : []),
          ...(summary.action_items?.length ? [{
            key: 'actions', label: <Text style={{ color: '#2563eb', fontSize: 13 }}>建议操作 ({summary.action_items.length})</Text>,
            children: <ul style={{ margin: 0, paddingLeft: 20 }}>{summary.action_items.map((a: string, i: number) => <li key={i} style={{ fontSize: 13 }}>{a}</li>)}</ul>,
          }] : []),
          ...(summary.key_metrics_to_watch?.length ? [{
            key: 'metrics', label: <Text style={{ color: '#7c3aed', fontSize: 13 }}>关键监控指标</Text>,
            children: <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>{summary.key_metrics_to_watch.map((m: string, i: number) => <Tag key={i}>{m}</Tag>)}</div>,
          }] : []),
          ...(summary.unresolved_questions?.length ? [{
            key: 'questions', label: <Text type="secondary" style={{ fontSize: 13 }}>待解决问题</Text>,
            children: <ul style={{ margin: 0, paddingLeft: 20 }}>{summary.unresolved_questions.map((q: string, i: number) => <li key={i} style={{ fontSize: 13 }}>{q}</li>)}</ul>,
          }] : []),
        ]} />
        {summary.mentioned_tickers?.length > 0 && (
          <div style={{ marginTop: 8 }}>
            <Text type="secondary" style={{ fontSize: 11 }}>涉及标的: </Text>
            {summary.mentioned_tickers.map((t: string, i: number) => <Tag key={i} style={{ fontSize: 11 }}>{t}</Tag>)}
          </div>
        )}
      </Card>
    )
  }

  const renderDebateResponses = (responses: ModelResponseData[]) => {
    const debateResps = [...responses].filter((r) => (r.debate_round || 0) > 0).sort((a, b) => (a.debate_round || 0) - (b.debate_round || 0))
    const summaryResp = responses.find((r) => r.debate_round === -1)
    let parsedSummary = null
    if (summaryResp) {
      try { parsedSummary = JSON.parse(summaryResp.content) } catch { /* ignore */ }
    }

    return (
      <div style={{ marginLeft: 42, display: 'flex', flexDirection: 'column', gap: 12 }}>
        {debateResps.length > 1 && (
          <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
            <Button type="text" size="small" icon={<CopyOutlined />} onClick={() => copyAllDebateRounds(responses)}>
              复制全部
            </Button>
          </div>
        )}
        {debateResps.map((resp) => {
          const round = resp.debate_round || 1
          const colorStyle = getDebateColor(round)
          return renderDebateRoundCard(
            resp.id, round, colorStyle.badge ? (resp.model_name || `Round ${round}`) : `Round ${round}`,
            resp.model_name, resp.content, false, resp.latency_ms, resp.tokens_used,
            resp.id, resp.error, resp.rating,
          )
        })}
        {parsedSummary && renderDebateSummaryCard(parsedSummary)}
      </div>
    )
  }

  // ── Render: Message bubble ─────────────────────────────────

  const renderMessage = (msg: ChatMessageData, idx: number) => (
    <div key={msg.id} style={{ marginBottom: 20 }}>
      {/* User message */}
      {msg.role === 'user' && (
        <div style={{ display: 'flex', gap: 10, marginBottom: 12 }}>
          <Avatar size={32} icon={<UserOutlined />} style={{ backgroundColor: '#2563eb', flexShrink: 0 }} />
          <div style={{ flex: 1 }}>
            <Text strong style={{ fontSize: 13, color: '#475569' }}>
              {user?.display_name || user?.username}
            </Text>
            <div
              style={{
                marginTop: 4, padding: '10px 14px', background: '#f0f4ff',
                borderRadius: '4px 12px 12px 12px', fontSize: 14, lineHeight: 1.7,
                whiteSpace: 'pre-wrap', wordBreak: 'break-word',
              }}
            >
              {msg.content}
              {msg.attachments && msg.attachments.length > 0 && (
                <div style={{ marginTop: 8, display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                  {msg.attachments.map((att: any, i: number) => (
                    <Tag key={i} icon={att.file_type?.startsWith('image/') ? <FileImageOutlined /> : <FilePdfOutlined />}>
                      {att.filename}
                    </Tag>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Debate responses - vertical timeline */}
      {msg.is_debate && msg.model_responses.length > 0 && renderDebateResponses(msg.model_responses)}

      {/* Normal model responses - side by side */}
      {!msg.is_debate && msg.model_responses.length > 0 && (() => {
        const visibleResponses = msg.model_responses
          .filter(resp => {
            if (closedModels.includes(resp.model_id)) return false
            if (expandedModel && resp.model_id !== expandedModel) return false
            return true
          })
          // Sort by user's configured selectedModels order so a model does not
          // shift columns between streaming and persisted views.
          .slice()
          .sort((a, b) => {
            const ia = selectedModels.indexOf(a.model_id)
            const ib = selectedModels.indexOf(b.model_id)
            if (ia === -1 && ib === -1) return a.model_id.localeCompare(b.model_id)
            if (ia === -1) return 1
            if (ib === -1) return -1
            return ia - ib
          })
        if (visibleResponses.length === 0) return null
        const colCount = Math.min(visibleResponses.length, 3)
        return (
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: `repeat(${colCount}, minmax(0, 1fr))`,
              gap: 12,
              marginLeft: 42,
              minWidth: 0,
            }}
          >
            {visibleResponses.map((resp) => (
              <Card
                key={resp.id}
                size="small"
                title={
                  <Space size={6}>
                    <RobotOutlined style={{ color: '#2563eb' }} />
                    <Text strong style={{ fontSize: 13 }}>{resp.model_name}</Text>
                    {resp.latency_ms && (
                      <Tag color="blue" style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>
                        {(resp.latency_ms / 1000).toFixed(1)}s
                      </Tag>
                    )}
                    {resp.tokens_used != null && resp.tokens_used > 0 && (
                      <Tag style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>
                        {resp.tokens_used} tokens
                      </Tag>
                    )}
                  </Space>
                }
                extra={
                  <Space size={2}>
                    {!expandedModel && msg.model_responses.length > 1 && (
                      <Tooltip title="展开单模型视图">
                        <Button
                          type="text" size="small" icon={<ExpandAltOutlined />}
                          onClick={() => setExpandedModel(resp.model_id)}
                        />
                      </Tooltip>
                    )}
                    {expandedModel && (
                      <Tooltip title="返回多模型视图">
                        <Button
                          type="text" size="small" icon={<CompressOutlined />}
                          onClick={() => setExpandedModel(null)}
                        />
                      </Tooltip>
                    )}
                    <Tooltip title={`单独回复 ${resp.model_name}`}>
                      <Button
                        type="text" size="small" icon={<MessageOutlined />}
                        onClick={() => { setSendTarget(resp.model_id); inputRef.current?.focus() }}
                        style={sendTarget === resp.model_id ? { color: '#2563eb' } : undefined}
                      />
                    </Tooltip>
                    {msg.model_responses.length > 1 && (
                      <Tooltip title={`关闭 ${resp.model_name}`}>
                        <Button
                          type="text" size="small" icon={<CloseOutlined />}
                          onClick={() => handleCloseModel(resp.model_id)}
                        />
                      </Tooltip>
                    )}
                    <Tooltip title="复制回答">
                      <Button
                        type="text" size="small" icon={<CopyOutlined />}
                        onClick={() => copyToClipboard(resp.content)}
                      />
                    </Tooltip>
                  </Space>
                }
                style={{
                  borderColor: resp.error ? '#fecaca' : sendTarget === resp.model_id ? '#2563eb' : '#e2e8f0',
                  borderRadius: 10,
                  minWidth: 0,
                  overflow: 'hidden',
                  ...(sendTarget === resp.model_id ? { boxShadow: '0 0 0 1px #2563eb' } : {}),
                }}
                styles={{
                  header: { minHeight: 40, padding: '6px 12px' },
                  body: {
                    padding: '10px 14px',
                    maxHeight: expandedModel ? 'none' : 520,
                    overflow: 'auto',
                    overflowWrap: 'anywhere',
                  },
                }}
              >
                {resp.error && !resp.content ? (
                  // Error only — no partial content to show
                  <div style={{ padding: '8px 0' }}>
                    <Text type="danger" style={{ fontSize: 13 }}>
                      {resp.error}
                    </Text>
                  </div>
                ) : resp.error && resp.content ? (
                  // Partial content + error — show both
                  <>
                    <div style={{ fontSize: 14, lineHeight: 1.8, wordBreak: 'break-word' }}>
                      <CitationRenderer content={resp.content} sources={resp.sources || modelSources[resp.model_id] || []} />
                    </div>
                    <div style={{ marginTop: 8, padding: '6px 10px', background: '#fef2f2', borderRadius: 6, fontSize: 12, color: '#991b1b' }}>
                      {resp.error}
                    </div>
                  </>
                ) : (
                  <div style={{ fontSize: 14, lineHeight: 1.8, wordBreak: 'break-word' }}>
                    <CitationRenderer content={resp.content} sources={resp.sources || modelSources[resp.model_id] || []} />
                  </div>
                )}
                {/* Rating + detailed feedback */}
                <Divider style={{ margin: '8px 0' }} />
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                  <Space size={8} align="center" wrap>
                    <Rate
                      value={resp.rating || 0}
                      onChange={(val) => handleRate(resp.id, val)}
                      style={{ fontSize: 14 }}
                    />
                    <Text type="secondary" style={{ fontSize: 11 }}>
                      {resp.rating ? `${resp.rating}星` : '未评分'}
                    </Text>
                  </Space>
                  <Tooltip title="详细评价：多选标签 + 文字反馈，系统将自动学习并记忆">
                    <Button
                      type="text"
                      size="small"
                      icon={<MessageOutlined />}
                      onClick={() => setFeedbackTarget({
                        responseId: resp.id,
                        modelName: resp.model_name,
                        initialRating: resp.rating,
                        initialText: resp.rating_comment || '',
                      })}
                      style={{ color: '#2563eb', fontSize: 12, padding: '0 6px' }}
                    >
                      详细评价
                    </Button>
                  </Tooltip>
                </div>
              </Card>
            ))}
          </div>
        )
      })()}

      {/* Regenerate button — only on last message with completed responses */}
      {!sending && idx === messages.length - 1 && msg.model_responses.length > 0 && !msg.is_debate && (
        <div style={{ marginLeft: 42, marginTop: 8 }}>
          <Tooltip title="使用相同模型重新生成回复">
            <Button
              type="text"
              size="small"
              icon={<ReloadOutlined />}
              onClick={() => handleRegenerate(msg)}
              style={{ color: '#64748b', fontSize: 12 }}
            >
              重新生成
            </Button>
          </Tooltip>
        </div>
      )}
    </div>
  )

  // ── Render: Debate streaming ────────────────────────────────

  const renderDebateStreaming = () => {
    if (!debateStreaming && debateRounds.length === 0) return null

    return (
      <div style={{ marginLeft: 42, display: 'flex', flexDirection: 'column', gap: 12, marginBottom: 20 }}>
        {/* Completed rounds */}
        {debateRounds.map((r) => renderDebateRoundCard(
          `streaming-${r.round}`, r.round, r.role, r.model_name,
          r.content, false, r.latency_ms, r.tokens, r.response_id, r.error, null,
        ))}

        {/* Currently streaming round */}
        {activeDebateRound > 0 && renderDebateRoundCard(
          `active-${activeDebateRound}`, activeDebateRound, '', '',
          debateStreamContent, true,
        )}

        {/* Summary loading indicator */}
        {debateSummaryLoading && (
          <Card size="small" style={{ borderLeft: '3px solid #6366f1', background: '#eef2ff' }}>
            <div style={{ textAlign: 'center', padding: 12 }}>
              <Spin size="small" />
              <Text type="secondary" style={{ marginLeft: 8, fontSize: 13 }}>正在生成辩论总结...</Text>
            </div>
          </Card>
        )}

        {/* Summary card */}
        {debateSummary && renderDebateSummaryCard(debateSummary)}
      </div>
    )
  }

  // ── Render: Streaming responses ────────────────────────────

  const renderStreaming = () => {
    const allModels = selectedModels.filter(m => {
      if (closedModels.includes(m)) return false
      if (expandedModel && m !== expandedModel) return false
      return true
    })
    if (Object.keys(streamingContents).length === 0 && streamingModels.length === 0) return null
    if (allModels.length === 0) return null

    const colCount = Math.min(allModels.length, 3)
    return (
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: `repeat(${colCount}, minmax(0, 1fr))`,
          gap: 12,
          marginLeft: 42,
          marginBottom: 20,
          minWidth: 0,
        }}
      >
        {allModels.map((modelId) => {
          const info = models.find((m) => m.id === modelId)
          const content = streamingContents[modelId] || ''
          const isStreaming = streamingModels.includes(modelId)

          return (
            <Card
              key={modelId}
              size="small"
              title={
                <Space size={6}>
                  <RobotOutlined style={{ color: '#2563eb' }} />
                  <Text strong style={{ fontSize: 13 }}>{info?.name || modelId}</Text>
                  {isStreaming && <Spin size="small" />}
                </Space>
              }
              extra={
                <Space size={2}>
                  {!expandedModel && allModels.length > 1 && (
                    <Tooltip title="展开单模型视图">
                      <Button type="text" size="small" icon={<ExpandAltOutlined />}
                        onClick={() => setExpandedModel(modelId)} />
                    </Tooltip>
                  )}
                  {expandedModel && (
                    <Tooltip title="返回多模型视图">
                      <Button type="text" size="small" icon={<CompressOutlined />}
                        onClick={() => setExpandedModel(null)} />
                    </Tooltip>
                  )}
                  {selectedModels.filter(m => !closedModels.includes(m)).length > 1 && (
                    <Tooltip title={`关闭 ${info?.name || modelId}`}>
                      <Button type="text" size="small" icon={<CloseOutlined />}
                        onClick={() => handleCloseModel(modelId)} />
                    </Tooltip>
                  )}
                </Space>
              }
              style={{ borderColor: '#bfdbfe', borderRadius: 10, minWidth: 0, overflow: 'hidden' }}
              styles={{
                header: { minHeight: 40, padding: '6px 12px' },
                body: {
                  padding: '10px 14px',
                  maxHeight: expandedModel ? 'none' : 520,
                  overflow: 'auto',
                  overflowWrap: 'anywhere',
                },
              }}
            >
              {content ? (
                <div style={{ fontSize: 14, lineHeight: 1.8, wordBreak: 'break-word' }}>
                  <CitationRenderer content={content} sources={modelSources[modelId] || []} />
                </div>
              ) : toolCallingModels[modelId] ? (
                <div style={{ textAlign: 'center', padding: 20 }}>
                  <Spin />
                  <div style={{
                    marginTop: 8, fontSize: 12,
                    color: toolCallingModels[modelId].startsWith('进门') ? '#0369a1' : '#7c3aed',
                  }}>
                    正在调用 {toolCallingModels[modelId]}...
                  </div>
                </div>
              ) : (
                <div style={{ textAlign: 'center', padding: 20 }}>
                  <Spin />
                  <div style={{ marginTop: 8, color: '#94a3b8', fontSize: 12 }}>正在思考...</div>
                </div>
              )}
            </Card>
          )
        })}
      </div>
    )
  }

  // ── Render: Right drawer (tools, links, rankings) ──────────

  // ── Personal knowledge base drawer (VS Code-style drag source) ─

  const renderKbDocLeaf = (d: KbDocumentRef): any => ({
    key: `doc::${d.id}`,
    title: (
      <div
        {...makeKbDragHandlers(d)}
        style={{
          display: 'flex', alignItems: 'center', gap: 4,
          cursor: 'grab', padding: '2px 0', userSelect: 'none',
        }}
        title="拖拽到输入框以引用，或点击「引用」按钮"
      >
        <FileTextOutlined style={{ color: '#64748b' }} />
        <Text ellipsis style={{ maxWidth: 170, flex: 1 }}>
          {d.title || d.filename}
        </Text>
        <Button
          size="small" type="link" style={{ padding: 0, height: 'auto', cursor: 'pointer' }}
          // stop mousedown so antd Tree's selection / drag bookkeeping
          // doesn't eat the click; stopPropagation on click is still needed
          // to keep the Tree from treating it as a node toggle.
          onMouseDown={(e) => e.stopPropagation()}
          onClick={(e) => { e.stopPropagation(); attachKbDoc(d) }}
        >
          引用
        </Button>
      </div>
    ),
    isLeaf: true,
    selectable: false,
  })

  const renderKbFolderItem = (node: KbFolderNode, scope: 'personal' | 'public'): any => {
    const cacheKey = `${scope}::${node.id}`
    const docs = kbFolderDocs[cacheKey] || []
    const loading = kbFolderDocsLoading[cacheKey]
    const childFolders = (node.children || []).map((c) => renderKbFolderItem(c, scope))
    let docChildren: any[]
    if (loading) {
      docChildren = [{
        key: `${node.id}::loading`,
        title: <Text type="secondary"><Spin size="small" /> 加载中…</Text>,
        isLeaf: true,
        selectable: false,
      }]
    } else if (docs.length === 0 && childFolders.length === 0) {
      docChildren = [{
        key: `${node.id}::empty`,
        title: (
          <Text type="secondary" style={{ fontSize: 12, fontStyle: 'italic' }}>
            （此目录暂无文件）
          </Text>
        ),
        isLeaf: true,
        selectable: false,
      }]
    } else {
      docChildren = docs.map(renderKbDocLeaf)
    }
    return {
      key: node.id,
      title: (
        <Space size={4} style={{ width: '100%' }}>
          {node.folder_type === 'stock'
            ? <LineChartOutlined style={{ color: '#f59e0b' }} />
            : node.folder_type === 'industry'
              ? <TagOutlined style={{ color: '#0ea5e9' }} />
              : <FolderOutlined style={{ color: '#2563eb' }} />}
          <Text ellipsis style={{ maxWidth: 160 }} title={node.name}>
            {node.name}
          </Text>
          {node.stock_ticker && (
            <Tag color="orange" style={{ margin: 0, fontSize: 10 }}>
              {node.stock_ticker}
            </Tag>
          )}
          {docs.length > 0 && (
            <Badge count={docs.length}
              style={{ backgroundColor: '#94a3b8' }} size="small" />
          )}
        </Space>
      ),
      children: [...childFolders, ...docChildren],
    }
  }

  const renderKbWorkspace = () => {
    const tree = kbPanelScope === 'public' ? kbTreePublic : kbTreePersonal
    const topNodes = tree.map((n) => renderKbFolderItem(n, kbPanelScope))
    const unfiledKey = `${kbPanelScope}::`
    const unfiledDocs = kbFolderDocs[unfiledKey] || []
    const unfiledLoading = kbFolderDocsLoading[`${kbPanelScope}::ALL`]
    const unfiled = {
      key: `__unfiled__::${kbPanelScope}`,
      title: (
        <Space>
          <FolderOpenOutlined style={{ color: '#94a3b8' }} />
          未归档
          {unfiledDocs.length > 0 && (
            <Badge count={unfiledDocs.length}
              style={{ backgroundColor: '#94a3b8' }} size="small" />
          )}
        </Space>
      ),
      children: unfiledDocs.length > 0
        ? unfiledDocs.map(renderKbDocLeaf)
        : [{
            key: `${kbPanelScope}::unfiled-empty`,
            title: (
              <Text type="secondary" style={{ fontSize: 12, fontStyle: 'italic' }}>
                （无未归档文件）
              </Text>
            ),
            isLeaf: true,
            selectable: false,
          }],
    }
    const hasAnyDocs = Object.keys(kbFolderDocs).some(
      (k) => k.startsWith(`${kbPanelScope}::`) && (kbFolderDocs[k] || []).length > 0,
    )
    return (
      <Drawer
        title={
          <Space>
            <DatabaseOutlined />
            <span>个人知识库工作区</span>
            {kbRefs.length > 0 && (
              <Badge count={kbRefs.length} style={{ backgroundColor: '#db2777' }} />
            )}
          </Space>
        }
        open={kbPanelOpen}
        onClose={() => setKbPanelOpen(false)}
        width={420}
        extra={
          <Button
            size="small"
            icon={<ReloadOutlined />}
            onClick={() => {
              fetchKbTreeFor(kbPanelScope)
              fetchKbFolderDocs(kbPanelScope)
            }}
            loading={kbTreeLoading || !!unfiledLoading}
          >
            刷新
          </Button>
        }
      >
        <Space direction="vertical" size={8} style={{ width: '100%' }}>
          <Text type="secondary" style={{ fontSize: 12 }}>
            将文件拖到左侧输入框引用；或点击「引用」按钮加入对话。每次最多引用 8 份文档。
          </Text>
          <Tabs
            activeKey={kbPanelScope}
            onChange={(k) => setKbPanelScope(k as 'personal' | 'public')}
            items={[
              { key: 'personal', label: '📁 个人' },
              { key: 'public', label: '🌐 公共' },
            ]}
            size="small"
          />
          {kbDocsError && (
            <div style={{
              padding: '6px 10px', background: '#fef2f2', border: '1px solid #fecaca',
              borderRadius: 4, fontSize: 12, color: '#b91c1c',
            }}>
              加载文件列表失败：{kbDocsError}
              <Button
                type="link" size="small"
                onClick={() => fetchKbFolderDocs(kbPanelScope)}
                style={{ padding: '0 4px', height: 'auto' }}
              >
                重试
              </Button>
            </div>
          )}
          {kbTreeLoading || unfiledLoading ? (
            <div style={{ textAlign: 'center', padding: 24 }}>
              <Spin />
            </div>
          ) : (tree.length === 0 && !hasAnyDocs) ? (
            <Empty
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description={
                <span>
                  此工作区为空。前往
                  <a href="/my-knowledge" target="_blank" rel="noreferrer"> 个人知识库 </a>
                  上传文件与新建目录。
                </span>
              }
            />
          ) : (
            <Tree
              treeData={[...topNodes, unfiled]}
              defaultExpandAll
              blockNode
              showLine
              selectable={false}
            />
          )}
          {kbRefs.length > 0 && (
            <Card size="small" title={<Text strong>当前引用 ({kbRefs.length})</Text>}>
              <Space direction="vertical" size={4} style={{ width: '100%' }}>
                {kbRefs.map((r) => (
                  <Space key={r.id} size={4}>
                    <FileTextOutlined style={{ color: '#64748b' }} />
                    <Text ellipsis style={{ maxWidth: 240 }}>
                      {r.title || r.filename}
                    </Text>
                    <Button
                      type="link" size="small" danger
                      onClick={() => removeKbDoc(r.id)}
                    >移除</Button>
                  </Space>
                ))}
                <Button size="small" onClick={() => setKbRefs([])}>全部清除</Button>
              </Space>
            </Card>
          )}
        </Space>
      </Drawer>
    )
  }

  const renderRightDrawer = () => (
    <Drawer
      title="研究工具箱"
      open={rightDrawerOpen}
      onClose={() => setRightDrawerOpen(false)}
      width={360}
    >
      <Collapse
        defaultActiveKey={['tracking', 'links', 'rankings', 'prompts']}
        ghost
        items={[
          {
            key: 'tracking',
            label: (
              <Space>
                <Text strong><SearchOutlined /> 跟踪提醒</Text>
                {trackingTopics.reduce((sum, t) => sum + t.unread_count, 0) > 0 && (
                  <Badge count={trackingTopics.reduce((sum, t) => sum + t.unread_count, 0)} size="small" />
                )}
              </Space>
            ),
            children: (
              <div>
                <Button
                  type="dashed" icon={<PlusOutlined />} size="small" block
                  onClick={() => setTrackingModalOpen(true)}
                  style={{ marginBottom: 8 }}
                >
                  新建跟踪
                </Button>
                {trackingTopics.length === 0 ? (
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    添加投资关注主题，AI将自动监控相关新闻并提醒
                  </Text>
                ) : (
                  trackingTopics.map((t) => (
                    <div
                      key={t.id}
                      style={{
                        padding: '8px 10px', borderRadius: 6, marginBottom: 6,
                        border: '1px solid #e2e8f0', background: t.is_active ? '#fff' : '#f8fafc',
                      }}
                    >
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <Text style={{ fontSize: 13, flex: 1 }}>{t.topic}</Text>
                        <Space size={4}>
                          {t.unread_count > 0 && <Badge count={t.unread_count} size="small" />}
                          <Button
                            type="text" size="small"
                            onClick={async () => {
                              try {
                                await api.patch(`/chat/tracking/${t.id}`, { is_active: !t.is_active })
                                fetchTrackingTopics()
                              } catch { message.error('更新失败') }
                            }}
                            style={{ color: t.is_active ? '#22c55e' : '#94a3b8', fontSize: 11 }}
                          >
                            {t.is_active ? '监控中' : '已暂停'}
                          </Button>
                          <Popconfirm
                            title="删除此跟踪？"
                            onConfirm={async () => {
                              try {
                                await api.delete(`/chat/tracking/${t.id}`)
                                fetchTrackingTopics()
                              } catch { message.error('删除失败') }
                            }}
                          >
                            <Button type="text" size="small" danger icon={<DeleteOutlined style={{ fontSize: 11 }} />} />
                          </Popconfirm>
                        </Space>
                      </div>
                      {t.keywords.length > 0 && (
                        <div style={{ marginTop: 4 }}>
                          {t.keywords.map((k) => (
                            <Tag key={k} style={{ fontSize: 10, marginBottom: 2 }}>{k}</Tag>
                          ))}
                          {t.related_tickers.map((tk) => (
                            <Tag key={tk} color="blue" style={{ fontSize: 10, marginBottom: 2 }}>{tk}</Tag>
                          ))}
                        </div>
                      )}
                    </div>
                  ))
                )}
              </div>
            ),
          },
          {
            key: 'links',
            label: <Text strong><GlobalOutlined /> 常用网站</Text>,
            children: (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                {USEFUL_LINKS.map((link) => (
                  <Card
                    key={link.url}
                    size="small"
                    hoverable
                    onClick={() => window.open(link.url, '_blank')}
                    style={{ cursor: 'pointer' }}
                    styles={{ body: { padding: '8px 12px' } }}
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <div>
                        <Text strong style={{ fontSize: 13 }}>{link.name}</Text>
                        <br />
                        <Text type="secondary" style={{ fontSize: 11 }}>{link.desc}</Text>
                      </div>
                      <LinkOutlined style={{ color: '#94a3b8' }} />
                    </div>
                  </Card>
                ))}
              </div>
            ),
          },
          {
            key: 'rankings',
            label: <Text strong><TrophyOutlined /> 模型排行榜</Text>,
            children: rankings.length === 0 ? (
              <Text type="secondary" style={{ fontSize: 12 }}>暂无评分数据，使用后为模型评分即可生成排行</Text>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                {rankings.map((r, i) => (
                  <div
                    key={r.model_id}
                    style={{
                      display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                      padding: '6px 10px', borderRadius: 6,
                      background: i === 0 ? '#fef3c7' : i === 1 ? '#f1f5f9' : 'transparent',
                    }}
                  >
                    <Space size={6}>
                      <Text style={{ fontSize: 13, fontWeight: 600, width: 16 }}>
                        {i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : `${i + 1}`}
                      </Text>
                      <Text style={{ fontSize: 13 }}>{r.model_name}</Text>
                    </Space>
                    <Space size={4}>
                      <Rate disabled value={Math.round(r.avg_rating)} style={{ fontSize: 10 }} />
                      <Text type="secondary" style={{ fontSize: 11 }}>
                        {r.avg_rating.toFixed(1)} ({r.total_ratings}次)
                      </Text>
                    </Space>
                  </div>
                ))}
              </div>
            ),
          },
          {
            key: 'prompts',
            label: <Text strong><BookOutlined /> 我的提示词</Text>,
            children: (
              <div>
                <Button
                  type="dashed"
                  icon={<PlusOutlined />}
                  size="small"
                  block
                  onClick={() => setTemplateModalOpen(true)}
                  style={{ marginBottom: 8 }}
                >
                  新建提示词
                </Button>
                {templates.map((tpl) => (
                  <div
                    key={tpl.id}
                    style={{
                      display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                      padding: '6px 8px', borderRadius: 6, marginBottom: 4,
                      border: '1px solid #e2e8f0',
                    }}
                  >
                    <div
                      style={{ cursor: 'pointer', flex: 1 }}
                      onClick={() => applyTemplate(tpl)}
                    >
                      <Text style={{ fontSize: 13 }}>{tpl.name}</Text>
                      <br />
                      <Text type="secondary" style={{ fontSize: 11 }}>
                        {CATEGORY_LABELS[tpl.category] || tpl.category}
                      </Text>
                    </div>
                    <Popconfirm title="删除此提示词？" onConfirm={() => handleDeleteTemplate(tpl.id)}>
                      <Button type="text" size="small" danger icon={<DeleteOutlined style={{ fontSize: 11 }} />} />
                    </Popconfirm>
                  </div>
                ))}
              </div>
            ),
          },
        ]}
      />
    </Drawer>
  )

  // ── Render: Main ───────────────────────────────────────────

  return (
    <Layout style={{ height: 'calc(100vh - 64px)', background: '#fff' }}>
      {/* Left sidebar: conversations */}
      {!sidebarCollapsed && (
        <Sider
          width={280}
          style={{
            background: '#fff',
            borderRight: '1px solid #e2e8f0',
            height: '100%',
            overflow: 'hidden',
          }}
        >
          {renderSidebar()}
        </Sider>
      )}

      {/* Main chat area */}
      <Content style={{ display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden' }}>
        {/* Header */}
        <div
          style={{
            display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            padding: '8px 16px', borderBottom: '1px solid #f1f5f9',
            background: '#fafbfd',
          }}
        >
          <Space size={8}>
            <Button
              type="text"
              icon={sidebarCollapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
              onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
            />
            <Title level={5} style={{ margin: 0, fontSize: 15 }}>
              <RobotOutlined style={{ marginRight: 6, color: '#2563eb' }} />
              AI 研究助手
            </Title>
            {activePromptName && (
              <Tag
                color="blue"
                closable
                onClose={() => { setSystemPrompt(null); setActivePromptName('') }}
                style={{ fontSize: 11 }}
              >
                {activePromptName}
              </Tag>
            )}
            {chatMode !== 'standard' && (
              <Tag
                color={chatMode === 'thinking' ? 'purple' : 'orange'}
                style={{ fontSize: 11 }}
              >
                {chatMode === 'thinking' ? '🧠 深度思考' : '⚡ 快速模式'}
              </Tag>
            )}
          </Space>
          <Space size={4}>
            {activeConvId && (
              <>
                <Tooltip title="总结对话">
                  <Button type="text" size="small" icon={<CompressOutlined />} onClick={handleSummarize} />
                </Tooltip>
                <Tooltip title="导出Markdown">
                  <Button type="text" size="small" icon={<DownloadOutlined />} onClick={handleExport} />
                </Tooltip>
              </>
            )}
            <Tooltip title={memoriesActiveCount > 0 ? `我的记忆（${memoriesActiveCount} 条生效中）` : '我的记忆'}>
              <Badge count={memoryIdsThisTurn.length} size="small" offset={[-2, 2]} color="#52c41a">
                <Button
                  type="text"
                  size="small"
                  icon={<ExperimentOutlined style={memoryIdsThisTurn.length > 0 ? { color: '#52c41a' } : undefined} />}
                  onClick={() => setMemoriesDrawerOpen(true)}
                />
              </Badge>
            </Tooltip>
            <Tooltip title="研究工具箱">
              <Button
                type="text"
                size="small"
                icon={<BulbOutlined />}
                onClick={() => setRightDrawerOpen(true)}
              />
            </Tooltip>
          </Space>
        </div>

        {/* Messages area */}
        <div ref={messagesContainerRef} onScroll={handleMessagesScroll} style={{ flex: 1, overflow: 'auto', padding: '16px 20px' }}>
          {/* Expanded model banner */}
          {expandedModel && (
            <div style={{
              display: 'flex', alignItems: 'center', justifyContent: 'space-between',
              padding: '8px 16px', marginBottom: 12,
              background: '#eff6ff', borderRadius: 8, border: '1px solid #bfdbfe',
              position: 'sticky', top: 0, zIndex: 10,
            }}>
              <Space>
                <ExpandAltOutlined style={{ color: '#2563eb' }} />
                <Text strong style={{ fontSize: 13 }}>
                  单模型视图: {models.find(m => m.id === expandedModel)?.name || expandedModel}
                </Text>
              </Space>
              <Button size="small" onClick={() => setExpandedModel(null)} icon={<CompressOutlined />}>
                返回多模型视图
              </Button>
            </div>
          )}
          {/* Closed models indicator */}
          {closedModels.length > 0 && (
            <div style={{
              display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap',
              padding: '6px 12px', marginBottom: 8,
              background: '#fef2f2', borderRadius: 6, border: '1px solid #fecaca',
            }}>
              <Text type="secondary" style={{ fontSize: 12 }}>已关闭:</Text>
              {closedModels.map(modelId => (
                <Tag
                  key={modelId}
                  closable
                  onClose={() => handleReopenModel(modelId)}
                  style={{ fontSize: 11 }}
                >
                  {models.find(m => m.id === modelId)?.name || modelId}
                </Tag>
              ))}
            </div>
          )}
          {!activeConvId && messages.length === 0 ? (
            // Welcome screen
            <div style={{ maxWidth: 600, margin: '60px auto', textAlign: 'center' }}>
              <RobotOutlined style={{ fontSize: 48, color: '#2563eb', marginBottom: 16 }} />
              <Title level={3} style={{ marginBottom: 8 }}>AI 研究助手</Title>
              <Paragraph type="secondary" style={{ marginBottom: 24 }}>
                支持多模型对比回答，帮助您更高效地进行股票研究和市场分析。
                选择多个模型获得不同视角的分析结果。
              </Paragraph>

              {/* Quick start prompts */}
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, justifyContent: 'center', marginBottom: 24 }}>
                {SYSTEM_PROMPTS.map((tpl) => (
                  <Tag
                    key={tpl.id}
                    style={{ cursor: 'pointer', padding: '4px 10px', fontSize: 13, borderRadius: 16 }}
                    onClick={() => applyTemplate(tpl)}
                  >
                    {tpl.name}
                  </Tag>
                ))}
              </div>

              <Divider />

              {/* Quick question suggestions — personalized, refreshed daily */}
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <Text type="secondary" style={{ fontSize: 13 }}>
                  为你推荐{recommendedLoading ? '中...' : '：'}
                </Text>
                <Tooltip title="基于你的历史聊天和自选股，由AI每天自动生成；点此立即重新生成">
                  <Button
                    type="text"
                    size="small"
                    icon={<ReloadOutlined spin={recommendedRefreshing} />}
                    onClick={refreshRecommendedQuestions}
                    disabled={recommendedRefreshing || recommendedLoading}
                    style={{ fontSize: 12, color: '#64748b' }}
                  >
                    换一批
                  </Button>
                </Tooltip>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8, marginTop: 8, textAlign: 'left' }}>
                {recommendedLoading && recommendedQuestions.length === 0 ? (
                  <div style={{ textAlign: 'center', padding: 16 }}>
                    <Spin size="small" />
                  </div>
                ) : (
                  recommendedQuestions.map((q) => (
                    <Card
                      key={q}
                      size="small"
                      hoverable
                      onClick={() => setInputText(q)}
                      style={{ cursor: 'pointer', borderRadius: 8 }}
                      styles={{ body: { padding: '8px 12px' } }}
                    >
                      <Text style={{ fontSize: 13 }}>
                        <ThunderboltOutlined style={{ color: '#f59e0b', marginRight: 6 }} />
                        {q}
                      </Text>
                    </Card>
                  ))
                )}
              </div>
            </div>
          ) : (
            <>
              {messages.map(renderMessage)}
              {!debateMode && renderStreaming()}
              {(debateStreaming || debateRounds.length > 0) && renderDebateStreaming()}
              <div ref={chatEndRef} />
            </>
          )}
        </div>

        {/* Input area */}
        <div
          style={{
            padding: '12px 16px',
            borderTop: '1px solid #e2e8f0',
            background: '#fafbfd',
          }}
        >
          {/* Send target indicator */}
          {sendTarget !== 'all' && (
            <div style={{
              display: 'flex', alignItems: 'center', justifyContent: 'space-between',
              padding: '6px 12px', marginBottom: 8,
              background: '#eff6ff', borderRadius: 6, border: '1px solid #bfdbfe',
            }}>
              <Space>
                <MessageOutlined style={{ color: '#2563eb' }} />
                <Text style={{ fontSize: 12 }}>
                  单独回复: <Text strong>{models.find(m => m.id === sendTarget)?.name || sendTarget}</Text>
                </Text>
              </Space>
              <Button type="text" size="small" onClick={() => setSendTarget('all')} icon={<CloseOutlined />}>
                取消
              </Button>
            </div>
          )}

          {/* Attachments preview */}
          {attachments.length > 0 && (
            <div style={{ display: 'flex', gap: 6, marginBottom: 8, flexWrap: 'wrap' }}>
              {attachments.map((att, i) => (
                <Tag
                  key={i}
                  closable
                  onClose={() => setAttachments((prev) => prev.filter((_, j) => j !== i))}
                  icon={att.file_type.startsWith('image/') ? <FileImageOutlined /> : <FilePdfOutlined />}
                >
                  {att.filename}
                </Tag>
              ))}
            </div>
          )}

          {/* Knowledge-base reference chips (dragged from the workspace panel) */}
          {kbRefs.length > 0 && (
            <div
              style={{
                display: 'flex', gap: 6, marginBottom: 8, flexWrap: 'wrap',
                padding: '6px 10px', background: '#fdf2f8', border: '1px solid #fbcfe8',
                borderRadius: 6,
              }}
            >
              <Text type="secondary" style={{ fontSize: 12 }}>
                📚 引用知识库文档 ({kbRefs.length}):
              </Text>
              {kbRefs.map((r) => (
                <Tag
                  key={r.id}
                  closable
                  onClose={() => removeKbDoc(r.id)}
                  color={r.scope === 'public' ? 'purple' : 'magenta'}
                  icon={<FileTextOutlined />}
                >
                  {r.title || r.filename}
                </Tag>
              ))}
              <Button type="link" size="small" onClick={() => setKbRefs([])}>
                全部清除
              </Button>
            </div>
          )}

          {/* Model selector row */}
          <div style={{ display: 'flex', gap: 8, marginBottom: 8, alignItems: 'center', flexWrap: 'wrap' }}>
            <Text type="secondary" style={{ fontSize: 12, whiteSpace: 'nowrap' }}>模型:</Text>
            <Select
              mode="multiple"
              value={selectedModels}
              onChange={setSelectedModels}
              style={{ flex: 1, minWidth: 200 }}
              placeholder="选择AI模型（可多选对比）"
              maxTagCount={4}
              size="small"
              options={models.map((m) => ({
                value: m.id,
                label: (
                  <Space size={4}>
                    <span>{m.name}</span>
                    <Text type="secondary" style={{ fontSize: 11 }}>{m.provider}</Text>
                    {m.supports_vision && <Tag color="green" style={{ fontSize: 10, lineHeight: '14px', padding: '0 3px' }}>视觉</Tag>}
                    {m.supports_thinking && <Tag color="purple" style={{ fontSize: 10, lineHeight: '14px', padding: '0 3px' }}>思考</Tag>}
                  </Space>
                ),
              }))}
            />

            {/* Mode selector */}
            <div style={{ display: 'flex', background: '#f1f5f9', borderRadius: 6, padding: 2 }}>
              {([
                { key: 'fast', icon: <ThunderboltOutlined />, label: '快速', color: '#f59e0b' },
                { key: 'standard', icon: <RobotOutlined />, label: '标准', color: '#2563eb' },
                { key: 'thinking', icon: <BulbOutlined />, label: '思考', color: '#7c3aed' },
              ] as const).map((m) => (
                <Tooltip key={m.key} title={
                  m.key === 'fast' ? '快速响应，适合简单查询' :
                  m.key === 'standard' ? '平衡速度与质量' :
                  '深度推理，适合复杂分析'
                }>
                  <Button
                    type={chatMode === m.key ? 'primary' : 'text'}
                    size="small"
                    icon={m.icon}
                    onClick={() => setChatMode(m.key)}
                    style={{
                      borderRadius: 4,
                      fontSize: 12,
                      height: 26,
                      ...(chatMode === m.key ? { backgroundColor: m.color, borderColor: m.color } : {}),
                    }}
                  >
                    {m.label}
                  </Button>
                </Tooltip>
              ))}
            </div>

            {/* 内部知识库 toggle — 7 crawled platforms via kb_search/kb_fetch_document/kb_list_facets */}
            <Tooltip title="内部知识库：统一检索全部7个投研来源（Alpha派+进门+久谦+第三方桥+Funda+港推+峰会），支持按股票代码+日期+类型过滤">
              <Button
                type={kbEnabled ? 'primary' : 'text'}
                size="small"
                icon={<DatabaseOutlined />}
                onClick={() => {
                  const next = !kbEnabled
                  setKbEnabled(next)
                  localStorage.setItem('kb_enabled', String(next))
                }}
                style={{
                  borderRadius: 4, fontSize: 12, height: 26,
                  ...(kbEnabled ? { backgroundColor: '#047857', borderColor: '#047857' } : {}),
                }}
              >
                知识库
              </Button>
            </Tooltip>

            {/* 个人知识库 toggle — user-uploaded files via user_kb_search / user_kb_fetch_document */}
            <Tooltip title="个人知识库：检索你自己上传的 PDF/文本/Markdown 等文件（前往「个人知识库」页面管理）">
              <Button
                type={userKbEnabled ? 'primary' : 'text'}
                size="small"
                icon={<BookOutlined />}
                onClick={() => {
                  const next = !userKbEnabled
                  setUserKbEnabled(next)
                  localStorage.setItem('user_kb_enabled', String(next))
                }}
                style={{
                  borderRadius: 4, fontSize: 12, height: 26,
                  ...(userKbEnabled ? { backgroundColor: '#7c3aed', borderColor: '#7c3aed' } : {}),
                }}
              >
                个人库
              </Button>
            </Tooltip>

            {/* 个人知识库工作区 — opens the drag-drop side panel */}
            <Tooltip title="打开个人知识库工作区，拖拽文件到输入框引用">
              <Button
                type={kbRefs.length > 0 ? 'primary' : 'text'}
                size="small"
                icon={<DatabaseOutlined />}
                onClick={() => setKbPanelOpen(true)}
                style={{
                  borderRadius: 4, fontSize: 12, height: 26,
                  ...(kbRefs.length > 0
                    ? { backgroundColor: '#db2777', borderColor: '#db2777' }
                    : {}),
                }}
              >
                {kbRefs.length > 0 ? `工作区 (${kbRefs.length})` : '工作区'}
              </Button>
            </Tooltip>

            {/* Web search tri-state toggle */}
            <Tooltip title={
              webSearchMode === 'off' ? '搜索已关闭：点击切换到自动模式' :
              webSearchMode === 'auto' ? '自动搜索：AI自动判断是否需要联网（点击切换到强制搜索）' :
              '强制联网：每次提问都会搜索最新信息（点击关闭）'
            }>
              <Button
                type={webSearchMode === 'off' ? 'text' : 'primary'}
                size="small"
                icon={<SearchOutlined />}
                onClick={() => {
                  const next = webSearchMode === 'off' ? 'auto' : webSearchMode === 'auto' ? 'on' : 'off'
                  setWebSearchMode(next)
                  localStorage.setItem('web_search_mode', next)
                }}
                style={{
                  borderRadius: 4, fontSize: 12, height: 26,
                  ...(webSearchMode === 'auto' ? { backgroundColor: '#2563eb', borderColor: '#2563eb' } : {}),
                  ...(webSearchMode === 'on' ? { backgroundColor: '#059669', borderColor: '#059669' } : {}),
                }}
              >
                {webSearchMode === 'off' ? '搜索关' : webSearchMode === 'auto' ? '自动搜索' : '联网'}
              </Button>
            </Tooltip>

            {/* Debate mode toggle + format selector */}
            <Tooltip title="辩论模式：多模型多角度投资分析辩论">
              <Button
                type={debateMode ? 'primary' : 'text'}
                size="small"
                icon={<TrophyOutlined />}
                onClick={() => setDebateMode(!debateMode)}
                style={{
                  borderRadius: 4, fontSize: 12, height: 26,
                  ...(debateMode ? { backgroundColor: '#dc2626', borderColor: '#dc2626' } : {}),
                }}
              >
                辩论
              </Button>
            </Tooltip>
            {debateMode && (
              <Select
                size="small"
                value={debateFormat}
                onChange={(v) => { setDebateFormat(v); localStorage.setItem('chat_debate_format', v) }}
                style={{ width: 130, fontSize: 12 }}
                options={[
                  { value: 'bull_bear', label: '多空辩论' },
                  { value: 'multi_perspective', label: '多维分析' },
                  { value: 'round_robin', label: '轮流讨论' },
                ]}
              />
            )}

            <Dropdown
              trigger={['click']}
              menu={{
                items: [
                  { key: 'header', label: <Text strong style={{ fontSize: 12 }}>研究助手模式</Text>, disabled: true },
                  { type: 'divider' },
                  ...allTemplates.map((tpl) => ({
                    key: tpl.id,
                    label: `${tpl.name}`,
                    onClick: () => applyTemplate(tpl),
                  })),
                  { type: 'divider' },
                  { key: 'clear', label: '🚫 清除提示词', onClick: () => { setSystemPrompt(null); setActivePromptName('') } },
                  { key: 'custom', label: '✏️ 自定义提示词', onClick: () => setTemplateModalOpen(true) },
                ],
              }}
            >
              <Button size="small" icon={<BookOutlined />}>
                提示词
              </Button>
            </Dropdown>
          </div>

          {/* Text input + send */}
          <div style={{ display: 'flex', gap: 8, alignItems: 'flex-end' }}>
            <Upload
              showUploadList={false}
              beforeUpload={handleUpload}
              accept="image/*,.pdf"
            >
              <Tooltip title="上传图片或PDF">
                <Button icon={<PaperClipOutlined />} size="large" />
              </Tooltip>
            </Upload>
            <div
              style={{
                flex: 1,
                position: 'relative',
                borderRadius: 8,
                // Visual highlight when dragging a KB doc over the input.
                outline: kbDragOverInput ? '2px dashed #db2777' : 'none',
                outlineOffset: 2,
                transition: 'outline-color 120ms',
              }}
              {...inputDropHandlers}
            >
              <TextArea
                ref={inputRef}
                value={inputText}
                onChange={(e) => setInputText(e.target.value)}
                placeholder={
                  sending
                    ? '正在等待回复...'
                    : kbDragging
                      ? '放开以引用此文档…'
                      : '输入问题，Shift+Enter换行，Enter发送；可拖拽「工作区」文件到这里引用'
                }
                autoSize={{ minRows: 3, maxRows: 12 }}
                style={{
                  width: '100%', borderRadius: 8, fontSize: 14,
                  padding: '10px 12px', lineHeight: 1.6,
                }}
                disabled={sending}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault()
                    handleSend()
                  }
                }}
              />
              {kbDragOverInput && (
                <div
                  style={{
                    position: 'absolute', inset: 0, display: 'flex',
                    alignItems: 'center', justifyContent: 'center',
                    pointerEvents: 'none',
                    background: 'rgba(219, 39, 119, 0.06)', borderRadius: 8,
                    color: '#db2777', fontWeight: 500,
                  }}
                >
                  将文档加入对话
                </div>
              )}
            </div>
            {sending ? (
              <Button
                danger
                size="large"
                icon={<StopOutlined />}
                onClick={handleCancel}
                style={{ borderRadius: 8 }}
              >
                停止生成
              </Button>
            ) : (
              <Button
                type="primary"
                size="large"
                icon={debateMode ? <TrophyOutlined /> : <SendOutlined />}
                onClick={handleSend}
                disabled={
                  !inputText.trim() &&
                  attachments.length === 0 &&
                  kbRefs.length === 0
                }
                danger={debateMode}
                style={{ borderRadius: 8 }}
              >
                {debateMode ? '开始辩论' : '发送'}
              </Button>
            )}
          </div>
        </div>
      </Content>

      {/* Right drawer */}
      {renderRightDrawer()}

      {/* Memories drawer */}
      <Drawer
        title={
          <Space size={8}>
            <ExperimentOutlined style={{ color: '#2563eb' }} />
            <span>我的记忆</span>
            <Badge count={memoriesActiveCount} color="blue" style={{ marginLeft: 4 }} />
            {memoryIdsThisTurn.length > 0 && (
              <Tag color="green" style={{ marginInlineEnd: 0 }}>
                本轮用了 {memoryIdsThisTurn.length} 条
              </Tag>
            )}
          </Space>
        }
        placement="right"
        open={memoriesDrawerOpen}
        onClose={() => setMemoriesDrawerOpen(false)}
        width={480}
        destroyOnHidden={false}
        extra={
          <Tooltip title="AI 会把这里的记忆自动注入每次对话的系统提示。来自反馈学习，也可手动添加/停用/置顶。">
            <BulbOutlined style={{ color: '#94a3b8' }} />
          </Tooltip>
        }
      >
        <MemoriesPanel
          ref={memoriesPanelRef}
          compact
          highlightedIds={memoryIdsThisTurn}
          onActiveCountChange={setMemoriesActiveCount}
        />
      </Drawer>

      {/* Personal knowledge base workspace (drag source) */}
      {renderKbWorkspace()}

      {/* Template creation modal */}
      <Modal
        title="新建自定义提示词"
        open={templateModalOpen}
        onOk={handleCreateTemplate}
        onCancel={() => setTemplateModalOpen(false)}
        okText="保存"
        cancelText="取消"
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <Input
            placeholder="提示词名称"
            value={newTplName}
            onChange={(e) => setNewTplName(e.target.value)}
          />
          <Select
            value={newTplCategory}
            onChange={setNewTplCategory}
            options={Object.entries(CATEGORY_LABELS).map(([k, v]) => ({ value: k, label: v }))}
            placeholder="选择分类"
          />
          <TextArea
            placeholder="输入提示词内容，例如：你是一位专业的xxx分析师，请从以下角度分析..."
            value={newTplContent}
            onChange={(e) => setNewTplContent(e.target.value)}
            autoSize={{ minRows: 4, maxRows: 10 }}
          />
        </div>
      </Modal>

      {/* Tracking topic creation modal */}
      <Modal
        title="新建跟踪提醒"
        open={trackingModalOpen}
        onOk={async () => {
          if (!newTrackingTopic.trim()) {
            message.warning('请输入关注主题')
            return
          }
          try {
            await api.post('/chat/tracking', {
              topic: newTrackingTopic,
              auto_extract: true,
              notify_channels: ['browser'],
            })
            setTrackingModalOpen(false)
            setNewTrackingTopic('')
            fetchTrackingTopics()
            message.success('跟踪已创建，AI将自动提取关键词并开始监控')
          } catch {
            message.error('创建跟踪失败')
          }
        }}
        onCancel={() => setTrackingModalOpen(false)}
        okText="创建"
        cancelText="取消"
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <Text type="secondary" style={{ fontSize: 12 }}>
            描述你关注的投资逻辑或主题，AI将自动提取关键词、相关股票和板块，并定期扫描新闻提醒你。
          </Text>
          <TextArea
            placeholder="例如：关注美联储降息节奏对港股科技股的影响"
            value={newTrackingTopic}
            onChange={(e) => setNewTrackingTopic(e.target.value)}
            autoSize={{ minRows: 3, maxRows: 6 }}
          />
        </div>
      </Modal>

      {/* Detailed feedback modal — lets users submit tags + qualitative text
          that the background memory processor distills into long-term user
          memories. See backend/app/services/chat_memory_extractor.py. */}
      <FeedbackModal
        open={feedbackTarget !== null}
        responseId={feedbackTarget?.responseId || null}
        modelName={feedbackTarget?.modelName}
        initialRating={feedbackTarget?.initialRating || null}
        initialText={feedbackTarget?.initialText || ''}
        onClose={() => setFeedbackTarget(null)}
        onSubmitted={(data) => {
          // Reflect the rating locally so the stars update without re-fetch
          if (!feedbackTarget) return
          const rid = feedbackTarget.responseId
          setMessages((prev) =>
            prev.map((m) => ({
              ...m,
              model_responses: m.model_responses.map((r) =>
                r.id === rid
                  ? { ...r, rating: data.rating ?? r.rating, rating_comment: data.text || r.rating_comment }
                  : r,
              ),
            })),
          )
          fetchRankings()
        }}
      />
    </Layout>
  )
}

