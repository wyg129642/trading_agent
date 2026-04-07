import { useState, useEffect, useRef, useCallback } from 'react'
import {
  Layout, Input, Button, List, Card, Tag, Rate, Space, Typography,
  Select, Upload, Tooltip, Modal, Dropdown, Empty, Spin, Badge,
  Popconfirm, message, Drawer, Collapse, Divider, Avatar, Grid,
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
} from '@ant-design/icons'
import api from '../services/api'
import { useAuthStore } from '../store/auth'
import MarkdownRenderer from '../components/MarkdownRenderer'

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

  // Input
  const [inputText, setInputText] = useState('')
  const [attachments, setAttachments] = useState<Attachment[]>([])
  const [systemPrompt, setSystemPrompt] = useState<string | null>(null)
  const [activePromptName, setActivePromptName] = useState<string>('')
  const [sending, setSending] = useState(false)
  const [chatMode, setChatMode] = useState<'standard' | 'thinking' | 'fast'>(() => {
    const saved = localStorage.getItem('chat_mode')
    if (saved === 'standard' || saved === 'thinking' || saved === 'fast') return saved
    return 'standard'
  })

  // Web search toggle
  const [webSearchEnabled, setWebSearchEnabled] = useState(false)

  // Streaming state: model_id -> accumulated content
  const [streamingContents, setStreamingContents] = useState<Record<string, string>>({})
  const [streamingModels, setStreamingModels] = useState<string[]>([])

  // Templates & rankings
  const [templates, setTemplates] = useState<PromptTemplate[]>([])
  const [rankings, setRankings] = useState<ModelRanking[]>([])

  // UI
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const [rightDrawerOpen, setRightDrawerOpen] = useState(false)
  const [templateModalOpen, setTemplateModalOpen] = useState(false)
  const [newTplName, setNewTplName] = useState('')
  const [newTplContent, setNewTplContent] = useState('')
  const [newTplCategory, setNewTplCategory] = useState('general')

  // Debate mode
  const [debateMode, setDebateMode] = useState(false)
  const [debateRounds, setDebateRounds] = useState<DebateRound[]>([])
  const [activeDebateRound, setActiveDebateRound] = useState(0)
  const [debateStreaming, setDebateStreaming] = useState(false)
  const [debateStreamContent, setDebateStreamContent] = useState('')

  // Tracking
  const [trackingTopics, setTrackingTopics] = useState<TrackingTopic[]>([])
  const [trackingModalOpen, setTrackingModalOpen] = useState(false)
  const [newTrackingTopic, setNewTrackingTopic] = useState('')

  // ── Persist preferences ─────────────────────────────────────

  useEffect(() => {
    if (selectedModels.length > 0) {
      localStorage.setItem('chat_selected_models', JSON.stringify(selectedModels))
    }
  }, [selectedModels])

  useEffect(() => {
    localStorage.setItem('chat_mode', chatMode)
  }, [chatMode])

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

  const fetchMessages = useCallback(async (convId: string) => {
    try {
      const res = await api.get(`/chat/conversations/${convId}`)
      setMessages(res.data.messages)
    } catch {
      message.error('加载对话失败')
    }
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

  useEffect(() => {
    fetchModels()
    fetchConversations()
    fetchTemplates()
    fetchRankings()
    fetchTrackingTopics()
  }, [])

  useEffect(() => {
    if (activeConvId) fetchMessages(activeConvId)
    else setMessages([])
  }, [activeConvId])

  // Auto-scroll to bottom
  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' })
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
    if (selectedModels.length < 2 || selectedModels.length > 3) {
      message.warning('辩论模式需要选择2-3个模型')
      return
    }
    if (sending) return

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

    setSending(true)
    setDebateStreaming(true)
    setDebateRounds([])
    setActiveDebateRound(0)
    setDebateStreamContent('')

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
      const token = useAuthStore.getState().token
      const response = await fetch(`/api/chat/conversations/${convId}/messages/debate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify({
          content: userText,
          debate_models: selectedModels,
          attachments: currentAttachments,
          system_prompt: systemPrompt,
        }),
      })

      if (!response.ok) throw new Error(`HTTP ${response.status}`)

      const reader = response.body?.getReader()
      const decoder = new TextDecoder()
      let buffer = ''
      let currentRoundContent = ''

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
              if (event.type === 'round_start') {
                setActiveDebateRound(event.round)
                setDebateStreamContent('')
                currentRoundContent = ''
              } else if (event.type === 'delta') {
                currentRoundContent += event.delta
                setDebateStreamContent(currentRoundContent)
              } else if (event.type === 'done') {
                if (event.error) {
                  message.error(`辩论模型出错: ${event.error}`)
                }
                setDebateRounds((prev) => [...prev, {
                  round: event.debate_round,
                  role: event.debate_round === 1 ? '看多方' : event.debate_round === 2 ? '质疑方' : '综合判断',
                  model: event.model,
                  model_name: event.model_name || event.model,
                  content: currentRoundContent,
                  response_id: event.response_id || '',
                  tokens: event.tokens || 0,
                  latency_ms: event.latency_ms || 0,
                  error: event.error || null,
                }])
                setDebateStreamContent('')
                currentRoundContent = ''
              } else if (event.type === 'all_done') {
                setActiveDebateRound(0)
              }
            } catch (parseErr) {
              console.warn('Debate SSE parse error:', parseErr)
            }
          }
        }
      }
    } catch (err) {
      message.error('辩论发送失败，请重试')
      console.error('Debate error:', err)
    } finally {
      setSending(false)
      setDebateStreaming(false)
      setActiveDebateRound(0)
      setDebateStreamContent('')
      if (convId) {
        fetchMessages(convId)
        fetchConversations()
      }
    }
  }

  // ── Normal send handler ─────────────────────────────────────

  const handleSend = async () => {
    if (debateMode) return handleDebateSend()

    if (!inputText.trim() && attachments.length === 0) return
    if (selectedModels.length === 0) {
      message.warning('请至少选择一个模型')
      return
    }
    if (sending) return

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

    setSending(true)
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
    setStreamingModels(selectedModels)
    setStreamingContents({})

    const currentAttachments = attachments.map((a) => ({
      filename: a.filename,
      file_type: a.file_type,
      file_url: a.file_url,
      file_path: a.file_path,  // server-side absolute path from upload response
    }))
    setAttachments([])

    try {
      // Use SSE streaming
      const token = useAuthStore.getState().token
      const response = await fetch(`/api/chat/conversations/${convId}/messages/stream`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`,
        },
        body: JSON.stringify({
          content: userText,
          models: selectedModels,
          attachments: currentAttachments,
          system_prompt: systemPrompt,
          mode: chatMode,
          web_search: webSearchEnabled,
        }),
      })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`)
      }

      const reader = response.body?.getReader()
      const decoder = new TextDecoder()
      let buffer = ''

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

              if (event.type === 'delta') {
                setStreamingContents((prev) => ({
                  ...prev,
                  [event.model]: (prev[event.model] || '') + event.delta,
                }))
              } else if (event.type === 'done') {
                if (event.error) {
                  message.error(`模型 ${event.model_name || event.model} 出错: ${event.error}`)
                }
                setStreamingModels((prev) => prev.filter((m) => m !== event.model))
              } else if (event.type === 'error') {
                message.error(`流式响应出错: ${event.error || '未知错误'}`)
              }
            } catch (parseErr) {
              console.warn('SSE JSON parse error:', parseErr, 'raw data:', data)
            }
          }
        }
      }
    } catch (err) {
      message.error('发送失败，请重试')
      console.error('Send error:', err)
    } finally {
      setSending(false)
      setStreamingModels([])
      setStreamingContents({})
      // Refresh messages and conversation list
      if (convId) {
        fetchMessages(convId)
        fetchConversations()
      }
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
          filteredConvs.map((conv) => (
            <div
              key={conv.id}
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
              <Text type="secondary" style={{ fontSize: 11 }} ellipsis>
                {conv.last_message_preview || '空对话'}
              </Text>
            </div>
          ))
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

  const DEBATE_COLORS: Record<number, { bg: string; border: string; badge: string; label: string }> = {
    1: { bg: '#eff6ff', border: '#3b82f6', badge: '#2563eb', label: '看多方' },
    2: { bg: '#fef2f2', border: '#ef4444', badge: '#dc2626', label: '质疑方' },
    3: { bg: '#fffbeb', border: '#f59e0b', badge: '#d97706', label: '综合判断' },
  }

  const renderDebateResponses = (responses: ModelResponseData[]) => {
    const debateResps = [...responses].sort((a, b) => (a.debate_round || 0) - (b.debate_round || 0))
    return (
      <div style={{ marginLeft: 42, display: 'flex', flexDirection: 'column', gap: 12 }}>
        {debateResps.map((resp) => {
          const round = resp.debate_round || 1
          const style = DEBATE_COLORS[round] || DEBATE_COLORS[1]
          return (
            <Card
              key={resp.id}
              size="small"
              style={{ borderLeft: `3px solid ${style.border}`, background: style.bg }}
              title={
                <Space size={8}>
                  <Tag color={style.badge} style={{ fontWeight: 600 }}>Round {round} - {style.label}</Tag>
                  <Text strong style={{ fontSize: 13 }}>{resp.model_name}</Text>
                  {resp.latency_ms && (
                    <Tag style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>
                      {(resp.latency_ms / 1000).toFixed(1)}s
                    </Tag>
                  )}
                  {resp.tokens_used && (
                    <Tag style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>
                      {resp.tokens_used} tokens
                    </Tag>
                  )}
                </Space>
              }
              extra={
                <Tooltip title="复制">
                  <Button
                    type="text" size="small" icon={<CopyOutlined />}
                    onClick={() => { navigator.clipboard.writeText(resp.content); message.success('已复制') }}
                  />
                </Tooltip>
              }
            >
              {resp.error ? (
                <Text type="danger" style={{ fontSize: 13 }}>&#9888;&#65039; {resp.error}</Text>
              ) : (
                <div style={{ fontSize: 14, lineHeight: 1.8, wordBreak: 'break-word' }}>
                  <MarkdownRenderer content={resp.content} />
                </div>
              )}
              <div style={{ marginTop: 8 }}>
                <Rate
                  value={resp.rating || 0}
                  onChange={(val) => handleRate(resp.id, val)}
                  style={{ fontSize: 14 }}
                />
              </div>
            </Card>
          )
        })}
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
      {!msg.is_debate && msg.model_responses.length > 0 && (
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: msg.model_responses.length === 1
              ? '1fr'
              : msg.model_responses.length === 2
                ? '1fr 1fr'
                : `repeat(${Math.min(msg.model_responses.length, 3)}, 1fr)`,
            gap: 12,
            marginLeft: 42,
          }}
        >
          {msg.model_responses.map((resp) => (
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
                  {resp.tokens_used && (
                    <Tag style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>
                      {resp.tokens_used} tokens
                    </Tag>
                  )}
                </Space>
              }
              extra={
                <Tooltip title="复制回答">
                  <Button
                    type="text"
                    size="small"
                    icon={<CopyOutlined />}
                    onClick={() => copyToClipboard(resp.content)}
                  />
                </Tooltip>
              }
              style={{
                borderColor: resp.error ? '#fecaca' : '#e2e8f0',
                borderRadius: 10,
              }}
              styles={{ body: { padding: '10px 14px', maxHeight: 500, overflow: 'auto' } }}
            >
              {resp.error ? (
                <Text type="danger" style={{ fontSize: 13 }}>⚠️ {resp.error}</Text>
              ) : (
                <div style={{ fontSize: 14, lineHeight: 1.8, wordBreak: 'break-word' }}>
                  <MarkdownRenderer content={resp.content} />
                </div>
              )}
              {/* Rating */}
              <Divider style={{ margin: '8px 0' }} />
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Rate
                  value={resp.rating || 0}
                  onChange={(val) => handleRate(resp.id, val)}
                  style={{ fontSize: 14 }}
                />
                <Text type="secondary" style={{ fontSize: 11 }}>
                  {resp.rating ? `${resp.rating}星` : '未评分'}
                </Text>
              </div>
            </Card>
          ))}
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
        {debateRounds.map((r) => {
          const style = DEBATE_COLORS[r.round] || DEBATE_COLORS[1]
          return (
            <Card
              key={r.round}
              size="small"
              style={{ borderLeft: `3px solid ${style.border}`, background: style.bg }}
              title={
                <Space size={8}>
                  <Tag color={style.badge} style={{ fontWeight: 600 }}>Round {r.round} - {r.role}</Tag>
                  <Text strong style={{ fontSize: 13 }}>{r.model_name}</Text>
                  <Tag style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>
                    {(r.latency_ms / 1000).toFixed(1)}s
                  </Tag>
                </Space>
              }
            >
              <div style={{ fontSize: 14, lineHeight: 1.8, wordBreak: 'break-word' }}>
                <MarkdownRenderer content={r.content} />
              </div>
            </Card>
          )
        })}

        {/* Currently streaming round */}
        {activeDebateRound > 0 && (
          <Card
            size="small"
            style={{
              borderLeft: `3px solid ${(DEBATE_COLORS[activeDebateRound] || DEBATE_COLORS[1]).border}`,
              background: (DEBATE_COLORS[activeDebateRound] || DEBATE_COLORS[1]).bg,
            }}
            title={
              <Space size={8}>
                <Tag
                  color={(DEBATE_COLORS[activeDebateRound] || DEBATE_COLORS[1]).badge}
                  style={{ fontWeight: 600 }}
                >
                  Round {activeDebateRound} - {(DEBATE_COLORS[activeDebateRound] || DEBATE_COLORS[1]).label}
                </Tag>
                <Spin size="small" />
              </Space>
            }
          >
            {debateStreamContent ? (
              <div style={{ fontSize: 14, lineHeight: 1.8, wordBreak: 'break-word' }}>
                <MarkdownRenderer content={debateStreamContent} />
              </div>
            ) : (
              <div style={{ textAlign: 'center', padding: 20 }}>
                <Spin />
                <div style={{ marginTop: 8, color: '#94a3b8', fontSize: 12 }}>正在思考...</div>
              </div>
            )}
          </Card>
        )}
      </div>
    )
  }

  // ── Render: Streaming responses ────────────────────────────

  const renderStreaming = () => {
    const allModels = selectedModels
    if (Object.keys(streamingContents).length === 0 && streamingModels.length === 0) return null

    return (
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: allModels.length === 1 ? '1fr' : allModels.length === 2 ? '1fr 1fr' : `repeat(${Math.min(allModels.length, 3)}, 1fr)`,
          gap: 12,
          marginLeft: 42,
          marginBottom: 20,
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
              style={{ borderColor: '#bfdbfe', borderRadius: 10 }}
              styles={{ body: { padding: '10px 14px', maxHeight: 500, overflow: 'auto' } }}
            >
              {content ? (
                <div style={{ fontSize: 14, lineHeight: 1.8, wordBreak: 'break-word' }}>
                  <MarkdownRenderer content={content} />
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
        <div style={{ flex: 1, overflow: 'auto', padding: '16px 20px' }}>
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

              {/* Quick question suggestions */}
              <Text type="secondary" style={{ fontSize: 13 }}>快速开始：</Text>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8, marginTop: 8, textAlign: 'left' }}>
                {[
                  '帮我分析贵州茅台(600519)的基本面，包括最近的财报表现',
                  '当前A股市场的宏观环境如何？有哪些板块值得关注？',
                  '比较宁德时代和比亚迪在新能源领域的竞争优势',
                  '近期半导体板块大跌，分析一下原因和后续走势',
                ].map((q) => (
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
                ))}
              </div>
            </div>
          ) : (
            <>
              {messages.map(renderMessage)}
              {sending && !debateMode && renderStreaming()}
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

            {/* Web search toggle */}
            <Tooltip title="联网搜索：获取实时信息（股价、新闻、最新数据）">
              <Button
                type={webSearchEnabled ? 'primary' : 'text'}
                size="small"
                icon={<SearchOutlined />}
                onClick={() => setWebSearchEnabled(!webSearchEnabled)}
                style={{
                  borderRadius: 4, fontSize: 12, height: 26,
                  ...(webSearchEnabled ? { backgroundColor: '#059669', borderColor: '#059669' } : {}),
                }}
              >
                联网
              </Button>
            </Tooltip>

            {/* Debate mode toggle */}
            <Tooltip title="辩论模式：多模型依次辩论，看多方 vs 质疑方">
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
                <Button icon={<PaperClipOutlined />} size="middle" />
              </Tooltip>
            </Upload>
            <TextArea
              ref={inputRef}
              value={inputText}
              onChange={(e) => setInputText(e.target.value)}
              placeholder={sending ? '正在等待回复...' : '输入问题，Shift+Enter换行，Enter发送...'}
              autoSize={{ minRows: 1, maxRows: 6 }}
              style={{ flex: 1, borderRadius: 8 }}
              disabled={sending}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault()
                  handleSend()
                }
              }}
            />
            <Button
              type="primary"
              icon={debateMode ? <TrophyOutlined /> : <SendOutlined />}
              onClick={handleSend}
              loading={sending}
              disabled={!inputText.trim() && attachments.length === 0}
              danger={debateMode}
              style={{ borderRadius: 8 }}
            >
              {debateMode ? '开始辩论' : '发送'}
            </Button>
          </div>
        </div>
      </Content>

      {/* Right drawer */}
      {renderRightDrawer()}

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
    </Layout>
  )
}

