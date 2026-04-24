import { useState, useEffect, useMemo, useImperativeHandle, forwardRef } from 'react'
import {
  Card, Button, Typography, Tag, Switch, Popconfirm, Empty, Spin,
  Space, message, Modal, Input, Select, Tooltip, Alert,
} from 'antd'
import {
  DeleteOutlined, PushpinOutlined, PushpinFilled, PlusOutlined,
  EditOutlined, InfoCircleOutlined, BulbOutlined, ReloadOutlined,
} from '@ant-design/icons'
import api from '../services/api'

const { Text, Paragraph } = Typography
const { TextArea } = Input

export interface MemoryResponse {
  id: string
  memory_type: string
  memory_key: string
  content: string
  evidence: any[]
  confidence_score: number
  source_type: string
  usage_count: number
  is_active: boolean
  is_pinned: boolean
  last_used_at: string | null
  created_at: string
  updated_at: string
}

export interface MemoryListResponse {
  memories: MemoryResponse[]
  total: number
  total_active: number
}

export const MEMORY_TYPE_META: Record<string, { label: string; color: string; desc: string }> = {
  correction: { label: '纠偏', color: 'red', desc: '您明确指出需要避免的错误或模式' },
  preference: { label: '偏好', color: 'blue', desc: '您喜欢的回答结构与格式' },
  style: { label: '风格', color: 'cyan', desc: '语气、篇幅、语言偏好' },
  profile: { label: '画像', color: 'purple', desc: '您的身份、角色、专业背景' },
  topic_interest: { label: '关注主题', color: 'orange', desc: '您长期关注的话题' },
  domain_knowledge: { label: '领域知识', color: 'geekblue', desc: '与您工作相关的事实上下文' },
}

const SOURCE_LABEL: Record<string, string> = {
  feedback_derived: '从反馈学习',
  conversation_derived: '从对话学习',
  manual: '手动添加',
}

export interface MemoriesPanelProps {
  /** Compact mode skips the info Alert and shrinks paddings (for use in a drawer) */
  compact?: boolean
  /** Memory IDs that were just used in the latest chat turn — these get a highlight ring */
  highlightedIds?: string[]
  /** Called whenever the active-memory count changes (for badges upstream) */
  onActiveCountChange?: (count: number) => void
}

export interface MemoriesPanelHandle {
  refresh: () => void
}

const MemoriesPanel = forwardRef<MemoriesPanelHandle, MemoriesPanelProps>(function MemoriesPanel(
  { compact = false, highlightedIds, onActiveCountChange },
  ref,
) {
  const [loading, setLoading] = useState(false)
  const [data, setData] = useState<MemoryListResponse>({ memories: [], total: 0, total_active: 0 })
  const [filterType, setFilterType] = useState<string>('all')
  const [includeInactive, setIncludeInactive] = useState<boolean>(true)
  const [createModalOpen, setCreateModalOpen] = useState(false)
  const [editTarget, setEditTarget] = useState<MemoryResponse | null>(null)

  const highlightSet = useMemo(() => new Set(highlightedIds || []), [highlightedIds])

  const fetchMemories = async () => {
    setLoading(true)
    try {
      const params: Record<string, any> = { include_inactive: includeInactive }
      if (filterType !== 'all') params.memory_type = filterType
      const res = await api.get<MemoryListResponse>('/chat-memory/memories', { params })
      setData(res.data)
      onActiveCountChange?.(res.data.total_active)
    } catch {
      message.error('加载失败')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchMemories()
  }, [filterType, includeInactive])

  useImperativeHandle(ref, () => ({ refresh: fetchMemories }), [includeInactive, filterType])

  const handleToggleActive = async (mem: MemoryResponse) => {
    try {
      await api.patch(`/chat-memory/memories/${mem.id}`, { is_active: !mem.is_active })
      message.success(mem.is_active ? '已停用' : '已启用')
      fetchMemories()
    } catch {
      message.error('更新失败')
    }
  }

  const handleTogglePin = async (mem: MemoryResponse) => {
    try {
      await api.patch(`/chat-memory/memories/${mem.id}`, { is_pinned: !mem.is_pinned })
      fetchMemories()
    } catch {
      message.error('更新失败')
    }
  }

  const handleDelete = async (mem: MemoryResponse) => {
    try {
      await api.delete(`/chat-memory/memories/${mem.id}`)
      message.success('已删除')
      fetchMemories()
    } catch {
      message.error('删除失败')
    }
  }

  const grouped = useMemo(() => {
    const g: Record<string, MemoryResponse[]> = {}
    for (const m of data.memories) {
      if (!g[m.memory_type]) g[m.memory_type] = []
      g[m.memory_type].push(m)
    }
    return g
  }, [data.memories])

  const typeOptions = useMemo(() => {
    const opts = [{ value: 'all', label: `全部 (${data.memories.length})` }]
    for (const t of Object.keys(MEMORY_TYPE_META)) {
      opts.push({
        value: t,
        label: `${MEMORY_TYPE_META[t].label} (${(grouped[t] || []).length})`,
      })
    }
    return opts
  }, [data.memories, grouped])

  const bodyPadding = compact ? 10 : 20

  return (
    <div style={{ padding: compact ? 0 : bodyPadding }}>
      {!compact && (
        <Alert
          type="info"
          showIcon
          icon={<BulbOutlined />}
          message="记忆如何工作？"
          description={
            <ul style={{ marginTop: 4, marginBottom: 0, paddingLeft: 18 }}>
              <li>当您对回答评价（打分 + 标签 + 文字反馈）时，系统会在后台自动提炼出长期记忆。</li>
              <li>📌 置顶记忆永远生效且不会被新数据覆盖，适合手动设定「我永远希望…」这种明确偏好。</li>
              <li>停用的记忆不会注入到下次对话，但保留历史可随时重新启用。</li>
              <li>「纠偏」类记忆优先级最高——系统会优先避免您指出过的错误。</li>
            </ul>
          }
          style={{ marginBottom: 16 }}
        />
      )}

      <Card bodyStyle={{ padding: compact ? 10 : 24 }} style={compact ? { boxShadow: 'none', border: 'none' } : undefined}>
        <Space wrap style={{ marginBottom: 12, width: '100%', justifyContent: 'space-between' }}>
          <Space wrap size={6}>
            <Switch
              checked={includeInactive}
              onChange={setIncludeInactive}
              checkedChildren="含停用"
              unCheckedChildren="仅启用"
            />
            <Select
              value={filterType}
              onChange={setFilterType}
              options={typeOptions}
              style={{ minWidth: 140 }}
              size={compact ? 'small' : 'middle'}
            />
          </Space>
          <Space size={6}>
            <Tooltip title="刷新">
              <Button
                icon={<ReloadOutlined />}
                onClick={fetchMemories}
                size={compact ? 'small' : 'middle'}
              />
            </Tooltip>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setCreateModalOpen(true)}
              size={compact ? 'small' : 'middle'}
            >
              添加
            </Button>
          </Space>
        </Space>

        <Spin spinning={loading}>
          {data.memories.length === 0 ? (
            <Empty description="暂无记忆 — 对 AI 回答进行详细评价后，系统会自动学习您的偏好" />
          ) : (
            <Space direction="vertical" size={8} style={{ width: '100%' }}>
              {data.memories.map((m) => {
                const meta = MEMORY_TYPE_META[m.memory_type] || { label: m.memory_type, color: 'default', desc: '' }
                const isHighlighted = highlightSet.has(m.id)
                return (
                  <Card
                    key={m.id}
                    size="small"
                    style={{
                      opacity: m.is_active ? 1 : 0.55,
                      borderLeft: `3px solid ${m.is_pinned ? '#faad14' : (m.is_active ? '#2563eb' : '#d9d9d9')}`,
                      boxShadow: isHighlighted ? '0 0 0 2px #52c41a' : undefined,
                      transition: 'box-shadow 0.3s',
                    }}
                    bodyStyle={{ padding: 10 }}
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 8 }}>
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <Space size={4} wrap style={{ marginBottom: 4 }}>
                          <Tag color={meta.color} style={{ marginInlineEnd: 0 }}>{meta.label}</Tag>
                          {m.is_pinned && <Tag icon={<PushpinFilled />} color="gold" style={{ marginInlineEnd: 0 }}>置顶</Tag>}
                          {!m.is_active && <Tag color="default" style={{ marginInlineEnd: 0 }}>已停用</Tag>}
                          {isHighlighted && <Tag color="green" style={{ marginInlineEnd: 0 }}>本轮已生效</Tag>}
                          <Tooltip title="记忆的唯一标识，用于去重">
                            <Text code style={{ fontSize: 11, color: '#94a3b8' }}>{m.memory_key}</Text>
                          </Tooltip>
                          <Tooltip title="置信度由 LLM 根据反馈明确程度打分">
                            <Text type="secondary" style={{ fontSize: 11 }}>
                              置信 {(m.confidence_score * 100).toFixed(0)}%
                            </Text>
                          </Tooltip>
                          <Text type="secondary" style={{ fontSize: 11 }}>
                            {SOURCE_LABEL[m.source_type] || m.source_type}
                          </Text>
                          {m.usage_count > 0 && (
                            <Tooltip title="该记忆被注入对话的次数">
                              <Text type="secondary" style={{ fontSize: 11 }}>
                                已生效 {m.usage_count} 次
                              </Text>
                            </Tooltip>
                          )}
                        </Space>
                        <Paragraph style={{ margin: 0, fontSize: 13 }}>{m.content}</Paragraph>
                        {m.evidence && m.evidence.length > 0 && (
                          <Tooltip
                            title={
                              <div style={{ maxWidth: 400 }}>
                                {m.evidence.slice(0, 4).map((ev: any, i: number) => (
                                  <div key={i} style={{ marginBottom: 4 }}>
                                    <Text style={{ color: '#fff' }}>
                                      · {(ev.excerpt || ev.note || '').toString().slice(0, 100)}
                                    </Text>
                                  </div>
                                ))}
                              </div>
                            }
                          >
                            <Text type="secondary" style={{ fontSize: 11, cursor: 'help' }}>
                              <InfoCircleOutlined /> 来源: {m.evidence.length} 条证据
                            </Text>
                          </Tooltip>
                        )}
                      </div>
                      <Space size={2} direction="vertical">
                        <Space size={2}>
                          <Tooltip title={m.is_pinned ? '取消置顶' : '置顶（永不覆盖）'}>
                            <Button
                              type="text"
                              size="small"
                              icon={m.is_pinned ? <PushpinFilled /> : <PushpinOutlined />}
                              onClick={() => handleTogglePin(m)}
                              style={{ color: m.is_pinned ? '#faad14' : undefined }}
                            />
                          </Tooltip>
                          <Tooltip title="编辑内容">
                            <Button
                              type="text"
                              size="small"
                              icon={<EditOutlined />}
                              onClick={() => setEditTarget(m)}
                            />
                          </Tooltip>
                          <Popconfirm
                            title="删除这条记忆？"
                            description="删除后 AI 不会再记住这一条。"
                            okText="删除"
                            cancelText="取消"
                            onConfirm={() => handleDelete(m)}
                          >
                            <Tooltip title="删除">
                              <Button type="text" size="small" icon={<DeleteOutlined />} danger />
                            </Tooltip>
                          </Popconfirm>
                        </Space>
                        <Switch
                          size="small"
                          checked={m.is_active}
                          onChange={() => handleToggleActive(m)}
                        />
                      </Space>
                    </div>
                  </Card>
                )
              })}
            </Space>
          )}
        </Spin>
      </Card>

      <CreateMemoryModal
        open={createModalOpen}
        onClose={() => setCreateModalOpen(false)}
        onCreated={() => {
          setCreateModalOpen(false)
          fetchMemories()
        }}
      />
      <EditMemoryModal
        target={editTarget}
        onClose={() => setEditTarget(null)}
        onUpdated={() => {
          setEditTarget(null)
          fetchMemories()
        }}
      />
    </div>
  )
})

export default MemoriesPanel


function CreateMemoryModal({
  open, onClose, onCreated,
}: { open: boolean; onClose: () => void; onCreated: () => void }) {
  const [memoryType, setMemoryType] = useState('preference')
  const [memoryKey, setMemoryKey] = useState('')
  const [content, setContent] = useState('')
  const [isPinned, setIsPinned] = useState(false)
  const [submitting, setSubmitting] = useState(false)

  useEffect(() => {
    if (open) {
      setMemoryType('preference')
      setMemoryKey('')
      setContent('')
      setIsPinned(false)
    }
  }, [open])

  const handleCreate = async () => {
    if (!memoryKey.trim() || !content.trim()) {
      message.warning('请填写标识和内容')
      return
    }
    setSubmitting(true)
    try {
      await api.post('/chat-memory/memories', {
        memory_type: memoryType,
        memory_key: memoryKey.trim(),
        content: content.trim(),
        is_pinned: isPinned,
      })
      message.success('添加成功')
      onCreated()
    } catch (err: any) {
      const detail = err?.response?.data?.detail || '添加失败'
      message.error(typeof detail === 'string' ? detail : '添加失败')
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Modal
      title="手动添加记忆"
      open={open}
      onCancel={onClose}
      onOk={handleCreate}
      confirmLoading={submitting}
      okText="添加"
      cancelText="取消"
      destroyOnHidden
    >
      <div style={{ marginBottom: 12 }}>
        <Text strong>类型</Text>
        <Select
          value={memoryType}
          onChange={setMemoryType}
          style={{ width: '100%', marginTop: 4 }}
          options={Object.entries(MEMORY_TYPE_META).map(([key, meta]) => ({
            value: key,
            label: `${meta.label} — ${meta.desc}`,
          }))}
        />
      </div>
      <div style={{ marginBottom: 12 }}>
        <Text strong>稳定标识（snake_case，用于去重）</Text>
        <Input
          value={memoryKey}
          onChange={(e) => setMemoryKey(e.target.value)}
          placeholder="例如: prefers_concise_bullets"
          style={{ marginTop: 4 }}
          maxLength={120}
        />
      </div>
      <div style={{ marginBottom: 12 }}>
        <Text strong>记忆内容（一句话，面向 AI 助手可读）</Text>
        <TextArea
          value={content}
          onChange={(e) => setContent(e.target.value)}
          placeholder="例如: 用户偏好结论在前、分条列表的简洁回答"
          rows={3}
          maxLength={600}
          showCount
          style={{ marginTop: 4 }}
        />
      </div>
      <div>
        <Switch checked={isPinned} onChange={setIsPinned} />
        <Text style={{ marginLeft: 8 }}>置顶此记忆（永不被后续学习覆盖）</Text>
      </div>
    </Modal>
  )
}


function EditMemoryModal({
  target, onClose, onUpdated,
}: {
  target: MemoryResponse | null
  onClose: () => void
  onUpdated: () => void
}) {
  const [content, setContent] = useState('')
  const [submitting, setSubmitting] = useState(false)

  useEffect(() => {
    if (target) setContent(target.content)
  }, [target])

  const handleSave = async () => {
    if (!target) return
    if (!content.trim()) {
      message.warning('内容不能为空')
      return
    }
    setSubmitting(true)
    try {
      await api.patch(`/chat-memory/memories/${target.id}`, {
        content: content.trim(),
      })
      message.success('已保存')
      onUpdated()
    } catch {
      message.error('保存失败')
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Modal
      title="编辑记忆"
      open={target !== null}
      onCancel={onClose}
      onOk={handleSave}
      confirmLoading={submitting}
      okText="保存"
      cancelText="取消"
      destroyOnHidden
    >
      {target && (
        <>
          <div style={{ marginBottom: 8 }}>
            <Tag color={MEMORY_TYPE_META[target.memory_type]?.color || 'default'}>
              {MEMORY_TYPE_META[target.memory_type]?.label || target.memory_type}
            </Tag>
            <Text code style={{ fontSize: 11, color: '#94a3b8' }}>{target.memory_key}</Text>
          </div>
          <TextArea
            value={content}
            onChange={(e) => setContent(e.target.value)}
            rows={4}
            maxLength={600}
            showCount
          />
        </>
      )}
    </Modal>
  )
}
