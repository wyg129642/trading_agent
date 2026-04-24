import { useState, useEffect } from 'react'
import { Modal, Rate, Tag, Input, Typography, Space, message, Alert } from 'antd'
import api from '../services/api'

const { TextArea } = Input
const { Text } = Typography

// Feedback tag presets. Keys are snake_case (what we store); labels are bilingual.
// Positive tags show green, negative tags show red — purely visual affordance.
export const FEEDBACK_TAGS: Array<{ key: string; label: string; sentiment: 'positive' | 'negative' }> = [
  // Positive
  { key: 'accurate', label: '准确', sentiment: 'positive' },
  { key: 'clear', label: '清晰', sentiment: 'positive' },
  { key: 'helpful', label: '有用', sentiment: 'positive' },
  { key: 'concise', label: '简洁', sentiment: 'positive' },
  { key: 'relevant', label: '相关', sentiment: 'positive' },
  { key: 'comprehensive', label: '全面', sentiment: 'positive' },
  { key: 'well_sourced', label: '引用可靠', sentiment: 'positive' },
  // Negative
  { key: 'too_long', label: '太冗长', sentiment: 'negative' },
  { key: 'outdated', label: '信息过时', sentiment: 'negative' },
  { key: 'off_topic', label: '偏题', sentiment: 'negative' },
  { key: 'wrong', label: '事实错误', sentiment: 'negative' },
  { key: 'biased', label: '立场偏颇', sentiment: 'negative' },
  { key: 'unclear', label: '表达不清', sentiment: 'negative' },
  { key: 'missing_sources', label: '缺少引用', sentiment: 'negative' },
  { key: 'hallucinated', label: '疑似编造', sentiment: 'negative' },
]

interface Props {
  open: boolean
  responseId: string | null
  modelName?: string
  // existing values (when editing/viewing prior feedback)
  initialRating?: number | null
  initialTags?: string[]
  initialText?: string
  onClose: () => void
  onSubmitted?: (data: { rating: number | null; tags: string[]; text: string }) => void
}

export default function FeedbackModal({
  open,
  responseId,
  modelName,
  initialRating,
  initialTags,
  initialText,
  onClose,
  onSubmitted,
}: Props) {
  const [rating, setRating] = useState<number>(0)
  const [selectedTags, setSelectedTags] = useState<Set<string>>(new Set())
  const [text, setText] = useState<string>('')
  const [submitting, setSubmitting] = useState(false)

  useEffect(() => {
    if (open) {
      setRating(initialRating || 0)
      setSelectedTags(new Set(initialTags || []))
      setText(initialText || '')
    }
  }, [open, initialRating, initialTags, initialText])

  const toggleTag = (tag: string) => {
    setSelectedTags((prev) => {
      const next = new Set(prev)
      if (next.has(tag)) {
        next.delete(tag)
      } else {
        next.add(tag)
      }
      return next
    })
  }

  const canSubmit = rating > 0 || selectedTags.size > 0 || text.trim().length > 0

  const handleSubmit = async () => {
    if (!responseId || !canSubmit) return
    setSubmitting(true)
    try {
      const tags = Array.from(selectedTags)
      await api.post(`/chat-memory/feedback/${responseId}`, {
        rating: rating || null,
        feedback_tags: tags,
        feedback_text: text.trim(),
      })
      message.success('评价已提交，系统将自动学习您的偏好')
      onSubmitted?.({ rating: rating || null, tags, text: text.trim() })
      onClose()
    } catch (err: any) {
      const detail = err?.response?.data?.detail || '提交失败'
      message.error(typeof detail === 'string' ? detail : '提交失败')
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Modal
      title={
        <span>
          <span>详细评价</span>
          {modelName && <Text type="secondary" style={{ marginLeft: 8, fontWeight: 400 }}>{modelName}</Text>}
        </span>
      }
      open={open}
      onCancel={onClose}
      onOk={handleSubmit}
      okText="提交评价"
      cancelText="取消"
      confirmLoading={submitting}
      okButtonProps={{ disabled: !canSubmit }}
      width={600}
      destroyOnHidden
    >
      <Alert
        type="info"
        showIcon
        message="这次评价将帮助 AI 记住您的偏好"
        description="系统会自动从您的反馈中提炼长期偏好（如喜欢的回答结构、关注的主题、需要避免的错误），并在未来对话中自动参考。您随时可以在「我的记忆」里查看或删除这些记忆。"
        style={{ marginBottom: 16 }}
      />

      <div style={{ marginBottom: 16 }}>
        <Text strong>评分</Text>
        <div style={{ marginTop: 6 }}>
          <Rate value={rating} onChange={setRating} />
          <Text type="secondary" style={{ marginLeft: 10 }}>
            {rating ? `${rating} 星` : '未评分（可选）'}
          </Text>
        </div>
      </div>

      <div style={{ marginBottom: 16 }}>
        <Text strong>标签（多选，描述这次回答的特点）</Text>
        <div style={{ marginTop: 8 }}>
          <Space size={[8, 8]} wrap>
            {FEEDBACK_TAGS.map((t) => {
              const active = selectedTags.has(t.key)
              const color = active
                ? (t.sentiment === 'positive' ? 'green' : 'red')
                : 'default'
              return (
                <Tag.CheckableTag
                  key={t.key}
                  checked={active}
                  onChange={() => toggleTag(t.key)}
                  style={{
                    padding: '4px 10px',
                    borderRadius: 14,
                    fontSize: 12,
                    userSelect: 'none',
                    border: `1px solid ${active
                      ? (t.sentiment === 'positive' ? '#52c41a' : '#ff4d4f')
                      : '#d9d9d9'}`,
                    background: active
                      ? (t.sentiment === 'positive' ? '#f6ffed' : '#fff1f0')
                      : 'transparent',
                    color: active
                      ? (t.sentiment === 'positive' ? '#389e0d' : '#cf1322')
                      : '#595959',
                  }}
                >
                  {t.label}
                </Tag.CheckableTag>
              )
            })}
          </Space>
        </div>
      </div>

      <div>
        <Text strong>详细文字反馈（可选，但会大幅提升学习效果）</Text>
        <TextArea
          value={text}
          onChange={(e) => setText(e.target.value)}
          rows={4}
          maxLength={4000}
          showCount
          placeholder="例如：请下次用表格对比几家公司 / 回答太长了，希望更简洁 / 对半导体行业持续深入……"
          style={{ marginTop: 6 }}
        />
      </div>
    </Modal>
  )
}
