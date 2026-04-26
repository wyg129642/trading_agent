import { useEffect, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import {
  Card,
  Tag,
  Typography,
  Descriptions,
  Button,
  Space,
  Spin,
  message,
  Statistic,
  Row,
  Col,
  Divider,
} from 'antd'
import {
  ArrowLeftOutlined,
  DownloadOutlined,
  ReloadOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import dayjs from 'dayjs'
import {
  chatAuditApi,
  AuditRunDetail,
  AuditEvent,
} from '../services/chatAudit'
import EventTimeline from '../components/audit/EventTimeline'
import MarkdownRenderer from '../components/MarkdownRenderer'
import CitationRenderer from '../components/CitationRenderer'
import { useAuthStore } from '../store/auth'

const STATUS_COLORS: Record<string, string> = {
  done: 'green',
  running: 'processing',
  error: 'red',
  cancelled: 'default',
}

export default function ChatAuditDetail() {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const { runId } = useParams<{ runId: string }>()
  const token = useAuthStore((s) => s.token)

  const [detail, setDetail] = useState<AuditRunDetail | null>(null)
  const [events, setEvents] = useState<AuditEvent[]>([])
  const [loading, setLoading] = useState(true)

  const loadDetail = async () => {
    if (!runId) return
    setLoading(true)
    try {
      const [d, ev] = await Promise.all([
        chatAuditApi.getRun(runId),
        chatAuditApi.listEvents(runId, { limit: 5000 }),
      ])
      setDetail(d)
      setEvents(ev.events)
    } catch (e: any) {
      message.error(e.response?.data?.detail || t('common.error'))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadDetail()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId])

  if (loading || !detail) {
    return (
      <div style={{ textAlign: 'center', padding: 64 }}>
        <Spin />
      </div>
    )
  }

  const r = detail.run

  const exportRun = async () => {
    if (!runId) return
    try {
      // Use fetch to attach Authorization header (axios baseURL='/api' would
      // also work, but we need to trigger a download not parse JSON).
      const resp = await fetch(chatAuditApi.exportRun(runId), {
        headers: { Authorization: `Bearer ${token}` },
      })
      if (!resp.ok) {
        message.error(`Export failed (HTTP ${resp.status})`)
        return
      }
      const blob = await resp.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `chat-audit-${r.trace_id}.json`
      a.click()
      URL.revokeObjectURL(url)
    } catch (e: any) {
      message.error(e.message || t('common.error'))
    }
  }

  return (
    <div>
      <Space style={{ marginBottom: 12 }}>
        <Button
          icon={<ArrowLeftOutlined />}
          onClick={() => navigate('/chat-audit')}
        >
          {t('chatAudit.back')}
        </Button>
        <Button icon={<ReloadOutlined />} onClick={loadDetail}>
          {t('common.refresh')}
        </Button>
        <Button icon={<DownloadOutlined />} onClick={exportRun}>
          {t('chatAudit.export')}
        </Button>
      </Space>

      {/* Run header */}
      <Card size="small" style={{ marginBottom: 12 }}>
        <Descriptions
          size="small"
          column={3}
          title={
            <Space>
              <Tag color={STATUS_COLORS[r.status] || 'default'}>{r.status}</Tag>
              <Typography.Text code>{r.trace_id}</Typography.Text>
              <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                {r.started_at
                  ? dayjs(r.started_at).tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
                  : '-'}
              </Typography.Text>
            </Space>
          }
        >
          <Descriptions.Item label={t('chatAudit.detail.user')}>
            <Typography.Text strong>{r.username || '-'}</Typography.Text>
          </Descriptions.Item>
          <Descriptions.Item label={t('chatAudit.detail.conversation')}>
            <Typography.Text code>
              {r.conversation_id?.slice(0, 8) ?? '-'}
            </Typography.Text>
          </Descriptions.Item>
          <Descriptions.Item label={t('chatAudit.detail.mode')}>
            {r.mode}
          </Descriptions.Item>
          <Descriptions.Item label={t('chatAudit.detail.models')}>
            <Space size={2} wrap>
              {(r.models_requested || []).map((m) => (
                <Tag key={m} color="blue">
                  {m.split('/').pop()}
                </Tag>
              ))}
            </Space>
          </Descriptions.Item>
          <Descriptions.Item label={t('chatAudit.detail.webSearch')}>
            {r.web_search_mode}
          </Descriptions.Item>
          <Descriptions.Item label={t('chatAudit.detail.flags')}>
            <Space size={2} wrap>
              {Object.entries(r.feature_flags || {})
                .filter(([, v]) => v)
                .map(([k]) => (
                  <Tag key={k}>{k}</Tag>
                ))}
            </Space>
          </Descriptions.Item>
          {r.error_message && (
            <Descriptions.Item label={t('chatAudit.detail.error')} span={3}>
              <Typography.Text type="danger">{r.error_message}</Typography.Text>
            </Descriptions.Item>
          )}
        </Descriptions>

        <Divider style={{ margin: '12px 0' }} />

        <Row gutter={12}>
          <Col span={3}>
            <Statistic
              title={t('chatAudit.detail.rounds')}
              value={r.rounds_used}
            />
          </Col>
          <Col span={3}>
            <Statistic
              title={t('chatAudit.detail.toolCalls')}
              value={r.tool_calls_total}
            />
          </Col>
          <Col span={3}>
            <Statistic
              title={t('chatAudit.detail.urlsRead')}
              value={r.urls_read}
              suffix={`/${r.urls_searched}`}
            />
          </Col>
          <Col span={3}>
            <Statistic
              title={t('chatAudit.detail.citations')}
              value={r.citations_count}
            />
          </Col>
          <Col span={3}>
            <Statistic
              title={t('chatAudit.detail.tokens')}
              value={r.total_tokens}
            />
          </Col>
          <Col span={3}>
            <Statistic
              title={t('chatAudit.detail.latency')}
              value={
                r.total_latency_ms
                  ? (r.total_latency_ms / 1000).toFixed(1)
                  : 0
              }
              suffix="s"
            />
          </Col>
          <Col span={6}>
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              {t('chatAudit.detail.toolBreakdown')}
            </Typography.Text>
            <div style={{ marginTop: 4 }}>
              {Object.entries(r.tool_calls_by_name || {}).map(([k, v]) => (
                <Tag key={k} color="green">
                  {k}·{v}
                </Tag>
              ))}
            </div>
          </Col>
        </Row>
      </Card>

      {/* User prompt */}
      <Card
        size="small"
        title={t('chatAudit.detail.userPrompt')}
        style={{ marginBottom: 12 }}
      >
        <MarkdownRenderer content={detail.user_content_full || '(empty)'} />
      </Card>

      {/* Final model responses */}
      {detail.model_responses.length > 0 && (
        <Card
          size="small"
          title={t('chatAudit.detail.finalAnswers')}
          style={{ marginBottom: 12 }}
        >
          {detail.model_responses.map((mr, i) => (
            <div key={mr.id} style={{ marginBottom: i < detail.model_responses.length - 1 ? 24 : 0 }}>
              <Space style={{ marginBottom: 8 }}>
                <Tag color="blue">{mr.model_name || mr.model_id}</Tag>
                {mr.error && <Tag color="red">error</Tag>}
                {mr.latency_ms != null && (
                  <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                    {(mr.latency_ms / 1000).toFixed(1)}s · {mr.tokens_used ?? 0} tokens
                  </Typography.Text>
                )}
              </Space>
              {mr.error ? (
                <Typography.Text type="danger">{mr.error}</Typography.Text>
              ) : (
                <CitationRenderer
                  content={mr.content || ''}
                  sources={(mr.sources || []) as any}
                />
              )}
              {i < detail.model_responses.length - 1 && (
                <Divider style={{ margin: '16px 0' }} />
              )}
            </div>
          ))}
        </Card>
      )}

      {/* Event timeline */}
      <Card
        size="small"
        title={
          <Space>
            <span>{t('chatAudit.detail.timeline')}</span>
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              {events.length} events
            </Typography.Text>
          </Space>
        }
      >
        <EventTimeline events={events} />
      </Card>
    </div>
  )
}
