import { useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Card,
  Table,
  Tag,
  Input,
  Select,
  DatePicker,
  Button,
  Space,
  Statistic,
  Row,
  Col,
  Typography,
  Tooltip,
  message,
} from 'antd'
import { ReloadOutlined, ClearOutlined, SearchOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import dayjs, { Dayjs } from 'dayjs'
import {
  chatAuditApi,
  AuditRunSummary,
  AuditStats,
} from '../services/chatAudit'
import { useAuthStore } from '../store/auth'

const { RangePicker } = DatePicker

const STATUS_COLORS: Record<string, string> = {
  done: 'green',
  running: 'processing',
  error: 'red',
  cancelled: 'default',
}

export default function ChatAudit() {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const user = useAuthStore((s) => s.user)
  const isAdmin = user?.role === 'admin'

  const [runs, setRuns] = useState<AuditRunSummary[]>([])
  const [loading, setLoading] = useState(false)
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [stats, setStats] = useState<AuditStats | null>(null)

  // Filters
  const [filterUsername, setFilterUsername] = useState('')
  const [filterModel, setFilterModel] = useState<string | undefined>()
  const [filterTool, setFilterTool] = useState<string | undefined>()
  const [filterStatus, setFilterStatus] = useState<string | undefined>()
  const [filterHasError, setFilterHasError] = useState<boolean | undefined>()
  const [filterQ, setFilterQ] = useState('')
  const [filterRange, setFilterRange] = useState<[Dayjs, Dayjs] | null>(null)

  const params = useMemo(
    () => ({
      ...(isAdmin && filterUsername ? { username: filterUsername.trim() } : {}),
      ...(filterModel ? { model: filterModel } : {}),
      ...(filterTool ? { tool: filterTool } : {}),
      ...(filterStatus ? { status: filterStatus } : {}),
      ...(filterHasError !== undefined ? { has_error: filterHasError } : {}),
      ...(filterQ.trim() ? { q: filterQ.trim() } : {}),
      ...(filterRange
        ? {
            started_from: filterRange[0].toISOString(),
            started_to: filterRange[1].toISOString(),
          }
        : {}),
      limit: 50,
    }),
    [
      isAdmin,
      filterUsername,
      filterModel,
      filterTool,
      filterStatus,
      filterHasError,
      filterQ,
      filterRange,
    ],
  )

  const fetchPage = async (cursor: string | null = null, append = false) => {
    setLoading(true)
    try {
      const res = await chatAuditApi.listRuns({
        ...params,
        cursor: cursor || undefined,
      })
      setRuns((prev) => (append ? [...prev, ...res.runs] : res.runs))
      setNextCursor(res.next_cursor)
    } catch (e: any) {
      message.error(e.response?.data?.detail || t('common.error'))
    } finally {
      setLoading(false)
    }
  }

  const fetchStats = async () => {
    try {
      const s = await chatAuditApi.stats(7)
      setStats(s)
    } catch {
      /* non-fatal */
    }
  }

  useEffect(() => {
    fetchPage(null, false)
    fetchStats()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    filterUsername,
    filterModel,
    filterTool,
    filterStatus,
    filterHasError,
    filterRange,
  ])

  const onSearchEnter = () => {
    fetchPage(null, false)
  }

  const clearFilters = () => {
    setFilterUsername('')
    setFilterModel(undefined)
    setFilterTool(undefined)
    setFilterStatus(undefined)
    setFilterHasError(undefined)
    setFilterQ('')
    setFilterRange(null)
  }

  const formatLatency = (ms: number) => {
    if (!ms) return '-'
    if (ms < 1000) return `${ms} ms`
    return `${(ms / 1000).toFixed(1)} s`
  }

  return (
    <div>
      <Typography.Title level={4} style={{ marginBottom: 16 }}>
        {t('chatAudit.title')}
      </Typography.Title>

      {/* Stats strip */}
      <Row gutter={12} style={{ marginBottom: 16 }}>
        <Col span={4}>
          <Card size="small">
            <Statistic
              title={t('chatAudit.stats.totalRuns7d')}
              value={stats?.total_runs ?? 0}
            />
          </Card>
        </Col>
        <Col span={4}>
          <Card size="small">
            <Statistic
              title={t('chatAudit.stats.errorRate')}
              value={stats ? (stats.error_rate * 100).toFixed(1) : '0.0'}
              suffix="%"
              valueStyle={{
                color:
                  stats && stats.error_rate > 0.05 ? '#ef4444' : '#10b981',
              }}
            />
          </Card>
        </Col>
        <Col span={4}>
          <Card size="small">
            <Statistic
              title={t('chatAudit.stats.avgLatency')}
              value={
                stats?.avg_latency_ms
                  ? Math.round(stats.avg_latency_ms / 1000)
                  : 0
              }
              suffix="s"
            />
          </Card>
        </Col>
        <Col span={4}>
          <Card size="small">
            <Statistic
              title={t('chatAudit.stats.p95Latency')}
              value={
                stats?.p95_latency_ms
                  ? Math.round(stats.p95_latency_ms / 1000)
                  : 0
              }
              suffix="s"
            />
          </Card>
        </Col>
        <Col span={4}>
          <Card size="small">
            <Statistic
              title={t('chatAudit.stats.tokens')}
              value={stats?.total_tokens ?? 0}
            />
          </Card>
        </Col>
        <Col span={4}>
          <Card size="small">
            <Statistic
              title={t('chatAudit.stats.topTool')}
              valueRender={() => (
                <span style={{ fontSize: 14 }}>
                  {stats?.top_tools?.[0]
                    ? `${stats.top_tools[0].name} · ${stats.top_tools[0].count}`
                    : '-'}
                </span>
              )}
            />
          </Card>
        </Col>
      </Row>

      {/* Filters */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap>
          {isAdmin && (
            <Input
              allowClear
              placeholder={t('chatAudit.filter.username')}
              value={filterUsername}
              onChange={(e) => setFilterUsername(e.target.value)}
              onPressEnter={onSearchEnter}
              style={{ width: 160 }}
            />
          )}
          <Select
            allowClear
            placeholder={t('chatAudit.filter.model')}
            value={filterModel}
            onChange={setFilterModel}
            style={{ width: 200 }}
            options={[
              { value: 'openai/gpt-5.4', label: 'GPT-5.4' },
              { value: 'anthropic/claude-opus-4-7', label: 'Claude Opus 4.7' },
              { value: 'anthropic/claude-sonnet-4-6', label: 'Claude Sonnet 4.6' },
              { value: 'google/gemini-3-pro', label: 'Gemini 3 Pro' },
            ]}
          />
          <Select
            allowClear
            placeholder={t('chatAudit.filter.tool')}
            value={filterTool}
            onChange={setFilterTool}
            style={{ width: 180 }}
            options={[
              { value: 'web_search', label: 'web_search' },
              { value: 'read_webpage', label: 'read_webpage' },
              { value: 'kb_search', label: 'kb_search' },
              { value: 'kb_fetch_document', label: 'kb_fetch_document' },
              { value: 'user_kb_search', label: 'user_kb_search' },
              { value: 'user_kb_fetch_document', label: 'user_kb_fetch_document' },
            ]}
          />
          <Select
            allowClear
            placeholder={t('chatAudit.filter.status')}
            value={filterStatus}
            onChange={setFilterStatus}
            style={{ width: 140 }}
            options={[
              { value: 'running', label: t('chatAudit.status.running') },
              { value: 'done', label: t('chatAudit.status.done') },
              { value: 'error', label: t('chatAudit.status.error') },
              { value: 'cancelled', label: t('chatAudit.status.cancelled') },
            ]}
          />
          <Select
            allowClear
            placeholder={t('chatAudit.filter.errorOnly')}
            value={filterHasError}
            onChange={setFilterHasError}
            style={{ width: 140 }}
            options={[
              { value: true, label: t('chatAudit.filter.errorYes') },
            ]}
          />
          <RangePicker
            value={filterRange as any}
            onChange={(v) => setFilterRange(v as any)}
            showTime={{ format: 'HH:mm' }}
            format="YYYY-MM-DD HH:mm"
          />
          <Input
            allowClear
            placeholder={t('chatAudit.filter.q')}
            value={filterQ}
            onChange={(e) => setFilterQ(e.target.value)}
            onPressEnter={onSearchEnter}
            prefix={<SearchOutlined />}
            style={{ width: 240 }}
          />
          <Button
            icon={<ReloadOutlined />}
            onClick={() => {
              fetchPage(null, false)
              fetchStats()
            }}
          >
            {t('common.refresh')}
          </Button>
          <Button icon={<ClearOutlined />} onClick={clearFilters}>
            {t('chatAudit.filter.clear')}
          </Button>
        </Space>
      </Card>

      {/* Table */}
      <Card>
        <Table
          loading={loading}
          dataSource={runs}
          rowKey="id"
          size="small"
          pagination={false}
          onRow={(record) => ({
            onClick: () => navigate(`/chat-audit/${record.id}`),
            style: { cursor: 'pointer' },
          })}
          columns={[
            {
              title: t('chatAudit.col.startedAt'),
              dataIndex: 'started_at',
              key: 'started_at',
              width: 150,
              render: (v: string | null) =>
                v ? dayjs(v).tz('Asia/Shanghai').format('MM-DD HH:mm:ss') : '-',
            },
            ...(isAdmin
              ? [
                  {
                    title: t('chatAudit.col.user'),
                    dataIndex: 'username',
                    key: 'username',
                    width: 120,
                    render: (v: string) => v || '-',
                  },
                ]
              : []),
            {
              title: t('chatAudit.col.userContent'),
              dataIndex: 'user_content_preview',
              key: 'content',
              ellipsis: { showTitle: true },
              render: (v: string) => (
                <Tooltip title={v} placement="topLeft">
                  <Typography.Text ellipsis style={{ maxWidth: 360 }}>
                    {v || '(empty)'}
                  </Typography.Text>
                </Tooltip>
              ),
            },
            {
              title: t('chatAudit.col.models'),
              dataIndex: 'models_requested',
              key: 'models',
              width: 200,
              render: (v: string[]) => (
                <Space size={2} wrap>
                  {(v || []).map((m) => (
                    <Tag key={m} color="blue" style={{ fontSize: 11 }}>
                      {m.split('/').pop()}
                    </Tag>
                  ))}
                </Space>
              ),
            },
            {
              title: t('chatAudit.col.tools'),
              dataIndex: 'tool_calls_by_name',
              key: 'tools',
              width: 180,
              render: (m: Record<string, number>) => (
                <Space size={2} wrap>
                  {Object.entries(m || {}).map(([k, v]) => (
                    <Tag key={k} color="green" style={{ fontSize: 11 }}>
                      {k}·{v}
                    </Tag>
                  ))}
                </Space>
              ),
            },
            {
              title: t('chatAudit.col.rounds'),
              dataIndex: 'rounds_used',
              key: 'rounds',
              width: 70,
              align: 'right' as const,
            },
            {
              title: t('chatAudit.col.urls'),
              key: 'urls',
              width: 90,
              align: 'right' as const,
              render: (_: any, r: AuditRunSummary) => (
                <Tooltip title={t('chatAudit.col.urlsTooltip')}>
                  <span>
                    {r.urls_read}/{r.urls_searched}
                  </span>
                </Tooltip>
              ),
            },
            {
              title: t('chatAudit.col.citations'),
              dataIndex: 'citations_count',
              key: 'cit',
              width: 80,
              align: 'right' as const,
            },
            {
              title: t('chatAudit.col.tokens'),
              dataIndex: 'total_tokens',
              key: 'tokens',
              width: 90,
              align: 'right' as const,
            },
            {
              title: t('chatAudit.col.latency'),
              dataIndex: 'total_latency_ms',
              key: 'latency',
              width: 90,
              align: 'right' as const,
              render: formatLatency,
            },
            {
              title: t('chatAudit.col.status'),
              dataIndex: 'status',
              key: 'status',
              width: 90,
              render: (v: string) => (
                <Tag color={STATUS_COLORS[v] || 'default'}>{v}</Tag>
              ),
            },
          ]}
        />
        {nextCursor && (
          <div style={{ textAlign: 'center', marginTop: 12 }}>
            <Button
              loading={loading}
              onClick={() => fetchPage(nextCursor, true)}
            >
              {t('chatAudit.loadMore')}
            </Button>
          </div>
        )}
      </Card>
    </div>
  )
}
