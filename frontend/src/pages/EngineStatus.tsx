import { useEffect, useState, useCallback } from 'react'
import {
  Card,
  Row,
  Col,
  Tag,
  Button,
  Space,
  Statistic,
  Typography,
  Descriptions,
  Modal,
  Spin,
  message,
  Alert,
  List,
} from 'antd'
import {
  PlayCircleOutlined,
  PauseCircleOutlined,
  ReloadOutlined,
  CheckCircleFilled,
  CloseCircleFilled,
  LoadingOutlined,
  FileTextOutlined,
  DashboardOutlined,
  ClockCircleOutlined,
  RocketOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { useAuthStore } from '../store/auth'
import api from '../services/api'

const { Text, Title, Paragraph } = Typography

interface EngineStatus {
  is_running: boolean
  pid: number | null
  start_time: string | null
  uptime_seconds: number | null
  restart_count: number
  auto_restart: boolean
  engine_status: string
  engine_message: string
  monitors: number
  queue_size: number
  stats: {
    news_items?: number
    filter_results?: number
    analysis_results?: number
    research_reports?: number
  } | null
  last_heartbeat: string | null
}

function formatUptime(seconds: number | null): string {
  if (!seconds) return '-'
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = seconds % 60
  if (h > 0) return `${h}h ${m}m ${s}s`
  if (m > 0) return `${m}m ${s}s`
  return `${s}s`
}

export default function EngineStatus() {
  const { t } = useTranslation()
  const user = useAuthStore((s) => s.user)
  const isAdmin = user?.role === 'admin'

  const [status, setStatus] = useState<EngineStatus | null>(null)
  const [logs, setLogs] = useState<string[]>([])
  const [loading, setLoading] = useState(true)
  const [actionLoading, setActionLoading] = useState(false)
  const [logsOpen, setLogsOpen] = useState(false)

  const fetchStatus = useCallback(async () => {
    try {
      const res = await api.get('/engine/status')
      setStatus(res.data)
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }, [])

  const fetchLogs = async () => {
    try {
      const res = await api.get('/engine/logs', { params: { lines: 200 } })
      setLogs(res.data.logs || [])
    } catch (e) {
      console.error(e)
    }
  }

  useEffect(() => {
    fetchStatus()
    const interval = setInterval(fetchStatus, 10000) // refresh every 10s
    return () => clearInterval(interval)
  }, [fetchStatus])

  const handleAction = async (action: 'start' | 'stop' | 'restart') => {
    setActionLoading(true)
    try {
      const res = await api.post(`/engine/${action}`)
      message.success(res.data.message)
      setTimeout(fetchStatus, 2000)
    } catch (e: any) {
      message.error(e.response?.data?.detail || `Failed to ${action} engine`)
    } finally {
      setActionLoading(false)
    }
  }

  if (loading) {
    return (
      <div style={{ textAlign: 'center', padding: 100 }}>
        <Spin size="large" />
      </div>
    )
  }

  const statusColor = status?.is_running ? '#52c41a' : '#ff4d4f'
  const statusIcon = status?.is_running ? <CheckCircleFilled /> : <CloseCircleFilled />
  const statusText = status?.is_running
    ? status.engine_status === 'running'
      ? t('engine.running')
      : t('engine.starting')
    : t('engine.stopped')

  return (
    <div>
      {/* Status Overview */}
      <Row gutter={[16, 16]}>
        <Col xs={24} lg={16}>
          <Card>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
              <Space size="middle">
                <span style={{ color: statusColor, fontSize: 28 }}>{statusIcon}</span>
                <div>
                  <Title level={4} style={{ margin: 0 }}>
                    {t('engine.title')}
                  </Title>
                  <Tag color={statusColor} style={{ marginTop: 4 }}>
                    {statusText}
                  </Tag>
                  {status?.auto_restart && (
                    <Tag color="blue">{t('engine.autoRestart')}</Tag>
                  )}
                </div>
              </Space>

              {isAdmin && (
                <Space>
                  {!status?.is_running ? (
                    <Button
                      type="primary"
                      icon={<PlayCircleOutlined />}
                      onClick={() => handleAction('start')}
                      loading={actionLoading}
                    >
                      {t('engine.start')}
                    </Button>
                  ) : (
                    <>
                      <Button
                        icon={<ReloadOutlined />}
                        onClick={() => handleAction('restart')}
                        loading={actionLoading}
                      >
                        {t('engine.restart')}
                      </Button>
                      <Button
                        danger
                        icon={<PauseCircleOutlined />}
                        onClick={() => handleAction('stop')}
                        loading={actionLoading}
                      >
                        {t('engine.stop')}
                      </Button>
                    </>
                  )}
                  <Button
                    icon={<FileTextOutlined />}
                    onClick={() => {
                      fetchLogs()
                      setLogsOpen(true)
                    }}
                  >
                    {t('engine.logs')}
                  </Button>
                </Space>
              )}
            </div>

            <Descriptions column={{ xs: 1, sm: 2, lg: 3 }} size="small" bordered>
              <Descriptions.Item label="PID">{status?.pid || '-'}</Descriptions.Item>
              <Descriptions.Item label={t('engine.uptime')}>
                <ClockCircleOutlined /> {formatUptime(status?.uptime_seconds ?? null)}
              </Descriptions.Item>
              <Descriptions.Item label={t('engine.restarts')}>
                {status?.restart_count || 0}
              </Descriptions.Item>
              <Descriptions.Item label={t('engine.monitors')}>
                <RocketOutlined /> {status?.monitors || 0} {t('engine.active')}
              </Descriptions.Item>
              <Descriptions.Item label={t('engine.queueSize')}>
                {status?.queue_size || 0}
              </Descriptions.Item>
              <Descriptions.Item label={t('engine.heartbeat')}>
                {status?.last_heartbeat
                  ? new Date(status.last_heartbeat).toLocaleTimeString()
                  : '-'}
              </Descriptions.Item>
            </Descriptions>
          </Card>
        </Col>

        <Col xs={24} lg={8}>
          <Row gutter={[16, 16]}>
            <Col span={12}>
              <Card>
                <Statistic
                  title={t('engine.newsProcessed')}
                  value={status?.stats?.news_items || 0}
                  prefix={<FileTextOutlined />}
                />
              </Card>
            </Col>
            <Col span={12}>
              <Card>
                <Statistic
                  title={t('engine.analyzed')}
                  value={status?.stats?.analysis_results || 0}
                  prefix={<DashboardOutlined />}
                />
              </Card>
            </Col>
            <Col span={12}>
              <Card>
                <Statistic
                  title={t('engine.filtered')}
                  value={status?.stats?.filter_results || 0}
                  valueStyle={{ color: '#1677ff' }}
                />
              </Card>
            </Col>
            <Col span={12}>
              <Card>
                <Statistic
                  title={t('engine.researched')}
                  value={status?.stats?.research_reports || 0}
                  valueStyle={{ color: '#52c41a' }}
                />
              </Card>
            </Col>
          </Row>
        </Col>
      </Row>

      {/* Engine not running warning */}
      {!status?.is_running && (
        <Alert
          message={t('engine.notRunningTitle')}
          description={t('engine.notRunningDesc')}
          type="warning"
          showIcon
          style={{ marginTop: 16 }}
        />
      )}

      {/* Logs Modal */}
      <Modal
        title={t('engine.logs')}
        open={logsOpen}
        onCancel={() => setLogsOpen(false)}
        footer={
          <Button onClick={fetchLogs} icon={<ReloadOutlined />}>
            {t('engine.refresh')}
          </Button>
        }
        width={900}
      >
        <div
          style={{
            background: '#1e1e1e',
            color: '#d4d4d4',
            padding: 16,
            borderRadius: 8,
            maxHeight: 500,
            overflow: 'auto',
            fontFamily: 'monospace',
            fontSize: 12,
            lineHeight: 1.6,
          }}
        >
          {logs.length === 0 ? (
            <Text style={{ color: '#808080' }}>{t('engine.noLogs')}</Text>
          ) : (
            logs.map((line, i) => (
              <div
                key={i}
                style={{
                  color: line.includes('ERROR')
                    ? '#f44747'
                    : line.includes('WARNING')
                    ? '#cca700'
                    : line.includes('INFO')
                    ? '#6a9955'
                    : '#d4d4d4',
                }}
              >
                {line}
              </div>
            ))
          )}
        </div>
      </Modal>
    </div>
  )
}
