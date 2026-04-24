/**
 * ExpertCallRequests — queue of expert-call requests the VERIFY_AND_ASK step
 * generated when a model cell lacked external citations. Researchers claim,
 * schedule, and mark complete; completion re-enqueues cells for re-run.
 */
import { useCallback, useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Button, Card, Input, Modal, Space, Table, Tag, Typography, message,
} from 'antd'
import { ArrowLeftOutlined, CheckCircleOutlined, PhoneOutlined } from '@ant-design/icons'
import api from '../services/api'

const { Paragraph } = Typography

interface ExpertCall {
  id: string
  model_id: string
  cell_path: string | null
  ticker: string
  topic: string
  questions: string[]
  rationale: string
  status: 'open' | 'scheduled' | 'completed' | 'cancelled'
  requested_by: string | null
  assigned_to: string | null
  interview_doc_id: string | null
  created_at: string
  resolved_at: string | null
}

export default function ExpertCallRequests() {
  const nav = useNavigate()
  const [rows, setRows] = useState<ExpertCall[]>([])
  const [loading, setLoading] = useState(false)
  const [statusFilter, setStatusFilter] = useState('open')
  const [completingId, setCompletingId] = useState<string | null>(null)
  const [interviewId, setInterviewId] = useState('')

  const reload = useCallback(async () => {
    setLoading(true)
    try {
      const res = await api.get('/expert-calls', { params: { status: statusFilter || undefined } })
      setRows(res.data || [])
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setLoading(false)
    }
  }, [statusFilter])

  useEffect(() => { reload() }, [reload])

  const updateStatus = async (id: string, status: string) => {
    try {
      await api.patch(`/expert-calls/${id}`, { status })
      message.success('已更新')
      reload()
    } catch (e: any) {
      message.error(String(e))
    }
  }

  const submitComplete = async () => {
    if (!completingId || !interviewId) return
    try {
      await api.post(`/expert-calls/${completingId}/mark-completed`, null, {
        params: { interview_doc_id: interviewId },
      })
      message.success('已标记为完成，相关 cell 会自动重跑')
      setCompletingId(null)
      setInterviewId('')
      reload()
    } catch (e: any) {
      message.error(String(e))
    }
  }

  return (
    <div style={{ padding: 16 }}>
      <Card size="small" style={{ marginBottom: 12 }}>
        <Space>
          <Button icon={<ArrowLeftOutlined />} onClick={() => nav('/modeling')}>返回</Button>
          <strong>专家访谈请求队列</strong>
          <Paragraph style={{ margin: 0, color: '#64748b' }}>
            VERIFY_AND_ASK 发现没有证据支撑的关键 cell → 自动生成访谈请求.
            约上专家后，填入访谈记录 ID 标记完成，相关 cell 会重跑.
          </Paragraph>
          <Space.Compact>
            {['open', 'scheduled', 'completed', ''].map(s => (
              <Button key={s || 'all'} size="small"
                      type={statusFilter === s ? 'primary' : 'default'}
                      onClick={() => setStatusFilter(s)}>{s || '全部'}</Button>
            ))}
          </Space.Compact>
        </Space>
      </Card>

      <Table
        size="small"
        rowKey="id"
        loading={loading}
        dataSource={rows}
        pagination={{ pageSize: 30 }}
        columns={[
          { title: 'Ticker', dataIndex: 'ticker', key: 'ticker', width: 100 },
          { title: '主题', dataIndex: 'topic', key: 'topic', ellipsis: true },
          { title: 'cell', dataIndex: 'cell_path', key: 'cell_path', width: 280,
            render: (v: string) => <code style={{ fontSize: 11 }}>{v}</code> },
          { title: '问题', key: 'q', render: (_: any, r: ExpertCall) =>
              <Tag>{r.questions?.length || 0} 条</Tag> },
          {
            title: '状态', dataIndex: 'status', key: 'status', width: 100,
            render: (s: string) => (
              <Tag color={s === 'completed' ? 'green' : s === 'open' ? 'orange' : s === 'cancelled' ? 'default' : 'blue'}>
                {s}
              </Tag>
            ),
          },
          { title: '创建时间', dataIndex: 'created_at', key: 'created_at', width: 160,
            render: (v: string) => new Date(v).toLocaleString('zh-CN') },
          {
            title: '操作', key: 'actions', width: 240,
            render: (_: any, r: ExpertCall) => (
              <Space>
                <Button size="small" onClick={() => nav(`/modeling/${r.model_id}`)}>查看模型</Button>
                {r.status === 'open' && (
                  <Button size="small" icon={<PhoneOutlined />}
                          onClick={() => updateStatus(r.id, 'scheduled')}>排期</Button>
                )}
                {r.status !== 'completed' && r.status !== 'cancelled' && (
                  <Button size="small" type="primary" icon={<CheckCircleOutlined />}
                          onClick={() => setCompletingId(r.id)}>完成</Button>
                )}
              </Space>
            ),
          },
        ]}
        expandable={{
          expandedRowRender: (r: ExpertCall) => (
            <div style={{ padding: 8 }}>
              <Paragraph><strong>原因:</strong> {r.rationale}</Paragraph>
              <Paragraph>
                <strong>问题:</strong>
                <ul>{(r.questions || []).map((q, i) => <li key={i}>{q}</li>)}</ul>
              </Paragraph>
              {r.interview_doc_id && (
                <Paragraph>
                  <strong>访谈记录:</strong> <code>{r.interview_doc_id}</code>
                </Paragraph>
              )}
            </div>
          ),
        }}
      />

      <Modal
        open={!!completingId}
        title="标记访谈完成"
        onOk={submitComplete}
        onCancel={() => { setCompletingId(null); setInterviewId('') }}
        okText="保存并触发重跑"
      >
        <Paragraph type="secondary">
          填入 KB 中对应访谈记录的 doc_id (可从 Third Bridge/Meritco 抓取完毕后的 _id 字段)。
          保存后相关 cell 自动重跑以吸收新证据.
        </Paragraph>
        <Input placeholder="例 meritco:67abc123..."
               value={interviewId}
               onChange={e => setInterviewId(e.target.value)} />
      </Modal>
    </div>
  )
}
