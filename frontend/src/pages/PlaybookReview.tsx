/**
 * Playbook + PendingLesson review page (admin/boss).
 *
 * Two tabs: "Industry packs" shows the rendered markdown for each pack's
 * overview/lessons/rules. "待审批" shows the PendingLesson queue — admin
 * approves / rejects / archives.
 */
import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Alert, Button, Card, Empty, Input, List, Modal, Space, Tabs, Tag,
  Typography, message,
} from 'antd'
import {
  ArrowLeftOutlined, CheckCircleOutlined, CloseCircleOutlined,
  InboxOutlined, ReloadOutlined, SyncOutlined,
} from '@ant-design/icons'
import {
  playbookApi, type Lesson, type PackInfo, type PendingLesson,
} from '../services/modeling'
import MarkdownRenderer from '../components/MarkdownRenderer'

const { TextArea } = Input

export default function PlaybookReview() {
  const nav = useNavigate()
  const [packs, setPacks] = useState<PackInfo[]>([])
  const [pending, setPending] = useState<PendingLesson[]>([])
  const [selectedPack, setSelectedPack] = useState<string | null>(null)
  const [packContent, setPackContent] = useState<Record<string, string>>({})
  const [lessons, setLessons] = useState<Lesson[]>([])
  const [consolidating, setConsolidating] = useState(false)
  const [reviewing, setReviewing] = useState<PendingLesson | null>(null)
  const [editedBody, setEditedBody] = useState('')

  const reload = async () => {
    try {
      const [p, l] = await Promise.all([
        playbookApi.listPacks(),
        playbookApi.listPending('pending'),
      ])
      setPacks(p)
      setPending(l)
      if (p.length > 0 && !selectedPack) {
        setSelectedPack(p[0].slug)
      }
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  useEffect(() => { reload() }, [])

  useEffect(() => {
    if (!selectedPack) return
    playbookApi.readPack(selectedPack).then(setPackContent).catch(() => {})
    playbookApi.listLessons(selectedPack).then(setLessons).catch(() => {})
  }, [selectedPack])

  const review = async (action: 'approve' | 'reject' | 'archive') => {
    if (!reviewing) return
    try {
      await playbookApi.review(reviewing.id, {
        action,
        edited_body: editedBody !== reviewing.body ? editedBody : undefined,
      })
      message.success(`已 ${action === 'approve' ? '批准' : action === 'reject' ? '拒绝' : '归档'}`)
      setReviewing(null)
      reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const triggerConsolidation = async () => {
    setConsolidating(true)
    try {
      const res = await playbookApi.consolidate()
      message.success(`蒸馏完成: ${res.proposals} 个新 lesson 提案`)
      reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setConsolidating(false)
    }
  }

  return (
    <div style={{ padding: '16px 24px' }}>
      <Card title={<Space>
        <Button icon={<ArrowLeftOutlined />} onClick={() => nav('/modeling')}>返回</Button>
        <span style={{ fontSize: 17 }}>Playbook 审查</span>
      </Space>} extra={
        <Space>
          <Button icon={<ReloadOutlined />} onClick={reload}>刷新</Button>
          <Button icon={<SyncOutlined spin={consolidating} />} onClick={triggerConsolidation} loading={consolidating}>
            手动蒸馏 (consolidate feedback)
          </Button>
        </Space>
      }>
        <Tabs
          items={[
            {
              key: 'packs',
              label: '行业 Playbook',
              children: (
                <div style={{ display: 'flex', gap: 16 }}>
                  <div style={{ width: 240 }}>
                    <List
                      bordered
                      dataSource={packs}
                      renderItem={(p) => (
                        <List.Item
                          onClick={() => setSelectedPack(p.slug)}
                          style={{
                            cursor: 'pointer',
                            background: p.slug === selectedPack ? '#eff6ff' : undefined,
                          }}
                        >
                          <List.Item.Meta
                            title={p.name}
                            description={
                              <Space size={4}>
                                <Tag>{p.slug}</Tag>
                                <Tag>{p.recipe_count} recipes</Tag>
                              </Space>
                            }
                          />
                        </List.Item>
                      )}
                    />
                  </div>
                  <div style={{ flex: 1, maxHeight: 700, overflow: 'auto' }}>
                    {selectedPack && packContent ? (
                      <Tabs
                        items={[
                          { key: 'overview', label: '概览', children: <MarkdownRenderer content={packContent['overview.md'] || ''} /> },
                          {
                            key: 'lessons',
                            label: `Lessons (${lessons.length})`,
                            children: <MarkdownRenderer content={packContent['lessons.md'] || ''} />,
                          },
                          { key: 'rules', label: 'Rules', children: <MarkdownRenderer content={packContent['rules.md'] || ''} /> },
                        ]}
                      />
                    ) : <Empty />}
                  </div>
                </div>
              ),
            },
            {
              key: 'pending',
              label: `待审批 (${pending.length})`,
              children: pending.length === 0 ? (
                <Empty description="当前没有待审批 lesson ✅" style={{ padding: 48 }} />
              ) : (
                <List
                  dataSource={pending}
                  renderItem={(l) => (
                    <List.Item actions={[
                      <Button key="approve" type="primary" onClick={() => {
                        setReviewing(l)
                        setEditedBody(l.body)
                      }}>审查</Button>,
                    ]}>
                      <List.Item.Meta
                        title={<Space>
                          <strong>{l.title}</strong>
                          <Tag>{l.lesson_id}</Tag>
                          <Tag color="purple">{l.industry}</Tag>
                          <span style={{ color: '#64748b', fontSize: 12 }}>
                            {new Date(l.created_at).toLocaleString('zh-CN')}
                          </span>
                        </Space>}
                        description={l.scenario}
                      />
                    </List.Item>
                  )}
                />
              ),
            },
          ]}
        />
      </Card>

      {reviewing && (
        <Modal
          title={<Space>
            <strong>{reviewing.title}</strong>
            <Tag>{reviewing.lesson_id}</Tag>
            <Tag color="purple">{reviewing.industry}</Tag>
          </Space>}
          open
          width={760}
          onCancel={() => setReviewing(null)}
          footer={[
            <Button key="archive" icon={<InboxOutlined />} onClick={() => review('archive')}>归档</Button>,
            <Button key="reject" danger icon={<CloseCircleOutlined />} onClick={() => review('reject')}>拒绝</Button>,
            <Button key="approve" type="primary" icon={<CheckCircleOutlined />} onClick={() => review('approve')}>批准 (写入 lessons.md)</Button>,
          ]}
        >
          <Alert
            type="info"
            message="批准后，lesson 将追加到对应行业的 lessons.md。Agent 在下次建模时会读取并遵循。"
            style={{ marginBottom: 12 }}
          />
          <div style={{ marginBottom: 8 }}><strong>场景</strong>: {reviewing.scenario}</div>
          <div style={{ marginBottom: 8 }}><strong>观察</strong>: {reviewing.observation}</div>
          <div style={{ marginBottom: 8 }}><strong>规则</strong>: {reviewing.rule}</div>
          <div style={{ marginBottom: 4 }}><strong>完整 body (可编辑)</strong>:</div>
          <TextArea
            rows={10}
            value={editedBody}
            onChange={(e) => setEditedBody(e.target.value)}
            style={{ fontFamily: 'monospace', fontSize: 12 }}
          />
        </Modal>
      )}
    </div>
  )
}
