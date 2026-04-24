/**
 * CellInspector — right-side drawer showing everything about a cell.
 *
 * Design goal: a researcher should be able to trust the value by reading
 * this panel alone. Every claim is visible: source, confidence, reasoning,
 * alternatives, full provenance trace, debate opinions, edit history.
 */
import { useEffect, useState } from 'react'
import {
  Alert, Button, Descriptions, Divider, Drawer, Form, Input, InputNumber,
  Modal, Select, Space, Switch, Tabs, Tag, Timeline, Tooltip, Typography, message,
} from 'antd'
import { LockOutlined, UnlockOutlined, InfoCircleOutlined } from '@ant-design/icons'
import {
  governanceApi, modelingApi, type Confidence, type DebateOpinion, type ModelCell,
  type ProvenanceTrace, type SourceType,
} from '../../services/modeling'
import ReasoningTimeline from './ReasoningTimeline'

const { Paragraph } = Typography

const SOURCE_TYPES: SourceType[] = [
  'historical', 'guidance', 'expert', 'inferred', 'assumption', 'derived',
]
const CONFIDENCE: Confidence[] = ['HIGH', 'MEDIUM', 'LOW']

interface Props {
  cell: ModelCell | null
  modelId: string
  /** Model's industry slug — used to fetch the industry-specific calibration map. */
  industry?: string
  onClose: () => void
  onUpdate: (patch: any) => void
}

interface CalibrationBucket {
  label: Confidence
  samples: number
  mae: number
  p50_err: number
  p90_err: number
  hit_rate: number
  expected_mae: number
  calibrated_label: Confidence
}

export default function CellInspector({ cell, modelId, industry, onClose, onUpdate }: Props) {
  const [editing, setEditing] = useState(false)
  const [form] = Form.useForm()
  const [history, setHistory] = useState<any[]>([])
  const [debate, setDebate] = useState<DebateOpinion[]>([])
  const [provenance, setProvenance] = useState<ProvenanceTrace | null>(null)
  const [calibration, setCalibration] = useState<CalibrationBucket[]>([])

  useEffect(() => {
    if (!cell) return
    setEditing(false)
    form.resetFields()
    modelingApi.cellHistory(modelId, cell.id).then(setHistory).catch(() => {})
    modelingApi.listDebate(modelId, cell.id).then(setDebate).catch(() => {})
    if (cell.provenance_trace_id) {
      modelingApi.getProvenance(modelId, cell.provenance_trace_id)
        .then(setProvenance).catch(() => setProvenance(null))
    } else {
      setProvenance(null)
    }
  }, [cell?.id])

  // Calibration map — loaded once per industry change (not per cell).
  useEffect(() => {
    if (!industry) {
      setCalibration([])
      return
    }
    governanceApi.calibration(industry, 365)
      .then(buckets => setCalibration(buckets || []))
      .catch(() => setCalibration([]))
  }, [industry])

  const bucketForCell = calibration.find(b => b.label === cell?.confidence)
  const calibratedLabel = bucketForCell?.calibrated_label
  const isDowngrade = bucketForCell && calibratedLabel !== bucketForCell.label

  if (!cell) return null

  const handleSave = async () => {
    try {
      const v = await form.validateFields()
      onUpdate({
        value: v.value,
        value_text: v.value_text,
        formula: v.formula || null,
        source_type: v.source_type,
        confidence: v.confidence,
        notes: v.notes,
        edit_reason: v.edit_reason || 'manual inspector edit',
      })
      setEditing(false)
    } catch (e) { /* validation errored; stay */ }
  }

  const handlePickAlt = async (idx: number) => {
    Modal.confirm({
      title: '采用此来源的值作为主值?',
      content: '原主值将作为备选保留。',
      onOk: () => onUpdate({
        pick_alternative_idx: idx,
        edit_reason: `pick alternative ${idx}`,
      }),
    })
  }

  return (
    <Drawer
      title={<Space>
        <code>{cell.path}</code>
        {cell.locked_by_human && <LockOutlined />}
        {cell.review_status === 'flagged' && <Tag color="orange">FLAGGED</Tag>}
      </Space>}
      open={!!cell}
      onClose={onClose}
      width={560}
      extra={
        <Space>
          {editing ? (
            <>
              <Button onClick={() => setEditing(false)}>取消</Button>
              <Button type="primary" onClick={handleSave}>保存</Button>
            </>
          ) : (
            <>
              <Button size="small" icon={cell.locked_by_human ? <UnlockOutlined /> : <LockOutlined />}
                onClick={() => onUpdate({
                  locked_by_human: !cell.locked_by_human,
                  edit_reason: cell.locked_by_human ? 'unlock' : 'lock',
                })}
              >
                {cell.locked_by_human ? '解锁' : '锁定'}
              </Button>
              <Button type="primary" onClick={() => {
                form.setFieldsValue({
                  value: cell.value,
                  value_text: cell.value_text,
                  formula: cell.formula,
                  source_type: cell.source_type,
                  confidence: cell.confidence,
                  notes: cell.notes,
                })
                setEditing(true)
              }}>编辑</Button>
            </>
          )}
        </Space>
      }
    >
      <Tabs
        items={[
          {
            key: 'values',
            label: '主值',
            children: editing ? (
              <Form form={form} layout="vertical">
                {cell.value_type === 'text' ? (
                  <Form.Item name="value_text" label="值 (text)">
                    <Input.TextArea rows={4} />
                  </Form.Item>
                ) : (
                  <Form.Item name="value" label={`值 (${cell.unit || cell.value_type})`}>
                    <InputNumber style={{ width: '100%' }} />
                  </Form.Item>
                )}
                <Form.Item name="formula" label="公式 (可选;填了会覆盖值)">
                  <Input placeholder="=segment.A.rev.FY26 * segment.A.margin.FY26" />
                </Form.Item>
                <Form.Item name="source_type" label="Source type">
                  <Select options={SOURCE_TYPES.map(s => ({ value: s, label: s }))} />
                </Form.Item>
                <Form.Item name="confidence" label="Confidence">
                  <Select options={CONFIDENCE.map(c => ({ value: c, label: c }))} />
                </Form.Item>
                <Form.Item name="notes" label="Notes">
                  <Input.TextArea rows={3} />
                </Form.Item>
                <Form.Item name="edit_reason" label="修改原因 (会记入反馈)">
                  <Input placeholder="为何要改此值?" />
                </Form.Item>
              </Form>
            ) : (
              <>
                <Descriptions column={1} size="small" bordered>
                  <Descriptions.Item label="值">
                    <strong style={{ fontSize: 18 }}>
                      {cell.value_text || cell.value?.toLocaleString() || '—'}
                      {cell.unit && <span style={{ color: '#64748b', fontSize: 13 }}> {cell.unit}</span>}
                    </strong>
                  </Descriptions.Item>
                  {cell.formula && (
                    <Descriptions.Item label="公式">
                      <code>{cell.formula}</code>
                    </Descriptions.Item>
                  )}
                  <Descriptions.Item label="Source type">
                    <Tag>{cell.source_type}</Tag>
                  </Descriptions.Item>
                  <Descriptions.Item label="Confidence">
                    <Space size={6} wrap>
                      <Tag color={cell.confidence === 'HIGH' ? 'green' : cell.confidence === 'MEDIUM' ? 'orange' : 'red'}>
                        {cell.confidence}
                      </Tag>
                      {isDowngrade && calibratedLabel && (
                        <Tooltip
                          title={(
                            <div style={{ fontSize: 12 }}>
                              <div>历史回测 (过去 365 天): {bucketForCell.samples} 个样本</div>
                              <div>实际 MAE: {(bucketForCell.mae * 100).toFixed(1)}%</div>
                              <div>预期 MAE: {(bucketForCell.expected_mae * 100).toFixed(1)}%</div>
                              <div>Hit rate: {(bucketForCell.hit_rate * 100).toFixed(0)}%</div>
                              <div style={{ marginTop: 6 }}>
                                由于实际误差超出该区间预期 1.5 倍, 该 confidence 已降级.
                              </div>
                            </div>
                          )}
                        >
                          <Tag icon={<InfoCircleOutlined />}
                            color={calibratedLabel === 'LOW' ? 'red' : calibratedLabel === 'MEDIUM' ? 'orange' : 'green'}>
                            实际表现 → {calibratedLabel}
                          </Tag>
                        </Tooltip>
                      )}
                      {bucketForCell && !isDowngrade && (
                        <Tooltip
                          title={(
                            <div style={{ fontSize: 12 }}>
                              <div>{bucketForCell.samples} 个历史样本</div>
                              <div>MAE {(bucketForCell.mae * 100).toFixed(1)}% (预期 {(bucketForCell.expected_mae * 100).toFixed(1)}%)</div>
                              <div>Hit rate {(bucketForCell.hit_rate * 100).toFixed(0)}%</div>
                            </div>
                          )}
                        >
                          <Tag icon={<InfoCircleOutlined />} color="blue">回测校准: 一致</Tag>
                        </Tooltip>
                      )}
                    </Space>
                    {cell.confidence_reason && <div style={{ marginTop: 4, color: '#64748b', fontSize: 12 }}>{cell.confidence_reason}</div>}
                  </Descriptions.Item>
                  <Descriptions.Item label="Label">{cell.label}</Descriptions.Item>
                  <Descriptions.Item label="Period">{cell.period}</Descriptions.Item>
                  <Descriptions.Item label="Unit">{cell.unit}</Descriptions.Item>
                </Descriptions>

                {cell.notes && (
                  <>
                    <Divider orientation="left">Notes</Divider>
                    <Paragraph style={{ whiteSpace: 'pre-wrap' }}>{cell.notes}</Paragraph>
                  </>
                )}

                {cell.alternative_values.length > 0 && (
                  <>
                    <Divider orientation="left">备选来源 ({cell.alternative_values.length})</Divider>
                    {cell.alternative_values.map((alt, i) => (
                      <div key={i} style={{
                        padding: 10, marginBottom: 8, border: '1px solid #e2e8f0', borderRadius: 4,
                      }}>
                        <Space size="small">
                          <strong>{alt.value ?? alt.value_text ?? '—'}</strong>
                          <Tag>{alt.source}</Tag>
                          {alt.label && <span style={{ color: '#64748b', fontSize: 12 }}>({alt.label})</span>}
                          <Button size="small" type="link" onClick={() => handlePickAlt(i)}>↩ 选为主值</Button>
                        </Space>
                        {alt.notes && <div style={{ color: '#64748b', fontSize: 12, marginTop: 4 }}>{alt.notes}</div>}
                      </div>
                    ))}
                  </>
                )}

                {cell.citations.length > 0 && (
                  <>
                    <Divider orientation="left">📚 Citations ({cell.citations.length})</Divider>
                    {cell.citations.map((c, i) => {
                      const kbDocId = (c as any).source_id || (c as any).doc_id
                      const kbViewerHref = kbDocId
                        ? `/modeling/kb-viewer?doc_id=${encodeURIComponent(kbDocId)}${c.snippet ? `&snippet=${encodeURIComponent(c.snippet.slice(0, 300))}` : ''}`
                        : null
                      return (
                        <div key={i} style={{
                          padding: 10, marginBottom: 6, background: '#f8fafc', borderRadius: 4,
                        }}>
                          <div>
                            <Tag>{`[${c.index ?? i + 1}]`}</Tag>
                            {kbViewerHref ? (
                              <a href={kbViewerHref} target="_blank" rel="noreferrer" title="打开 KB 原文并高亮片段">
                                📖 {c.title || kbDocId}
                              </a>
                            ) : c.url ? (
                              <a href={c.url} target="_blank" rel="noreferrer">{c.title}</a>
                            ) : (
                              <strong>{c.title}</strong>
                            )}
                            {c.date && <span style={{ color: '#64748b', marginLeft: 8, fontSize: 12 }}>{c.date}</span>}
                          </div>
                          {c.snippet && (
                            <div style={{ color: '#475569', fontSize: 12, marginTop: 4 }}>
                              {c.snippet}
                            </div>
                          )}
                        </div>
                      )
                    })}
                  </>
                )}
              </>
            ),
          },
          {
            key: 'provenance',
            label: `推理链 (${provenance?.steps?.length || 0})`,
            children: <ReasoningTimeline trace={provenance} citations={cell.citations as any} />,
          },
          {
            key: 'debate',
            label: `Debate (${debate.length})`,
            children: debate.length === 0 ? (
              <Alert type="info" message="No cross-model debate ran for this cell (either it's not critical, or a debate hasn't triggered)." />
            ) : (
              <>
                {debate.map((op) => (
                  <div key={op.id} style={{
                    padding: 12, marginBottom: 8, border: '1px solid #e2e8f0', borderRadius: 4,
                  }}>
                    <Space>
                      <Tag color={op.role === 'drafter' ? 'blue' : op.role === 'verifier' ? 'purple' : 'gold'}>
                        {op.role}
                      </Tag>
                      <code>{op.model_key}</code>
                      <strong>{op.value?.toLocaleString()}</strong>
                      <Tag>{op.confidence}</Tag>
                    </Space>
                    {op.reasoning && <div style={{ marginTop: 6, color: '#475569', fontSize: 13 }}>{op.reasoning}</div>}
                  </div>
                ))}
              </>
            ),
          },
          {
            key: 'history',
            label: `History (${history.length})`,
            children: history.length === 0 ? (
              <Alert type="info" message="No edit history yet." />
            ) : (
              <Timeline>
                {history.map((h) => (
                  <Timeline.Item key={h.id}>
                    <Space direction="vertical" size={4}>
                      <Space>
                        <strong>{h.value ?? h.value_text ?? 'null'}</strong>
                        <Tag>{h.source_type}</Tag>
                        <Tag>{h.confidence}</Tag>
                        <span style={{ color: '#64748b', fontSize: 12 }}>{new Date(h.created_at).toLocaleString('zh-CN')}</span>
                      </Space>
                      {h.formula && <code style={{ fontSize: 12 }}>{h.formula}</code>}
                      {h.edit_reason && <span style={{ color: '#64748b', fontSize: 12 }}>“{h.edit_reason}”</span>}
                    </Space>
                  </Timeline.Item>
                ))}
              </Timeline>
            ),
          },
        ]}
      />
    </Drawer>
  )
}
