/**
 * RecipeABCompare — side-by-side comparison of an A/B recipe run.
 *
 * Researcher picks a winner per metric or sets a preference; result is
 * sent as feedback so the consolidator can learn prompts that work.
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import {
  Alert, Button, Card, Space, Table, Tabs, Tag, Typography, message,
} from 'antd'
import { ArrowLeftOutlined, CheckCircleOutlined } from '@ant-design/icons'
import api from '../services/api'
import type { ModelCell, RecipeRun } from '../services/modeling'

const { Paragraph } = Typography

interface ABGroup {
  group: 'A' | 'B'
  run: RecipeRun
  cells: ModelCell[]
}

export default function RecipeABCompare() {
  const { session = '' } = useParams()
  const nav = useNavigate()
  const [groups, setGroups] = useState<ABGroup[]>([])
  const [loading, setLoading] = useState(false)
  const [winner, setWinner] = useState<'A' | 'B' | null>(null)

  const reload = useCallback(async () => {
    setLoading(true)
    try {
      const res = await api.get(`/models/ab/${session}`)
      setGroups(res.data.groups || [])
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setLoading(false)
    }
  }, [session])

  useEffect(() => { reload() }, [reload])
  useEffect(() => {
    // Light polling while any run is still in progress
    const interval = setInterval(() => {
      const running = groups.some(g => g.run.status === 'running' || g.run.status === 'pending')
      if (running) reload()
    }, 3000)
    return () => clearInterval(interval)
  }, [groups, reload])

  const a = groups.find(g => g.group === 'A')
  const b = groups.find(g => g.group === 'B')

  const rows = useMemo(() => {
    if (!a || !b) return []
    const paths = new Set<string>()
    a.cells.forEach(c => paths.add(c.path))
    b.cells.forEach(c => paths.add(c.path))
    const aMap = new Map(a.cells.map(c => [c.path, c]))
    const bMap = new Map(b.cells.map(c => [c.path, c]))
    return [...paths].sort().map(p => {
      const ca = aMap.get(p)
      const cb = bMap.get(p)
      const av = ca?.value ?? null
      const bv = cb?.value ?? null
      const diffPct = av != null && bv != null && av !== 0
        ? (bv - av) / Math.abs(av)
        : null
      return { path: p, a: ca, b: cb, diffPct }
    })
  }, [a, b])

  const declare = async (w: 'A' | 'B') => {
    setWinner(w)
    try {
      const winnerGroup = groups.find(g => g.group === w)
      if (!winnerGroup) return
      await api.post(`/models/${winnerGroup.run.model_id}/feedback`, {
        event_type: 'ab_winner',
        payload: { session, winner: w, recipe_id: winnerGroup.run.recipe_id },
      })
      message.success(`已记录 ${w} 胜选 — 会纳入下周 lesson 蒸馏`)
    } catch (e: any) {
      message.warning('反馈保存失败: ' + String(e))
    }
  }

  return (
    <div style={{ padding: 16 }}>
      <Card size="small" style={{ marginBottom: 12 }}>
        <Space>
          <Button icon={<ArrowLeftOutlined />} onClick={() => nav(-1)}>返回</Button>
          <strong>A/B 对比 · session={session}</strong>
          {a && <Tag color="blue">A: {a.run.status}</Tag>}
          {b && <Tag color="purple">B: {b.run.status}</Tag>}
          <Button size="small" onClick={reload} loading={loading}>刷新</Button>
          <Button size="small" type="primary" icon={<CheckCircleOutlined />}
                  disabled={!a || !b} onClick={() => declare('A')}>A 胜</Button>
          <Button size="small" type="primary" icon={<CheckCircleOutlined />}
                  disabled={!a || !b} onClick={() => declare('B')}>B 胜</Button>
          {winner && <Tag color="green">已记录 {winner} 胜选</Tag>}
        </Space>
      </Card>

      {(!a || !b) && <Alert type="info" message="正在加载 A/B 运行…" showIcon />}

      {a && b && (
        <Tabs
          items={[
            {
              key: 'diff', label: '差异视图',
              children: (
                <Table
                  size="small"
                  rowKey="path"
                  dataSource={rows}
                  pagination={{ pageSize: 50 }}
                  columns={[
                    { title: '路径', dataIndex: 'path', key: 'path', width: 320,
                      render: (v: string) => <code style={{ fontSize: 11 }}>{v}</code> },
                    {
                      title: 'A 值', key: 'a', width: 180,
                      render: (_: any, r: any) => {
                        if (!r.a) return <span style={{ color: '#cbd5e1' }}>—</span>
                        return <Space>
                          <strong>{fmt(r.a)}</strong>
                          <Tag color={confColor(r.a.confidence)}>{r.a.confidence}</Tag>
                        </Space>
                      },
                    },
                    {
                      title: 'B 值', key: 'b', width: 180,
                      render: (_: any, r: any) => {
                        if (!r.b) return <span style={{ color: '#cbd5e1' }}>—</span>
                        return <Space>
                          <strong>{fmt(r.b)}</strong>
                          <Tag color={confColor(r.b.confidence)}>{r.b.confidence}</Tag>
                        </Space>
                      },
                    },
                    {
                      title: '差异 %', dataIndex: 'diffPct', key: 'diff', width: 120,
                      render: (v: number | null) => {
                        if (v == null) return '—'
                        const color = Math.abs(v) < 0.05 ? 'default' : Math.abs(v) < 0.25 ? 'orange' : 'red'
                        return <Tag color={color}>{(v * 100).toFixed(1)}%</Tag>
                      },
                    },
                    {
                      title: '来源A → 来源B', key: 'src', width: 200,
                      render: (_: any, r: any) =>
                        <span style={{ fontSize: 11 }}>
                          {r.a?.source_type || '—'} → {r.b?.source_type || '—'}
                        </span>,
                    },
                  ]}
                />
              ),
            },
            {
              key: 'runs', label: '两次 Run 元信息',
              children: (
                <Space direction="vertical" style={{ width: '100%' }}>
                  {[a, b].map(g => (
                    <Card key={g.group} size="small" title={`${g.group} · recipe=${g.run.recipe_id.slice(0, 8)}`}>
                      <Paragraph>
                        <Tag>{g.run.status}</Tag>
                        <span>tokens: {g.run.total_tokens}, cost: ${g.run.total_cost_usd?.toFixed(2)}</span>
                      </Paragraph>
                      <pre style={{ fontSize: 10, background: '#f1f5f9', padding: 8 }}>
                        {JSON.stringify(g.run.step_results, null, 2).slice(0, 1500)}
                      </pre>
                    </Card>
                  ))}
                </Space>
              ),
            },
          ]}
        />
      )}
    </div>
  )
}

function fmt(c: ModelCell): string {
  if (c.value_text && c.value_type === 'text') return c.value_text
  if (c.value == null) return '—'
  if (c.value_type === 'percent') return (c.value * 100).toFixed(1) + '%'
  if (c.value_type === 'currency') return c.value.toLocaleString('en-US', { maximumFractionDigits: 2 })
  return c.value.toLocaleString('en-US', { maximumFractionDigits: 4 })
}

function confColor(c: string): string {
  return c === 'HIGH' ? 'green' : c === 'MEDIUM' ? 'orange' : 'red'
}
