/**
 * Cost Dashboard — aggregated LLM spend across the revenue-modeling platform.
 *
 * Top-trading-firm discipline: you can't manage agent cost that you can't see.
 * This page surfaces:
 *   - My current quota (always visible, even for non-admins).
 *   - An aggregated cost view (group by industry / user / recipe / day).
 *   - For admins, the ability to bump a user's budget inline.
 */
import { useEffect, useMemo, useState } from 'react'
import {
  Alert, Button, Card, Col, InputNumber, Modal, Progress, Radio, Row,
  Select, Space, Statistic, Table, Tag, Typography, message,
} from 'antd'
import ReactECharts from 'echarts-for-react'
import { governanceApi } from '../services/modeling'
import { useAuthStore } from '../store/auth'

const { Title, Paragraph } = Typography

type GroupBy = 'industry' | 'user' | 'recipe' | 'day'

export default function CostDashboard() {
  const user = useAuthStore(s => s.user)
  const isAdmin = user?.role === 'admin' || user?.role === 'boss'

  const [quota, setQuota] = useState<Awaited<ReturnType<typeof governanceApi.myQuota>> | null>(null)
  const [groupBy, setGroupBy] = useState<GroupBy>('industry')
  const [sinceDays, setSinceDays] = useState(30)
  const [data, setData] = useState<Awaited<ReturnType<typeof governanceApi.costDashboard>> | null>(null)
  const [loading, setLoading] = useState(false)
  const [editUser, setEditUser] = useState<string | null>(null)
  const [editBudget, setEditBudget] = useState<number>(200)
  const [editCap, setEditCap] = useState<number>(15)

  const reload = async () => {
    setLoading(true)
    try {
      const [q, d] = await Promise.all([
        governanceApi.myQuota(),
        governanceApi.costDashboard({ group_by: groupBy, since_days: sinceDays }),
      ])
      setQuota(q)
      setData(d)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { reload() }, [groupBy, sinceDays])

  const spendPct = useMemo(() => {
    if (!quota) return 0
    return Math.min(100, Math.round(100 * quota.spent_this_month_usd / Math.max(quota.monthly_budget_usd, 1)))
  }, [quota])

  // Build bar-chart config for top-20 rows
  const chartOpt = useMemo(() => {
    const rows = (data?.rows || []).slice(0, 20).reverse()
    return {
      grid: { left: 120, right: 40, top: 10, bottom: 30 },
      xAxis: { type: 'value', axisLabel: { formatter: '${value}' } },
      yAxis: { type: 'category', data: rows.map(r => r.key) },
      tooltip: { trigger: 'axis', formatter: (p: any) => {
        const v = p?.[0]?.value || 0
        const key = p?.[0]?.name
        const row = rows.find(r => r.key === key)
        return `<div><strong>${key}</strong><br/>$${v.toFixed(2)} · ${row?.runs} runs · ${row?.total_tokens.toLocaleString()} tokens</div>`
      }},
      series: [{
        type: 'bar',
        data: rows.map(r => r.total_cost_usd),
        itemStyle: { color: '#6366f1' },
      }],
    }
  }, [data])

  const handleEditBudget = async () => {
    if (!editUser) return
    try {
      await governanceApi.patchUserBudget(editUser, {
        monthly_budget_usd: editBudget,
        run_cap_usd: editCap,
      })
      message.success('预算已更新')
      setEditUser(null)
      await reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  return (
    <div style={{ padding: 24, maxWidth: 1400, margin: '0 auto' }}>
      <div style={{ marginBottom: 16 }}>
        <Title level={3} style={{ marginBottom: 4 }}>💰 成本仪表盘</Title>
        <Paragraph style={{ color: '#64748b', marginBottom: 0 }}>
          收入拆分建模 LLM 成本概览 — 预估、实际、user/行业/recipe 分组、趋势。
        </Paragraph>
      </div>

      {/* My quota card — always visible */}
      {quota && (
        <Card style={{ marginBottom: 16 }} size="small">
          <Row gutter={16}>
            <Col span={6}>
              <Statistic
                title="我本月预算"
                value={quota.monthly_budget_usd}
                prefix="$"
                precision={0}
              />
            </Col>
            <Col span={6}>
              <Statistic
                title="已使用"
                value={quota.spent_this_month_usd}
                prefix="$"
                precision={2}
                valueStyle={{ color: quota.exceeded ? '#dc2626' : '#16a34a' }}
              />
            </Col>
            <Col span={6}>
              <Statistic
                title="剩余"
                value={quota.remaining_usd}
                prefix="$"
                precision={2}
              />
            </Col>
            <Col span={6}>
              <div style={{ marginBottom: 4, fontSize: 12, color: '#64748b' }}>使用率</div>
              <Progress
                percent={spendPct}
                status={quota.exceeded ? 'exception' : spendPct > 80 ? 'active' : 'normal'}
                strokeColor={quota.exceeded ? '#dc2626' : spendPct > 80 ? '#f59e0b' : '#22c55e'}
              />
            </Col>
          </Row>
          {quota.exceeded && (
            <Alert
              type="error"
              showIcon
              style={{ marginTop: 12 }}
              message="本月预算已耗尽"
              description="新的 recipe run 将被预检拒绝 (HTTP 402). 请联系管理员提升 llm_budget_usd_monthly."
            />
          )}
          {!quota.exceeded && quota.spent_this_month_usd >= quota.warn_threshold_usd && (
            <Alert
              type="warning"
              showIcon
              style={{ marginTop: 12 }}
              message={`已超过 80% 警戒线 ($${quota.warn_threshold_usd})`}
            />
          )}
        </Card>
      )}

      {/* Aggregation controls */}
      <Card style={{ marginBottom: 16 }} size="small">
        <Space size="middle" wrap>
          <Radio.Group
            value={groupBy}
            onChange={e => setGroupBy(e.target.value)}
            optionType="button"
            buttonStyle="solid"
          >
            <Radio.Button value="industry">按行业</Radio.Button>
            <Radio.Button value="recipe">按 Recipe</Radio.Button>
            <Radio.Button value="user" disabled={!isAdmin}>按用户</Radio.Button>
            <Radio.Button value="day">按日期</Radio.Button>
          </Radio.Group>
          <Select
            value={sinceDays}
            onChange={setSinceDays}
            style={{ width: 140 }}
            options={[
              { value: 7, label: '最近 7 天' },
              { value: 30, label: '最近 30 天' },
              { value: 90, label: '最近 90 天' },
              { value: 180, label: '最近 180 天' },
            ]}
          />
          <Button onClick={reload} loading={loading}>刷新</Button>
          {data && (
            <Tag color="blue" style={{ fontSize: 14, padding: '4px 10px' }}>
              窗口总成本 ${data.total_usd.toFixed(2)}
            </Tag>
          )}
          {data && data.scope === 'me' && (
            <Tag color="gold">仅展示您本人的数据 (非管理员)</Tag>
          )}
        </Space>
      </Card>

      {/* Bar chart */}
      {data && data.rows.length > 0 && (
        <Card style={{ marginBottom: 16 }} size="small" title="Top spenders">
          <ReactECharts option={chartOpt} style={{ height: 480 }} />
        </Card>
      )}

      {/* Table */}
      <Card size="small" title={`明细 (${data?.rows.length || 0})`}>
        <Table
          rowKey="key"
          loading={loading}
          dataSource={data?.rows || []}
          pagination={{ pageSize: 30 }}
          size="small"
          columns={[
            {
              title: groupBy === 'industry' ? '行业' : groupBy === 'user' ? 'User ID' : groupBy === 'recipe' ? 'Recipe' : '日期',
              dataIndex: 'key',
              render: (k: string) => <code>{k}</code>,
            },
            {
              title: '成本 (USD)',
              dataIndex: 'total_cost_usd',
              align: 'right' as const,
              sorter: (a, b) => a.total_cost_usd - b.total_cost_usd,
              defaultSortOrder: 'descend' as const,
              render: (v: number) => <strong>${v.toFixed(2)}</strong>,
            },
            {
              title: 'Runs',
              dataIndex: 'runs',
              align: 'right' as const,
              sorter: (a, b) => a.runs - b.runs,
            },
            {
              title: 'Tokens',
              dataIndex: 'total_tokens',
              align: 'right' as const,
              sorter: (a, b) => a.total_tokens - b.total_tokens,
              render: (v: number) => v.toLocaleString(),
            },
            {
              title: 'Cost / Run',
              align: 'right' as const,
              render: (_: any, r: any) => `$${(r.total_cost_usd / Math.max(r.runs, 1)).toFixed(2)}`,
            },
            ...(isAdmin && groupBy === 'user'
              ? [{
                  title: '操作',
                  align: 'right' as const,
                  render: (_: any, r: any) => (
                    <Button size="small" onClick={() => {
                      setEditUser(r.key)
                      setEditBudget(200)
                      setEditCap(15)
                    }}>调整预算</Button>
                  ),
                }]
              : []),
          ]}
        />
      </Card>

      <Modal
        open={!!editUser}
        onCancel={() => setEditUser(null)}
        onOk={handleEditBudget}
        title="调整用户 LLM 预算"
      >
        <div style={{ marginBottom: 12 }}>
          <div style={{ fontSize: 12, color: '#64748b' }}>User ID</div>
          <code>{editUser}</code>
        </div>
        <div style={{ marginBottom: 12 }}>
          <div style={{ fontSize: 12, color: '#64748b', marginBottom: 4 }}>月度预算 (USD)</div>
          <InputNumber
            value={editBudget}
            onChange={v => setEditBudget(Number(v ?? 200))}
            style={{ width: '100%' }}
            min={0}
            max={10000}
          />
        </div>
        <div>
          <div style={{ fontSize: 12, color: '#64748b', marginBottom: 4 }}>单次运行硬上限 (USD)</div>
          <InputNumber
            value={editCap}
            onChange={v => setEditCap(Number(v ?? 15))}
            style={{ width: '100%' }}
            min={0}
            max={1000}
          />
        </div>
      </Modal>
    </div>
  )
}
