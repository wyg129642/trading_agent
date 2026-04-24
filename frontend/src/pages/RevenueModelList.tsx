/**
 * List + create Revenue Models.
 *
 * Entry point for the "收入拆分建模" workflow. Researchers land here,
 * see their existing models, and can create a new one for any ticker.
 */
import { useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Button, Card, Empty, Form, Input, List, Modal, Select, Space, Spin,
  Table, Tag, Tooltip, message,
} from 'antd'
import { LineChartOutlined, PlusOutlined, ReloadOutlined } from '@ant-design/icons'
import api from '../services/api'
import { modelingApi, playbookApi, type PackInfo, type RevenueModel } from '../services/modeling'

interface StockSuggestion {
  code: string
  name: string
  market: string
  label?: string
  rank?: number
}

// Market label → industry pack hint (first-match wins; user can still override)
const MARKET_TO_PACK: Record<string, string> = {
  // Optical modules is our only pack so far. Later packs will register their
  // ticker patterns via pack.yaml and we'll resolve by pattern match.
}

export default function RevenueModelList() {
  const nav = useNavigate()
  const [models, setModels] = useState<RevenueModel[]>([])
  const [packs, setPacks] = useState<PackInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [creating, setCreating] = useState(false)
  const [form] = Form.useForm()
  const [stockOptions, setStockOptions] = useState<StockSuggestion[]>([])
  const [stockSearching, setStockSearching] = useState(false)
  const stockTimerRef = useRef<number | null>(null)

  const debouncedStockSearch = (q: string) => {
    if (stockTimerRef.current) {
      window.clearTimeout(stockTimerRef.current); stockTimerRef.current = null
    }
    if (!q || !q.trim()) { setStockOptions([]); return }
    stockTimerRef.current = window.setTimeout(async () => {
      setStockSearching(true)
      try {
        const res = await api.get<StockSuggestion[]>(
          '/stock/suggest', { params: { q: q.trim(), limit: 10 } },
        )
        setStockOptions(Array.isArray(res.data) ? res.data : [])
      } catch {
        setStockOptions([])
      } finally {
        setStockSearching(false)
      }
    }, 200) as unknown as number
  }

  const guessPackFromMarket = (market: string): string => {
    if (MARKET_TO_PACK[market]) return MARKET_TO_PACK[market]
    // Try to match the ticker against each pack's ticker_patterns
    return packs[0]?.slug || 'optical_modules'
  }

  const reload = async () => {
    setLoading(true)
    try {
      const [ms, ps] = await Promise.all([
        modelingApi.listModels(),
        playbookApi.listPacks().catch(() => [] as PackInfo[]),
      ])
      setModels(ms)
      setPacks(ps)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { reload() }, [])

  const handleCreate = async (values: any) => {
    const industry = values.industry
    const pack = packs.find(p => p.slug === industry)
    // stock_value is a labelInValue object from AutoComplete
    const sv = values.stock_value
    if (!sv || !sv.value) {
      message.error('请先搜索并选择股票')
      return
    }
    const picked = stockOptions.find((o) => o.code === sv.value)
    const ticker = picked ? _formatTicker(picked) : sv.value
    const company_name = picked?.name || sv.label || sv.value
    try {
      const periods = values.fiscal_periods?.split(',').map((s: string) => s.trim()).filter(Boolean)
        || pack?.default_periods
        || ['FY25E', 'FY26E', 'FY27E']
      const m = await modelingApi.createModel({
        ticker,
        company_name,
        industry: industry || 'optical_modules',
        fiscal_periods: periods,
        title: values.title || `${company_name} (${ticker})`,
        base_currency: values.base_currency || 'USD',
      })
      message.success(`建模会话已创建: ${m.title}`)
      setCreating(false)
      form.resetFields()
      setStockOptions([])
      nav(`/modeling/${m.id}`)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const _formatTicker = (s: StockSuggestion): string => {
    // Mirror conventions used elsewhere in the app:
    //   US: "AAPL.US", HK: "00700.HK", A-share: "600000.SH" / "000001.SZ".
    // If the raw code already has a market suffix, keep it.
    if (/\.[A-Z]{2}$/.test(s.code)) return s.code
    const m = (s.market || '').toLowerCase()
    if (m.includes('美') || m === 'us') return `${s.code}.US`
    if (m.includes('港') || m === 'hk') return `${s.code}.HK`
    if (m.includes('科创') || m.includes('上海') || s.code.startsWith('6'))
      return `${s.code}.SH`
    if (m.includes('深') || m.includes('创业')) return `${s.code}.SZ`
    return s.code
  }

  const columns = [
    {
      title: '公司 / Ticker', dataIndex: 'ticker', key: 'ticker',
      render: (_: any, r: RevenueModel) => (
        <a onClick={() => nav(`/modeling/${r.id}`)}>
          <strong>{r.company_name}</strong>{' '}
          <Tag color="blue">{r.ticker}</Tag>
        </a>
      ),
    },
    { title: '行业', dataIndex: 'industry', key: 'industry' },
    {
      title: '期间', dataIndex: 'fiscal_periods', key: 'periods',
      render: (ps: string[]) => ps.join(' · '),
    },
    {
      title: '单元格', key: 'cells',
      render: (_: any, r: RevenueModel) => (
        <Space>
          <span>{r.cell_count}</span>
          {r.flagged_count > 0 && (
            <Tag color="orange">⚠️ {r.flagged_count} flagged</Tag>
          )}
        </Space>
      ),
    },
    {
      title: '状态', dataIndex: 'status', key: 'status',
      render: (s: string) => {
        const color =
          s === 'ready' ? 'green' :
          s === 'running' ? 'processing' :
          s === 'failed' ? 'error' :
          s === 'archived' ? 'default' : 'default'
        return <Tag color={color}>{s}</Tag>
      },
    },
    {
      title: '更新时间', dataIndex: 'updated_at', key: 'updated',
      render: (v: string) => new Date(v).toLocaleString('zh-CN'),
    },
    {
      title: '操作', key: 'actions',
      render: (_: any, r: RevenueModel) => (
        <Space>
          <Button size="small" onClick={() => nav(`/modeling/${r.id}`)}>打开</Button>
          <Button size="small" danger onClick={async () => {
            Modal.confirm({
              title: `确认删除 ${r.title}?`,
              content: '此操作不可恢复',
              okType: 'danger',
              onOk: async () => {
                try {
                  await modelingApi.deleteModel(r.id)
                  message.success('已删除')
                  reload()
                } catch (e: any) {
                  message.error(e?.response?.data?.detail || String(e))
                }
              },
            })
          }}>删除</Button>
        </Space>
      ),
    },
  ]

  return (
    <div style={{ padding: '16px 24px' }}>
      <Card
        title={<><span>📊 收入拆分建模</span></>}
        extra={
          <Space>
            <Button icon={<ReloadOutlined />} onClick={reload}>刷新</Button>
            <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreating(true)}>
              新建建模
            </Button>
            <Button onClick={() => nav('/modeling/recipes')}>Recipe 管理</Button>
            <Button onClick={() => nav('/modeling/playbook')}>Playbook</Button>
          </Space>
        }
      >
        <p style={{ color: '#64748b', marginTop: 0 }}>
          每个建模会话都是一张<strong>可审计、可编辑、可进化</strong>的活 Excel —
          每个数字点开都能看到来源、推理链、置信度。
          系统可多轮核验降低幻觉，并通过 playbook 不断积累经验。
        </p>
        {models.length === 0 && !loading ? (
          <Empty description="没有建模会话" style={{ padding: 48 }}>
            <Button type="primary" onClick={() => setCreating(true)}>创建第一个</Button>
          </Empty>
        ) : (
          <Table
            rowKey="id"
            loading={loading}
            columns={columns}
            dataSource={models}
            pagination={{ pageSize: 20 }}
          />
        )}
      </Card>

      <Modal
        title="新建建模会话"
        open={creating}
        onCancel={() => { setCreating(false); setStockOptions([]) }}
        onOk={() => form.submit()}
        okText="创建"
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={handleCreate}
          initialValues={{
            industry: 'optical_modules',
            base_currency: 'USD',
            fiscal_periods: 'FY25E, FY26E, FY27E',
          }}
        >
          <Form.Item
            label="股票 (模糊搜索代码或名称)"
            name="stock_value"
            rules={[{ required: true, message: '请搜索并选择股票' }]}
            extra="支持 A股 / 港股 / 美股；输入代码或中文/英文名称即可"
          >
            <Select
              showSearch
              placeholder="输入代码或名称,如 LITE / 光模块 / 600519"
              notFoundContent={stockSearching
                ? <Spin size="small" />
                : <span style={{ color: '#94a3b8' }}>输入关键字…</span>}
              labelInValue
              filterOption={false}
              onSearch={debouncedStockSearch}
              optionLabelProp="title"
              onChange={(v) => {
                const picked = stockOptions.find(o => o.code === v?.value)
                if (picked && !form.getFieldValue('title')) {
                  form.setFieldsValue({
                    title: `${picked.name} (${_formatTicker(picked)})`,
                  })
                }
                if (picked && !form.getFieldValue('base_currency')) {
                  const m = (picked.market || '').toLowerCase()
                  if (m.includes('港') || m === 'hk') form.setFieldsValue({ base_currency: 'HKD' })
                  else if (m.includes('美') || m === 'us') form.setFieldsValue({ base_currency: 'USD' })
                  else form.setFieldsValue({ base_currency: 'CNY' })
                }
              }}
              options={stockOptions.map((s) => ({
                value: s.code,
                title: `${s.name} (${s.code})`,
                label: (
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8 }}>
                    <span>
                      <LineChartOutlined style={{ color: '#2563eb', marginRight: 6 }} />
                      {s.name}
                    </span>
                    <Space size={4}>
                      <Tag color="blue" style={{ margin: 0 }}>{s.code}</Tag>
                      <Tag color="default" style={{ margin: 0 }}>{s.market}</Tag>
                    </Space>
                  </div>
                ),
              }))}
            />
          </Form.Item>
          <Form.Item label="行业 (Industry Pack)" name="industry" rules={[{ required: true }]}>
            <Select
              options={packs.map(p => ({
                value: p.slug,
                label: `${p.name} (${p.recipe_count} recipes)`,
              }))}
              placeholder="选择行业"
            />
          </Form.Item>
          <Form.Item label="模型标题 (可选)" name="title">
            <Input placeholder="默认: 公司名 (Ticker)" />
          </Form.Item>
          <Form.Item label="预测期 (逗号分隔)" name="fiscal_periods">
            <Input placeholder="FY25E, FY26E, FY27E" />
          </Form.Item>
          <Form.Item label="基准货币" name="base_currency">
            <Select
              options={[
                { value: 'USD', label: 'USD' },
                { value: 'CNY', label: 'CNY' },
                { value: 'HKD', label: 'HKD' },
              ]}
            />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  )
}
