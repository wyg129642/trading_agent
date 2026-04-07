import { useState, useEffect, useCallback, useRef } from 'react'
import {
  Card, Form, Input, Select, InputNumber, Button, Rate, message, AutoComplete,
  Tag, Typography, Space, Alert, Divider,
} from 'antd'
import {
  StockOutlined, ArrowUpOutlined, ArrowDownOutlined,
  SendOutlined, UserOutlined,
} from '@ant-design/icons'
import { useAuthStore } from '../store/auth'
import api from '../services/api'

const { TextArea } = Input
const { Title, Text } = Typography
const { Option } = Select

interface StockSuggestion {
  name: string
  code: string
  market: string
  label: string
}

const MARKET_COLORS: Record<string, string> = {
  'A股': '#e11d48',
  '美股': '#2563eb',
  '港股': '#7c3aed',
}

const HORIZON_OPTIONS = [
  { value: '1w', label: '1周' },
  { value: '2w', label: '2周' },
  { value: '1m', label: '1个月' },
  { value: '3m', label: '3个月' },
  { value: '6m', label: '6个月' },
]

export default function PredictionSubmit() {
  const [form] = Form.useForm()
  const user = useAuthStore((s) => s.user)
  const [loading, setLoading] = useState(false)
  const [analysts, setAnalysts] = useState<any[]>([])
  const [suggestions, setSuggestions] = useState<StockSuggestion[]>([])
  const [suggestLoading, setSuggestLoading] = useState(false)
  const suggestTimer = useRef<ReturnType<typeof setTimeout>>()

  const isBossOrAdmin = user?.role === 'admin' || user?.role === 'boss'

  useEffect(() => {
    if (isBossOrAdmin) {
      api.get('/predictions/users/analysts').then((res) => {
        setAnalysts(res.data)
      }).catch(() => {})
    }
  }, [isBossOrAdmin])

  // Stock search
  const fetchSuggestions = useCallback(async (text: string) => {
    if (!text || text.length < 1) {
      setSuggestions([])
      return
    }
    setSuggestLoading(true)
    try {
      const res = await api.get<StockSuggestion[]>('/stock/suggest', {
        params: { q: text, limit: 8 },
      })
      setSuggestions(res.data)
    } catch {
      setSuggestions([])
    } finally {
      setSuggestLoading(false)
    }
  }, [])

  const onStockSearch = (text: string) => {
    if (suggestTimer.current) clearTimeout(suggestTimer.current)
    suggestTimer.current = setTimeout(() => fetchSuggestions(text), 200)
  }

  const onStockSelect = (_value: string, option: any) => {
    const stock = option.stock as StockSuggestion
    form.setFieldsValue({
      stock_code: stock.code,
      stock_name: stock.name,
      market: stock.market,
    })
  }

  const stockOptions = suggestions.map((s) => ({
    value: s.label,
    label: (
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span>
          <StockOutlined style={{ marginRight: 6, color: '#94a3b8' }} />
          <b>{s.name}</b>
          <span style={{ color: '#94a3b8', marginLeft: 6 }}>{s.code}</span>
        </span>
        <Tag color={MARKET_COLORS[s.market] || '#94a3b8'} style={{ margin: 0, fontSize: 11 }}>
          {s.market}
        </Tag>
      </div>
    ),
    stock: s,
  }))

  const onFinish = async (values: any) => {
    setLoading(true)
    try {
      const payload: any = {
        stock_code: values.stock_code,
        stock_name: values.stock_name,
        market: values.market,
        direction: values.direction,
        time_horizon: values.time_horizon,
        reason: values.reason || null,
        confidence: values.confidence || 3,
        target_price: values.target_price || null,
      }
      if (values.user_id) {
        payload.user_id = values.user_id
      }
      await api.post('/predictions/', payload)
      message.success('预测提交成功')
      form.resetFields()
    } catch (e: any) {
      message.error(e.response?.data?.detail || '提交失败')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{ maxWidth: 720, margin: '0 auto' }}>
      <Title level={3} style={{ marginBottom: 4 }}>
        <StockOutlined style={{ marginRight: 8 }} />
        提交荐股预测
      </Title>
      <Text type="secondary" style={{ display: 'block', marginBottom: 24 }}>
        提交对股票的看涨/看跌预测，系统将在到期后自动回测评分
      </Text>

      <Card>
        <Form
          form={form}
          layout="vertical"
          onFinish={onFinish}
          initialValues={{ confidence: 3, time_horizon: '1m', market: 'A股' }}
        >
          {/* Boss/admin: select analyst */}
          {isBossOrAdmin && (
            <Form.Item
              name="user_id"
              label="预测分析师"
              extra="留空则为自己提交"
            >
              <Select
                placeholder="选择员工（留空为自己）"
                allowClear
                showSearch
                optionFilterProp="children"
              >
                {analysts.map((a) => (
                  <Option key={a.id} value={a.id}>
                    <UserOutlined style={{ marginRight: 6 }} />
                    {a.display_name || a.username}
                    <Tag style={{ marginLeft: 8, fontSize: 11 }}>{a.role}</Tag>
                  </Option>
                ))}
              </Select>
            </Form.Item>
          )}

          {/* Stock search */}
          <Form.Item label="搜索股票" required>
            <AutoComplete
              options={stockOptions}
              onSearch={onStockSearch}
              onSelect={onStockSelect}
              placeholder="输入股票名称或代码搜索..."
              style={{ width: '100%' }}
            />
          </Form.Item>

          <Space size="middle" style={{ width: '100%' }}>
            <Form.Item
              name="stock_code"
              label="股票代码"
              rules={[{ required: true, message: '请输入股票代码' }]}
              style={{ flex: 1 }}
            >
              <Input placeholder="如 600519" />
            </Form.Item>
            <Form.Item
              name="stock_name"
              label="股票名称"
              rules={[{ required: true, message: '请输入股票名称' }]}
              style={{ flex: 1 }}
            >
              <Input placeholder="如 贵州茅台" />
            </Form.Item>
            <Form.Item
              name="market"
              label="市场"
              rules={[{ required: true }]}
              style={{ flex: 1 }}
            >
              <Select>
                <Option value="A股">A股</Option>
                <Option value="港股">港股</Option>
                <Option value="美股">美股</Option>
              </Select>
            </Form.Item>
          </Space>

          <Divider style={{ margin: '8px 0 16px' }} />

          <Space size="middle" style={{ width: '100%' }}>
            <Form.Item
              name="direction"
              label="方向判断"
              rules={[{ required: true, message: '请选择方向' }]}
              style={{ flex: 1 }}
            >
              <Select placeholder="看涨或看跌">
                <Option value="bullish">
                  <ArrowUpOutlined style={{ color: '#ef4444', marginRight: 6 }} />
                  看涨
                </Option>
                <Option value="bearish">
                  <ArrowDownOutlined style={{ color: '#22c55e', marginRight: 6 }} />
                  看跌
                </Option>
              </Select>
            </Form.Item>

            <Form.Item
              name="time_horizon"
              label="预测周期"
              rules={[{ required: true, message: '请选择预测周期' }]}
              style={{ flex: 1 }}
            >
              <Select options={HORIZON_OPTIONS} />
            </Form.Item>
          </Space>

          <Space size="middle" style={{ width: '100%' }}>
            <Form.Item
              name="confidence"
              label="置信度"
              style={{ flex: 1 }}
            >
              <Rate allowHalf={false} />
            </Form.Item>

            <Form.Item
              name="target_price"
              label="目标价位（选填）"
              style={{ flex: 1 }}
            >
              <InputNumber
                style={{ width: '100%' }}
                min={0}
                step={0.01}
                placeholder="预期目标价格"
              />
            </Form.Item>
          </Space>

          <Form.Item name="reason" label="推荐理由">
            <TextArea
              rows={4}
              placeholder="请输入推荐理由、逻辑分析..."
              maxLength={2000}
              showCount
            />
          </Form.Item>

          <Alert
            type="info"
            showIcon
            message="提交后，预测到期时系统将自动获取实际股价进行回测评分。提交前的股价将作为基准价格记录。"
            style={{ marginBottom: 16 }}
          />

          <Form.Item>
            <Button
              type="primary"
              htmlType="submit"
              loading={loading}
              icon={<SendOutlined />}
              size="large"
              block
            >
              提交预测
            </Button>
          </Form.Item>
        </Form>
      </Card>
    </div>
  )
}
