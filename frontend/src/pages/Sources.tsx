import { useEffect, useState, useCallback, useMemo } from 'react'
import {
  Card,
  Tabs,
  Table,
  Tag,
  Button,
  Modal,
  Form,
  Input,
  Select,
  Space,
  Typography,
  Popconfirm,
  message,
  Empty,
  Switch,
  Badge,
  Segmented,
} from 'antd'
import {
  PlusOutlined,
  DeleteOutlined,
  GlobalOutlined,
  StockOutlined,
  LinkOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  QuestionCircleOutlined,
  AppstoreOutlined,
  UnorderedListOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { useAuthStore } from '../store/auth'
import api from '../services/api'

const { Text, Title } = Typography

const MARKET_LABELS: Record<string, string> = {
  US: '美股',
  A: 'A 股',
  HK: '港股',
  KR: '韩股',
  JP: '日股',
  us: '美股',
  china: 'A 股',
  hk: '港股',
  global: '全球',
}

const PRIORITY_COLORS: Record<string, string> = {
  p0: '#ef4444',
  p1: '#f59e0b',
  p2: '#2563eb',
  p3: '#94a3b8',
}

const SOURCE_TYPE_LABELS: Record<string, string> = {
  rss: 'RSS',
  web_scraper: 'Web',
  api: 'API',
}

const CATEGORY_LABELS: Record<string, { en: string; zh: string }> = {
  ai_technology:      { en: 'AI & Technology',      zh: 'AI与科技' },
  semiconductors:     { en: 'Semiconductors',       zh: '半导体' },
  financial_news:     { en: 'Financial News',       zh: '财经新闻' },
  central_banks:      { en: 'Central Banks',        zh: '央行政策' },
  macro_economics:    { en: 'Macro Economics',      zh: '宏观经济' },
  commodities_energy: { en: 'Commodities & Energy', zh: '大宗商品与能源' },
  regulatory:         { en: 'Regulatory & Policy',  zh: '监管与政策' },
  pharma_healthcare:  { en: 'Pharma & Healthcare',  zh: '医药与健康' },
  china_news:         { en: 'China Hot News',       zh: '中国热点' },
  exchanges:          { en: 'Exchanges & Futures',  zh: '交易所与期货' },
  geopolitics:        { en: 'Geopolitics & Trade',  zh: '地缘政治与贸易' },
  portfolio:          { en: 'Portfolio Holdings',   zh: '持仓股监控' },
}

const CATEGORY_COLORS: Record<string, string> = {
  ai_technology:      '#722ed1',
  semiconductors:     '#1677ff',
  financial_news:     '#fa8c16',
  central_banks:      '#eb2f96',
  macro_economics:    '#13c2c2',
  commodities_energy: '#faad14',
  regulatory:         '#f5222d',
  pharma_healthcare:  '#52c41a',
  china_news:         '#ff4d4f',
  exchanges:          '#2f54eb',
  geopolitics:        '#a0d911',
  portfolio:          '#597ef7',
}

interface SystemSource {
  name: string
  type: string
  source_type: string
  url: string
  enabled: boolean
  priority: string
  market: string
  category: string
  group: string
  tags: string[]
  stock_ticker: string
  stock_name: string
  is_healthy: boolean | null
  last_success: string | null
  consecutive_failures: number
  total_items_fetched: number
}

interface UserSourceItem {
  id: string
  name: string
  url: string
  type: string
  source_type: string
  priority: string
  category: string
  is_active: boolean
  stock_market: string | null
  stock_ticker: string | null
  stock_name: string | null
}

interface PortfolioHolding {
  name: string
  url: string
  enabled: boolean
  priority: string
  market: string
  category: string
  tags: string[]
  stock_ticker: string
  stock_name: string
  stock_market: string
}

export default function Sources() {
  const { t, i18n } = useTranslation()
  const lang = i18n.language === 'zh' ? 'zh' : 'en'
  const user = useAuthStore((s) => s.user)
  const isAdmin = user?.role === 'admin'
  const [systemSources, setSystemSources] = useState<SystemSource[]>([])
  const [userSources, setUserSources] = useState<UserSourceItem[]>([])
  const [portfolio, setPortfolio] = useState<PortfolioHolding[]>([])
  const [loading, setLoading] = useState(true)
  const [modalOpen, setModalOpen] = useState(false)
  const [form] = Form.useForm()
  const [selectedCategory, setSelectedCategory] = useState<string>('all')
  const [viewMode, setViewMode] = useState<string>('category')

  const getCategoryLabel = useCallback((cat: string) => {
    const entry = CATEGORY_LABELS[cat]
    return entry ? entry[lang] : cat || '-'
  }, [lang])

  const fetchData = useCallback(async () => {
    setLoading(true)
    try {
      const [sourcesRes, portfolioRes] = await Promise.all([
        api.get('/sources'),
        isAdmin ? api.get('/sources/portfolio') : Promise.resolve({ data: { holdings: [] } }),
      ])
      setSystemSources(sourcesRes.data.system_sources || [])
      setUserSources(sourcesRes.data.user_sources || [])
      setPortfolio(portfolioRes.data.holdings || [])
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }, [isAdmin])

  useEffect(() => {
    fetchData()
  }, [fetchData])

  const handleAdd = async (values: any) => {
    try {
      await api.post('/sources', {
        name: values.name,
        url: values.url || '',
        source_type: values.source_type || 'rss',
        priority: values.priority || 'p1',
        category: values.category || '',
        stock_market: values.stock_market || null,
        stock_ticker: values.stock_ticker || null,
        stock_name: values.stock_name || null,
      })
      message.success(t('sources.addSuccess'))
      setModalOpen(false)
      form.resetFields()
      fetchData()
    } catch (e: any) {
      message.error(e.response?.data?.detail || t('common.error'))
    }
  }

  const handleDelete = async (id: string) => {
    try {
      await api.delete(`/sources/${id}`)
      message.success(t('sources.deleteSuccess'))
      fetchData()
    } catch {
      message.error(t('common.error'))
    }
  }

  const handleToggleActive = async (id: string, active: boolean) => {
    try {
      await api.put(`/sources/${id}`, { is_active: active })
      fetchData()
    } catch {
      message.error(t('common.error'))
    }
  }

  // Compute category counts from system sources
  const categoryStats = useMemo(() => {
    const stats: Record<string, { total: number; enabled: number }> = {}
    const allSys = [...systemSources, ...portfolio.map(p => ({ ...p, category: p.category || 'portfolio' }))]
    for (const s of allSys) {
      const cat = s.category || 'other'
      if (!stats[cat]) stats[cat] = { total: 0, enabled: 0 }
      stats[cat].total++
      if ('enabled' in s && s.enabled) stats[cat].enabled++
    }
    return stats
  }, [systemSources, portfolio])

  // Filter system sources by selected category
  const filteredSystemSources = useMemo(() => {
    const all = systemSources.filter((s) => s.group !== 'portfolio')
    if (selectedCategory === 'all') return all
    return all.filter((s) => (s.category || '') === selectedCategory)
  }, [systemSources, selectedCategory])

  // Group sources by category for the card view
  const groupedSources = useMemo(() => {
    const groups: Record<string, SystemSource[]> = {}
    const sources = systemSources.filter((s) => s.group !== 'portfolio')
    for (const s of sources) {
      const cat = s.category || 'other'
      if (!groups[cat]) groups[cat] = []
      groups[cat].push(s)
    }
    // Sort categories by count descending
    return Object.entries(groups).sort((a, b) => b[1].length - a[1].length)
  }, [systemSources])

  // System Sources columns
  const systemColumns = [
    {
      title: t('sources.name'),
      dataIndex: 'name',
      key: 'name',
      width: 260,
      render: (name: string, r: SystemSource) => (
        <div>
          <Text strong>{name}</Text>
          {r.stock_name && (
            <div><Text type="secondary" style={{ fontSize: 12 }}>{r.stock_name}</Text></div>
          )}
        </div>
      ),
    },
    {
      title: t('sources.category'),
      dataIndex: 'category',
      key: 'category',
      width: 160,
      render: (v: string) => v ? (
        <Tag color={CATEGORY_COLORS[v] || '#8c8c8c'}>{getCategoryLabel(v)}</Tag>
      ) : '-',
    },
    {
      title: t('sources.type'),
      dataIndex: 'source_type',
      key: 'source_type',
      width: 70,
      render: (v: string) => <Tag>{SOURCE_TYPE_LABELS[v] || v}</Tag>,
    },
    {
      title: t('sources.priority'),
      dataIndex: 'priority',
      key: 'priority',
      width: 60,
      render: (v: string) => <Tag color={PRIORITY_COLORS[v] || '#94a3b8'}>{v?.toUpperCase()}</Tag>,
    },
    {
      title: t('sources.market'),
      dataIndex: 'market',
      key: 'market',
      width: 70,
      render: (v: string) => MARKET_LABELS[v] || v || '-',
    },
    {
      title: t('sources.status'),
      key: 'status',
      width: 120,
      render: (_: any, r: SystemSource) => {
        if (!r.enabled) return <Tag color="default">{t('sources.disabled')}</Tag>
        if (r.is_healthy === null) return <Tag icon={<QuestionCircleOutlined />}>{t('sources.pending')}</Tag>
        if (r.is_healthy) {
          return <Tag icon={<CheckCircleOutlined />} color="success">{t('sources.healthy')}</Tag>
        }
        return (
          <span>
            <Tag icon={<CloseCircleOutlined />} color="error">{t('sources.unhealthy')}</Tag>
            {r.consecutive_failures > 0 && (
              <span style={{ fontSize: 11, color: '#ef4444' }}>
                {r.consecutive_failures}x
              </span>
            )}
          </span>
        )
      },
    },
    {
      title: t('sources.fetched'),
      dataIndex: 'total_items_fetched',
      key: 'fetched',
      width: 70,
      render: (v: number) => v || 0,
    },
  ]

  // Portfolio columns
  const portfolioColumns = [
    {
      title: t('sources.stockName'),
      dataIndex: 'stock_name',
      key: 'stock_name',
      width: 140,
      render: (v: string) => <Text strong>{v}</Text>,
    },
    {
      title: t('sources.stockTicker'),
      dataIndex: 'stock_ticker',
      key: 'stock_ticker',
      width: 100,
      render: (v: string) => <Tag color="blue">{v}</Tag>,
    },
    {
      title: t('sources.stockMarket'),
      dataIndex: 'stock_market',
      key: 'stock_market',
      width: 100,
      render: (v: string) => MARKET_LABELS[v] || v || '-',
    },
    {
      title: t('sources.name'),
      dataIndex: 'name',
      key: 'name',
      ellipsis: true,
    },
    {
      title: t('sources.tags'),
      dataIndex: 'tags',
      key: 'tags',
      width: 200,
      render: (tags: string[]) => (
        <Space size={2} wrap>
          {(tags || []).map((tag: string) => (
            <Tag key={tag} style={{ fontSize: 11 }}>{tag}</Tag>
          ))}
        </Space>
      ),
    },
    {
      title: t('sources.status'),
      key: 'status',
      width: 80,
      render: (_: any, r: PortfolioHolding) =>
        r.enabled
          ? <Tag color="success">{t('sources.enabled')}</Tag>
          : <Tag color="default">{t('sources.disabled')}</Tag>,
    },
  ]

  // User Sources columns
  const userColumns = [
    {
      title: t('sources.name'),
      dataIndex: 'name',
      key: 'name',
      render: (v: string, r: UserSourceItem) => (
        <div>
          <Text strong>{v}</Text>
          {r.stock_name && (
            <div>
              <Text type="secondary" style={{ fontSize: 12 }}>
                {r.stock_name} ({r.stock_ticker})
              </Text>
            </div>
          )}
        </div>
      ),
    },
    {
      title: t('sources.category'),
      key: 'category',
      width: 140,
      render: (_: any, r: UserSourceItem) => r.category ? (
        <Tag color={CATEGORY_COLORS[r.category] || '#8c8c8c'}>{getCategoryLabel(r.category)}</Tag>
      ) : '-',
    },
    {
      title: t('sources.stockMarket'),
      key: 'market',
      width: 80,
      render: (_: any, r: UserSourceItem) =>
        r.stock_market ? <Tag color="blue">{MARKET_LABELS[r.stock_market] || r.stock_market}</Tag> : '-',
    },
    {
      title: t('sources.url'),
      dataIndex: 'url',
      key: 'url',
      ellipsis: true,
      render: (v: string) =>
        v ? (
          <a href={v} target="_blank" rel="noopener noreferrer">
            <LinkOutlined /> {v.substring(0, 50)}
          </a>
        ) : '-',
    },
    {
      title: t('sources.priority'),
      dataIndex: 'priority',
      key: 'priority',
      width: 60,
      render: (v: string) => <Tag color={PRIORITY_COLORS[v] || '#94a3b8'}>{v?.toUpperCase()}</Tag>,
    },
    {
      title: t('sources.status'),
      key: 'is_active',
      width: 80,
      render: (_: any, r: UserSourceItem) => (
        <Switch
          checked={r.is_active}
          size="small"
          onChange={(checked) => handleToggleActive(r.id, checked)}
        />
      ),
    },
    {
      title: '',
      key: 'action',
      width: 50,
      render: (_: any, r: UserSourceItem) => (
        <Popconfirm
          title={t('sources.confirmDelete')}
          onConfirm={() => handleDelete(r.id)}
          okText={t('common.confirm')}
          cancelText={t('common.cancel')}
        >
          <Button type="text" danger icon={<DeleteOutlined />} size="small" />
        </Popconfirm>
      ),
    },
  ]

  // Category selector options
  const categoryOptions = useMemo(() => {
    const allCats = Object.keys(categoryStats).sort()
    return [
      { value: 'all', label: lang === 'zh' ? '全部分类' : 'All Categories' },
      ...allCats.map((cat) => ({
        value: cat,
        label: `${getCategoryLabel(cat)} (${categoryStats[cat]?.enabled || 0}/${categoryStats[cat]?.total || 0})`,
      })),
    ]
  }, [categoryStats, getCategoryLabel, lang])

  // Render category cards view
  const renderCategoryCards = () => (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))', gap: 16 }}>
      {groupedSources.map(([cat, sources]) => (
        <Card
          key={cat}
          size="small"
          title={
            <Space>
              <Tag color={CATEGORY_COLORS[cat] || '#8c8c8c'} style={{ margin: 0 }}>
                {getCategoryLabel(cat)}
              </Tag>
              <Badge
                count={sources.filter(s => s.enabled).length}
                style={{ backgroundColor: '#52c41a' }}
                size="small"
              />
              <Text type="secondary" style={{ fontSize: 12 }}>
                / {sources.length}
              </Text>
            </Space>
          }
          style={{ height: 'fit-content' }}
        >
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {sources.map((s) => (
              <div
                key={s.name}
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  padding: '4px 0',
                  opacity: s.enabled ? 1 : 0.5,
                }}
              >
                <div style={{ flex: 1, minWidth: 0 }}>
                  <Text
                    ellipsis
                    style={{ fontSize: 13 }}
                    title={s.url}
                  >
                    {s.name}
                  </Text>
                  <div>
                    <Tag style={{ fontSize: 10 }}>{SOURCE_TYPE_LABELS[s.source_type] || s.source_type}</Tag>
                    <Tag color={PRIORITY_COLORS[s.priority]} style={{ fontSize: 10 }}>{s.priority?.toUpperCase()}</Tag>
                    <Text type="secondary" style={{ fontSize: 11 }}>{MARKET_LABELS[s.market] || s.market}</Text>
                  </div>
                </div>
                <div style={{ marginLeft: 8 }}>
                  {!s.enabled ? (
                    <Tag color="default" style={{ fontSize: 10 }}>{t('sources.disabled')}</Tag>
                  ) : s.is_healthy === null ? (
                    <Tag style={{ fontSize: 10 }}>{t('sources.pending')}</Tag>
                  ) : s.is_healthy ? (
                    <Tag color="success" style={{ fontSize: 10 }}>{t('sources.healthy')}</Tag>
                  ) : (
                    <Tag color="error" style={{ fontSize: 10 }}>{t('sources.unhealthy')}</Tag>
                  )}
                </div>
              </div>
            ))}
          </div>
        </Card>
      ))}
    </div>
  )

  // Build tab items based on role
  const tabItems: any[] = [
    {
      key: 'custom',
      label: (
        <span>
          <LinkOutlined /> {t('sources.mySources')} ({userSources.length})
        </span>
      ),
      children: (
        <div>
          <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'flex-end' }}>
            <Button type="primary" icon={<PlusOutlined />} onClick={() => setModalOpen(true)}>
              {t('sources.addSource')}
            </Button>
          </div>
          {userSources.length === 0 ? (
            <Empty description={t('sources.noCustomSources')} />
          ) : (
            <Table
              dataSource={userSources}
              columns={userColumns}
              rowKey="id"
              size="small"
              pagination={false}
            />
          )}
        </div>
      ),
    },
  ]

  // System sources tab - visible to all users (not just admin)
  tabItems.push({
    key: 'system',
    label: (
      <span>
        <GlobalOutlined /> {t('sources.systemSources')} ({systemSources.filter((s) => !s.group).length})
      </span>
    ),
    children: (
      <div>
        <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8 }}>
          <Select
            value={selectedCategory}
            options={categoryOptions}
            onChange={setSelectedCategory}
            style={{ minWidth: 220 }}
            size="small"
          />
          <Segmented
            options={[
              { value: 'category', icon: <AppstoreOutlined />, label: lang === 'zh' ? '分类视图' : 'Cards' },
              { value: 'table', icon: <UnorderedListOutlined />, label: lang === 'zh' ? '列表视图' : 'Table' },
            ]}
            value={viewMode}
            onChange={(v) => setViewMode(v as string)}
            size="small"
          />
        </div>
        {viewMode === 'category' && selectedCategory === 'all' ? (
          renderCategoryCards()
        ) : (
          <Table
            dataSource={filteredSystemSources}
            columns={systemColumns}
            rowKey="name"
            size="small"
            pagination={false}
            loading={loading}
          />
        )}
      </div>
    ),
  })

  // Portfolio tab - admin only
  if (isAdmin) {
    tabItems.push({
      key: 'portfolio',
      label: (
        <span>
          <StockOutlined /> {t('sources.portfolioHoldings')} ({portfolio.length})
        </span>
      ),
      children: (
        <Table
          dataSource={portfolio}
          columns={portfolioColumns}
          rowKey={(r) => `${r.stock_market}-${r.stock_ticker}`}
          size="small"
          pagination={false}
          loading={loading}
        />
      ),
    })
  }

  return (
    <div>
      <Card>
        <Tabs items={tabItems} />
      </Card>

      <Modal
        title={t('sources.addSource')}
        open={modalOpen}
        onCancel={() => {
          setModalOpen(false)
          form.resetFields()
        }}
        onOk={() => form.submit()}
        okText={t('sources.add')}
        cancelText={t('common.cancel')}
        width={560}
      >
        <Form form={form} layout="vertical" onFinish={handleAdd}>
          <Form.Item
            name="name"
            label={t('sources.name')}
            rules={[{ required: true, message: t('sources.nameRequired') }]}
          >
            <Input placeholder={t('sources.namePlaceholder')} />
          </Form.Item>

          <Form.Item name="category" label={t('sources.category')}>
            <Select
              placeholder={t('sources.selectCategory')}
              allowClear
              options={Object.entries(CATEGORY_LABELS).map(([key, labels]) => ({
                value: key,
                label: labels[lang],
              }))}
            />
          </Form.Item>

          <Form.Item label={t('sources.stockInfo')}>
            <Space.Compact style={{ width: '100%' }}>
              <Form.Item name="stock_market" noStyle>
                <Select
                  placeholder={t('sources.selectMarket')}
                  style={{ width: 140 }}
                  allowClear
                  options={[
                    { value: 'US', label: '美股' },
                    { value: 'A', label: 'A 股' },
                    { value: 'HK', label: '港股' },
                    { value: 'KR', label: '韩股' },
                    { value: 'JP', label: '日股' },
                  ]}
                />
              </Form.Item>
              <Form.Item name="stock_ticker" noStyle>
                <Input placeholder={t('sources.tickerPlaceholder')} style={{ width: 140 }} />
              </Form.Item>
              <Form.Item name="stock_name" noStyle>
                <Input placeholder={t('sources.stockNamePlaceholder')} style={{ flex: 1 }} />
              </Form.Item>
            </Space.Compact>
          </Form.Item>

          <Form.Item name="url" label={t('sources.url')}>
            <Input placeholder={t('sources.urlPlaceholder')} />
          </Form.Item>

          <Space>
            <Form.Item name="source_type" label={t('sources.type')} initialValue="rss">
              <Select
                style={{ width: 120 }}
                options={[
                  { value: 'rss', label: 'RSS' },
                  { value: 'web_scraper', label: 'Web Scraper' },
                  { value: 'stock', label: t('sources.stockSub') },
                ]}
              />
            </Form.Item>
            <Form.Item name="priority" label={t('sources.priority')} initialValue="p1">
              <Select
                style={{ width: 100 }}
                options={[
                  { value: 'p0', label: 'P0' },
                  { value: 'p1', label: 'P1' },
                  { value: 'p2', label: 'P2' },
                  { value: 'p3', label: 'P3' },
                ]}
              />
            </Form.Item>
          </Space>
        </Form>
      </Modal>
    </div>
  )
}
