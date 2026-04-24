import { useEffect, useState, useCallback, useRef } from 'react'
import {
  Card,
  Button,
  Modal,
  Form,
  Input,
  Tag,
  Space,
  Select,
  Empty,
  Popconfirm,
  Typography,
  message,
  Table,
  Spin,
  Segmented,
  Divider,
  Collapse,
} from 'antd'
import {
  PlusOutlined,
  DeleteOutlined,
  StarOutlined,
  SearchOutlined,
  CheckOutlined,
  FolderOutlined,
  FolderAddOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import api from '../services/api'

const { Text } = Typography

interface WatchlistItem {
  id: string
  item_type: string
  value: string
  display_name: string | null
  added_at: string
}

interface Watchlist {
  id: string
  name: string
  description: string | null
  is_default?: boolean
  items: WatchlistItem[]
  item_count: number
}

interface StockResult {
  code: string
  name: string
  name_cn?: string
  market: string
}

const TYPE_COLORS: Record<string, string> = {
  ticker: 'blue',
  sector: 'purple',
  keyword: 'green',
}

const MARKETS = ['A', 'HK', 'US', 'KR', 'JP'] as const

const MARKET_COLORS: Record<string, string> = {
  A: '#cf1322',
  HK: '#d46b08',
  US: '#0958d9',
  KR: '#531dab',
  JP: '#c41d7f',
}

// Use "quick-add" as a sentinel to mean "default watchlist (auto-create)"
const DEFAULT_TARGET = '__default__'

export default function WatchlistPage() {
  const { t } = useTranslation()
  const [watchlists, setWatchlists] = useState<Watchlist[]>([])
  const [loading, setLoading] = useState(true)
  const [createModalOpen, setCreateModalOpen] = useState(false)
  const [addItemModal, setAddItemModal] = useState<{ watchlistId: string } | null>(null)
  const [form] = Form.useForm()
  const [itemForm] = Form.useForm()

  // Stock search state
  const [stockSearchOpen, setStockSearchOpen] = useState(false)
  const [stockSearchQuery, setStockSearchQuery] = useState('')
  const [stockSearchMarket, setStockSearchMarket] = useState<string | undefined>(undefined)
  const [stockSearchResults, setStockSearchResults] = useState<StockResult[]>([])
  const [stockSearchLoading, setStockSearchLoading] = useState(false)
  const [targetWatchlistId, setTargetWatchlistId] = useState<string>(DEFAULT_TARGET)
  const [addedStocks, setAddedStocks] = useState<Set<string>>(new Set())
  const searchTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Derived: split default vs custom watchlists
  const defaultWatchlist = watchlists.find((wl) => wl.is_default) || watchlists[0]
  const customWatchlists = watchlists.filter((wl) => wl.id !== defaultWatchlist?.id)
  const allItems = watchlists.flatMap((wl) => wl.items)

  const existingTickers = new Set(
    allItems.filter((i) => i.item_type === 'ticker').map((i) => i.value),
  )

  const fetchWatchlists = async () => {
    try {
      const res = await api.get('/watchlists')
      setWatchlists(res.data.watchlists)
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchWatchlists()
  }, [])

  const handleCreate = async (values: { name: string; description?: string }) => {
    try {
      await api.post('/watchlists', values)
      message.success(t('watchlist.createSuccess'))
      setCreateModalOpen(false)
      form.resetFields()
      fetchWatchlists()
    } catch (e: any) {
      message.error(e.response?.data?.detail || t('common.error'))
    }
  }

  const handleDelete = async (id: string) => {
    try {
      await api.delete(`/watchlists/${id}`)
      message.success(t('watchlist.deleteSuccess'))
      fetchWatchlists()
    } catch {
      message.error(t('common.error'))
    }
  }

  const handleAddItem = async (values: {
    item_type: string
    value: string
    display_name?: string
  }) => {
    if (!addItemModal) return
    const rawValues = values.value
      .split(/[,，\s]+/)
      .map((v) => v.trim())
      .filter(Boolean)
    const unique = [...new Set(rawValues)]
    let successCount = 0
    let dupCount = 0
    for (const val of unique) {
      try {
        await api.post(`/watchlists/${addItemModal.watchlistId}/items`, {
          item_type: values.item_type,
          value: val,
          display_name: unique.length === 1 ? values.display_name : undefined,
        })
        successCount++
      } catch (e: any) {
        if (e.response?.status === 409) {
          dupCount++
        } else {
          message.error(`${val}: ${e.response?.data?.detail || t('common.error')}`)
        }
      }
    }
    if (successCount > 0) {
      message.success(t('watchlist.batchAddSuccess', { count: successCount }))
    }
    if (dupCount > 0) {
      message.info(t('watchlist.batchAddDup', { count: dupCount }))
    }
    setAddItemModal(null)
    itemForm.resetFields()
    fetchWatchlists()
  }

  const handleRemoveItem = async (watchlistId: string, itemId: string) => {
    try {
      await api.delete(`/watchlists/${watchlistId}/items/${itemId}`)
      message.success(t('watchlist.removeSuccess'))
      fetchWatchlists()
    } catch {
      message.error(t('common.error'))
    }
  }

  // Stock search with debounce
  const doStockSearch = useCallback(
    async (query: string, market: string | undefined) => {
      setStockSearchLoading(true)
      try {
        const params: Record<string, any> = { q: query, limit: 50 }
        if (market) params.market = market
        const res = await api.get('/watchlists/stock-search', { params })
        setStockSearchResults(res.data.results || [])
      } catch (e) {
        console.error(e)
        setStockSearchResults([])
      } finally {
        setStockSearchLoading(false)
      }
    },
    [],
  )

  useEffect(() => {
    if (!stockSearchOpen) return
    if (searchTimerRef.current) clearTimeout(searchTimerRef.current)
    searchTimerRef.current = setTimeout(() => {
      doStockSearch(stockSearchQuery, stockSearchMarket)
    }, 300)
    return () => {
      if (searchTimerRef.current) clearTimeout(searchTimerRef.current)
    }
  }, [stockSearchQuery, stockSearchMarket, stockSearchOpen, doStockSearch])

  const handleAddStock = async (stock: StockResult) => {
    const displayName = stock.name_cn || stock.name
    try {
      if (targetWatchlistId === DEFAULT_TARGET) {
        // Use quick-add: auto-creates default watchlist if none exists
        await api.post('/watchlists/quick-add', {
          item_type: 'ticker',
          value: stock.code,
          display_name: displayName,
          metadata: { market: stock.market },
        })
      } else {
        await api.post(`/watchlists/${targetWatchlistId}/items`, {
          item_type: 'ticker',
          value: stock.code,
          display_name: displayName,
          metadata: { market: stock.market },
        })
      }
      message.success(t('watchlist.stockAdded', { name: `${stock.code} ${displayName}` }))
      setAddedStocks((prev) => new Set(prev).add(`${stock.market}:${stock.code}`))
      fetchWatchlists()
    } catch (e: any) {
      if (e.response?.status === 409) {
        message.info(t('watchlist.batchAddDup', { count: 1 }))
        setAddedStocks((prev) => new Set(prev).add(`${stock.market}:${stock.code}`))
      } else {
        message.error(e.response?.data?.detail || t('common.error'))
      }
    }
  }

  const openStockSearch = (watchlistId?: string) => {
    setTargetWatchlistId(watchlistId || DEFAULT_TARGET)
    setStockSearchQuery('')
    setStockSearchMarket(undefined)
    setStockSearchResults([])
    setAddedStocks(new Set())
    setStockSearchOpen(true)
  }

  const marketLabel = (key: string) => t(`watchlist.market${key}` as any, key)

  // Render stock tags for a given watchlist
  const renderItems = (wl: Watchlist) => (
    <Space wrap style={{ marginBottom: 4 }}>
      {wl.items.map((item) => (
        <Tag
          key={item.id}
          color={TYPE_COLORS[item.item_type] || 'default'}
          closable
          onClose={(e) => {
            e.preventDefault()
            handleRemoveItem(wl.id, item.id)
          }}
        >
          {item.display_name ? `${item.value} ${item.display_name}` : item.value}
        </Tag>
      ))}
    </Space>
  )

  return (
    <div>
      {/* ── Header ── */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: 16,
        }}
      >
        <Typography.Title level={4} style={{ margin: 0 }}>
          <StarOutlined style={{ marginRight: 8 }} />
          {t('watchlist.myWatchlists')}
        </Typography.Title>
        <Button type="primary" icon={<SearchOutlined />} onClick={() => openStockSearch()}>
          {t('watchlist.addStock')}
        </Button>
      </div>

      {/* ── Default watchlist stocks (top-level, always visible) ── */}
      <Card
        size="small"
        style={{ marginBottom: 16 }}
        title={
          <Space>
            <StarOutlined style={{ color: '#faad14' }} />
            <Text strong>{defaultWatchlist?.name || t('watchlist.defaultName')}</Text>
            {defaultWatchlist && (
              <Text type="secondary">
                ({defaultWatchlist.item_count} {t('watchlist.items')})
              </Text>
            )}
          </Space>
        }
        extra={
          defaultWatchlist ? (
            <Button
              size="small"
              onClick={() => setAddItemModal({ watchlistId: defaultWatchlist.id })}
            >
              <PlusOutlined /> {t('watchlist.addItem')}
            </Button>
          ) : undefined
        }
        loading={loading}
      >
        {!defaultWatchlist || defaultWatchlist.items.length === 0 ? (
          <Empty
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description={t('watchlist.emptyDefaultDesc')}
          />
        ) : (
          renderItems(defaultWatchlist)
        )}
      </Card>

      {/* ── Custom watchlists (collapsible) ── */}
      <Divider orientation="left" style={{ fontSize: 13 }}>
        <FolderOutlined style={{ marginRight: 6 }} />
        {t('watchlist.customLists')}
        <Button
          type="link"
          size="small"
          icon={<FolderAddOutlined />}
          onClick={() => setCreateModalOpen(true)}
          style={{ marginLeft: 8 }}
        >
          {t('watchlist.newWatchlist')}
        </Button>
      </Divider>

      {customWatchlists.length === 0 ? (
        <Text type="secondary" style={{ display: 'block', textAlign: 'center', padding: '8px 0' }}>
          {t('watchlist.noCustomLists')}
        </Text>
      ) : (
        <Collapse
          size="small"
          items={customWatchlists.map((wl) => ({
            key: wl.id,
            label: (
              <Space>
                <Text strong>{wl.name}</Text>
                <Text type="secondary">
                  ({wl.item_count} {t('watchlist.items')})
                </Text>
              </Space>
            ),
            extra: (
              <Space
                onClick={(e) => e.stopPropagation()}
                style={{ marginRight: 4 }}
              >
                <Button
                  size="small"
                  type="link"
                  icon={<SearchOutlined />}
                  onClick={() => openStockSearch(wl.id)}
                />
                <Button
                  size="small"
                  type="link"
                  icon={<PlusOutlined />}
                  onClick={() => setAddItemModal({ watchlistId: wl.id })}
                />
                <Popconfirm
                  title={t('watchlist.deleteConfirm')}
                  onConfirm={() => handleDelete(wl.id)}
                  okText={t('common.confirm')}
                  cancelText={t('common.cancel')}
                >
                  <Button size="small" type="link" danger icon={<DeleteOutlined />} />
                </Popconfirm>
              </Space>
            ),
            children: (
              <>
                {wl.description && (
                  <Text
                    type="secondary"
                    style={{ display: 'block', marginBottom: 8, fontSize: 13 }}
                  >
                    {wl.description}
                  </Text>
                )}
                {wl.items.length === 0 ? (
                  <Text type="secondary">{t('watchlist.emptyListHint')}</Text>
                ) : (
                  renderItems(wl)
                )}
              </>
            ),
          }))}
        />
      )}

      {/* ── Create Watchlist Modal ── */}
      <Modal
        title={t('watchlist.newWatchlist')}
        open={createModalOpen}
        onCancel={() => setCreateModalOpen(false)}
        onOk={() => form.submit()}
        okText={t('common.confirm')}
        cancelText={t('common.cancel')}
      >
        <Form form={form} onFinish={handleCreate} layout="vertical">
          <Form.Item name="name" label={t('watchlist.name')} rules={[{ required: true }]}>
            <Input placeholder={t('watchlist.namePlaceholder')} />
          </Form.Item>
          <Form.Item name="description" label={t('watchlist.description')}>
            <Input.TextArea rows={2} placeholder={t('watchlist.descPlaceholder')} />
          </Form.Item>
        </Form>
      </Modal>

      {/* ── Add Item Modal (sector / keyword / ticker by code) ── */}
      <Modal
        title={t('watchlist.addItem')}
        open={!!addItemModal}
        onCancel={() => setAddItemModal(null)}
        onOk={() => itemForm.submit()}
        okText={t('common.confirm')}
        cancelText={t('common.cancel')}
      >
        <Form
          form={itemForm}
          onFinish={handleAddItem}
          layout="vertical"
          initialValues={{ item_type: 'ticker' }}
        >
          <Form.Item name="item_type" label={t('watchlist.type')} rules={[{ required: true }]}>
            <Select
              options={[
                { value: 'ticker', label: t('watchlist.addTicker') },
                { value: 'sector', label: t('watchlist.addSector') },
                { value: 'keyword', label: t('watchlist.addKeyword') },
              ]}
            />
          </Form.Item>
          <Form.Item
            name="value"
            label={t('watchlist.value')}
            rules={[{ required: true }]}
            extra={
              <Text type="secondary" style={{ fontSize: 12 }}>
                {t('watchlist.batchAddHint')}
              </Text>
            }
          >
            <Input.TextArea rows={2} placeholder={t('watchlist.valuePlaceholder')} />
          </Form.Item>
          <Form.Item name="display_name" label={t('watchlist.displayName')}>
            <Input placeholder={t('watchlist.displayNamePlaceholder')} />
          </Form.Item>
        </Form>
      </Modal>

      {/* ── Stock Search Modal ── */}
      <Modal
        title={
          <Space>
            <SearchOutlined />
            {t('watchlist.addStock')}
          </Space>
        }
        open={stockSearchOpen}
        onCancel={() => setStockSearchOpen(false)}
        footer={null}
        width={720}
        styles={{ body: { padding: '12px 24px 24px' } }}
      >
        {/* Target watchlist selector */}
        {watchlists.length > 1 && (
          <div style={{ marginBottom: 12 }}>
            <Text type="secondary" style={{ marginRight: 8 }}>
              {t('watchlist.addToWatchlist')}:
            </Text>
            <Select
              size="small"
              value={targetWatchlistId}
              onChange={setTargetWatchlistId}
              style={{ minWidth: 180 }}
              options={[
                {
                  value: DEFAULT_TARGET,
                  label: defaultWatchlist
                    ? `${defaultWatchlist.name} (${t('watchlist.defaultLabel')})`
                    : t('watchlist.defaultName'),
                },
                ...customWatchlists.map((wl) => ({
                  value: wl.id,
                  label: `${wl.name} (${wl.item_count})`,
                })),
              ]}
            />
          </div>
        )}

        {/* Market filter */}
        <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
          <Segmented
            size="small"
            value={stockSearchMarket || ''}
            onChange={(val) => setStockSearchMarket(val === '' ? undefined : String(val))}
            options={[
              { value: '', label: t('watchlist.allMarkets') },
              ...MARKETS.map((m) => ({ value: m, label: marketLabel(m) })),
            ]}
          />
        </div>

        {/* Search input */}
        <Input
          placeholder={t('watchlist.searchStock')}
          prefix={<SearchOutlined />}
          allowClear
          value={stockSearchQuery}
          onChange={(e) => setStockSearchQuery(e.target.value)}
          autoFocus
          style={{ marginBottom: 12 }}
        />

        {/* Results */}
        <div style={{ maxHeight: 420, overflow: 'auto' }}>
          {stockSearchLoading ? (
            <div style={{ textAlign: 'center', padding: 40 }}>
              <Spin />
            </div>
          ) : stockSearchResults.length === 0 && stockSearchQuery ? (
            <Empty
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description={t('watchlist.noSearchResult')}
            />
          ) : (
            <Table
              dataSource={stockSearchResults}
              rowKey={(r) => `${r.market}:${r.code}`}
              pagination={false}
              size="small"
              columns={[
                {
                  title: t('watchlist.market'),
                  dataIndex: 'market',
                  width: 70,
                  render: (m: string) => (
                    <Tag
                      color={MARKET_COLORS[m]}
                      style={{ margin: 0, fontSize: 11, lineHeight: '18px' }}
                    >
                      {marketLabel(m)}
                    </Tag>
                  ),
                },
                {
                  title: t('watchlist.stockCode'),
                  dataIndex: 'code',
                  width: 110,
                  render: (code: string) => (
                    <Text strong style={{ fontFamily: 'monospace' }}>
                      {code}
                    </Text>
                  ),
                },
                {
                  title: t('watchlist.stockName'),
                  dataIndex: 'name',
                  ellipsis: true,
                  render: (_: string, record: StockResult) =>
                    record.name_cn ? (
                      <span>
                        {record.name_cn}{' '}
                        <Text type="secondary" style={{ fontSize: 12 }}>
                          {record.name}
                        </Text>
                      </span>
                    ) : (
                      record.name
                    ),
                },
                {
                  title: '',
                  width: 48,
                  render: (_: any, record: StockResult) => {
                    const key = `${record.market}:${record.code}`
                    const alreadyAdded =
                      addedStocks.has(key) || existingTickers.has(record.code)
                    return alreadyAdded ? (
                      <CheckOutlined style={{ color: '#52c41a' }} />
                    ) : (
                      <Button
                        type="link"
                        size="small"
                        icon={<PlusOutlined />}
                        onClick={() => handleAddStock(record)}
                        style={{ padding: 0 }}
                      />
                    )
                  },
                },
              ]}
              onRow={(record) => ({
                style: { cursor: 'pointer' },
                onClick: () => {
                  const key = `${record.market}:${record.code}`
                  if (!addedStocks.has(key) && !existingTickers.has(record.code)) {
                    handleAddStock(record)
                  }
                },
              })}
            />
          )}
        </div>
        <Text type="secondary" style={{ fontSize: 12, marginTop: 8, display: 'block' }}>
          {t('watchlist.searchHint')}
        </Text>
      </Modal>
    </div>
  )
}
