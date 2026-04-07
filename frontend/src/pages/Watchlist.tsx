import { useEffect, useState } from 'react'
import {
  Card,
  Button,
  Modal,
  Form,
  Input,
  Tag,
  Space,
  List,
  Select,
  Empty,
  Popconfirm,
  Typography,
  message,
} from 'antd'
import { PlusOutlined, DeleteOutlined, StarOutlined } from '@ant-design/icons'
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
  items: WatchlistItem[]
  item_count: number
}

const TYPE_COLORS: Record<string, string> = {
  ticker: 'blue',
  sector: 'purple',
  keyword: 'green',
}

export default function WatchlistPage() {
  const { t } = useTranslation()
  const [watchlists, setWatchlists] = useState<Watchlist[]>([])
  const [loading, setLoading] = useState(true)
  const [createModalOpen, setCreateModalOpen] = useState(false)
  const [addItemModal, setAddItemModal] = useState<{ watchlistId: string } | null>(null)
  const [form] = Form.useForm()
  const [itemForm] = Form.useForm()

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
    try {
      await api.post(`/watchlists/${addItemModal.watchlistId}/items`, values)
      message.success(t('watchlist.addSuccess'))
      setAddItemModal(null)
      itemForm.resetFields()
      fetchWatchlists()
    } catch (e: any) {
      message.error(e.response?.data?.detail || t('common.error'))
    }
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

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
        <Typography.Title level={4} style={{ margin: 0 }}>
          <StarOutlined style={{ marginRight: 8 }} />
          {t('watchlist.myWatchlists')}
        </Typography.Title>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateModalOpen(true)}>
          {t('watchlist.newWatchlist')}
        </Button>
      </div>

      {watchlists.length === 0 && !loading ? (
        <Empty description={t('watchlist.emptyDesc')}>
          <Button type="primary" onClick={() => setCreateModalOpen(true)}>
            {t('watchlist.newWatchlist')}
          </Button>
        </Empty>
      ) : (
        <List
          loading={loading}
          dataSource={watchlists}
          renderItem={(wl) => (
            <Card
              key={wl.id}
              size="small"
              style={{ marginBottom: 12 }}
              title={
                <Space>
                  <Text strong>{wl.name}</Text>
                  <Text type="secondary">({wl.item_count} {t('watchlist.items')})</Text>
                </Space>
              }
              extra={
                <Space>
                  <Button
                    size="small"
                    onClick={() => setAddItemModal({ watchlistId: wl.id })}
                  >
                    <PlusOutlined /> {t('watchlist.addItem')}
                  </Button>
                  <Popconfirm
                    title={t('watchlist.deleteConfirm')}
                    onConfirm={() => handleDelete(wl.id)}
                    okText={t('common.confirm')}
                    cancelText={t('common.cancel')}
                  >
                    <Button size="small" danger>
                      <DeleteOutlined />
                    </Button>
                  </Popconfirm>
                </Space>
              }
            >
              {wl.description && (
                <Text type="secondary" style={{ display: 'block', marginBottom: 8, fontSize: 13 }}>
                  {wl.description}
                </Text>
              )}
              <Space wrap>
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
                    {item.display_name || item.value}
                  </Tag>
                ))}
              </Space>
            </Card>
          )}
        />
      )}

      {/* Create Watchlist Modal */}
      <Modal
        title={t('watchlist.newWatchlist')}
        open={createModalOpen}
        onCancel={() => setCreateModalOpen(false)}
        onOk={() => form.submit()}
        okText={t('common.confirm')}
        cancelText={t('common.cancel')}
      >
        <Form form={form} onFinish={handleCreate} layout="vertical">
          <Form.Item
            name="name"
            label={t('watchlist.name')}
            rules={[{ required: true }]}
          >
            <Input placeholder={t('watchlist.namePlaceholder')} />
          </Form.Item>
          <Form.Item name="description" label={t('watchlist.description')}>
            <Input.TextArea rows={2} placeholder={t('watchlist.descPlaceholder')} />
          </Form.Item>
        </Form>
      </Modal>

      {/* Add Item Modal */}
      <Modal
        title={t('watchlist.addItem')}
        open={!!addItemModal}
        onCancel={() => setAddItemModal(null)}
        onOk={() => itemForm.submit()}
        okText={t('common.confirm')}
        cancelText={t('common.cancel')}
      >
        <Form form={itemForm} onFinish={handleAddItem} layout="vertical">
          <Form.Item
            name="item_type"
            label={t('watchlist.type')}
            rules={[{ required: true }]}
          >
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
          >
            <Input placeholder={t('watchlist.valuePlaceholder')} />
          </Form.Item>
          <Form.Item name="display_name" label={t('watchlist.displayName')}>
            <Input placeholder={t('watchlist.displayNamePlaceholder')} />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  )
}
