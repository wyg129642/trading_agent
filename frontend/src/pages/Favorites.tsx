import { useState, useEffect, useCallback } from 'react'
import {
  Card,
  List,
  Tag,
  Tabs,
  Popconfirm,
  Button,
  Typography,
  Empty,
  Space,
  message,
} from 'antd'
import {
  DeleteOutlined,
  StarFilled,
  FileTextOutlined,
  ReadOutlined,
  AudioOutlined,
  CommentOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'
import api from '../services/api'
import dayjs from 'dayjs'

const { Text } = Typography

interface FavoriteItem {
  id: number
  item_type: string
  item_id: string
  note: string | null
  title: string | null
  created_at: string
}

interface FavoriteListResponse {
  favorites: FavoriteItem[]
  total: number
}

const ITEM_TYPE_CONFIG: Record<
  string,
  { color: string; label: string; icon: React.ReactNode; getPath: (itemId: string) => string }
> = {
  news: {
    color: 'blue',
    label: '新闻资讯',
    icon: <FileTextOutlined />,
    getPath: (itemId) => `/news/${itemId}`,
  },
  wechat: {
    color: 'green',
    label: '微信文章',
    icon: <ReadOutlined />,
    getPath: () => '/alphapai/feed',
  },
  roadshow_cn: {
    color: 'orange',
    label: 'A股路演',
    icon: <AudioOutlined />,
    getPath: () => '/alphapai/roadshows',
  },
  roadshow_us: {
    color: 'purple',
    label: '美股路演',
    icon: <AudioOutlined />,
    getPath: () => '/alphapai/roadshows',
  },
  comment: {
    color: 'cyan',
    label: '分析师点评',
    icon: <CommentOutlined />,
    getPath: () => '/alphapai/comments',
  },
}

const TAB_ITEMS = [
  { key: '', label: '全部' },
  { key: 'news', label: '新闻资讯' },
  { key: 'wechat', label: '微信文章' },
  { key: 'roadshow_cn', label: 'A股路演' },
  { key: 'roadshow_us', label: '美股路演' },
  { key: 'comment', label: '分析师点评' },
]

const PAGE_SIZE = 20

export default function Favorites() {
  const { t } = useTranslation()
  const navigate = useNavigate()

  const [favorites, setFavorites] = useState<FavoriteItem[]>([])
  const [loading, setLoading] = useState(true)
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [activeTab, setActiveTab] = useState('')

  const fetchFavorites = useCallback(async () => {
    setLoading(true)
    try {
      const params: Record<string, string | number> = {
        offset: (page - 1) * PAGE_SIZE,
        limit: PAGE_SIZE,
      }
      if (activeTab) {
        params.item_type = activeTab
      }
      const res = await api.get<FavoriteListResponse>('/favorites', { params })
      setFavorites(res.data.favorites)
      setTotal(res.data.total)
    } catch (e) {
      console.error(e)
      message.error(t('common.error'))
    } finally {
      setLoading(false)
    }
  }, [page, activeTab, t])

  useEffect(() => {
    fetchFavorites()
  }, [fetchFavorites])

  const handleDelete = async (favoriteId: number, e?: React.MouseEvent) => {
    if (e) e.stopPropagation()
    try {
      await api.delete(`/favorites/${favoriteId}`)
      message.success(t('favorites.remove'))
      fetchFavorites()
    } catch {
      message.error(t('common.error'))
    }
  }

  const handleTabChange = (key: string) => {
    setActiveTab(key)
    setPage(1)
  }

  const handleRowClick = (item: FavoriteItem) => {
    const config = ITEM_TYPE_CONFIG[item.item_type]
    if (config) navigate(config.getPath(item.item_id))
  }

  return (
    <div>
      <Typography.Title level={4} style={{ margin: 0, marginBottom: 16 }}>
        <StarFilled style={{ marginRight: 8, color: '#faad14' }} />
        {t('favorites.title')}
      </Typography.Title>

      <Card>
        <Tabs
          activeKey={activeTab}
          onChange={handleTabChange}
          items={TAB_ITEMS.map((tab) => ({ key: tab.key, label: tab.label }))}
        />

        {favorites.length === 0 && !loading ? (
          <Empty description={t('favorites.empty', '暂无收藏内容')} />
        ) : (
          <List
            loading={loading}
            dataSource={favorites}
            pagination={{
              current: page,
              pageSize: PAGE_SIZE,
              total,
              onChange: (p) => setPage(p),
              showSizeChanger: false,
              showTotal: (tot) => `${t('common.total')} ${tot} ${t('common.items')}`,
            }}
            renderItem={(item) => {
              const config = ITEM_TYPE_CONFIG[item.item_type]
              return (
                <List.Item
                  style={{ cursor: 'pointer' }}
                  onClick={() => handleRowClick(item)}
                  actions={[
                    <Popconfirm
                      key="delete"
                      title={t('favorites.deleteConfirm', '确认取消收藏？')}
                      onConfirm={(e) => handleDelete(item.id, e as unknown as React.MouseEvent)}
                      onCancel={(e) => e?.stopPropagation()}
                      okText={t('common.confirm')}
                      cancelText={t('common.cancel')}
                    >
                      <Button
                        type="text"
                        danger
                        size="small"
                        icon={<DeleteOutlined />}
                        onClick={(e) => e.stopPropagation()}
                      />
                    </Popconfirm>,
                  ]}
                >
                  <div style={{ display: 'flex', alignItems: 'center', gap: 12, flex: 1, minWidth: 0 }}>
                    <Tag color={config?.color || 'default'} icon={config?.icon} style={{ flexShrink: 0 }}>
                      {config?.label || item.item_type}
                    </Tag>
                    <Text
                      ellipsis
                      style={{ flex: 1, minWidth: 0, fontWeight: 500 }}
                    >
                      {item.title || item.item_id}
                    </Text>
                    <Text type="secondary" style={{ fontSize: 12, flexShrink: 0 }}>
                      {dayjs(item.created_at).tz('Asia/Shanghai').format('MM-DD HH:mm')}
                    </Text>
                  </div>
                </List.Item>
              )
            }}
          />
        )}
      </Card>
    </div>
  )
}
