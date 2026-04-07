import { useEffect, useState, useCallback } from 'react'
import { Card, Row, Col, Statistic, Typography, Spin, Empty, Button, Space, Tag } from 'antd'
import {
  FileTextOutlined,
  AudioOutlined,
  CommentOutlined,
  LeftOutlined,
  RightOutlined,
  CalendarOutlined,
  RiseOutlined,
  FallOutlined,
  FireOutlined,
  SyncOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import api from '../services/api'
import dayjs from 'dayjs'

const { Title, Text } = Typography

interface DigestData {
  id: number
  digest_date: string
  content_markdown: string
  stats: {
    articles?: number
    roadshows_cn?: number
    comments?: number
    hot_tickers?: { name: string; count: number }[]
    hot_sectors?: { name: string; count: number }[]
    bullish_count?: number
    bearish_count?: number
  }
  generated_at: string | null
  model_used: string
}

interface StatsData {
  articles_total: number
  articles_today: number
  roadshows_cn_total: number
  roadshows_cn_today: number
  roadshows_us_total: number
  roadshows_us_today: number
  comments_total: number
  comments_today: number
  enriched_total: number
  last_sync_at: string | null
}

export default function AlphaPaiDigest() {
  const { t } = useTranslation()
  const [stats, setStats] = useState<StatsData | null>(null)
  const [digest, setDigest] = useState<DigestData | null>(null)
  const [digestList, setDigestList] = useState<DigestData[]>([])
  const [currentIndex, setCurrentIndex] = useState(0)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    Promise.all([
      api.get('/alphapai/stats'),
      api.get('/alphapai/digests/latest'),
      api.get('/alphapai/digests', { params: { limit: 7 } }),
    ])
      .then(([statsRes, latestRes, listRes]) => {
        setStats(statsRes.data)
        setDigest(latestRes.data)
        const list = Array.isArray(listRes.data) ? listRes.data : listRes.data?.items || []
        setDigestList(list)
        setCurrentIndex(0)
      })
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [])

  const navigateDigest = useCallback(
    (direction: 'prev' | 'next') => {
      const newIndex = direction === 'prev' ? currentIndex + 1 : currentIndex - 1
      if (newIndex < 0 || newIndex >= digestList.length) return
      setCurrentIndex(newIndex)
      setDigest(digestList[newIndex])
    },
    [currentIndex, digestList],
  )

  const renderMarkdown = (content: string): string => {
    return content
      .replace(/### (.+)/g, '<h4 style="margin:16px 0 8px;color:#1e293b;font-size:15px;">$1</h4>')
      .replace(/## (.+)/g, '<h3 style="margin:20px 0 10px;color:#1e293b;font-size:16px;border-bottom:1px solid #e2e8f0;padding-bottom:6px;">$1</h3>')
      .replace(/# (.+)/g, '<h2 style="margin:24px 0 12px;color:#0f172a;font-size:18px;">$1</h2>')
      .replace(/\*\*(.+?)\*\*/g, '<strong style="color:#1e293b;">$1</strong>')
      .replace(/\*(.+?)\*/g, '<em>$1</em>')
      .replace(/^- (.+)$/gm, '<li style="margin-bottom:4px;line-height:1.7;">$1</li>')
      .replace(/(<li[^>]*>.*<\/li>\n?)+/g, '<ul style="margin:8px 0;padding-left:20px;">$&</ul>')
      .replace(/\n\n/g, '<br/>')
      .replace(/\n/g, '<br/>')
  }

  if (loading) {
    return (
      <div style={{ textAlign: 'center', padding: 100 }}>
        <Spin size="large" />
      </div>
    )
  }

  const digestStats = digest?.stats || {}
  const hotTickers = digestStats.hot_tickers || []
  const hotSectors = digestStats.hot_sectors || []

  return (
    <div>
      {/* Stats Row */}
      <Row gutter={[16, 16]}>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic
              title="今日公众号文章"
              value={stats?.articles_today || 0}
              prefix={<FileTextOutlined />}
              valueStyle={{ fontSize: 20 }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic
              title="今日会议纪要"
              value={stats?.roadshows_cn_today || 0}
              prefix={<AudioOutlined />}
              valueStyle={{ fontSize: 20 }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic
              title="今日点评"
              value={stats?.comments_today || 0}
              prefix={<CommentOutlined />}
              valueStyle={{ fontSize: 20 }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic
              title="AI已处理"
              value={stats?.enriched_total || 0}
              prefix={<SyncOutlined />}
              valueStyle={{ fontSize: 20, color: '#2563eb' }}
            />
          </Card>
        </Col>
      </Row>

      {/* Hot Stocks & Sectors */}
      {(hotTickers.length > 0 || hotSectors.length > 0) && (
        <Card size="small" style={{ marginTop: 16 }}>
          <Row gutter={24}>
            {hotTickers.length > 0 && (
              <Col span={12}>
                <div style={{ marginBottom: 8 }}>
                  <FireOutlined style={{ color: '#ef4444', marginRight: 6 }} />
                  <Text strong>热门关注股票</Text>
                </div>
                <Space wrap>
                  {hotTickers.slice(0, 8).map((t, i) => (
                    <Tag key={i} color={i < 3 ? 'red' : 'default'}>
                      {t.name} ({t.count})
                    </Tag>
                  ))}
                </Space>
              </Col>
            )}
            {hotSectors.length > 0 && (
              <Col span={12}>
                <div style={{ marginBottom: 8 }}>
                  <FireOutlined style={{ color: '#f59e0b', marginRight: 6 }} />
                  <Text strong>热门行业板块</Text>
                </div>
                <Space wrap>
                  {hotSectors.slice(0, 6).map((s, i) => (
                    <Tag key={i} color={i < 3 ? 'orange' : 'default'}>
                      {s.name} ({s.count})
                    </Tag>
                  ))}
                </Space>
              </Col>
            )}
          </Row>
          {(digestStats.bullish_count != null || digestStats.bearish_count != null) && (
            <div style={{ marginTop: 12, display: 'flex', gap: 16 }}>
              {digestStats.bullish_count != null && (
                <Tag color="green" icon={<RiseOutlined />}>
                  看多信号 {digestStats.bullish_count}
                </Tag>
              )}
              {digestStats.bearish_count != null && (
                <Tag color="red" icon={<FallOutlined />}>
                  看空信号 {digestStats.bearish_count}
                </Tag>
              )}
            </div>
          )}
        </Card>
      )}

      {/* Digest Content */}
      <Card
        style={{ marginTop: 16 }}
        title={
          <Space>
            <CalendarOutlined />
            <span>每日研究简报</span>
            {digest && (
              <Text type="secondary" style={{ fontWeight: 400, fontSize: 13 }}>
                {dayjs(digest.digest_date).tz('Asia/Shanghai').format('YYYY-MM-DD')}
                {digest.generated_at && (
                  <span> (生成于 {dayjs(digest.generated_at).tz('Asia/Shanghai').format('HH:mm')})</span>
                )}
              </Text>
            )}
          </Space>
        }
        extra={
          <Space>
            <Button
              icon={<LeftOutlined />}
              size="small"
              disabled={currentIndex >= digestList.length - 1}
              onClick={() => navigateDigest('prev')}
            >
              更早
            </Button>
            <Button
              icon={<RightOutlined />}
              size="small"
              disabled={currentIndex <= 0}
              onClick={() => navigateDigest('next')}
            >
              更新
            </Button>
          </Space>
        }
      >
        {digest && digest.content_markdown ? (
          <div
            className="digest-content"
            style={{
              lineHeight: 1.8,
              fontSize: 14,
              color: 'rgba(0,0,0,0.85)',
            }}
            dangerouslySetInnerHTML={{ __html: renderMarkdown(digest.content_markdown) }}
          />
        ) : (
          <Empty
            description={
              <span>
                暂无简报
                <br />
                <Text type="secondary" style={{ fontSize: 12 }}>
                  简报将在数据累积后自动生成
                </Text>
              </span>
            }
          />
        )}
      </Card>

      {/* Sync info */}
      {stats?.last_sync_at && (
        <div style={{ marginTop: 8, textAlign: 'right' }}>
          <Text type="secondary" style={{ fontSize: 12 }}>
            最后同步: {dayjs(stats.last_sync_at).tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')}
          </Text>
        </div>
      )}
    </div>
  )
}
