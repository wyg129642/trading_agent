/**
 * Roadshows page — shows AI-summarized roadshow transcripts
 * with stock/sector tags, sentiment, and key points.
 */
import { useEffect, useState, useCallback } from 'react'
import {
  Card,
  List,
  Tag,
  Select,
  Space,
  Input,
  Tabs,
  Typography,
  Spin,
  Drawer,
  Empty,
  Tooltip,
} from 'antd'
import {
  SearchOutlined,
  ClockCircleOutlined,
  RiseOutlined,
  FallOutlined,
  RobotOutlined,
  BulbOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { useSearchParams } from 'react-router-dom'
import api from '../services/api'
import { useFavorites } from '../hooks/useFavorites'
import FavoriteButton from '../components/FavoriteButton'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'

dayjs.extend(relativeTime)

const { Text, Paragraph } = Typography

interface Enrichment {
  summary?: string
  relevance_score?: number
  tickers?: string[]
  sectors?: string[]
  tags?: string[]
  sentiment?: string
  key_points?: string[]
}

interface RoadshowCNItem {
  trans_id: string
  roadshow_id: string
  show_title: string
  company: string | null
  guest: string | null
  stime: string | null
  word_count: number
  est_reading_time: string
  ind_json: { code?: string; name?: string }[]
  trans_source: string
  enrichment: Enrichment
  is_enriched: boolean
}

interface RoadshowUSItem {
  trans_id: string
  show_title: string
  company: string | null
  stime: string | null
  word_count: number
  trans_source: string
  quarter_year: string | null
  ai_auxiliary_json: any
  enrichment: Enrichment
  is_enriched: boolean
}

interface ListResponse<T> {
  items: T[]
  total: number
  page: number
  page_size: number
  has_next: boolean
}

const SENTIMENT_MAP: Record<string, { color: string; label: string; icon: any }> = {
  bullish: { color: '#52c41a', label: '看多', icon: <RiseOutlined /> },
  bearish: { color: '#ff4d4f', label: '看空', icon: <FallOutlined /> },
  neutral: { color: '#d9d9d9', label: '中性', icon: null },
}

const HOURS_OPTIONS = [
  { value: 24, label: '24小时' },
  { value: 48, label: '48小时' },
  { value: 168, label: '7天' },
]

export default function AlphaPaiRoadshows() {
  const { t } = useTranslation()
  const [searchParams] = useSearchParams()
  const initialTab = searchParams.get('tab') || 'cn'
  const [activeTab, setActiveTab] = useState(initialTab)
  const { favoriteIds, toggleFavorite } = useFavorites(activeTab === 'cn' ? 'roadshow_cn' : 'roadshow_us')

  // --- CN State ---
  const [cnItems, setCnItems] = useState<RoadshowCNItem[]>([])
  const [cnTotal, setCnTotal] = useState(0)
  const [cnPage, setCnPage] = useState(1)
  const [cnLoading, setCnLoading] = useState(true)
  const [cnCompany, setCnCompany] = useState('')
  const [cnHours, setCnHours] = useState(48)

  // --- US State ---
  const [usItems, setUsItems] = useState<RoadshowUSItem[]>([])
  const [usTotal, setUsTotal] = useState(0)
  const [usPage, setUsPage] = useState(1)
  const [usLoading, setUsLoading] = useState(true)
  const [usHours, setUsHours] = useState(48)

  // --- Drawer ---
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [drawerItem, setDrawerItem] = useState<RoadshowCNItem | RoadshowUSItem | null>(null)
  const [drawerContent, setDrawerContent] = useState<string>('')
  const [drawerLoading, setDrawerLoading] = useState(false)

  // --- CN Fetching (AI source only by default) ---
  const fetchCN = useCallback(async () => {
    setCnLoading(true)
    try {
      const params: Record<string, any> = {
        page: cnPage,
        page_size: 20,
        hours: cnHours,
        trans_source: 'AI',
      }
      if (cnCompany) params.company = cnCompany

      const res = await api.get<ListResponse<RoadshowCNItem>>('/alphapai/roadshows/cn', { params })
      setCnItems(res.data.items)
      setCnTotal(res.data.total)
    } catch (e) {
      console.error(e)
    } finally {
      setCnLoading(false)
    }
  }, [cnPage, cnHours, cnCompany])

  useEffect(() => {
    if (activeTab === 'cn') fetchCN()
  }, [fetchCN, activeTab])

  useEffect(() => {
    setCnPage(1)
  }, [cnCompany, cnHours])

  // --- US Fetching ---
  const fetchUS = useCallback(async () => {
    setUsLoading(true)
    try {
      const res = await api.get<ListResponse<RoadshowUSItem>>('/alphapai/roadshows/us', {
        params: { page: usPage, page_size: 20, hours: usHours },
      })
      setUsItems(res.data.items)
      setUsTotal(res.data.total)
    } catch (e) {
      console.error(e)
    } finally {
      setUsLoading(false)
    }
  }, [usPage, usHours])

  useEffect(() => {
    if (activeTab === 'us') fetchUS()
  }, [fetchUS, activeTab])

  useEffect(() => {
    setUsPage(1)
  }, [usHours])

  // --- Open Drawer ---
  const openDrawer = useCallback(async (item: RoadshowCNItem | RoadshowUSItem, type: 'cn' | 'us') => {
    setDrawerItem(item)
    setDrawerOpen(true)
    setDrawerContent('')
    setDrawerLoading(true)
    try {
      const res = await api.get(`/alphapai/roadshows/${type}/${item.trans_id}`)
      setDrawerContent(res.data?.content_cached || '')
    } catch (e) {
      console.error(e)
    } finally {
      setDrawerLoading(false)
    }
  }, [])

  const closeDrawer = useCallback(() => {
    setDrawerOpen(false)
    setDrawerItem(null)
    setDrawerContent('')
  }, [])

  // --- Render enrichment tags ---
  const renderEnrichment = (enr: Enrichment) => {
    const sentiment = SENTIMENT_MAP[enr.sentiment || '']
    const tickers = enr.tickers || []
    const sectors = enr.sectors || []

    return (
      <>
        {/* Sentiment + stock/sector tags */}
        <Space size={4} wrap style={{ marginBottom: 6 }}>
          {sentiment && (
            <Tag color={sentiment.color} icon={sentiment.icon}>
              {sentiment.label}
            </Tag>
          )}
          {tickers.map((tk, i) => (
            <Tag key={`t-${i}`} color="blue" style={{ fontSize: 12 }}>
              {tk}
            </Tag>
          ))}
          {sectors.map((s, i) => (
            <Tag key={`s-${i}`} color="cyan" style={{ fontSize: 12 }}>
              {s}
            </Tag>
          ))}
        </Space>

        {/* AI Summary */}
        {enr.summary && (
          <div
            style={{
              background: '#f8fafc',
              borderRadius: 4,
              padding: '6px 10px',
              marginBottom: 6,
              fontSize: 13,
              color: '#475569',
              lineHeight: 1.6,
            }}
          >
            {enr.summary}
          </div>
        )}
      </>
    )
  }

  // --- Render CN List ---
  const renderCNList = () => (
    <div>
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap size="middle">
          <Input
            prefix={<SearchOutlined />}
            placeholder="搜索券商/公司..."
            value={cnCompany}
            onChange={(e) => setCnCompany(e.target.value)}
            allowClear
            style={{ width: 200 }}
          />
          <Select
            value={cnHours}
            onChange={setCnHours}
            style={{ width: 120 }}
            options={HOURS_OPTIONS}
          />
          <Text type="secondary">{cnTotal} 条AI纪要</Text>
        </Space>
      </Card>

      <List
        loading={cnLoading}
        dataSource={cnItems}
        locale={{ emptyText: <Empty description="暂无AI纪要数据" /> }}
        pagination={{
          current: cnPage,
          total: cnTotal,
          pageSize: 20,
          onChange: setCnPage,
          showSizeChanger: false,
        }}
        renderItem={(item) => {
          const enr = item.enrichment || {}
          const sentiment = SENTIMENT_MAP[enr.sentiment || '']
          const industries = item.ind_json
            ?.filter((i) => i.name)
            .map((i) => i.name!) || []

          return (
            <Card
              size="small"
              style={{
                marginBottom: 10,
                cursor: 'pointer',
                borderLeft: sentiment ? `3px solid ${sentiment.color}` : '3px solid #e2e8f0',
              }}
              onClick={() => openDrawer(item, 'cn')}
              hoverable
            >
              <div>
                {/* Title */}
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                  <div style={{ fontWeight: 600, fontSize: 15, marginBottom: 6 }}>
                    <Tag color="green" style={{ marginRight: 6 }}>AI纪要</Tag>
                    {industries.map((ind, i) => (
                      <Tag key={i} style={{ fontSize: 11 }}>{ind}</Tag>
                    ))}
                    <span style={{ marginLeft: 4 }}>{item.show_title}</span>
                  </div>
                  <FavoriteButton
                    itemType="roadshow_cn"
                    itemId={item.trans_id}
                    favoriteIds={favoriteIds}
                    onToggle={toggleFavorite}
                  />
                </div>

                {/* Enrichment info */}
                {item.is_enriched && renderEnrichment(enr)}

                {/* Meta row */}
                <div style={{ display: 'flex', gap: 12, fontSize: 12, color: '#8c8c8c' }}>
                  {item.company && <span>{item.company}</span>}
                  {item.stime && (
                    <span>
                      <ClockCircleOutlined style={{ marginRight: 3 }} />
                      {dayjs(item.stime).tz('Asia/Shanghai').fromNow()}
                    </span>
                  )}
                  {enr.relevance_score != null && enr.relevance_score > 0 && (
                    <Tooltip title={`AI评分: ${(enr.relevance_score * 100).toFixed(0)}%`}>
                      <Tag
                        color={enr.relevance_score >= 0.7 ? 'green' : 'default'}
                        style={{ fontSize: 11, lineHeight: '18px', margin: 0 }}
                      >
                        {(enr.relevance_score * 100).toFixed(0)}%
                      </Tag>
                    </Tooltip>
                  )}
                </div>
              </div>
            </Card>
          )
        }}
      />
    </div>
  )

  // --- Render US List ---
  const renderUSList = () => (
    <div>
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap size="middle">
          <Select
            value={usHours}
            onChange={setUsHours}
            style={{ width: 120 }}
            options={HOURS_OPTIONS}
          />
          <Text type="secondary">{usTotal} 条美股纪要</Text>
        </Space>
      </Card>

      <List
        loading={usLoading}
        dataSource={usItems}
        locale={{ emptyText: <Empty description="暂无美股纪要" /> }}
        pagination={{
          current: usPage,
          total: usTotal,
          pageSize: 20,
          onChange: setUsPage,
          showSizeChanger: false,
        }}
        renderItem={(item) => {
          const enr = item.enrichment || {}
          const sentiment = SENTIMENT_MAP[enr.sentiment || '']

          return (
            <Card
              size="small"
              style={{
                marginBottom: 10,
                cursor: 'pointer',
                borderLeft: sentiment ? `3px solid ${sentiment.color}` : '3px solid #e2e8f0',
              }}
              onClick={() => openDrawer(item, 'us')}
              hoverable
            >
              <div>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                  <div style={{ fontWeight: 600, fontSize: 15, marginBottom: 6 }}>
                    <Tag color="cyan">US</Tag>
                    {item.quarter_year && <Tag>{item.quarter_year}</Tag>}
                    {item.show_title}
                  </div>
                  <FavoriteButton
                    itemType="roadshow_us"
                    itemId={item.trans_id}
                    favoriteIds={favoriteIds}
                    onToggle={toggleFavorite}
                  />
                </div>

                {item.is_enriched && renderEnrichment(enr)}

                <div style={{ display: 'flex', gap: 12, fontSize: 12, color: '#8c8c8c' }}>
                  {item.company && <span>{item.company}</span>}
                  {item.stime && (
                    <span>
                      <ClockCircleOutlined style={{ marginRight: 3 }} />
                      {dayjs(item.stime).tz('Asia/Shanghai').fromNow()}
                    </span>
                  )}
                </div>
              </div>
            </Card>
          )
        }}
      />
    </div>
  )

  return (
    <div>
      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        items={[
          { key: 'cn', label: 'A股纪要 (AI摘要)' },
          { key: 'us', label: '美股纪要' },
        ]}
      />

      {activeTab === 'cn' ? renderCNList() : renderUSList()}

      {/* Detail Drawer */}
      <Drawer
        title={drawerItem?.show_title || '纪要详情'}
        placement="right"
        width={720}
        open={drawerOpen}
        onClose={closeDrawer}
        destroyOnClose
      >
        {drawerLoading ? (
          <div style={{ textAlign: 'center', padding: 60 }}>
            <Spin size="large" />
          </div>
        ) : (
          <div>
            {/* Enrichment section */}
            {drawerItem?.enrichment && drawerItem.is_enriched && (
              <Card size="small" style={{ marginBottom: 16 }} title={<><BulbOutlined /> AI分析</>}>
                {renderEnrichment(drawerItem.enrichment)}
                {drawerItem.enrichment.key_points && drawerItem.enrichment.key_points.length > 0 && (
                  <div style={{ marginTop: 8 }}>
                    <Text strong style={{ fontSize: 13 }}>核心要点:</Text>
                    <ul style={{ margin: '4px 0 0', paddingLeft: 20 }}>
                      {drawerItem.enrichment.key_points.map((pt, i) => (
                        <li key={i} style={{ marginBottom: 4, fontSize: 13 }}>{pt}</li>
                      ))}
                    </ul>
                  </div>
                )}
              </Card>
            )}

            {/* Meta */}
            {drawerItem && (
              <div style={{ marginBottom: 12 }}>
                <Space wrap>
                  {'company' in drawerItem && drawerItem.company && (
                    <Tag color="blue">{drawerItem.company}</Tag>
                  )}
                  {'guest' in drawerItem && (drawerItem as RoadshowCNItem).guest && (
                    <Tag>{(drawerItem as RoadshowCNItem).guest}</Tag>
                  )}
                </Space>
                <div style={{ marginTop: 4 }}>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    {dayjs(drawerItem.stime).tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm')}
                  </Text>
                </div>
              </div>
            )}

            {/* Full content */}
            {drawerContent ? (
              <Card size="small" title="纪要全文">
                <div
                  style={{ maxHeight: 600, overflow: 'auto', fontSize: 14, lineHeight: 1.8 }}
                  dangerouslySetInnerHTML={{ __html: drawerContent }}
                />
              </Card>
            ) : (
              <Empty description="暂无全文内容" />
            )}
          </div>
        )}
      </Drawer>
    </div>
  )
}
