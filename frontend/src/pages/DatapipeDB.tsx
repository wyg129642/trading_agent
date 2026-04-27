/**
 * Datapipe · 港擎官方数据同步 (124.71.193.17:9200, mode=down)
 * 13 个产品订阅,各自 schema 不同,统一通过 brief 字段展示。
 * 后端: /api/datapipe-db/{products,items,stats}
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Card,
  Drawer,
  Empty,
  Input,
  Space,
  Spin,
  Statistic,
  Table,
  Tag,
  Typography,
  Alert,
  Descriptions,
  Tooltip,
  Button,
} from 'antd'

const { CheckableTag } = Tag
import {
  ReloadOutlined,
  ClockCircleOutlined,
  DatabaseOutlined,
  CloudSyncOutlined,
  FileSearchOutlined,
  StockOutlined,
  ApiOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'

dayjs.extend(relativeTime)

const { Text, Paragraph, Title } = Typography

interface ProductInfo {
  product: string
  label_cn: string
  label_en: string
  has_ticker: boolean
  count: number
  today: number
  latest_update_time: string | null
}

interface StatsResponse {
  total: number
  products: number
  today_total: number
  last_7_days: { date: string; total: number; [k: string]: number | string }[]
  importer_state: {
    files_imported_ok: number
    files_with_errors: number
    last_imported_product: string | null
    last_imported_at: string | null
  } | null
}

interface BriefItem {
  id: string
  product: string
  title: string
  preview: string
  time: string | null
  update_time: string | null
  ticker: string | null
  ticker_name: string | null
  op_mode: number | null
  deleted: boolean
  imported_at: string | null
  extras: Record<string, unknown>
}

interface DetailResponse extends BriefItem {
  raw: Record<string, unknown>
}

interface ListResponse {
  items: BriefItem[]
  total: number
  page: number
  page_size: number
  has_next: boolean
}

// Tiered by独特性 × 信息密度. Tier 1 = scraper 完全没有的高价值信息;
// Tier 2 = 有补充价值; Tier 3 = 跟既有 scraper / 行情数据高度重叠或元数据.
const PRODUCT_GROUPS: { label: string; tier: 1 | 2 | 3; products: string[] }[] = [
  { label: '⭐ 投资者问答', tier: 1, products: ['QAtelconferce', 'QAirmcninfo', 'QAmessagerecord'] },
  { label: '⭐ 一致预期', tier: 1, products: ['opinion_statistic'] },
  { label: '市场情绪', tier: 2, products: ['news_skthottopics', 'postinfo_xq'] },
  { label: '调研 / 路演', tier: 2, products: ['minutsofcompsurvey', 'scheduleofalln'] },
  { label: '其他 (新闻/日历/概念)', tier: 3, products: ['news_financialflash', 'news_financial', 'fina_calendar', 'investmtcal', 'stkproperterms'] },
]


function formatRawValue(v: unknown): string {
  if (v === null || v === undefined) return '—'
  if (typeof v === 'object') return JSON.stringify(v, null, 2)
  return String(v)
}

export default function DatapipeDB() {
  const [products, setProducts] = useState<ProductInfo[]>([])
  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [activeProduct, setActiveProduct] = useState<string>('')
  const [items, setItems] = useState<BriefItem[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(20)
  const [q, setQ] = useState('')
  const [ticker, setTicker] = useState('')
  const [loading, setLoading] = useState(false)
  const [productsLoading, setProductsLoading] = useState(true)
  const [detail, setDetail] = useState<DetailResponse | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)
  const [detailLoading, setDetailLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const activeProductCfg = useMemo(
    () => products.find((p) => p.product === activeProduct),
    [products, activeProduct],
  )

  // load products + stats in parallel
  const loadMeta = useCallback(async () => {
    setProductsLoading(true)
    try {
      const [pr, st] = await Promise.all([
        api.get<{ products: ProductInfo[] }>('/datapipe-db/products'),
        api.get<StatsResponse>('/datapipe-db/stats'),
      ])
      setProducts(pr.data.products)
      setStats(st.data)
      if (!activeProduct && pr.data.products.length > 0) {
        // default to the product with the most rows
        const top = [...pr.data.products].sort((a, b) => b.count - a.count)[0]
        setActiveProduct(top.product)
      }
      setError(null)
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail
        || (err as Error)?.message
        || 'failed to load datapipe metadata'
      setError(msg)
    } finally {
      setProductsLoading(false)
    }
  }, [activeProduct])

  const loadItems = useCallback(async () => {
    if (!activeProduct) return
    setLoading(true)
    try {
      const res = await api.get<ListResponse>('/datapipe-db/items', {
        params: { product: activeProduct, page, page_size: pageSize, q: q || undefined, ticker: ticker || undefined },
      })
      setItems(res.data.items)
      setTotal(res.data.total)
      setError(null)
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail
        || (err as Error)?.message
        || 'failed to load items'
      setError(msg)
    } finally {
      setLoading(false)
    }
  }, [activeProduct, page, pageSize, q, ticker])

  useEffect(() => { loadMeta() }, [loadMeta])
  useEffect(() => { loadItems() }, [loadItems])
  useEffect(() => { setPage(1) }, [activeProduct, q, ticker])

  const openDetail = useCallback(async (item: BriefItem) => {
    setDetailOpen(true)
    setDetail(null)
    setDetailLoading(true)
    try {
      const res = await api.get<DetailResponse>(`/datapipe-db/items/${item.product}/${item.id}`)
      setDetail(res.data)
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail
        || (err as Error)?.message
        || 'failed to load detail'
      setError(msg)
      setDetailOpen(false)
    } finally {
      setDetailLoading(false)
    }
  }, [])

  const columns = useMemo(() => {
    const isQA = ['QAtelconferce', 'QAirmcninfo', 'QAmessagerecord'].includes(activeProduct)
    const isOpinion = activeProduct === 'opinion_statistic'
    const isSchedule = activeProduct === 'scheduleofalln'
    const isHotTopics = activeProduct === 'news_skthottopics'
    const isXqPost = activeProduct === 'postinfo_xq'

    const tickerCol = {
      title: '股票',
      dataIndex: 'ticker',
      key: 'ticker',
      width: 130,
      render: (_: unknown, item: BriefItem) => {
        if (!item.ticker && !item.ticker_name) return <Text type="secondary">—</Text>
        return (
          <Space direction="vertical" size={0}>
            {item.ticker && <Tag color="blue" style={{ marginRight: 0 }}>{item.ticker}</Tag>}
            {item.ticker_name && <Text style={{ fontSize: 12 }}>{item.ticker_name}</Text>}
          </Space>
        )
      },
    }
    const updateTimeCol = {
      title: '更新',
      dataIndex: 'update_time',
      key: 'update_time',
      width: 110,
      render: (v: string | null) => v ? (
        <Tooltip title={v}>
          <span style={{ fontSize: 12, color: '#94a3b8' }}>
            <ClockCircleOutlined style={{ marginRight: 4 }} />
            {dayjs(v).fromNow()}
          </span>
        </Tooltip>
      ) : <Text type="secondary">—</Text>,
    }

    // ---- 高价值: 投资者问答 (Q/A 两行) ---- //
    if (isQA) {
      return [
        {
          title: 'Q&A',
          key: 'qa',
          render: (_: unknown, item: BriefItem) => {
            const q = String(item.extras?.question || item.title || '')
            const a = String(item.extras?.answer || item.preview || '')
            return (
              <div style={{ minWidth: 320, maxWidth: 720 }}>
                <a onClick={() => openDetail(item)} style={{ display: 'block', fontWeight: 500, lineHeight: 1.5 }}>
                  <Tag color="purple" style={{ marginRight: 6 }}>问</Tag>{q}
                </a>
                {a && (
                  <div style={{ color: '#475569', fontSize: 12, marginTop: 4, lineHeight: 1.55 }}>
                    <Tag color="green" style={{ marginRight: 6 }}>答</Tag>
                    {a.length > 280 ? a.slice(0, 280) + '…' : a}
                  </div>
                )}
              </div>
            )
          },
        },
        tickerCol,
        {
          title: '提问时间',
          dataIndex: 'time',
          key: 'time',
          width: 130,
          render: (v: string | null) => v ? <span style={{ fontSize: 12 }}>{v}</span> : <Text type="secondary">—</Text>,
        },
        updateTimeCol,
      ]
    }

    // ---- 高价值: 一致预期 (纯数字, 数值列) ---- //
    if (isOpinion) {
      return [
        {
          title: '股票',
          key: 'stock',
          render: (_: unknown, item: BriefItem) => (
            <a onClick={() => openDetail(item)}>
              {item.ticker && <Tag color="blue">{item.ticker}</Tag>}
              <span style={{ fontWeight: 500 }}>{item.ticker_name || item.title}</span>
            </a>
          ),
          width: 220,
        },
        {
          title: <Tooltip title="覆盖该股的券商家数">覆盖券商</Tooltip>,
          key: 'cvg_num',
          width: 100,
          align: 'right' as const,
          render: (_: unknown, item: BriefItem) => {
            const v = item.extras?.cvg_num as number | null
            return v != null ? <Text strong>{v}</Text> : <Text type="secondary">—</Text>
          },
        },
        {
          title: <Tooltip title="今日推荐家数">今日推荐</Tooltip>,
          key: 'recom_num',
          width: 100,
          align: 'right' as const,
          render: (_: unknown, item: BriefItem) => {
            const v = item.extras?.recom_num as number | null
            return v != null ? <Tag color={v > 0 ? 'green' : 'default'}>{v}</Tag> : <Text type="secondary">—</Text>
          },
        },
        {
          title: '涨跌幅',
          key: 'chg_pct',
          width: 100,
          align: 'right' as const,
          render: (_: unknown, item: BriefItem) => {
            const v = item.extras?.chg_pct as number | null
            if (v == null) return <Text type="secondary">—</Text>
            const pct = (v * 100).toFixed(2)
            return <Text style={{ color: v > 0 ? '#dc2626' : v < 0 ? '#10b981' : '#64748b' }}>{v > 0 ? '+' : ''}{pct}%</Text>
          },
        },
        {
          title: '数据日期',
          key: 'stat_date',
          width: 110,
          render: (_: unknown, item: BriefItem) => {
            const v = item.extras?.stat_date as string | null
            const trade = item.extras?.is_trade as boolean
            return (
              <Space size={4}>
                <span style={{ fontSize: 12 }}>{v || '—'}</span>
                {!trade && <Tag color="default" style={{ fontSize: 10 }}>非交易日</Tag>}
              </Space>
            )
          },
        },
        updateTimeCol,
      ]
    }

    // ---- 路演会议日程 (会议时间 prominent) ---- //
    if (isSchedule) {
      return [
        {
          title: '会议',
          key: 'title',
          render: (_: unknown, item: BriefItem) => (
            <div style={{ minWidth: 240, maxWidth: 500 }}>
              <a onClick={() => openDetail(item)} style={{ fontWeight: 500 }}>
                {item.title || <Text type="secondary">(无标题)</Text>}
              </a>
              {item.preview && (
                <div style={{ color: '#64748b', fontSize: 11, marginTop: 4 }}>{item.preview}</div>
              )}
            </div>
          ),
        },
        {
          title: '会议时间',
          key: 'schdl_time',
          width: 150,
          render: (_: unknown, item: BriefItem) => {
            const v = item.extras?.schdl_time as string | null
            if (!v) return <Text type="secondary">—</Text>
            const isFuture = dayjs(v).isAfter(dayjs())
            return (
              <Space direction="vertical" size={0}>
                <Text style={{ fontSize: 12, color: isFuture ? '#2563eb' : '#64748b', fontWeight: isFuture ? 500 : 400 }}>
                  {v}
                </Text>
                {isFuture && <Tag color="blue" style={{ fontSize: 10, marginRight: 0 }}>{dayjs(v).fromNow()}</Tag>}
              </Space>
            )
          },
        },
        {
          title: '类型',
          key: 'schdl_type',
          width: 100,
          render: (_: unknown, item: BriefItem) => {
            const v = item.extras?.schdl_type as string | null
            return v ? <Tag>{v}</Tag> : <Text type="secondary">—</Text>
          },
        },
        tickerCol,
        {
          title: '行业',
          key: 'indsty_type',
          width: 110,
          render: (_: unknown, item: BriefItem) => {
            const v = item.extras?.indsty_type as string | null
            return v ? <Text style={{ fontSize: 12 }}>{v}</Text> : <Text type="secondary">—</Text>
          },
        },
      ]
    }

    // ---- 股吧热门话题 (阅读 + 评论 cols) ---- //
    if (isHotTopics) {
      return [
        {
          title: '话题',
          key: 'topic',
          render: (_: unknown, item: BriefItem) => (
            <div style={{ minWidth: 280, maxWidth: 600 }}>
              <a onClick={() => openDetail(item)} style={{ fontWeight: 500 }}>{item.title}</a>
              {item.preview && <div style={{ color: '#64748b', fontSize: 12, marginTop: 4 }}>{item.preview}</div>}
              {item.extras?.topic_tags ? (
                <div style={{ marginTop: 6 }}>
                  {String(item.extras.topic_tags).split('|').slice(0, 8).map((t) => (
                    <Tag key={t} style={{ fontSize: 10, marginRight: 2 }}>{t}</Tag>
                  ))}
                </div>
              ) : null}
            </div>
          ),
        },
        {
          title: '阅读',
          key: 'read_n',
          width: 90,
          align: 'right' as const,
          render: (_: unknown, item: BriefItem) => {
            const v = item.extras?.read_n as number | null
            return v != null ? <Text style={{ fontSize: 12 }}>{v >= 10000 ? `${(v / 10000).toFixed(1)}w` : v}</Text> : <Text type="secondary">—</Text>
          },
        },
        {
          title: '评论',
          key: 'comments_n',
          width: 80,
          align: 'right' as const,
          render: (_: unknown, item: BriefItem) => {
            const v = item.extras?.comments_n as number | null
            return v != null ? <Text style={{ fontSize: 12 }}>{v}</Text> : <Text type="secondary">—</Text>
          },
        },
        updateTimeCol,
      ]
    }

    // ---- 雪球发帖 (评论/点赞 cols + 链接) ---- //
    if (isXqPost) {
      return [
        {
          title: '帖子',
          key: 'post',
          render: (_: unknown, item: BriefItem) => (
            <div style={{ minWidth: 280, maxWidth: 600 }}>
              <a onClick={() => openDetail(item)} style={{ fontWeight: 500 }}>{item.title}</a>
            </div>
          ),
        },
        {
          title: '评论',
          key: 'comments_n',
          width: 70,
          align: 'right' as const,
          render: (_: unknown, item: BriefItem) => {
            const v = item.extras?.comments_n as number | null
            return v ? <Text style={{ fontSize: 12 }}>{v}</Text> : <Text type="secondary">—</Text>
          },
        },
        {
          title: '点赞',
          key: 'likes_n',
          width: 70,
          align: 'right' as const,
          render: (_: unknown, item: BriefItem) => {
            const v = item.extras?.likes_n as number | null
            return v ? <Text style={{ fontSize: 12 }}>{v}</Text> : <Text type="secondary">—</Text>
          },
        },
        {
          title: '发布',
          dataIndex: 'time',
          key: 'time',
          width: 130,
          render: (v: string | null) => v ? <span style={{ fontSize: 12 }}>{v}</span> : <Text type="secondary">—</Text>,
        },
        {
          title: '原帖',
          key: 'link',
          width: 60,
          render: (_: unknown, item: BriefItem) => {
            const v = item.extras?.source_link as string | null
            return v ? <a href={v} target="_blank" rel="noreferrer">↗</a> : null
          },
        },
      ]
    }

    // ---- 默认 (新闻/调研/日历/等) ---- //
    return [
      {
        title: '标题 / 内容',
        dataIndex: 'title',
        key: 'title',
        render: (_: unknown, item: BriefItem) => (
          <div style={{ minWidth: 280, maxWidth: 600 }}>
            <a onClick={() => openDetail(item)} style={{ fontWeight: 500 }}>
              {item.title || <Text type="secondary">(无标题)</Text>}
            </a>
            {item.preview && item.preview !== item.title && (
              <div style={{ color: '#64748b', fontSize: 12, marginTop: 4, lineHeight: 1.5 }}>
                {item.preview}
              </div>
            )}
          </div>
        ),
      },
      tickerCol,
      {
        title: '事件时间',
        dataIndex: 'time',
        key: 'time',
        width: 140,
        render: (v: string | null) => v ? <span style={{ fontSize: 12, color: '#475569' }}>{v}</span> : <Text type="secondary">—</Text>,
      },
      updateTimeCol,
    ]
  }, [activeProduct, openDetail])

  const fmtCount = (n: number) =>
    n >= 10000 ? `${(n / 10000).toFixed(1)}w` : n >= 1000 ? `${(n / 1000).toFixed(1)}k` : String(n)

  return (
    <div style={{ padding: 24 }}>
      <div style={{ marginBottom: 16, display: 'flex', alignItems: 'center', gap: 12 }}>
        <ApiOutlined style={{ fontSize: 24, color: '#2563eb' }} />
        <Title level={3} style={{ margin: 0 }}>Datapipe 数据接入</Title>
        <Text type="secondary" style={{ marginLeft: 8 }}>
          港擎官方数据同步 · 124.71.193.17:9200
        </Text>
        <Button
          icon={<ReloadOutlined />}
          size="small"
          style={{ marginLeft: 'auto' }}
          onClick={() => { loadMeta(); loadItems() }}
        >刷新</Button>
      </div>

      {error && <Alert type="error" message={error} closable onClose={() => setError(null)} style={{ marginBottom: 12 }} />}

      {/* Stats cards */}
      {stats && (
        <Card size="small" style={{ marginBottom: 16 }} bodyStyle={{ padding: '12px 16px' }}>
          <Space size="large" wrap>
            <Statistic prefix={<DatabaseOutlined />} title="总行数" value={stats.total} />
            <Statistic prefix={<FileSearchOutlined />} title="产品数" value={stats.products} />
            <Statistic prefix={<CloudSyncOutlined />} title="今日新增" value={stats.today_total} />
            {stats.importer_state && (
              <>
                <Statistic title="导入文件数" value={stats.importer_state.files_imported_ok} />
                <Statistic
                  title="导入错误"
                  value={stats.importer_state.files_with_errors}
                  valueStyle={{ color: stats.importer_state.files_with_errors > 0 ? '#ef4444' : undefined }}
                />
                {stats.importer_state.last_imported_at && (
                  <Statistic
                    title="最近导入"
                    value={dayjs(stats.importer_state.last_imported_at).fromNow()}
                    valueStyle={{ fontSize: 14 }}
                  />
                )}
              </>
            )}
          </Space>
        </Card>
      )}

      {/* Product picker — tiered by价值, tier 3 visually demoted */}
      <Card size="small" style={{ marginBottom: 12 }} bodyStyle={{ padding: '8px 12px' }}>
        {productsLoading ? (
          <Spin />
        ) : products.length === 0 ? (
          <Empty description="无 Datapipe 数据" />
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {PRODUCT_GROUPS.map((group) => (
              <div
                key={group.label}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  flexWrap: 'wrap',
                  gap: 6,
                  opacity: group.tier === 3 ? 0.65 : 1,
                }}
              >
                <Text
                  type="secondary"
                  style={{
                    fontSize: 11,
                    width: 92,
                    flexShrink: 0,
                    fontWeight: group.tier === 1 ? 600 : 400,
                    color: group.tier === 1 ? '#1d4ed8' : group.tier === 3 ? '#94a3b8' : undefined,
                  }}
                >
                  {group.label}
                </Text>
                {group.products.map((p) => {
                  const info = products.find((pp) => pp.product === p)
                  if (!info) return null
                  const active = info.product === activeProduct
                  return (
                    <CheckableTag
                      key={p}
                      checked={active}
                      onChange={() => setActiveProduct(p)}
                      style={{
                        padding: group.tier === 3 ? '1px 8px' : '2px 10px',
                        fontSize: group.tier === 3 ? 11 : 12,
                        userSelect: 'none',
                        border: active ? undefined : '1px solid #e2e8f0',
                      }}
                    >
                      {info.label_cn}
                      <span
                        style={{
                          marginLeft: 6,
                          fontSize: 11,
                          opacity: active ? 0.85 : 0.55,
                        }}
                      >
                        {fmtCount(info.count)}
                        {info.today > 0 && (
                          <span
                            style={{
                              color: active ? '#fff' : '#10b981',
                              marginLeft: 4,
                              fontWeight: 500,
                            }}
                          >
                            +{info.today}
                          </span>
                        )}
                      </span>
                    </CheckableTag>
                  )
                })}
              </div>
            ))}
          </div>
        )}
      </Card>

      {/* Source-side encoding warning for minutsofcompsurvey */}
      {activeProduct === 'minutsofcompsurvey' && (
        <Alert
          type="warning"
          showIcon
          message="港擎 Datapipe XML 源数据字符集异常 (mojibake) — 调研纪要正文显示为乱码,已反馈厂商。其它字段(标题/股票/调研机构)正常可用。"
          style={{ marginBottom: 12 }}
        />
      )}

      {/* Filters */}
      {activeProductCfg && (
        <Card size="small" style={{ marginBottom: 12 }} bodyStyle={{ padding: 12 }}>
          <Space wrap>
            <Input
              prefix={<FileSearchOutlined />}
              placeholder="标题/内容关键词"
              value={q}
              onChange={(e) => setQ(e.target.value)}
              onPressEnter={loadItems}
              allowClear
              style={{ width: 240 }}
            />
            {activeProductCfg.has_ticker && (
              <Input
                prefix={<StockOutlined />}
                placeholder="股票代码 (如 600535)"
                value={ticker}
                onChange={(e) => setTicker(e.target.value)}
                onPressEnter={loadItems}
                allowClear
                style={{ width: 200 }}
              />
            )}
            <Text type="secondary" style={{ fontSize: 12 }}>
              当前: <strong>{activeProductCfg.label_cn}</strong> ·
              {' '}{activeProductCfg.count.toLocaleString()} 行 ·
              {' '}今日 {activeProductCfg.today} ·
              {' '}最近更新 {activeProductCfg.latest_update_time || '—'}
            </Text>
          </Space>
        </Card>
      )}

      {/* Table */}
      <Card size="small" bodyStyle={{ padding: 0 }}>
        <Table
          rowKey="id"
          columns={columns}
          dataSource={items}
          loading={loading}
          size="middle"
          pagination={{
            current: page,
            pageSize,
            total,
            showSizeChanger: true,
            pageSizeOptions: [20, 50, 100],
            showTotal: (t) => `共 ${t.toLocaleString()} 行`,
            onChange: (p, ps) => { setPage(p); setPageSize(ps) },
          }}
        />
      </Card>

      {/* Detail drawer */}
      <Drawer
        width={720}
        open={detailOpen}
        onClose={() => setDetailOpen(false)}
        title={detail ? (detail.title || '(无标题)') : 'Datapipe 详情'}
      >
        {detailLoading ? (
          <div style={{ textAlign: 'center', padding: 48 }}><Spin /></div>
        ) : detail ? (
          <>
            <Descriptions size="small" column={1} bordered style={{ marginBottom: 16 }}>
              <Descriptions.Item label="产品">{detail.product}</Descriptions.Item>
              {detail.ticker && (
                <Descriptions.Item label="股票">
                  <Tag color="blue">{detail.ticker}</Tag>
                  {detail.ticker_name && <Text>{detail.ticker_name}</Text>}
                </Descriptions.Item>
              )}
              {detail.time && <Descriptions.Item label="事件时间">{detail.time}</Descriptions.Item>}
              {detail.update_time && <Descriptions.Item label="更新时间">{detail.update_time}</Descriptions.Item>}
              {detail.imported_at && (
                <Descriptions.Item label="入库时间">
                  {dayjs(detail.imported_at).format('YYYY-MM-DD HH:mm:ss')}
                </Descriptions.Item>
              )}
              {detail.deleted && <Descriptions.Item label="状态"><Tag color="red">已删除 (op_mode=2)</Tag></Descriptions.Item>}
            </Descriptions>

            {detail.preview && (
              <Card size="small" title="预览" style={{ marginBottom: 16 }}>
                <Paragraph style={{ whiteSpace: 'pre-wrap', margin: 0 }}>{detail.preview}</Paragraph>
              </Card>
            )}

            <Card
              size="small"
              title={<span>原始数据 (Mongo doc) <Text type="secondary" style={{ fontSize: 12, fontWeight: 'normal' }}>· {Object.keys(detail.raw).length} 字段</Text></span>}
              bodyStyle={{ padding: 0 }}
            >
              <table style={{ width: '100%', fontSize: 12, borderCollapse: 'collapse' }}>
                <tbody>
                  {Object.entries(detail.raw).map(([k, v]) => (
                    <tr key={k} style={{ borderBottom: '1px solid #f1f5f9' }}>
                      <td style={{ padding: '6px 12px', color: '#64748b', verticalAlign: 'top', width: 160, fontFamily: 'monospace' }}>{k}</td>
                      <td style={{ padding: '6px 12px', wordBreak: 'break-word', whiteSpace: 'pre-wrap' }}>
                        {formatRawValue(v)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Card>
          </>
        ) : null}
      </Drawer>
    </div>
  )
}
