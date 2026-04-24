/**
 * AlphaEngine · 基于 MongoDB (`alphaengine.*`) 的视图.
 *
 * 4 个 UI 类别 (后端与 scraper 同名):
 *   summary       → 纪要
 *   chinaReport   → 国内研报 (含 PDF)
 *   foreignReport → 海外研报 (含 PDF)
 *   news          → 资讯 (HTML EOD wrap 等)
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useParams } from 'react-router-dom'
import { notification } from 'antd'
import { useWebSocket } from '../hooks/useWebSocket'
import {
  Alert,
  Button,
  Card,
  Drawer,
  Empty,
  Input,
  List,
  Pagination,
  Segmented,
  Space,
  Spin,
  Statistic,
  Tag,
  Typography,
} from 'antd'
import {
  ReloadOutlined,
  LinkOutlined,
  FileTextOutlined,
  FilePdfOutlined,
  GlobalOutlined,
  NotificationOutlined,
  BankOutlined,
  TeamOutlined,
  ReadOutlined,
} from '@ant-design/icons'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'

dayjs.extend(relativeTime)

const { Text, Title, Paragraph } = Typography

type CategoryKey = 'summary' | 'chinaReport' | 'foreignReport' | 'news'

const CATEGORY_META: Record<
  CategoryKey,
  { label: string; color: string; icon: React.ReactNode; hasPdf: boolean }
> = {
  summary: { label: '纪要', color: '#2563eb', icon: <TeamOutlined />, hasPdf: false },
  chinaReport: { label: '国内研报', color: '#f59e0b', icon: <FilePdfOutlined />, hasPdf: true },
  foreignReport: { label: '海外研报', color: '#10b981', icon: <GlobalOutlined />, hasPdf: true },
  news: { label: '资讯', color: '#ef4444', icon: <NotificationOutlined />, hasPdf: false },
}

interface Item {
  id: string
  doc_id: string
  summary_id: string
  category: CategoryKey
  category_label: string
  title: string
  title_cn: string
  release_time: string | null
  release_time_ms: number | null
  publish_time: string | null
  publish_time_ms: number | null
  rank_date: string | null
  rank_date_ms: number | null
  organization: string
  institution_names: string[]
  authors: string[]
  document_type_name: string | null
  type_full_name: string | null
  first_type_name: string | null
  type_show_name: string | null
  industry_names: string[]
  company_codes: string[]
  company_names: string[]
  doc_icon: string | null
  page_num: number
  web_url: string | null
  has_pdf: boolean
  pdf_size_bytes: number
  preview: string
  content_length: number
  crawled_at: string | null
  _canonical_tickers: string[]
}

interface ListResponse {
  items: Item[]
  total: number
  page: number
  page_size: number
  has_next: boolean
}

interface DetailResponse extends Item {
  content_md: string
  doc_introduce: string
  pdf_rel_path: string | null
  company_multi_map: Record<string, string[]>
  first_industry_name: string[]
  company_show_name: string[]
  pdf_download_error: string
}

interface StatsResponse {
  total: number
  per_category: Record<CategoryKey, number>
  today: Record<CategoryKey, number>
  last_7_days: { date: string; summary?: number; chinaReport?: number; foreignReport?: number; news?: number }[]
  crawler_state: {
    category: string
    in_progress: boolean
    last_processed_at: string | null
    last_run_end_at: string | null
    last_run_stats: { added?: number; skipped?: number; failed?: number }
  }[]
  top_organizations: Record<CategoryKey, { name: string; count: number }[]>
  latest_per_category: Record<CategoryKey, string | null>
  daily_platform_stats: Record<
    CategoryKey,
    { platform_count: number; in_db: number; missing: number }
  > | null
}

const CATEGORY_KEYS: CategoryKey[] = ['summary', 'chinaReport', 'foreignReport', 'news']

const SLUG_TO_CATEGORY: Record<string, CategoryKey> = {
  summary: 'summary',
  'china-report': 'chinaReport',
  chinaReport: 'chinaReport',
  'foreign-report': 'foreignReport',
  foreignReport: 'foreignReport',
  news: 'news',
}

export default function AlphaEngineDB() {
  const { category: slug } = useParams<{ category?: string }>()
  const initialCategory: CategoryKey = (slug && SLUG_TO_CATEGORY[slug]) || 'summary'

  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [statsLoading, setStatsLoading] = useState(true)
  const [statsError, setStatsError] = useState<string | null>(null)

  const [category, setCategory] = useState<CategoryKey>(initialCategory)
  const [items, setItems] = useState<Item[]>([])
  const [itemsLoading, setItemsLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [total, setTotal] = useState(0)

  const [query, setQuery] = useState('')
  const [organizationFilter, setOrganizationFilter] = useState('')
  const [tickerFilter, setTickerFilter] = useState('')
  const [industryFilter, setIndustryFilter] = useState('')
  // 默认 rank_date = 平台重新索引时间, 与 alphaengine.top 原站列表排序一致.
  const [sortBy, setSortBy] = useState<'rank_date' | 'publish_time' | 'crawled_at'>(
    'rank_date',
  )

  const [detailOpen, setDetailOpen] = useState(false)
  const [detail, setDetail] = useState<DetailResponse | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)

  useEffect(() => {
    const fromUrl = (slug && SLUG_TO_CATEGORY[slug]) || 'summary'
    setCategory(fromUrl)
    setPage(1)
  }, [slug])

  const loadStats = useCallback(async () => {
    setStatsLoading(true)
    setStatsError(null)
    try {
      const res = await api.get<StatsResponse>('/alphaengine-db/stats')
      setStats(res.data)
    } catch (err: any) {
      setStatsError(err?.response?.data?.detail || err?.message || '加载失败')
    } finally {
      setStatsLoading(false)
    }
  }, [])

  const loadItems = useCallback(async () => {
    setItemsLoading(true)
    try {
      const res = await api.get<ListResponse>('/alphaengine-db/items', {
        params: {
          category,
          page,
          page_size: 20,
          sort: sortBy,
          q: query || undefined,
          organization: organizationFilter || undefined,
          ticker: tickerFilter || undefined,
          industry: industryFilter || undefined,
        },
      })
      setItems(res.data.items)
      setTotal(res.data.total)
    } catch {
      setItems([])
      setTotal(0)
    } finally {
      setItemsLoading(false)
    }
  }, [category, page, sortBy, query, organizationFilter, tickerFilter, industryFilter])

  useEffect(() => {
    loadStats()
  }, [loadStats])

  useEffect(() => {
    loadItems()
  }, [loadItems])

  // Live push via backend WebSocket (/ws/feed). When scrapers insert a new
  // AlphaEngine item, they publish to Redis `crawl:new-item`; ws/feed wraps
  // it as `{type:"crawl_new_item", event:{...}}`. We react by:
  //   1. Showing a toast for items in any category (so the user sees activity
  //      even while viewing a different tab).
  //   2. Silently reloading the list + stats if the new item's category
  //      matches what the user is currently viewing (without opening a new
  //      drawer or losing scroll position).
  const pushNotifiedRef = useRef<Set<string>>(new Set())
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const scheduleRefresh = useCallback(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => {
      loadItems()
      loadStats()
    }, 1500)
  }, [loadItems, loadStats])

  const handleWsMessage = useCallback(
    (data: any) => {
      if (!data || data.type !== 'crawl_new_item') return
      const ev = data.event || {}
      if (ev.platform !== 'alphaengine') return
      // Dedup bursts — the scraper emits every doc, user only needs one toast
      // every ~10s per category.
      const dedupKey = `${ev.category}:${Math.floor(Date.now() / 10_000)}`
      if (!pushNotifiedRef.current.has(dedupKey)) {
        pushNotifiedRef.current.add(dedupKey)
        // Cap cache so we don't leak forever.
        if (pushNotifiedRef.current.size > 500) {
          pushNotifiedRef.current = new Set([...pushNotifiedRef.current].slice(-200))
        }
        const catMeta = CATEGORY_META[(ev.category as CategoryKey) || 'summary']
        notification.open({
          message: `${catMeta?.label || ev.category} · 新条目`,
          description: (ev.title || '').slice(0, 80),
          placement: 'bottomRight',
          duration: 3,
        })
      }
      if (ev.category === category) {
        scheduleRefresh()
      }
    },
    [category, scheduleRefresh],
  )
  useWebSocket(handleWsMessage)

  const openDetail = useCallback(
    async (item: Item) => {
      setDetailOpen(true)
      setDetailLoading(true)
      setDetail(null)
      try {
        const res = await api.get<DetailResponse>(
          `/alphaengine-db/items/${category}/${encodeURIComponent(item.id)}`,
        )
        setDetail(res.data)
      } catch {
        setDetail(null)
      } finally {
        setDetailLoading(false)
      }
    },
    [category],
  )

  const todayCount = stats?.today[category] ?? (stats ? 0 : null)
  const totalForCategory = stats?.per_category[category] ?? 0
  const latest = stats?.latest_per_category?.[category]
  const platformDaily = stats?.daily_platform_stats?.[category]
  const meta = CATEGORY_META[category]

  const segOptions = useMemo(
    () =>
      CATEGORY_KEYS.map((k) => {
        const m = CATEGORY_META[k]
        const today = stats?.today[k] ?? 0
        return {
          label: (
            <span>
              {m.icon} {m.label}
              {today > 0 ? (
                <Tag color="green" style={{ marginLeft: 6, fontSize: 10 }}>
                  今日 +{today}
                </Tag>
              ) : null}
            </span>
          ),
          value: k,
        }
      }),
    [stats],
  )

  // PDF is fetched as blob via axios (to carry the Authorization header —
  // <iframe src="/api/..."> wouldn't, giving a 401). Result is a blob: URL
  // that works equally for the inline iframe preview and the download button.
  const [pdfBlobUrl, setPdfBlobUrl] = useState<string | null>(null)
  useEffect(() => {
    if (!detail?.has_pdf) {
      setPdfBlobUrl(null)
      return
    }
    let cancelled = false
    let createdUrl: string | null = null
    api
      .get(
        `/alphaengine-db/items/${category}/${encodeURIComponent(detail.id)}/pdf`,
        { responseType: 'blob' },
      )
      .then((res) => {
        if (cancelled) return
        const blob = new Blob([res.data], { type: 'application/pdf' })
        createdUrl = URL.createObjectURL(blob)
        setPdfBlobUrl(createdUrl)
      })
      .catch(() => {
        if (!cancelled) setPdfBlobUrl(null)
      })
    return () => {
      cancelled = true
      if (createdUrl) URL.revokeObjectURL(createdUrl)
    }
  }, [category, detail?.id, detail?.has_pdf])

  const pdfDownloadUrl = useMemo(() => {
    if (!detail || !detail.has_pdf) return null
    return `/api/alphaengine-db/items/${category}/${encodeURIComponent(detail.id)}/pdf?download=1`
  }, [category, detail])

  return (
    <div style={{ padding: 20 }}>
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          marginBottom: 16,
        }}
      >
        <div>
          <Title level={3} style={{ margin: 0 }}>
            <BankOutlined /> AlphaEngine · 阿尔法引擎
          </Title>
          <Text type="secondary">
            alphaengine.top · 纪要 / 国内研报 / 海外研报 / 资讯 (JWT 认证 · 研报 PDF 本地缓存)
          </Text>
        </div>
        <a onClick={loadStats} style={{ fontSize: 13 }}>
          <ReloadOutlined /> 刷新
        </a>
      </div>

      {statsError && (
        <Alert
          type="warning"
          showIcon
          message="无法从 MongoDB 加载 AlphaEngine 数据"
          description={statsError}
          style={{ marginBottom: 16 }}
        />
      )}

      <Spin spinning={statsLoading}>
        <Card size="small" bodyStyle={{ padding: 14 }} style={{ marginBottom: 16 }}>
          <Space size={18} align="center" wrap>
            <Statistic
              title={
                <span style={{ color: meta.color }}>
                  {meta.icon} 今日新增 · {meta.label}
                </span>
              }
              value={todayCount ?? 0}
              valueStyle={{ color: meta.color, fontSize: 28 }}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              {dayjs().format('YYYY-MM-DD')}
              {latest && <> · 最近发布 {latest}</>}
              {totalForCategory > 0 && <> · 该分类累计 {totalForCategory.toLocaleString()}</>}
            </Text>
            {platformDaily && (
              <Text type="secondary" style={{ fontSize: 12 }}>
                平台 {platformDaily.platform_count} · 已入库 {platformDaily.in_db} ·{' '}
                {platformDaily.missing > 0 ? (
                  <span style={{ color: '#f59e0b' }}>缺失 {platformDaily.missing}</span>
                ) : (
                  <span style={{ color: '#10b981' }}>无缺失</span>
                )}
              </Text>
            )}
          </Space>
        </Card>
      </Spin>

      <Card size="small">
        <Space direction="vertical" size={10} style={{ width: '100%', marginBottom: 12 }}>
          <Segmented
            value={category}
            onChange={(v) => {
              setCategory(v as CategoryKey)
              setPage(1)
            }}
            options={segOptions as any}
          />
          <Space wrap>
            <Input.Search
              placeholder="搜索标题 / 摘要 / 正文"
              allowClear
              style={{ width: 320 }}
              onSearch={(v) => {
                setQuery(v)
                setPage(1)
              }}
            />
            <Input
              placeholder="机构"
              allowClear
              style={{ width: 160 }}
              onChange={(e) => {
                setOrganizationFilter(e.target.value)
                setPage(1)
              }}
            />
            <Input
              placeholder="股票 / 公司"
              allowClear
              style={{ width: 160 }}
              onChange={(e) => {
                setTickerFilter(e.target.value)
                setPage(1)
              }}
            />
            <Input
              placeholder="行业"
              allowClear
              style={{ width: 140 }}
              onChange={(e) => {
                setIndustryFilter(e.target.value)
                setPage(1)
              }}
            />
            <Segmented
              size="small"
              value={sortBy}
              onChange={(v) => {
                setSortBy(v as typeof sortBy)
                setPage(1)
              }}
              options={[
                { label: '最近更新', value: 'rank_date' },
                { label: '发布时间', value: 'publish_time' },
                { label: '最近入库', value: 'crawled_at' },
              ]}
            />
          </Space>
        </Space>

        <Spin spinning={itemsLoading}>
          {items.length === 0 && !itemsLoading ? (
            <Empty description={`暂无 ${meta.label} 数据`} style={{ padding: 40 }} />
          ) : (
            <List
              dataSource={items}
              renderItem={(it) => (
                <List.Item
                  style={{ cursor: 'pointer', padding: '10px 6px' }}
                  onClick={() => openDetail(it)}
                  actions={[
                    it.has_pdf ? (
                      <Tag color="red" key="pdf" icon={<FilePdfOutlined />}>
                        PDF
                      </Tag>
                    ) : null,
                    it.page_num > 0 ? (
                      <Tag key="pg">{it.page_num} 页</Tag>
                    ) : null,
                  ].filter(Boolean)}
                >
                  <List.Item.Meta
                    title={
                      <Space size={4} wrap>
                        {it.first_type_name && (
                          <Tag color={meta.color} style={{ fontSize: 11 }}>
                            {it.type_full_name || it.first_type_name}
                          </Tag>
                        )}
                        <Text strong>{it.title}</Text>
                        {it.title_cn && it.title_cn !== it.title && (
                          <Text type="secondary" style={{ fontSize: 12 }}>
                            · {it.title_cn}
                          </Text>
                        )}
                      </Space>
                    }
                    description={
                      <div>
                        <Space size={8} wrap style={{ marginBottom: 4, fontSize: 12 }}>
                          {it.organization && (
                            <Text type="secondary">
                              <BankOutlined /> {it.organization}
                            </Text>
                          )}
                          {(it.authors || []).length > 0 && (
                            <Text type="secondary">✎ {it.authors.join('、')}</Text>
                          )}
                          {(() => {
                            // Primary time = the dimension user is sorting by.
                            // Always show rank_date when different from publish_time
                            // (the delta indicates the platform re-indexed the doc).
                            const rank = (it.rank_date || '').slice(0, 16)
                            const pub = (it.publish_time || '').slice(0, 16)
                            const primary =
                              sortBy === 'publish_time'
                                ? pub || rank
                                : sortBy === 'crawled_at'
                                ? dayjs(it.crawled_at || '').format('MM-DD HH:mm')
                                : rank || pub
                            if (!primary) return null
                            const showSecondary =
                              sortBy !== 'crawled_at' && rank && pub && rank !== pub
                            return (
                              <Text type="secondary">
                                🕒 {primary}
                                {showSecondary && (
                                  <Text type="secondary" style={{ fontSize: 10, marginLeft: 4 }}>
                                    (原发 {sortBy === 'publish_time' ? rank : pub})
                                  </Text>
                                )}
                              </Text>
                            )
                          })()}
                          {(it.industry_names || []).slice(0, 2).map((ind) => (
                            <Tag key={ind} color="blue-inverse" style={{ fontSize: 10 }}>
                              {ind}
                            </Tag>
                          ))}
                          {(it.company_codes || []).slice(0, 3).map((c, i) => (
                            <Tag key={c} color="purple">
                              {c}
                              {it.company_names[i] && it.company_names[i] !== c
                                ? ` · ${it.company_names[i]}`
                                : ''}
                            </Tag>
                          ))}
                          {(it._canonical_tickers || []).slice(0, 3).map((t) => (
                            <Tag key={t} color="geekblue">
                              {t}
                            </Tag>
                          ))}
                        </Space>
                        {it.preview && (
                          <Paragraph
                            type="secondary"
                            style={{ fontSize: 12, marginBottom: 0 }}
                            ellipsis={{ rows: 2 }}
                          >
                            {it.preview}
                          </Paragraph>
                        )}
                      </div>
                    }
                  />
                </List.Item>
              )}
            />
          )}
          <div style={{ textAlign: 'right', marginTop: 12 }}>
            <Pagination
              current={page}
              total={total}
              pageSize={20}
              onChange={(p) => setPage(p)}
              showSizeChanger={false}
              showTotal={(t) => `共 ${t.toLocaleString()} 条`}
            />
          </div>
        </Spin>
      </Card>

      <Drawer
        open={detailOpen}
        onClose={() => setDetailOpen(false)}
        width={Math.min(980, window.innerWidth - 120)}
        title={
          detail ? (
            <Space size={6}>
              <Tag color={meta.color}>{meta.label}</Tag>
              <span>{detail.title}</span>
            </Space>
          ) : (
            '加载中…'
          )
        }
        destroyOnClose
      >
        {detailLoading ? (
          <Spin />
        ) : !detail ? (
          <Empty description="加载失败" />
        ) : (
          <Space direction="vertical" size={14} style={{ width: '100%' }}>
            <Space size={8} wrap>
              {detail.organization && (
                <Tag color="geekblue" icon={<BankOutlined />}>
                  {detail.organization}
                </Tag>
              )}
              {(detail.authors || []).map((a) => (
                <Tag key={a}>✎ {a}</Tag>
              ))}
              {detail.type_full_name && <Tag color={meta.color}>{detail.type_full_name}</Tag>}
              {detail.release_time && (
                <Tag color="default">🕒 {detail.release_time}</Tag>
              )}
              {detail.page_num > 0 && <Tag>{detail.page_num} 页</Tag>}
              {detail.web_url && (
                <Button
                  size="small"
                  type="link"
                  icon={<LinkOutlined />}
                  href={detail.web_url}
                  target="_blank"
                >
                  打开源页
                </Button>
              )}
              {detail.has_pdf && (
                <Button
                  size="small"
                  type="primary"
                  icon={<FilePdfOutlined />}
                  disabled={!pdfBlobUrl}
                  onClick={() => {
                    if (!pdfBlobUrl) return
                    // Download via a transient <a download>, so we get a
                    // readable filename instead of a random blob uuid.
                    const a = document.createElement('a')
                    a.href = pdfBlobUrl
                    a.download = `alphaengine-${detail.id.slice(0, 12)}.pdf`
                    document.body.appendChild(a)
                    a.click()
                    document.body.removeChild(a)
                  }}
                >
                  {pdfBlobUrl
                    ? `下载 PDF (${(detail.pdf_size_bytes / 1024).toFixed(0)} KB)`
                    : '加载 PDF 中…'}
                </Button>
              )}
            </Space>

            {(detail.industry_names || []).length > 0 && (
              <Space wrap>
                {detail.industry_names.map((i) => (
                  <Tag key={i} color="blue">
                    {i}
                  </Tag>
                ))}
              </Space>
            )}

            {(detail.company_codes || []).length > 0 && (
              <Space wrap>
                {detail.company_codes.map((c, i) => (
                  <Tag key={c} color="purple">
                    {c}
                    {detail.company_names[i] && detail.company_names[i] !== c
                      ? ` · ${detail.company_names[i]}`
                      : ''}
                  </Tag>
                ))}
                {(detail._canonical_tickers || []).map((t) => (
                  <Tag key={t} color="geekblue">
                    {t}
                  </Tag>
                ))}
              </Space>
            )}

            {detail.pdf_download_error && (
              <Alert
                type="warning"
                showIcon
                message="PDF 本地缓存失败"
                description={detail.pdf_download_error}
              />
            )}

            {detail.has_pdf ? (
              pdfBlobUrl ? (
                <iframe
                  src={pdfBlobUrl}
                  title="pdf"
                  style={{
                    width: '100%',
                    height: 640,
                    border: '1px solid #f0f0f0',
                    borderRadius: 4,
                  }}
                />
              ) : (
                <Card
                  size="small"
                  style={{ minHeight: 120, display: 'flex', alignItems: 'center', justifyContent: 'center' }}
                >
                  <Spin tip="加载 PDF..." />
                </Card>
              )
            ) : detail.content_md || detail.doc_introduce ? (
              <Card size="small" type="inner" title={<><ReadOutlined /> 摘要 / 预览</>}>
                <ReactMarkdown remarkPlugins={[remarkGfm]}>
                  {detail.content_md || detail.doc_introduce}
                </ReactMarkdown>
              </Card>
            ) : (
              <Empty
                description="本条目无可预览内容 (可能需在源页阅读)"
                image={Empty.PRESENTED_IMAGE_SIMPLE}
              />
            )}
          </Space>
        )}
      </Drawer>
    </div>
  )
}
