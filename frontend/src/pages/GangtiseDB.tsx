/**
 * Gangtise · 基于 MongoDB (`gangtise.*`) 的视图。
 *
 * 3 个数据类型：
 *   summary   → 纪要
 *   research  → 研报 (含 PDF)
 *   chief     → 首席观点
 */
import { useCallback, useEffect, useRef, useState } from 'react'
import { useParams, useSearchParams } from 'react-router-dom'
import {
  Alert,
  Button,
  Card,
  Drawer,
  Empty,
  Input,
  List,
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
  ClockCircleOutlined,
  FileTextOutlined,
  AuditOutlined,
  MessageOutlined,
  StockOutlined,
  FundProjectionScreenOutlined,
  FilePdfOutlined,
  BankOutlined,
  UserOutlined,
  DownloadOutlined,
} from '@ant-design/icons'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'

dayjs.extend(relativeTime)

const { Text, Title } = Typography

type CategoryKey = 'summary' | 'research' | 'chief'

const CATEGORY_META: Record<
  CategoryKey,
  { label: string; color: string; icon: React.ReactNode }
> = {
  summary: { label: '纪要', color: '#2563eb', icon: <FileTextOutlined /> },
  research: { label: '研报', color: '#ef4444', icon: <AuditOutlined /> },
  chief: { label: '首席观点', color: '#10b981', icon: <MessageOutlined /> },
}

// 首席观点 4 个 variant 的颜色方案 (色相按"内/外", "机构/独立"分开):
//   domestic_institution  绿 — 内资 + 机构
//   foreign_institution   蓝 — 外资 + 机构
//   foreign_independent   紫 — 外资 + 独立
//   kol                   橙 — 大V (网红式, 橙色提示"社交")
const CHIEF_VARIANT_COLORS: Record<string, string> = {
  domestic_institution: 'green',
  foreign_institution: 'geekblue',
  foreign_independent: 'purple',
  kol: 'orange',
}

interface Stock {
  code: string
  name: string
  rating?: string | null
  rating_change?: string | null
}

interface Item {
  id: string
  category: CategoryKey
  category_label: string
  title: string
  release_time: string | null
  release_time_ms: number | null
  organization: string
  analysts: string[]
  stocks: Stock[]
  industries: string[]
  column_names: string[]
  rpt_type_name: string
  pages: number
  head_party: boolean
  foreign_party: boolean
  first_coverage: boolean
  has_audio: boolean
  web_url: string | null
  preview: string
  content_length: number
  brief_length: number
  has_pdf: boolean
  pdf_size_bytes: number
  research_directions: string[]
  guest: string
  // chief 专区分区: domestic_institution / foreign_institution /
  // foreign_independent / kol  (一条记录可能跨多个 variant, backend 返回主值)
  chief_variant: string | null
  chief_variant_name: string | null
  crawled_at: string | null
}

interface ListResponse {
  items: Item[]
  total: number
  page: number
  page_size: number
  has_next: boolean
}

interface StatsResponse {
  total: number
  per_category: Record<CategoryKey, number>
  today: Record<CategoryKey, number>
  latest_per_category: Record<CategoryKey, string | null>
  last_7_days: { date: string; summary?: number; research?: number; chief?: number }[]
  crawler_state: {
    category: string
    in_progress: boolean
    last_processed_at: string | null
    last_run_end_at: string | null
    last_run_stats: { added?: number; updated?: number; skipped?: number; failed?: number }
  }[]
  top_organizations: Record<CategoryKey, { name: string; count: number }[]>
  daily_platform_stats: Record<
    CategoryKey,
    { platform_count: number; in_db: number; missing: number }
  > | null
}

interface DetailResponse extends Item {
  content_md: string
  brief_md: string
  description_md: string
  msg_text: string[]
  pdf_rel_path: string | null
  source_name: string
  location: string
  researcher: string
}

// URL slug → backend category key
const SLUG_TO_CATEGORY: Record<string, CategoryKey> = {
  summary: 'summary',
  research: 'research',
  chief: 'chief',
}

export default function GangtiseDB() {
  // URL-param 决定初始分类 (/gangtise/summary, /gangtise/research, /gangtise/chief)
  // 不匹配时 fallback 到 "summary"
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
  // chief 专属: 分区筛选 (空 = 全部)
  const [chiefVariant, setChiefVariant] = useState<string>('')
  // research 专属: 内资/外资 (空 = 全部)
  const [researchOrigin, setResearchOrigin] = useState<string>('')

  const [detailOpen, setDetailOpen] = useState(false)
  const [detail, setDetail] = useState<DetailResponse | null>(null)
  const [detailError, setDetailError] = useState<string | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)

  // Inline PDF preview state — renders iframe in the detail drawer so
  // research PDFs stay in-app instead of popping a new tab. Same pattern
  // as JinmenReports.tsx: fetch blob (carries auth) → createObjectURL →
  // revoke on close.
  const [pdfBlobUrl, setPdfBlobUrl] = useState<string | null>(null)
  const [pdfLoading, setPdfLoading] = useState(false)
  const [pdfError, setPdfError] = useState<string | null>(null)
  const [pdfVisible, setPdfVisible] = useState(false)

  const clearPdf = useCallback(() => {
    setPdfVisible(false)
    setPdfError(null)
    if (pdfBlobUrl) {
      URL.revokeObjectURL(pdfBlobUrl)
      setPdfBlobUrl(null)
    }
  }, [pdfBlobUrl])

  // 侧栏切换时 URL 变 → 同步内部 category state + 重置页码
  useEffect(() => {
    const fromUrl = (slug && SLUG_TO_CATEGORY[slug]) || 'summary'
    setCategory(fromUrl)
    setPage(1)
  }, [slug])

  const loadStats = useCallback(async () => {
    setStatsLoading(true)
    setStatsError(null)
    try {
      const res = await api.get<StatsResponse>('/gangtise-db/stats')
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
      const res = await api.get<ListResponse>('/gangtise-db/items', {
        params: {
          category,
          page,
          page_size: 20,
          q: query || undefined,
          organization: organizationFilter || undefined,
          ticker: tickerFilter || undefined,
          industry: industryFilter || undefined,
          chief_variant:
            category === 'chief' && chiefVariant ? chiefVariant : undefined,
          research_origin:
            category === 'research' && researchOrigin ? researchOrigin : undefined,
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
  }, [category, page, query, organizationFilter, tickerFilter, industryFilter, chiefVariant, researchOrigin])

  useEffect(() => {
    loadStats()
  }, [loadStats])

  useEffect(() => {
    loadItems()
  }, [loadItems])

  // Inline preview: fetch PDF as blob and mount in iframe inside the drawer.
  const loadPdfInline = useCallback(async (itemId: string) => {
    setPdfLoading(true)
    setPdfError(null)
    try {
      const res = await api.get(`/gangtise-db/items/research/${encodeURIComponent(itemId)}/pdf`, {
        responseType: 'blob',
        timeout: 60000,
      })
      const blob = new Blob([res.data], { type: 'application/pdf' })
      const url = URL.createObjectURL(blob)
      setPdfBlobUrl((prev) => {
        if (prev) URL.revokeObjectURL(prev)
        return url
      })
      setPdfVisible(true)
    } catch (err: any) {
      const msg = err?.response?.data?.detail
        || (err?.response?.status ? `HTTP ${err.response.status}` : null)
        || err?.message || '加载 PDF 失败'
      setPdfError(typeof msg === 'string' ? msg : JSON.stringify(msg))
    } finally {
      setPdfLoading(false)
    }
  }, [])

  // Download-to-disk: hits same endpoint with ?download=1 and triggers an
  // <a download> click. No iframe, no state change.
  const downloadPdf = useCallback(async (itemId: string, title: string) => {
    try {
      const res = await api.get(`/gangtise-db/items/research/${encodeURIComponent(itemId)}/pdf`, {
        params: { download: 1 },
        responseType: 'blob',
        timeout: 60000,
      })
      const blob = new Blob([res.data], { type: 'application/pdf' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `${(title || itemId.slice(0, 12)).replace(/[\\/:*?"<>|\r\n\t]/g, '_').slice(0, 120)}.pdf`
      document.body.appendChild(a); a.click(); document.body.removeChild(a)
      setTimeout(() => URL.revokeObjectURL(url), 60_000)
    } catch (err: any) {
      const msg = err?.response?.data?.detail || err?.message || '下载失败'
      // eslint-disable-next-line no-alert
      alert(msg)
    }
  }, [])

  const openDetail = useCallback(
    async (item: Item, autoPreviewPdf = false) => {
      setDetailOpen(true)
      setDetailLoading(true)
      setDetail(null)
      setDetailError(null)
      clearPdf()
      // Fire PDF preview in parallel with detail fetch — saves one click
      // for users who clicked the PDF tag in the list.
      if (autoPreviewPdf && item.has_pdf) loadPdfInline(item.id)
      try {
        const res = await api.get<DetailResponse>(
          `/gangtise-db/items/${category}/${encodeURIComponent(item.id)}`,
        )
        setDetail(res.data)
      } catch (e: any) {
        setDetail(null)
        const status = e?.response?.status
        if (status === 404) {
          setDetailError(`该条目 (id=${item.id}) 尚未同步到本地数据库 — 爬虫下一轮会拉取`)
        } else {
          setDetailError(
            e?.response?.data?.detail || e?.message || '加载详情失败',
          )
        }
      } finally {
        setDetailLoading(false)
      }
    },
    [category, clearPdf, loadPdfInline],
  )

  // Deep-link: ?open=<id> opens the drawer directly — used by
  // GangtisePlatformInfo feed widgets to jump to a DB detail view.
  const [searchParams] = useSearchParams()
  const openParam = searchParams.get('open')
  const lastOpenedRef = useRef<string | null>(null)
  useEffect(() => {
    if (openParam && openParam !== lastOpenedRef.current) {
      lastOpenedRef.current = openParam
      openDetail({ id: openParam } as Item)
    }
  }, [openParam, openDetail])

  const todayCount = stats?.today[category] ?? (stats ? 0 : null) // null = still loading
  const totalForCategory = stats?.per_category[category] ?? 0
  const latest = stats?.latest_per_category?.[category]
  const platformDaily = stats?.daily_platform_stats?.[category]

  // Segmented options showing today increments for each category
  const segOptions = (['summary', 'research', 'chief'] as CategoryKey[]).map((k) => {
    const meta = CATEGORY_META[k]
    const today = stats?.today[k] ?? 0
    return {
      label: (
        <span>
          {meta.icon} {meta.label}
          {today > 0 ? (
            <Tag color="green" style={{ marginLeft: 6, fontSize: 10 }}>
              今日 +{today}
            </Tag>
          ) : null}
        </span>
      ),
      value: k,
    }
  })

  const formatSize = (bytes: number): string => {
    if (!bytes) return ''
    if (bytes < 1024) return `${bytes} B`
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  }

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
            <FundProjectionScreenOutlined /> Gangtise
          </Title>
          <Text type="secondary">open.gangtise.com · 纪要 + 研报 + 首席观点</Text>
        </div>
        <a onClick={loadStats} style={{ fontSize: 13 }}>
          <ReloadOutlined /> 刷新
        </a>
      </div>

      {statsError && (
        <Alert
          type="warning"
          showIcon
          message="无法从 MongoDB 加载 Gangtise 数据"
          description={statsError}
          style={{ marginBottom: 16 }}
        />
      )}

      <Spin spinning={statsLoading}>
        <Card size="small" bodyStyle={{ padding: 14 }} style={{ marginBottom: 16 }}>
          <Space size={18} align="center" wrap>
            <Statistic
              title={
                <span style={{ color: CATEGORY_META[category].color }}>
                  {CATEGORY_META[category].icon} 今日新增 · {CATEGORY_META[category].label}
                </span>
              }
              value={todayCount ?? 0}
              valueStyle={{ color: CATEGORY_META[category].color, fontSize: 28 }}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              {dayjs().format('YYYY-MM-DD')}
              {latest && <> · 最近发布 {latest}</>}
              {totalForCategory > 0 && <> · 该分类累计 {totalForCategory}</>}
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
          {category === 'chief' && (
            <Segmented
              value={chiefVariant || 'all'}
              onChange={(v) => {
                setChiefVariant(v === 'all' ? '' : String(v))
                setPage(1)
              }}
              options={[
                { label: '全部', value: 'all' },
                { label: '内资机构观点', value: 'domestic_institution' },
                { label: '外资机构观点', value: 'foreign_institution' },
                { label: '外资独立观点', value: 'foreign_independent' },
                { label: '大V观点', value: 'kol' },
              ]}
            />
          )}
          {category === 'research' && (
            <Segmented
              value={researchOrigin || 'all'}
              onChange={(v) => {
                setResearchOrigin(v === 'all' ? '' : String(v))
                setPage(1)
              }}
              options={[
                { label: '全部', value: 'all' },
                { label: '内资研报', value: 'domestic' },
                { label: '外资研报', value: 'foreign' },
              ]}
            />
          )}
          <Space wrap>
            <Input.Search
              placeholder="搜索标题 / 摘要 / 正文"
              allowClear
              style={{ width: 300 }}
              onSearch={(v) => {
                setQuery(v)
                setPage(1)
              }}
            />
            <Input
              placeholder="机构"
              allowClear
              prefix={<BankOutlined />}
              style={{ width: 160 }}
              onPressEnter={(e) => {
                setOrganizationFilter((e.target as HTMLInputElement).value)
                setPage(1)
              }}
            />
            <Input
              placeholder="Ticker / 名称"
              allowClear
              prefix={<StockOutlined />}
              style={{ width: 160 }}
              onPressEnter={(e) => {
                setTickerFilter((e.target as HTMLInputElement).value)
                setPage(1)
              }}
            />
            <Input
              placeholder="行业"
              allowClear
              style={{ width: 160 }}
              onPressEnter={(e) => {
                setIndustryFilter((e.target as HTMLInputElement).value)
                setPage(1)
              }}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              共 {total} 条
            </Text>
          </Space>
        </Space>

        <List
          loading={itemsLoading}
          dataSource={items}
          locale={{ emptyText: <Empty description="暂无数据" /> }}
          pagination={{
            current: page,
            pageSize: 20,
            total,
            showSizeChanger: false,
            onChange: (p) => setPage(p),
          }}
          renderItem={(item) => {
            const meta = CATEGORY_META[item.category]
            return (
              <List.Item
                key={item.id}
                style={{ cursor: 'pointer' }}
                onClick={() => openDetail(item)}
              >
                <List.Item.Meta
                  title={
                    <Space size={6} wrap>
                      <Tag color={meta.color} style={{ color: '#fff', border: 'none' }}>
                        {meta.icon} {item.category_label}
                      </Tag>
                      {item.chief_variant_name && (
                        <Tag
                          color={CHIEF_VARIANT_COLORS[item.chief_variant || ''] || 'default'}
                          style={{ fontSize: 11 }}
                        >
                          {item.chief_variant_name}
                        </Tag>
                      )}
                      {item.rpt_type_name && (
                        <Tag style={{ fontSize: 11 }}>{item.rpt_type_name}</Tag>
                      )}
                      {item.first_coverage && (
                        <Tag color="gold" style={{ fontSize: 11 }}>
                          首次覆盖
                        </Tag>
                      )}
                      {item.head_party && (
                        <Tag color="purple" style={{ fontSize: 11 }}>
                          头部券商
                        </Tag>
                      )}
                      {item.foreign_party && (
                        <Tag color="geekblue" style={{ fontSize: 11 }}>
                          外资
                        </Tag>
                      )}
                      {item.has_pdf ? (
                        <Tag
                          color="volcano"
                          icon={<FilePdfOutlined />}
                          style={{ fontSize: 11, cursor: 'pointer' }}
                          onClick={(e) => {
                            e.stopPropagation()
                            openDetail(item, true)
                          }}
                        >
                          PDF {item.pdf_size_bytes > 0 ? formatSize(item.pdf_size_bytes) : ''}
                        </Tag>
                      ) : (
                        category === 'research' && (
                          <Tag style={{ fontSize: 11 }}>无 PDF</Tag>
                        )
                      )}
                      {item.stocks.slice(0, 3).map((s) => (
                        <Tag key={s.code || s.name} color="cyan" style={{ fontSize: 11 }}>
                          <StockOutlined /> {s.name || s.code}
                          {s.rating ? ` · ${s.rating}` : ''}
                        </Tag>
                      ))}
                      <Text strong>{item.title}</Text>
                    </Space>
                  }
                  description={
                    <Space direction="vertical" size={2} style={{ width: '100%' }}>
                      <Space size={10} wrap style={{ fontSize: 12 }}>
                        <Text type="secondary">
                          <ClockCircleOutlined /> {item.release_time || '—'}
                        </Text>
                        {item.organization && (
                          <Text type="secondary">
                            <BankOutlined /> {item.organization}
                          </Text>
                        )}
                        {item.analysts.length > 0 && (
                          <Text type="secondary">
                            <UserOutlined /> {item.analysts.slice(0, 3).join(' / ')}
                            {item.analysts.length > 3 ? ` +${item.analysts.length - 3}` : ''}
                          </Text>
                        )}
                        {item.industries.slice(0, 3).map((ind) => (
                          <Tag key={ind} style={{ fontSize: 11 }}>
                            {ind}
                          </Tag>
                        ))}
                        {item.pages > 0 && (
                          <Tag color="blue" style={{ fontSize: 11 }}>
                            {item.pages} 页
                          </Tag>
                        )}
                      </Space>
                      {item.preview && (
                        <Text
                          style={{ fontSize: 12, color: '#64748b' }}
                          ellipsis={{ tooltip: item.preview } as any}
                        >
                          {item.preview.replace(/\n+/g, ' ')}
                        </Text>
                      )}
                      <Space size={10} style={{ fontSize: 11, color: '#94a3b8' }}>
                        <span>
                          <FileTextOutlined /> {item.content_length.toLocaleString()} 字
                        </span>
                        {item.has_pdf && item.pdf_size_bytes > 0 && (
                          <span>
                            <FilePdfOutlined /> {formatSize(item.pdf_size_bytes)}
                          </span>
                        )}
                      </Space>
                    </Space>
                  }
                />
              </List.Item>
            )
          }}
        />
      </Card>

      <Drawer
        title={detail?.title || '详情'}
        open={detailOpen}
        onClose={() => { clearPdf(); setDetailOpen(false) }}
        width={pdfVisible ? 1280 : 880}
        extra={
          <Space>
            {detail?.category === 'research' && detail?.has_pdf && (
              <>
                {!pdfVisible ? (
                  <Button
                    size="small" type="primary"
                    icon={<FilePdfOutlined />}
                    loading={pdfLoading}
                    onClick={() => loadPdfInline(detail.id)}
                  >
                    查看 PDF
                  </Button>
                ) : (
                  <Button size="small" onClick={clearPdf}>关闭 PDF</Button>
                )}
                <Button
                  size="small"
                  icon={<DownloadOutlined />}
                  onClick={() => downloadPdf(detail.id, detail.title || '')}
                >
                  下载 PDF
                </Button>
              </>
            )}
            {detail?.web_url && (
              <a href={detail.web_url} target="_blank" rel="noreferrer">
                <LinkOutlined /> 原文
              </a>
            )}
          </Space>
        }
      >
        <Spin spinning={detailLoading}>
          {detail ? (
            <div>
              <Space wrap size={6} style={{ marginBottom: 10 }}>
                <Tag
                  color={CATEGORY_META[detail.category].color}
                  style={{ color: '#fff', border: 'none' }}
                >
                  {CATEGORY_META[detail.category].icon} {detail.category_label}
                </Tag>
                {detail.chief_variant_name && (
                  <Tag color={CHIEF_VARIANT_COLORS[detail.chief_variant || ''] || 'default'}>
                    {detail.chief_variant_name}
                  </Tag>
                )}
                {detail.release_time && (
                  <Tag icon={<ClockCircleOutlined />}>{detail.release_time}</Tag>
                )}
                {detail.rpt_type_name && <Tag>{detail.rpt_type_name}</Tag>}
                {detail.first_coverage && <Tag color="gold">首次覆盖</Tag>}
                {detail.head_party && <Tag color="purple">头部券商</Tag>}
                {detail.foreign_party && <Tag color="geekblue">外资</Tag>}
                {detail.organization && (
                  <Tag icon={<BankOutlined />}>{detail.organization}</Tag>
                )}
                {detail.stocks.map((s) => (
                  <Tag key={s.code || s.name} color="cyan">
                    <StockOutlined /> {s.name || s.code}
                    {s.rating ? ` · ${s.rating}` : ''}
                  </Tag>
                ))}
                {detail.industries.map((ind) => (
                  <Tag key={ind}>{ind}</Tag>
                ))}
                {detail.pages > 0 && <Tag color="blue">{detail.pages} 页</Tag>}
                {detail.has_pdf && detail.pdf_size_bytes > 0 && (
                  <Tag
                    color="volcano"
                    icon={<FilePdfOutlined />}
                    style={{ cursor: detail.category === 'research' ? 'pointer' : 'default' }}
                    onClick={() => {
                      if (detail.category !== 'research') return
                      if (pdfVisible) clearPdf()
                      else loadPdfInline(detail.id)
                    }}
                  >
                    PDF {formatSize(detail.pdf_size_bytes)}
                    {detail.category === 'research' && (pdfVisible ? ' · 收起' : ' · 点击预览')}
                  </Tag>
                )}
              </Space>

              {detail.analysts.length > 0 && (
                <div style={{ marginBottom: 8 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    分析师:
                  </Text>
                  {detail.analysts.map((a) => (
                    <Tag key={a} icon={<UserOutlined />}>
                      {a}
                    </Tag>
                  ))}
                </div>
              )}

              {detail.guest && (
                <div style={{ marginBottom: 8 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    嘉宾:
                  </Text>
                  <Text>{detail.guest}</Text>
                </div>
              )}

              {(detail.location || detail.researcher || detail.source_name) && (
                <div style={{ marginBottom: 8, fontSize: 12, color: '#64748b' }}>
                  {detail.source_name && <span style={{ marginRight: 12 }}>来源: {detail.source_name}</span>}
                  {detail.location && <span style={{ marginRight: 12 }}>地点: {detail.location}</span>}
                  {detail.researcher && <span>研究员: {detail.researcher}</span>}
                </div>
              )}

              {detail.research_directions.length > 0 && (
                <div style={{ marginBottom: 8 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    方向:
                  </Text>
                  {detail.research_directions.map((d) => (
                    <Tag key={d}>{d}</Tag>
                  ))}
                </div>
              )}

              {detail.brief_md && (
                <Card
                  size="small"
                  title="核心观点"
                  style={{ marginTop: 8, marginBottom: 8 }}
                  bodyStyle={{
                    fontSize: 13,
                    lineHeight: 1.75,
                    background: '#fffbeb',
                  }}
                >
                  <div className="gangtise-md">
                    <ReactMarkdown remarkPlugins={[remarkGfm]}>{detail.brief_md}</ReactMarkdown>
                  </div>
                </Card>
              )}

              {/* Inline PDF preview — shown when user clicked 查看 PDF.
                  Drawer width switches to 1280px (see Drawer width prop) to
                  give the iframe room. Blob URL is revoked in clearPdf. */}
              {pdfVisible && pdfBlobUrl && (
                <Card
                  size="small"
                  title={<Space><FilePdfOutlined style={{color:'#dc2626'}} /> PDF 预览</Space>}
                  style={{ marginTop: 8 }}
                  bodyStyle={{ padding: 0 }}
                >
                  <iframe src={pdfBlobUrl} title="Gangtise PDF preview"
                          style={{ width: '100%', height: '82vh',
                                   border: 'none', display: 'block' }} />
                </Card>
              )}
              {pdfError && !pdfVisible && (
                <div style={{ marginTop: 8, padding: '8px 12px',
                              background:'#fef2f2', border:'1px solid #fecaca',
                              borderRadius:6, color:'#991b1b', fontSize:12 }}>
                  PDF 加载失败: {pdfError}
                </div>
              )}

              <Card
                size="small"
                title={
                  detail.category === 'research'
                    ? '研报正文'
                    : detail.category === 'chief'
                    ? '首席观点正文'
                    : '纪要正文'
                }
                style={{ marginTop: 8 }}
                bodyStyle={{
                  maxHeight: '62vh',
                  overflowY: 'auto',
                  fontSize: 13,
                  lineHeight: 1.75,
                  background: '#f8fafc',
                }}
              >
                {detail.content_md ? (
                  <div className="gangtise-md">
                    <ReactMarkdown remarkPlugins={[remarkGfm]}>
                      {detail.content_md}
                    </ReactMarkdown>
                  </div>
                ) : detail.msg_text && detail.msg_text.length > 0 ? (
                  <pre
                    style={{
                      whiteSpace: 'pre-wrap',
                      fontFamily: 'inherit',
                      margin: 0,
                    }}
                  >
                    {detail.msg_text.join('\n\n')}
                  </pre>
                ) : (
                  <Empty description="无正文" />
                )}
              </Card>

              <Text
                type="secondary"
                style={{ fontSize: 11, display: 'block', marginTop: 16 }}
              >
                ID: {detail.id}
                {detail.crawled_at &&
                  ` · 抓取于 ${dayjs(detail.crawled_at).format('YYYY-MM-DD HH:mm')}`}
              </Text>
            </div>
          ) : detailError ? (
            <Empty description={detailError} />
          ) : detailLoading ? null : (
            <Empty />
          )}
        </Spin>
      </Drawer>

      <style>{`
        .gangtise-md {
          font-size: 13.5px;
          line-height: 1.8;
        }
        .gangtise-md h1, .gangtise-md h2, .gangtise-md h3 {
          color: #0f172a;
          margin-top: 14px;
        }
        .gangtise-md table {
          border-collapse: collapse;
          margin: 10px 0;
          font-size: 12px;
        }
        .gangtise-md th, .gangtise-md td {
          border: 1px solid #e2e8f0;
          padding: 5px 9px;
        }
        .gangtise-md th { background: #f8fafc; font-weight: 600; }
        .gangtise-md a { color: #2563eb; text-decoration: none; }
        .gangtise-md a:hover { text-decoration: underline; }
      `}</style>
    </div>
  )
}
