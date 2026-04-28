/**
 * 进门专区 · 研报 (内资 + 外资复用同一组件)
 *
 * brm.comein.cn 抓下来的券商研报聚合 (含 PDF 预览).
 *
 * 路由驱动的变体:
 *   /jinmen/reports          → 内资研报  → /api/jinmen-db/reports*
 *   /jinmen/oversea-reports  → 外资研报  → /api/jinmen-db/oversea-reports*
 *
 * 后端 schema 一致 (ReportItem), 故组件共用; 页面差异仅在 endpoint 前缀 + 标题.
 * 爬虫: crawl/jinmen/scraper.py --reports | --oversea-reports
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useLocation, useSearchParams } from 'react-router-dom'
import {
  Alert, Button, Card, Col, Drawer, Empty, Input, List, message,
  Pagination, Row, Select, Space, Spin, Statistic, Tag, Typography,
} from 'antd'
import {
  AuditOutlined, BankOutlined, ClockCircleOutlined,
  DownloadOutlined, FilePdfOutlined, GlobalOutlined, LinkOutlined,
  NumberOutlined, ReloadOutlined, SearchOutlined, StockOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'
import TickerTagsTabs, { TickerTags } from '../components/TickerTagsTabs'
import MarkdownRenderer from '../components/MarkdownRenderer'

dayjs.extend(relativeTime)

const { Text, Title, Paragraph } = Typography

interface ReportItem {
  id: string
  report_id: string
  title: string
  release_time: string
  release_time_ms: number | null
  web_url: string | null
  organization_name: string
  type_name: string
  content_tags: string[]
  industry_tags: string[]
  companies: string[]
  is_vip: boolean
  pdf_num: number
  has_pdf: boolean
  // 后端新增 (2026-04-23): has_pdf=false 但源链仍可回源下载时为 true.
  // 用于列表/抽屉里把「无 PDF」标签升级为可点击的「下载」按钮.
  pdf_downloadable: boolean
  pdf_size: number
  summary_preview: string
  stats: Record<string, number>
  crawled_at: string | null
}

interface ReportDetail extends ReportItem {
  summary_md: string
  summary_point_md: string
  pdf_text_md: string
  original_url: string
  link_url: string
  pdf_local_path: string
  pdf_download_error: string
  ticker_tags?: TickerTags
}

interface ReportsStats {
  total: number
  today: number
  with_pdf: number
  without_pdf: number
  latest_release_time: string | null
  top_organizations: { name: string; count: number }[]
}

interface ListResp {
  items: ReportItem[]
  total: number
  page: number
  page_size: number
  has_next: boolean
}

function formatSize(b: number): string {
  if (!b) return '—'
  if (b >= 1024 * 1024) return `${(b / 1024 / 1024).toFixed(1)}MB`
  return `${Math.round(b / 1024)}KB`
}

export default function JinmenReports() {
  // 路由驱动的 variant: domestic (内资) vs oversea (外资).
  // 两套数据在 MongoDB 的不同 collection, 但 schema 一致, 走不同 endpoint 前缀.
  const { pathname } = useLocation()
  const isOversea = pathname.includes('/oversea-reports')
  const variant = useMemo(
    () => ({
      isOversea,
      label: isOversea ? '外资研报' : '研报',
      subtitle: isOversea
        ? 'brm.comein.cn/foreignResearch — 摩根大通 / 高盛 / 花旗等海外投行研报聚合 · --oversea-reports'
        : 'brm.comein.cn/reportManage — 国内券商研报聚合 · --reports',
      apiBase: isOversea ? '/jinmen-db/oversea-reports' : '/jinmen-db/reports',
      statsPath: isOversea ? '/jinmen-db/oversea-reports-stats' : '/jinmen-db/reports-stats',
      emptyHint: isOversea
        ? '无外资研报 (先跑 python3 scraper.py --oversea-reports)'
        : '无研报 (先跑 python3 scraper.py --reports)',
      orgPlaceholder: isOversea
        ? '机构 (e.g. 摩根大通 / JPMorgan / Goldman)'
        : '机构 (e.g. 中信证券)',
      headerIcon: isOversea ? <GlobalOutlined /> : <AuditOutlined />,
      accent: isOversea ? '#06b6d4' : '#ec4899',
    }),
    [isOversea],
  )
  const [stats, setStats] = useState<ReportsStats | null>(null)
  const [items, setItems] = useState<ReportItem[]>([])
  const [itemsLoading, setItemsLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [total, setTotal] = useState(0)

  const [query, setQuery] = useState('')
  const [organization, setOrganization] = useState('')
  const [ticker, setTicker] = useState('')
  const [hasPdfOnly, setHasPdfOnly] = useState<boolean | null>(true)

  const [detailOpen, setDetailOpen] = useState(false)
  const [detail, setDetail] = useState<ReportDetail | null>(null)
  const [detailError, setDetailError] = useState<string | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)

  // PDF 预览
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

  // Reset PDF + detail when switching between /reports and /oversea-reports.
  // React Router keeps the component instance mounted, so without this the
  // old iframe + stale detail from the other route hang around, and the
  // drawer shows a PDF from the wrong collection.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => {
    clearPdf()
    setDetailOpen(false)
    setDetail(null)
  }, [isOversea])

  // IMPORTANT: both handlers must depend on `variant.apiBase`. This
  // component is route-switched between /jinmen/reports and
  // /jinmen/oversea-reports without remounting; an empty deps array
  // captures the *initial* route's apiBase forever. Symptom seen in
  // 2026-04-22 backend log: user on `oversea-reports` page clicked
  // 查看 PDF → frontend sent GET `/jinmen-db/reports/<id>/pdf`
  // (wrong collection) → 404 Not Found. Depend on the url base so
  // handlers rebuild on route change.
  const loadPdf = useCallback(async (itemId: string) => {
    setPdfLoading(true)
    setPdfError(null)
    try {
      const res = await api.get(`${variant.apiBase}/${itemId}/pdf`, {
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
      message.error(`PDF 加载失败: ${msg}`)
    } finally {
      setPdfLoading(false)
    }
  }, [variant.apiBase])

  const downloadPdf = useCallback(async (itemId: string, title: string) => {
    try {
      const res = await api.get(`${variant.apiBase}/${itemId}/pdf`, {
        responseType: 'blob',
        params: { download: 1 },
        timeout: 60000,
      })
      const blob = new Blob([res.data], { type: 'application/pdf' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `${title.replace(/[\\/:*?"<>|\r\n\t]/g, '_').slice(0, 120)}.pdf`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      setTimeout(() => URL.revokeObjectURL(url), 1000)
    } catch (err: any) {
      message.error(`下载失败: ${err?.response?.data?.detail || err?.message}`)
    }
  }, [variant.apiBase])

  const loadStats = useCallback(async () => {
    try {
      const res = await api.get<ReportsStats>(variant.statsPath)
      setStats(res.data)
    } catch {
      setStats(null)
    }
  }, [variant.statsPath])

  const loadItems = useCallback(async () => {
    setItemsLoading(true)
    try {
      const res = await api.get<ListResp>(variant.apiBase, {
        params: {
          page, page_size: 20,
          q: query || undefined,
          organization: organization || undefined,
          ticker: ticker || undefined,
          has_pdf: hasPdfOnly === null ? undefined : hasPdfOnly,
        },
      })
      setItems(res.data.items || [])
      setTotal(res.data.total || 0)
    } catch {
      setItems([])
      setTotal(0)
    } finally {
      setItemsLoading(false)
    }
  }, [variant.apiBase, page, query, organization, ticker, hasPdfOnly])

  // Route switch (内资 ↔ 外资): reset filters + paging so we don't see the
  // other tab's stale results for a moment.
  //
  // CRITICAL: do NOT depend on `clearPdf` here. `clearPdf` is a useCallback
  // with [pdfBlobUrl] as a dep, so every successful `setPdfBlobUrl(url)` in
  // loadPdf gives clearPdf a NEW identity → this effect re-fires → calls
  // setDetailOpen(false) → drawer closes BEFORE the iframe can mount. That
  // was exactly the "download works, preview doesn't" bug — by inlining the
  // clear logic we keep the deps stable and match AlphaPaiReports' pattern.
  useEffect(() => {
    setPage(1); setItems([]); setTotal(0)
    setStats(null); setQuery(''); setOrganization(''); setTicker('')
    setDetailOpen(false); setDetail(null)
    setPdfVisible(false); setPdfError(null)
    setPdfBlobUrl((prev) => { if (prev) URL.revokeObjectURL(prev); return null })
  }, [variant.apiBase])

  useEffect(() => { loadStats() }, [loadStats])
  useEffect(() => { loadItems() }, [loadItems])

  const openDetail = useCallback(
    async (item: ReportItem, autoPreviewPdf = false) => {
      setDetailOpen(true)
      setDetailLoading(true)
      setDetail(null)
      setDetailError(null)
      clearPdf()
      // If user clicked the PDF/下载 tag, fire the PDF load immediately in
      // parallel with the detail fetch — saves one click + shows progress
      // inside the drawer via pdfLoading.
      if (autoPreviewPdf) loadPdf(item.id)
      try {
        const res = await api.get<ReportDetail>(`${variant.apiBase}/${item.id}`)
        setDetail(res.data)
      } catch (e: any) {
        setDetail(null)
        const status = e?.response?.status
        if (status === 404) {
          setDetailError(`该研报 (id=${item.id}) 尚未同步到本地数据库 — 爬虫下一轮会拉取`)
        } else {
          setDetailError(
            e?.response?.data?.detail || e?.message || '加载详情失败',
          )
        }
      } finally {
        setDetailLoading(false)
      }
    },
    [clearPdf, loadPdf, variant.apiBase],
  )

  // Deep-link: ?open=<reportId> opens the drawer directly — used by the
  // JinmenPlatformInfo "最新研报" feed to jump to a DB detail view.
  const [searchParams] = useSearchParams()
  const openParam = searchParams.get('open')
  const lastOpenedRef = useRef<string | null>(null)
  useEffect(() => {
    if (openParam && openParam !== lastOpenedRef.current) {
      lastOpenedRef.current = openParam
      openDetail({ id: openParam } as ReportItem)
    }
  }, [openParam, openDetail])

  return (
    <div style={{ padding: 20 }}>
      <div style={{ display: 'flex', alignItems: 'center',
                    justifyContent: 'space-between', marginBottom: 16 }}>
        <div>
          <Title level={3} style={{ margin: 0 }}>
            <span style={{ color: variant.accent }}>{variant.headerIcon}</span>{' '}
            进门 · {variant.label}
          </Title>
          <Text type="secondary">{variant.subtitle}</Text>
        </div>
        <a onClick={() => { loadStats(); loadItems() }} style={{ fontSize: 13 }}>
          <ReloadOutlined /> 刷新
        </a>
      </div>

      {stats && (
        <Row gutter={16} style={{ marginBottom: 16 }}>
          <Col xs={12} md={6}>
            <Card size="small"><Statistic title={`${variant.label}总数`} value={stats.total} /></Card>
          </Col>
          <Col xs={12} md={6}>
            <Card size="small">
              <Statistic title="今日新增" value={stats.today}
                         valueStyle={{ color: stats.today > 0 ? '#10b981' : undefined }} />
            </Card>
          </Col>
          <Col xs={12} md={6}>
            <Card size="small">
              <Statistic title="PDF 已下载" value={stats.with_pdf}
                         suffix={stats.total > 0 ? `/ ${stats.total}` : ''} />
            </Card>
          </Col>
          <Col xs={12} md={6}>
            <Card size="small">
              <Statistic title="最新发布" value={stats.latest_release_time || '—'}
                         valueStyle={{ fontSize: 16 }} />
            </Card>
          </Col>
        </Row>
      )}

      {/* 过滤条 */}
      <Card size="small" style={{ marginBottom: 12 }}>
        <Space wrap>
          <Input.Search
            placeholder="搜索标题 / 摘要"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onSearch={() => { setPage(1); loadItems() }}
            allowClear
            style={{ width: 260 }}
            prefix={<SearchOutlined />}
          />
          <Input
            placeholder={variant.orgPlaceholder}
            value={organization}
            onChange={(e) => setOrganization(e.target.value)}
            onPressEnter={() => { setPage(1); loadItems() }}
            allowClear
            style={{ width: 180 }}
            prefix={<BankOutlined />}
          />
          <Input
            placeholder="股票代码/名称 (例: 贵州茅台)"
            value={ticker}
            onChange={(e) => setTicker(e.target.value)}
            onPressEnter={() => { setPage(1); loadItems() }}
            allowClear
            style={{ width: 220 }}
            prefix={<StockOutlined />}
          />
          <Select
            value={hasPdfOnly === null ? 'all' : hasPdfOnly ? 'pdf' : 'nopdf'}
            onChange={(v) => {
              setHasPdfOnly(v === 'all' ? null : v === 'pdf')
              setPage(1)
            }}
            options={[
              { value: 'all', label: '全部' },
              { value: 'pdf', label: '仅含 PDF' },
              { value: 'nopdf', label: '无 PDF' },
            ]}
            style={{ width: 120 }}
          />
        </Space>
      </Card>

      {/* 列表 */}
      <Card size="small" bodyStyle={{ padding: 0 }}>
        <List
          loading={itemsLoading}
          dataSource={items}
          locale={{ emptyText: <Empty description={variant.emptyHint} /> }}
          renderItem={(item) => (
            <List.Item
              style={{ padding: '12px 16px', cursor: 'pointer' }}
              onClick={() => openDetail(item)}
              actions={[
                item.has_pdf ? (
                  <Tag
                    color="volcano"
                    icon={<FilePdfOutlined />}
                    key="pdf"
                    style={{ cursor: 'pointer' }}
                    onClick={(e) => { e.stopPropagation(); openDetail(item, true) }}
                  >
                    PDF {formatSize(item.pdf_size)}
                  </Tag>
                ) : item.pdf_downloadable ? (
                  <Tag
                    color="cyan"
                    icon={<DownloadOutlined />}
                    key="dl"
                    style={{ cursor: 'pointer' }}
                    onClick={(e) => { e.stopPropagation(); openDetail(item, true) }}
                  >
                    下载
                  </Tag>
                ) : (
                  <Tag key="nopdf">无 PDF</Tag>
                ),
              ]}
            >
              <List.Item.Meta
                title={
                  <Space wrap size={[4, 4]}>
                    <Text strong style={{ fontSize: 14 }}>{item.title}</Text>
                    {item.is_vip && <Tag color="gold">VIP</Tag>}
                  </Space>
                }
                description={
                  <Space direction="vertical" size={2} style={{ width: '100%' }}>
                    <Space wrap size={4}>
                      {item.organization_name && (
                        <Tag color="red" icon={<BankOutlined />}>{item.organization_name}</Tag>
                      )}
                      {item.release_time && (
                        <Tag icon={<ClockCircleOutlined />}>
                          {item.release_time.slice(0, 10)}
                        </Tag>
                      )}
                      {item.type_name && <Tag color="blue">{item.type_name}</Tag>}
                      {item.content_tags.slice(0, 3).map((t) => (
                        <Tag key={t}>{t}</Tag>
                      ))}
                      {item.pdf_num > 0 && (
                        <Tag icon={<NumberOutlined />}>{item.pdf_num} 页</Tag>
                      )}
                    </Space>
                    {item.companies.length > 0 && (
                      <Space size={4} wrap>
                        <Text type="secondary" style={{ fontSize: 11 }}>个股:</Text>
                        {item.companies.slice(0, 5).map((c) => (
                          <Tag key={c} color="cyan">{c}</Tag>
                        ))}
                      </Space>
                    )}
                    {item.summary_preview && (
                      <Paragraph
                        type="secondary"
                        style={{ fontSize: 12, marginBottom: 0, lineHeight: 1.6 }}
                        ellipsis={{ rows: 2 }}
                      >
                        {item.summary_preview}
                      </Paragraph>
                    )}
                  </Space>
                }
              />
            </List.Item>
          )}
        />
        <div style={{ padding: 12, textAlign: 'right' }}>
          <Pagination
            current={page}
            total={total}
            pageSize={20}
            onChange={setPage}
            showSizeChanger={false}
            showTotal={(t) => `共 ${t} 条`}
          />
        </div>
      </Card>

      {/* 详情抽屉 — PDF 控件放在 header extra, 参考 GangtiseDB.tsx 的布局.
          extra 区域不随 body 滚动, 不管摘要多长都一直看得到按钮. */}
      <Drawer
        title={detail?.title || '研报详情'}
        open={detailOpen}
        onClose={() => { clearPdf(); setDetailOpen(false) }}
        width={pdfVisible ? 1280 : 820}
        extra={
          <Space>
            {detail && (detail.has_pdf || detail.pdf_downloadable) && (
              <>
                {!pdfVisible ? (
                  <Button
                    size="small" type="primary"
                    icon={<FilePdfOutlined />}
                    loading={pdfLoading}
                    onClick={() => loadPdf(detail.id)}
                  >
                    {detail.has_pdf ? '查看 PDF' : '下载 PDF'}
                  </Button>
                ) : (
                  <Button size="small" onClick={clearPdf}>关闭 PDF</Button>
                )}
                <Button
                  size="small" icon={<DownloadOutlined />}
                  onClick={() => downloadPdf(detail.id, detail.title)}
                >
                  下载到本地
                </Button>
              </>
            )}
            {detail?.link_url && (
              <a href={detail.link_url} target="_blank" rel="noreferrer">
                <LinkOutlined /> 进门原页
              </a>
            )}
          </Space>
        }
      >
        <Spin spinning={detailLoading}>
          {detail ? (
            <div>
              <Space wrap size={6} style={{ marginBottom: 10 }}>
                {detail.organization_name && (
                  <Tag color="red" icon={<BankOutlined />}>{detail.organization_name}</Tag>
                )}
                {detail.release_time && (
                  <Tag icon={<ClockCircleOutlined />}>{detail.release_time}</Tag>
                )}
                {detail.type_name && <Tag color="blue">{detail.type_name}</Tag>}
                {detail.has_pdf && (
                  <Tag color="volcano" icon={<FilePdfOutlined />}>
                    PDF {formatSize(detail.pdf_size)} · {detail.pdf_num} 页
                  </Tag>
                )}
                {!detail.has_pdf && detail.pdf_downloadable && (
                  <Tag color="cyan" icon={<DownloadOutlined />}>PDF 未缓存 · 按「下载 PDF」回源拉取</Tag>
                )}
                {!detail.has_pdf && !detail.pdf_downloadable && (
                  <Tag color={detail.pdf_download_error ? 'red' : 'default'}>
                    {detail.pdf_download_error
                      ? `PDF 不可用: ${detail.pdf_download_error.slice(0, 40)}`
                      : '无 PDF 源链'}
                  </Tag>
                )}
              </Space>

              {detail.content_tags.length > 0 && (
                <div style={{ marginBottom: 6 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>标签:</Text>
                  {detail.content_tags.map((t) => <Tag key={t}>{t}</Tag>)}
                </div>
              )}
              {detail.companies.length > 0 && (
                <div style={{ marginBottom: 10 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>个股:</Text>
                  {detail.companies.map((c) => <Tag key={c} color="cyan">{c}</Tag>)}
                </div>
              )}
              <TickerTagsTabs tags={detail.ticker_tags} />

              {/* Inline PDF preview — 与 GangtiseDB 同构 iframe 方案, blob
                  URL 在 clearPdf 里 revoke. 抽屉宽度在 pdfVisible 时切到 1280px. */}
              {pdfVisible && pdfBlobUrl && (
                <Card size="small"
                      title={<Space><FilePdfOutlined style={{color:'#dc2626'}} /> PDF 预览</Space>}
                      style={{ marginTop: 8 }}
                      bodyStyle={{ padding: 0 }}>
                  <iframe src={pdfBlobUrl} title="Jinmen PDF preview"
                          style={{ width: '100%', height: '82vh', border: 'none', display: 'block' }} />
                </Card>
              )}
              {pdfError && !pdfVisible && (
                <Alert type="error" showIcon message="PDF 加载失败" description={pdfError}
                       style={{ marginTop: 8 }}
                       closable onClose={() => setPdfError(null)} />
              )}

              <Card size="small" title="核心观点 (平台摘要)"
                    style={{ marginTop: 8 }}
                    bodyStyle={{
                      maxHeight: pdfVisible ? '24vh' : '32vh',
                      overflowY: 'auto',
                      padding: '14px 18px',
                      background: '#fff',
                    }}>
                {detail.summary_md ? (
                  <div className="jinmen-report-md">
                    <MarkdownRenderer content={detail.summary_md} />
                  </div>
                ) : (
                  <Empty description="无摘要 (见下方 PDF 全文)" />
                )}
              </Card>

              {detail.pdf_text_md && (
                <Card size="small"
                      title={`PDF 全文 (${detail.pdf_text_md.length.toLocaleString()} 字)`}
                      style={{ marginTop: 8 }}
                      bodyStyle={{
                        maxHeight: pdfVisible ? '28vh' : '40vh',
                        overflowY: 'auto',
                        padding: '14px 18px',
                        background: '#fff',
                        whiteSpace: 'pre-wrap',
                        fontSize: 13,
                        lineHeight: 1.7,
                      }}>
                  <div className="jinmen-report-md">
                    <MarkdownRenderer content={detail.pdf_text_md} />
                  </div>
                </Card>
              )}

              <Paragraph type="secondary" style={{ fontSize: 11, marginTop: 12 }}>
                ID: {detail.id} · reportId: {detail.report_id}
                {detail.crawled_at && ` · 抓取于 ${dayjs(detail.crawled_at).format('YYYY-MM-DD HH:mm')}`}
              </Paragraph>
            </div>
          ) : detailError ? (
            <Empty description={detailError} />
          ) : detailLoading ? null : (
            <Empty />
          )}
        </Spin>
      </Drawer>
    </div>
  )
}
