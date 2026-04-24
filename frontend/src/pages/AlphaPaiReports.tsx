/**
 * AlphaPai · 研报 (Reports)
 *
 * 基于 MongoDB (`alphapai.reports`) 的视图。
 * 每条为券商研究报告 (含 PDF 链接、页数、研报类型)。
 */
import { useCallback, useEffect, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Drawer,
  Empty,
  Input,
  List,
  message,
  Segmented,
  Select,
  Space,
  Spin,
  Statistic,
  Tag,
  Typography,
} from 'antd'
import {
  ReloadOutlined,
  FileTextOutlined,
  LinkOutlined,
  ClockCircleOutlined,
  BankOutlined,
  UserOutlined,
  FilePdfOutlined,
  TagOutlined,
  NumberOutlined,
  AimOutlined,
  DownloadOutlined,
  EyeOutlined,
  CloseOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'

dayjs.extend(relativeTime)

import MarkdownRenderer from '../components/MarkdownRenderer'

const { Text, Title, Paragraph } = Typography

// 研报发布时间多数是当日 00:00，前端只保留日期部分
function fmtReportTime(t: string | null | undefined): string {
  if (!t) return '—'
  return t.endsWith(' 00:00') ? t.slice(0, 10) : t
}

interface Item {
  id: string
  category: string
  title: string
  publish_time: string | null
  web_url: string | null
  institution: string | null
  stocks: { code: string | null; name: string | null }[]
  industries: string[]
  analysts: string[]
  content_preview: string
  content_length: number
  has_pdf: boolean
  account_name: string | null
  source_url: string | null
  // 对齐 alphapai-web.rabyte.cn 左侧分区 Tab
  report_type_id: number | null
  report_type_name: string | null
  market_v2: number | null
  market_name: string | null
  crawled_at: string | null
  core_viewpoint?: string
}

// 研报分区列表 — 和 /reading/home/point 页面左侧保持一致
const REPORT_TYPES: { id: number; name: string; color: string }[] = [
  { id: 1,  name: 'A股公司研究', color: 'red' },
  { id: 4,  name: '港股研究',    color: 'purple' },
  { id: 13, name: '行业研究',    color: 'blue' },
  { id: 14, name: '宏观研究',    color: 'orange' },
  { id: 6,  name: '日报晨会',    color: 'gold' },
  { id: 5,  name: '固收研究',    color: 'green' },
  { id: 7,  name: '金融工程',    color: 'cyan' },
]
const REPORT_TYPE_COLOR: Record<number, string> = Object.fromEntries(
  REPORT_TYPES.map((t) => [t.id, t.color]),
)

interface ListResponse {
  items: Item[]
  total: number
  page: number
  page_size: number
  has_next: boolean
}

interface StatsResponse {
  total: number
  per_category: Record<string, number>
  today: Record<string, number>
  last_7_days: {
    date: string
    roadshow: number
    report: number
    comment: number
    wechat: number
  }[]
  recent_publishers: Record<string, { name: string; count: number }[]>
  latest_per_category: Record<string, string | null>
}

interface DetailResponse extends Item {
  content: string
  pdf_local_path: string | null
  pdf_size: number | null
  raw_id: string | null
}

export default function AlphaPaiReports() {
  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [statsLoading, setStatsLoading] = useState(true)
  const [statsError, setStatsError] = useState<string | null>(null)

  const [items, setItems] = useState<Item[]>([])
  const [itemsLoading, setItemsLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [total, setTotal] = useState(0)

  const [query, setQuery] = useState('')
  const [institutionFilter, setInstitutionFilter] = useState<string | undefined>()
  const [tickerFilter, setTickerFilter] = useState('')
  // Sub-category (reportType from list_item): 1/4/5/6/7/13/14 — 对齐官方 UI 分区
  const [reportType, setReportType] = useState<number | undefined>()
  // 顶层 3-tab (对齐 AlphaPai SPA 研报页): 内资 / 外资 / 独立 研究
  const [subcategory, setSubcategory] = useState<string | undefined>()

  const [detailOpen, setDetailOpen] = useState(false)
  const [detail, setDetail] = useState<DetailResponse | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)

  // PDF 预览: 用 axios 拿 blob (带 JWT) → createObjectURL 塞给 iframe
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

  const loadPdf = useCallback(async (itemId: string) => {
    setPdfLoading(true)
    setPdfError(null)
    try {
      const res = await api.get(`/alphapai-db/items/report/${itemId}/pdf`, {
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
  }, [])

  const downloadPdf = useCallback(async (itemId: string, title: string) => {
    try {
      const res = await api.get(`/alphapai-db/items/report/${itemId}/pdf`, {
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
      const msg = err?.response?.data?.detail || err?.message || '下载失败'
      message.error(`下载失败: ${msg}`)
    }
  }, [])

  const loadStats = useCallback(async () => {
    setStatsLoading(true)
    setStatsError(null)
    try {
      const res = await api.get<StatsResponse>('/alphapai-db/stats')
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
      const res = await api.get<ListResponse>('/alphapai-db/items', {
        params: {
          category: 'report',
          page,
          page_size: 20,
          q: query || undefined,
          institution: institutionFilter || undefined,
          ticker: tickerFilter || undefined,
          report_type: reportType,
          subcategory: subcategory || undefined,
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
  }, [page, query, institutionFilter, tickerFilter, reportType, subcategory])

  useEffect(() => {
    loadStats()
  }, [loadStats])

  useEffect(() => {
    loadItems()
  }, [loadItems])

  const openDetail = useCallback(async (item: Item) => {
    setDetailOpen(true)
    setDetailLoading(true)
    setDetail(null)
    try {
      const res = await api.get<DetailResponse>(
        `/alphapai-db/items/report/${encodeURIComponent(item.id)}`,
      )
      setDetail(res.data)
    } catch {
      setDetail(null)
    } finally {
      setDetailLoading(false)
    }
  }, [])

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
            <FileTextOutlined /> AlphaPai · 研报
          </Title>
          <Text type="secondary">
            券商研究报告聚合 (PDF + 正文) · 来自 crawl/alphapai_crawl
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
          message="无法从 MongoDB 加载数据"
          description={statsError}
          style={{ marginBottom: 16 }}
        />
      )}

      <Spin spinning={statsLoading}>
        <Card size="small" bodyStyle={{ padding: 14 }} style={{ marginBottom: 16 }}>
          <Space size={18} align="center">
            <Statistic
              title={
                <span style={{ color: '#10b981' }}>
                  <ClockCircleOutlined /> 今日新增研报
                </span>
              }
              value={stats?.today.report ?? 0}
              valueStyle={{ color: '#10b981', fontSize: 28 }}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              {dayjs().format('YYYY-MM-DD')}
              {stats?.latest_per_category?.report && (
                <> · 最近发布 {fmtReportTime(stats.latest_per_category.report)}</>
              )}
            </Text>
          </Space>
        </Card>
      </Spin>

      <Card size="small">
        <Segmented
          style={{ marginBottom: 8 }}
          value={subcategory ?? 'all'}
          onChange={(v) => {
            setSubcategory(v === 'all' ? undefined : String(v))
            setPage(1)
          }}
          options={[
            { label: '全部', value: 'all' },
            { label: '内资报告', value: 'ashare' },
            { label: '外资报告', value: 'us' },
            { label: '独立研究', value: 'indep' },
          ]}
        />
        <Segmented
          style={{ marginBottom: 12 }}
          value={reportType ?? 'all'}
          onChange={(v) => {
            setReportType(v === 'all' ? undefined : Number(v))
            setPage(1)
          }}
          options={[
            { label: '全部', value: 'all' },
            ...REPORT_TYPES.map((t) => ({ label: t.name, value: t.id })),
          ]}
        />
        <Space wrap style={{ marginBottom: 12 }}>
          <Input.Search
            placeholder="搜索标题 / 内容"
            allowClear
            style={{ width: 260 }}
            onSearch={(v) => {
              setQuery(v)
              setPage(1)
            }}
          />
          <Select
            placeholder="研报机构"
            allowClear
            value={institutionFilter}
            onChange={(v) => {
              setInstitutionFilter(v)
              setPage(1)
            }}
            style={{ width: 200 }}
            options={(stats?.recent_publishers?.report || []).map((p) => ({
              value: p.name,
              label: `${p.name} (${p.count})`,
            }))}
          />
          <Input
            placeholder="个股代码/名称"
            allowClear
            style={{ width: 180 }}
            onPressEnter={(e) => {
              setTickerFilter((e.target as HTMLInputElement).value)
              setPage(1)
            }}
          />
          <Text type="secondary" style={{ fontSize: 12 }}>
            共 {total} 条
          </Text>
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
          renderItem={(item) => (
            <List.Item
              key={item.id}
              style={{ cursor: 'pointer' }}
              onClick={() => openDetail(item)}
            >
              <List.Item.Meta
                title={
                  <Space size={6} wrap>
                    {item.report_type_name && (
                      <Tag
                        color={REPORT_TYPE_COLOR[item.report_type_id || 0] || 'default'}
                        style={{ fontSize: 11 }}
                      >
                        {item.report_type_name}
                      </Tag>
                    )}
                    {item.institution && (
                      <Tag color="red" icon={<BankOutlined />}>
                        {item.institution}
                      </Tag>
                    )}
                    {item.has_pdf && (
                      <Tag color="volcano" icon={<FilePdfOutlined />}>
                        PDF
                      </Tag>
                    )}
                    <Text strong>{item.title}</Text>
                  </Space>
                }
                description={
                  <Space direction="vertical" size={2} style={{ width: '100%' }}>
                    <Space size={10} wrap style={{ fontSize: 12 }}>
                      <Text type="secondary">
                        <ClockCircleOutlined /> {fmtReportTime(item.publish_time)}
                      </Text>
                      {item.analysts.length > 0 && (
                        <Text type="secondary">
                          <UserOutlined /> {item.analysts.join(' / ')}
                        </Text>
                      )}
                      {item.stocks.map((s, idx) => (
                        <Tag
                          key={`${s.code}-${idx}`}
                          color="cyan"
                          style={{ fontSize: 11 }}
                        >
                          {s.name} {s.code}
                        </Tag>
                      ))}
                      {item.industries.map((ind) => (
                        <Tag key={ind} style={{ fontSize: 11 }}>
                          {ind}
                        </Tag>
                      ))}
                    </Space>
                    <Text
                      style={{ fontSize: 12, color: '#64748b' }}
                      ellipsis={{ tooltip: item.core_viewpoint || item.content_preview } as any}
                    >
                      {(item.core_viewpoint || item.content_preview).replace(/\n+/g, ' ')}
                    </Text>
                    <Text type="secondary" style={{ fontSize: 11 }}>
                      <NumberOutlined /> {item.content_length} 字
                      {item.core_viewpoint && (
                        <span style={{ color: '#ef4444', marginLeft: 8 }}>
                          <AimOutlined /> 核心观点已提取
                        </span>
                      )}
                    </Text>
                  </Space>
                }
              />
            </List.Item>
          )}
        />
      </Card>

      <Drawer
        title={detail?.title || '详情'}
        open={detailOpen}
        onClose={() => { clearPdf(); setDetailOpen(false) }}
        width={pdfVisible ? 1280 : 820}
        extra={
          detail?.web_url ? (
            <a href={detail.web_url} target="_blank" rel="noreferrer">
              <LinkOutlined /> AlphaPai 原页
            </a>
          ) : null
        }
      >
        <Spin spinning={detailLoading}>
          {detail ? (
            <div>
              <Space wrap size={6} style={{ marginBottom: 10 }}>
                {detail.institution && (
                  <Tag color="red" icon={<BankOutlined />}>
                    {detail.institution}
                  </Tag>
                )}
                {detail.publish_time && (
                  <Tag icon={<ClockCircleOutlined />}>{fmtReportTime(detail.publish_time)}</Tag>
                )}
                {detail.has_pdf && (
                  <Tag color="volcano" icon={<FilePdfOutlined />}>
                    PDF{detail.pdf_size ? ` ${Math.round(detail.pdf_size / 1024)}KB` : ''}
                  </Tag>
                )}
                {detail.has_pdf && detail.pdf_local_path && !pdfVisible && (
                  <Button
                    size="small"
                    type="primary"
                    icon={<EyeOutlined />}
                    loading={pdfLoading}
                    onClick={() => loadPdf(detail.id)}
                  >
                    查看 PDF
                  </Button>
                )}
                {detail.has_pdf && detail.pdf_local_path && pdfVisible && (
                  <Button
                    size="small"
                    icon={<CloseOutlined />}
                    onClick={clearPdf}
                  >
                    关闭 PDF
                  </Button>
                )}
                {detail.has_pdf && detail.pdf_local_path && (
                  <Button
                    size="small"
                    icon={<DownloadOutlined />}
                    onClick={() => downloadPdf(detail.id, detail.title)}
                  >
                    下载
                  </Button>
                )}
                {detail.has_pdf && !detail.pdf_local_path && (
                  <Tag color="red">PDF 未落盘 (可能下载失败, 重启爬虫 --resume 即可补抓)</Tag>
                )}
                {detail.analysts.map((a) => (
                  <Tag key={a} color="geekblue" icon={<UserOutlined />}>
                    {a}
                  </Tag>
                ))}
              </Space>
              {detail.stocks.length > 0 && (
                <div style={{ marginBottom: 6 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    <TagOutlined /> 个股:
                  </Text>
                  {detail.stocks.map((s, idx) => (
                    <Tag key={`${s.code}-${idx}`} color="cyan">
                      {s.name} {s.code}
                    </Tag>
                  ))}
                </div>
              )}
              {detail.industries.length > 0 && (
                <div style={{ marginBottom: 10 }}>
                  <Text type="secondary" style={{ marginRight: 6 }}>
                    行业:
                  </Text>
                  {detail.industries.map((i) => (
                    <Tag key={i}>{i}</Tag>
                  ))}
                </div>
              )}
              {pdfVisible && pdfBlobUrl && (
                <Card
                  size="small"
                  title={
                    <Space>
                      <FilePdfOutlined /> PDF 预览
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        浏览器内嵌, 底部滚动条查看; 右上角按钮可放大/下载
                      </Text>
                    </Space>
                  }
                  style={{ marginTop: 8 }}
                  bodyStyle={{ padding: 0 }}
                >
                  <iframe
                    src={pdfBlobUrl}
                    title="PDF preview"
                    style={{
                      width: '100%',
                      height: '82vh',
                      border: 'none',
                      display: 'block',
                    }}
                  />
                </Card>
              )}
              {pdfError && !pdfVisible && (
                <Alert
                  type="error"
                  showIcon
                  message="PDF 加载失败"
                  description={pdfError}
                  style={{ marginTop: 8 }}
                  closable
                  onClose={() => setPdfError(null)}
                />
              )}
              {detail.core_viewpoint && (
                <Card
                  size="small"
                  title={
                    <span>
                      <AimOutlined style={{ color: '#ef4444', marginRight: 6 }} />
                      核心观点
                    </span>
                  }
                  style={{
                    marginTop: 8,
                    background:
                      'linear-gradient(135deg, #fff7ed 0%, #fef3c7 100%)',
                    borderLeft: '3px solid #ef4444',
                  }}
                  bodyStyle={{
                    fontSize: 13,
                    lineHeight: 1.75,
                    color: '#1e293b',
                    maxHeight: '28vh',
                    overflowY: 'auto',
                  }}
                >
                  <MarkdownRenderer content={detail.core_viewpoint} />
                </Card>
              )}
              <Card
                size="small"
                title="研报内容 (文本)"
                style={{ marginTop: 8 }}
                bodyStyle={{
                  maxHeight: pdfVisible ? '32vh' : '62vh',
                  overflowY: 'auto',
                  fontSize: 13,
                  lineHeight: 1.8,
                  background: '#f8fafc',
                }}
              >
                {detail.content ? (
                  <MarkdownRenderer content={detail.content} />
                ) : (
                  <Empty description={detail.has_pdf
                    ? '此研报正文仅存于 PDF, JSON 无文本 — 请用"查看 PDF"按钮预览'
                    : '无内容'} />
                )}
              </Card>
              <Paragraph type="secondary" style={{ fontSize: 11, marginTop: 12 }}>
                ID: {detail.id}
                {detail.crawled_at &&
                  ` · 抓取于 ${dayjs(detail.crawled_at).format('YYYY-MM-DD HH:mm')}`}
              </Paragraph>
            </div>
          ) : (
            <Empty />
          )}
        </Spin>
      </Drawer>
    </div>
  )
}
