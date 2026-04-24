/**
 * 进门 · 平台信息 — redesigned dense layout.
 *
 * Widgets backed by `backend/app/services/jinmen_platform_info.py` (15-min
 * Redis/memory cache, stale-on-upstream-error). Each widget hits the server
 * at most once per TTL regardless of UI refresh frequency.
 *
 * Layout (top-to-bottom):
 *   1. Gradient header + stat strip (热搜 #1 / 今日活动 / 推荐机构 / 行业数)
 *   2. Two-column main grid:
 *        L (wide)   — 最新纪要 · 最新研报 · 最新路演 · 最新点评
 *        R (narrow) — 热搜 Top-10 · 快速入口 · 活动日历 · 推荐机构
 *   3. Full-width 资讯流
 *   4. 一级行业 tag rail
 */
import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import {
  Alert, Avatar, Badge, Card, Col, Divider, Empty, Row, Segmented, Space, Spin,
  Switch, Tag, Tooltip, Typography,
} from 'antd'
import {
  ArrowDownOutlined, ArrowUpOutlined, AudioOutlined, BankOutlined,
  BookOutlined, CalendarOutlined, ClockCircleOutlined, FileTextOutlined,
  FireOutlined, GlobalOutlined, MessageOutlined, PauseCircleOutlined,
  PlayCircleOutlined, ReadOutlined, ReloadOutlined, RobotOutlined, StarFilled,
  StockOutlined, ThunderboltOutlined, VideoCameraOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import 'dayjs/locale/zh-cn'
import api from '../services/api'

dayjs.extend(relativeTime)
dayjs.locale('zh-cn')

const { Title, Text, Paragraph } = Typography

// ----------------------------------------------------------------- //
// Types                                                              //
// ----------------------------------------------------------------- //
interface Envelope<T> {
  ok: boolean
  data: T | null
  code?: string | number
  msg?: string
  fetched_at?: string | null
  stale?: boolean
}

interface HotItem { isUp: -1 | 1; name: string }
interface RecItem { content: string; stime: number; etime: number }
interface NewsAccount { accountId: string; accountName: string; accountAvatar?: string; accountProfile?: string }
interface NewsArticle {
  newsId: number; newsTitle: string; newsSummary?: string
  newsPublishSite?: string; accountAvatar?: string; effectiveTimeTs?: number
}
interface NewsList { list: NewsArticle[]; hasNext?: boolean; count?: number }
interface CalendarDay { time: number; date: string; count: number }
interface Industry { id: number; name: string; iconUrl?: string; type: number }

// Content-feed types (shared shape in `data.list`, each type has slightly
// different fields — unified via optional properties).
interface FeedItem {
  // Common across types
  title?: string
  content?: string
  releaseTime?: number
  publishTime?: number
  stime?: string | number
  organizationName?: string
  organizationId?: number | string
  // Per-type specific
  roadshowId?: number; summaryId?: number; summaryType?: number
  id?: number; reportId?: string; pdfNum?: number
  // Industries / themes / stocks — used across feeds
  industryTagList?: string[]
  industryList?: { industryId: number; industryName: string }[]
  stockList?: { code: string; name: string; market: string }[]
  stockInfoVos?: { code: string; name: string; market: string }[]
  companyDtoList?: { ticker?: string; name?: string }[]
  // Badges
  featuredTag?: string
  contentTypeTag?: string
  authTag?: string
  speakerTag?: string
  hasAISummary?: number
  hasVideo?: number
  hasNewWealth?: number
  // Org list + authors (reports)
  organizationList?: { name: string; logoUrl?: string }[]
  showAuthorList?: { name: string; avatarUrl?: string }[]
  creatorNames?: string[]
  // Comment / summary summary text
  summary?: string
  summaryPoint?: string
}

interface FeedList { list: FeedItem[]; total?: number }

// ----------------------------------------------------------------- //
// Bundle hook — fetch all 10 widgets in one round-trip               //
// ----------------------------------------------------------------- //
// The /platform-info/summary backend endpoint fans out to all widgets
// in parallel (asyncio.gather) so we spend max-of-widgets, not sum-of-
// widgets. Warm path is ~20 ms, cold path is bounded by the slowest
// upstream (~3-5 s).
type BundleKey =
  | 'hot_search' | 'search_recommend' | 'news_accounts' | 'meeting_calendar'
  | 'industries' | 'news_articles' | 'latest_summary' | 'latest_report'
  | 'latest_roadshow' | 'latest_comment'

type BundleEnvelope<T> = Envelope<T> & { loading: boolean }

function useBundle() {
  const [state, setState] = useState<Record<BundleKey, Envelope<any>> | null>(null)
  const [loading, setLoading] = useState(true)
  const reload = useCallback(async () => {
    setLoading(true)
    try {
      const res = await api.get('/jinmen-db/platform-info/summary')
      setState(res.data)
    } catch (e: any) {
      const msg = e?.response?.data?.detail || e?.message || 'request failed'
      // Keep whatever we had from last tick so UI doesn't flash empty on a
      // transient hiccup; stamp everything with the error message.
      setState((prev) => prev ?? ({
        hot_search: { ok: false, data: null, msg },
        search_recommend: { ok: false, data: null, msg },
        news_accounts: { ok: false, data: null, msg },
        meeting_calendar: { ok: false, data: null, msg },
        industries: { ok: false, data: null, msg },
        news_articles: { ok: false, data: null, msg },
        latest_summary: { ok: false, data: null, msg },
        latest_report: { ok: false, data: null, msg },
        latest_roadshow: { ok: false, data: null, msg },
        latest_comment: { ok: false, data: null, msg },
      } as Record<BundleKey, Envelope<any>>))
    } finally {
      setLoading(false)
    }
  }, [])
  useEffect(() => { reload() }, [reload])

  const widget = useCallback(<T,>(key: BundleKey): BundleEnvelope<T> => {
    const env = state?.[key] || { ok: false, data: null }
    return { ...env, loading: loading && !state } as BundleEnvelope<T>
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [state, loading])

  return { state, loading, reload, widget }
}

// ----------------------------------------------------------------- //
// Helpers                                                            //
// ----------------------------------------------------------------- //
function relTime(ts?: number | string | null): string {
  if (!ts) return ''
  const n = typeof ts === 'string' ? Number(ts) : ts
  if (!n || Number.isNaN(n)) return ''
  return dayjs(n).fromNow()
}

// Consistent industry-tag color by hashing the name
const INDUSTRY_COLORS = ['magenta', 'volcano', 'orange', 'gold', 'lime',
  'green', 'cyan', 'blue', 'geekblue', 'purple']
function hashColor(s: string): string {
  let h = 0
  for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0
  return INDUSTRY_COLORS[Math.abs(h) % INDUSTRY_COLORS.length]
}

// Normalize per-feed stock extraction (schemas differ slightly)
function itemStocks(it: FeedItem): { code: string; name: string }[] {
  const src = it.stockInfoVos || it.stockList || it.companyDtoList || []
  return src.map((s: any) => ({ code: s.code || s.ticker || '', name: s.name || '' }))
    .filter((s) => s.code || s.name)
}
function itemIndustries(it: FeedItem): string[] {
  if (it.industryTagList && it.industryTagList.length) return it.industryTagList
  if (it.industryList && it.industryList.length) return it.industryList.map((i) => i.industryName)
  return []
}
function itemTime(it: FeedItem): number {
  return (it.releaseTime || it.publishTime
          || (it.stime ? Number(it.stime) : 0)) as number
}

// ----------------------------------------------------------------- //
// Visual sub-components                                              //
// ----------------------------------------------------------------- //

function StaleBadge({ stale }: { stale?: boolean }) {
  return stale ? (
    <Tooltip title="上游不可达,显示 15 分钟内缓存">
      <Tag color="orange" style={{ fontSize: 11 }}>缓存</Tag>
    </Tooltip>
  ) : null
}

// Map a feed item to its corresponding in-DB deep-link. Returns null when
// we have no viewer page for that feed type (e.g. comments — not wired yet).
//
// ID precedence matters — Jinmen's local Mongo `_id`s are:
//   • meetings._id   = roadshowId            (crawl/jinmen/scraper.py L349)
//   • reports._id    = item.id (research id) (crawl/jinmen/scraper.py L1092)
//   • oversea._id    = researchId
// The upstream platform-info feed often carries BOTH `roadshowId` and
// `summaryId` on summary rows, and BOTH `id` and `reportId` on report rows —
// we must pick the one the scraper used as `_id` or the drawer shows empty.
function feedLinkFor(kind: 'summary' | 'report' | 'roadshow' | 'comment',
                     it: FeedItem): string | null {
  if (kind === 'summary' || kind === 'roadshow') {
    // meetings._id = roadshowId
    const id = it.roadshowId ?? it.summaryId ?? it.id
    return id ? `/jinmen/meetings?open=${encodeURIComponent(String(id))}` : null
  }
  if (kind === 'report') {
    // reports._id = research id (item.id), NOT reportId (human-readable code)
    const id = it.id ?? it.reportId
    return id ? `/jinmen/reports?open=${encodeURIComponent(String(id))}` : null
  }
  return null
}

// A single feed row — shared renderer for summary / report / roadshow / comment.
// Memoized: bundle reloads re-create per-row arrays but identity only changes
// when an item's content actually changes, so this cuts row re-renders to zero
// on idle refreshes.
const FeedRow = memo(function FeedRow({ it, kind }: {
  it: FeedItem
  kind: 'summary' | 'report' | 'roadshow' | 'comment'
}) {
  const stocks = itemStocks(it)
  const inds = itemIndustries(it)
  const t = itemTime(it)
  const orgName = it.organizationName
    || (it.organizationList && it.organizationList[0]?.name)
    || ''
  const orgLogo = it.organizationList && it.organizationList[0]?.logoUrl
  const authors = it.showAuthorList?.map((a) => a.name).slice(0, 3)
                || it.creatorNames?.slice(0, 3) || []
  const href = feedLinkFor(kind, it)

  return (
    <div style={{ padding: '10px 0', borderBottom: '1px dashed #f0f0f0' }}>
      <Space size={6} wrap style={{ marginBottom: 4 }}>
        {it.featuredTag && (
          <Tag color="red" style={{ fontSize: 11 }}>
            <StarFilled /> {it.featuredTag}
          </Tag>
        )}
        {it.hasAISummary ? (
          <Tag color="purple" style={{ fontSize: 11 }}><RobotOutlined /> AI 纪要</Tag>
        ) : null}
        {it.hasVideo ? (
          <Tag color="blue" style={{ fontSize: 11 }}><VideoCameraOutlined /> 视频</Tag>
        ) : null}
        {it.hasNewWealth ? (
          <Tag color="gold" style={{ fontSize: 11 }}>新财富</Tag>
        ) : null}
        {it.contentTypeTag && (
          <Tag style={{ fontSize: 11 }}>{it.contentTypeTag}</Tag>
        )}
        {it.authTag && it.authTag !== '专场' && (
          <Tag color="cyan" style={{ fontSize: 11 }}>{it.authTag}</Tag>
        )}
        {it.pdfNum ? (
          <Tag color="volcano" style={{ fontSize: 11 }}>PDF · {it.pdfNum}p</Tag>
        ) : null}
      </Space>

      {href ? (
        <Link
          to={href}
          style={{ display: 'block', lineHeight: 1.5, fontSize: 13,
                   color: '#0f172a', fontWeight: 600 }}
          title="点击打开本地数据库中的详情"
        >
          {it.title}
        </Link>
      ) : (
        <Text strong style={{ display: 'block', lineHeight: 1.5, fontSize: 13 }}>
          {it.title}
        </Text>
      )}

      <Space size={8} wrap style={{ fontSize: 11, color: '#64748b', marginTop: 3 }}>
        {orgLogo && <Avatar src={orgLogo} size={14} />}
        {orgName && <span><BankOutlined /> {orgName}</span>}
        {authors.length > 0 && <span>· {authors.join(' / ')}</span>}
        {t > 0 && <span><ClockCircleOutlined /> {relTime(t)}</span>}
      </Space>

      {(stocks.length > 0 || inds.length > 0) && (
        <Space size={4} wrap style={{ marginTop: 4 }}>
          {stocks.slice(0, 4).map((s, i) => (
            <Tag key={`s${i}`} color="cyan" style={{ fontSize: 10 }}>
              <StockOutlined /> {s.name} {s.code}
            </Tag>
          ))}
          {inds.slice(0, 3).map((ind) => (
            <Tag key={ind} color={hashColor(ind)} style={{ fontSize: 10 }}>
              {ind}
            </Tag>
          ))}
        </Space>
      )}

      {(it.summaryPoint || it.summary || it.content) && (
        <Paragraph type="secondary"
                   style={{ fontSize: 12, marginBottom: 0, marginTop: 4, lineHeight: 1.55 }}
                   ellipsis={{ rows: 2, tooltip: it.summaryPoint || it.summary || it.content }}>
          {it.summaryPoint || it.summary || it.content}
        </Paragraph>
      )}
    </div>
  )
})

const FeedCard = memo(function FeedCard({ title, icon, accent, widget, kind }: {
  title: string; icon: React.ReactNode; accent: string
  widget: { data: FeedList | null; loading: boolean; stale?: boolean; msg?: string }
  kind: 'summary' | 'report' | 'roadshow' | 'comment'
}) {
  const items = widget.data?.list || []
  return (
    <Card
      size="small"
      title={
        <Space>
          <span style={{ color: accent }}>{icon}</span>
          <Text strong>{title}</Text>
          {widget.data?.total != null && (
            <Text type="secondary" style={{ fontSize: 11, fontWeight: 'normal' }}>
              {widget.data.total.toLocaleString()} 条
            </Text>
          )}
          <StaleBadge stale={widget.stale} />
        </Space>
      }
      style={{ height: '100%' }}
      bodyStyle={{ padding: '8px 14px', minHeight: 200 }}
    >
      <Spin spinning={widget.loading}>
        {items.length > 0 ? items.map((it, i) => <FeedRow key={i} it={it} kind={kind} />)
          : <Empty description={widget.msg || '无数据'}
                   image={Empty.PRESENTED_IMAGE_SIMPLE} />}
      </Spin>
    </Card>
  )
})

// ----------------------------------------------------------------- //
// Main                                                               //
// ----------------------------------------------------------------- //
export default function JinmenPlatformInfo() {
  // One parallel fetch → 10 widgets in a single HTTP round-trip.
  const bundle = useBundle()
  const hot = bundle.widget<HotItem[]>('hot_search')
  const rec = bundle.widget<RecItem[]>('search_recommend')
  const accounts = bundle.widget<NewsAccount[]>('news_accounts')
  const cal = bundle.widget<CalendarDay[]>('meeting_calendar')
  const inds = bundle.widget<Industry[]>('industries')
  const news = bundle.widget<NewsList>('news_articles')
  const summary = bundle.widget<FeedList>('latest_summary')
  const report = bundle.widget<FeedList>('latest_report')
  const roadshow = bundle.widget<FeedList>('latest_roadshow')
  const comment = bundle.widget<FeedList>('latest_comment')

  const allWidgets = [hot, rec, accounts, cal, inds, news, summary, report, roadshow, comment]
  const reloadAll = useCallback(() => {
    bundle.reload()
    setLastFetchMs(Date.now())
  }, [bundle])

  // Auto-refresh controls (unified with AlphaPai PlatformInfo)
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [refreshSec, setRefreshSec] = useState<number>(30)
  const [lastFetchMs, setLastFetchMs] = useState<number | null>(null)
  const [nextCountdown, setNextCountdown] = useState<number>(0)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const countdownRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => {
    if (timerRef.current) clearInterval(timerRef.current)
    if (autoRefresh) {
      timerRef.current = setInterval(() => reloadAll(), refreshSec * 1000)
    }
    return () => {
      if (timerRef.current) clearInterval(timerRef.current)
    }
  }, [autoRefresh, refreshSec, reloadAll])

  useEffect(() => {
    if (countdownRef.current) clearInterval(countdownRef.current)
    if (!autoRefresh || !lastFetchMs) {
      setNextCountdown(0)
      return
    }
    countdownRef.current = setInterval(() => {
      const elapsed = (Date.now() - lastFetchMs) / 1000
      setNextCountdown(Math.max(0, Math.ceil(refreshSec - elapsed)))
    }, 500)
    return () => {
      if (countdownRef.current) clearInterval(countdownRef.current)
    }
  }, [autoRefresh, refreshSec, lastFetchMs])

  const meta = useMemo(() => {
    const fetched = allWidgets.map(w => w.fetched_at).filter(Boolean).sort()
    const anyStale = allWidgets.some(w => w.stale)
    const totalItems = (summary.data?.total || 0) + (report.data?.total || 0)
                      + (roadshow.data?.total || 0) + (comment.data?.total || 0)
    return { oldest: fetched[0] || null, anyStale, totalItems }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bundle.state])

  return (
    <div style={{ padding: 16, background: '#f8fafc', minHeight: '100vh' }}>
      {/* Unified gradient header (Jinmen brand: pink → amber) */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: 20,
          padding: '14px 20px',
          background: 'linear-gradient(135deg, #be185d 0%, #f59e0b 100%)',
          borderRadius: 10,
          color: '#fff',
          boxShadow: '0 4px 12px rgba(190, 24, 93, 0.3)',
        }}
      >
        <div>
          <Title level={3} style={{ color: '#fff', margin: 0, fontSize: 20 }}>
            <AudioOutlined /> 进门 · 平台信息
          </Title>
          <Text style={{ color: 'rgba(255,255,255,0.9)', fontSize: 12 }}>
            brm.comein.cn 首页镜像 · 15 min 缓存
            {meta.oldest && <> · 最近拉取 {dayjs(meta.oldest).format('HH:mm:ss')}</>}
            {meta.anyStale && (
              <Tag color="orange" style={{ marginLeft: 8 }}>部分缓存</Tag>
            )}
          </Text>
        </div>
        <Space size={12}>
          <Badge
            status={autoRefresh ? 'processing' : 'default'}
            text={
              <span style={{ color: '#fff', fontSize: 12 }}>
                {autoRefresh ? `${nextCountdown}s 后刷新` : '已暂停'}
              </span>
            }
          />
          <Segmented
            size="small"
            value={refreshSec}
            onChange={(v) => setRefreshSec(Number(v))}
            options={[
              { value: 15, label: '15s' },
              { value: 30, label: '30s' },
              { value: 60, label: '60s' },
              { value: 120, label: '2m' },
            ]}
          />
          <Switch
            checkedChildren={<PlayCircleOutlined />}
            unCheckedChildren={<PauseCircleOutlined />}
            checked={autoRefresh}
            onChange={setAutoRefresh}
          />
          <Tooltip title="立即刷新">
            <ReloadOutlined
              style={{ fontSize: 18, color: '#fff', cursor: 'pointer' }}
              onClick={reloadAll}
            />
          </Tooltip>
        </Space>
      </div>

      {/* Stat strip */}
      <Row gutter={[12, 12]} style={{ marginBottom: 14 }}>
        <Col xs={12} md={6}>
          <StatCard icon={<FireOutlined />} color="#ef4444"
            title="当前热搜 #1"
            value={hot.data?.[0]?.name || '—'}
            extra={hot.data?.[0]
              ? (hot.data[0].isUp === 1
                  ? <ArrowUpOutlined style={{ color: '#10b981' }} />
                  : <ArrowDownOutlined style={{ color: '#64748b' }} />)
              : null}
          />
        </Col>
        <Col xs={12} md={6}>
          <StatCard icon={<CalendarOutlined />} color="#3b82f6"
            title="今日活动"
            value={
              cal.data?.find((d) => d.date === dayjs().format('YYYY-MM-DD'))?.count.toString()
              || (cal.data?.[1]?.count?.toString() ?? '—')
            }
            extra={<Text type="secondary" style={{ fontSize: 11 }}>场会议</Text>}
          />
        </Col>
        <Col xs={12} md={6}>
          <StatCard icon={<BankOutlined />} color="#8b5cf6"
            title="推荐机构"
            value={(accounts.data?.length || 0).toString()}
            extra={<Text type="secondary" style={{ fontSize: 11 }}>个活跃账号</Text>}
          />
        </Col>
        <Col xs={12} md={6}>
          <StatCard icon={<GlobalOutlined />} color="#06b6d4"
            title="覆盖行业"
            value={(inds.data?.length || 0).toString()}
            extra={<Text type="secondary" style={{ fontSize: 11 }}>个一级行业</Text>}
          />
        </Col>
      </Row>

      {/* Main 2-col layout: wide content + narrow sidebar */}
      <Row gutter={[14, 14]}>
        {/* Left: 4 content feeds in 2x2 */}
        <Col xs={24} lg={17}>
          <Row gutter={[12, 12]}>
            <Col xs={24} md={12}>
              <FeedCard title="最新纪要" icon={<AudioOutlined />} accent="#ec4899"
                        widget={summary} kind="summary" />
            </Col>
            <Col xs={24} md={12}>
              <FeedCard title="最新研报" icon={<FileTextOutlined />} accent="#3b82f6"
                        widget={report} kind="report" />
            </Col>
            <Col xs={24} md={12}>
              <FeedCard title="最新路演" icon={<PlayCircleOutlined />} accent="#8b5cf6"
                        widget={roadshow} kind="roadshow" />
            </Col>
            <Col xs={24} md={12}>
              <FeedCard title="最新点评" icon={<MessageOutlined />} accent="#10b981"
                        widget={comment} kind="comment" />
            </Col>
          </Row>
        </Col>

        {/* Right: compact sidebar */}
        <Col xs={24} lg={7}>
          <Space direction="vertical" size={12} style={{ width: '100%' }}>
            {/* 热搜 */}
            <Card size="small"
              title={<Space><FireOutlined style={{ color: '#ef4444' }} /> 热搜 Top-10
                     <StaleBadge stale={hot.stale} /></Space>}
              bodyStyle={{ padding: '8px 14px' }}>
              <Spin spinning={hot.loading}>
                {hot.data && hot.data.length > 0 ? hot.data.map((it, i) => (
                  <div key={i} style={{
                    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                    padding: '4px 0', borderBottom: i < 9 ? '1px solid #fafafa' : 'none',
                  }}>
                    <Space size={6}>
                      <span style={{
                        minWidth: 20, fontSize: 12, fontWeight: 'bold', textAlign: 'center',
                        color: i < 3 ? '#ef4444' : '#94a3b8',
                      }}>{i + 1}</span>
                      <Text style={{ fontSize: 13 }}>{it.name}</Text>
                    </Space>
                    {it.isUp === 1
                      ? <ArrowUpOutlined style={{ color: '#10b981', fontSize: 11 }} />
                      : <ArrowDownOutlined style={{ color: '#94a3b8', fontSize: 11 }} />}
                  </div>
                )) : <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="—" />}
              </Spin>
            </Card>

            {/* 快速入口 */}
            <Card size="small"
              title={<Space><ThunderboltOutlined style={{ color: '#f59e0b' }} /> 快速入口</Space>}
              bodyStyle={{ padding: '10px 14px' }}>
              <Spin spinning={rec.loading}>
                {rec.data && rec.data.length > 0 ? (
                  <Space wrap size={[6, 6]}>
                    {rec.data.map((it, i) => (
                      <Tag key={i} color="orange" style={{ padding: '3px 10px', fontSize: 12 }}>
                        {it.content}
                      </Tag>
                    ))}
                  </Space>
                ) : <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="—" />}
              </Spin>
            </Card>

            {/* 活动日历 */}
            <Card size="small"
              title={<Space><CalendarOutlined style={{ color: '#3b82f6' }} /> 活动日历</Space>}
              bodyStyle={{ padding: '10px 14px' }}>
              <Spin spinning={cal.loading}>
                {cal.data && cal.data.length > 0 ? cal.data.map((d) => {
                  const isToday = d.date === dayjs().format('YYYY-MM-DD')
                  return (
                    <div key={d.time} style={{
                      display: 'flex', justifyContent: 'space-between',
                      padding: '5px 0', alignItems: 'center',
                    }}>
                      <Text strong={isToday}>
                        {d.date}
                        {isToday && <Tag color="red" style={{ marginLeft: 6, fontSize: 10 }}>今日</Tag>}
                      </Text>
                      <Badge count={d.count} overflowCount={999}
                             style={{ backgroundColor: isToday ? '#ef4444' : '#3b82f6' }} />
                    </div>
                  )
                }) : <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="—" />}
              </Spin>
            </Card>

            {/* 推荐机构 */}
            <Card size="small"
              title={<Space><BankOutlined style={{ color: '#8b5cf6' }} /> 推荐机构</Space>}
              bodyStyle={{ padding: '8px 14px' }}>
              <Spin spinning={accounts.loading}>
                {accounts.data && accounts.data.length > 0 ? (
                  <Space direction="vertical" size={4} style={{ width: '100%' }}>
                    {accounts.data.slice(0, 8).map((a) => (
                      <div key={a.accountId} style={{
                        display: 'flex', alignItems: 'center', gap: 8, padding: '3px 0',
                      }}>
                        <Avatar src={a.accountAvatar} size={24} icon={<BankOutlined />} />
                        <div style={{ flex: 1, minWidth: 0 }}>
                          <Text strong style={{ fontSize: 12, display: 'block' }}>
                            {a.accountName}
                          </Text>
                          <Text type="secondary" style={{ fontSize: 10 }} ellipsis>
                            {a.accountProfile}
                          </Text>
                        </div>
                      </div>
                    ))}
                  </Space>
                ) : <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="—" />}
              </Spin>
            </Card>
          </Space>
        </Col>
      </Row>

      {/* Full-width 资讯 */}
      <Divider style={{ margin: '14px 0' }} />
      <Card size="small"
        title={
          <Space>
            <ReadOutlined style={{ color: '#10b981' }} />
            <Text strong>资讯流</Text>
            {news.data?.count != null && (
              <Text type="secondary" style={{ fontSize: 11, fontWeight: 'normal' }}>
                {news.data.count} 条
              </Text>
            )}
            <StaleBadge stale={news.stale} />
          </Space>
        }
        bodyStyle={{ padding: '8px 14px' }}>
        <Spin spinning={news.loading}>
          {news.data?.list && news.data.list.length > 0 ? (
            <Row gutter={[10, 8]}>
              {news.data.list.map((n) => (
                <Col xs={24} md={12} lg={8} key={n.newsId}>
                  <div style={{
                    padding: '8px 10px', background: '#fafafa', borderRadius: 6,
                    height: '100%', border: '1px solid #f5f5f5',
                  }}>
                    <Space size={6} style={{ marginBottom: 4, width: '100%' }}>
                      {n.accountAvatar
                        ? <Avatar src={n.accountAvatar} size={22} />
                        : <Avatar icon={<BookOutlined />} size={22} />}
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <Text strong style={{ fontSize: 12, display: 'block' }} ellipsis={{ tooltip: n.newsTitle }}>
                          {n.newsTitle}
                        </Text>
                        <Space size={6} style={{ fontSize: 10, color: '#94a3b8' }}>
                          {n.newsPublishSite && <span>{n.newsPublishSite}</span>}
                          {n.effectiveTimeTs && <span>· {relTime(n.effectiveTimeTs)}</span>}
                        </Space>
                      </div>
                    </Space>
                    {n.newsSummary && (
                      <Paragraph type="secondary" style={{ fontSize: 11, marginBottom: 0 }}
                                 ellipsis={{ rows: 2, tooltip: n.newsSummary }}>
                        {n.newsSummary}
                      </Paragraph>
                    )}
                  </div>
                </Col>
              ))}
            </Row>
          ) : <Empty description={news.msg || '无数据'} />}
        </Spin>
      </Card>

      {/* 一级行业 rail */}
      <Divider style={{ margin: '14px 0' }} />
      <Card size="small"
        title={<Space><GlobalOutlined style={{ color: '#06b6d4' }} /> 一级行业</Space>}
        bodyStyle={{ padding: '10px 14px' }}>
        <Spin spinning={inds.loading}>
          {inds.data && inds.data.length > 0 ? (
            <Space wrap size={[8, 8]}>
              {inds.data.map((ind) => (
                <Tag key={ind.id} color={hashColor(ind.name)}
                     style={{ fontSize: 13, padding: '4px 12px' }}>
                  {ind.name}
                </Tag>
              ))}
            </Space>
          ) : <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} />}
        </Spin>
      </Card>

      {/* Error banner if anything hard-failed */}
      {allWidgets.some(w => !w.loading && !w.ok && !w.data) && (
        <Alert type="warning" showIcon style={{ marginTop: 14 }}
               message="部分模块加载失败"
               description="上游 brm.comein.cn 暂时不可达。已自动兜底至缓存;无缓存的模块保持空状态。" />
      )}
    </div>
  )
}

// ----------------------------------------------------------------- //
// Small stat card                                                    //
// ----------------------------------------------------------------- //
function StatCard({ icon, color, title, value, extra }: {
  icon: React.ReactNode; color: string; title: string; value: string; extra?: React.ReactNode
}) {
  return (
    <Card size="small" bodyStyle={{ padding: '14px 16px' }}
          style={{ borderLeft: `3px solid ${color}`, height: '100%' }}>
      <Space align="center" size={10}>
        <div style={{
          fontSize: 22, color, width: 36, height: 36, borderRadius: 6,
          background: color + '14',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
        }}>{icon}</div>
        <div>
          <Text type="secondary" style={{ fontSize: 11, display: 'block' }}>{title}</Text>
          <Space size={4} align="center">
            <Text strong style={{ fontSize: 17 }}>{value}</Text>
            {extra}
          </Space>
        </div>
      </Space>
    </Card>
  )
}
