import { useEffect, useState, useCallback, useRef } from 'react'
import {
  Card, Col, Row, Tag, Typography, Spin, Space,
  Collapse, Empty, Tooltip, Select,
} from 'antd'
import {
  ThunderboltOutlined, RiseOutlined, FallOutlined,
  CheckCircleOutlined, CloseCircleOutlined, StockOutlined,
  ClockCircleOutlined, FireOutlined, AlertOutlined,
  SearchOutlined, BellOutlined,
  FundOutlined, SyncOutlined,
  WifiOutlined, DisconnectOutlined, NotificationOutlined,
  LinkOutlined, HistoryOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'
import { useAuthStore } from '../store/auth'
import api from '../services/api'
import SentimentTraderCards from '../components/SentimentTraderCards'

const { Text } = Typography

/* ── Types ── */

interface SourceItem {
  url?: string
  title?: string
  source_label?: string
  source_type?: string
  date?: string
  source?: string
}

interface BreakingNewsItem {
  id: string
  ticker: string
  name_cn: string
  name_en: string
  market: string
  market_label: string
  scan_time: string
  news_materiality: string
  news_summary: string
  new_developments: string[]
  novelty_status: string
  earliest_report_time: string | null
  deep_research_performed: boolean
  research_iterations: number
  key_findings: string[]
  news_timeline: Array<{ time?: string; source?: string; title?: string }>
  referenced_sources: Array<{ url?: string; title?: string; source?: string }>
  sources: SourceItem[]
  historical_precedents: Array<{
    event_date?: string; description?: string; ticker?: string
    return_1d?: number; return_3d?: number; return_5d?: number
  }>
  historical_evidence_summary: string
  alert_confidence: number
  alert_rationale: string
  sentiment: string
  impact_magnitude: string
  impact_timeframe: string
  surprise_factor: number
  bull_case: string
  bear_case: string
  recommended_action: string
  should_alert?: boolean
}

interface NewsSummary {
  news_count: number
  latest_scan: string
  latest_materiality: string
  latest_sentiment: string
  latest_summary: string
}

interface PortfolioHolding {
  stock_name: string
  stock_ticker: string
  stock_market: string
  tags: string[]
}

interface StockQuote {
  ticker: string
  yf_symbol: string
  prev_close: number | null
  latest_price: number | null
  change_pct: number | null
  market_cap: number | null
  pe_ttm: number | null
  currency: string
  fetched_at: number
  error: string
}

interface SourceHealth {
  source_name: string
  is_healthy: boolean
  total_items_fetched: number
  consecutive_failures: number
  last_success: string | null
  last_failure: string | null
}

interface ScannerStatus {
  status: 'active' | 'stale' | 'inactive' | 'error'
  total_stocks: number
  active_stocks: number
  last_scan_at: string | null
  total_scans: number
  total_alerts: number
  stocks_alerted_24h: number
}

interface FyForecast {
  year: string | null
  net_profit: number | null  // raw RMB
  eps: number | null
  pe: number | null
  pb: number | null
  roe: number | null         // %
  revenue: number | null     // raw RMB
}

interface ConsensusData {
  ticker: string
  windcode: string
  as_of: string | null       // YYYYMMDD
  analyst_count: number | null
  target_price: number | null
  target_price_num_inst: number | null
  rating_avg: number | null
  rating_label: string
  rating_num_buy: number
  rating_num_outperform: number
  rating_num_hold: number
  rating_num_underperform: number
  rating_num_sell: number
  fy1: FyForecast
  fy2: FyForecast
  fy3: FyForecast
  yoy_net_profit: number | null  // YoY NP growth %
}

// funda · 推特情绪因子 — /api/funda-db/sentiment/my-watchlist
// 0-10 分, twitter_score 主, reddit_score 副; ai_summary 是当日要点
interface FundaSentimentItem {
  ticker: string                    // 持仓 ticker (裸码, upper)
  funda_ticker?: string             // funda 原始 (可能带 .HK/.SS 后缀)
  date: string                      // YYYY-MM-DD
  company: string
  sector: string
  industry: string
  reddit_score: number | null
  reddit_count: number
  twitter_score: number | null
  twitter_count: number
  ai_summary: string
}

interface FundaSentimentTrend {
  delta: number | null              // (latest - earliest) twitter_score in window
  scored_days: number               // # of days with twitter_score in window
  earliest_score: number | null
  earliest_date: string | null
}

/* ── Helpers ── */

// Map portfolio (stock_ticker, stock_market) to CODE.MARKET canonical id.
// Mirrors backend ticker_normalizer._canonical_from_code_market and the same
// helper in Portfolio.tsx — kept inline here to keep this diff small.
function classifyAshare(code: string): 'SH' | 'SZ' | 'BJ' | null {
  if (!/^\d{6}$/.test(code)) return null
  const p3 = code.slice(0, 3)
  const p2 = code.slice(0, 2)
  if (['600', '601', '603', '605', '688', '900'].includes(p3)) return 'SH'
  if (['000', '001', '002', '003', '300', '301', '200'].includes(p3)) return 'SZ'
  if (['43', '83', '87', '88', '92'].includes(p2)) return 'BJ'
  return null
}

function toCanonical(ticker: string, market: string): string | null {
  const t = (ticker || '').trim()
  if (!t) return null
  if (market === '美股') return `${t.toUpperCase()}.US`
  if (market === '港股') {
    const digits = t.replace(/\D/g, '').padStart(5, '0')
    return digits ? `${digits}.HK` : null
  }
  if (market === '主板' || market === '创业板' || market === '科创板') {
    const cls = classifyAshare(t)
    return cls ? `${t}.${cls}` : null
  }
  if (market === '韩股') return `${t.toUpperCase()}.KS`
  if (market === '日股') return `${t.toUpperCase()}.JP`
  if (market === '澳股') return `${t.toUpperCase()}.AU`
  if (market === '德股') return `${t.toUpperCase()}.DE`
  return null
}

/* ── Constants ── */

const MATERIALITY_CONFIG: Record<string, { color: string; bg: string; icon: React.ReactNode; label: string }> = {
  critical: { color: '#f5222d', bg: '#fff1f0', icon: <FireOutlined />,    label: '重大' },
  material: { color: '#fa8c16', bg: '#fff7e6', icon: <AlertOutlined />,   label: '重要' },
  routine:  { color: '#1890ff', bg: '#e6f7ff', icon: <BellOutlined />,    label: '常规' },
  none:     { color: '#8c8c8c', bg: '#fafafa', icon: <ClockCircleOutlined />, label: '无' },
}

const SENTIMENT_CONFIG: Record<string, { color: string; icon: React.ReactNode; label: string }> = {
  very_bullish: { color: '#389e0d', icon: <RiseOutlined />, label: '强烈看多' },
  bullish:      { color: '#52c41a', icon: <RiseOutlined />, label: '看多' },
  neutral:      { color: '#8c8c8c', icon: <span>—</span>,  label: '中性' },
  bearish:      { color: '#f5222d', icon: <FallOutlined />, label: '看空' },
  very_bearish: { color: '#cf1322', icon: <FallOutlined />, label: '强烈看空' },
}

const MARKET_TAG_COLORS: Record<string, string> = {
  '美股': 'blue', '科创板': 'cyan', '创业板': 'green', '主板': 'orange',
  '港股': 'red', '韩股': 'purple', '日股': 'magenta', 'A股': 'red',
  '澳股': 'gold',
}

/* Chinese A-share price convention: up = red, down = green. */
const UP_COLOR = '#f5222d'
const DOWN_COLOR = '#00a854'
const FLAT_COLOR = '#8c8c8c'

/* funda 情绪因子配色 — 与 FundaSentimentCard.tsx 保持一致.
 * 注意: 情绪 ≠ 涨跌, 不套用 A 股红绿; 0-10 分高分用绿色 (国际惯例: 看多=正面=绿). */
function fundaScoreColor(score: number | null | undefined): string {
  if (score == null) return '#94a3b8'
  if (score >= 7) return '#10b981'
  if (score >= 4) return '#f59e0b'
  return '#ef4444'
}

function fundaScoreLabel(score: number | null | undefined): string {
  if (score == null) return '—'
  if (score >= 7) return '看多'
  if (score >= 5.5) return '偏多'
  if (score >= 4.5) return '中性'
  if (score >= 3) return '偏空'
  return '看空'
}

/* 7 日趋势箭头 — delta = latest_score - earliest_score (twitter).
 * 阈值 ±0.5 与 funda_db.py::_classify_trend 的 WARMING/COOLING 起点对齐. */
function fundaTrendArrow(delta: number | null): { glyph: string; color: string } {
  if (delta == null) return { glyph: '·', color: '#bfbfbf' }
  if (delta >= 0.5) return { glyph: '↗', color: '#10b981' }
  if (delta <= -0.5) return { glyph: '↘', color: '#ef4444' }
  return { glyph: '→', color: '#bfbfbf' }
}

/* Region grouping for portfolio overview. 主板/科创板/创业板 collapse into A股. */
const REGION_MAP: Record<string, string> = {
  '美股': '美股',
  '主板': 'A股', '科创板': 'A股', '创业板': 'A股', 'A股': 'A股',
  '港股': '港股',
  '韩股': '韩股',
  '日股': '日股',
  '澳股': '澳股',
}
const REGION_ORDER = ['美股', 'A股', '港股', '韩股', '日股', '澳股', '其他']
const REGION_META: Record<string, { flag: string; accent: string }> = {
  '美股': { flag: '🇺🇸', accent: '#1677ff' },
  'A股':  { flag: '🇨🇳', accent: '#f5222d' },
  '港股': { flag: '🇭🇰', accent: '#fa541c' },
  '韩股': { flag: '🇰🇷', accent: '#722ed1' },
  '日股': { flag: '🇯🇵', accent: '#eb2f96' },
  '澳股': { flag: '🇦🇺', accent: '#fa8c16' },
  '其他': { flag: '🌐', accent: '#8c8c8c' },
}

const HOURS_OPTIONS = [
  { value: 24, label: '24h' },
  { value: 48, label: '48h' },
  { value: 168, label: '7天' },
  { value: 720, label: '30天' },
]

const AUTO_REFRESH_INTERVAL = 60_000

/* ── Helpers ── */

function timeAgo(isoStr: string): string {
  const diff = Date.now() - new Date(isoStr).getTime()
  const mins = Math.floor(diff / 60000)
  if (mins < 1) return '刚刚'
  if (mins < 60) return `${mins}分钟前`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}小时前`
  const days = Math.floor(hrs / 24)
  return `${days}天前`
}

function isRecent(isoStr: string, thresholdMinutes = 60): boolean {
  return Date.now() - new Date(isoStr).getTime() < thresholdMinutes * 60000
}

function formatReturn(val: number | undefined | null): React.ReactNode {
  if (val == null) return <Text type="secondary">—</Text>
  const pct = (val * 100).toFixed(1)
  const color = val > 0 ? '#52c41a' : val < 0 ? '#f5222d' : '#8c8c8c'
  return <span style={{ color, fontWeight: 500 }}>{val > 0 ? '+' : ''}{pct}%</span>
}

function formatPrice(val: number | null): string {
  if (val == null) return '—'
  // Asian currency prices are often 4-6 digits (JPY, KRW); use no decimals there.
  if (val >= 1000) return val.toLocaleString(undefined, { maximumFractionDigits: 0 })
  return val.toFixed(2)
}

const CURRENCY_NAME_CN: Record<string, string> = {
  USD: '美元', CNY: '元', HKD: '港元',
  KRW: '韩元', JPY: '日元', AUD: '澳元',
}

function formatMarketCap(val: number | null, currency: string): string {
  if (val == null || val <= 0) return '—'
  const cur = CURRENCY_NAME_CN[currency] || currency
  if (val >= 1e12) return `${(val / 1e12).toFixed(2)}万亿${cur}`
  if (val >= 1e8) return `${(val / 1e8).toFixed(1)}亿${cur}`
  if (val >= 1e4) return `${(val / 1e4).toFixed(0)}万${cur}`
  return `${val.toFixed(0)}${cur}`
}

function formatPE(val: number | null): string {
  if (val == null || val === 0) return '—'
  if (val < 0) return '亏损'
  if (val > 1000) return '>1000'
  return val.toFixed(1)
}

function formatRMB(val: number | null): string {
  // Net profit / revenue arrive in raw RMB; condense to 亿/万亿.
  if (val == null || val === 0) return '—'
  const abs = Math.abs(val)
  const sign = val < 0 ? '-' : ''
  if (abs >= 1e12) return `${sign}${(abs / 1e12).toFixed(2)}万亿`
  if (abs >= 1e8) return `${sign}${(abs / 1e8).toFixed(1)}亿`
  if (abs >= 1e4) return `${sign}${(abs / 1e4).toFixed(0)}万`
  return `${sign}${abs.toFixed(0)}`
}

function formatConsensusDate(yyyymmdd: string | null): string {
  if (!yyyymmdd || yyyymmdd.length < 8) return '—'
  return `${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}`
}

function ratingColor(label: string): string {
  switch (label) {
    case '买入': case 'Buy':           return '#cf1322'  // strongest = red (CN bullish convention)
    case '增持': case 'Outperform':    return '#fa541c'
    case '中性': case 'Hold':          return '#8c8c8c'
    case '减持': case 'Underperform':  return '#13a8a8'
    case '卖出': case 'Sell':          return '#00a854'
    default: return '#8c8c8c'
  }
}

function priceColor(val: number | null | undefined): string {
  if (val == null || val === 0) return FLAT_COLOR
  return val > 0 ? UP_COLOR : DOWN_COLOR
}

function priceArrow(val: number | null | undefined): string {
  if (val == null || val === 0) return ''
  return val > 0 ? '▲' : '▼'
}

/* ── Scanner Status Badge ── */

function ScannerStatusBadge({ scanner }: { scanner: ScannerStatus | null }) {
  const { t } = useTranslation()
  if (!scanner) return null
  const cfgMap: Record<string, { color: string; icon: React.ReactNode; label: string }> = {
    active:   { color: '#52c41a', icon: <WifiOutlined />,        label: t('dashboard.scannerActive') },
    stale:    { color: '#fa8c16', icon: <DisconnectOutlined />,   label: t('dashboard.scannerStale') },
    inactive: { color: '#8c8c8c', icon: <DisconnectOutlined />,   label: t('dashboard.scannerInactive') },
    error:    { color: '#f5222d', icon: <CloseCircleOutlined />,  label: t('dashboard.scannerInactive') },
  }
  const cfg = cfgMap[scanner.status] || cfgMap.inactive
  return (
    <Tooltip title={
      <div style={{ fontSize: 12 }}>
        <div>{t('dashboard.stocksMonitored')}: {scanner.total_stocks}</div>
        {scanner.last_scan_at && <div>{t('dashboard.lastScan')}: {timeAgo(scanner.last_scan_at)}</div>}
        <div>{t('dashboard.alertedToday')}: {scanner.stocks_alerted_24h}</div>
      </div>
    }>
      <Tag
        icon={cfg.icon}
        color={scanner.status === 'active' ? 'success' : scanner.status === 'stale' ? 'warning' : 'default'}
        style={{ cursor: 'default', marginRight: 0 }}
      >
        {cfg.label}
        {scanner.last_scan_at && <span style={{ marginLeft: 4, opacity: 0.8 }}>({timeAgo(scanner.last_scan_at)})</span>}
      </Tag>
    </Tooltip>
  )
}

/* ── Section styles ── */
const secTitle: React.CSSProperties = { fontSize: 13, fontWeight: 600, marginBottom: 6, color: '#262626' }
const secBody: React.CSSProperties = { fontSize: 13, color: '#434343', lineHeight: '20px' }
const hr: React.CSSProperties = { margin: '10px 0', borderTop: '1px solid #f0f0f0' }

/* ── Expanded detail panel for a single news item ── */

function NewsDetail({ item, onTickerClick }: { item: BreakingNewsItem; onTickerClick: (t: string) => void }) {
  const matCfg = MATERIALITY_CONFIG[item.news_materiality] || MATERIALITY_CONFIG.none
  const sentCfg = SENTIMENT_CONFIG[item.sentiment] || SENTIMENT_CONFIG.neutral
  const priceUrl = item.market === 'us' ? `https://finance.yahoo.com/quote/${item.ticker}`
    : item.market === 'hk' ? `https://finance.yahoo.com/quote/${item.ticker}.HK`
    : item.market === 'china' ? `https://quote.eastmoney.com/${item.ticker}.html` : ''

  return (
    <div>
      {/* Summary */}
      <div style={{ fontSize: 14, lineHeight: '22px', color: '#262626', marginBottom: 8 }}>{item.news_summary}</div>

      {/* Developments */}
      {item.new_developments?.length > 0 && (
        <div style={{ marginBottom: 8 }}>
          {item.new_developments.slice(0, 3).map((dev, i) => (
            <div key={i} style={{ fontSize: 12, color: '#595959', lineHeight: '18px', marginBottom: 2 }}>
              <ThunderboltOutlined style={{ color: matCfg.color, marginRight: 4, fontSize: 11 }} />{dev}
            </div>
          ))}
        </div>
      )}

      <div style={hr} />

      {/* Key findings */}
      {item.key_findings?.length > 0 && (<>
        <div style={secTitle}><SearchOutlined style={{ marginRight: 4 }} />关键发现 ({item.key_findings.length})</div>
        <ul style={{ margin: '0 0 4px', paddingLeft: 18 }}>
          {item.key_findings.slice(0, 6).map((f, i) => <li key={i} style={{ ...secBody, marginBottom: 2 }}>{f}</li>)}
        </ul>
        <div style={hr} />
      </>)}

      {/* Historical precedent table */}
      {item.historical_precedents?.length > 0 && (<>
        <div style={secTitle}><HistoryOutlined style={{ marginRight: 4 }} />历史先例对比</div>
        <div style={{ overflowX: 'auto', marginBottom: 4 }}>
          <table style={{ width: '100%', fontSize: 12, borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #e8e8e8' }}>
                {['日期', '事件', 'T+1', 'T+3', 'T+5'].map((h, i) => (
                  <th key={h} style={{ textAlign: i < 2 ? 'left' : 'right', padding: '4px 8px', color: '#8c8c8c', fontWeight: 500 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {item.historical_precedents.slice(0, 5).map((p, i) => (
                <tr key={i} style={{ borderBottom: '1px solid #fafafa' }}>
                  <td style={{ padding: '4px 8px', whiteSpace: 'nowrap', color: '#595959' }}>{p.event_date || '—'}</td>
                  <td style={{ padding: '4px 8px', color: '#434343', maxWidth: 220, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{p.description || '—'}</td>
                  <td style={{ padding: '4px 8px', textAlign: 'right' }}>{formatReturn(p.return_1d)}</td>
                  <td style={{ padding: '4px 8px', textAlign: 'right' }}>{formatReturn(p.return_3d)}</td>
                  <td style={{ padding: '4px 8px', textAlign: 'right' }}>{formatReturn(p.return_5d)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div style={hr} />
      </>)}

      {/* Historical evidence summary */}
      {item.historical_evidence_summary && (<>
        <div style={secTitle}>📝 历史分析</div>
        <div style={{ ...secBody, marginBottom: 4 }}>{item.historical_evidence_summary}</div>
        <div style={hr} />
      </>)}

      {/* News timeline */}
      {item.news_timeline?.length > 0 && (<>
        <div style={secTitle}><ClockCircleOutlined style={{ marginRight: 4 }} />新闻传播时间线</div>
        <div style={{ marginBottom: 4 }}>
          {item.news_timeline.slice(0, 8).map((entry, i) => (
            <div key={i} style={{ fontSize: 12, color: '#595959', lineHeight: '20px', display: 'flex', gap: 6 }}>
              <span style={{ color: '#8c8c8c', whiteSpace: 'nowrap', minWidth: 120 }}>{entry.time || '?'}</span>
              <span style={{ color: '#1890ff', whiteSpace: 'nowrap' }}>{entry.source || '?'}</span>
              <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{entry.title || ''}</span>
            </div>
          ))}
        </div>
        <div style={hr} />
      </>)}

      {/* Impact assessment */}
      <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap', marginBottom: 6 }}>
        <Text style={{ fontSize: 12, color: sentCfg.color, fontWeight: 600 }}>{sentCfg.icon} {sentCfg.label}</Text>
        <Text type="secondary" style={{ fontSize: 11 }}>
          置信度: <span style={{ color: item.alert_confidence >= 0.9 ? '#389e0d' : item.alert_confidence >= 0.8 ? '#fa8c16' : '#8c8c8c', fontWeight: 600 }}>{(item.alert_confidence * 100).toFixed(0)}%</span>
        </Text>
        <Text type="secondary" style={{ fontSize: 11 }}>影响: {item.impact_magnitude === 'critical' ? '重大' : item.impact_magnitude === 'high' ? '高' : item.impact_magnitude === 'medium' ? '中' : '低'}</Text>
        <Text type="secondary" style={{ fontSize: 11 }}>窗口: {item.impact_timeframe === 'long_term' ? '长期' : item.impact_timeframe === 'medium_term' ? '中期' : '短期'}</Text>
        {item.novelty_status && <Text type="secondary" style={{ fontSize: 11 }}>新鲜度: {item.novelty_status === 'verified_fresh' ? '✅确认' : item.novelty_status === 'likely_fresh' ? '🔵可能' : item.novelty_status}</Text>}
      </div>

      {/* Bull / Bear */}
      {(item.bull_case || item.bear_case) && (
        <div style={{ fontSize: 13, marginBottom: 6 }}>
          {item.bull_case && <div style={{ marginBottom: 4 }}><span style={{ color: '#52c41a', fontWeight: 600 }}>🐂 看多: </span><span style={{ color: '#434343' }}>{item.bull_case}</span></div>}
          {item.bear_case && <div><span style={{ color: '#f5222d', fontWeight: 600 }}>🐻 看空: </span><span style={{ color: '#434343' }}>{item.bear_case}</span></div>}
        </div>
      )}

      {/* Recommended action */}
      {item.recommended_action && (
        <div style={{ fontSize: 13, marginBottom: 6, padding: '6px 10px', background: 'rgba(24,144,255,0.06)', borderRadius: 6 }}>
          <span style={{ color: '#1890ff', fontWeight: 600 }}>💡 建议: </span><span style={{ color: '#262626' }}>{item.recommended_action}</span>
        </div>
      )}

      <div style={hr} />

      {/* Source citations */}
      {item.sources?.length > 0 && (<>
        <div style={secTitle}><LinkOutlined style={{ marginRight: 4 }} />信息来源 ({item.sources.length})</div>
        <div style={{ marginBottom: 4 }}>
          {item.sources.slice(0, 8).map((src, i) => (
            <div key={i} style={{ fontSize: 12, lineHeight: '20px', color: '#595959' }}>
              <span style={{ color: '#8c8c8c' }}>{src.source_type === 'internal' ? '[内部]' : '[外部]'} {src.source_label || src.source || ''}: </span>
              {src.url
                ? <a href={src.url} target="_blank" rel="noopener noreferrer" style={{ color: '#1890ff' }} onClick={(e) => e.stopPropagation()}>{(src.title || src.url || '').slice(0, 60)}{(src.title || '').length > 60 ? '...' : ''}</a>
                : <span>{(src.title || '').slice(0, 60)}</span>}
              {src.date && <span style={{ color: '#bfbfbf', marginLeft: 6 }}>({src.date})</span>}
            </div>
          ))}
        </div>
        <div style={hr} />
      </>)}

      {/* Footer */}
      <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap', fontSize: 11, color: '#8c8c8c' }}>
        {item.deep_research_performed && <span>研究深度: {item.research_iterations}轮 / {item.sources?.length || item.referenced_sources?.length || 0}源</span>}
        {priceUrl && <a href={priceUrl} target="_blank" rel="noopener noreferrer" style={{ color: '#1890ff', fontSize: 11 }} onClick={(e) => e.stopPropagation()}>查看行情 →</a>}
        <span>{item.market_label}</span>
      </div>
    </div>
  )
}

/* ── Build collapse header (the one-line title row) ── */

function newsCollapseLabel(item: BreakingNewsItem) {
  const matCfg = MATERIALITY_CONFIG[item.news_materiality] || MATERIALITY_CONFIG.none
  const sentCfg = SENTIMENT_CONFIG[item.sentiment] || SENTIMENT_CONFIG.neutral
  const recent = isRecent(item.scan_time, 60)
  // Truncate summary for the title row
  const shortSummary = (item.news_summary || '').length > 80
    ? item.news_summary.slice(0, 80) + '...'
    : item.news_summary

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, width: '100%', minWidth: 0 }}>
      {/* Tags cluster */}
      <Space size={4} style={{ flexShrink: 0 }}>
        <Tag color={matCfg.color} style={{ margin: 0, fontSize: 11, lineHeight: '20px', fontWeight: 600 }}>{matCfg.icon} {matCfg.label}</Tag>
        <Tag color="blue" style={{ margin: 0, fontSize: 11, lineHeight: '20px' }}>{item.name_cn} {item.ticker}</Tag>
        <Tag style={{ margin: 0, fontSize: 11, lineHeight: '20px', color: sentCfg.color, borderColor: sentCfg.color, background: 'transparent' }}>{sentCfg.icon} {sentCfg.label}</Tag>
        {item.should_alert && <Tag color="green" style={{ margin: 0, fontSize: 11, lineHeight: '20px' }}><NotificationOutlined /></Tag>}
        {recent && <Tag color="red" style={{ margin: 0, fontSize: 11, lineHeight: '20px', fontWeight: 600 }}>NEW</Tag>}
      </Space>
      {/* Summary text */}
      <span style={{ flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: 13, color: '#262626' }}>
        {shortSummary}
      </span>
      {/* Time */}
      <span style={{ flexShrink: 0, fontSize: 11, color: '#8c8c8c', marginLeft: 8 }}>{timeAgo(item.scan_time)}</span>
    </div>
  )
}

/* ── Main Component ── */

export default function Dashboard() {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const user = useAuthStore((s) => s.user)
  const role = user?.role || 'viewer'

  const [breakingNews, setBreakingNews] = useState<BreakingNewsItem[]>([])
  const [newsSummary, setNewsSummary] = useState<Record<string, NewsSummary>>({})
  const [holdings, setHoldings] = useState<PortfolioHolding[]>([])
  const [quotes, setQuotes] = useState<Record<string, StockQuote>>({})
  const [consensus, setConsensus] = useState<Record<string, ConsensusData>>({})
  const [fundaLatest, setFundaLatest] = useState<Record<string, FundaSentimentItem>>({})
  const [fundaTrends, setFundaTrends] = useState<Record<string, FundaSentimentTrend>>({})
  const [sources, setSources] = useState<SourceHealth[]>([])
  const [scanner, setScanner] = useState<ScannerStatus | null>(null)
  const [loading, setLoading] = useState(true)
  const [hours, setHours] = useState(168)
  const [refreshing, setRefreshing] = useState(false)
  const [apiError, setApiError] = useState<string | null>(null)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Quotes are fetched separately — they're the slowest call (external APIs)
  // and we don't want to block the main page render on them.
  const fetchQuotes = useCallback(() => {
    if (role !== 'boss' && role !== 'admin') return
    api.get('/sources/portfolio/quotes')
      .then((res) => setQuotes(res.data.quotes || {}))
      .catch((err) => console.warn('[quotes] fetch failed', err))
  }, [role])

  // Consensus forecast — fed by the 30-min-cached Wind MySQL. Non-blocking.
  const fetchConsensus = useCallback(() => {
    if (role !== 'boss' && role !== 'admin') return
    api.get('/sources/portfolio/consensus')
      .then((res) => setConsensus(res.data.consensus || {}))
      .catch((err) => console.warn('[consensus] fetch failed', err))
  }, [role])

  // funda 推特情绪 — 持仓覆盖列表 + 7 日历史. 数据每日刷新, 无需高频轮询.
  const fetchFundaSentiment = useCallback(() => {
    if (role !== 'boss' && role !== 'admin') return
    api.get('/funda-db/sentiment/my-watchlist', { params: { days: 7 } })
      .then((res) => {
        const data = res.data || {}
        const latestArr: FundaSentimentItem[] = data.latest || []
        const historyArr: FundaSentimentItem[] = data.history || []
        const latestMap: Record<string, FundaSentimentItem> = {}
        latestArr.forEach((it) => { if (it.ticker) latestMap[it.ticker] = it })

        // 趋势: 取每只股 history 里有 twitter_score 的最早 / 最晚两天, 算 delta
        const grouped: Record<string, FundaSentimentItem[]> = {}
        historyArr.forEach((it) => {
          if (!it.ticker) return
          if (!grouped[it.ticker]) grouped[it.ticker] = []
          grouped[it.ticker].push(it)
        })
        const trendMap: Record<string, FundaSentimentTrend> = {}
        Object.entries(grouped).forEach(([ticker, items]) => {
          const scored = items
            .filter((x) => x.twitter_score != null)
            .sort((a, b) => a.date.localeCompare(b.date))
          if (scored.length === 0) {
            trendMap[ticker] = { delta: null, scored_days: 0, earliest_score: null, earliest_date: null }
            return
          }
          const first = scored[0]
          const last = scored[scored.length - 1]
          trendMap[ticker] = {
            delta: scored.length > 1 ? (last.twitter_score! - first.twitter_score!) : null,
            scored_days: scored.length,
            earliest_score: first.twitter_score,
            earliest_date: first.date,
          }
        })
        setFundaLatest(latestMap)
        setFundaTrends(trendMap)
      })
      .catch((err) => console.warn('[funda-sentiment] fetch failed', err))
  }, [role])

  const fetchData = useCallback((h: number, silent = false) => {
    if (!silent) setRefreshing(true)
    setApiError(null)

    const requests: Promise<any>[] = [
      api.get('/portfolio/breaking-news', { params: { hours: h, limit: 100 } }),
      api.get('/portfolio/breaking-news/summary', { params: { hours: h } }),
      api.get('/portfolio/scanner-status'),
    ]
    if (role === 'boss' || role === 'admin') requests.push(api.get('/sources/portfolio'))
    if (role === 'admin') requests.push(api.get('/sources/health'))

    return Promise.all(requests)
      .then((results) => {
        setBreakingNews(results[0].data.items || [])
        setNewsSummary(results[1].data.summary || {})
        setScanner(results[2].data || null)
        if (results[0].data.error) setApiError(results[0].data.error)
        let idx = 3
        if (role === 'boss' || role === 'admin') {
          setHoldings(results[idx]?.data?.holdings || [])
          idx++
        }
        if (role === 'admin') setSources(results[idx]?.data?.sources || [])
      })
      .catch((err) => { console.error(err); setApiError(err.message || 'Failed to fetch data') })
      .finally(() => { if (!silent) setRefreshing(false) })
  }, [role])

  useEffect(() => {
    // Initial load: main data unblocks the page, quotes trickle in separately
    fetchData(hours).finally(() => setLoading(false))
    fetchQuotes()
    fetchConsensus()
    fetchFundaSentiment()
  }, [role]) // eslint-disable-line
  useEffect(() => { if (!loading) { setRefreshing(true); fetchData(hours).finally(() => setRefreshing(false)) } }, [hours]) // eslint-disable-line
  useEffect(() => {
    if (timerRef.current) clearInterval(timerRef.current)
    if (!loading) {
      timerRef.current = setInterval(() => { fetchData(hours, true); fetchQuotes() }, AUTO_REFRESH_INTERVAL)
    }
    return () => { if (timerRef.current) clearInterval(timerRef.current) }
  }, [hours, loading, fetchData, fetchQuotes])

  if (loading) return <div style={{ textAlign: 'center', padding: 100 }}><Spin size="large" /></div>

  const totalAlerts = breakingNews.length

  return (
    <div>
      {/* ── Header ── */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <Text strong style={{ fontSize: 18 }}><FundOutlined style={{ marginRight: 8 }} />{t('dashboard.title')}</Text>
            {refreshing && <SyncOutlined spin style={{ color: '#1677ff', fontSize: 14 }} />}
            <ScannerStatusBadge scanner={scanner} />
          </div>
          <Text type="secondary" style={{ fontSize: 13 }}>{t('dashboard.subtitle')}</Text>
        </div>
        <Select value={hours} onChange={setHours} style={{ width: 90 }} options={HOURS_OPTIONS} size="small" />
      </div>

      {/* ── Error ── */}
      {apiError && (
        <Card size="small" style={{ marginBottom: 12, background: '#fff2e8', borderColor: '#ffbb96' }}
          styles={{ body: { padding: '8px 16px' } }}>
          <Text style={{ color: '#d4380d', fontSize: 13 }}><CloseCircleOutlined style={{ marginRight: 6 }} />{apiError}</Text>
        </Card>
      )}

      {/* ── News Feed (single-column collapsible list) ── */}
      {totalAlerts === 0 ? (
        <Card style={{ marginBottom: 16 }}>
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={
            <div>
              <Text type="secondary" style={{ fontSize: 14 }}>{t('dashboard.noAlerts')}</Text>
              <br /><Text type="secondary" style={{ fontSize: 12 }}>{t('dashboard.noAlertsHint')}</Text>
              {scanner?.last_scan_at && <><br /><Text type="secondary" style={{ fontSize: 12 }}>{t('dashboard.lastScan')}: {timeAgo(scanner.last_scan_at)}</Text></>}
            </div>
          } />
        </Card>
      ) : (
        <Card size="small" style={{ marginBottom: 16 }} styles={{ body: { padding: 0 } }}>
          <Collapse
            accordion
            ghost
            expandIconPosition="start"
            items={breakingNews.map((item) => {
              const matCfg = MATERIALITY_CONFIG[item.news_materiality] || MATERIALITY_CONFIG.none
              return {
                key: item.id,
                label: newsCollapseLabel(item),
                style: { borderLeft: `3px solid ${matCfg.color}`, marginBottom: 0 },
                children: (
                  <NewsDetail
                    item={item}
                    onTickerClick={(ticker) => navigate(`/stock-search?q=${encodeURIComponent(ticker)}`)}
                  />
                ),
              }
            })}
          />
        </Card>
      )}

      {/* ── Market Sentiment (SentimenTrader) — three paid indicators shown above
          the portfolio overview. Silent-fails if the scraper hasn't run yet. ── */}
      {(role === 'boss' || role === 'admin') && <SentimentTraderCards />}

      {/* ── Portfolio Overview (grouped by region, Chinese price convention) ── */}
      {(role === 'boss' || role === 'admin') && holdings.length > 0 && (() => {
        // Group by region
        const grouped: Record<string, PortfolioHolding[]> = {}
        holdings.forEach((h) => {
          const region = REGION_MAP[h.stock_market] || '其他'
          if (!grouped[region]) grouped[region] = []
          grouped[region].push(h)
        })
        // Sort within each group: gainers first, no-quote last, then by name
        Object.values(grouped).forEach((arr) => {
          arr.sort((a, b) => {
            const ca = quotes[a.stock_ticker]?.change_pct
            const cb = quotes[b.stock_ticker]?.change_pct
            if (ca == null && cb == null) return a.stock_name.localeCompare(b.stock_name)
            if (ca == null) return 1
            if (cb == null) return -1
            if (ca !== cb) return cb - ca
            return a.stock_name.localeCompare(b.stock_name)
          })
        })
        const regions = REGION_ORDER.filter((r) => grouped[r]?.length > 0)

        return (
          <Card title={<span><StockOutlined style={{ marginRight: 8 }} />{t('dashboard.portfolioOverview')}</span>}
            size="small" style={{ marginBottom: 16 }}>
            {regions.map((region, rIdx) => {
              const items = grouped[region]
              const meta = REGION_META[region]
              // Aggregate stats for this region
              let up = 0, down = 0, flat = 0
              const changes: number[] = []
              items.forEach((h) => {
                const q = quotes[h.stock_ticker]
                if (q?.change_pct != null) {
                  changes.push(q.change_pct)
                  if (q.change_pct > 0) up++
                  else if (q.change_pct < 0) down++
                  else flat++
                }
              })
              const avgChange = changes.length ? changes.reduce((a, b) => a + b, 0) / changes.length : null

              return (
                <div key={region} style={{ marginBottom: rIdx < regions.length - 1 ? 18 : 0 }}>
                  {/* Group header */}
                  <div style={{
                    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                    padding: '6px 12px', marginBottom: 8,
                    background: '#fafafa', borderRadius: 6,
                    borderLeft: `3px solid ${meta.accent}`,
                  }}>
                    <Space size={8}>
                      <span style={{ fontSize: 16 }}>{meta.flag}</span>
                      <Text strong style={{ fontSize: 14, color: '#262626' }}>{region}</Text>
                      <Text type="secondary" style={{ fontSize: 12 }}>{items.length}</Text>
                    </Space>
                    <Space size={12} style={{ fontSize: 12, fontVariantNumeric: 'tabular-nums' }}>
                      {avgChange != null && (
                        <span>
                          <Text type="secondary" style={{ marginRight: 4 }}>{t('dashboard.avgChange')}</Text>
                          <span style={{ color: priceColor(avgChange), fontWeight: 700 }}>
                            {avgChange > 0 ? '+' : ''}{avgChange.toFixed(2)}%
                          </span>
                        </span>
                      )}
                      <Space size={6}>
                        <span style={{ color: UP_COLOR, fontWeight: 700 }}>▲ {up}</span>
                        <span style={{ color: DOWN_COLOR, fontWeight: 700 }}>▼ {down}</span>
                        {flat > 0 && <span style={{ color: FLAT_COLOR, fontWeight: 700 }}>— {flat}</span>}
                      </Space>
                    </Space>
                  </div>

                  {/* Grid of cards */}
                  <Row gutter={[8, 8]} align="stretch">
                    {items.map((h) => {
                      const quote = quotes[h.stock_ticker]
                      const hasQuote = quote && !quote.error && quote.latest_price != null
                      const chg = quote?.change_pct ?? null
                      const col = priceColor(chg)
                      const tickerSummary = newsSummary[h.stock_ticker]
                      const hasAlert = !!tickerSummary
                      const matCfg = hasAlert ? MATERIALITY_CONFIG[tickerSummary.latest_materiality] || MATERIALITY_CONFIG.none : null
                      const showSubmarket = region === 'A股'
                      const cons = consensus[h.stock_ticker]
                      const hasConsensus = region === 'A股' && !!cons
                      const upside = hasConsensus && cons!.target_price && quote?.latest_price
                        ? ((cons!.target_price - quote.latest_price) / quote.latest_price) * 100
                        : null
                      const rcol = hasConsensus ? ratingColor(cons!.rating_label) : FLAT_COLOR
                      const fundaSent = fundaLatest[h.stock_ticker]
                      const fundaTrend = fundaTrends[h.stock_ticker]
                      const hasFunda = !!fundaSent && fundaSent.twitter_score != null
                      const fundaCol = fundaScoreColor(fundaSent?.twitter_score)
                      const fundaLab = fundaScoreLabel(fundaSent?.twitter_score)
                      const fundaArr = fundaTrendArrow(fundaTrend?.delta ?? null)

                      return (
                        <Col key={`${h.stock_market}-${h.stock_ticker}`} xs={12} sm={8} md={6} lg={6} xl={4}>
                          <Card size="small" hoverable
                            style={{
                              height: '100%',
                              borderLeft: `3px solid ${hasQuote ? col : '#f0f0f0'}`,
                            }}
                            styles={{ body: { padding: '10px 12px' } }}
                            onMouseEnter={() => {
                              // Speculative prefetch — warms the backend's
                              // 5-min Redis cache so the click that follows
                              // loads from cache instead of paying the full
                              // 21-way Mongo fan-out.
                              const canonical = toCanonical(h.stock_ticker, h.stock_market)
                              if (canonical) {
                                api.get(`/stock-hub/${canonical}`, {
                                  params: { limit: 80, stock_name: h.stock_name || undefined },
                                }).catch(() => {})
                              }
                            }}
                            onClick={() => {
                              const canonical = toCanonical(h.stock_ticker, h.stock_market)
                              if (canonical) {
                                const url = `/stock/${canonical}?name=${encodeURIComponent(h.stock_name || '')}`
                                window.open(url, '_blank', 'noopener,noreferrer')
                              } else {
                                navigate(`/stock-search?q=${encodeURIComponent(h.stock_ticker)}`)
                              }
                            }}>
                            {/* Top: name + optional submarket tag */}
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 6, marginBottom: 2 }}>
                              <div style={{ flex: 1, minWidth: 0 }}>
                                <div style={{ fontWeight: 600, fontSize: 13, lineHeight: 1.3, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', color: '#262626' }}>
                                  {h.stock_name}
                                </div>
                                <Text type="secondary" style={{ fontSize: 11, fontVariantNumeric: 'tabular-nums' }}>{h.stock_ticker}</Text>
                              </div>
                              {showSubmarket && (
                                <Tag style={{ fontSize: 10, margin: 0, padding: '0 5px', lineHeight: '16px', flexShrink: 0 }}
                                  color={MARKET_TAG_COLORS[h.stock_market] || 'default'}>
                                  {h.stock_market}
                                </Tag>
                              )}
                            </div>

                            {/* Primary data: large price + change% */}
                            {hasQuote ? (
                              <>
                                <div style={{ textAlign: 'center', margin: '8px 0 6px' }}>
                                  <div style={{
                                    fontSize: 22, fontWeight: 700,
                                    color: col, lineHeight: 1.1,
                                    fontVariantNumeric: 'tabular-nums',
                                    letterSpacing: '-0.3px',
                                  }}>
                                    {formatPrice(quote.latest_price)}
                                  </div>
                                  <div style={{
                                    fontSize: 14, fontWeight: 700,
                                    color: col, marginTop: 3,
                                    fontVariantNumeric: 'tabular-nums',
                                  }}>
                                    {chg != null && (
                                      <>
                                        {priceArrow(chg)} {chg > 0 ? '+' : ''}{chg.toFixed(2)}%
                                      </>
                                    )}
                                  </div>
                                </div>

                                {/* Divider */}
                                <div style={{ height: 1, background: '#f0f0f0', margin: '6px -12px 6px' }} />

                                {/* Secondary data */}
                                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, color: '#8c8c8c', fontVariantNumeric: 'tabular-nums' }}>
                                  <span>{t('dashboard.prevClose')} <span style={{ color: '#595959' }}>{formatPrice(quote.prev_close)}</span></span>
                                  <span>{t('dashboard.pe')} <span style={{ color: '#595959' }}>{formatPE(quote.pe_ttm)}</span></span>
                                </div>
                                <div style={{ fontSize: 10, color: '#8c8c8c', marginTop: 3, textAlign: 'center' }}>
                                  <Tooltip title={`${t('dashboard.marketCap')}: ${quote.market_cap?.toLocaleString() ?? '—'} ${quote.currency}`}>
                                    <span>{t('dashboard.marketCap')} <span style={{ color: '#595959', fontVariantNumeric: 'tabular-nums' }}>{formatMarketCap(quote.market_cap, quote.currency)}</span></span>
                                  </Tooltip>
                                </div>
                              </>
                            ) : (
                              <div style={{
                                textAlign: 'center',
                                margin: '14px 0',
                                fontSize: 13,
                                color: '#bfbfbf',
                                minHeight: 52,
                                display: 'flex', alignItems: 'center', justifyContent: 'center',
                              }}>
                                {quote?.error ? t('dashboard.quoteUnavailable') : '···'}
                              </div>
                            )}

                            {/* Consensus forecast (A-share only) */}
                            {hasConsensus && (
                              <Tooltip
                                title={
                                  <div style={{ fontSize: 12, minWidth: 240 }}>
                                    <div style={{ fontWeight: 700, marginBottom: 6, borderBottom: '1px solid rgba(255,255,255,0.2)', paddingBottom: 4 }}>
                                      {t('dashboard.consensusTitle')} · {h.stock_name}
                                    </div>
                                    <div style={{ display: 'grid', gridTemplateColumns: 'auto 1fr 1fr 1fr', gap: '4px 8px', fontVariantNumeric: 'tabular-nums' }}>
                                      <span style={{ opacity: 0.7 }}></span>
                                      <span style={{ opacity: 0.7, textAlign: 'right' }}>FY1·{cons!.fy1.year ?? '—'}</span>
                                      <span style={{ opacity: 0.7, textAlign: 'right' }}>FY2·{cons!.fy2.year ?? '—'}</span>
                                      <span style={{ opacity: 0.7, textAlign: 'right' }}>FY3·{cons!.fy3.year ?? '—'}</span>

                                      <span style={{ opacity: 0.8 }}>PE</span>
                                      <span style={{ textAlign: 'right' }}>{formatPE(cons!.fy1.pe)}</span>
                                      <span style={{ textAlign: 'right' }}>{formatPE(cons!.fy2.pe)}</span>
                                      <span style={{ textAlign: 'right' }}>{formatPE(cons!.fy3.pe)}</span>

                                      <span style={{ opacity: 0.8 }}>{t('dashboard.consensusPB')}</span>
                                      <span style={{ textAlign: 'right' }}>{cons!.fy1.pb?.toFixed(2) ?? '—'}</span>
                                      <span style={{ textAlign: 'right' }}>{cons!.fy2.pb?.toFixed(2) ?? '—'}</span>
                                      <span style={{ textAlign: 'right' }}>{cons!.fy3.pb?.toFixed(2) ?? '—'}</span>

                                      <span style={{ opacity: 0.8 }}>{t('dashboard.consensusROE')}</span>
                                      <span style={{ textAlign: 'right' }}>{cons!.fy1.roe != null ? `${cons!.fy1.roe.toFixed(1)}%` : '—'}</span>
                                      <span style={{ textAlign: 'right' }}>{cons!.fy2.roe != null ? `${cons!.fy2.roe.toFixed(1)}%` : '—'}</span>
                                      <span style={{ textAlign: 'right' }}>{cons!.fy3.roe != null ? `${cons!.fy3.roe.toFixed(1)}%` : '—'}</span>

                                      <span style={{ opacity: 0.8 }}>{t('dashboard.consensusNetProfit')}</span>
                                      <span style={{ textAlign: 'right' }}>{formatRMB(cons!.fy1.net_profit)}</span>
                                      <span style={{ textAlign: 'right' }}>{formatRMB(cons!.fy2.net_profit)}</span>
                                      <span style={{ textAlign: 'right' }}>{formatRMB(cons!.fy3.net_profit)}</span>

                                      <span style={{ opacity: 0.8 }}>{t('dashboard.consensusEPS')}</span>
                                      <span style={{ textAlign: 'right' }}>{cons!.fy1.eps?.toFixed(2) ?? '—'}</span>
                                      <span style={{ textAlign: 'right' }}>{cons!.fy2.eps?.toFixed(2) ?? '—'}</span>
                                      <span style={{ textAlign: 'right' }}>{cons!.fy3.eps?.toFixed(2) ?? '—'}</span>
                                    </div>
                                    <div style={{ marginTop: 6, paddingTop: 4, borderTop: '1px solid rgba(255,255,255,0.2)', fontSize: 11 }}>
                                      {cons!.yoy_net_profit != null && (
                                        <div>{t('dashboard.consensusNPGrowth')}: <b>{cons!.yoy_net_profit.toFixed(1)}%</b></div>
                                      )}
                                      <div style={{ marginTop: 2 }}>
                                        <span style={{ color: '#ff7875' }}>{t('dashboard.consensusBuy')} {cons!.rating_num_buy}</span>
                                        {' · '}<span style={{ color: '#ffc069' }}>{t('dashboard.consensusOutperform')} {cons!.rating_num_outperform}</span>
                                        {' · '}<span style={{ opacity: 0.75 }}>{t('dashboard.consensusHold')} {cons!.rating_num_hold}</span>
                                        {(cons!.rating_num_underperform + cons!.rating_num_sell) > 0 && (
                                          <>{' · '}<span style={{ color: '#95de64' }}>{t('dashboard.consensusUnderperform')} {cons!.rating_num_underperform} / {t('dashboard.consensusSell')} {cons!.rating_num_sell}</span></>
                                        )}
                                      </div>
                                      <div style={{ marginTop: 2, opacity: 0.7 }}>{t('dashboard.consensusAsOf')}: {formatConsensusDate(cons!.as_of)}</div>
                                    </div>
                                  </div>
                                }
                              >
                                <div style={{
                                  marginTop: 6,
                                  padding: '4px 6px',
                                  background: '#fafafa',
                                  border: '1px solid #f0f0f0',
                                  borderRadius: 4,
                                  fontSize: 10,
                                  color: '#595959',
                                  fontVariantNumeric: 'tabular-nums',
                                }}>
                                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 2 }}>
                                    <span style={{ color: '#8c8c8c' }}>{t('dashboard.consensusTargetPrice')}</span>
                                    <span style={{ fontWeight: 700, color: '#262626' }}>
                                      {cons!.target_price != null ? `¥${cons!.target_price.toFixed(2)}` : '—'}
                                      {upside != null && (
                                        <span style={{ color: upside > 0 ? UP_COLOR : DOWN_COLOR, marginLeft: 4, fontWeight: 700 }}>
                                          ({upside > 0 ? '+' : ''}{upside.toFixed(1)}%)
                                        </span>
                                      )}
                                    </span>
                                  </div>
                                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                    <span>
                                      <span style={{
                                        display: 'inline-block', padding: '0 4px', borderRadius: 2,
                                        background: rcol, color: '#fff', fontWeight: 600, fontSize: 9, marginRight: 4,
                                      }}>{cons!.rating_label || '—'}</span>
                                      <span style={{ color: '#8c8c8c' }}>{cons!.analyst_count ?? 0}{t('dashboard.consensusAnalysts')}</span>
                                    </span>
                                    <span style={{ color: '#8c8c8c' }}>
                                      {t('dashboard.consensusForwardPE')} <span style={{ color: '#262626', fontWeight: 600 }}>{formatPE(cons!.fy1.pe)}</span>
                                    </span>
                                  </div>
                                </div>
                              </Tooltip>
                            )}

                            {/* funda · 推特情绪因子 (仅当有当日数据) */}
                            {hasFunda && (
                              <Tooltip
                                title={
                                  <div style={{ fontSize: 12, maxWidth: 340 }}>
                                    <div style={{ fontWeight: 700, marginBottom: 6, borderBottom: '1px solid rgba(255,255,255,0.2)', paddingBottom: 4 }}>
                                      <FundOutlined style={{ marginRight: 4 }} />
                                      funda · {t('dashboard.fundaSentimentTitle')} · {h.stock_name}
                                    </div>
                                    <div style={{ display: 'grid', gridTemplateColumns: 'auto 1fr', gap: '3px 10px', fontVariantNumeric: 'tabular-nums' }}>
                                      <span style={{ opacity: 0.7 }}>Twitter</span>
                                      <span>
                                        <b style={{ color: fundaCol }}>{fundaSent!.twitter_score!.toFixed(1)}</b>
                                        <span style={{ marginLeft: 4 }}>{fundaLab}</span>
                                        <span style={{ opacity: 0.7, marginLeft: 6 }}>· {fundaSent!.twitter_count} {t('dashboard.fundaSentimentTweets')}</span>
                                      </span>
                                      <span style={{ opacity: 0.7 }}>{t('dashboard.fundaSentimentDate')}</span>
                                      <span>{fundaSent!.date}</span>
                                      {fundaTrend && fundaTrend.delta != null && fundaTrend.scored_days > 1 && (
                                        <>
                                          <span style={{ opacity: 0.7 }}>{t('dashboard.fundaSentimentTrend')}</span>
                                          <span style={{ color: fundaArr.color }}>
                                            {fundaArr.glyph} {fundaTrend.delta > 0 ? '+' : ''}{fundaTrend.delta.toFixed(2)}
                                            <span style={{ opacity: 0.7, color: 'inherit' }}> ({t('dashboard.fundaSentimentFrom')} {fundaTrend.earliest_score?.toFixed(1)}, {fundaTrend.scored_days} {t('dashboard.fundaSentimentDays')})</span>
                                          </span>
                                        </>
                                      )}
                                      {fundaSent!.reddit_score != null && (
                                        <>
                                          <span style={{ opacity: 0.7 }}>Reddit</span>
                                          <span>
                                            <b style={{ color: fundaScoreColor(fundaSent!.reddit_score) }}>{fundaSent!.reddit_score.toFixed(1)}</b>
                                            <span style={{ opacity: 0.7, marginLeft: 6 }}>· {fundaSent!.reddit_count} {t('dashboard.fundaSentimentPosts')}</span>
                                          </span>
                                        </>
                                      )}
                                      {(fundaSent!.sector || fundaSent!.industry) && (
                                        <>
                                          <span style={{ opacity: 0.7 }}>{t('dashboard.fundaSentimentSector')}</span>
                                          <span>{fundaSent!.sector}{fundaSent!.industry ? ` / ${fundaSent!.industry}` : ''}</span>
                                        </>
                                      )}
                                    </div>
                                    {fundaSent!.ai_summary && (
                                      <div style={{ marginTop: 8, paddingTop: 6, borderTop: '1px solid rgba(255,255,255,0.2)', whiteSpace: 'pre-wrap', fontSize: 11, lineHeight: 1.5 }}>
                                        {fundaSent!.ai_summary}
                                      </div>
                                    )}
                                  </div>
                                }
                              >
                                <div style={{
                                  marginTop: 6,
                                  padding: '3px 6px',
                                  background: '#fafafa',
                                  border: '1px solid #f0f0f0',
                                  borderRadius: 4,
                                  fontSize: 10,
                                  color: '#595959',
                                  display: 'flex',
                                  alignItems: 'center',
                                  justifyContent: 'space-between',
                                  gap: 4,
                                  fontVariantNumeric: 'tabular-nums',
                                }}>
                                  <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, minWidth: 0 }}>
                                    <FundOutlined style={{ color: fundaCol, fontSize: 11 }} />
                                    <span style={{ fontWeight: 700, color: fundaCol }}>{fundaSent!.twitter_score!.toFixed(1)}</span>
                                    <span style={{
                                      display: 'inline-block', padding: '0 4px', borderRadius: 2,
                                      background: fundaCol, color: '#fff', fontWeight: 600, fontSize: 9,
                                    }}>{fundaLab}</span>
                                  </span>
                                  <span style={{ color: '#8c8c8c', fontSize: 9, display: 'inline-flex', alignItems: 'center', gap: 3 }}>
                                    {fundaSent!.twitter_count}{t('dashboard.fundaSentimentPostShort')}
                                    <span style={{ color: fundaArr.color, fontWeight: 700, fontSize: 11 }}>{fundaArr.glyph}</span>
                                  </span>
                                </div>
                              </Tooltip>
                            )}

                            {/* Alert badge */}
                            {hasAlert && (
                              <div style={{
                                marginTop: 6, padding: '2px 6px',
                                background: matCfg!.bg, borderRadius: 3,
                                fontSize: 10, color: matCfg!.color,
                                textAlign: 'center', fontWeight: 600,
                              }}>
                                <ThunderboltOutlined style={{ marginRight: 2 }} />
                                {tickerSummary.news_count}条{matCfg!.label}
                              </div>
                            )}
                          </Card>
                        </Col>
                      )
                    })}
                  </Row>
                </div>
              )
            })}
          </Card>
        )
      })()}

      {/* ── Source Health (admin) ── */}
      {role === 'admin' && sources.length > 0 && (
        <Card title={t('dashboard.sourceHealth')} size="small"
          extra={<Text type="secondary" style={{ fontSize: 12 }}>
            <CheckCircleOutlined style={{ color: '#10b981', marginRight: 4 }} />
            {sources.filter((s) => s.is_healthy).length} / {sources.length}
            {sources.filter((s) => !s.is_healthy).length > 0 && (
              <span style={{ color: '#ef4444', marginLeft: 8 }}><CloseCircleOutlined style={{ marginRight: 2 }} />{sources.filter((s) => !s.is_healthy).length} unhealthy</span>
            )}
          </Text>}>
          {sources.filter((s) => !s.is_healthy).length > 0 && (
            <div style={{ marginBottom: 12, padding: '8px 12px', background: '#fef2f2', borderRadius: 6, border: '1px solid #fecaca' }}>
              <Text strong style={{ color: '#dc2626', fontSize: 13 }}><CloseCircleOutlined style={{ marginRight: 4 }} />{t('dashboard.unhealthySources')}</Text>
              <div style={{ marginTop: 6 }}>
                {sources.filter((s) => !s.is_healthy).map((s) => (
                  <div key={s.source_name} style={{ padding: '4px 0', fontSize: 12, color: '#374151' }}>
                    <Text strong style={{ color: '#dc2626' }}>{s.source_name}</Text>
                    <Text type="secondary" style={{ marginLeft: 8 }}>{t('dashboard.consecutiveFailures')}: {s.consecutive_failures}</Text>
                  </div>
                ))}
              </div>
            </div>
          )}
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            {sources.sort((a, b) => (a.is_healthy === b.is_healthy ? 0 : a.is_healthy ? 1 : -1)).map((s) => (
              <Tag key={s.source_name} icon={s.is_healthy ? <CheckCircleOutlined /> : <CloseCircleOutlined />}
                color={s.is_healthy ? 'success' : 'error'}>
                {s.source_name} {s.is_healthy ? `(${s.total_items_fetched})` : `(${s.consecutive_failures}x fail)`}
              </Tag>
            ))}
          </div>
        </Card>
      )}
    </div>
  )
}
