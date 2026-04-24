/**
 * 情绪因子卡 · 读工作台 (watchlist) 里股票的 funda.ai 情绪数据.
 *
 * 后端: GET /api/funda-db/sentiment/my-watchlist?days=N
 *   → { tickers_in_watchlist, covered_tickers, missing_tickers, latest[], history[] }
 *
 * 数据来源: crawl/funda/scraper.py --sentiment → funda.sentiments MongoDB collection.
 * 分数范围 0-10, 中值 5. 着色: ≥7 绿, 4-7 黄, <4 红.
 */
import { Component, ErrorInfo, ReactNode, useCallback, useEffect, useState } from 'react'
import {
  Alert, Button, Card, Empty, Popover, Progress, Space, Spin, Table, Tag, Typography,
} from 'antd'
import {
  ReloadOutlined, MessageOutlined, InfoCircleOutlined,
  FundOutlined,
} from '@ant-design/icons'
import api from '../services/api'

const { Text, Paragraph } = Typography

interface SentimentItem {
  ticker: string               // 持仓 ticker (裸码)
  portfolio_ticker?: string
  funda_ticker?: string        // funda.ai 原始 ticker (可能带 .KS/.HK 后缀)
  stock_name?: string          // 持仓 YAML 的中文/英文名, 优先显示
  stock_market?: string        // 美股 / 港股 / 创业板 / ...
  date: string
  company: string              // funda 给的英文全称 (fallback)
  sector: string
  industry: string
  reddit_score: number | null
  reddit_count: number
  twitter_score: number | null
  twitter_count: number
  ai_summary: string
  crawled_at: string | null
}

interface MissingDetail {
  ticker: string
  stock_name: string
  stock_market: string
}

interface WatchlistSentimentResp {
  tickers_in_watchlist: string[]
  covered_tickers: string[]
  missing_tickers: string[]
  missing_tickers_detail?: MissingDetail[]
  date_range: { from?: string; to?: string; days?: number }
  latest: SentimentItem[]
  history: SentimentItem[]
  note?: string
}

// 市场标签颜色 — 对齐 Dashboard 持仓概览
const MARKET_TAG_COLORS: Record<string, string> = {
  '美股': 'blue',
  '港股': 'purple',
  '主板': 'red',
  '创业板': 'orange',
  '科创板': 'geekblue',
  '北交所': 'cyan',
  '韩股': 'magenta',
  '日股': 'gold',
}

function scoreColor(score: number | null | undefined): string {
  if (score === null || score === undefined) return '#94a3b8'  // grey
  if (score >= 7) return '#10b981'   // green
  if (score >= 4) return '#f59e0b'   // amber
  return '#ef4444'                    // red
}

function formatScore(score: number | null | undefined): string {
  if (score === null || score === undefined) return '—'
  return score.toFixed(1)
}

function scoreLabel(score: number | null | undefined): string {
  if (score === null || score === undefined) return '无数据'
  if (score >= 7) return '看多'
  if (score >= 5.5) return '偏多'
  if (score >= 4.5) return '中性'
  if (score >= 3) return '偏空'
  return '看空'
}

// 兜底错误边界 — 任何子组件 render 异常都显示提示, 不拖垮整页白屏
class CardErrorBoundary extends Component<
  { children: ReactNode },
  { error: Error | null }
> {
  state = { error: null as Error | null }

  static getDerivedStateFromError(error: Error) {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('[FundaSentimentCard] render crash:', error, info)
  }

  render() {
    if (this.state.error) {
      return (
        <Alert
          type="error"
          showIcon
          message="情绪因子卡渲染失败"
          description={String(this.state.error?.message || this.state.error)}
          closable
          onClose={() => this.setState({ error: null })}
          action={
            <Button size="small" onClick={() => this.setState({ error: null })}>
              重试
            </Button>
          }
        />
      )
    }
    return this.props.children
  }
}

function FundaSentimentCardInner({
  days = 7,
  compact = false,
}: {
  days?: number
  compact?: boolean
}) {
  const [data, setData] = useState<WatchlistSentimentResp | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await api.get<WatchlistSentimentResp>(
        '/funda-db/sentiment/my-watchlist',
        { params: { days } },
      )
      const raw = res.data || ({} as WatchlistSentimentResp)
      // 防御式补齐: 任何缺失的数组字段归零, 避免 render 访问 undefined.length 崩溃
      setData({
        tickers_in_watchlist: raw.tickers_in_watchlist || [],
        covered_tickers: raw.covered_tickers || [],
        missing_tickers: raw.missing_tickers || [],
        missing_tickers_detail: raw.missing_tickers_detail || [],
        date_range: raw.date_range || {},
        latest: raw.latest || [],
        history: raw.history || [],
        note: raw.note,
      })
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || '加载失败')
    } finally {
      setLoading(false)
    }
  }, [days])

  useEffect(() => {
    load()
  }, [load])

  const renderSummary = (item: SentimentItem) => (
    <div style={{ maxWidth: 520, fontSize: 12, lineHeight: 1.7 }}>
      <Paragraph style={{ fontSize: 12, marginBottom: 6 }}>
        <b>{item.stock_name || item.company}</b>
        {item.ticker && <span style={{ marginLeft: 6, color: '#64748b' }}>({item.ticker})</span>}
        {item.stock_market && (
          <Tag color={MARKET_TAG_COLORS[item.stock_market] || 'default'}
               style={{ marginLeft: 6, fontSize: 10 }}>
            {item.stock_market}
          </Tag>
        )}
        {(item.sector || item.industry) && (
          <span style={{ display: 'block', color: '#94a3b8', marginTop: 2 }}>
            {item.sector}{item.industry && ` / ${item.industry}`}
          </span>
        )}
      </Paragraph>
      <Space wrap size={4} style={{ marginBottom: 8 }}>
        <Tag color={scoreColor(item.twitter_score)}>
          Twitter {formatScore(item.twitter_score)} ({item.twitter_count} 条)
        </Tag>
        <Tag>{item.date}</Tag>
      </Space>
      <div style={{ whiteSpace: 'pre-wrap' }}>{item.ai_summary || '(无 AI 摘要)'}</div>
    </div>
  )

  const columns = [
    {
      title: '股票',
      dataIndex: 'ticker',
      key: 'ticker',
      width: 170,
      render: (t: string, rec: SentimentItem) => (
        <Space direction="vertical" size={0} style={{ lineHeight: 1.3 }}>
          <Space size={6} wrap>
            <Text strong style={{ fontSize: 13 }}>
              {rec.stock_name || rec.company || t}
            </Text>
            {rec.stock_market && (
              <Tag
                color={MARKET_TAG_COLORS[rec.stock_market] || 'default'}
                style={{ margin: 0, fontSize: 10, padding: '0 5px', lineHeight: '16px' }}
              >
                {rec.stock_market}
              </Tag>
            )}
          </Space>
          <Text type="secondary" style={{ fontSize: 11, fontVariantNumeric: 'tabular-nums' }}>
            {t}
            {rec.funda_ticker && rec.funda_ticker !== t && (
              <span style={{ marginLeft: 4, color: '#cbd5e1' }}>({rec.funda_ticker})</span>
            )}
          </Text>
        </Space>
      ),
    },
    {
      title: 'Twitter',
      dataIndex: 'twitter_score',
      key: 'tw',
      width: 150,
      render: (s: number | null, rec: SentimentItem) => (
        <Space size={8}>
          <Progress
            type="circle"
            percent={s ? s * 10 : 0}
            size={30}
            strokeColor={scoreColor(s)}
            format={() => formatScore(s)}
            strokeWidth={10}
          />
          <Space direction="vertical" size={0}>
            <Tag color={scoreColor(s)} style={{ margin: 0, fontSize: 11 }}>
              {scoreLabel(s)}
            </Tag>
            <Text type="secondary" style={{ fontSize: 10 }}>
              {rec.twitter_count} 推文
            </Text>
          </Space>
        </Space>
      ),
      sorter: (a: SentimentItem, b: SentimentItem) =>
        (a.twitter_score ?? -1) - (b.twitter_score ?? -1),
    },
    {
      title: '摘要',
      dataIndex: 'ai_summary',
      key: 'summary',
      ellipsis: true,
      render: (_s: string, rec: SentimentItem) => (
        <Popover
          content={renderSummary(rec)}
          title={<Space><MessageOutlined /> {rec.stock_name || rec.ticker} 情绪摘要</Space>}
          trigger="click"
          placement="left"
        >
          <a style={{ fontSize: 12 }}>
            <InfoCircleOutlined /> {(rec.ai_summary || '').slice(0, 50)}...
          </a>
        </Popover>
      ),
    },
    {
      title: '日期',
      dataIndex: 'date',
      key: 'date',
      width: 90,
      render: (d: string) => <Text style={{ fontSize: 11 }}>{d}</Text>,
    },
  ]

  // 精简模式: 只显示 ticker + twitter_score 的 chip 网格
  if (compact && data?.latest?.length) {
    return (
      <Card
        size="small"
        title={
          <Space>
            <FundOutlined />
            <span>情绪因子 (funda.ai)</span>
            <Text type="secondary" style={{ fontSize: 11 }}>
              {data.covered_tickers.length}/{data.tickers_in_watchlist.length} 有数据
            </Text>
          </Space>
        }
        extra={<a onClick={load}><ReloadOutlined /></a>}
      >
        <Space wrap size={6}>
          {data.latest.map((item) => (
            <Popover
              key={item.ticker}
              content={renderSummary(item)}
              title={<Space><MessageOutlined /> {item.stock_name || item.ticker}</Space>}
              trigger="click"
              placement="left"
            >
              <Tag
                color={scoreColor(item.twitter_score)}
                style={{ margin: 0, cursor: 'pointer', fontSize: 12, padding: '2px 8px' }}
              >
                <b>{item.stock_name || item.ticker}</b> {formatScore(item.twitter_score)}
              </Tag>
            </Popover>
          ))}
        </Space>
      </Card>
    )
  }

  return (
    <Card
      size="small"
      title={
        <Space>
          <FundOutlined style={{ color: '#2563eb' }} />
          <span>情绪因子 · funda.ai</span>
          {data && (
            <Text type="secondary" style={{ fontSize: 12 }}>
              工作台 {data.tickers_in_watchlist.length} 只 · 覆盖 {data.covered_tickers.length} · 最近 {days} 天
            </Text>
          )}
        </Space>
      }
      extra={
        <Space>
          <Text type="secondary" style={{ fontSize: 11 }}>
            {data?.date_range?.from} ~ {data?.date_range?.to}
          </Text>
          <Button size="small" icon={<ReloadOutlined />} onClick={load} loading={loading}>
            刷新
          </Button>
        </Space>
      }
      bodyStyle={{ padding: loading ? 40 : 0 }}
    >
      {error && (
        <Alert type="warning" showIcon message="无法加载情绪因子" description={error} style={{ margin: 12 }} />
      )}
      {loading && <Spin />}
      {!loading && !error && data && data.latest.length === 0 && (
        <Empty
          description={
            data.note ||
            (data.tickers_in_watchlist.length > 0
              ? '工作台股票暂无情绪数据 (运行 funda scraper --sentiment 抓取当日)'
              : '工作台是空的')
          }
          style={{ padding: 24 }}
        />
      )}
      {!loading && !error && data && data.latest.length > 0 && (
        <Table
          size="small"
          rowKey="ticker"
          dataSource={data.latest}
          columns={columns}
          pagination={false}
          scroll={{ y: 380 }}
        />
      )}
    </Card>
  )
}

export default function FundaSentimentCard(props: { days?: number; compact?: boolean }) {
  return (
    <CardErrorBoundary>
      <FundaSentimentCardInner {...props} />
    </CardErrorBoundary>
  )
}
