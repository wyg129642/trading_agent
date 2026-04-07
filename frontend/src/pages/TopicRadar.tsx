import { useEffect, useState } from 'react'
import {
  Card, Col, Row, Statistic, Tag, List, Typography, Spin, Space,
  Tabs, Table, Badge, Tooltip, Divider, Empty, Select,
} from 'antd'
import {
  RadarChartOutlined, FireOutlined, RiseOutlined, FallOutlined,
  ThunderboltOutlined, ClockCircleOutlined, AlertOutlined,
  StockOutlined, EyeOutlined, LineChartOutlined,
} from '@ant-design/icons'
import ReactECharts from 'echarts-for-react'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'

dayjs.extend(relativeTime)
const { Text, Title } = Typography

/* ── Types ── */

interface HotNewsItem {
  id: string
  title: string
  source_name: string
  url: string
  fetched_at: string | null
  published_at: string | null
  relevance_score: number | null
  sentiment: string | null
  impact: string | null
  tickers: string[]
}

interface ClusterAnomaly {
  cluster_id: number
  size: number
  pct: number
  representative_titles: string[]
  tickers: Array<{ name?: string; code?: string }>
}

interface ClusterResult {
  id: number
  cluster_date: string
  run_time: string
  total_items: number
  n_clusters: number
  anomalies: ClusterAnomaly[]
  top_clusters: ClusterAnomaly[]
  summary: string
}

interface TickerMention {
  name: string
  code: string
  count: number
  bullish: number
  bearish: number
  neutral: number
}

interface OverviewData {
  hot_news_counts: Record<string, number>
  hot_news_items: HotNewsItem[]
  pending_filter_count: number
  enrichment_stats: { bullish: number; bearish: number; neutral: number }
  cluster_results: ClusterResult[]
}

/* ── Color maps ── */

const SOURCE_COLORS: Record<string, string> = {
  '华尔街见闻热点': '#1677ff',
  '财联社热点': '#eb2f96',
  '雪球热榜': '#fa8c16',
  '微博热搜': '#ff4d4f',
}

const SOURCE_SHORT: Record<string, string> = {
  '华尔街见闻热点': '华尔街',
  '财联社热点': '财联社',
  '雪球热榜': '雪球',
  '微博热搜': '微博',
}

/* ── Component ── */

export default function TopicRadar() {
  const [data, setData] = useState<OverviewData | null>(null)
  const [tickers, setTickers] = useState<TickerMention[]>([])
  const [loading, setLoading] = useState(true)
  const [sourceFilter, setSourceFilter] = useState<string | null>(null)

  useEffect(() => {
    Promise.all([
      api.get('/topic-radar/overview'),
      api.get('/topic-radar/top-tickers'),
    ])
      .then(([overviewRes, tickerRes]) => {
        setData(overviewRes.data)
        setTickers(tickerRes.data.tickers || [])
      })
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return <div style={{ textAlign: 'center', padding: 100 }}><Spin size="large" /></div>
  }

  if (!data) {
    return <Empty description="无法加载数据" />
  }

  const totalHotNews = Object.values(data.hot_news_counts).reduce((a, b) => a + b, 0)
  const latestCluster = data.cluster_results.length > 0 ? data.cluster_results[0] : null
  const anomalyCount = latestCluster?.anomalies?.length || 0

  // Filter hot news by source
  const filteredNews = sourceFilter
    ? data.hot_news_items.filter(n => n.source_name === sourceFilter)
    : data.hot_news_items

  // Top tickers bubble chart
  const topTickersChartOption = tickers.length > 0 ? {
    tooltip: {
      trigger: 'item' as const,
      formatter: (p: any) => {
        const d = p.data
        return `<b>${d[3]}</b> (${d[4]})<br/>提及: ${d[2]}次<br/>看多: ${d[5]}  看空: ${d[6]}  中性: ${d[7]}`
      },
    },
    xAxis: { show: false, min: 0, max: 100 },
    yAxis: { show: false, min: 0, max: 100 },
    series: [{
      type: 'scatter',
      symbolSize: (val: number[]) => Math.min(Math.max(val[2] * 3, 20), 80),
      data: tickers.slice(0, 20).map((t, i) => {
        const row = Math.floor(i / 5)
        const col = i % 5
        const sentiment = t.bullish > t.bearish ? 1 : t.bearish > t.bullish ? -1 : 0
        return {
          value: [col * 20 + 10, 80 - row * 25, t.count, t.name, t.code, t.bullish, t.bearish, t.neutral],
          itemStyle: {
            color: sentiment > 0 ? '#10b981' : sentiment < 0 ? '#ef4444' : '#94a3b8',
            opacity: 0.8,
          },
        }
      }),
      label: {
        show: true,
        formatter: (p: any) => p.data.value[3],
        fontSize: 11,
        color: '#1e293b',
      },
    }],
  } : null

  return (
    <div>
      {/* ── Header ── */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <Title level={4} style={{ margin: 0 }}>
          <RadarChartOutlined style={{ marginRight: 8 }} />
          舆情雷达
        </Title>
        <Text type="secondary" style={{ fontSize: 12 }}>
          <ClockCircleOutlined style={{ marginRight: 4 }} />
          数据更新: {dayjs().format('HH:mm:ss')}
        </Text>
      </div>

      {/* ── Stats Cards ── */}
      <Row gutter={[16, 16]}>
        <Col xs={24} sm={12} lg={8}>
          <Card className="stat-card">
            <Statistic
              title="市场相关热点 (24h)"
              value={totalHotNews}
              prefix={<FireOutlined style={{ color: '#ff4d4f' }} />}
              suffix={
                <Text type="secondary" style={{ fontSize: 12 }}>
                  条{data.pending_filter_count > 0 ? ` (${data.pending_filter_count}条待评估)` : ' (LLM过滤)'}
                </Text>
              }
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={8}>
          <Card className="stat-card">
            <Statistic
              title="研报多空比 (24h)"
              value={data.enrichment_stats.bearish > 0
                ? (data.enrichment_stats.bullish / data.enrichment_stats.bearish).toFixed(1)
                : data.enrichment_stats.bullish > 0 ? '∞' : '—'}
              prefix={<RiseOutlined style={{ color: '#10b981' }} />}
              suffix={
                <Text type="secondary" style={{ fontSize: 12 }}>
                  <span style={{ color: '#10b981' }}>{data.enrichment_stats.bullish}</span>
                  {' / '}
                  <span style={{ color: '#ef4444' }}>{data.enrichment_stats.bearish}</span>
                </Text>
              }
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={8}>
          <Card className="stat-card">
            <Statistic
              title="异常聚集话题"
              value={anomalyCount}
              prefix={<AlertOutlined style={{ color: anomalyCount > 0 ? '#ef4444' : '#94a3b8' }} />}
              valueStyle={{ color: anomalyCount > 0 ? '#ef4444' : undefined }}
            />
          </Card>
        </Col>
      </Row>

      {/* ── Main Content: Two columns ── */}
      <Row gutter={[16, 16]} style={{ marginTop: 16 }}>

        {/* Left: Hot News Feed */}
        <Col xs={24} lg={14}>
          <Card
            title={<><FireOutlined style={{ color: '#ff4d4f', marginRight: 6 }} />实时热点</>}
            size="small"
            extra={
              <Select
                allowClear
                placeholder="全部来源"
                value={sourceFilter}
                onChange={setSourceFilter}
                style={{ width: 130 }}
                options={[
                  { value: '华尔街见闻热点', label: '华尔街见闻' },
                  { value: '财联社热点', label: '财联社' },
                  { value: '雪球热榜', label: '雪球' },
                  { value: '微博热搜', label: '微博' },
                ]}
              />
            }
          >
            <List
              size="small"
              dataSource={filteredNews.slice(0, 30)}
              locale={{ emptyText: '暂无热点数据' }}
              renderItem={(item: HotNewsItem, index: number) => (
                <List.Item
                  style={{ padding: '6px 0', cursor: 'pointer' }}
                  onClick={() => window.open(item.url, '_blank')}
                >
                  <div style={{ width: '100%', display: 'flex', alignItems: 'flex-start', gap: 8 }}>
                    <Text
                      strong
                      style={{
                        fontSize: 12, width: 20, textAlign: 'center', flexShrink: 0,
                        color: index < 3 ? '#ff4d4f' : '#94a3b8',
                      }}
                    >
                      {index + 1}
                    </Text>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontSize: 13, lineHeight: 1.4, wordBreak: 'break-all' }}>
                        {item.title}
                      </div>
                      <Space size={4} style={{ marginTop: 2 }} wrap>
                        <Tag
                          color={SOURCE_COLORS[item.source_name] || '#94a3b8'}
                          style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px', margin: 0 }}
                        >
                          {SOURCE_SHORT[item.source_name] || item.source_name}
                        </Tag>
                        {item.sentiment && item.sentiment !== 'neutral' && (
                          <Tag
                            color={item.sentiment.includes('bullish') ? 'green' : item.sentiment.includes('bearish') ? 'red' : undefined}
                            style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px', margin: 0 }}
                          >
                            {item.sentiment.includes('bullish') ? '看多' : item.sentiment.includes('bearish') ? '看空' : '中性'}
                          </Tag>
                        )}
                        {item.impact && item.impact !== 'low' && (
                          <Tag
                            color={item.impact === 'critical' ? 'red' : item.impact === 'high' ? 'orange' : 'blue'}
                            style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px', margin: 0 }}
                          >
                            {item.impact === 'critical' ? '重大' : item.impact === 'high' ? '高' : '中'}
                          </Tag>
                        )}
                        {item.tickers?.slice(0, 2).map((t: string) => (
                          <Tag key={t} style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px', margin: 0 }}>
                            {t}
                          </Tag>
                        ))}
                        <Text type="secondary" style={{ fontSize: 11 }}>
                          {item.fetched_at ? dayjs(item.fetched_at).fromNow() : ''}
                        </Text>
                      </Space>
                    </div>
                  </div>
                </List.Item>
              )}
            />
          </Card>
        </Col>

        {/* Right: Top Tickers + Anomalies */}
        <Col xs={24} lg={10}>
          {/* Top Mentioned Tickers */}
          <Card title={<><StockOutlined style={{ marginRight: 6 }} />热门标的 (24h)</>} size="small">
            {tickers.length === 0 ? (
              <Empty description="暂无数据" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            ) : (
              <Table
                dataSource={tickers.slice(0, 15)}
                size="small"
                pagination={false}
                rowKey="code"
                columns={[
                  {
                    title: '标的', dataIndex: 'name', key: 'name', width: 100, ellipsis: true,
                    render: (name: string, r: TickerMention) => (
                      <Tooltip title={r.code}>
                        <Text strong style={{ fontSize: 12 }}>{name || r.code}</Text>
                      </Tooltip>
                    ),
                  },
                  {
                    title: '提及', dataIndex: 'count', key: 'count', width: 55, align: 'center' as const,
                    sorter: (a: TickerMention, b: TickerMention) => a.count - b.count,
                    defaultSortOrder: 'descend' as const,
                    render: (v: number) => <Text strong>{v}</Text>,
                  },
                  {
                    title: '情绪', key: 'sentiment', width: 120,
                    render: (_: any, r: TickerMention) => {
                      const total = r.bullish + r.bearish + r.neutral
                      if (total === 0) return <Text type="secondary">-</Text>
                      return (
                        <div style={{ display: 'flex', gap: 2, fontSize: 11 }}>
                          {r.bullish > 0 && (
                            <Tag color="green" style={{ margin: 0, fontSize: 10, padding: '0 3px' }}>
                              <RiseOutlined /> {r.bullish}
                            </Tag>
                          )}
                          {r.bearish > 0 && (
                            <Tag color="red" style={{ margin: 0, fontSize: 10, padding: '0 3px' }}>
                              <FallOutlined /> {r.bearish}
                            </Tag>
                          )}
                          {r.neutral > 0 && r.bullish === 0 && r.bearish === 0 && (
                            <Tag style={{ margin: 0, fontSize: 10, padding: '0 3px' }}>
                              {r.neutral}
                            </Tag>
                          )}
                        </div>
                      )
                    },
                  },
                ]}
              />
            )}
          </Card>

          {/* Anomaly Detection */}
          <Card
            title={
              <>
                <AlertOutlined style={{ color: anomalyCount > 0 ? '#ef4444' : '#94a3b8', marginRight: 6 }} />
                话题聚集检测
              </>
            }
            size="small"
            style={{ marginTop: 16 }}
          >
            {!latestCluster ? (
              <Empty description="聚类服务尚未运行" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            ) : latestCluster.anomalies.length === 0 ? (
              <div style={{ padding: '12px 0', textAlign: 'center' }}>
                <Text type="secondary">
                  最近一次分析: {latestCluster.total_items} 条数据，{latestCluster.n_clusters} 个话题簇
                </Text>
                <br />
                <Tag color="green" style={{ marginTop: 8 }}>未检测到异常聚集</Tag>
                <br />
                <Text type="secondary" style={{ fontSize: 11 }}>
                  {dayjs(latestCluster.run_time).fromNow()}更新
                </Text>
              </div>
            ) : (
              <div>
                <div style={{ marginBottom: 8 }}>
                  <Badge status="error" />
                  <Text style={{ fontSize: 12, marginLeft: 4 }}>
                    检测到 {latestCluster.anomalies.length} 个异常聚集
                    <Text type="secondary" style={{ marginLeft: 8, fontSize: 11 }}>
                      ({latestCluster.total_items} 条数据, {dayjs(latestCluster.run_time).fromNow()}更新)
                    </Text>
                  </Text>
                </div>
                {latestCluster.anomalies.map((a, i) => (
                  <Card
                    key={i}
                    size="small"
                    style={{
                      marginBottom: 8,
                      background: '#fef2f2',
                      border: '1px solid #fecaca',
                    }}
                    bodyStyle={{ padding: '8px 12px' }}
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                      <Text strong style={{ color: '#dc2626', fontSize: 12 }}>
                        <ThunderboltOutlined style={{ marginRight: 4 }} />
                        {a.size} 条 ({(a.pct * 100).toFixed(1)}%)
                      </Text>
                      <Space size={2}>
                        {a.tickers?.slice(0, 3).map((t, j) => (
                          <Tag key={j} color="red" style={{ fontSize: 10, margin: 0, padding: '0 4px' }}>
                            {t.name || t.code}
                          </Tag>
                        ))}
                      </Space>
                    </div>
                    <div style={{ fontSize: 12, color: '#374151' }}>
                      {a.representative_titles?.slice(0, 3).map((title, j) => (
                        <div key={j} style={{ padding: '1px 0', lineHeight: 1.4 }}>
                          · {title.length > 50 ? title.substring(0, 50) + '...' : title}
                        </div>
                      ))}
                    </div>
                  </Card>
                ))}
              </div>
            )}
          </Card>

          {/* Source distribution */}
          <Card title="来源分布 (24h)" size="small" style={{ marginTop: 16 }}>
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              {Object.entries(data.hot_news_counts).map(([source, count]) => (
                <Tag
                  key={source}
                  color={SOURCE_COLORS[source]}
                  style={{ fontSize: 12, padding: '2px 8px', cursor: 'pointer' }}
                  onClick={() => setSourceFilter(source === sourceFilter ? null : source)}
                >
                  {SOURCE_SHORT[source] || source}: {count}
                </Tag>
              ))}
            </div>
          </Card>
        </Col>
      </Row>

      {/* ── Top Clusters (if available) ── */}
      {latestCluster && latestCluster.top_clusters.length > 0 && (
        <Card
          title={<><LineChartOutlined style={{ marginRight: 6 }} />话题簇概览</>}
          size="small"
          style={{ marginTop: 16 }}
        >
          <Row gutter={[8, 8]}>
            {latestCluster.top_clusters.slice(0, 8).map((cluster, i) => (
              <Col key={i} xs={24} sm={12} lg={6}>
                <Card
                  size="small"
                  style={{
                    border: cluster.pct > 0.05 ? '1px solid #fed7aa' : '1px solid #e2e8f0',
                    background: cluster.pct > 0.05 ? '#fffbeb' : undefined,
                  }}
                  bodyStyle={{ padding: '8px 10px' }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                    <Text strong style={{ fontSize: 12 }}>
                      #{i + 1}
                    </Text>
                    <Tag
                      color={cluster.pct > 0.1 ? 'red' : cluster.pct > 0.05 ? 'orange' : 'default'}
                      style={{ fontSize: 10, margin: 0 }}
                    >
                      {cluster.size}条 ({(cluster.pct * 100).toFixed(0)}%)
                    </Tag>
                  </div>
                  <div style={{ fontSize: 11, color: '#475569', lineHeight: 1.4 }}>
                    {cluster.representative_titles?.slice(0, 2).map((t, j) => (
                      <div key={j} style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {t}
                      </div>
                    ))}
                  </div>
                  {cluster.tickers?.length > 0 && (
                    <div style={{ marginTop: 4 }}>
                      {cluster.tickers.slice(0, 2).map((t, j) => (
                        <Tag key={j} style={{ fontSize: 10, margin: '0 2px 0 0', padding: '0 3px' }}>
                          {t.name || t.code}
                        </Tag>
                      ))}
                    </div>
                  )}
                </Card>
              </Col>
            ))}
          </Row>
        </Card>
      )}
    </div>
  )
}
