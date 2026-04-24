/**
 * 深度研究 · 每日简报
 *
 * 面向做股票决策的老板：最重要的是"今天该看多什么 / 该看空什么"。
 *
 * 页面顺序：
 *   1. HERO — 巨型 看多 / 看空 信号统计卡
 *   2. 热门标的 + 热门板块（大号 Tag）
 *   3. AI 简报全文 (Markdown + GFM 表格)
 *   4. 日期选择 / 翻页
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Col,
  DatePicker,
  Empty,
  Row,
  Space,
  Spin,
  Tag,
  Typography,
} from 'antd'
import {
  LeftOutlined,
  RightOutlined,
  CalendarOutlined,
  RiseOutlined,
  FallOutlined,
  FireOutlined,
  ReloadOutlined,
  ProfileOutlined,
  AimOutlined,
  SolutionOutlined,
  ArrowUpOutlined,
  ArrowDownOutlined,
} from '@ant-design/icons'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'

dayjs.extend(relativeTime)

const { Title, Text } = Typography

interface DigestData {
  id: number
  digest_date: string
  content_markdown: string
  stats: {
    articles?: number
    roadshows_cn?: number
    comments?: number
    hot_tickers?: { name: string; count: number }[]
    hot_sectors?: { name: string; count: number }[]
    bullish_count?: number
    bearish_count?: number
  }
  generated_at: string | null
  model_used: string
}

export default function AlphaPaiDigest() {
  const [digest, setDigest] = useState<DigestData | null>(null)
  const [digestList, setDigestList] = useState<DigestData[]>([])
  const [currentIndex, setCurrentIndex] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const loadAll = useCallback(() => {
    setLoading(true)
    setError(null)
    Promise.all([
      api.get<DigestData>('/alphapai/digests/latest'),
      api.get('/alphapai/digests', { params: { limit: 30 } }),
    ])
      .then(([latest, list]) => {
        setDigest(latest.data)
        const arr = Array.isArray(list.data) ? list.data : list.data?.items || []
        setDigestList(arr)
        setCurrentIndex(0)
      })
      .catch((err) => {
        setError(err?.response?.data?.detail || err?.message || '无法加载每日简报')
      })
      .finally(() => setLoading(false))
  }, [])

  useEffect(() => {
    loadAll()
  }, [loadAll])

  const navigateDigest = useCallback(
    (direction: 'prev' | 'next') => {
      const newIndex = direction === 'prev' ? currentIndex + 1 : currentIndex - 1
      if (newIndex < 0 || newIndex >= digestList.length) return
      setCurrentIndex(newIndex)
      setDigest(digestList[newIndex])
    },
    [currentIndex, digestList],
  )

  const pickDate = useCallback(
    (d: dayjs.Dayjs | null) => {
      if (!d) return
      const key = d.format('YYYY-MM-DD')
      const idx = digestList.findIndex(
        (x) => dayjs(x.digest_date).format('YYYY-MM-DD') === key,
      )
      if (idx >= 0) {
        setCurrentIndex(idx)
        setDigest(digestList[idx])
      }
    },
    [digestList],
  )

  const availableDates = useMemo(
    () => new Set(digestList.map((d) => dayjs(d.digest_date).format('YYYY-MM-DD'))),
    [digestList],
  )

  const digestStats = digest?.stats || {}
  const hotTickers = digestStats.hot_tickers || []
  const hotSectors = digestStats.hot_sectors || []
  const bullishCount = digestStats.bullish_count ?? null
  const bearishCount = digestStats.bearish_count ?? null

  const digestDate = digest ? dayjs(digest.digest_date).format('YYYY-MM-DD') : null
  const digestAgeDays = digest
    ? Math.max(0, dayjs().startOf('day').diff(dayjs(digest.digest_date), 'day'))
    : 0
  const generatedAt = digest?.generated_at
    ? dayjs(digest.generated_at).format('HH:mm')
    : null

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
            <ProfileOutlined /> 每日简报 · 操盘决策板
          </Title>
          <Text type="secondary">
            今日看多 / 看空信号 + 热门标的 + 关键纪要
          </Text>
        </div>
        <Space>
          <DatePicker
            size="small"
            onChange={pickDate}
            value={digestDate ? dayjs(digestDate) : null}
            disabledDate={(d) => !availableDates.has(d.format('YYYY-MM-DD'))}
            allowClear={false}
            style={{ width: 140 }}
          />
          <Button
            icon={<LeftOutlined />}
            size="small"
            disabled={currentIndex >= digestList.length - 1}
            onClick={() => navigateDigest('prev')}
          >
            更早
          </Button>
          <Button
            icon={<RightOutlined />}
            size="small"
            disabled={currentIndex <= 0}
            onClick={() => navigateDigest('next')}
          >
            更新
          </Button>
          <a onClick={loadAll} style={{ fontSize: 13 }}>
            <ReloadOutlined /> 刷新
          </a>
        </Space>
      </div>

      {/* Digest header — date + fresh badge */}
      <div style={{ marginBottom: 12 }}>
        <Space wrap>
          <Tag
            icon={<CalendarOutlined />}
            color={digestAgeDays === 0 ? 'green' : digestAgeDays < 3 ? 'blue' : 'default'}
            style={{ fontSize: 13, padding: '2px 10px' }}
          >
            {digestDate || '—'}
            {digestAgeDays > 0 && ` · ${digestAgeDays} 天前`}
          </Tag>
          {generatedAt && (
            <Text type="secondary" style={{ fontSize: 12 }}>
              AI 生成于 {generatedAt}
              {digest?.model_used && ` · ${digest.model_used}`}
            </Text>
          )}
        </Space>
      </div>

      {error && (
        <Alert
          type="warning"
          showIcon
          message="无法加载每日简报"
          description={error}
          style={{ marginBottom: 16 }}
        />
      )}

      <Spin spinning={loading}>
        {/* --- HERO: 看多 / 看空 大卡 --- */}
        <Row gutter={16} style={{ marginBottom: 16 }}>
          <Col xs={24} md={12}>
            <Card
              size="small"
              bodyStyle={{ padding: 20 }}
              style={{
                background:
                  'linear-gradient(135deg, #ecfdf5 0%, #d1fae5 100%)',
                border: '1px solid #10b981',
              }}
            >
              <Space align="center" size={18}>
                <div
                  style={{
                    width: 52,
                    height: 52,
                    borderRadius: 26,
                    background: '#10b981',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                  }}
                >
                  <RiseOutlined style={{ color: '#fff', fontSize: 26 }} />
                </div>
                <div>
                  <Text style={{ fontSize: 13, color: '#047857', fontWeight: 600 }}>
                    看多信号
                  </Text>
                  <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
                    <span
                      style={{
                        fontSize: 44,
                        fontWeight: 700,
                        color: '#059669',
                        lineHeight: 1,
                      }}
                    >
                      {bullishCount ?? '—'}
                    </span>
                    <Text style={{ fontSize: 14, color: '#047857' }}>条</Text>
                    <ArrowUpOutlined style={{ color: '#059669', fontSize: 18 }} />
                  </div>
                  <Text style={{ fontSize: 12, color: '#047857' }}>
                    今日券商/公众号释放的多头观点
                  </Text>
                </div>
              </Space>
            </Card>
          </Col>
          <Col xs={24} md={12}>
            <Card
              size="small"
              bodyStyle={{ padding: 20 }}
              style={{
                background:
                  'linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%)',
                border: '1px solid #ef4444',
              }}
            >
              <Space align="center" size={18}>
                <div
                  style={{
                    width: 52,
                    height: 52,
                    borderRadius: 26,
                    background: '#ef4444',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                  }}
                >
                  <FallOutlined style={{ color: '#fff', fontSize: 26 }} />
                </div>
                <div>
                  <Text style={{ fontSize: 13, color: '#b91c1c', fontWeight: 600 }}>
                    看空信号 / 风险提示
                  </Text>
                  <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
                    <span
                      style={{
                        fontSize: 44,
                        fontWeight: 700,
                        color: '#dc2626',
                        lineHeight: 1,
                      }}
                    >
                      {bearishCount ?? '—'}
                    </span>
                    <Text style={{ fontSize: 14, color: '#b91c1c' }}>条</Text>
                    <ArrowDownOutlined style={{ color: '#dc2626', fontSize: 18 }} />
                  </div>
                  <Text style={{ fontSize: 12, color: '#b91c1c' }}>
                    今日业绩暴雷 / 行业利空 / 风险事件
                  </Text>
                </div>
              </Space>
            </Card>
          </Col>
        </Row>

        {/* --- 热门标的 + 热门板块 --- */}
        {(hotTickers.length > 0 || hotSectors.length > 0) && (
          <Row gutter={16} style={{ marginBottom: 16 }}>
            {hotTickers.length > 0 && (
              <Col xs={24} md={12}>
                <Card
                  size="small"
                  title={
                    <Space>
                      <AimOutlined style={{ color: '#ef4444' }} />
                      <span>今日热门标的</span>
                      <Text type="secondary" style={{ fontSize: 12, fontWeight: 400 }}>
                        (按关注度排序)
                      </Text>
                    </Space>
                  }
                >
                  <Space size={[8, 10]} wrap>
                    {hotTickers.slice(0, 14).map((tk, i) => (
                      <Tag
                        key={`${tk.name}-${i}`}
                        color={i < 3 ? 'red' : i < 6 ? 'volcano' : 'orange'}
                        style={{
                          fontSize: 13,
                          padding: '4px 10px',
                          borderRadius: 4,
                          margin: 0,
                        }}
                      >
                        {i < 3 && <FireOutlined style={{ marginRight: 4 }} />}
                        <b>{tk.name}</b>
                        <span style={{ opacity: 0.75, marginLeft: 6 }}>× {tk.count}</span>
                      </Tag>
                    ))}
                  </Space>
                </Card>
              </Col>
            )}
            {hotSectors.length > 0 && (
              <Col xs={24} md={12}>
                <Card
                  size="small"
                  title={
                    <Space>
                      <SolutionOutlined style={{ color: '#f59e0b' }} />
                      <span>今日热门板块</span>
                    </Space>
                  }
                >
                  <Space size={[8, 10]} wrap>
                    {hotSectors.slice(0, 12).map((s, i) => (
                      <Tag
                        key={`${s.name}-${i}`}
                        color={i < 3 ? 'gold' : 'default'}
                        style={{
                          fontSize: 13,
                          padding: '4px 10px',
                          borderRadius: 4,
                          margin: 0,
                        }}
                      >
                        <b>{s.name}</b>
                        <span style={{ opacity: 0.75, marginLeft: 6 }}>× {s.count}</span>
                      </Tag>
                    ))}
                  </Space>
                </Card>
              </Col>
            )}
          </Row>
        )}

        {/* --- Markdown 正文 --- */}
        <Card
          title={
            <Space>
              <ProfileOutlined />
              <span>AI 研究简报全文</span>
            </Space>
          }
        >
          {digest && digest.content_markdown ? (
            <div className="alphapai-digest-md">
              <ReactMarkdown remarkPlugins={[remarkGfm]}>
                {digest.content_markdown}
              </ReactMarkdown>
            </div>
          ) : (
            <Empty
              description={
                <span>
                  暂无简报
                  <br />
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    简报由后台定时任务基于当日数据自动生成
                  </Text>
                </span>
              }
            />
          )}
        </Card>
      </Spin>

      {/* --- Markdown styles --- */}
      <style>{`
        .alphapai-digest-md {
          font-size: 13.5px;
          line-height: 1.85;
          color: #1e293b;
        }
        .alphapai-digest-md h1 {
          font-size: 19px;
          margin: 18px 0 10px;
          color: #0f172a;
          border-bottom: 2px solid #e2e8f0;
          padding-bottom: 6px;
        }
        .alphapai-digest-md h2 {
          font-size: 16px;
          margin: 22px 0 10px;
          color: #1e293b;
          border-bottom: 1px solid #e2e8f0;
          padding-bottom: 4px;
        }
        /* 看多信号 / 看空信号 标题用色块强化 */
        .alphapai-digest-md h2:has(+ ul) {
          padding: 6px 10px;
          border-radius: 3px;
          border-bottom: none;
        }
        .alphapai-digest-md h3 {
          font-size: 14.5px;
          margin: 14px 0 6px;
          color: #1e293b;
        }
        .alphapai-digest-md h4 {
          font-size: 13.5px;
          margin: 10px 0 4px;
          color: #334155;
        }
        .alphapai-digest-md p {
          margin: 6px 0;
        }
        .alphapai-digest-md ul,
        .alphapai-digest-md ol {
          margin: 6px 0 10px;
          padding-left: 22px;
        }
        .alphapai-digest-md li {
          margin-bottom: 4px;
        }
        .alphapai-digest-md strong {
          color: #0f172a;
        }
        .alphapai-digest-md table {
          border-collapse: collapse;
          margin: 12px 0;
          font-size: 12.5px;
          width: 100%;
        }
        .alphapai-digest-md th,
        .alphapai-digest-md td {
          border: 1px solid #e2e8f0;
          padding: 7px 11px;
          text-align: left;
          vertical-align: top;
        }
        .alphapai-digest-md th {
          background: #f8fafc;
          font-weight: 600;
        }
        .alphapai-digest-md tr:nth-child(even) td {
          background: #fafbfc;
        }
        /* 看多 / 看空 关键词文字着色 */
        .alphapai-digest-md td strong:first-child {
          color: #0f172a;
        }
        .alphapai-digest-md code {
          background: #f1f5f9;
          padding: 1px 5px;
          border-radius: 3px;
          font-size: 12px;
        }
        .alphapai-digest-md blockquote {
          border-left: 3px solid #cbd5e1;
          padding: 4px 12px;
          margin: 10px 0;
          color: #475569;
          background: #f8fafc;
        }
        .alphapai-digest-md a {
          color: #2563eb;
          text-decoration: none;
        }
        .alphapai-digest-md a:hover {
          text-decoration: underline;
        }
      `}</style>
    </div>
  )
}
