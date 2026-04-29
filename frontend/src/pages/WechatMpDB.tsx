/**
 * 微信公众号 (mp.weixin.qq.com) DB 浏览页.
 *
 * 数据源: backend `/api/wechat-mp-db` → MongoDB `wechat-mp.articles`.
 * 走 MP 后台直采路径,白名单起步只放机器之心.
 *
 * UI:
 *   - 顶部统计卡 (总篇数 / 公众号数 / 最新发布 / 按公众号分布)
 *   - 公众号 + 关键词 + 股票代码 三联过滤 + 按时间排序
 *   - 卡片列表 (cover + 标题 + digest + 公众号 + 时间 + ticker tags)
 *   - 详情抽屉: 完整 markdown + 图片 gallery (走镜像 API, 不暴露 mmbiz.qpic.cn 防盗链)
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Card,
  Drawer,
  Empty,
  Image,
  Input,
  List,
  Pagination,
  Select,
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
  WechatOutlined,
} from '@ant-design/icons'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import api from '../services/api'

dayjs.extend(relativeTime)

const { Text, Title, Paragraph } = Typography

interface ArticleBrief {
  id: string
  url: string | null
  biz: string | null
  appmsgid: number | null
  itemidx: number | null
  sn: string | null
  account_name: string | null
  title: string
  author: string
  digest: string
  cover: string
  release_time: string | null
  release_time_ms: number | null
  content_length: number
  image_count: number
  fetch_error: string | null
  _canonical_tickers: string[]
}

interface ArticleDetail extends ArticleBrief {
  content_md: string
  images: { src: string; local_path: string | null; size_bytes: number | null;
            download_error: string | null }[]
  html_raw?: string
}

interface ListResponse {
  total: number
  items: ArticleBrief[]
  skip: number
  limit: number
}

interface StatsResponse {
  total_articles: number
  total_accounts: number
  latest: { title?: string; release_time?: string; account_name?: string } | null
  release_time_range_ms: { max_ms?: number; min_ms?: number } | null
  by_account: { account_name: string; count: number }[]
}

const PAGE_SIZE = 20

export default function WechatMpDB() {
  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [statsLoading, setStatsLoading] = useState(true)
  const [statsError, setStatsError] = useState<string | null>(null)

  const [items, setItems] = useState<ArticleBrief[]>([])
  const [itemsLoading, setItemsLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [total, setTotal] = useState(0)

  const [accountFilter, setAccountFilter] = useState<string | undefined>(undefined)
  const [query, setQuery] = useState('')
  const [tickerFilter, setTickerFilter] = useState('')

  const [detailOpen, setDetailOpen] = useState(false)
  const [detail, setDetail] = useState<ArticleDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)

  const loadStats = useCallback(async () => {
    setStatsLoading(true)
    setStatsError(null)
    try {
      const res = await api.get<StatsResponse>('/wechat-mp-db/stats')
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
      const res = await api.get<ListResponse>('/wechat-mp-db/articles', {
        params: {
          account: accountFilter || undefined,
          q: query || undefined,
          ticker: tickerFilter || undefined,
          limit: PAGE_SIZE,
          skip: (page - 1) * PAGE_SIZE,
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
  }, [accountFilter, query, tickerFilter, page])

  useEffect(() => {
    loadStats()
  }, [loadStats])

  useEffect(() => {
    loadItems()
  }, [loadItems])

  const openDetail = useCallback(async (item: ArticleBrief) => {
    setDetailOpen(true)
    setDetailLoading(true)
    setDetail(null)
    try {
      const res = await api.get<ArticleDetail>(
        `/wechat-mp-db/articles/${encodeURIComponent(item.id)}`,
      )
      setDetail(res.data)
    } catch {
      setDetail(null)
    } finally {
      setDetailLoading(false)
    }
  }, [])

  // 镜像 API 走 axios (带 Authorization),返回 blob → 转 objectURL
  // 这样图片不会请求 mmbiz.qpic.cn (防盗链 + 平台风控),也不会绕过 JWT
  const [imageBlobUrls, setImageBlobUrls] = useState<Record<number, string>>({})
  useEffect(() => {
    if (!detail?.id || !(detail.images?.length)) {
      setImageBlobUrls({})
      return
    }
    let cancelled = false
    const created: string[] = []
    Promise.all(
      detail.images.map(async (info, idx) => {
        if (!info.local_path) return [idx, null] as const
        try {
          const res = await api.get(
            `/wechat-mp-db/articles/${encodeURIComponent(detail.id)}/image/${idx}`,
            { responseType: 'blob' },
          )
          const url = URL.createObjectURL(new Blob([res.data]))
          created.push(url)
          return [idx, url] as const
        } catch {
          return [idx, null] as const
        }
      }),
    ).then((results) => {
      if (cancelled) {
        created.forEach((u) => URL.revokeObjectURL(u))
        return
      }
      const map: Record<number, string> = {}
      results.forEach(([idx, url]) => {
        if (url) map[idx as number] = url as string
      })
      setImageBlobUrls(map)
    })
    return () => {
      cancelled = true
      created.forEach((u) => URL.revokeObjectURL(u))
    }
  }, [detail?.id, detail?.images])

  const accountOptions = useMemo(() => {
    return (stats?.by_account || []).map((b) => ({
      label: `${b.account_name} (${b.count})`,
      value: b.account_name,
    }))
  }, [stats])

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
            <WechatOutlined style={{ color: '#07c160' }} /> 微信公众号
          </Title>
          <Text type="secondary">
            mp.weixin.qq.com · MP 后台直采 · 白名单 + 图片本地化镜像
          </Text>
        </div>
        <a onClick={() => { loadStats(); loadItems() }} style={{ fontSize: 13 }}>
          <ReloadOutlined /> 刷新
        </a>
      </div>

      {statsError && (
        <Alert
          type="warning"
          showIcon
          message="无法加载微信公众号数据"
          description={statsError}
          style={{ marginBottom: 16 }}
        />
      )}

      <Spin spinning={statsLoading}>
        <Card size="small" bodyStyle={{ padding: 14 }} style={{ marginBottom: 16 }}>
          <Space size={32} wrap>
            <Statistic
              title="累计入库"
              value={stats?.total_articles ?? 0}
              valueStyle={{ color: '#07c160', fontSize: 26 }}
            />
            <Statistic
              title="已跟踪公众号"
              value={stats?.total_accounts ?? 0}
              valueStyle={{ fontSize: 22 }}
            />
            {stats?.latest && (
              <div>
                <Text type="secondary" style={{ fontSize: 12 }}>最新发布</Text>
                <div style={{ fontSize: 14, marginTop: 4, maxWidth: 480 }}>
                  <Tag color="green">{stats.latest.account_name}</Tag>
                  <Text ellipsis>{stats.latest.title}</Text>
                  <Text type="secondary" style={{ fontSize: 11, marginLeft: 6 }}>
                    {stats.latest.release_time}
                  </Text>
                </div>
              </div>
            )}
          </Space>
          {stats?.by_account?.length ? (
            <div style={{ marginTop: 10 }}>
              <Text type="secondary" style={{ fontSize: 12, marginRight: 8 }}>
                按公众号分布:
              </Text>
              {stats.by_account.map((b) => (
                <Tag
                  key={b.account_name}
                  style={{ cursor: 'pointer', marginBottom: 4 }}
                  color={b.account_name === accountFilter ? 'green' : 'default'}
                  onClick={() => {
                    setAccountFilter(b.account_name === accountFilter ? undefined : b.account_name)
                    setPage(1)
                  }}
                >
                  {b.account_name} · {b.count}
                </Tag>
              ))}
            </div>
          ) : null}
        </Card>
      </Spin>

      <Card size="small" bodyStyle={{ padding: 12 }} style={{ marginBottom: 14 }}>
        <Space wrap>
          <Select
            placeholder="筛选公众号"
            allowClear
            options={accountOptions}
            value={accountFilter}
            onChange={(v) => {
              setAccountFilter(v)
              setPage(1)
            }}
            style={{ minWidth: 180 }}
          />
          <Input.Search
            placeholder="标题关键词 (regex)"
            allowClear
            style={{ width: 280 }}
            onSearch={(v) => {
              setQuery(v)
              setPage(1)
            }}
          />
          <Input
            placeholder="股票代码 (NVDA.US, 0700.HK …)"
            allowClear
            style={{ width: 200 }}
            value={tickerFilter}
            onChange={(e) => setTickerFilter(e.target.value)}
            onPressEnter={(e) => {
              setTickerFilter((e.target as HTMLInputElement).value)
              setPage(1)
            }}
          />
        </Space>
      </Card>

      <Spin spinning={itemsLoading}>
        {!items.length && !itemsLoading ? (
          <Empty description="还没有抓到的文章" />
        ) : (
          <>
            <List
              dataSource={items}
              renderItem={(item) => (
                <List.Item
                  key={item.id}
                  style={{
                    cursor: 'pointer',
                    padding: '14px 8px',
                    borderRadius: 6,
                  }}
                  onClick={() => openDetail(item)}
                >
                  <Space align="start" style={{ width: '100%' }}>
                    {item.cover ? (
                      <img
                        src={`/api/wechat-mp-db/cover?url=${encodeURIComponent(item.cover)}`}
                        alt=""
                        loading="lazy"
                        decoding="async"
                        style={{
                          width: 96,
                          height: 96,
                          objectFit: 'cover',
                          borderRadius: 4,
                          flexShrink: 0,
                          background: '#f5f5f5',
                        }}
                        onError={(e) => {
                          // 后端代理失败时回落到原 URL (referrerPolicy 兜底)
                          const img = e.currentTarget
                          if (img.dataset.fallback !== '1') {
                            img.dataset.fallback = '1'
                            img.referrerPolicy = 'no-referrer'
                            img.src = item.cover
                          }
                        }}
                      />
                    ) : (
                      <div
                        style={{
                          width: 96,
                          height: 96,
                          background: '#f5f5f5',
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          color: '#bbb',
                        }}
                      >
                        <FileTextOutlined />
                      </div>
                    )}
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div>
                        <Tag color="green" icon={<WechatOutlined />}>
                          {item.account_name}
                        </Tag>
                        {item.author ? (
                          <Tag color="blue">{item.author}</Tag>
                        ) : null}
                        <Text type="secondary" style={{ fontSize: 12 }}>
                          {item.release_time}
                          {item.release_time_ms ? (
                            <> · {dayjs(item.release_time_ms).fromNow()}</>
                          ) : null}
                          {' · '}{item.content_length} 字
                          {item.image_count > 0 && <> · {item.image_count} 图</>}
                          {item.fetch_error && (
                            <Tag color="red" style={{ marginLeft: 6, fontSize: 10 }}>
                              抓取失败
                            </Tag>
                          )}
                        </Text>
                      </div>
                      <Title level={5} style={{ margin: '6px 0', wordBreak: 'break-word' }}>
                        {item.title}
                      </Title>
                      {item.digest ? (
                        <Paragraph
                          ellipsis={{ rows: 2 }}
                          style={{ color: '#666', fontSize: 13, margin: 0 }}
                        >
                          {item.digest}
                        </Paragraph>
                      ) : null}
                      {item._canonical_tickers?.length ? (
                        <div style={{ marginTop: 6 }}>
                          {item._canonical_tickers.map((t) => (
                            <Tag key={t} color="orange" style={{ fontSize: 11 }}>
                              {t}
                            </Tag>
                          ))}
                        </div>
                      ) : null}
                    </div>
                  </Space>
                </List.Item>
              )}
            />
            <div style={{ textAlign: 'right', marginTop: 16 }}>
              <Pagination
                current={page}
                pageSize={PAGE_SIZE}
                total={total}
                onChange={(p) => setPage(p)}
                showSizeChanger={false}
                showTotal={(t) => `共 ${t} 篇`}
              />
            </div>
          </>
        )}
      </Spin>

      <Drawer
        open={detailOpen}
        onClose={() => setDetailOpen(false)}
        width={Math.min(960, typeof window !== 'undefined' ? window.innerWidth - 80 : 960)}
        title={
          detail ? (
            <Space>
              <Tag color="green" icon={<WechatOutlined />}>
                {detail.account_name}
              </Tag>
              <Text strong style={{ fontSize: 14 }}>{detail.title}</Text>
            </Space>
          ) : (
            '加载中…'
          )
        }
      >
        <Spin spinning={detailLoading}>
          {detail ? (
            <div>
              <Space wrap style={{ marginBottom: 16 }}>
                {detail.author ? <Tag color="blue">{detail.author}</Tag> : null}
                <Text type="secondary">{detail.release_time}</Text>
                {detail.url ? (
                  <a href={detail.url} target="_blank" rel="noreferrer">
                    <LinkOutlined /> 原文
                  </a>
                ) : null}
                {detail._canonical_tickers?.map((t) => (
                  <Tag key={t} color="orange">{t}</Tag>
                ))}
              </Space>

              {detail.digest ? (
                <Alert
                  type="info"
                  showIcon={false}
                  message={detail.digest}
                  style={{ marginBottom: 16, background: '#f6ffed' }}
                />
              ) : null}

              <article className="wechat-md">
                <ReactMarkdown
                  remarkPlugins={[remarkGfm]}
                  components={{
                    img: ({ src, alt }) => {
                      // markdown 中的相对路径已被 scraper 改写为
                      // wechat_mp_images/.../N.png — 但更稳的渲染走 imageBlobUrls
                      // (按 detail.images 顺序映射 idx). 这里 fallback 直接用 src,
                      // 给 referrerPolicy=no-referrer 让 mmbiz.qpic.cn 占位图也能加载。
                      return (
                        <img
                          src={src}
                          alt={alt}
                          referrerPolicy="no-referrer"
                          style={{ maxWidth: '100%', height: 'auto', borderRadius: 4 }}
                        />
                      )
                    },
                  }}
                >
                  {detail.content_md || '*正文为空*'}
                </ReactMarkdown>
              </article>

              {detail.images?.length ? (
                <div style={{ marginTop: 24 }}>
                  <Title level={5}>原文图片 ({detail.images.length})</Title>
                  <Image.PreviewGroup>
                    <Space wrap size={8}>
                      {detail.images.map((info, idx) => {
                        const blobUrl = imageBlobUrls[idx]
                        const fallback = info.src
                        return (
                          <Image
                            key={idx}
                            src={blobUrl || fallback}
                            referrerPolicy="no-referrer"
                            width={120}
                            height={120}
                            style={{ objectFit: 'cover' }}
                            placeholder
                            alt={info.local_path || info.src}
                          />
                        )
                      })}
                    </Space>
                  </Image.PreviewGroup>
                </div>
              ) : null}
            </div>
          ) : null}
        </Spin>
      </Drawer>
    </div>
  )
}
