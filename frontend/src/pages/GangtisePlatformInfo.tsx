import { useEffect, useState, useCallback, useMemo, useRef } from 'react'
import { Link } from 'react-router-dom'
import {
  Card, Row, Col, Table, Tag, Typography, Space, Spin, Alert, Button, Tooltip,
  Segmented, Empty, Badge, Divider, Image, Switch,
} from 'antd'
import {
  FireOutlined, BulbOutlined, BarChartOutlined, FileTextOutlined, RiseOutlined,
  FallOutlined, AppstoreOutlined, ReloadOutlined, ThunderboltOutlined,
  GlobalOutlined, CalendarOutlined, ClockCircleOutlined, DatabaseOutlined,
  PictureOutlined, CrownOutlined,
  DownOutlined, UpOutlined, PlayCircleOutlined, PauseCircleOutlined,
} from '@ant-design/icons'
import api from '../services/api'

const { Title, Paragraph, Text } = Typography

// -------------------------------------------------------------------------
// Types + helpers
// -------------------------------------------------------------------------

interface ModuleDoc {
  key: string
  label: string
  ok: boolean
  item_count: number
  items: any[]
  status_msg?: string | null
  method?: string
  path?: string
  fetched_at?: string | null
  latency_ms?: number | null
  age_seconds?: number | null
}

interface Snapshot {
  platform: string
  platform_label: string
  modules: ModuleDoc[]
  module_count: number
  ok_count: number
  total_items: number
  oldest_fetched_at?: string | null
  newest_fetched_at?: string | null
}

function formatAge(sec: number | null | undefined): string {
  if (sec == null) return '-'
  if (sec < 60) return `${sec}s`
  if (sec < 3600) return `${Math.floor(sec / 60)}m`
  if (sec < 86400) return `${Math.floor(sec / 3600)}h`
  return `${Math.floor(sec / 86400)}d`
}

function formatNum(n?: number | null, digits = 2): string {
  if (n == null || Number.isNaN(n)) return '-'
  return Number(n).toFixed(digits)
}

function formatAmount(n?: number | null): string {
  if (n == null || Number.isNaN(n)) return '-'
  const abs = Math.abs(n)
  if (abs >= 1e12) return (n / 1e12).toFixed(2) + '万亿'
  if (abs >= 1e8) return (n / 1e8).toFixed(2) + '亿'
  if (abs >= 1e4) return (n / 1e4).toFixed(2) + '万'
  return n.toFixed(0)
}

function formatCnfrTime(ts?: number | null): string {
  if (!ts) return '-'
  const d = new Date(ts)
  return `${d.getMonth() + 1}/${d.getDate()} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`
}

function pick(snap: Snapshot | null, key: string): ModuleDoc | undefined {
  return snap?.modules.find((m) => m.key === key)
}

// 涨跌颜色
const upColor = '#ef4444'
const downColor = '#10b981'
const neutralColor = '#94a3b8'
function changeColor(v: number | null | undefined): string {
  if (v == null) return neutralColor
  if (v > 0) return upColor
  if (v < 0) return downColor
  return neutralColor
}

// -------------------------------------------------------------------------
// Mini sparkline (inline SVG, no deps)
// -------------------------------------------------------------------------
function Sparkline({ data, width = 80, height = 24, strokeColor }: {
  data: number[]
  width?: number
  height?: number
  strokeColor?: string
}) {
  if (!data || data.length < 2) {
    return <span style={{ color: neutralColor, fontSize: 11 }}>-</span>
  }
  const min = Math.min(...data)
  const max = Math.max(...data)
  const range = max - min || 1
  const last = data[data.length - 1]
  const first = data[0]
  const color = strokeColor || (last >= first ? upColor : downColor)
  const stepX = width / (data.length - 1)
  const pts = data.map((v, i) => {
    const x = i * stepX
    const y = height - ((v - min) / range) * (height - 4) - 2
    return `${x.toFixed(1)},${y.toFixed(1)}`
  }).join(' ')
  return (
    <svg width={width} height={height} style={{ display: 'block' }}>
      <polyline fill="none" stroke={color} strokeWidth={1.5} points={pts} />
      <circle cx={(data.length - 1) * stepX}
        cy={height - ((last - min) / range) * (height - 4) - 2}
        r={2} fill={color} />
    </svg>
  )
}

// -------------------------------------------------------------------------
// Shared ModuleCard shell
// -------------------------------------------------------------------------
function ModuleCard({ mod, children, icon, accentColor = '#1890ff', extra }: {
  mod: ModuleDoc | undefined
  children: React.ReactNode
  icon?: React.ReactNode
  accentColor?: string
  extra?: React.ReactNode
}) {
  if (!mod) return null
  return (
    <Card
      size="small"
      style={{
        height: '100%',
        borderLeft: `3px solid ${accentColor}`,
        boxShadow: '0 1px 3px rgba(0,0,0,0.05)',
      }}
      bodyStyle={{ padding: 12 }}
      title={
        <Space size={8}>
          <span style={{ color: accentColor }}>{icon}</span>
          <Text strong style={{ fontSize: 14 }}>{mod.label}</Text>
          <Badge count={mod.item_count} style={{ backgroundColor: mod.ok ? accentColor : '#8c8c8c' }} />
        </Space>
      }
      extra={
        <Space size={4}>
          {extra}
          <Tooltip title={`拉取于 ${mod.fetched_at || '-'} · ${mod.latency_ms}ms`}>
            <Text type="secondary" style={{ fontSize: 11 }}>
              <ClockCircleOutlined /> {formatAge(mod.age_seconds)}
            </Text>
          </Tooltip>
        </Space>
      }
    >
      {mod.ok ? children : (
        <Alert type="warning" showIcon
          message={mod.status_msg || '未获取到数据'}
          description={<Text code style={{ fontSize: 11 }}>{mod.method} {mod.path}</Text>}
        />
      )}
    </Card>
  )
}

// -------------------------------------------------------------------------
// MarketIndex — horizontal ticker strip at the top
// -------------------------------------------------------------------------
function MarketTicker({ mod }: { mod?: ModuleDoc }) {
  const items = (mod?.items || [])
  const rows = items.map((it: any) => {
    const s = it.snap || []
    return {
      code: it.code,
      name: s[11],
      last: s[12],
      change: s[13],
      chgPct: s[14],
      prev: s[10],
      amount: s[15],
      up: s[16],
      zero: s[17],
      down: s[18],
      time: s[8],
    }
  })
  if (!mod?.ok || rows.length === 0) return null
  return (
    <Card bodyStyle={{ padding: '12px 16px' }}
      style={{
        background: 'linear-gradient(90deg, #1f2937 0%, #111827 100%)',
        border: 'none',
      }}>
      <Space size={0} style={{ width: '100%', overflowX: 'auto' }} wrap={false}>
        {rows.map((r) => {
          const color = changeColor(r.chgPct)
          return (
            <div key={r.code} style={{
              minWidth: 160,
              padding: '0 16px',
              borderRight: '1px solid #374151',
            }}>
              <div style={{ color: '#e5e7eb', fontSize: 13, fontWeight: 500 }}>{r.name}</div>
              <Space size={8} align="baseline">
                <span style={{ color, fontSize: 16, fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>
                  {formatNum(r.last, 2)}
                </span>
                <span style={{ color, fontSize: 12 }}>
                  {r.chgPct != null ? (r.chgPct > 0 ? '+' : '') + formatNum(r.chgPct, 2) + '%' : '-'}
                </span>
              </Space>
              {r.up != null && (
                <div style={{ fontSize: 10, color: '#9ca3af', marginTop: 2 }}>
                  <span style={{ color: upColor }}>↑{r.up}</span>
                  {' '}
                  <span style={{ color: neutralColor }}>-{r.zero}</span>
                  {' '}
                  <span style={{ color: downColor }}>↓{r.down}</span>
                  {r.amount && r.amount > 0 && (
                    <span style={{ marginLeft: 6 }}>{formatAmount(r.amount)}</span>
                  )}
                </div>
              )}
            </div>
          )
        })}
      </Space>
    </Card>
  )
}

// -------------------------------------------------------------------------
// HotStocks — 机构热议个股
// -------------------------------------------------------------------------
function HotStocks({ mod }: { mod?: ModuleDoc }) {
  const items = (mod?.items || []).slice(0, 20)
  const maxHot = items.length > 0 ? Math.max(...items.map((i: any) => i.hotTimes || 0)) : 1
  return (
    <ModuleCard mod={mod} accentColor="#ef4444"
      icon={<FireOutlined />}>
      <div style={{ maxHeight: 440, overflowY: 'auto' }}>
        {items.map((it: any, i: number) => {
          const ratio = (it.hotTimes / maxHot)
          return (
            <div key={it.rltInfo || i}
              style={{
                padding: '6px 4px',
                borderBottom: i < items.length - 1 ? '1px solid #f3f4f6' : 'none',
                position: 'relative',
              }}>
              <div style={{
                position: 'absolute', left: 0, top: 0, bottom: 0,
                width: `${ratio * 100}%`,
                background: 'linear-gradient(90deg, rgba(239,68,68,0.08) 0%, rgba(239,68,68,0.02) 100%)',
                borderRadius: 3,
                pointerEvents: 'none',
              }} />
              <Row align="middle" style={{ position: 'relative' }}>
                <Col flex="24px">
                  <Text strong style={{
                    color: i < 3 ? '#ef4444' : i < 10 ? '#f59e0b' : '#94a3b8',
                    fontSize: 14,
                    fontVariantNumeric: 'tabular-nums',
                  }}>{i + 1}</Text>
                </Col>
                <Col flex="auto">
                  <Space size={6}>
                    <Text strong style={{ fontSize: 13 }}>{it.rltInfoStmt}</Text>
                    <Text code style={{ fontSize: 11, color: '#6b7280' }}>{it.gtsCode}</Text>
                  </Space>
                </Col>
                <Col flex="none">
                  <Tag color={it.hotTimes >= 10 ? 'red' : it.hotTimes >= 5 ? 'orange' : 'default'}
                    style={{ margin: 0, fontWeight: 600 }}>
                    {it.hotTimes} 次
                  </Tag>
                </Col>
              </Row>
            </div>
          )
        })}
        {items.length === 0 && <Empty description="暂无数据" />}
      </div>
    </ModuleCard>
  )
}

// -------------------------------------------------------------------------
// HotConcepts — A 股热门题材 with 7-day sparkline
// -------------------------------------------------------------------------
function HotConcepts({ mod }: { mod?: ModuleDoc }) {
  const items = (mod?.items || []).slice(0, 15)
  return (
    <ModuleCard mod={mod} accentColor="#f59e0b"
      icon={<ThunderboltOutlined />}>
      <Table
        size="small" pagination={false} rowKey="cncptId"
        dataSource={items}
        showHeader={false}
        scroll={{ y: 420 }}
        columns={[
          {
            title: '', dataIndex: 'cncptName', key: 'n',
            render: (v, r: any, i) => (
              <Space size={6}>
                <Text style={{
                  color: i < 3 ? '#f59e0b' : '#94a3b8',
                  fontSize: 12, width: 18, display: 'inline-block',
                  textAlign: 'right', fontVariantNumeric: 'tabular-nums',
                }}>{i + 1}</Text>
                <Text strong style={{ fontSize: 13 }}>{v}</Text>
              </Space>
            ),
          },
          {
            title: '', dataIndex: 'heats', key: 'h', width: 90,
            render: (heats: number[]) => <Sparkline data={heats || []} width={80} height={22} />,
          },
          {
            title: '', dataIndex: 'heat', key: 't', width: 55, align: 'right',
            render: (v: number) => (
              <Tooltip title="当前热度">
                <Text style={{ fontSize: 12, fontVariantNumeric: 'tabular-nums' }}>
                  {formatNum(v, 1)}
                </Text>
              </Tooltip>
            ),
          },
          {
            title: '', dataIndex: 'chgPct', key: 'c', width: 70, align: 'right',
            render: (v: number) => {
              if (v == null) return null
              const pct = (v * 100).toFixed(2)
              const color = changeColor(v)
              const Icon = v >= 0 ? RiseOutlined : FallOutlined
              return (
                <Space size={2} style={{ color }}>
                  <Icon style={{ fontSize: 10 }} />
                  <Text style={{ color, fontSize: 12, fontVariantNumeric: 'tabular-nums' }}>
                    {v > 0 ? '+' : ''}{pct}%
                  </Text>
                </Space>
              )
            },
          },
        ]}
      />
    </ModuleCard>
  )
}

// -------------------------------------------------------------------------
// HotTopics — 每日热点话题 with type filter
// -------------------------------------------------------------------------
const TOPIC_TYPE_LABEL: Record<number, { label: string; color: string }> = {
  1: { label: '晨报', color: '#3b82f6' },
  2: { label: '午报', color: '#f59e0b' },
  3: { label: '纪要', color: '#8b5cf6' },
  4: { label: '研报', color: '#10b981' },
}

function HotTopics({ mod }: { mod?: ModuleDoc }) {
  const allItems = mod?.items || []
  const [filter, setFilter] = useState<string>('all')

  const types = useMemo(() => {
    const set = new Set<number>()
    allItems.forEach((it: any) => it.statType && set.add(it.statType))
    return Array.from(set).sort()
  }, [allItems])

  const items = useMemo(() => {
    if (filter === 'all') return allItems.slice(0, 20)
    const t = parseInt(filter, 10)
    return allItems.filter((it: any) => it.statType === t).slice(0, 20)
  }, [allItems, filter])

  return (
    <ModuleCard mod={mod} accentColor="#8b5cf6"
      icon={<BulbOutlined />}
      extra={
        types.length > 1 && (
          <Segmented size="small" value={filter}
            onChange={(v) => setFilter(String(v))}
            options={[
              { label: `全部 ${allItems.length}`, value: 'all' },
              ...types.map((t) => ({
                label: (TOPIC_TYPE_LABEL[t]?.label || `类型${t}`),
                value: String(t),
              })),
            ]}
          />
        )
      }
    >
      <div style={{ maxHeight: 440, overflowY: 'auto' }}>
        {items.map((it: any, i: number) => {
          const meta = TOPIC_TYPE_LABEL[it.statType] || { label: `类型${it.statType}`, color: '#64748b' }
          return (
            <div key={it.id || i} style={{
              padding: '8px 4px',
              borderBottom: i < items.length - 1 ? '1px solid #f3f4f6' : 'none',
            }}>
              <Space size={6} style={{ marginBottom: 4 }}>
                <Tag color={meta.color} style={{ margin: 0, fontWeight: 500 }}>
                  {meta.label}
                </Tag>
                <Text type="secondary" style={{ fontSize: 11 }}>
                  <CalendarOutlined /> {String(it.statDate).replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3')}
                </Text>
              </Space>
              <Paragraph style={{ margin: 0, fontSize: 13, lineHeight: 1.5 }}>
                {it.title}
              </Paragraph>
            </div>
          )
        })}
        {items.length === 0 && <Empty description="暂无数据" />}
      </div>
    </ModuleCard>
  )
}

// -------------------------------------------------------------------------
// HotMeetings — 机构热议纪要
// -------------------------------------------------------------------------
function HotMeetings({ mod }: { mod?: ModuleDoc }) {
  const items = (mod?.items || []).slice(0, 12)
  return (
    <ModuleCard mod={mod} accentColor="#3b82f6"
      icon={<FileTextOutlined />}>
      {items.length === 0 ? (
        <Empty description="当前时段无热议纪要" image={Empty.PRESENTED_IMAGE_SIMPLE} />
      ) : (
        <div style={{ maxHeight: 440, overflowY: 'auto' }}>
          {items.map((m: any, i: number) => {
            const title = m.topic || m.title
            const href = m.id
              ? `/gangtise/summary?open=${encodeURIComponent(String(m.id))}`
              : null
            return (
              <div key={m.id || i} style={{
                padding: '8px 4px',
                borderBottom: i < items.length - 1 ? '1px solid #f3f4f6' : 'none',
              }}>
                {href ? (
                  <Link to={href} title="点击打开本地数据库中的详情"
                        style={{ fontSize: 13, fontWeight: 500, color: '#0f172a' }}>
                    {title}
                  </Link>
                ) : (
                  <Paragraph style={{ margin: 0, fontSize: 13, fontWeight: 500 }}>
                    {title}
                  </Paragraph>
                )}
                <Space size={6} style={{ marginTop: 4, fontSize: 11, color: '#64748b' }}>
                  {m.partyName && <span>🏦 {m.partyName}</span>}
                  {m.blockName && <Tag color="purple" style={{ margin: 0 }}>{m.blockName}</Tag>}
                  {m.categoryStmt && <Tag color="cyan" style={{ margin: 0 }}>{m.categoryStmt}</Tag>}
                  {m.cnfrTime && <span>⏰ {formatCnfrTime(m.cnfrTime)}</span>}
                </Space>
              </div>
            )
          })}
        </div>
      )}
    </ModuleCard>
  )
}

// -------------------------------------------------------------------------
// ResearchSchedule — 近期研究行程 (rich fields: publicNum/orgNum/brokerNum/privateNum/eventTypeStmt)
// -------------------------------------------------------------------------
function ResearchSchedule({ mod }: { mod?: ModuleDoc }) {
  const allItems = (mod?.items || [])
  // 按 industry 统计排序 & 支持行业筛选
  const [industryFilter, setIndustryFilter] = useState<string>('all')
  const industries = useMemo(() => {
    const counts = new Map<string, number>()
    allItems.forEach((it: any) => {
      const ind = it.industry || '其他'
      counts.set(ind, (counts.get(ind) || 0) + 1)
    })
    return Array.from(counts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 8)
  }, [allItems])

  const items = useMemo(() => {
    const base = industryFilter === 'all' ? allItems
      : allItems.filter((it: any) => (it.industry || '其他') === industryFilter)
    return base.slice(0, 40)
  }, [allItems, industryFilter])

  return (
    <ModuleCard mod={mod} accentColor="#8b5cf6"
      icon={<BarChartOutlined />}
      extra={
        industries.length > 1 && (
          <Segmented size="small" value={industryFilter}
            onChange={(v) => setIndustryFilter(String(v))}
            options={[
              { label: `全部 ${allItems.length}`, value: 'all' },
              ...industries.map(([name, n]) => ({
                label: `${name} ${n}`,
                value: name,
              })),
            ]}
          />
        )
      }
    >
      <Table
        size="small" pagination={false} rowKey={(r, i) => (r.gtsCode || '') + '-' + i}
        dataSource={items}
        scroll={{ y: 420 }}
        columns={[
          {
            title: '个股', dataIndex: 'gtsCode', key: 'c', width: 110, fixed: 'left',
            render: (v: any, r: any) => (
              <Space size={4} direction="vertical" style={{ lineHeight: 1.2 }}>
                <Text code style={{ fontSize: 11 }}>{v}</Text>
                {r.industry && <Tag style={{ margin: 0 }} color="geekblue">{r.industry}</Tag>}
              </Space>
            ),
          },
          {
            title: '类型', dataIndex: 'eventTypeStmt', key: 't', width: 80,
            render: (v: any) => v ? <Tag color="cyan">{v}</Tag> : <Text type="secondary">-</Text>,
          },
          {
            title: '最近', dataIndex: 'recentRsTime', key: 'rt', width: 100,
            render: (v: any, r: any) => (
              <Space direction="vertical" size={0} style={{ lineHeight: 1.2 }}>
                <Text style={{ fontSize: 11 }}>{v}</Text>
                {r.time2Now && <Text type="secondary" style={{ fontSize: 10 }}>{r.time2Now}</Text>}
              </Space>
            ),
          },
          {
            title: '券商/机构',
            key: 'party', width: 170,
            render: (_: any, r: any) => {
              const parts = [
                { label: '券商', v: r.brokerNum, color: '#1890ff' },
                { label: '公募', v: r.publicNum, color: '#ef4444' },
                { label: '私募', v: r.privateNum, color: '#f59e0b' },
                { label: '其他', v: r.orgNum, color: '#8b5cf6' },
              ].filter(p => p.v && parseInt(p.v, 10) > 0)
              return (
                <Space size={4} wrap>
                  {parts.map(p => (
                    <Tag key={p.label} color={p.color} style={{ margin: 0, fontSize: 10 }}>
                      {p.label} {p.v}
                    </Tag>
                  ))}
                </Space>
              )
            },
          },
          {
            title: '调研数', dataIndex: 'researchCount', key: 'r', width: 60, align: 'right',
            render: (v: any) => (
              <Badge count={parseInt(v, 10) || 0}
                style={{ backgroundColor: '#3b82f6' }}
                overflowCount={999} />
            ),
          },
          ...(allItems[0]?.recentResearchParty ? [{
            title: '最近券商', dataIndex: 'recentResearchParty', key: 'p', width: 100,
            render: (v: string) => <Text style={{ fontSize: 11 }}>{v}</Text>,
          }] : []),
        ] as any}
      />
    </ModuleCard>
  )
}

// -------------------------------------------------------------------------
// QuickEntries — 快速入口 with iconfont icons
// -------------------------------------------------------------------------
function QuickEntries({ mod }: { mod?: ModuleDoc }) {
  const items = (mod?.items || [])
  // Flatten tree if nested
  const flat: any[] = []
  const walk = (list: any[]) => list.forEach((m: any) => {
    if (m.menuType === 'A' && (m.menuName || m.appName)) {
      flat.push(m)
    }
    if (m.children) walk(m.children)
  })
  walk(items)

  return (
    <ModuleCard mod={mod} accentColor="#06b6d4"
      icon={<AppstoreOutlined />}>
      <div style={{ maxHeight: 440, overflowY: 'auto' }}>
        <Row gutter={[6, 6]}>
          {flat.slice(0, 40).map((m: any, i: number) => {
            // 解析 "&#xe9f5;" 为 unicode char (iconfont). 前端没加载 gangtise iconfont,
            // 所以显示为方框 — 作为视觉占位也算个 tag.
            const iconChar = m.menuIcon && m.menuIcon.includes('&#x')
              ? String.fromCodePoint(parseInt(m.menuIcon.replace(/[^0-9a-fx]/gi, '').replace('x', ''), 16))
              : null
            return (
              <Col key={m.id || i} xs={24} sm={12}>
                <Tooltip title={`${m.appPath || '-'} · #${m.appCode || '-'}`}>
                  <div style={{
                    padding: '8px 10px',
                    border: '1px solid #e5e7eb',
                    borderRadius: 6,
                    background: '#f9fafb',
                    fontSize: 13,
                    cursor: 'default',
                    transition: 'all 0.15s',
                  }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.background = '#e0f2fe'
                      e.currentTarget.style.borderColor = '#06b6d4'
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.background = '#f9fafb'
                      e.currentTarget.style.borderColor = '#e5e7eb'
                    }}
                  >
                    <Space size={6}>
                      {iconChar && (
                        <span style={{
                          fontFamily: '"iconfont", monospace', fontSize: 14,
                          color: '#06b6d4', minWidth: 16, display: 'inline-block',
                        }}>{iconChar}</span>
                      )}
                      <Text strong>{m.menuName || m.appName}</Text>
                      {m.abbreviation && (
                        <Text type="secondary" style={{ fontSize: 10 }}>[{m.abbreviation}]</Text>
                      )}
                    </Space>
                  </div>
                </Tooltip>
              </Col>
            )
          })}
        </Row>
      </div>
    </ModuleCard>
  )
}

// -------------------------------------------------------------------------
// Banners — image thumbnails
// -------------------------------------------------------------------------
function Banners({ mod }: { mod?: ModuleDoc }) {
  const items = (mod?.items || [])
  return (
    <ModuleCard mod={mod} accentColor="#ec4899"
      icon={<PictureOutlined />}>
      <Space direction="vertical" size={8} style={{ width: '100%' }}>
        {items.map((b: any) => (
          <div key={b.bannerId} style={{
            borderRadius: 6,
            overflow: 'hidden',
            border: '1px solid #f3f4f6',
          }}>
            {b.previewUrl && (
              <Image src={b.previewUrl}
                preview={{ mask: b.title }}
                style={{ width: '100%', display: 'block', objectFit: 'cover', maxHeight: 120 }}
                fallback="data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='1' height='1'/>"
              />
            )}
            <div style={{ padding: '6px 10px', background: '#f9fafb' }}>
              <Space size={6}>
                <Text strong style={{ fontSize: 12 }}>{b.title}</Text>
                <Tag color="magenta" style={{ margin: 0 }}>#{b.category}</Tag>
              </Space>
            </div>
          </div>
        ))}
        {items.length === 0 && <Empty description="无 banner" />}
      </Space>
    </ModuleCard>
  )
}

// -------------------------------------------------------------------------
// PlatformTodayCounts — 平台今日 (纪要 / 研报 / 观点) 条数 + 分类拆解
// -------------------------------------------------------------------------
interface ClassifyCount {
  key: number | string
  name: string
  platform_count: number
  scanned?: number
  pages?: number
}
interface DailyType {
  kind: 'summary' | 'research' | 'chief'
  label: string
  platform_count: number
  classifies: ClassifyCount[]
}
interface DailySnapshot {
  date: string
  tz: string
  generated_at: string
  elapsed_s: number
  types: DailyType[]
}

const KIND_ACCENT: Record<string, string> = {
  summary:  '#3b82f6',
  research: '#8b5cf6',
  chief:    '#ef4444',
}

function PlatformTodayCountsCard() {
  const [snap, setSnap] = useState<DailySnapshot | null>(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [expandedKind, setExpandedKind] = useState<string | null>(null)

  const load = useCallback(async (refresh = false) => {
    try {
      setLoading(true)
      const r = await api.get('/platform-info/gangtise/daily-counts', {
        params: refresh ? { refresh: true } : undefined,
      })
      setSnap(r.data)
      setErr(null)
    } catch (e: any) {
      setErr(e?.response?.data?.detail || e?.message || '加载失败')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    load()
    const id = setInterval(() => load(), 5 * 60_000)   // server caches 300s
    return () => clearInterval(id)
  }, [load])

  const total = (snap?.types || []).reduce((sum, t) => sum + (t.platform_count || 0), 0)

  return (
    <Card
      size="small"
      style={{
        borderLeft: '3px solid #0ea5e9',
        boxShadow: '0 1px 3px rgba(0,0,0,0.05)',
      }}
      bodyStyle={{ padding: 12 }}
      title={
        <Space size={8}>
          <CalendarOutlined style={{ color: '#0ea5e9' }} />
          <Text strong style={{ fontSize: 14 }}>今日平台数据量</Text>
          <Text type="secondary" style={{ fontSize: 11 }}>
            open.gangtise.com · {snap?.date || '-'} · 合计 {total}
          </Text>
        </Space>
      }
      extra={
        <Tooltip title="重新采样 (绕过 5 分钟缓存)">
          <Button size="small" loading={loading}
            icon={<ReloadOutlined />}
            onClick={() => load(true)}>
            刷新
          </Button>
        </Tooltip>
      }
    >
      {err && <Alert type="error" showIcon message="加载失败" description={err} />}
      {!err && !snap && loading && <Spin />}
      {snap && (
        <Row gutter={[12, 12]}>
          {snap.types.map((t) => {
            const color = KIND_ACCENT[t.kind] || '#64748b'
            const hasDetail = t.classifies && t.classifies.length > 0
            const expanded = expandedKind === t.kind
            return (
              <Col xs={24} sm={8} key={t.kind}>
                <div style={{
                  padding: 10,
                  border: '1px solid #e5e7eb',
                  borderRadius: 6,
                  background: '#fafbfc',
                  borderLeft: `3px solid ${color}`,
                }}>
                  <Row align="middle" gutter={8}>
                    <Col flex="auto">
                      <Text strong style={{ fontSize: 13 }}>{t.label}</Text>
                    </Col>
                    <Col flex="none">
                      <Text strong style={{
                        fontSize: 22, fontVariantNumeric: 'tabular-nums',
                        color,
                      }}>
                        {t.platform_count}
                      </Text>
                      <Text type="secondary" style={{ fontSize: 11, marginLeft: 2 }}>
                        条
                      </Text>
                    </Col>
                  </Row>
                  {hasDetail && (
                    <Button type="link" size="small"
                      style={{ padding: 0, fontSize: 11, marginTop: 4 }}
                      icon={expanded ? <UpOutlined /> : <DownOutlined />}
                      onClick={() => setExpandedKind(expanded ? null : t.kind)}>
                      {expanded ? '收起' : `按分类拆解 (${t.classifies.length})`}
                    </Button>
                  )}
                  {expanded && hasDetail && (
                    <div style={{
                      marginTop: 6, paddingTop: 6,
                      borderTop: '1px dashed #e5e7eb',
                    }}>
                      {t.classifies.map((c) => (
                        <Row key={c.key} gutter={4} style={{ padding: '2px 0' }}>
                          <Col flex="auto">
                            <Text style={{ fontSize: 12 }}>{c.name}</Text>
                          </Col>
                          <Col flex="none">
                            <Tag style={{ margin: 0, fontSize: 11, fontWeight: 600 }}
                              color={c.platform_count > 0 ? color : undefined}>
                              {c.platform_count}
                            </Tag>
                          </Col>
                        </Row>
                      ))}
                    </div>
                  )}
                </div>
              </Col>
            )
          })}
        </Row>
      )}
    </Card>
  )
}

// -------------------------------------------------------------------------
// Summary strip at the top
// -------------------------------------------------------------------------
function Header({
  snap, onReload,
  autoRefresh, setAutoRefresh,
  refreshSec, setRefreshSec,
  nextCountdown,
}: {
  snap: Snapshot
  onReload: () => void
  autoRefresh: boolean
  setAutoRefresh: (v: boolean) => void
  refreshSec: number
  setRefreshSec: (v: number) => void
  nextCountdown: number
}) {
  const fetchedAge = (() => {
    if (!snap.newest_fetched_at) return null
    const age = Math.floor((Date.now() - new Date(snap.newest_fetched_at).getTime()) / 1000)
    return age
  })()
  return (
    <div
      style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: 20,
        padding: '14px 20px',
        background: 'linear-gradient(135deg, #0ea5e9 0%, #6366f1 100%)',
        borderRadius: 10,
        color: '#fff',
        boxShadow: '0 4px 12px rgba(14, 165, 233, 0.3)',
      }}
    >
      <div>
        <Title level={3} style={{ color: '#fff', margin: 0, fontSize: 20 }}>
          <GlobalOutlined /> Gangtise 岗底斯 · 平台信息
        </Title>
        <Text style={{ color: 'rgba(255,255,255,0.9)', fontSize: 12 }}>
          open.gangtise.com 主页实时快照 · 正常 {snap.ok_count}/{snap.module_count}
          {' · '}总条数 {snap.total_items}
          {fetchedAge != null && <> · 最新 {formatAge(fetchedAge)} 前</>}
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
            { value: 30, label: '30s' },
            { value: 60, label: '60s' },
            { value: 120, label: '2m' },
            { value: 300, label: '5m' },
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
            onClick={onReload}
          />
        </Tooltip>
      </Space>
    </div>
  )
}

// -------------------------------------------------------------------------
// Main page
// -------------------------------------------------------------------------
export default function GangtisePlatformInfo() {
  const [snap, setSnap] = useState<Snapshot | null>(null)
  const [loading, setLoading] = useState(true)
  const [err, setErr] = useState<string | null>(null)

  // Auto-refresh controls (unified with AlphaPai PlatformInfo)
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [refreshSec, setRefreshSec] = useState<number>(60)
  const [lastFetchMs, setLastFetchMs] = useState<number | null>(null)
  const [nextCountdown, setNextCountdown] = useState<number>(0)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const countdownRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const load = useCallback(async () => {
    try {
      setLoading(true)
      const r = await api.get('/platform-info/gangtise')
      setSnap(r.data)
      setErr(null)
      setLastFetchMs(Date.now())
    } catch (e: any) {
      setErr(e?.response?.data?.detail || e?.message || '加载失败')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    load()
  }, [load])

  useEffect(() => {
    if (timerRef.current) clearInterval(timerRef.current)
    if (autoRefresh) {
      timerRef.current = setInterval(() => load(), refreshSec * 1000)
    }
    return () => {
      if (timerRef.current) clearInterval(timerRef.current)
    }
  }, [autoRefresh, refreshSec, load])

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

  if (loading && !snap) {
    return <Spin tip="加载 Gangtise 平台信息..." size="large"
      style={{ display: 'block', marginTop: 80 }} />
  }
  if (err) {
    return <Alert type="error" showIcon message="加载失败" description={err}
      action={<Button size="small" onClick={load}>重试</Button>} />
  }
  if (!snap) return null

  const marketMod = pick(snap, 'market_index')

  return (
    <div style={{ padding: 16, background: '#f8fafc', minHeight: '100vh' }}>
      <Header
        snap={snap}
        onReload={load}
        autoRefresh={autoRefresh}
        setAutoRefresh={setAutoRefresh}
        refreshSec={refreshSec}
        setRefreshSec={setRefreshSec}
        nextCountdown={nextCountdown}
      />

      {marketMod && (
        <div style={{ marginBottom: 16 }}>
          <MarketTicker mod={marketMod} />
        </div>
      )}

      <div style={{ marginBottom: 16 }}>
        <PlatformTodayCountsCard />
      </div>

      <Row gutter={[16, 16]}>
        <Col xs={24} md={12} xl={8}>
          <HotStocks mod={pick(snap, 'hot_stocks')} />
        </Col>
        <Col xs={24} md={12} xl={8}>
          <HotConcepts mod={pick(snap, 'hot_concepts')} />
        </Col>
        <Col xs={24} md={12} xl={8}>
          <HotTopics mod={pick(snap, 'hot_topics')} />
        </Col>
        <Col xs={24} xl={16}>
          <ResearchSchedule mod={pick(snap, 'research_sched')} />
        </Col>
        <Col xs={24} md={12} xl={8}>
          <HotMeetings mod={pick(snap, 'hot_meetings')} />
        </Col>
        <Col xs={24} md={12} xl={16}>
          <QuickEntries mod={pick(snap, 'quick_entries')} />
        </Col>
        <Col xs={24} md={12} xl={8}>
          <Banners mod={pick(snap, 'banners')} />
        </Col>
      </Row>

      <Divider />

      <Row justify="space-between" align="middle" style={{ padding: '0 8px' }}>
        <Col>
          <Space size={16} style={{ fontSize: 11, color: '#64748b' }}>
            <span><DatabaseOutlined /> 8 个模块 · 总 {snap.total_items} 条</span>
            <span>最新: {snap.newest_fetched_at?.slice(0, 19).replace('T', ' ')}</span>
            <span>最旧: {snap.oldest_fetched_at?.slice(0, 19).replace('T', ' ')}</span>
          </Space>
        </Col>
        <Col>
          <Text type="secondary" style={{ fontSize: 11 }}>
            数据源: open.gangtise.com · Scraper 每 10 min 写 MongoDB gangtise.homepage
          </Text>
        </Col>
      </Row>
    </div>
  )
}
