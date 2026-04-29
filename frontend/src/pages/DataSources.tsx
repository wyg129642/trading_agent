import { useEffect, useRef, useState } from 'react'
import {
  Alert,
  Badge,
  Button,
  Card,
  Checkbox,
  Col,
  Descriptions,
  Drawer,
  Dropdown,
  Empty,
  Form,
  Input,
  Popconfirm,
  Row,
  Space,
  Spin,
  Tabs,
  Tag,
  Tooltip,
  Typography,
  message,
} from 'antd'
import {
  ReloadOutlined,
  LoginOutlined,
  EditOutlined,
  CheckCircleFilled,
  CloseCircleFilled,
  QuestionCircleFilled,
  QrcodeOutlined,
  DeleteOutlined,
  ThunderboltOutlined,
  PlayCircleOutlined,
  PauseCircleOutlined,
  BugOutlined,
  EyeOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import api from '../services/api'
import CdpViewer from '../components/CdpViewer'

interface PlatformItem {
  key: string
  display_name: string
  supports_auto_login: boolean
  login_hint: string
  login_identifier?: 'phone' | 'email' | string
  login_needs_password?: boolean
  login_mode?: 'password' | 'qr' | 'sms' | string
  supports_qr_login?: boolean
  has_saved_login?: boolean
  saved_identifier?: string
  has_credentials: boolean
  credentials_path: string
  last_refreshed: string | null
  token_fields: Record<string, string>
  health: 'ok' | 'expired' | 'unknown' | string
  health_detail: string
  health_checked_at: string | null
  last_data_at?: string | null
  data_age_hours?: number | null
  data_total?: number | null
  content_empty_ratio?: number | null
  content_sample_size?: number | null
}

// Platform health classification — MUST align with backend
// `credential_manager.py::PlatformStatus.health` comment.
//
// Colors:
//   green  = full access verified, no action needed
//   red    = action needed NOW (token dead — re-login to fix)
//   amber  = something suboptimal but token itself is fine
//            (recoverable without user action, or user can choose to re-login)
//   gray   = probe inconclusive (不明状态, 点探活再试)
const HEALTH_META: Record<string, { color: string; label: string; icon: React.ReactNode }> = {
  ok: {
    color: '#10b981',
    label: '健康',
    icon: <CheckCircleFilled style={{ color: '#10b981' }} />,
  },
  // HTTP 401/403 or platform said "token invalid" — user must re-login
  expired: {
    color: '#ef4444',
    label: '已过期',
    icon: <CloseCircleFilled style={{ color: '#ef4444' }} />,
  },
  // cookie 能过 users/me, 但最近入库的文档 content_md 大面积为空 →
  // 平台后台已掐掉本账号的 detail (正文) 权限, list 端点仍返摘要, 所以
  // dashboard "今日入库" 会虚高. 典型案例: AceCamp 团队金卡 quota:0 封控.
  // 换 token 没用, 必须联系平台或换账号.
  degraded: {
    color: '#ef4444',
    label: 'detail 被封',
    icon: <CloseCircleFilled style={{ color: '#ef4444' }} />,
  },
  // Cookie/token accepted but not bound to a real user. Still gets preview
  // content; re-login IF user wants full access. Not urgent — amber not red.
  anonymous: {
    color: '#f59e0b',
    label: '匿名访问',
    icon: <QuestionCircleFilled style={{ color: '#f59e0b' }} />,
  },
  // token 仍然有效, 只是平台级日刷新额度用尽 — 换 token 没用, 0 点自动重置
  ratelimited: {
    color: '#f59e0b',
    label: '额度用尽',
    icon: <QuestionCircleFilled style={{ color: '#f59e0b' }} />,
  },
  unknown: {
    color: '#64748b',
    label: '未知',
    icon: <QuestionCircleFilled style={{ color: '#64748b' }} />,
  },
}

function fmtTime(iso: string | null): string {
  if (!iso) return '—'
  return dayjs(iso).format('YYYY-MM-DD HH:mm')
}

export default function DataSources() {
  const [platforms, setPlatforms] = useState<PlatformItem[]>([])
  const [loading, setLoading] = useState(true)
  const [refreshingKey, setRefreshingKey] = useState<string | null>(null)
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [selected, setSelected] = useState<PlatformItem | null>(null)
  // Viewer drawer — reuses CdpViewer in mode='viewer' to open the platform
  // already-logged-in for side-by-side data comparison.
  const [viewerOpen, setViewerOpen] = useState(false)
  const [viewerPlatform, setViewerPlatform] = useState<PlatformItem | null>(null)
  const [viewerSection, setViewerSection] = useState<string | undefined>()

  const fetchAll = async () => {
    setLoading(true)
    try {
      const res = await api.get<PlatformItem[]>('/data-sources')
      setPlatforms(res.data)
    } catch (e: any) {
      message.error(e.response?.data?.detail || '加载失败')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchAll()
  }, [])

  const reprobeOne = async (key: string) => {
    setRefreshingKey(key)
    try {
      const res = await api.get<PlatformItem>(`/data-sources/${key}`)
      setPlatforms((prev) => prev.map((p) => (p.key === key ? res.data : p)))
    } catch (e: any) {
      message.error(e.response?.data?.detail || '探活失败')
    } finally {
      setRefreshingKey(null)
    }
  }

  const openDrawer = (p: PlatformItem) => {
    setSelected(p)
    setDrawerOpen(true)
  }

  const openViewer = (p: PlatformItem, section?: string) => {
    setViewerPlatform(p)
    setViewerSection(section)
    setViewerOpen(true)
  }

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <Typography.Title level={4} style={{ margin: 0 }}>
          数据源管理
        </Typography.Title>
        <Button icon={<ReloadOutlined />} onClick={fetchAll} loading={loading}>
          刷新全部
        </Button>
      </div>

      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 16 }}
        message="在这里管理 7 个爬虫平台的登录凭证"
        description={
          <>
            支持自动登录的平台可直接输入账号密码; 其他平台请手动粘贴 token。凭证只保存在服务端 credentials.json, 不进 Git。
            {' '}每日入库量图表已迁移至 <Typography.Link href="/database-overview">数据库看板</Typography.Link>。
          </>
        }
      />

      <Row gutter={[16, 16]}>
        {platforms.map((p) => {
          const meta = HEALTH_META[p.health] || HEALTH_META.unknown
          return (
            <Col xs={24} md={12} xl={8} key={p.key}>
              <Card
                size="small"
                title={
                  <Space>
                    {meta.icon}
                    <span>{p.display_name}</span>
                  </Space>
                }
                extra={
                  <Tag color={meta.color} style={{ marginRight: 0 }}>
                    {meta.label}
                  </Tag>
                }
                actions={[
                  <Tooltip title="重新探活" key="probe">
                    <Button
                      type="text"
                      icon={<ReloadOutlined />}
                      loading={refreshingKey === p.key}
                      onClick={() => reprobeOne(p.key)}
                    >
                      探活
                    </Button>
                  </Tooltip>,
                  <Button type="text" icon={<EditOutlined />} onClick={() => openDrawer(p)} key="edit">
                    {p.supports_auto_login ? '登录 / 粘贴' : '粘贴 Token'}
                  </Button>,
                ].concat(
                  p.has_saved_login
                    ? [
                        <QuickLoginButton
                          key="quick"
                          platform={p}
                          onLoggedIn={fetchAll}
                        />,
                      ]
                    : [],
                ).concat(
                  // 支持注入的平台就显示"实时查看"
                  // (jinmen 没 credentials.json 也行 — 后端 fallback 到 scraper 常量)
                  VIEWER_SECTIONS[p.key]
                    ? [
                        <Dropdown
                          key="viewer"
                          trigger={['click']}
                          placement="top"
                          menu={{
                            items: VIEWER_SECTIONS[p.key].map((s) => ({
                              key: s.key,
                              label: s.label,
                              onClick: () => openViewer(p, s.key),
                            })),
                          }}
                        >
                          <Button type="text" icon={<EyeOutlined />}>
                            实时查看
                          </Button>
                        </Dropdown>,
                      ]
                    : [],
                )}
              >
                <Descriptions column={1} size="small">
                  <Descriptions.Item label="最后刷新">{fmtTime(p.last_refreshed)}</Descriptions.Item>
                  <Descriptions.Item label="最新入库">
                    {p.last_data_at ? (
                      <Space size={6}>
                        <span>{fmtTime(p.last_data_at)}</span>
                        {typeof p.data_age_hours === 'number' && (
                          <Tag
                            color={
                              p.data_age_hours < 4
                                ? 'green'
                                : p.data_age_hours < 24
                                ? 'blue'
                                : p.data_age_hours < 72
                                ? 'orange'
                                : 'red'
                            }
                            style={{ marginRight: 0, fontSize: 11 }}
                          >
                            {p.data_age_hours < 1
                              ? `${Math.round(p.data_age_hours * 60)}m`
                              : `${p.data_age_hours.toFixed(1)}h`}{' '}
                            前
                          </Tag>
                        )}
                        {typeof p.data_total === 'number' && (
                          <Typography.Text type="secondary" style={{ fontSize: 11 }}>
                            · 总 {p.data_total} 条
                          </Typography.Text>
                        )}
                        {typeof p.content_empty_ratio === 'number' && p.content_sample_size ? (
                          <Tag
                            color={p.content_empty_ratio >= 0.7 ? 'red'
                              : p.content_empty_ratio >= 0.3 ? 'orange' : 'green'}
                            style={{ marginRight: 0, fontSize: 11 }}
                            title={`最近 ${p.content_sample_size} 条入库文档中, ${Math.round(p.content_empty_ratio * p.content_sample_size)} 条 content_md 不足 200 字 (仅标题/摘要)`}
                          >
                            {p.content_empty_ratio >= 0.7 ? '空壳 ' : '正文 '}
                            {Math.round(p.content_empty_ratio * 100)}%
                          </Tag>
                        ) : null}
                      </Space>
                    ) : (
                      <Typography.Text type="secondary">—</Typography.Text>
                    )}
                  </Descriptions.Item>
                  <Descriptions.Item label="登录方式">
                    <Badge
                      status={p.supports_auto_login ? 'success' : 'default'}
                      text={p.supports_auto_login ? `自动 (${p.login_hint})` : p.login_hint}
                    />
                  </Descriptions.Item>
                  {p.has_saved_login && (
                    <Descriptions.Item label="已存密码">
                      <Tag color="gold" style={{ marginRight: 0 }}>
                        {p.saved_identifier}
                      </Tag>
                    </Descriptions.Item>
                  )}
                </Descriptions>
                {p.health_detail && (
                  <Typography.Paragraph
                    type="secondary"
                    style={{ fontSize: 11, marginTop: 8, marginBottom: 0, whiteSpace: 'pre-wrap' }}
                    ellipsis={{ rows: 3, expandable: true, symbol: '展开' }}
                  >
                    {p.health_detail}
                  </Typography.Paragraph>
                )}
                <CrawlerControl platformKey={p.key} />
              </Card>
            </Col>
          )
        })}
      </Row>

      {platforms.length === 0 && !loading && <Empty description="暂无数据源" />}

      <CredentialDrawer
        open={drawerOpen}
        platform={selected}
        onClose={() => {
          setDrawerOpen(false)
          setSelected(null)
        }}
        onSaved={() => {
          setDrawerOpen(false)
          setSelected(null)
          fetchAll()
        }}
      />

      <Drawer
        title={
          <Space>
            <EyeOutlined />
            <span>
              实时查看 · {viewerPlatform?.display_name}
              {viewerSection && viewerPlatform && VIEWER_SECTIONS[viewerPlatform.key]?.find(s => s.key === viewerSection) && (
                <Tag color="purple" style={{ marginLeft: 8 }}>
                  {VIEWER_SECTIONS[viewerPlatform.key].find(s => s.key === viewerSection)!.label}
                </Tag>
              )}
            </span>
          </Space>
        }
        open={viewerOpen}
        onClose={() => {
          setViewerOpen(false)
          setViewerPlatform(null)
          setViewerSection(undefined)
        }}
        width={1100}
        destroyOnClose
      >
        {viewerPlatform && (
          <CdpViewer
            mode="viewer"
            platformKey={viewerPlatform.key}
            section={viewerSection}
            onSuccess={() => {
              setViewerOpen(false)
              setViewerPlatform(null)
              setViewerSection(undefined)
            }}
            onCancel={() => {
              setViewerOpen(false)
              setViewerPlatform(null)
              setViewerSection(undefined)
            }}
          />
        )}
      </Drawer>
    </div>
  )
}

// Per-platform deep-link sections for the viewer dropdown. Keys + URLs
// must match backend `_VIEWER_SECTIONS` — source of truth is the backend;
// this mirror just drives the frontend menu labels.
const VIEWER_SECTIONS: Record<string, { key: string; label: string }[]> = {
  gangtise: [
    { key: 'research', label: '研报' },
    { key: 'summary',  label: '纪要' },
    { key: 'chief',    label: '首席观点' },
  ],
  alphapai: [
    { key: 'roadshow', label: '会议/路演' },
    { key: 'comment',  label: '券商点评' },
    { key: 'report',   label: '券商研报' },
    { key: 'wechat',   label: '社媒/微信' },
  ],
  jinmen: [
    { key: 'meetings', label: '会议纪要' },
    { key: 'reports',  label: '研报' },
    { key: 'home',     label: '主页' },
  ],
  alphaengine: [
    { key: 'summary',       label: '纪要' },
    { key: 'chinaReport',   label: '国内研报' },
    { key: 'foreignReport', label: '海外研报' },
    { key: 'news',          label: '资讯' },
  ],
  meritco: [
    { key: 't2',   label: '专业内容' },
    { key: 't3',   label: '久谦自研' },
    { key: 't1',   label: '活动' },
    { key: 'home', label: '主页' },
  ],
  funda:       [{ key: 'home', label: '主页' }],
  acecamp:     [{ key: 'home', label: '主页' }],
  thirdbridge: [{ key: 'home', label: '全部论坛' }],
  // 微信公众号管理后台 — 扫码登录后跳到 home/index 页, 这里做主页 deep-link
  wechat_mp:   [{ key: 'home', label: '后台首页' }],
  // meritco viewer seeds localStorage.token + X-User-Type (2026-04-24);
  // sections above drive the deep-link menu.
}

// ── Drawer: tabs for auto-login vs manual token paste ────────────────────

interface DrawerProps {
  open: boolean
  platform: PlatformItem | null
  onClose: () => void
  onSaved: () => void
}

function CredentialDrawer({ open, platform, onClose, onSaved }: DrawerProps) {
  if (!platform) return null

  const preferQr = platform.login_mode === 'qr'
  const hasQr = preferQr || platform.supports_qr_login === true
  const defaultTab = !platform.supports_auto_login
    ? 'manual'
    : preferQr
    ? 'qr'
    : 'password'

  // Remote-browser tab — enabled for every platform that supports
  // auto-login. Uses the CDP screencast + JS-storage-hook stack; works
  // for all sites since the hook is site-agnostic.
  const hasRemoteBrowser = platform.supports_auto_login === true
  const defaultTabWithRemote = hasRemoteBrowser ? 'remote' : defaultTab

  const tabs: any[] = []
  if (platform.supports_auto_login) {
    if (hasRemoteBrowser) {
      tabs.push({
        key: 'remote',
        label: '🖥 远程浏览器',
        children: (
          <CdpViewer
            platformKey={platform.key}
            onSuccess={onSaved}
            onCancel={() => onClose()}
          />
        ),
      })
    }
    if (hasQr) {
      tabs.push({
        key: 'qr',
        label: (
          <span>
            <QrcodeOutlined /> 扫码登录
          </span>
        ),
        children: <QrTab platform={platform} onSaved={onSaved} />,
      })
    }
    tabs.push({
      key: 'password',
      label: platform.login_needs_password ? '账号密码' : '短信登录',
      children: <PasswordTab platform={platform} onSaved={onSaved} />,
    })
  }
  tabs.push({
    key: 'manual',
    label: '手动粘贴 Token',
    children: <ManualTab platform={platform} onSaved={onSaved} />,
  })

  return (
    <Drawer
      title={`配置 · ${platform.display_name}`}
      open={open}
      onClose={onClose}
      width={hasRemoteBrowser ? 900 : 560}
      destroyOnClose
    >
      <Tabs defaultActiveKey={defaultTabWithRemote} items={tabs} />
    </Drawer>
  )
}

// ── Auto-login tab ───────────────────────────────────────────────────────

interface TabProps {
  platform: PlatformItem
  onSaved: () => void
}

// Shared polling state — both Password and Qr tabs use this to watch a session.
function useLoginSession(platformKey: string, onSuccess: () => void) {
  const [sessionId, setSessionId] = useState<string | null>(null)
  const [status, setStatus] = useState<Record<string, string> | null>(null)
  const [starting, setStarting] = useState(false)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const stopPolling = () => {
    if (pollRef.current) {
      clearInterval(pollRef.current)
      pollRef.current = null
    }
  }

  useEffect(() => () => stopPolling(), [])

  const pollOnce = async (sid: string) => {
    try {
      const res = await api.get(`/data-sources/${platformKey}/login/${sid}`)
      setStatus(res.data)
      if (['SUCCESS', 'FAILED', 'LOCKED_OUT', 'NOT_FOUND'].includes(res.data.status)) {
        stopPolling()
        setStarting(false)
        if (res.data.status === 'SUCCESS') {
          message.success('登录成功, 凭证已更新')
          // Keep the drawer open long enough for the success panel to register
          // with the eye — 3 seconds — then auto-close + refetch.
          setTimeout(onSuccess, 3000)
        } else if (res.data.status === 'LOCKED_OUT') {
          // 平台触发风控锁定 — 警告用户不要立即重试, 继续点击会延长锁定时间
          message.warning({
            content: res.data.message || '账号被平台锁定 · 等 10 分钟后再试',
            duration: 10,
          })
        } else if (res.data.status === 'FAILED') {
          message.error(res.data.message || '登录失败')
        }
      }
    } catch {
      /* keep polling */
    }
  }

  const begin = async (body: Record<string, any>): Promise<string | null> => {
    setStarting(true)
    setStatus(null)
    try {
      const res = await api.post(`/data-sources/${platformKey}/login`, body)
      const sid = res.data.session_id
      setSessionId(sid)
      pollRef.current = setInterval(() => pollOnce(sid), 600)
      pollOnce(sid)
      return sid
    } catch (e: any) {
      setStarting(false)
      message.error(e.response?.data?.detail || '启动失败')
      return null
    }
  }

  const reset = () => {
    stopPolling()
    setSessionId(null)
    setStatus(null)
    setStarting(false)
  }

  return { sessionId, status, starting, begin, reset }
}

// ── Success panel — shown briefly in both QR and password tabs ───────────

function SuccessPanel({
  platform,
  onClose,
}: {
  platform: PlatformItem
  onClose: () => void
}) {
  return (
    <div
      style={{
        textAlign: 'center',
        padding: '40px 24px',
        background: '#f0fdf4',
        border: '1px solid #86efac',
        borderRadius: 8,
      }}
    >
      <CheckCircleFilled style={{ fontSize: 56, color: '#10b981' }} />
      <Typography.Title level={4} style={{ marginTop: 16, marginBottom: 4 }}>
        授权成功
      </Typography.Title>
      <Typography.Paragraph type="secondary" style={{ marginBottom: 20 }}>
        {platform.display_name} 的凭证已写入 <code>credentials.json</code>。
        <br />
        窗口将在 3 秒后自动关闭。
      </Typography.Paragraph>
      <Button type="primary" onClick={onClose}>
        完成
      </Button>
    </div>
  )
}

// ── QR-scan tab ──────────────────────────────────────────────────────────

function QrTab({ platform, onSaved }: TabProps) {
  const { sessionId, status, starting, begin, reset } = useLoginSession(platform.key, onSaved)
  const [refreshing, setRefreshing] = useState(false)

  const finished = status?.status === 'SUCCESS' || status?.status === 'FAILED'
  const succeeded = status?.status === 'SUCCESS'
  const qr = status?.qr_image
  const awaitingScan = status?.status === 'QR_NEEDED' && !!qr

  const onStart = () => begin({ mode: 'qr' })

  // Track the captured-at timestamp so we can tell when a fresh QR has arrived.
  const lastCapturedRef = useRef<string | undefined>(undefined)
  useEffect(() => {
    const cap = status?.qr_captured_at
    if (!cap) return
    if (lastCapturedRef.current && cap !== lastCapturedRef.current) {
      // New QR — refresh finished.
      setRefreshing(false)
    }
    lastCapturedRef.current = cap
  }, [status?.qr_captured_at])

  const onRefresh = async () => {
    if (!sessionId) return
    setRefreshing(true)
    try {
      await api.post(`/data-sources/${platform.key}/login/${sessionId}/refresh-qr`, {})
    } catch (e: any) {
      setRefreshing(false)
      message.error(e.response?.data?.detail || '刷新失败')
      return
    }
    // Hard-refresh (page reload) takes ~10s. Fail-safe: clear the spinner
    // after 20s even if the capture timestamp doesn't change.
    setTimeout(() => setRefreshing(false), 20000)
  }

  if (succeeded) {
    return <SuccessPanel platform={platform} onClose={onSaved} />
  }

  return (
    <Space direction="vertical" size={14} style={{ width: '100%' }}>
      <Alert
        type="info"
        showIcon
        message="扫码登录"
        description={
          <>
            点下方按钮后,服务端会启动 Chromium 打开登录页,把二维码抓回来显示在这里。
            用 <b>{platform.display_name.split(' ')[0]}</b> 手机 App 扫码即可,无需输入密码 / 短信。
          </>
        }
      />

      {!sessionId && !finished && (
        <Button type="primary" icon={<QrcodeOutlined />} onClick={onStart} loading={starting} block>
          生成二维码
        </Button>
      )}

      {sessionId && !finished && !awaitingScan && (
        <div style={{ textAlign: 'center', padding: 40 }}>
          <Spin tip={status?.message || '正在准备二维码…'} />
        </div>
      )}

      {awaitingScan && (
        <div style={{ textAlign: 'center' }}>
          <div
            style={{
              padding: 12,
              background: '#fff',
              border: '1px solid #e2e8f0',
              borderRadius: 8,
              display: 'inline-block',
              position: 'relative',
            }}
          >
            <img
              src={qr}
              alt="登录二维码"
              style={{
                width: 260,
                height: 260,
                opacity: refreshing ? 0.2 : 1,
                transition: 'opacity 0.2s',
              }}
            />
            {refreshing && (
              <div
                style={{
                  position: 'absolute',
                  inset: 0,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  flexDirection: 'column',
                  gap: 8,
                }}
              >
                <Spin />
                <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                  正在重载登录页…
                </Typography.Text>
              </div>
            )}
          </div>
          <Typography.Paragraph type="secondary" style={{ marginTop: 12, marginBottom: 8 }}>
            打开 App → 扫一扫 → 确认登录
          </Typography.Paragraph>
          <Button
            icon={<ReloadOutlined />}
            onClick={onRefresh}
            loading={refreshing}
            size="small"
            disabled={refreshing}
          >
            {refreshing ? '正在刷新…' : '二维码过期?点这里刷新'}
          </Button>
        </div>
      )}

      {status && status.status !== 'QR_NEEDED' && (
        <Alert
          type={
            status.status === 'SUCCESS' ? 'success' :
            status.status === 'FAILED' ? 'error' :
            status.status === 'LOCKED_OUT' ? 'warning' : 'info'
          }
          showIcon
          message={`状态: ${status.status}`}
          description={status.message}
        />
      )}

      {(sessionId || finished) && (
        <Button onClick={reset} block>
          {finished ? '再来一次' : '取消并关闭'}
        </Button>
      )}
    </Space>
  )
}

// ── Password tab ─────────────────────────────────────────────────────────

function PasswordTab({ platform, onSaved }: TabProps) {
  const useEmail = platform.login_identifier === 'email'
  const needsPassword = platform.login_needs_password === true
  const [identifier, setIdentifier] = useState('')
  const [password, setPassword] = useState('')
  const [remember, setRemember] = useState(true)
  const [otpCode, setOtpCode] = useState('')
  const [submittingOtp, setSubmittingOtp] = useState(false)

  const { sessionId, status, starting, begin, reset } = useLoginSession(platform.key, onSaved)

  const idLabel = useEmail ? '邮箱' : '手机号'
  const idPlaceholder = useEmail ? 'you@example.com' : '13xxxxxxxxx'
  const awaitingOtp = status?.status === 'OTP_NEEDED'
  const finished = status?.status === 'SUCCESS' || status?.status === 'FAILED'
  const succeeded = status?.status === 'SUCCESS'

  if (succeeded) {
    return <SuccessPanel platform={platform} onClose={onSaved} />
  }

  const onLogin = async () => {
    if (!identifier.trim()) {
      message.warning(`请输入${idLabel}`)
      return
    }
    if (needsPassword && !password) {
      message.warning('请输入密码')
      return
    }
    // needsPassword=false → SMS flow: send mode=sms, no password field.
    const body: Record<string, any> = needsPassword
      ? { mode: 'password', password, remember }
      : { mode: 'sms' }
    if (useEmail) body.email = identifier.trim()
    else body.phone = identifier.trim()
    await begin(body)
  }

  const onSubmitOtp = async () => {
    if (!sessionId || !otpCode.trim()) {
      message.warning('请输入验证码')
      return
    }
    setSubmittingOtp(true)
    try {
      await api.post(`/data-sources/${platform.key}/login/${sessionId}/otp`, {
        code: otpCode.trim(),
      })
      setOtpCode('')
    } catch (e: any) {
      message.error(e.response?.data?.detail || '提交失败')
    } finally {
      setSubmittingOtp(false)
    }
  }

  const onForget = async () => {
    try {
      await api.delete(`/data-sources/${platform.key}/saved-login`)
      message.success('已清除保存的密码')
      onSaved()
    } catch (e: any) {
      message.error(e.response?.data?.detail || '清除失败')
    }
  }

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {platform.has_saved_login && (
        <Alert
          type="success"
          showIcon
          message={
            <Space>
              <span>
                已保存 {platform.saved_identifier} 的密码, 可以直接快速登录
              </span>
              <Popconfirm title="清除保存的密码?" onConfirm={onForget} okText="清除" cancelText="取消">
                <Button size="small" type="link" danger icon={<DeleteOutlined />}>
                  忘记密码
                </Button>
              </Popconfirm>
            </Space>
          }
        />
      )}

      <div>
        <Typography.Text strong>{idLabel}</Typography.Text>
        <Input
          placeholder={idPlaceholder}
          value={identifier}
          onChange={(e) => setIdentifier(e.target.value)}
          disabled={sessionId !== null}
          style={{ marginTop: 4 }}
          autoComplete="off"
        />
      </div>

      {needsPassword && (
        <>
          <div>
            <Typography.Text strong>密码</Typography.Text>
            <Input.Password
              placeholder="登录密码"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              disabled={sessionId !== null}
              style={{ marginTop: 4 }}
              autoComplete="new-password"
              onPressEnter={onLogin}
            />
          </div>
          <Checkbox
            checked={remember}
            onChange={(e) => setRemember(e.target.checked)}
            disabled={sessionId !== null}
          >
            记住密码(仅后端本地保存, 不进 Git)
          </Checkbox>
        </>
      )}

      {!needsPassword && !sessionId && (
        <Alert
          type="info"
          showIcon
          message="短信验证码登录"
          description={'点击下方按钮后, 浏览器会勾选同意条款并点"获取验证码"。短信到手后填入即可。'}
        />
      )}

      {!sessionId && (
        <Button type="primary" icon={<LoginOutlined />} onClick={onLogin} loading={starting} block>
          {needsPassword ? '登录' : '发送验证码'}
        </Button>
      )}

      {awaitingOtp && (
        <div>
          <Typography.Text strong>短信验证码</Typography.Text>
          <Space.Compact style={{ width: '100%', marginTop: 4 }}>
            <Input
              placeholder="收到短信后输入"
              value={otpCode}
              onChange={(e) => setOtpCode(e.target.value)}
              onPressEnter={onSubmitOtp}
              disabled={submittingOtp}
              maxLength={8}
              autoFocus
            />
            <Button type="primary" onClick={onSubmitOtp} loading={submittingOtp}>
              提交
            </Button>
          </Space.Compact>
        </div>
      )}

      {status && (
        <Alert
          type={
            status.status === 'SUCCESS' ? 'success' :
            status.status === 'FAILED' ? 'error' :
            status.status === 'LOCKED_OUT' ? 'warning' : 'info'
          }
          showIcon
          message={`状态: ${status.status}`}
          description={status.message}
        />
      )}

      {(sessionId || finished) && (
        <Button onClick={reset} block>
          {finished ? '再来一次' : '取消'}
        </Button>
      )}
    </Space>
  )
}

// ── Quick-login button (uses saved identifier+password) ──────────────────

function QuickLoginButton({ platform, onLoggedIn }: { platform: PlatformItem; onLoggedIn: () => void }) {
  const [loading, setLoading] = useState(false)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const pollOnce = async (sid: string) => {
    try {
      const res = await api.get(`/data-sources/${platform.key}/login/${sid}`)
      if (['SUCCESS', 'FAILED', 'LOCKED_OUT', 'NOT_FOUND'].includes(res.data.status)) {
        if (pollRef.current) clearInterval(pollRef.current)
        setLoading(false)
        if (res.data.status === 'SUCCESS') {
          message.success(`${platform.display_name} 登录成功`)
          onLoggedIn()
        } else if (res.data.status === 'LOCKED_OUT') {
          message.warning({
            content: res.data.message || `${platform.display_name} · 平台已锁定 10 分钟`,
            duration: 10,
          })
        } else {
          message.error(res.data.message || '登录失败')
        }
      }
    } catch {
      /* keep polling */
    }
  }

  const onClick = async () => {
    setLoading(true)
    try {
      const res = await api.post(`/data-sources/${platform.key}/login/saved`, {})
      const sid = res.data.session_id
      pollRef.current = setInterval(() => pollOnce(sid), 600)
      pollOnce(sid)
    } catch (e: any) {
      setLoading(false)
      message.error(e.response?.data?.detail || '启动失败')
    }
  }

  return (
    <Tooltip title={`使用已保存的密码登录 (${platform.saved_identifier})`}>
      <Button
        type="text"
        icon={<ThunderboltOutlined style={{ color: '#f59e0b' }} />}
        loading={loading}
        onClick={onClick}
      >
        闪电登录
      </Button>
    </Tooltip>
  )
}

// ── Manual token paste tab ───────────────────────────────────────────────

function ManualTab({ platform, onSaved }: TabProps) {
  const [form] = Form.useForm()
  const [saving, setSaving] = useState(false)

  const fieldKeys = Object.keys(platform.token_fields)

  const onSave = async (values: Record<string, string>) => {
    const filtered: Record<string, string> = {}
    for (const [k, v] of Object.entries(values)) {
      if (typeof v === 'string' && v.trim()) {
        filtered[k] = v.trim()
      }
    }
    if (Object.keys(filtered).length === 0) {
      message.warning('请至少填写一个字段')
      return
    }
    setSaving(true)
    try {
      await api.post(`/data-sources/${platform.key}/token`, { fields: filtered })
      message.success('已保存 Token')
      onSaved()
    } catch (e: any) {
      message.error(e.response?.data?.detail || '保存失败')
    } finally {
      setSaving(false)
    }
  }

  return (
    <Form layout="vertical" form={form} onFinish={onSave}>
      <Alert
        type="warning"
        showIcon
        style={{ marginBottom: 16 }}
        message="手动粘贴 Token"
        description="浏览器登录后打开 F12 → Application → Local Storage / Cookies, 找到对应字段的值粘贴下来。留空则保留现有值。"
      />

      {fieldKeys.map((k) => (
        <Form.Item
          label={
            <Space>
              <span>{k}</span>
              <Typography.Text type="secondary" style={{ fontSize: 11 }}>
                当前: {platform.token_fields[k] || '(空)'}
              </Typography.Text>
            </Space>
          }
          name={k}
          key={k}
        >
          <Input.TextArea autoSize={{ minRows: 2, maxRows: 6 }} placeholder="粘贴新值…" />
        </Form.Item>
      ))}

      <Form.Item>
        <Button type="primary" htmlType="submit" loading={saving} block>
          保存
        </Button>
      </Form.Item>
    </Form>
  )
}

// ── Crawler lifecycle control (per-card) ─────────────────────────────────

interface CrawlerState {
  platform: string
  running: boolean
  pid: number
  uptime_s: number
  started_at: number | null
  log_tail: string
  log_path: string
}

function fmtUptime(s: number): string {
  if (!s) return '—'
  if (s < 60) return `${s}秒`
  if (s < 3600) return `${Math.floor(s / 60)}分${s % 60}秒`
  const h = Math.floor(s / 3600)
  const m = Math.floor((s % 3600) / 60)
  return `${h}小时${m}分`
}

function CrawlerControl({ platformKey }: { platformKey: string }) {
  const [state, setState] = useState<CrawlerState | null>(null)
  const [busy, setBusy] = useState(false)
  const [logOpen, setLogOpen] = useState(false)

  const fetchState = async () => {
    try {
      const res = await api.get<CrawlerState>(`/data-sources/${platformKey}/crawler`)
      setState(res.data)
    } catch {
      /* transient */
    }
  }

  useEffect(() => {
    fetchState()
    const id = setInterval(fetchState, 8000) // 8s cadence is fine for crawler
    return () => clearInterval(id)
  }, [platformKey])

  const onStart = async () => {
    setBusy(true)
    try {
      await api.post(`/data-sources/${platformKey}/crawler/start`)
      message.success('爬虫已启动')
      fetchState()
    } catch (e: any) {
      message.error(e.response?.data?.detail || '启动失败')
    } finally {
      setBusy(false)
    }
  }

  const onStop = async () => {
    setBusy(true)
    try {
      await api.post(`/data-sources/${platformKey}/crawler/stop`)
      message.success('爬虫已停止')
      fetchState()
    } catch (e: any) {
      message.error(e.response?.data?.detail || '停止失败')
    } finally {
      setBusy(false)
    }
  }

  if (!state) return null

  return (
    <div style={{ marginTop: 10, paddingTop: 10, borderTop: '1px dashed #e2e8f0' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Space size={6}>
          <Badge
            status={state.running ? 'processing' : 'default'}
            text={
              <Typography.Text style={{ fontSize: 12 }}>
                爬虫: {state.running ? `运行中 · ${fmtUptime(state.uptime_s)}` : '已停止'}
              </Typography.Text>
            }
          />
        </Space>
        <Space size={4}>
          {state.log_tail && (
            <Tooltip title="查看日志">
              <Button
                type="text"
                size="small"
                icon={<BugOutlined />}
                onClick={() => setLogOpen(true)}
              />
            </Tooltip>
          )}
          {state.running ? (
            <Popconfirm title="确认停止爬虫?" onConfirm={onStop} okText="停止">
              <Button type="text" size="small" icon={<PauseCircleOutlined />} danger loading={busy}>
                停止
              </Button>
            </Popconfirm>
          ) : (
            <Button
              type="text"
              size="small"
              icon={<PlayCircleOutlined style={{ color: '#10b981' }} />}
              onClick={onStart}
              loading={busy}
            >
              启动
            </Button>
          )}
        </Space>
      </div>

      <Drawer
        title={`爬虫日志 · ${platformKey}`}
        open={logOpen}
        onClose={() => setLogOpen(false)}
        width={720}
        extra={
          <Button size="small" onClick={fetchState} icon={<ReloadOutlined />}>
            刷新
          </Button>
        }
      >
        <Typography.Paragraph type="secondary" style={{ fontSize: 11 }}>
          {state.log_path} · PID {state.pid} · uptime {fmtUptime(state.uptime_s)}
        </Typography.Paragraph>
        <pre
          style={{
            background: '#0f172a',
            color: '#e2e8f0',
            padding: 12,
            borderRadius: 6,
            fontSize: 11,
            maxHeight: '70vh',
            overflow: 'auto',
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-all',
          }}
        >
          {state.log_tail || '(无日志)'}
        </pre>
      </Drawer>
    </div>
  )
}
