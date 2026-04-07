import { useState, useEffect, useRef, useCallback } from 'react'
import { Outlet, useNavigate, useLocation } from 'react-router-dom'
import { Layout, Menu, Avatar, Dropdown, Badge, Input, AutoComplete, Tag, Spin, Typography } from 'antd'
import {
  DashboardOutlined,
  FileTextOutlined,
  StarOutlined,
  StarFilled,
  BarChartOutlined,
  SettingOutlined,
  TeamOutlined,
  BellOutlined,
  SearchOutlined,
  LogoutOutlined,
  UserOutlined,
  ReadOutlined,
  RocketOutlined,
  FundOutlined,
  DatabaseOutlined,
  AppstoreOutlined,
  LinkOutlined,
  CrownOutlined,
  StockOutlined,
  RadarChartOutlined,
  TrophyOutlined,
  SolutionOutlined,
  RobotOutlined,
  AimOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { useAuthStore } from '../store/auth'
import api from '../services/api'

const { Header, Sider, Content } = Layout
const { Text } = Typography

interface StockSuggestion {
  name: string
  code: string
  market: string
  label: string
}

const MARKET_COLORS: Record<string, string> = {
  'A股': '#e11d48',
  '美股': '#2563eb',
  '港股': '#7c3aed',
}

export default function AppLayout() {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const location = useLocation()
  const user = useAuthStore((s) => s.user)
  const logout = useAuthStore((s) => s.logout)
  const fetchProfile = useAuthStore((s) => s.fetchProfile)
  const [collapsed, setCollapsed] = useState(false)

  // Header search autocomplete state
  const [headerInput, setHeaderInput] = useState('')
  const [headerSuggestions, setHeaderSuggestions] = useState<StockSuggestion[]>([])
  const [headerSuggestLoading, setHeaderSuggestLoading] = useState(false)
  const suggestTimer = useRef<ReturnType<typeof setTimeout>>()

  useEffect(() => {
    if (!user) fetchProfile()
  }, [user, fetchProfile])

  const isAdmin = user?.role === 'admin'
  const isBossOrAdmin = user?.role === 'admin' || user?.role === 'boss'

  // Header search: fetch suggestions
  const fetchHeaderSuggestions = useCallback(async (text: string) => {
    if (!text || text.length < 1) {
      setHeaderSuggestions([])
      return
    }
    setHeaderSuggestLoading(true)
    try {
      const res = await api.get<StockSuggestion[]>('/stock/suggest', {
        params: { q: text, limit: 8 },
      })
      setHeaderSuggestions(res.data)
    } catch {
      setHeaderSuggestions([])
    } finally {
      setHeaderSuggestLoading(false)
    }
  }, [])

  const onHeaderSearch = (text: string) => {
    setHeaderInput(text)
    if (suggestTimer.current) clearTimeout(suggestTimer.current)
    suggestTimer.current = setTimeout(() => fetchHeaderSuggestions(text), 200)
  }

  const onHeaderSelect = (_value: string, option: any) => {
    const stock = option.stock as StockSuggestion
    setHeaderInput('')
    setHeaderSuggestions([])
    navigate(`/stock-search?q=${encodeURIComponent(stock.label)}`)
  }

  const onHeaderEnter = () => {
    const val = headerInput.trim()
    if (!val) return
    setHeaderInput('')
    setHeaderSuggestions([])
    navigate(`/stock-search?q=${encodeURIComponent(val)}`)
  }

  const headerOptions = headerSuggestions.map((s) => ({
    value: s.label,
    label: (
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span>
          <StockOutlined style={{ marginRight: 6, color: '#94a3b8' }} />
          <b>{s.name}</b>
          <span style={{ color: '#94a3b8', marginLeft: 6 }}>{s.code}</span>
        </span>
        <Tag
          color={MARKET_COLORS[s.market] || '#94a3b8'}
          style={{ margin: 0, fontSize: 11 }}
        >
          {s.market}
        </Tag>
      </div>
    ),
    stock: s,
  }))

  // Build menu items based on role
  const menuItems: any[] = [
    {
      type: 'group',
      label: t('nav.sectionTrading'),
      children: [
        { key: '/', icon: <DashboardOutlined />, label: t('nav.dashboard') },
        { key: '/feed', icon: <FileTextOutlined />, label: t('nav.feed') },
        { key: '/topic-radar', icon: <RadarChartOutlined />, label: '舆情雷达' },
        { key: '/stock-search', icon: <SearchOutlined />, label: '股票搜索' },
        { key: '/watchlists', icon: <StarOutlined />, label: t('nav.watchlists') },
        { key: '/favorites', icon: <StarFilled />, label: t('favorites.title') },
        { key: '/leaderboard', icon: <TrophyOutlined />, label: t('nav.leaderboard') },
        { key: '/analyst-rating', icon: <SolutionOutlined />, label: t('nav.analystRating') },
        { key: '/ai-chat', icon: <RobotOutlined />, label: t('nav.aiChat') },
        {
          key: '/predictions',
          icon: <AimOutlined />,
          label: t('nav.predictions'),
          children: [
            { key: '/predictions', label: t('nav.predictionList') },
            { key: '/predictions/submit', label: t('nav.predictionSubmit') },
            ...(isBossOrAdmin
              ? [{ key: '/predictions/backtest', label: t('nav.predictionBacktest') }]
              : []),
          ],
        },
        ...(isBossOrAdmin
          ? [{ key: '/portfolio', icon: <FundOutlined />, label: t('nav.portfolio') }]
          : []),
      ],
    },
    {
      type: 'group',
      label: t('nav.sectionResearch'),
      children: [
        {
          key: '/alphapai',
          icon: <ReadOutlined />,
          label: t('nav.alphapai'),
          children: [
            { key: '/alphapai/digest', label: t('nav.alphapaiDigest') },
            { key: '/alphapai/feed', label: t('nav.alphapaiFeed') },
            { key: '/alphapai/roadshows', label: t('nav.alphapaiRoadshows') },
            { key: '/alphapai/comments', label: t('nav.alphapaiComments') },
          ],
        },
        {
          key: '/jiuqian',
          icon: <CrownOutlined />,
          label: t('nav.jiuqian'),
          children: [
            { key: '/jiuqian/forum', label: t('nav.jiuqianForum') },
            { key: '/jiuqian/minutes', label: t('nav.jiuqianMinutes') },
            { key: '/jiuqian/wechat', label: t('nav.jiuqianWechat') },
          ],
        },
        { key: '/sources', icon: <LinkOutlined />, label: t('nav.sources') },
      ],
    },
  ]

  // Admin-only section
  if (isAdmin) {
    menuItems.push({
      type: 'group',
      label: t('nav.sectionSystem'),
      children: [
        { key: '/admin', icon: <TeamOutlined />, label: t('nav.adminUsers') },
        { key: '/admin/feed', icon: <DatabaseOutlined />, label: t('nav.adminFeed') },
        { key: '/admin/sources', icon: <AppstoreOutlined />, label: t('nav.adminSources') },
        { key: '/engine', icon: <RocketOutlined />, label: t('nav.adminEngine') },
        { key: '/analytics', icon: <BarChartOutlined />, label: t('nav.adminAnalytics') },
      ],
    })
  }

  const userMenu = {
    items: [
      {
        key: 'user-info',
        label: (
          <div style={{ padding: '4px 0' }}>
            <div style={{ fontWeight: 600, fontSize: 13 }}>{user?.display_name || user?.username}</div>
            <div style={{ fontSize: 11, color: '#94a3b8' }}>{user?.email}</div>
          </div>
        ),
        disabled: true,
      },
      { type: 'divider' as const },
      { key: 'settings', icon: <SettingOutlined />, label: t('nav.settings') },
      { type: 'divider' as const },
      { key: 'logout', icon: <LogoutOutlined />, label: t('auth.logout'), danger: true },
    ],
    onClick: ({ key }: { key: string }) => {
      if (key === 'logout') {
        logout()
        navigate('/login')
      } else if (key === 'settings') {
        navigate('/settings')
      }
    },
  }

  // Determine selected key
  const selectedKey = location.pathname === '/' ? '/' : location.pathname

  // Determine open submenu
  const openKeys: string[] = []
  if (location.pathname.startsWith('/alphapai')) openKeys.push('/alphapai')
  if (location.pathname.startsWith('/jiuqian')) openKeys.push('/jiuqian')
  if (location.pathname.startsWith('/predictions')) openKeys.push('/predictions')

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider
        collapsible
        collapsed={collapsed}
        onCollapse={setCollapsed}
        width={210}
        className="sidebar-custom"
        theme="dark"
      >
        <div className="sidebar-logo">
          <div className="logo-icon">TI</div>
          {!collapsed && <div className="logo-text">{t('app.title')}</div>}
        </div>
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[selectedKey]}
          defaultOpenKeys={openKeys}
          items={menuItems}
          onClick={({ key }) => navigate(key)}
          className="sidebar-menu"
        />
      </Sider>
      <Layout>
        <Header className="header-bar">
          <div className="header-search">
            <AutoComplete
              value={headerInput}
              options={headerOptions}
              onSearch={onHeaderSearch}
              onSelect={onHeaderSelect}
              style={{ width: 320 }}
              notFoundContent={
                headerSuggestLoading ? <Spin size="small" /> :
                headerInput.length >= 1 ? <Text type="secondary" style={{ fontSize: 12 }}>回车搜索全部相关内容</Text> : null
              }
            >
              <Input
                prefix={<SearchOutlined style={{ color: '#94a3b8' }} />}
                placeholder="搜索股票名称/代码..."
                onPressEnter={onHeaderEnter}
                allowClear
              />
            </AutoComplete>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
            <Badge count={0} size="small">
              <BellOutlined style={{ fontSize: 16, cursor: 'pointer', color: '#64748b' }} />
            </Badge>
            <Dropdown menu={userMenu} placement="bottomRight" trigger={['click']}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer' }}>
                <Avatar
                  size={30}
                  style={{ backgroundColor: '#2563eb', fontSize: 12 }}
                  icon={<UserOutlined />}
                />
                {!collapsed && (
                  <span style={{ fontSize: 13, color: '#475569', fontWeight: 500 }}>
                    {user?.display_name || user?.username}
                  </span>
                )}
              </div>
            </Dropdown>
          </div>
        </Header>
        <Content className="main-content">
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  )
}
