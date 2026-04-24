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
  DatabaseOutlined,
  AppstoreOutlined,
  LinkOutlined,
  CrownOutlined,
  StockOutlined,
  TrophyOutlined,
  SolutionOutlined,
  RobotOutlined,
  AimOutlined,
  ProfileOutlined,
  AudioOutlined,
  GlobalOutlined,
  FundProjectionScreenOutlined,
  ExperimentOutlined,
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

  // Build menu items based on role — grouped by researcher workflow
  const menuItems: any[] = [
    {
      type: 'group',
      label: '研究工作台',
      children: [
        { key: '/', icon: <DashboardOutlined />, label: t('nav.dashboard') },
        { key: '/ai-chat', icon: <RobotOutlined />, label: 'AI 研究助手' },
        {
          key: '/modeling',
          icon: <FileTextOutlined />,
          label: '收入拆分建模',
          children: [
            { key: '/modeling', label: '模型列表' },
            { key: '/modeling/cost', label: '成本仪表盘' },
            { key: '/modeling/feedback', label: '反馈闭环' },
            { key: '/modeling/expert-calls', label: '专家访谈请求' },
            { key: '/modeling/recipes', label: 'Recipe 编辑器' },
            { key: '/modeling/playbook', label: 'Playbook / Lessons' },
          ],
        },
        { key: '/my-knowledge', icon: <DatabaseOutlined />, label: '个人知识库' },
        { key: '/feed', icon: <FileTextOutlined />, label: t('nav.feed') },
      ],
    },
    {
      type: 'group',
      label: '深度研究',
      children: [
        { key: '/alphapai/digest', icon: <ProfileOutlined />, label: '每日简报' },
        {
          key: '/alphapai',
          icon: <ReadOutlined />,
          label: t('nav.alphapai'),
          children: [
            { key: '/alphapai/platform-info', label: '平台信息' },
            { key: '/alphapai/reports', label: t('nav.alphapaiReports') },
            { key: '/alphapai/roadshows', label: t('nav.alphapaiRoadshows') },
            { key: '/alphapai/comments', label: t('nav.alphapaiComments') },
            { key: '/alphapai/feed', label: t('nav.alphapaiFeed') },
          ],
        },
        {
          key: '/jiuqian',
          icon: <CrownOutlined />,
          label: t('nav.jiuqian'),
          children: [
            { key: '/meritco/minutes', label: '纪要' },
            { key: '/meritco/research', label: '研究' },
            { key: '/meritco/weekly', label: '调研周报' },
          ],
        },
        {
          key: '/jinmen',
          icon: <AudioOutlined />,
          label: '进门专区',
          children: [
            { key: '/jinmen/platform-info', label: '平台信息' },
            { key: '/jinmen/meetings', label: '纪要' },
            { key: '/jinmen/reports', label: '研报' },
            { key: '/jinmen/oversea-reports', label: '外资研报' },
          ],
        },
        {
          key: '/thirdbridge',
          icon: <GlobalOutlined />,
          label: '高临专区',
          children: [
            { key: '/thirdbridge/interviews', label: '专家访谈' },
          ],
        },
        {
          key: '/funda',
          icon: <FundProjectionScreenOutlined />,
          label: 'Funda 专区',
          children: [
            { key: '/funda/posts', label: t('nav.fundaPosts') },
            { key: '/funda/earnings-reports', label: t('nav.fundaEarningsReports') },
            { key: '/funda/earnings-transcripts', label: t('nav.fundaEarningsTranscripts') },
            { key: '/funda/sentiment', label: t('nav.fundaSentiment') },
          ],
        },
        {
          key: '/gangtise',
          icon: <ReadOutlined />,
          label: t('nav.gangtise'),
          children: [
            { key: '/gangtise/platform-info', label: '平台信息' },
            { key: '/gangtise/summary', label: t('nav.gangtiseSummary') },
            { key: '/gangtise/research', label: t('nav.gangtiseResearch') },
            { key: '/gangtise/chief', label: t('nav.gangtiseChief') },
          ],
        },
        {
          key: '/acecamp',
          icon: <ReadOutlined />,
          label: t('nav.acecamp'),
          children: [
            { key: '/acecamp/platform-info', label: '平台信息' },
            { key: '/acecamp/minutes', label: t('nav.acecampMinutes') },
            { key: '/acecamp/research', label: t('nav.acecampResearch') },
            { key: '/acecamp/article', label: t('nav.acecampArticle') },
            { key: '/acecamp/opinion', label: t('nav.acecampOpinion') },
          ],
        },
        {
          key: '/alphaengine',
          icon: <ReadOutlined />,
          label: t('nav.alphaengine'),
          children: [
            { key: '/alphaengine/summary', label: t('nav.alphaengineSummary') },
            { key: '/alphaengine/china-report', label: t('nav.alphaengineChinaReport') },
            { key: '/alphaengine/foreign-report', label: t('nav.alphaengineForeignReport') },
            { key: '/alphaengine/news', label: t('nav.alphaengineNews') },
          ],
        },
        {
          key: '/semianalysis',
          icon: <ExperimentOutlined />,
          label: t('nav.semianalysis'),
        },
      ],
    },
    {
      type: 'group',
      label: '跟踪管理',
      children: [
        { key: '/watchlists', icon: <StarOutlined />, label: t('nav.watchlists') },
        { key: '/favorites', icon: <StarFilled />, label: t('favorites.title') },
      ],
    },
    {
      type: 'group',
      label: '评估排行',
      children: [
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
        { key: '/leaderboard', icon: <TrophyOutlined />, label: t('nav.leaderboard') },
        { key: '/analyst-rating', icon: <SolutionOutlined />, label: t('nav.analystRating') },
      ],
    },
    {
      type: 'group',
      label: '配置',
      children: [
        { key: '/sources', icon: <LinkOutlined />, label: t('nav.sources') },
        { key: '/data-sources', icon: <DatabaseOutlined />, label: t('nav.dataSources') },
        { key: '/database-overview', icon: <DatabaseOutlined />, label: t('nav.databaseOverview') },
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
        { key: '/admin/research-logs', icon: <ProfileOutlined />, label: t('nav.adminResearchLogs') },
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

  // Determine open submenu (digest is standalone, not inside alphapai submenu)
  const openKeys: string[] = []
  if (location.pathname.startsWith('/alphapai') && location.pathname !== '/alphapai/digest') openKeys.push('/alphapai')
  if (location.pathname.startsWith('/jiuqian') || location.pathname.startsWith('/meritco')) openKeys.push('/jiuqian')
  if (location.pathname.startsWith('/jinmen')) openKeys.push('/jinmen')
  if (location.pathname.startsWith('/thirdbridge')) openKeys.push('/thirdbridge')
  if (location.pathname.startsWith('/funda')) openKeys.push('/funda')
  if (location.pathname.startsWith('/gangtise')) openKeys.push('/gangtise')
  if (location.pathname.startsWith('/acecamp')) openKeys.push('/acecamp')
  if (location.pathname.startsWith('/alphaengine')) openKeys.push('/alphaengine')
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
